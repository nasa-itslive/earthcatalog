"""Tests for earthcatalog/index_backfill.py (local-first pointer backfill)."""

from __future__ import annotations

import hashlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import LocalStore

from earthcatalog import index_backfill as ib
from earthcatalog.index import Index


def _write_warehouse(root, cell, year, ids):
    d = root / "warehouse" / "grid=h3" / "level=1" / f"tile={cell}" / f"year={year}"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"id": pa.array(ids)}), d / "part_000.parquet")


@pytest.fixture()
def env(tmp_path):
    """Warehouse: A@{c1,c2}, B@c1, C@c3.  Index knows only A@c1, B@c1."""
    root = tmp_path
    _write_warehouse(root, "c1", 2020, ["A", "B"])
    _write_warehouse(root, "c2", 2020, ["A"])
    _write_warehouse(root, "c3", 2021, ["C"])
    work = root / "work"
    (work / "index").mkdir(parents=True)
    store = LocalStore(str(work / "index"))
    Index(store, "warehouse_index").append(
        [
            {"s3_key": "s3://b/A.stac.json", "stac_id": "A", "grid_partition": "c1", "year": 2020},
            {"s3_key": "s3://b/B.stac.json", "stac_id": "B", "grid_partition": "c1", "year": 2020},
        ]
    )
    con = duckdb.connect()
    scan = ib.scan_warehouse(LocalStore(str(root / "warehouse")), str(root / "warehouse"), work, con)
    return {
        "root": root,
        "work": work,
        "store": store,
        "con": con,
        "scan_rows": scan["rows"],
    }


def _hash_dir(path):
    out = {}
    for p in sorted(path.rglob("*.parquet")):
        out[str(p.relative_to(path))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


class TestScan:
    def test_counts_every_record_copy(self, env):
        assert env["scan_rows"] == 4

    def test_cache_is_reused(self, env):
        n = ib.scan_warehouse(
            LocalStore(str(env["root"] / "warehouse")), str(env["root"] / "warehouse"), env["work"], env["con"]
        )
        assert n["cached"] is True and n["rows"] == 4


class TestReport:
    def test_missing_pairs_and_keyless_granules(self, env):
        rep = ib.report(env["work"] / "warehouse_triples.parquet", ib.staged_locations(env["work"]), env["con"])
        assert rep["warehouse_rows"] == 4
        assert rep["index_rows"] == 2
        assert rep["distinct_keys"] == 2
        assert rep["missing_pairs"] == 2  # A@c2, C@c3
        assert rep["granules_without_key"] == 1  # C


class TestBuild:
    def test_emits_exactly_the_missing_pairs(self, env):
        locs = ib.staged_locations(env["work"])
        before = _hash_dir(env["work"] / "index")
        m = ib.build(
            env["work"] / "warehouse_triples.parquet",
            locs,
            env["store"],
            "warehouse_index",
            env["work"],
            env["con"],
            run_id="t",
            chunk_rows=1,
        )
        assert m.rows_written == 2
        assert [p["part"] for p in m.parts] == ["t--0000", "t--0001"]
        # existing parts untouched
        after_existing = {k: v for k, v in _hash_dir(env["work"] / "index").items() if k in before}
        assert after_existing == before
        # full verification passes
        v = ib.verify(env["work"] / "warehouse_triples.parquet", ib.staged_locations(env["work"]), env["con"])
        assert v["index_rows"] == 4
        assert v["distinct_keys"] == 2
        assert v["duplicate_pairs"] == 0
        assert v["cards_without_copy"] == 0
        assert v["copies_without_card"] == 0

    def test_rerun_is_a_no_op(self, env):
        common = dict(
            cache=env["work"] / "warehouse_triples.parquet",
            index_locs=ib.staged_locations(env["work"]),
            out_store=env["store"],
            index_key="warehouse_index",
            work_dir=env["work"],
            con=env["con"],
            run_id="t",
            chunk_rows=1,
        )
        m1 = ib.build(**common)
        m2 = ib.build(**common)
        assert m2.rows_written == m1.rows_written
        # still exactly 4 index rows, no duplicates
        v = ib.verify(env["work"] / "warehouse_triples.parquet", ib.staged_locations(env["work"]), env["con"])
        assert v["index_rows"] == 4 and v["duplicate_pairs"] == 0

    def test_detects_new_warehouse_copy_without_card(self, env):
        _write_warehouse(env["root"], "c9", 2022, ["B"])
        # force a fresh cache to pick up the new copy
        (env["work"] / "warehouse_triples.parquet").unlink()
        ib.scan_warehouse(
            LocalStore(str(env["root"] / "warehouse")), str(env["root"] / "warehouse"), env["work"], env["con"]
        )
        v = ib.verify(env["work"] / "warehouse_triples.parquet", ib.staged_locations(env["work"]), env["con"])
        assert v["copies_without_card"] == 3  # A@c2, C@c3, and the new B@c9


class TestUploadRollback:
    def test_upload_copies_then_rollback_deletes(self, env, tmp_path):
        m = ib.build(
            env["work"] / "warehouse_triples.parquet",
            ib.staged_locations(env["work"]),
            env["store"],
            "warehouse_index",
            env["work"],
            env["con"],
            run_id="t",
            chunk_rows=1_000,
        )
        dest_root = tmp_path / "live"
        dest_root.mkdir()
        dest = LocalStore(str(dest_root))
        n = ib.upload(m, env["store"], "warehouse_index", dest)
        assert n == 2
        for p in m.parts:
            data = (tmp_path / "live" / "warehouse_index" / f"{p['part']}.parquet").read_bytes()
            assert len(data) > 0
        removed = ib.rollback(m, dest, "warehouse_index")
        assert removed == 1  # 2 rows, chunk_rows=1000 → one part
        assert not any((tmp_path / "live").rglob("*.parquet"))


class TestManifest:
    def test_round_trip(self, tmp_path):
        m = ib.BackfillManifest(run_id="r", chunk_rows=10, parts=[{"part": "r--0000", "rows": 10}], rows_written=10)
        m.save(tmp_path)
        m2 = ib.BackfillManifest.load(tmp_path)
        assert m2.run_id == "r" and m2.rows_written == 10


def test_warehouse_scan_requires_hive_files(tmp_path):
    con = duckdb.connect()
    with pytest.raises(RuntimeError):
        ib.scan_warehouse(LocalStore(str(tmp_path)), str(tmp_path / "nowhere"), tmp_path, con)
