"""Tests for the hardened GC: Iceberg-metadata discovery + fixpoint guarantee.

The historical index can lack pointer rows for some physical copies (data
written before the per-cell pointer convention).  Cleanup must therefore
also consult Iceberg metadata — the source of truth for where copies live —
and iterate until a metadata scan finds no surviving orphan rows.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.gc import iceberg_orphan_file_scan, run_garbage_collection
from earthcatalog.index import Index


def _s3_key(key: str) -> str:
    return f"s3://data-bucket/{key}"


def _write_inventory_csv(path: Path, keys: list[str]) -> Path:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "key"])
        for k in keys:
            writer.writerow(["data-bucket", k])
    return path


def _put_warehouse_file(store, key: str, item_ids: list[str]) -> None:
    buf = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "id": pa.array(item_ids, type=pa.string()),
                "grid_partition": pa.array(["x"] * len(item_ids), type=pa.string()),
                "year": pa.array([2020] * len(item_ids), type=pa.int32()),
            }
        ),
        buf,
        compression="zstd",
    )
    store.put(key, buf.getvalue())


def _read_ids(store, key: str) -> list[str]:
    raw = bytes(store.get(key).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read().column("id").to_pylist()


def _list_keys(store, prefix: str = "") -> list[str]:
    out = []
    for batch in store.list(prefix=prefix):
        for obj in batch:
            out.append(obj["path"])
    return out


def _setup_underindexed_copy():
    """item-B: indexed only in cellA, but a second copy lives in cellB."""
    store = MemoryStore()
    index = Index(store, "warehouse_index.parquet")
    index.append(
        [
            {"s3_key": _s3_key("a.stac.json"), "stac_id": "item-A", "grid_partition": "cellA", "year": 2020},
            {"s3_key": _s3_key("b.stac.json"), "stac_id": "item-B", "grid_partition": "cellA", "year": 2020},
        ]
    )
    _put_warehouse_file(store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B"])
    _put_warehouse_file(store, "grid=h3/level=1/tile=cellB/year=2020/part_9.parquet", ["item-B"])
    discover = lambda ids: {  # noqa: E731
        "grid_partition=cellA/year=2020/part_0.parquet",
        "grid=h3/level=1/tile=cellB/year=2020/part_9.parquet",
    }
    return store, index, discover


class TestIcebergBackedCleanup:
    def test_removes_copy_outside_index_partitions(self, tmp_path):
        store, index, discover = _setup_underindexed_copy()
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])

        result = run_garbage_collection(
            str(inv),
            store=store,
            index=index,
            warehouse_prefix="",
            head_fn=lambda k: k == _s3_key("a.stac.json"),  # noqa: E731
            discover_fn=discover,
        )

        assert result["confirmed"] == 1
        assert result["files_rewritten"] == 2
        assert result["rows_removed"] == 2
        assert result["copies_outside_index"] == 1
        assert result["residual_copies"] == 0
        # both copies physically gone (cellB file fully orphaned -> empty gc_ file)
        gc_b = [k for k in _list_keys(store, "grid=h3/level=1/tile=cellB/") if "/gc_" in k]
        assert len(gc_b) == 1
        assert _read_ids(store, gc_b[0]) == []
        # index: item-B fully marked deleted
        assert [r["stac_id"] for r in index.stream_active()] == ["item-A"]

    def test_dry_run_reports_outside_copies_without_writes(self, tmp_path):
        store, index, discover = _setup_underindexed_copy()
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])

        result = run_garbage_collection(
            str(inv),
            store=store,
            index=index,
            warehouse_prefix="",
            head_fn=lambda k: k == _s3_key("a.stac.json"),  # noqa: E731
            discover_fn=discover,
            dry_run=True,
        )

        assert result["copies_outside_index"] == 1
        assert result["rows_removed"] == 2
        assert result["residual_copies"] == 0  # every copy was located
        # nothing written, nothing deleted, index untouched
        assert _read_ids(store, "grid_partition=cellA/year=2020/part_0.parquet") == ["item-A", "item-B"]
        assert "grid=h3/level=1/tile=cellB/year=2020/part_9.parquet" in _list_keys(store)
        assert [r["stac_id"] for r in index.stream_active()] == ["item-A", "item-B"]

    def test_unlocatable_orphan_raises_and_index_untouched(self, tmp_path):
        """If discovery cannot account for every orphan, refuse to finish."""
        store = MemoryStore()
        index = Index(store, "warehouse_index.parquet")
        index.append(
            [
                {"s3_key": _s3_key("b.stac.json"), "stac_id": "item-B", "grid_partition": "cellA", "year": 2020},
            ]
        )
        # item-B's only copy lives in cellB — nowhere the index names.
        _put_warehouse_file(store, "grid=h3/level=1/tile=cellB/year=2020/part_9.parquet", ["item-B"])
        inv = _write_inventory_csv(tmp_path / "inv.csv", [])

        with pytest.raises(RuntimeError, match="did not converge"):
            run_garbage_collection(
                str(inv),
                store=store,
                index=index,
                warehouse_prefix="",
                head_fn=lambda k: False,  # noqa: E731
                discover_fn=lambda ids: set(),  # finds nothing
            )
        # index rows must NOT be marked deleted on a failed run
        assert [r["stac_id"] for r in index.stream_active()] == ["item-B"]


class _FakeTask:
    def __init__(self, path):
        self.file = type("F", (), {"file_path": path})()


class _FakeScan:
    def __init__(self, paths):
        self._paths = paths

    def plan_files(self):
        return [_FakeTask(p) for p in self._paths]


class _FakeTable:
    def __init__(self, paths):
        self._paths = paths

    def scan(self, row_filter=None):
        assert row_filter is not None  # an In("id", ...) filter must be used
        return _FakeScan(self._paths)


class TestIcebergOrphanFileScan:
    def test_maps_uris_to_store_keys(self):
        table = _FakeTable(["s3://data-bucket/a/part.parquet", "s3://data-bucket/b/part.parquet"])
        discover = iceberg_orphan_file_scan(table)
        assert discover({"x"}) == {"a/part.parquet", "b/part.parquet"}

    def test_batches_large_id_sets(self):
        seen_sizes = []

        class CountingTable(_FakeTable):
            def scan(self, row_filter=None):
                # pyiceberg collapses a 1-literal In into EqualTo
                lits = getattr(row_filter, "literals", None)
                seen_sizes.append(len(lits) if lits is not None else 1)
                return _FakeScan([])

        discover = iceberg_orphan_file_scan(CountingTable([]), batch_size=2)
        discover({f"id-{i}" for i in range(5)})
        assert seen_sizes == [2, 2, 1]

    def test_scan_failure_degrades_to_empty(self):
        class BrokenTable:
            def scan(self, row_filter=None):
                raise RuntimeError("metadata unavailable")

        discover = iceberg_orphan_file_scan(BrokenTable())
        assert discover({"x"}) == set()
