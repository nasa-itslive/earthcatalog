"""CLI-level daily-ingest flow — the exact commands daily_delta.yml runs.

Hermetic end-to-end through the typer CLI (not the library layer), on a
LocalStore warehouse + local SQLite Iceberg catalog with a faked STAC
fetcher.  Runs in the PR gate so the daily path cannot regress silently:

1. day-1 bootstrap:  ``earthcatalog ingest --inventory day1 --mode full``
2. daily diff:       ``earthcatalog diff --current day2 --previous day1``
3. daily ingest:     ``earthcatalog ingest --diff new.parquet --mode delta``
4. idempotency:      re-running step 3 is a no-op
5. postconditions:   items searchable via EarthCatalog; ``_last_run.json``
   written to the warehouse root.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

_LM = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)


def _write_day(path: Path, rows: list[tuple[str, int]]) -> str:
    """One inventory day parquet: (bucket, key, size, last_modified_date)."""
    tbl = pa.table(
        {
            "bucket": ["data-bucket"] * len(rows),
            "key": [k for k, _ in rows],
            "size": [s for _, s in rows],
            "last_modified_date": [_LM] * len(rows),
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    path.write_bytes(buf.getvalue())
    return str(path)


@pytest.fixture(autouse=True)
def _restore_store_config():
    """The CLI ingest path mutates the legacy store_config globals
    (set_store/set_catalog_key/set_lock_key); restore them so the rest of
    the suite is unaffected."""
    from earthcatalog import store_config

    saved = (store_config._store, store_config._catalog_key, store_config._lock_key)
    yield
    store_config.set_store(saved[0])
    store_config.set_catalog_key(saved[1])
    store_config.set_lock_key(saved[2])


@pytest.fixture()
def cli_env(monkeypatch, tmp_path):
    """Faked credentials + STAC fetcher so the CLI path never touches S3."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    def _fetch(bucket: str, key: str) -> dict:
        item_id = key.rsplit("/", 1)[-1].removesuffix(".stac.json")
        return {
            "id": item_id,
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "bbox": [0.0, 0.0, 0.0, 0.0],
            "properties": {
                "datetime": "2020-05-01T00:00:00Z",
                "platform": "sentinel-1",
            },
            "_source_bucket": bucket,
            "_source_key": key,
        }

    # The serial fetcher routes to fetch_items_async when the resolved
    # fetch_fn *is* the module attribute — patch both sides.
    monkeypatch.setattr("earthcatalog.inventory.fetch_item", _fetch)
    monkeypatch.setattr(
        "earthcatalog.inventory.fetch_items_async",
        lambda pairs, **kw: [it for it in (_fetch(b, k) for b, k in pairs) if it],
    )
    return tmp_path


def _invoke(app, args: list[str]) -> str:
    result = CliRunner().invoke(app, args)
    if result.exit_code != 0:
        raise AssertionError(f"earthcatalog {' '.join(args)} failed:\n{result.output}")
    return result.output


class TestDailyWorkflow:
    def test_diff_then_ingest_via_cli(self, cli_env):
        from earthcatalog.cli import app

        tmp_path = cli_env
        warehouse = tmp_path / "warehouse"
        catalog = str(tmp_path / "catalog.db")
        wh = str(warehouse)
        common = [
            "--catalog", catalog,
            "--warehouse", wh,
            "--catalog-key", "earthcatalog.db",
            "--scheduler", "synchronous",
            "--chunk-size", "10000",
        ]

        # --- Day 1: bootstrap full ingest ----------------------------------
        day1 = _write_day(
            tmp_path / "day1.parquet",
            [("dir/a.stac.json", 100), ("dir/b.stac.json", 200)],
        )
        _invoke(app, ["ingest", "--inventory", day1, "--mode", "full", *common])

        # --- Day 2: the daily diff (day2 EXCEPT day1) ----------------------
        day2 = _write_day(
            tmp_path / "day2.parquet",
            [("dir/b.stac.json", 200), ("dir/c.stac.json", 400)],
        )
        new_path = str(tmp_path / "new-day1-day2.parquet")
        out = _invoke(
            app,
            ["diff", "--current", day2, "--previous", day1, "--out", new_path],
        )
        assert "new keys: 1" in out, out  # only c; b is byte-identical

        # --- Day 2: ingest the diff (the daily job) -------------------------
        out = _invoke(app, ["ingest", "--diff", new_path, "--mode", "delta", *common])
        assert "fetched 1" in out, out  # only the new key; the anti-join skipped the rest

        # --- Idempotency: re-running the same command is a no-op ------------
        out = _invoke(app, ["ingest", "--diff", new_path, "--mode", "delta", *common])
        assert "fetched 0" in out, out
        # _last_run.json is a sibling of the warehouse dir (the store is
        # rooted at the parent — same convention as the index).
        last_run = json.loads((tmp_path / "_last_run.json").read_text())
        assert last_run["items"] == 0, last_run

        # --- Postconditions: searchable via the facade ----------------------
        from obstore.store import LocalStore

        from earthcatalog.catalog import (
            EarthCatalog,
            _catalog_info,
            get_or_create,
            open_sqlite,
        )

        cat = open_sqlite(db_path=catalog, warehouse_path=wh)
        table = get_or_create(cat)
        ec = EarthCatalog(
            catalog=cat,
            table=table,
            info=_catalog_info(table),
            store=LocalStore(wh),
            catalog_key=None,
        )
        ids = {
            it.id
            for it in ec.search(bbox=[-1, -1, 1, 1], datetime="2020-01-01/2021-01-01").item_collection()
        }
        assert ids == {"a", "b", "c"}, ids
