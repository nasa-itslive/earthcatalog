"""End-to-end daily-flow test: diff → ingest --diff → GC.

Simulates two consecutive days of the daily workflow on a LocalStore + a
real local Iceberg SqlCatalog:

1. day-1 ingest: the previous run ingested the full inventory.
2. diff: DuckDB EXCEPT of day-2 vs day-1 → new/changed keys (and the
   disappeared ones), exact on (key, size, last_modified).
3. ingest --diff: the Ingester anti-joins the diff against the unified
   index — already-known keys (the changed re-upload) are skipped, only
   truly new keys are fetched.
4. gc: item dropped from the later inventory is confirmed and removed.
5. re-run the same diff: idempotent no-op.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.diff import run_diff
from earthcatalog.index import Index
from earthcatalog.inventory import iter_inventory_parquet
from earthcatalog.schema import layout_of

_LM = datetime(2026, 9, 5, 1, 0, tzinfo=UTC)


def _make_item(item_id: str, key: str) -> dict:
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [0.0, 0.0, 0.0, 0.0],
        "properties": {
            "datetime": "2020-05-01T00:00:00Z",
            "grid_partition": "cellA",
            "platform": "sentinel-1",
        },
        "_source_bucket": "data-bucket",
        "_source_key": key,
    }


def _write_day(path: Path, rows: list[tuple[str, int]]) -> str:
    """Write one inventory day parquet: (bucket, key, size, last_modified_date)."""
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


def _read_warehouse_ids(store) -> set[str]:
    """All item ids present in warehouse GeoParquet files (not the index)."""
    ids: set[str] = set()
    for batch in store.list(prefix="warehouse/"):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet") and "index" not in k and "staging" not in k:
                raw = bytes(store.get(k).bytes())
                t = pq.ParquetFile(io.BytesIO(raw)).read()
                ids.update(str(i) for i in t.column("id").to_pylist())
    return ids


def _find_index_key(store) -> str:
    """The index base — parts live under it; Index() handles both layouts."""
    for batch in store.list(prefix=""):
        for obj in batch:
            k: str = obj["path"]
            if "warehouse_index" in k:
                return k
    raise AssertionError("no index found")


class TestEndToEndDaily:
    def test_diff_then_ingest_then_gc(self, tmp_path):
        from obstore.store import LocalStore

        store = LocalStore(str(tmp_path))
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        Path(wh).mkdir(parents=True, exist_ok=True)
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
        table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))

        from earthcatalog.ingest import Ingester

        # --- Two synthetic inventory days ---------------------------------
        # day1: items 1,2,3 — day2: item-1 gone, item-3 re-uploaded (size
        # changed), item-4 new, item-2 untouched.
        day1 = _write_day(
            tmp_path / "day1.parquet",
            [
                ("dir/item-1.stac.json", 100),
                ("dir/item-2.stac.json", 200),
                ("dir/item-3.stac.json", 300),
            ],
        )
        day2 = _write_day(
            tmp_path / "day2.parquet",
            [
                ("dir/item-2.stac.json", 200),
                ("dir/item-3.stac.json", 999),
                ("dir/item-4.stac.json", 400),
            ],
        )

        # --- Phase 1: day-1 run ingested the full snapshot -----------------
        def _fetch(bucket, key):
            return _make_item(key.rsplit("/", 1)[-1].removesuffix(".stac.json"), key)

        index = Index(store, "warehouse_index.parquet")
        ing1 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=wh,
        )
        pairs1 = iter_inventory_parquet(day1)
        summary1 = ing1.run(pairs1)
        assert summary1["items"] == 3, summary1

        # --- Phase 2: DuckDB diff day2 vs day1 -----------------------------
        new_path = str(tmp_path / "new-20260905-20260906.parquet")
        old_path = str(tmp_path / "old-20260905-20260906.parquet")
        result = run_diff(current=day2, previous=day1, out=new_path, out_old=old_path)
        # item-3 changed (new tuple), item-4 new; item-1 and item-3 disappeared.
        assert result.new_rows == 2, result
        assert result.old_rows == 2, result

        new_keys = {k for _, k in iter_inventory_parquet(new_path)}
        old_keys = {k for _, k in iter_inventory_parquet(old_path)}
        assert new_keys == {"dir/item-3.stac.json", "dir/item-4.stac.json"}
        assert old_keys == {"dir/item-1.stac.json", "dir/item-3.stac.json"}

        # --- Phase 3: ingest --diff (index anti-join skips the changed key) -
        ing2 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=wh,
        )
        summary2 = ing2.run(iter_inventory_parquet(new_path))
        # item-3's key is already known → skipped; only item-4 fetched.
        assert summary2["items"] == 1, summary2
        assert summary2["considered"] == 2, summary2
        assert index.known_source_keys() == {
            f"s3://data-bucket/dir/item-{i}.stac.json" for i in (1, 2, 3, 4)
        }
        assert _read_warehouse_ids(store) == {"item-1", "item-2", "item-3", "item-4"}

        # --- Phase 4: GC confirms the disappeared item-1 --------------------
        import csv as _csv

        from earthcatalog.gc import run_garbage_collection

        live_keys = ["dir/item-2.stac.json", "dir/item-3.stac.json", "dir/item-4.stac.json"]
        inv_csv = tmp_path / "day2_inventory.csv"
        with inv_csv.open("w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["bucket", "key"])
            for k in live_keys:
                w.writerow(["data-bucket", k])

        gc_result = run_garbage_collection(
            str(inv_csv),
            store=store,
            index=Index(store, "warehouse_index.parquet"),
            warehouse_prefix="warehouse/",
            head_fn=lambda k: k not in {"s3://data-bucket/dir/item-1.stac.json"},
            layout=layout_of(table.properties),
        )
        assert gc_result["confirmed"] == 1, gc_result
        assert gc_result["orphaned"] == 1, gc_result
        assert _read_warehouse_ids(store) == {"item-2", "item-3", "item-4"}
        active = {r["stac_id"] for r in Index(store, "warehouse_index.parquet").stream_active()}
        assert active == {"item-2", "item-3", "item-4"}

        # --- Phase 5: re-running the same diff is a no-op -------------------
        ing3 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=wh,
        )
        summary3 = ing3.run(iter_inventory_parquet(new_path))
        assert summary3["items"] == 0, summary3
        assert summary3["considered"] == 2, summary3
