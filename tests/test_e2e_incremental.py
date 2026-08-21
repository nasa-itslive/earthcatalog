"""End-to-end incremental ingest test: new + removed items.

Simulates the daily workflow end to end on MemoryStore + a real local
Iceberg SqlCatalog:

1. produce-delta: scan a synthetic S3 inventory manifest against the unified
   index → a delta Parquet of NEW source keys (daily_delta.run_daily_delta).
2. ingest: feed the delta through the resumable Ingester → Iceberg
   ``add_files`` + index append.
3. gc: a later inventory manifest drops some items → garbage collection
   rewrites the GeoParquet and marks the index rows deleted.

Asserts that new items land in the catalog and removed items are gone.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.index import Index


def _hash_id(item_id: str) -> bytes:
    import xxhash

    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=42).digest()


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


def _make_inventory_parquet(keys: list[str]) -> bytes:
    tbl = pa.table(
        {
            "bucket": ["data-bucket"] * len(keys),
            "key": keys,
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _make_manifest(data_keys: list[str]) -> bytes:
    manifest = {
        "sourceBucket": "its-live-data",
        "destinationBucket": "arn:aws:s3:::log-bucket",
        "fileFormat": "Parquet",
        "files": [{"key": k, "MD5checksum": "abc"} for k in data_keys],
    }
    return json.dumps(manifest).encode()


def _put_inventory(store, keys: list[str]) -> str:
    store.put("data/inv.parquet", _make_inventory_parquet(keys))
    store.put("manifest.json", _make_manifest(["data/inv.parquet"]))
    return "s3://log-bucket/manifest.json"


def _read_warehouse_ids(store, index: Index) -> set[str]:
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
    for batch in store.list(prefix=""):
        for obj in batch:
            k: str = obj["path"]
            if "index.parquet" in k:
                return k
    raise AssertionError("no index file found")


class TestEndToEndIncremental:
    def test_new_then_removed_items(self, tmp_path):
        # --- shared catalog + store ----------------------------------------
        # Use a LocalStore rooted at tmp_path so the catalog's local warehouse
        # path, the Ingester's GeoParquet writes, and Iceberg add_files all
        # resolve to the same filesystem (same pattern as test_gc_bugs).
        from obstore.store import LocalStore

        store = LocalStore(str(tmp_path))
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        Path(wh).mkdir(parents=True, exist_ok=True)
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
        table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))

        from earthcatalog.ingest import Ingester

        # --- Phase 1: produce-delta against an empty index -------------------
        keys = ["dir/item-1.stac.json", "dir/item-2.stac.json", "dir/item-3.stac.json"]
        manifest_uri = _put_inventory(store, keys)
        index = Index(store, "warehouse_index.parquet")

        from unittest.mock import patch

        from scripts.daily_delta import run_daily_delta

        delta_store = MemoryStore()

        with patch(
            "scripts.daily_delta._get_store",
            side_effect=lambda bucket, **kw: store if bucket == "log-bucket" else delta_store,
        ):
            result = run_daily_delta(
                manifest_uri=manifest_uri,
                warehouse_hash_uri="s3://log-bucket/warehouse_index.parquet",
                delta_prefix="s3://delta-bucket/delta",
                date_str="2026-04-28",
            )

        assert result["new_items"] == 3, result

        # --- Phase 2: ingest the delta through the Ingester ------------------
        def _fetch(bucket, key):
            # Only serve keys present in the inventory (delta = the inventory here).
            return _make_item(key.rsplit("/", 1)[-1].removesuffix(".stac.json"), key)

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=wh,
            stage="direct",
        )
        summary = ing.run(("data-bucket", k) for k in keys)
        assert summary["items"] == 3

        # Index now knows all three source keys.
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}
        assert _read_warehouse_ids(store, index) == {
            "item-1",
            "item-2",
            "item-3",
        }

        # --- Phase 3: GC removes item-1 (dropped from a later inventory) -----
        import csv as _csv

        from earthcatalog.gc import run_garbage_collection

        later_keys = ["dir/item-2.stac.json", "dir/item-3.stac.json"]
        # Write a local CSV inventory (no S3) so _iter_inventory reads it
        # directly; the catalog-side manifest is irrelevant to GC.
        inv_csv = tmp_path / "later_inventory.csv"
        with inv_csv.open("w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["bucket", "key"])
            for k in later_keys:
                w.writerow(["data-bucket", k])

        index_key = _find_index_key(store)

        gc_result = run_garbage_collection(
            str(inv_csv),
            store=store,
            index=Index(store, index_key),
            warehouse_prefix="warehouse/",
            head_fn=lambda k: k not in {f"s3://data-bucket/{x}" for x in ("dir/item-1.stac.json",)},
        )
        assert gc_result["confirmed"] == 1, gc_result
        assert gc_result["orphaned"] == 1, gc_result

        # item-1 gone from the warehouse; index still tracks it but marked deleted.
        assert _read_warehouse_ids(store, index) == {"item-2", "item-3"}
        active = {r["stac_id"] for r in Index(store, index_key).stream_active()}
        assert active == {"item-2", "item-3"}

        # A fresh delta against the same later inventory finds nothing new.
        later_manifest = _put_inventory(store, later_keys)  # overwrite manifest + data
        with patch(
            "scripts.daily_delta._get_store",
            side_effect=lambda bucket, **kw: store if bucket == "log-bucket" else delta_store,
        ):
            result2 = run_daily_delta(
                manifest_uri=later_manifest,
                warehouse_hash_uri="s3://log-bucket/warehouse_index.parquet",
                delta_prefix="s3://delta-bucket/delta2",
                date_str="2026-04-29",
            )
        assert result2["new_items"] == 0, result2
