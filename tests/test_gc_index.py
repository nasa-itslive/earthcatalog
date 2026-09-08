"""Tests for the unified-Index garbage collection (earthcatalog.gc)."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.gc import run_garbage_collection
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


def _make_warehouse_parquet(item_ids: list[str]) -> bytes:
    tbl = pa.table(
        {
            "id": pa.array(item_ids, type=pa.string()),
            "grid_partition": pa.array(["cellA"] * len(item_ids), type=pa.string()),
            "year": pa.array([2020] * len(item_ids), type=pa.int32()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _put_warehouse_file(store, key: str, item_ids: list[str]) -> None:
    store.put(key, _make_warehouse_parquet(item_ids))


def _read_ids(store, key: str) -> list[str]:
    raw = bytes(store.get(key).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read().column("id").to_pylist()


def _list_keys(store, prefix: str = "") -> list[str]:
    out = []
    for batch in store.list(prefix=prefix):
        for obj in batch:
            out.append(obj["path"])
    return out


class TestRunGarbageCollection:
    def test_e2e_removes_orphan(self, tmp_path):
        """Full cycle with a single unified Index (no separate hash file)."""
        store = MemoryStore()
        index = Index(store, "warehouse_index.parquet")
        index.append(
            [
                {
                    "s3_key": _s3_key("a.stac.json"),
                    "stac_id": "item-A",
                    "grid_partition": "cellA",
                    "year": 2020,
                },
                {
                    "s3_key": _s3_key("gone.stac.json"),
                    "stac_id": "item-B",
                    "grid_partition": "cellA",
                    "year": 2020,
                },
            ]
        )
        _put_warehouse_file(
            store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B"]
        )

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        head_fn = lambda k: k == _s3_key("a.stac.json")  # noqa: E731

        result = run_garbage_collection(
            str(inv),
            store=store,
            index=index,
            warehouse_prefix="",
            head_fn=head_fn,
        )

        assert result["confirmed"] == 1
        assert result["orphaned"] == 1
        assert result["files_rewritten"] == 1
        assert result["rows_removed"] == 1

        # Warehouse file rewritten to a gc_* file containing only item-A.
        gc_keys = [k for k in _list_keys(store, "grid_partition=cellA/year=2020/") if "/gc_" in k]
        assert len(gc_keys) == 1
        assert _read_ids(store, gc_keys[0]) == ["item-A"]
        assert "grid_partition=cellA/year=2020/part_0.parquet" not in _list_keys(store)

        # Index: item-B marked deleted, not present in active rows.
        active = [r["stac_id"] for r in index.stream_active()]
        assert active == ["item-A"]

    def test_no_orphans_is_noop(self, tmp_path):
        store = MemoryStore()
        index = Index(store, "warehouse_index.parquet")
        index.append(
            [
                {
                    "s3_key": _s3_key("a.stac.json"),
                    "stac_id": "item-A",
                    "grid_partition": "cellA",
                    "year": 2020,
                },
            ]
        )
        _put_warehouse_file(store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A"])

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        result = run_garbage_collection(
            str(inv),
            store=store,
            index=index,
            warehouse_prefix="",
            head_fn=lambda k: k == _s3_key("a.stac.json"),  # noqa: E731
        )
        assert result["confirmed"] == 0
        assert result["orphaned"] == 0
        assert result["files_rewritten"] == 0

    def test_dry_run_does_not_rewrite(self, tmp_path):
        store = MemoryStore()
        index = Index(store, "warehouse_index.parquet")
        index.append(
            [
                {
                    "s3_key": _s3_key("gone.stac.json"),
                    "stac_id": "item-B",
                    "grid_partition": "cellA",
                    "year": 2020,
                },
            ]
        )
        _put_warehouse_file(store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-B"])

        inv = _write_inventory_csv(tmp_path / "inv.csv", [])
        result = run_garbage_collection(
            str(inv),
            store=store,
            index=index,
            warehouse_prefix="",
            head_fn=lambda k: False,  # noqa: E731
            dry_run=True,
        )
        assert result["files_rewritten"] == 1  # reports what *would* be rewritten
        # Original file untouched.
        assert "grid_partition=cellA/year=2020/part_0.parquet" in _list_keys(store)
        active = [r["stac_id"] for r in index.stream_active()]
        assert active == ["item-B"]
