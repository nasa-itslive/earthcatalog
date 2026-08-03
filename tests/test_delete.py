"""Tests for earthcatalog.pipelines.delete."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.hash_index import hash_id, read_hashes, write_hashes
from earthcatalog.pipelines.delete import (
    build_inventory_bloom,
    confirm_deletions,
    find_deletion_candidates,
    rewrite_file_without_orphans,
    run_garbage_collection,
)
from earthcatalog.source_index import append_source_index, mark_deleted


def _write_inventory_csv(path: Path, keys: list[str]) -> Path:
    """Write an S3-inventory-style CSV with bucket/key columns."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "key"])
        for k in keys:
            writer.writerow(["data-bucket", k])
    return path


def _s3_key(key: str) -> str:
    return f"s3://data-bucket/{key}"


def _make_warehouse_parquet(item_ids: list[str]) -> bytes:
    """Write a minimal warehouse parquet with id + grid_partition + year."""
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
    keys: list[str] = []
    for batch in store.list(prefix=prefix):
        for obj in batch:
            keys.append(obj["path"])
    return keys


class TestBuildInventoryBloom:
    def test_contains_inventory_keys(self, tmp_path):
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json", "b.stac.json"])
        bloom = build_inventory_bloom(str(inv))
        assert _s3_key("a.stac.json") in bloom
        assert _s3_key("b.stac.json") in bloom

    def test_excludes_non_stac(self, tmp_path):
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json", "b.txt", "c.stac.json"])
        bloom = build_inventory_bloom(str(inv))
        assert _s3_key("b.txt") not in bloom
        assert _s3_key("a.stac.json") in bloom

    def test_no_false_negative_for_missing_key(self, tmp_path):
        """A key never inserted must be reported as absent (no false negatives)."""
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        bloom = build_inventory_bloom(str(inv))
        assert _s3_key("missing.stac.json") not in bloom


class TestFindDeletionCandidates:
    def test_finds_missing_keys(self, tmp_path):
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        bloom = build_inventory_bloom(str(inv))

        store = MemoryStore()
        append_source_index(
            [
                (_s3_key("a.stac.json"), "item-A", "cellA", 2020),
                (_s3_key("gone.stac.json"), "item-B", "cellA", 2020),
            ],
            store,
            "idx.parquet",
        )

        candidates = find_deletion_candidates(store, "idx.parquet", bloom)
        assert [c["stac_id"] for c in candidates] == ["item-B"]

    def test_skips_deleted_rows(self, tmp_path):
        inv = _write_inventory_csv(tmp_path / "inv.csv", [])
        bloom = build_inventory_bloom(str(inv))

        store = MemoryStore()
        append_source_index(
            [
                (_s3_key("gone.stac.json"), "item-B", "cellA", 2020),
                (_s3_key("also-gone.stac.json"), "item-C", "cellA", 2020),
            ],
            store,
            "idx.parquet",
        )
        mark_deleted({"item-B"}, store, "idx.parquet")

        candidates = find_deletion_candidates(store, "idx.parquet", bloom)
        assert [c["stac_id"] for c in candidates] == ["item-C"]

    def test_missing_source_index_yields_nothing(self, tmp_path):
        inv = _write_inventory_csv(tmp_path / "inv.csv", [])
        bloom = build_inventory_bloom(str(inv))
        assert find_deletion_candidates(MemoryStore(), "idx.parquet", bloom) == []


class TestConfirmDeletions:
    def test_confirms_absent(self):
        head_fn = lambda k: k != _s3_key("gone.stac.json")  # noqa: E731
        candidates = [
            {"s3_key": _s3_key("present.stac.json"), "stac_id": "item-A"},
            {"s3_key": _s3_key("gone.stac.json"), "stac_id": "item-B"},
        ]
        confirmed = confirm_deletions(candidates, head_fn=head_fn)
        assert [c["stac_id"] for c in confirmed] == ["item-B"]

    def test_empty_candidates(self):
        assert confirm_deletions([]) == []


class TestRewriteFileWithoutOrphans:
    def test_removes_orphaned_ids(self):
        store = MemoryStore()
        _put_warehouse_file(
            store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B", "item-C"]
        )

        new_key, rows = rewrite_file_without_orphans(
            "grid_partition=cellA/year=2020/part_0.parquet", {"item-B"}, store
        )
        assert rows == 2
        assert _read_ids(store, new_key) == ["item-A", "item-C"]
        # original file untouched
        assert _read_ids(store, "grid_partition=cellA/year=2020/part_0.parquet") == [
            "item-A",
            "item-B",
            "item-C",
        ]

    def test_no_orphans_returns_all_rows(self):
        store = MemoryStore()
        _put_warehouse_file(
            store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B"]
        )
        new_key, rows = rewrite_file_without_orphans(
            "grid_partition=cellA/year=2020/part_0.parquet", set(), store
        )
        assert rows == 2
        assert _read_ids(store, new_key) == ["item-A", "item-B"]


class TestExecuteCleanup:
    def test_no_orphans_noop(self):
        store = MemoryStore()
        from earthcatalog.pipelines.delete import execute_cleanup

        summary = execute_cleanup(
            [],
            store=store,
            hash_index_key="hash.parquet",
            source_index_key="idx.parquet",
        )
        assert summary["orphaned"] == 0
        assert summary["files_rewritten"] == 0


class TestRunGarbageCollection:
    def test_e2e_removes_orphan(self, tmp_path):
        """Full cycle: inventory, source index, warehouse file, hash index."""
        store = MemoryStore()

        # Source index: items A and B came from a.stac.json / gone.stac.json
        append_source_index(
            [
                (_s3_key("a.stac.json"), "item-A", "cellA", 2020),
                (_s3_key("gone.stac.json"), "item-B", "cellA", 2020),
            ],
            store,
            "source.parquet",
        )

        # Warehouse: both items in one file.
        _put_warehouse_file(
            store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B"]
        )

        # Hash index tracks both.
        write_hashes({hash_id("item-A"), hash_id("item-B")}, store, "hash.parquet")

        # Inventory only has a.stac.json -> item-B's source is gone.
        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])

        head_fn = lambda k: k == _s3_key("a.stac.json")  # noqa: E731

        result = run_garbage_collection(
            str(inv),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            warehouse_prefix="",
            head_fn=head_fn,
        )

        assert result["confirmed"] == 1
        assert result["orphaned"] == 1
        assert result["files_rewritten"] == 1
        assert result["rows_removed"] == 1

        # Warehouse now has only item-A (in a gc_* file).
        gc_keys = [k for k in _list_keys(store, "grid_partition=cellA/year=2020/") if "/gc_" in k]
        assert len(gc_keys) == 1
        assert _read_ids(store, gc_keys[0]) == ["item-A"]

        # Old file deleted, hash index updated, source index marked.
        assert "grid_partition=cellA/year=2020/part_0.parquet" not in _list_keys(
            store, "grid_partition=cellA/year=2020/"
        )
        hashes = read_hashes(store, "hash.parquet")
        assert hash_id("item-A") in hashes
        assert hash_id("item-B") not in hashes

    def test_no_orphans_is_noop(self, tmp_path):
        store = MemoryStore()
        append_source_index(
            [(_s3_key("a.stac.json"), "item-A", "cellA", 2020)],
            store,
            "source.parquet",
        )
        _put_warehouse_file(store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A"])
        write_hashes({hash_id("item-A")}, store, "hash.parquet")

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        result = run_garbage_collection(
            str(inv),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            head_fn=lambda k: True,
        )
        assert result["confirmed"] == 0
        assert result["files_rewritten"] == 0
        # hash index unchanged
        assert hash_id("item-A") in read_hashes(store, "hash.parquet")

    def test_dry_run_makes_no_changes(self, tmp_path):
        store = MemoryStore()
        append_source_index(
            [
                (_s3_key("a.stac.json"), "item-A", "cellA", 2020),
                (_s3_key("gone.stac.json"), "item-B", "cellA", 2020),
            ],
            store,
            "source.parquet",
        )
        _put_warehouse_file(
            store, "grid_partition=cellA/year=2020/part_0.parquet", ["item-A", "item-B"]
        )
        write_hashes({hash_id("item-A"), hash_id("item-B")}, store, "hash.parquet")

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        result = run_garbage_collection(
            str(inv),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            head_fn=lambda k: k == _s3_key("a.stac.json"),
            dry_run=True,
        )
        assert result["files_rewritten"] == 1
        # No gc_ files, old file still present, hash index unchanged
        keys = _list_keys(store, "grid_partition=cellA/year=2020/")
        assert all("/gc_" not in k for k in keys)
        assert "grid_partition=cellA/year=2020/part_0.parquet" in keys
        assert hash_id("item-B") in read_hashes(store, "hash.parquet")
