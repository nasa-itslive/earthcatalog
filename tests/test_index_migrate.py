"""Tests for migrate_indices — folding legacy hash+source index files into one."""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.hash_index import hash_id
from earthcatalog.index import migrate_indices
from earthcatalog.source_index import append_source_index

_HASH_SCHEMA = pa.schema([pa.field("id_hash", pa.binary(16))])


def _put_hash_index(store, key: str, ids: list[str]) -> None:
    tbl = pa.table({"id_hash": pa.array([hash_id(i) for i in ids], type=pa.binary(16))})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    store.put(key, buf.getvalue())


class TestMigrateIndices:
    def test_migrates_both_files(self):
        store = MemoryStore()
        append_source_index(
            [
                ("s3://b/a.stac.json", "item-A", "cellA", 2020),
                ("s3://b/b.stac.json", "item-B", "cellB", 2021),
            ],
            store,
            "src.parquet",
        )
        # Hash index has item-B only (item-A missing -> source wins for content).
        _put_hash_index(store, "hash.parquet", ["item-B"])

        idx = migrate_indices(
            store, hash_key="hash.parquet", source_key="src.parquet", out_key="idx.parquet"
        )

        assert idx is not None
        rows = {r["stac_id"]: r for r in idx.stream_active()}
        assert set(rows) == {"item-A", "item-B"}
        assert rows["item-B"]["grid_partition"] == "cellB"
        assert rows["item-B"]["year"] == 2021

        # id_hash matches the legacy hash for item-B.
        assert idx.hash_set() == {hash_id("item-A"), hash_id("item-B")}

    def test_missing_files_returns_none(self):
        store = MemoryStore()
        assert migrate_indices(store, hash_key="h.parquet", source_key="s.parquet") is None

    def test_source_only(self):
        store = MemoryStore()
        append_source_index(
            [("s3://b/a.stac.json", "item-A", "cellA", 2020)],
            store,
            "src.parquet",
        )
        idx = migrate_indices(
            store, hash_key="nonexistent.parquet", source_key="src.parquet", out_key="idx.parquet"
        )
        assert [r["stac_id"] for r in idx.stream_active()] == ["item-A"]
