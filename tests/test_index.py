"""Tests for the unified warehouse Index (merges hash_index + source_index)."""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.index import Index


def _row(s3_key: str, stac_id: str, cell: str = "cellA", year: int = 2020):
    return {
        "s3_key": s3_key,
        "stac_id": stac_id,
        "grid_partition": cell,
        "year": year,
    }


def _read_table(store, key: str) -> pa.Table:
    raw = io.BytesIO(store.get(key).bytes())
    return pq.ParquetFile(raw).read()


class TestAppend:
    def test_append_creates_part(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        n = idx.append([_row("s3://b/a.stac.json", "a")])
        assert n == 1
        assert idx.known_source_keys() == {"s3://b/a.stac.json"}

    def test_append_extends_existing(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a")])
        n = idx.append([_row("s3://b/b.stac.json", "b")])
        assert n == 1  # rows written by THIS append (parts are O(delta))
        assert idx.known_source_keys() == {
            "s3://b/a.stac.json",
            "s3://b/b.stac.json",
        }

    def test_append_empty_is_noop(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        assert idx.append([]) == 0
        try:
            store.head("index.parquet")
            assert False, "empty append should not create the file"
        except Exception:
            pass


class TestKnownSourceKeys:
    def test_empty_index(self):
        idx = Index(MemoryStore(), "index.parquet")
        assert idx.known_source_keys() == set()

    def test_returns_all_s3_keys(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a"), _row("s3://b/b.stac.json", "b")])
        assert idx.known_source_keys() == {"s3://b/a.stac.json", "s3://b/b.stac.json"}

    def test_contains_source_key(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a")])
        assert idx.contains_source_key("s3://b/a.stac.json")
        assert not idx.contains_source_key("s3://b/zzz.stac.json")


class TestStreamActive:
    def test_streams_non_deleted(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append(
            [
                _row("s3://b/a.stac.json", "a", cell="c1", year=2019),
                _row("s3://b/b.stac.json", "b", cell="c2", year=2020),
            ]
        )
        idx.mark_deleted({"a"})
        rows = list(idx.stream_active())
        assert rows == [
            {"s3_key": "s3://b/b.stac.json", "stac_id": "b", "grid_partition": "c2", "year": 2020}
        ]


class TestMarkDeleted:
    def test_marks_matching_rows(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a"), _row("s3://b/b.stac.json", "b")])
        assert idx.mark_deleted({"a"}) == 1
        active = [r["stac_id"] for r in idx.stream_active()]
        assert active == ["b"]

    def test_noop_when_none_match(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a")])
        assert idx.mark_deleted({"zzz"}) == 0


class TestCompact:
    def test_drops_deleted_rows(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a"), _row("s3://b/b.stac.json", "b")])
        idx.mark_deleted({"a"})
        assert idx.compact() == 1
        tbl = _read_table(store, "index.parquet")
        assert tbl.column("stac_id").to_pylist() == ["b"]
        assert tbl.column("deleted").to_pylist() == [False]


class TestHashSet:
    def test_returns_hash_of_stac_ids(self):
        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a"), _row("s3://b/b.stac.json", "b")])
        hs = idx.hash_set()
        assert len(hs) == 2
        # id_hash must equal xxh3_128(stac_id) for dedup compat
        import xxhash

        assert hs == {
            xxhash.xxh3_128("a", seed=42).digest(),
            xxhash.xxh3_128("b", seed=42).digest(),
        }

    def test_hash_set_excludes_deleted(self):
        """hash_set() must reflect only *active* (non-deleted) rows so a
        re-added item can be re-ingested after GC removes it."""
        import xxhash

        store = MemoryStore()
        idx = Index(store, "index.parquet")
        idx.append([_row("s3://b/a.stac.json", "a"), _row("s3://b/b.stac.json", "b")])
        idx.mark_deleted({"a"})
        hs = idx.hash_set()
        assert hs == {xxhash.xxh3_128("b", seed=42).digest()}
