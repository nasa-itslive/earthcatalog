"""Tests for earthcatalog.source_index."""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.source_index import (
    append_source_index,
    compact_source_index,
    mark_deleted,
    stream_active,
)


def _rows(n: int = 3) -> list[tuple[str, str, str, int]]:
    return [(f"s3://bucket/a{i}.stac.json", f"item-{i}", f"cell{i}", 2020 + i) for i in range(n)]


def _read_index(store, key: str) -> pa.Table:
    raw = io.BytesIO(store.get(key).bytes())
    return pq.ParquetFile(raw).read()


class TestAppendSourceIndex:
    def test_append_creates_file(self):
        store = MemoryStore()
        total = append_source_index(_rows(2), store, "idx.parquet")
        assert total == 2
        tbl = _read_index(store, "idx.parquet")
        assert tbl.column("stac_id").to_pylist() == ["item-0", "item-1"]
        assert tbl.column("deleted").to_pylist() == [False, False]

    def test_append_extends_existing(self):
        store = MemoryStore()
        append_source_index(_rows(2), store, "idx.parquet")
        total = append_source_index(_rows(1), store, "idx.parquet")
        assert total == 3
        tbl = _read_index(store, "idx.parquet")
        assert tbl.column("stac_id").to_pylist() == ["item-0", "item-1", "item-0"]
        assert len(tbl.column("stac_id").to_pylist()) == 3

    def test_append_empty_returns_current(self):
        store = MemoryStore()
        append_source_index(_rows(2), store, "idx.parquet")
        total = append_source_index([], store, "idx.parquet")
        assert total == 2

    def test_schema_columns(self):
        store = MemoryStore()
        append_source_index(_rows(1), store, "idx.parquet")
        tbl = _read_index(store, "idx.parquet")
        assert tbl.schema.names == [
            "s3_key",
            "stac_id",
            "grid_partition",
            "year",
            "ingested_at",
            "deleted",
        ]
        assert tbl.schema.field("year").type == pa.int32()
        assert tbl.schema.field("deleted").type == pa.bool_()


class TestMarkDeleted:
    def test_marks_matching_rows(self):
        store = MemoryStore()
        append_source_index(_rows(3), store, "idx.parquet")
        n = mark_deleted({"item-1"}, store, "idx.parquet")
        assert n == 1
        tbl = _read_index(store, "idx.parquet")
        assert tbl.column("deleted").to_pylist() == [False, True, False]

    def test_no_match_returns_zero(self):
        store = MemoryStore()
        append_source_index(_rows(3), store, "idx.parquet")
        n = mark_deleted({"nonexistent"}, store, "idx.parquet")
        assert n == 0

    def test_missing_file_returns_zero(self):
        store = MemoryStore()
        n = mark_deleted({"item-1"}, store, "idx.parquet")
        assert n == 0

    def test_empty_set_is_noop(self):
        store = MemoryStore()
        append_source_index(_rows(2), store, "idx.parquet")
        n = mark_deleted(set(), store, "idx.parquet")
        assert n == 0


class TestCompactSourceIndex:
    def test_removes_deleted_rows(self):
        store = MemoryStore()
        append_source_index(_rows(3), store, "idx.parquet")
        mark_deleted({"item-1"}, store, "idx.parquet")
        kept = compact_source_index(store, "idx.parquet")
        assert kept == 2
        tbl = _read_index(store, "idx.parquet")
        assert tbl.column("stac_id").to_pylist() == ["item-0", "item-2"]

    def test_missing_file_returns_zero(self):
        store = MemoryStore()
        assert compact_source_index(store, "idx.parquet") == 0


class TestStreamActive:
    def test_yields_non_deleted_rows(self):
        store = MemoryStore()
        append_source_index(_rows(3), store, "idx.parquet")
        mark_deleted({"item-1"}, store, "idx.parquet")
        rows = list(stream_active(store, "idx.parquet"))
        assert len(rows) == 2
        assert all(r["stac_id"] != "item-1" for r in rows)

    def test_skips_empty_s3_key(self):
        store = MemoryStore()
        # Rows with empty s3_key (legacy items) are skipped.
        append_source_index(
            [("", "legacy", "cell0", 2020), ("s3://bucket/a.stac.json", "item-x", "cell1", 2021)],
            store,
            "idx.parquet",
        )
        rows = list(stream_active(store, "idx.parquet"))
        assert len(rows) == 1
        assert rows[0]["stac_id"] == "item-x"

    def test_missing_file_yields_nothing(self):
        store = MemoryStore()
        assert list(stream_active(store, "idx.parquet")) == []

    def test_includes_provenance_fields(self):
        store = MemoryStore()
        append_source_index([("s3://b/k.stac.json", "id1", "cell7", 2024)], store, "idx.parquet")
        (row,) = stream_active(store, "idx.parquet")
        assert row == {
            "s3_key": "s3://b/k.stac.json",
            "stac_id": "id1",
            "grid_partition": "cell7",
            "year": 2024,
        }
