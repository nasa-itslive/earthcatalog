"""
Tests for scripts/consolidate.py.

Uses MemoryStore — no real S3 traffic.
"""

import io
import tempfile
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.transform import write_geoparquet as _write_gp


def _make_item(item_id: str, cell: str = "testcell", year: str = "2024") -> dict:
    return {
        "id": item_id,
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        "properties": {
            "datetime": f"{year}-01-15T00:00:00Z",
            "platform": "TEST",
            "updated": "2024-01-01T00:00:00Z",
            "grid_partition": cell,
        },
        "links": [],
        "assets": {},
    }


def _write_part(store, cell, year, part_idx, items):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    _write_gp(items, tmp_path)
    data = Path(tmp_path).read_bytes()
    Path(tmp_path).unlink(missing_ok=True)
    path = f"grid_partition={cell}/year={year}/part_{part_idx:06d}.parquet"
    obstore.put(store, path, data)
    return path


# ---------------------------------------------------------------------------
# _scan_warehouse
# ---------------------------------------------------------------------------


class TestScanWarehouse:
    def test_finds_partitioned_parquets(self):
        from scripts.consolidate import _scan_warehouse

        store = MemoryStore()
        obstore.put(store, "grid_partition=c1/year=2024/part_000000.parquet", b"a")
        obstore.put(store, "grid_partition=c1/year=2024/part_000001.parquet", b"b")
        obstore.put(store, "grid_partition=c2/year=2023/part_000000.parquet", b"c")

        result = _scan_warehouse(store)
        assert len(result) == 2
        assert len(result[("c1", "2024")]) == 2
        assert result[("c1", "2024")][0]["index"] == 0
        assert result[("c1", "2024")][1]["index"] == 1

    def test_ignores_non_parquet(self):
        from scripts.consolidate import _scan_warehouse

        store = MemoryStore()
        obstore.put(store, "grid_partition=c1/year=2024/readme.txt", b"skip")
        obstore.put(store, "grid_partition=c1/year=2024/part_000000.parquet", b"data")

        result = _scan_warehouse(store)
        assert len(result) == 1
        assert len(result[("c1", "2024")]) == 1

    def test_empty_warehouse(self):
        from scripts.consolidate import _scan_warehouse

        store = MemoryStore()
        result = _scan_warehouse(store)
        assert result == {}


# ---------------------------------------------------------------------------
# _dedup_by_id
# ---------------------------------------------------------------------------


class TestDedupTable:
    def test_deduplicates(self):
        from scripts.consolidate import _dedup_table

        tbl = pa.table({
            "id": ["a", "a", "b"],
            "value": [1, 2, 3],
        })
        result = _dedup_table(tbl)
        assert result.num_rows == 2
        ids = result.column("id").to_pylist()
        assert ids == ["a", "b"]

    def test_keeps_all_unique(self):
        from scripts.consolidate import _dedup_table

        tbl = pa.table({"id": [f"item-{i}" for i in range(10)]})
        assert _dedup_table(tbl).num_rows == 10


# ---------------------------------------------------------------------------
# consolidate_partition
# ---------------------------------------------------------------------------


class TestConsolidatePartition:
    @pytest.fixture(autouse=True)
    def _setup(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_merges_all_small_files(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "testcell"
        year = "2024"

        items_0 = [_make_item(f"item-{i}", cell, year) for i in range(5)]
        items_1 = [_make_item(f"item-{i+5}", cell, year) for i in range(3)]
        items_2 = [_make_item(f"item-{i+8}", cell, year) for i in range(2)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)
        _write_part(store, cell, year, 2, items_2)

        files_info = [
            {"path": f"grid_partition={cell}/year={year}/part_{idx:06d}.parquet", "name": f"part_{idx:06d}.parquet", "index": idx, "size": 0}
            for idx in range(3)
        ]

        result = consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=False, compact_rows=10)

        assert result is not None
        assert result["merged_files"] == 3
        assert result["output_rows"] == 10

        consolidated = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()))
        ).read()
        assert consolidated.num_rows == 10

        with pytest.raises(Exception):
            obstore.get(store, f"grid_partition={cell}/year={year}/part_000001.parquet")
        with pytest.raises(Exception):
            obstore.get(store, f"grid_partition={cell}/year={year}/part_000002.parquet")

    def test_keeps_full_files_intact(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "keep"
        year = "2024"

        items_0 = [_make_item(f"big-{i}", cell, year) for i in range(100)]
        items_1 = [_make_item(f"small-{i}", cell, year) for i in range(3)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)

        files_info = [
            {"path": f"grid_partition={cell}/year={year}/part_{i:06d}.parquet", "name": f"part_{i:06d}.parquet", "index": i, "size": 0}
            for i in range(2)
        ]

        consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=False, compact_rows=10)

        big = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()))
        ).read()
        assert big.num_rows == 100

        small = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000001.parquet").bytes()))
        ).read()
        assert small.num_rows == 3

    def test_consolidates_trailing_after_full(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "trailing"
        year = "2024"

        items_0 = [_make_item(f"full-{i}", cell, year) for i in range(100)]
        items_1 = [_make_item(f"mid-{i}", cell, year) for i in range(24)]
        items_2 = [_make_item(f"tiny-{i}", cell, year) for i in range(2)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)
        _write_part(store, cell, year, 2, items_2)

        files_info = [
            {"path": f"grid_partition={cell}/year={year}/part_{i:06d}.parquet", "name": f"part_{i:06d}.parquet", "index": i, "size": 0}
            for i in range(3)
        ]

        result = consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=False, compact_rows=100)

        assert result is not None
        assert result["merged_files"] == 2
        assert result["output_rows"] == 26

        full = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()))
        ).read()
        assert full.num_rows == 100

        merged = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000001.parquet").bytes()))
        ).read()
        assert merged.num_rows == 26

        with pytest.raises(Exception):
            obstore.get(store, f"grid_partition={cell}/year={year}/part_000002.parquet")

    def test_dry_run_makes_no_changes(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "dry"
        year = "2024"

        items_0 = [_make_item(f"item-{i}", cell, year) for i in range(5)]
        items_1 = [_make_item(f"item-{i+5}", cell, year) for i in range(3)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)

        files_info = [
            {"path": f"grid_partition={cell}/year={year}/part_{i:06d}.parquet", "name": f"part_{i:06d}.parquet", "index": i, "size": 0}
            for i in range(2)
        ]

        result = consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=True, compact_rows=10)
        assert result["merged_files"] == 2

        assert len(obstore.list(store, prefix=f"grid_partition={cell}").__next__()) == 2

    def test_below_threshold_skips(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "skip"
        year = "2024"

        items_0 = [_make_item("only", cell, year)]
        _write_part(store, cell, year, 0, items_0)

        files_info = [{"path": f"grid_partition={cell}/year={year}/part_000000.parquet", "name": "part_000000.parquet", "index": 0, "size": 0}]

        result = consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=False)
        assert result is None

    def test_deduplicates_across_files(self):
        from scripts.consolidate import consolidate_partition

        store = MemoryStore()
        cell = "dedup"
        year = "2024"

        items_0 = [_make_item("shared", cell, year)]
        items_1 = [_make_item("shared", cell, year), _make_item("unique", cell, year)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)

        files_info = [
            {"path": f"grid_partition={cell}/year={year}/part_{i:06d}.parquet", "name": f"part_{i:06d}.parquet", "index": i, "size": 0}
            for i in range(2)
        ]

        result = consolidate_partition(store, cell, year, files_info, threshold=2, dry_run=False, compact_rows=10)
        assert result["output_rows"] == 2

        consolidated = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()))
        ).read()
        ids = set(consolidated.column("id").to_pylist())
        assert ids == {"shared", "unique"}


# ---------------------------------------------------------------------------
# run_consolidate integration
# ---------------------------------------------------------------------------


class TestRunConsolidate:
    @pytest.fixture(autouse=True)
    def _setup(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_end_to_end(self, tmp_path):
        from scripts.consolidate import run_consolidate

        store = MemoryStore()
        cell = "e2e"
        year = "2024"

        items_0 = [_make_item(f"item-{i}", cell, year) for i in range(5)]
        items_1 = [_make_item(f"item-{i+5}", cell, year) for i in range(3)]
        items_2 = [_make_item(f"item-{i+8}", cell, year) for i in range(2)]

        _write_part(store, cell, year, 0, items_0)
        _write_part(store, cell, year, 1, items_1)
        _write_part(store, cell, year, 2, items_2)

        from unittest.mock import patch

        catalog_path = str(tmp_path / "catalog.db")
        with (
            patch("scripts.consolidate._get_store", return_value=store),
            patch("scripts.consolidate.rebuild_catalog"),
        ):
            result = run_consolidate(
                warehouse_uri="s3://test-bucket/warehouse",
                catalog_path=catalog_path,
                threshold=2,
                compact_rows=10,
            )

        assert result["partitions_consolidated"] == 1
        assert result["files_merged"] == 3
