"""
Tests for earthcatalog.pipelines.backfill.

Grouped by component
--------------------
TestIngestChunkImpl
    Unit tests for ``_ingest_chunk_impl()``.
    All I/O is fully mocked: items come from an in-process dict, output is
    written to a ``MemoryStore``.  No Dask scheduler is involved.

TestCompactGroupImpl
    Unit tests for ``_compact_group_impl()``.
    Part files are pre-written to a ``MemoryStore``; compaction reads, merges,
    and writes back into the same store.  No Dask scheduler is involved.

TestRunBackfillUnit
    Integration tests for ``run_backfill()`` using the *synchronous* Dask
    scheduler, a ``LocalStore`` warehouse, and a fully mocked item fetcher.
    Exercises the full three-level pipeline (Level 0 → Level 1 → Level 2)
    without any real S3 traffic.

TestRunBackfillDask
    End-to-end integration tests that fetch **real** STAC items from the public
    ITS_LIVE S3 bucket (no credentials required).  The warehouse is written to
    a ``LocalStore`` backed by ``tmp_path``.  The Dask *synchronous* scheduler
    is used for deterministic, single-process execution.

    Marked ``pytest.mark.integration`` — skip with ``-m "not integration"``
    when offline.
"""

import io
import json
import uuid
from datetime import UTC
from pathlib import Path
from unittest.mock import patch

import dask
import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import LocalStore, MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.transform import (
    FileMetadata,
    fan_out,
    group_by_partition,
    write_geoparquet_s3,
)
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.pipelines.backfill import (
    _compact_group_impl,
    _ingest_chunk_impl,
    _write_ingest_stats,
    compact_group,
    ingest_chunk,
    run_backfill,
)

# ---------------------------------------------------------------------------
# Shared STAC item fixtures
# ---------------------------------------------------------------------------

_STAC_ITEMS = [
    {
        "id": f"backfill-item-{i:04d}",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-10 + i, 60], [10 + i, 60], [10 + i, 70],
                [-10 + i, 70], [-10 + i, 60],
            ]],
        },
        "properties": {
            "datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-15T00:00:00Z",
            "platform": "NISAR",
            "percent_valid_pixels": float(80 + i),
            "date_dt": float(12 + i),
            "proj:code": "EPSG:32632",
            "sat:orbit_state": "ascending",
            "scene_1_id": f"scene1_{i}",
            "scene_2_id": f"scene2_{i}",
            "scene_1_frame": f"frame1_{i}",
            "scene_2_frame": f"frame2_{i}",
            "version": "002",
            "start_datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-10T00:00:00Z",
            "end_datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-20T00:00:00Z",
        },
        "links": [],
        "assets": {},
    }
    for i in range(8)
]

# Map (bucket, key) → item dict — used by the mock fetcher.
_ITEM_MAP: dict[tuple[str, str], dict] = {
    ("mock-bucket", f"stac/item-{i:04d}.stac.json"): item
    for i, item in enumerate(_STAC_ITEMS)
}


def _mock_fetcher(bucket: str, key: str) -> dict | None:
    return _ITEM_MAP.get((bucket, key))


_ALL_PAIRS = list(_ITEM_MAP.keys())

_PARTITIONER = H3Partitioner(resolution=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_from_store(store, key: str) -> pa.Table:
    """Read a Parquet file stored in an obstore store into an Arrow table."""
    raw = bytes(obstore.get(store, key).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read()


def _write_parquet_to_store(store, key: str, items: list[dict]) -> FileMetadata:
    """Write fan-out items to store and return FileMetadata."""
    n_rows, n_bytes = write_geoparquet_s3(items, store, key)
    cell = items[0]["properties"]["grid_partition"]
    dt = items[0]["properties"].get("datetime", "")
    year = int(dt[:4]) if dt else None
    return FileMetadata(
        s3_key=key,
        grid_partition=cell,
        year=year,
        row_count=n_rows,
        file_size_bytes=n_bytes,
    )


# ---------------------------------------------------------------------------
# TestIngestChunkImpl
# ---------------------------------------------------------------------------

class TestIngestChunkImpl:
    """Unit tests for _ingest_chunk_impl() — no Dask, MemoryStore warehouse."""

    def test_returns_file_metadata_list(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        assert isinstance(metas, list)
        assert len(metas) > 0
        assert all(isinstance(m, FileMetadata) for m in metas)

    def test_row_count_at_least_one_per_item(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        total_rows = sum(m.row_count for m in metas)
        assert total_rows >= len(_STAC_ITEMS)

    def test_each_file_has_single_grid_partition(self):
        """Every file must contain rows for exactly one grid_partition value."""
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        for fm in metas:
            tbl = _read_from_store(store, fm.s3_key)
            unique_cells = set(tbl.column("grid_partition").to_pylist())
            assert unique_cells == {fm.grid_partition}, (
                f"file {fm.s3_key} has multiple grid_partition values: {unique_cells}"
            )

    def test_file_size_matches_actual_bytes(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        for fm in metas:
            raw = bytes(obstore.get(store, fm.s3_key).bytes())
            assert len(raw) == fm.file_size_bytes

    def test_hive_key_format(self):
        """Keys must follow grid_partition=<cell>/year=<year>/part_...parquet."""
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=5,
            item_fetcher=_mock_fetcher,
        )
        for fm in metas:
            assert f"grid_partition={fm.grid_partition}" in fm.s3_key
            year_str = str(fm.year) if fm.year is not None else "unknown"
            assert f"year={year_str}" in fm.s3_key
            assert "part_000005_" in fm.s3_key
            assert fm.s3_key.endswith(".parquet")

    def test_chunk_id_in_filename(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=42,
            item_fetcher=_mock_fetcher,
        )
        assert all("part_000042_" in fm.s3_key for fm in metas)

    def test_empty_fetch_returns_empty_list(self):
        """If all fetches return None, no files should be written."""
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=lambda b, k: None,
        )
        assert metas == []

    def test_geo_metadata_in_written_files(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        for fm in metas:
            raw = bytes(obstore.get(store, fm.s3_key).bytes())
            pf = pq.ParquetFile(io.BytesIO(raw))
            assert b"geo" in pf.metadata.metadata

    def test_int_fields_are_int32(self):
        store = MemoryStore()
        metas, _ = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        for fm in metas:
            tbl = _read_from_store(store, fm.s3_key)
            assert tbl.schema.field("percent_valid_pixels").type == pa.int32()
            assert tbl.schema.field("date_dt").type == pa.int32()

    def test_dask_delayed_version_matches_impl(self):
        """ingest_chunk(...).compute() must return the same result as _impl()."""
        store = MemoryStore()
        with dask.config.set(scheduler="synchronous"):
            metas_delayed, counter_delayed = ingest_chunk(
                _ALL_PAIRS, _PARTITIONER, store, 0, _mock_fetcher,
            ).compute()
        metas_direct, counter_direct = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, MemoryStore(), 0, _mock_fetcher,
        )
        # Same number of files and same total row count (stores are independent).
        assert len(metas_delayed) == len(metas_direct)
        assert (sum(m.row_count for m in metas_delayed)
                == sum(m.row_count for m in metas_direct))
        # Both counters must cover the same total item count.
        assert sum(counter_delayed.values()) == sum(counter_direct.values())


# ---------------------------------------------------------------------------
# TestCompactGroupImpl
# ---------------------------------------------------------------------------

class TestCompactGroupImpl:
    """Unit tests for _compact_group_impl() — no Dask, MemoryStore."""

    def _make_part_files(
        self,
        store: MemoryStore,
        n_parts: int = 3,
    ) -> list[FileMetadata]:
        """Write n_parts part files for the same (cell, year) bucket."""
        # Use a single item to guarantee all parts land in the same cell.
        item = _STAC_ITEMS[0]
        fo = fan_out([item], _PARTITIONER)
        groups = group_by_partition(fo)
        (cell, year), group_items = next(iter(groups.items()))
        year_str = str(year) if year is not None else "unknown"

        metas = []
        for i in range(n_parts):
            key = (
                f"grid_partition={cell}/year={year_str}/"
                f"part_{i:06d}_{uuid.uuid4().hex[:8]}.parquet"
            )
            n_rows, n_bytes = write_geoparquet_s3(group_items, store, key)
            metas.append(FileMetadata(
                s3_key=key,
                grid_partition=cell,
                year=year,
                row_count=n_rows,
                file_size_bytes=n_bytes,
            ))
        return metas

    def test_single_file_passthrough(self):
        """A single-file bucket must be returned unchanged (no I/O)."""
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=1)
        result = _compact_group_impl(metas, "ignored_key.parquet", store)
        assert result is metas[0]

    def test_merges_multiple_files(self):
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=3)
        # After dedup, row count may be <= sum of parts (identical items deduped).
        out_key = f"grid_partition={metas[0].grid_partition}/year={metas[0].year}/compacted.parquet"
        result = _compact_group_impl(metas, out_key, store)
        tbl = _read_from_store(store, out_key)
        assert tbl.num_rows == result.row_count
        assert tbl.num_rows > 0
        assert tbl.num_rows <= sum(m.row_count for m in metas)

    def test_returns_correct_metadata(self):
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=2)
        out_key = (
            f"grid_partition={metas[0].grid_partition}/"
            f"year={metas[0].year}/compacted.parquet"
        )
        result = _compact_group_impl(metas, out_key, store)
        assert result.s3_key == out_key
        assert result.grid_partition == metas[0].grid_partition
        assert result.year == metas[0].year
        # row_count may be less than sum due to dedup; must be > 0 and consistent.
        assert 0 < result.row_count <= sum(m.row_count for m in metas)
        tbl = _read_from_store(store, out_key)
        assert tbl.num_rows == result.row_count
        assert result.file_size_bytes > 0

    def test_deletes_input_part_files(self):
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=3)
        out_key = (
            f"grid_partition={metas[0].grid_partition}/"
            f"year={metas[0].year}/compacted.parquet"
        )
        _compact_group_impl(metas, out_key, store)
        for fm in metas:
            with pytest.raises(Exception):
                obstore.get(store, fm.s3_key)

    def test_compacted_file_has_geo_metadata(self):
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=2)
        out_key = (
            f"grid_partition={metas[0].grid_partition}/"
            f"year={metas[0].year}/compacted.parquet"
        )
        _compact_group_impl(metas, out_key, store)
        raw = bytes(obstore.get(store, out_key).bytes())
        pf = pq.ParquetFile(io.BytesIO(raw))
        assert b"geo" in pf.metadata.metadata

    def test_compacted_file_sorted_by_platform_datetime(self):
        """Rows in the compacted file must be sorted by (platform, datetime)."""
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=2)
        out_key = (
            f"grid_partition={metas[0].grid_partition}/"
            f"year={metas[0].year}/compacted.parquet"
        )
        _compact_group_impl(metas, out_key, store)
        tbl = _read_from_store(store, out_key)
        platforms = tbl.column("platform").to_pylist()
        datetimes = tbl.column("datetime").to_pylist()
        keys = list(zip(
            [p or "" for p in platforms],
            [str(d) if d is not None else "" for d in datetimes],
        ))
        assert keys == sorted(keys)

    def test_dask_delayed_version_matches_impl(self):
        store = MemoryStore()
        metas = self._make_part_files(store, n_parts=2)
        out_key = (
            f"grid_partition={metas[0].grid_partition}/"
            f"year={metas[0].year}/compacted_delayed.parquet"
        )
        with dask.config.set(scheduler="synchronous"):
            result = compact_group(metas, out_key, store).compute()
        # Row count after dedup must be > 0 and ≤ sum of inputs.
        assert 0 < result.row_count <= sum(m.row_count for m in metas)


# ---------------------------------------------------------------------------
# TestRunBackfillUnit
# ---------------------------------------------------------------------------

class TestRunBackfillUnit:
    """
    Integration tests for run_backfill() with the synchronous Dask scheduler.

    Warehouse is a ``LocalStore``; item fetching uses the mock dict fetcher;
    no real S3 traffic.
    """

    @pytest.fixture(autouse=True)
    def _use_memory_store_config(self, tmp_path):
        """Point store_config at a MemoryStore so catalog up/download are in-proc."""
        store = MemoryStore()
        store_config.set_store(store)
        store_config.set_catalog_key("catalog.db")
        yield store

    @pytest.fixture()
    def inventory_csv(self, tmp_path) -> Path:
        import csv
        inv = tmp_path / "inventory.csv"
        with open(inv, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["bucket", "key"])
            writer.writerows(_ALL_PAIRS)
        return inv

    @pytest.fixture()
    def warehouse_dirs(self, tmp_path):
        wh = tmp_path / "warehouse"
        wh.mkdir()
        return LocalStore(str(wh)), str(wh)

    def _run(self, inventory_csv, warehouse_dirs, **kwargs):
        wh_store, wh_root = warehouse_dirs
        catalog_path = str(Path(wh_root).parent / "catalog.db")
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inventory_csv),
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                partitioner=_PARTITIONER,
                chunk_size=4,
                max_workers_per_task=2,
                use_lock=False,
                **kwargs,
            )
        from earthcatalog.core.catalog import get_or_create_table, open_catalog
        catalog = open_catalog(db_path=catalog_path, warehouse_path=wh_root)
        return get_or_create_table(catalog)

    def test_rows_written_to_iceberg(self, inventory_csv, warehouse_dirs):
        table = self._run(inventory_csv, warehouse_dirs)
        result = table.scan().to_arrow()
        assert result.num_rows >= len(_STAC_ITEMS)

    def test_single_snapshot(self, inventory_csv, warehouse_dirs):
        """run_backfill must produce exactly one Iceberg snapshot."""
        table = self._run(inventory_csv, warehouse_dirs)
        assert len(table.history()) == 1

    def test_all_item_ids_present(self, inventory_csv, warehouse_dirs):
        table = self._run(inventory_csv, warehouse_dirs)
        found = set(table.scan().to_arrow().column("id").to_pylist())
        for item in _STAC_ITEMS:
            assert item["id"] in found

    def test_compact_reduces_file_count(self, inventory_csv, warehouse_dirs):
        """With compact=True, buckets with >1 part file are merged."""
        wh_store, wh_root = warehouse_dirs

        # Run without compaction — fresh catalog store.
        store_config.set_store(MemoryStore())
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inventory_csv),
                catalog_path=str(Path(wh_root).parent / "catalog_no.db"),
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                partitioner=_PARTITIONER,
                chunk_size=4,
                max_workers_per_task=2,
                compact=False,
                use_lock=False,
            )
        no_compact_files = len(list(Path(wh_root).glob("**/*.parquet")))

        # Fresh warehouse + fresh catalog store for compacted run.
        wh2 = Path(wh_root).parent / "wh2"
        wh2.mkdir()
        wh_store2 = LocalStore(str(wh2))
        store_config.set_store(MemoryStore())
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inventory_csv),
                catalog_path=str(Path(wh_root).parent / "catalog_yes.db"),
                warehouse_store=wh_store2,
                warehouse_root=str(wh2),
                item_fetcher=_mock_fetcher,
                partitioner=_PARTITIONER,
                chunk_size=4,
                max_workers_per_task=2,
                compact=True,
                use_lock=False,
            )
        compact_files = len(list(wh2.glob("**/*.parquet")))

        # Row counts must be equal.
        from earthcatalog.core.catalog import get_or_create_table, open_catalog
        t1 = get_or_create_table(open_catalog(
            str(Path(wh_root).parent / "catalog_no.db"), wh_root,
        ))
        t2 = get_or_create_table(open_catalog(
            str(Path(wh_root).parent / "catalog_yes.db"), str(wh2),
        ))
        assert t1.scan().to_arrow().num_rows == t2.scan().to_arrow().num_rows

        # Compaction should not increase file count.
        assert compact_files <= no_compact_files

    def test_compact_false_skips_level1(self, inventory_csv, warehouse_dirs):
        """With compact=False, all output files are the raw part files."""
        table = self._run(inventory_csv, warehouse_dirs, compact=False)
        result = table.scan().to_arrow()
        assert result.num_rows >= len(_STAC_ITEMS)

    def test_hive_layout_in_warehouse(self, inventory_csv, warehouse_dirs):
        """All Parquet files must sit under grid_partition=.../year=... dirs."""
        _, wh_root = warehouse_dirs
        self._run(inventory_csv, warehouse_dirs)
        for f in Path(wh_root).glob("**/*.parquet"):
            parts = f.parts
            assert any(p.startswith("grid_partition=") for p in parts)
            assert any(p.startswith("year=") for p in parts)


# ---------------------------------------------------------------------------
# TestRunBackfillDask  (integration — requires real S3 read access)
# ---------------------------------------------------------------------------

INTEGRATION_INVENTORY = "/tmp/test_inventory_200.csv"


@pytest.mark.integration
class TestRunBackfillDask:
    """
    End-to-end tests that fetch real STAC items from the public ITS_LIVE S3
    bucket and write GeoParquet to a local warehouse.

    Requires the 200-item inventory at ``/tmp/test_inventory_200.csv``
    (generated by ``make_test_inventory.py`` in the smoke-test phase).

    The Dask *synchronous* scheduler is used, so the full pipeline runs in a
    single process and stack traces are readable.
    """

    @pytest.fixture(autouse=True)
    def _use_memory_store_config(self, tmp_path):
        """Point store_config at a MemoryStore so catalog up/download are in-proc."""
        store = MemoryStore()
        store_config.set_store(store)
        store_config.set_catalog_key("catalog.db")
        yield store

    @pytest.fixture()
    def warehouse_dirs(self, tmp_path):
        wh = tmp_path / "warehouse"
        wh.mkdir()
        return LocalStore(str(wh)), str(wh)

    def _run(self, warehouse_dirs, **kwargs):
        wh_store, wh_root = warehouse_dirs
        catalog_path = str(Path(wh_root).parent / "catalog.db")
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=INTEGRATION_INVENTORY,
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=None,   # real S3 fetcher
                h3_resolution=1,
                chunk_size=50,
                max_workers_per_task=8,
                use_lock=False,
                **kwargs,
            )
        from earthcatalog.core.catalog import get_or_create_table, open_catalog
        catalog = open_catalog(db_path=catalog_path, warehouse_path=wh_root)
        return get_or_create_table(catalog)

    def test_rows_written_from_real_s3(self, warehouse_dirs):
        """200 real STAC items must produce ≥ 200 rows (H3 fan-out)."""
        if not Path(INTEGRATION_INVENTORY).exists():
            pytest.skip(f"inventory not found: {INTEGRATION_INVENTORY}")
        table = self._run(warehouse_dirs)
        result = table.scan().to_arrow()
        assert result.num_rows >= 200

    def test_single_snapshot_real_data(self, warehouse_dirs):
        if not Path(INTEGRATION_INVENTORY).exists():
            pytest.skip(f"inventory not found: {INTEGRATION_INVENTORY}")
        table = self._run(warehouse_dirs)
        assert len(table.history()) == 1

    def test_hive_layout_real_data(self, warehouse_dirs):
        if not Path(INTEGRATION_INVENTORY).exists():
            pytest.skip(f"inventory not found: {INTEGRATION_INVENTORY}")
        _, wh_root = warehouse_dirs
        self._run(warehouse_dirs)
        parquet_files = list(Path(wh_root).glob("**/*.parquet"))
        assert len(parquet_files) > 0
        for f in parquet_files:
            parts = f.parts
            assert any(p.startswith("grid_partition=") for p in parts)
            assert any(p.startswith("year=") for p in parts)

    def test_compact_produces_fewer_files_than_no_compact(self, tmp_path):
        if not Path(INTEGRATION_INVENTORY).exists():
            pytest.skip(f"inventory not found: {INTEGRATION_INVENTORY}")

        # No compaction
        wh_no = tmp_path / "wh_no"
        wh_no.mkdir()
        store_config.set_store(MemoryStore())
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=INTEGRATION_INVENTORY,
                catalog_path=str(tmp_path / "cat_no.db"),
                warehouse_store=LocalStore(str(wh_no)),
                warehouse_root=str(wh_no),
                h3_resolution=1,
                chunk_size=50,
                compact=False,
                use_lock=False,
            )
        n_no = len(list(wh_no.glob("**/*.parquet")))

        # With compaction
        wh_yes = tmp_path / "wh_yes"
        wh_yes.mkdir()
        store_config.set_store(MemoryStore())
        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=INTEGRATION_INVENTORY,
                catalog_path=str(tmp_path / "cat_yes.db"),
                warehouse_store=LocalStore(str(wh_yes)),
                warehouse_root=str(wh_yes),
                h3_resolution=1,
                chunk_size=50,
                compact=True,
                compact_threshold=2,
                use_lock=False,
            )
        n_yes = len(list(wh_yes.glob("**/*.parquet")))

        assert n_yes <= n_no, (
            f"compaction increased file count: {n_yes} > {n_no}"
        )


# ---------------------------------------------------------------------------
# TestCompactGroupImplDedup  — deduplication and orphan cleanup
# ---------------------------------------------------------------------------

class TestCompactGroupImplDedup:
    """
    Unit tests for the deduplication and orphan-file cleanup added to
    ``_compact_group_impl()``.  All I/O uses MemoryStore — no Dask, no S3.
    """

    def _cell_year(self):
        """Return a stable (cell, year) for fixture construction."""
        item = _STAC_ITEMS[0]
        fo = fan_out([item], _PARTITIONER)
        groups = group_by_partition(fo)
        return next(iter(groups.keys()))

    def _write_item_to_store(
        self, store: MemoryStore, key: str, item_overrides: dict
    ) -> FileMetadata:
        """Write a single STAC item (with property overrides) to store."""
        import copy
        item = copy.deepcopy(_STAC_ITEMS[0])
        item["properties"].update(item_overrides)
        if "id" in item_overrides:
            item["id"] = item_overrides["id"]
        fo = fan_out([item], _PARTITIONER)
        groups = group_by_partition(fo)
        (cell, year), group_items = next(iter(groups.items()))
        n_rows, n_bytes = write_geoparquet_s3(group_items, store, key)
        return FileMetadata(
            s3_key=key,
            grid_partition=cell,
            year=year,
            row_count=n_rows,
            file_size_bytes=n_bytes,
        )

    def test_dedup_removes_duplicate_ids(self):
        """Two part files with the same item id → compaction yields one row per id."""
        store = MemoryStore()
        cell, year = self._cell_year()
        year_str = str(year) if year is not None else "unknown"
        prefix = f"grid_partition={cell}/year={year_str}"

        # Write the same item twice with different `updated` timestamps.
        fm1 = self._write_item_to_store(
            store, f"{prefix}/part_000000_aaa.parquet",
            {"id": "dup-item", "updated": "2026-01-01T00:00:00Z"},
        )
        fm2 = self._write_item_to_store(
            store, f"{prefix}/part_000001_bbb.parquet",
            {"id": "dup-item", "updated": "2026-04-01T00:00:00Z"},
        )
        out_key = f"{prefix}/compacted.parquet"
        _compact_group_impl([fm1, fm2], out_key, store)

        tbl = _read_from_store(store, out_key)
        ids = tbl.column("id").to_pylist()
        # All ids that represent the same source item collapse after H3 fan-out
        # — each unique (id, grid_partition) pair must appear exactly once.
        assert len(ids) == len(set(ids)), f"duplicate ids after compaction: {ids}"

    def test_dedup_keeps_most_recent_updated(self):
        """When two rows share an id, the one with the later `updated` is kept."""
        store = MemoryStore()
        cell, year = self._cell_year()
        year_str = str(year) if year is not None else "unknown"
        prefix = f"grid_partition={cell}/year={year_str}"

        fm_old = self._write_item_to_store(
            store, f"{prefix}/part_old.parquet",
            {"id": "versioned-item", "updated": "2026-01-01T00:00:00Z",
             "version": "001"},
        )
        fm_new = self._write_item_to_store(
            store, f"{prefix}/part_new.parquet",
            {"id": "versioned-item", "updated": "2026-04-01T00:00:00Z",
             "version": "002"},
        )
        out_key = f"{prefix}/compacted.parquet"
        _compact_group_impl([fm_old, fm_new], out_key, store)

        tbl = _read_from_store(store, out_key)
        # Find the row(s) for "versioned-item" (may be fan-out duplicates with
        # the same id; all should carry the newer version).
        ids = tbl.column("id").to_pylist()
        versions = tbl.column("version").to_pylist()
        for i, id_val in enumerate(ids):
            if id_val == "versioned-item":
                assert versions[i] == "002", (
                    f"expected version '002' but got '{versions[i]}'"
                )

    def test_orphan_files_are_deleted(self):
        """Files present in the prefix but absent from file_metas are deleted."""
        store = MemoryStore()
        cell, year = self._cell_year()
        year_str = str(year) if year is not None else "unknown"
        prefix = f"grid_partition={cell}/year={year_str}"

        # Two known part files.
        fm1 = self._write_item_to_store(
            store, f"{prefix}/part_known_1.parquet", {},
        )
        fm2 = self._write_item_to_store(
            store, f"{prefix}/part_known_2.parquet", {},
        )
        # One orphan — exists on disk but NOT in file_metas.
        orphan_key = f"{prefix}/part_orphan.parquet"
        self._write_item_to_store(store, orphan_key, {})

        out_key = f"{prefix}/compacted.parquet"
        _compact_group_impl([fm1, fm2], out_key, store)

        # Orphan must be gone.
        with pytest.raises(Exception):
            obstore.get(store, orphan_key)

        # Compacted output must still be present.
        raw = bytes(obstore.get(store, out_key).bytes())
        assert len(raw) > 0

    def test_orphan_sweep_on_single_file_bucket(self):
        """Even a single-file bucket triggers orphan sweep before passthrough."""
        store = MemoryStore()
        cell, year = self._cell_year()
        year_str = str(year) if year is not None else "unknown"
        prefix = f"grid_partition={cell}/year={year_str}"

        fm = self._write_item_to_store(
            store, f"{prefix}/part_only.parquet", {},
        )
        orphan_key = f"{prefix}/part_stale.parquet"
        self._write_item_to_store(store, orphan_key, {})

        out_key = f"{prefix}/compacted_unused.parquet"
        result = _compact_group_impl([fm], out_key, store)

        # Single-file passthrough: result is the original fm.
        assert result is fm
        # Orphan must be gone.
        with pytest.raises(Exception):
            obstore.get(store, orphan_key)


# ---------------------------------------------------------------------------
# TestIngestStats
# ---------------------------------------------------------------------------

class TestIngestStats:
    """
    Unit tests for _write_ingest_stats() and fan-out counter propagation
    through the full pipeline.
    """

    def _make_final_metas(self, cell="81007ffffffffff"):
        """Return two minimal FileMetadata entries for testing."""
        return [
            FileMetadata(s3_key=f"grid_partition={cell}/year=2019/compacted_a.parquet",
                         grid_partition=cell, year=2019,
                         row_count=1000, file_size_bytes=500_000),
            FileMetadata(s3_key=f"grid_partition={cell}/year=2020/compacted_b.parquet",
                         grid_partition=cell, year=2020,
                         row_count=200, file_size_bytes=100_000),
        ]

    # ------------------------------------------------------------------
    # _write_ingest_stats — file creation and content
    # ------------------------------------------------------------------

    def test_creates_stats_file(self, tmp_path):
        """Stats file is created next to the catalog."""
        catalog = tmp_path / "catalog.db"
        catalog.touch()
        _write_ingest_stats(
            catalog_path=str(catalog),
            inventory_path="s3://bucket/manifest.json",
            since=None,
            source_items=1000,
            total_rows=2800,
            final_metas=self._make_final_metas(),
            fan_out_counter={1: 50, 2: 400, 3: 550},
            h3_resolution=1,
        )
        stats_path = tmp_path / "ingest_stats.json"
        assert stats_path.exists()

    def test_stats_values_correct(self, tmp_path):
        """Written record contains correct computed fields."""
        from collections import Counter
        catalog = tmp_path / "catalog.db"
        catalog.touch()
        _write_ingest_stats(
            catalog_path=str(catalog),
            inventory_path="s3://bucket/manifest.json",
            since=None,
            source_items=1000,
            total_rows=2800,
            final_metas=self._make_final_metas(),
            fan_out_counter=Counter({1: 50, 2: 400, 3: 550}),
            h3_resolution=1,
        )
        records = json.loads((tmp_path / "ingest_stats.json").read_text())
        assert len(records) == 1
        r = records[0]
        assert r["source_items"]  == 1000
        assert r["total_rows"]    == 2800
        assert r["avg_fan_out"]   == 2.8
        assert r["overhead_pct"]  == 180.0
        assert r["unique_cells"]  == 1
        assert r["unique_years"]  == 2
        assert r["files_written"] == 2
        assert r["h3_resolution"] == 1
        assert r["since"]         is None
        assert r["fan_out_distribution"] == {"1": 50, "2": 400, "3": 550}

    def test_appends_across_runs(self, tmp_path):
        """Each call appends a new record; existing records are preserved."""
        from collections import Counter
        catalog = tmp_path / "catalog.db"
        catalog.touch()
        for i in range(3):
            _write_ingest_stats(
                catalog_path=str(catalog),
                inventory_path=f"s3://bucket/manifest_{i}.json",
                since=None,
                source_items=100 * (i + 1),
                total_rows=200 * (i + 1),
                final_metas=self._make_final_metas(),
                fan_out_counter=Counter({2: 100 * (i + 1)}),
                h3_resolution=1,
            )
        records = json.loads((tmp_path / "ingest_stats.json").read_text())
        assert len(records) == 3
        assert records[0]["source_items"] == 100
        assert records[1]["source_items"] == 200
        assert records[2]["source_items"] == 300

    def test_since_serialised(self, tmp_path):
        """When since is set, it is written as an ISO string."""
        from collections import Counter
        from datetime import datetime
        catalog = tmp_path / "catalog.db"
        catalog.touch()
        since = datetime(2026, 4, 1, tzinfo=UTC)
        _write_ingest_stats(
            catalog_path=str(catalog),
            inventory_path="s3://bucket/manifest.json",
            since=since,
            source_items=10,
            total_rows=20,
            final_metas=self._make_final_metas(),
            fan_out_counter=Counter({2: 10}),
            h3_resolution=1,
        )
        records = json.loads((tmp_path / "ingest_stats.json").read_text())
        assert records[0]["since"] == since.isoformat()

    # ------------------------------------------------------------------
    # fan-out counter propagation through _ingest_chunk_impl
    # ------------------------------------------------------------------

    def test_ingest_chunk_returns_counter(self):
        """_ingest_chunk_impl must return a Counter alongside file metas."""
        from collections import Counter
        store = MemoryStore()
        metas, counter = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        assert isinstance(counter, Counter)
        # Total items counted must equal number of source items fetched.
        assert sum(counter.values()) == len(_STAC_ITEMS)
        # All keys must be positive integers (cells per item).
        assert all(k >= 1 for k in counter)

    def test_ingest_chunk_counter_sums_to_total_rows(self):
        """sum(k * v for k, v in counter) == total rows written."""
        store = MemoryStore()
        metas, counter = _ingest_chunk_impl(
            _ALL_PAIRS, _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        total_rows_from_metas   = sum(m.row_count for m in metas)
        total_rows_from_counter = sum(k * v for k, v in counter.items())
        assert total_rows_from_counter == total_rows_from_metas

    def test_empty_chunk_returns_empty_counter(self):
        """An empty bucket-key list returns an empty Counter, not an error."""
        from collections import Counter
        store = MemoryStore()
        metas, counter = _ingest_chunk_impl(
            [], _PARTITIONER, store, chunk_id=0,
            item_fetcher=_mock_fetcher,
        )
        assert metas == []
        assert counter == Counter()

    # ------------------------------------------------------------------
    # end-to-end: stats file written by run_backfill
    # ------------------------------------------------------------------

    def test_run_backfill_writes_stats_file(self, tmp_path):
        """run_backfill() must create ingest_stats.json in the catalog directory."""
        from obstore.store import LocalStore, MemoryStore

        from earthcatalog.core import store_config

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh = tmp_path / "warehouse"
        wh.mkdir()
        wh_store = LocalStore(str(wh))
        catalog_path = str(tmp_path / "catalog.db")

        # Empty CSV with header — produces 0 items, still writes stats.
        inv = tmp_path / "inv.csv"
        inv.write_text("bucket,key\n")

        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=str(wh),
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                chunk_size=10,
                use_lock=False,
            )

        stats_path = tmp_path / "ingest_stats.json"
        assert stats_path.exists(), "ingest_stats.json was not created"
        records = json.loads(stats_path.read_text())
        assert len(records) == 1
        r = records[0]
        assert r["h3_resolution"] == 2
        assert r["source_items"]  == 0
        assert r["total_rows"]    == 0

    def test_run_backfill_stats_with_items(self, tmp_path):
        """Stats record reflects correct counts when items are actually ingested."""
        import csv

        from obstore.store import LocalStore, MemoryStore

        from earthcatalog.core import store_config

        # Write a tiny CSV inventory pointing at mock keys.
        inv = tmp_path / "inv.csv"
        with open(inv, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["bucket", "key"])
            for b, k in _ALL_PAIRS:
                w.writerow([b, k])

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh = tmp_path / "warehouse"
        wh.mkdir()
        wh_store = LocalStore(str(wh))
        catalog_path = str(tmp_path / "catalog.db")

        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=str(wh),
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                chunk_size=50,
                use_lock=False,
            )

        records = json.loads((tmp_path / "ingest_stats.json").read_text())
        r = records[0]
        assert r["source_items"] == len(_STAC_ITEMS)
        assert r["total_rows"]   == sum(
            int(k) * v for k, v in r["fan_out_distribution"].items()
        )
        assert r["avg_fan_out"]  > 0
        assert r["files_written"] > 0
        assert isinstance(r["fan_out_distribution"], dict)


# ---------------------------------------------------------------------------
# TestLoadProcessedFiles
# ---------------------------------------------------------------------------

class TestLoadProcessedFiles:
    """Unit tests for _load_processed_files()."""

    def test_returns_empty_set_when_file_missing(self, tmp_path):
        from earthcatalog.pipelines.backfill import _load_processed_files
        result = _load_processed_files(str(tmp_path / "nonexistent.json"))
        assert result == set()

    def test_returns_empty_set_on_corrupt_file(self, tmp_path):
        from earthcatalog.pipelines.backfill import _load_processed_files
        p = tmp_path / "stats.json"
        p.write_text("not valid json{{")
        result = _load_processed_files(str(p))
        assert result == set()

    def test_returns_inventory_file_keys(self, tmp_path):
        from earthcatalog.pipelines.backfill import _load_processed_files
        p = tmp_path / "stats.json"
        records = [
            {"inventory_file": "inventory/data/file1.parquet", "run_id": "2026-01-01T00:00:00+00:00"},
            {"inventory_file": "inventory/data/file2.parquet", "run_id": "2026-01-01T00:01:00+00:00"},
            {"inventory_file": None, "run_id": "2026-01-01T00:02:00+00:00"},  # null → excluded
        ]
        p.write_text(json.dumps(records))
        result = _load_processed_files(str(p))
        assert result == {"inventory/data/file1.parquet", "inventory/data/file2.parquet"}

    def test_records_without_inventory_file_field_excluded(self, tmp_path):
        from earthcatalog.pipelines.backfill import _load_processed_files
        p = tmp_path / "stats.json"
        # Old-format records (before this field was added) have no inventory_file key.
        records = [{"run_id": "2026-01-01T00:00:00+00:00", "source_items": 100}]
        p.write_text(json.dumps(records))
        result = _load_processed_files(str(p))
        assert result == set()


# ---------------------------------------------------------------------------
# TestPerFileMiniRuns
# ---------------------------------------------------------------------------

class TestPerFileMiniRuns:
    """
    Tests for the spot-resilient per-file mini-run path in run_backfill().

    We simulate a manifest by patching _list_manifest_files and
    _iter_inventory_file_from_store so no real S3 calls are made.
    """

    def _make_env(self, tmp_path):
        """Return (catalog_path, wh_store, wh_root, stats_path)."""
        from obstore.store import LocalStore, MemoryStore

        from earthcatalog.core import store_config

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh = tmp_path / "warehouse"
        wh.mkdir()
        wh_store = LocalStore(str(wh))
        catalog_path = str(tmp_path / "catalog.db")
        stats_path = tmp_path / "ingest_stats.json"
        return catalog_path, wh_store, str(wh), stats_path

    def test_per_file_writes_one_stats_record_per_file(self, tmp_path):
        """
        With two manifest data files, ingest_stats.json should have two records
        — one per file — each with a distinct inventory_file field.
        """
        import io

        import dask

        catalog_path, wh_store, wh_root, stats_path = self._make_env(tmp_path)

        # Build a fake inventory parquet file for each manifest data file.
        import pyarrow as pa
        import pyarrow.parquet as pq

        def _make_parquet_bytes(pairs):
            tbl = pa.table({
                "bucket": [b for b, _ in pairs],
                "key":    [k for _, k in pairs],
            })
            buf = io.BytesIO()
            pq.write_table(tbl, buf)
            buf.seek(0)
            return buf.read()

        file_a_pairs = [("mock-bucket", "stac/item-0000.stac.json"),
                        ("mock-bucket", "stac/item-0001.stac.json")]
        file_b_pairs = [("mock-bucket", "stac/item-0002.stac.json"),
                        ("mock-bucket", "stac/item-0003.stac.json")]

        fake_dest_store = object()  # dummy — never used directly
        fake_data_keys = ["inv/data/fileA.parquet", "inv/data/fileB.parquet"]

        pairs_by_key = {
            "inv/data/fileA.parquet": file_a_pairs,
            "inv/data/fileB.parquet": file_b_pairs,
        }

        def fake_list_manifest(uri):
            return "mock-bucket", fake_dest_store, fake_data_keys

        def fake_iter_file(store, data_key, batch_size=65536, since=None):
            yield from pairs_by_key[data_key]

        manifest_uri = "s3://fake-log-bucket/inv/manifest.json"

        with (
            patch("earthcatalog.pipelines.backfill._list_manifest_files",
                  side_effect=fake_list_manifest),
            patch("earthcatalog.pipelines.backfill._iter_inventory_file_from_store",
                  side_effect=fake_iter_file),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=manifest_uri,
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                chunk_size=50,
                use_lock=False,
                per_file_mini_runs=True,
            )

        assert stats_path.exists(), "ingest_stats.json was not created"
        records = json.loads(stats_path.read_text())
        assert len(records) == 2, f"expected 2 records, got {len(records)}"
        keys = {r["inventory_file"] for r in records}
        assert keys == set(fake_data_keys)

    def test_checkpoint_skips_already_processed_files(self, tmp_path):
        """
        If ingest_stats.json already records fileA, only fileB should be
        processed in the new run.
        """
        import dask

        catalog_path, wh_store, wh_root, stats_path = self._make_env(tmp_path)

        # Pre-seed stats with fileA already done.
        stats_path.write_text(json.dumps([
            {
                "inventory_file": "inv/data/fileA.parquet",
                "run_id": "2026-01-01T00:00:00+00:00",
                "source_items": 2,
            }
        ]))

        file_b_pairs = [("mock-bucket", "stac/item-0002.stac.json")]
        fake_data_keys = ["inv/data/fileA.parquet", "inv/data/fileB.parquet"]
        pairs_by_key = {"inv/data/fileB.parquet": file_b_pairs}
        processed = []

        def fake_list_manifest(uri):
            return "mock-bucket", object(), fake_data_keys

        def fake_iter_file(store, data_key, batch_size=65536, since=None):
            processed.append(data_key)
            yield from pairs_by_key.get(data_key, [])

        with (
            patch("earthcatalog.pipelines.backfill._list_manifest_files",
                  side_effect=fake_list_manifest),
            patch("earthcatalog.pipelines.backfill._iter_inventory_file_from_store",
                  side_effect=fake_iter_file),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path="s3://fake-log-bucket/inv/manifest.json",
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                chunk_size=50,
                use_lock=False,
                per_file_mini_runs=True,
                report_location=str(stats_path),
            )

        # Only fileB should have been iterated.
        assert processed == ["inv/data/fileB.parquet"], \
            f"fileA should be skipped; processed={processed}"

        records = json.loads(stats_path.read_text())
        # One pre-existing record + one new record for fileB.
        assert len(records) == 2
        keys = {r["inventory_file"] for r in records}
        assert "inv/data/fileB.parquet" in keys

    def test_per_file_false_uses_monolithic_path(self, tmp_path):
        """
        When per_file_mini_runs=False, a manifest URI goes through the
        monolithic _iter_inventory path (no _list_manifest_files call).
        """
        import csv

        import dask

        catalog_path, wh_store, wh_root, stats_path = self._make_env(tmp_path)

        # Use a CSV inventory (not a manifest) to keep it simple.
        inv = tmp_path / "inv.csv"
        with open(inv, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["bucket", "key"])
            for b, k in _ALL_PAIRS[:2]:
                w.writerow([b, k])

        list_manifest_called = []

        def fake_list_manifest(uri):
            list_manifest_called.append(uri)
            return "mock-bucket", object(), []

        with (
            patch("earthcatalog.pipelines.backfill._list_manifest_files",
                  side_effect=fake_list_manifest),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                chunk_size=50,
                use_lock=False,
                per_file_mini_runs=False,
            )

        assert list_manifest_called == [], \
            "_list_manifest_files should not be called in monolithic mode"

    def test_inventory_file_field_is_null_for_monolithic_run(self, tmp_path):
        """
        In monolithic mode the ingest_stats.json record should have
        inventory_file=null (not a data-file key).
        """
        import csv

        import dask

        catalog_path, wh_store, wh_root, stats_path = self._make_env(tmp_path)

        inv = tmp_path / "inv.csv"
        with open(inv, "w", newline="") as fh:
            csv.writer(fh).writerow(["bucket", "key"])

        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                warehouse_store=wh_store,
                warehouse_root=wh_root,
                item_fetcher=_mock_fetcher,
                h3_resolution=2,
                use_lock=False,
            )

        records = json.loads(stats_path.read_text())
        assert records[0]["inventory_file"] is None
