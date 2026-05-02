"""
High-level tests for earthcatalog.pipelines.backfill.

Tests the compact → register → end-to-end pipeline phases using
MemoryStore.  Internal helper functions (write_chunks, NDJSON I/O,
dedup, fetch internals) are exercised indirectly through the e2e tests.

Integration tests (marked ``pytest.mark.integration``) fetch real STAC
items from the public ITS_LIVE bucket.
"""

import io
import json

import dask
import obstore
import pyarrow.parquet as pq
import pytest
from obstore.store import LocalStore, MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.transform import write_geoparquet as _write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

_PARTITIONER = H3Partitioner(resolution=2)

_STAC_ITEMS = [
    {
        "id": f"v2-item-{i:04d}",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-10 + i, 60], [10 + i, 60], [10 + i, 70], [-10 + i, 70], [-10 + i, 60]]
            ],
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
            "updated": f"202{i % 4 + 1}-06-01T00:00:00Z",
        },
        "links": [],
        "assets": {},
    }
    for i in range(8)
]

_ITEM_MAP = {
    ("mock-bucket", f"stac/item-{i:04d}.stac.json"): item for i, item in enumerate(_STAC_ITEMS)
}

_ALL_PAIRS = list(_ITEM_MAP.keys())


def _make_inventory_csv(pairs: list[tuple[str, str]]) -> str:
    lines = ["bucket,key"]
    for b, k in pairs:
        lines.append(f"{b},{k}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Compact — NDJSON → GeoParquet
# ---------------------------------------------------------------------------


class TestCompactCellYear:
    def test_reads_ndjson_and_writes_parquet(self, tmp_path):
        from earthcatalog.core.transform import fan_out, group_by_partition
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()
        fo = fan_out(_STAC_ITEMS, _PARTITIONER)
        (cell, year), items = list(group_by_partition(fo).items())[0]
        for item in items:
            item["properties"]["updated"] = "2024-01-01T00:00:00Z"
        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        obstore.put(store, f"staging/{cell}/{year}/worker.ndjson", ndjson_data)

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        report = compact_cell_year(
            cell=cell,
            year=str(year),
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=LocalStore(str(wh_root)),
        )

        assert report["output_rows"] > 0
        assert len(report["output_files"]) == 1
        out_key = report["output_files"][0]
        assert "grid_partition=" in out_key
        tbl = pq.ParquetFile(str(wh_root / out_key)).read()
        assert tbl.num_rows == len(items)

    def test_empty_prefix_returns_empty(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()
        report = compact_cell_year(
            cell="nocell",
            year="2024",
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=LocalStore(str(tmp_path)),
        )
        assert report["output_rows"] == 0

    def test_writes_to_store(self):
        from earthcatalog.core.transform import fan_out, group_by_partition
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()
        wh_store = MemoryStore()
        fo = fan_out(_STAC_ITEMS, _PARTITIONER)
        (cell, year), items = list(group_by_partition(fo).items())[0]
        for item in items:
            item["properties"]["updated"] = "2024-01-01T00:00:00Z"
        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        obstore.put(store, f"staging/{cell}/{year}/worker.ndjson", ndjson_data)

        report = compact_cell_year(
            cell=cell,
            year=str(year),
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=wh_store,
        )

        assert report["output_rows"] > 0
        out_key = report["output_files"][0]
        raw = memoryview(obstore.get(wh_store, out_key).bytes()).tobytes()
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.num_rows == len(items)


class TestCompactCellYearDelta:
    def test_writes_new_files_without_overwriting(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year_delta

        store = MemoryStore()
        cell, year = "deltacell", "2024"
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        wh_store = LocalStore(str(wh_root))

        # Existing file
        existing = [
            {
                "id": "old",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
        ]
        _write_geoparquet(
            existing, str(wh_root / f"grid_partition={cell}/year={year}/part_000000.parquet")
        )

        # New delta
        new_items = [
            {
                "id": "new",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
        ]
        obstore.put(
            store,
            f"staging/{cell}/{year}/chunk.ndjson",
            "\n".join(json.dumps(i) for i in new_items).encode(),
        )

        report = compact_cell_year_delta(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=wh_store,
        )

        assert report["output_rows"] == 1
        assert "part_000001.parquet" in report["output_files"][0]
        # Existing file untouched
        raw = memoryview(
            obstore.get(wh_store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()
        ).tobytes()
        assert pq.ParquetFile(io.BytesIO(raw)).read().num_rows == 1

    def test_writes_new_files_with_offset(self):
        from earthcatalog.pipelines.backfill import compact_cell_year_delta

        store = MemoryStore()
        wh_store = MemoryStore()
        cell, year = "s3delta", "2024"
        obstore.put(wh_store, f"grid_partition={cell}/year={year}/part_000000.parquet", b"old")
        obstore.put(wh_store, f"grid_partition={cell}/year={year}/part_000001.parquet", b"old")

        items = [
            {
                "id": "new",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
        ]
        obstore.put(
            store,
            f"staging/{cell}/{year}/chunk.ndjson",
            "\n".join(json.dumps(i) for i in items).encode(),
        )

        report = compact_cell_year_delta(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=wh_store,
        )

        assert report["output_rows"] == 1
        assert "part_000002.parquet" in report["output_files"][0]


# ---------------------------------------------------------------------------
# Register — Iceberg catalog lifecycle
# ---------------------------------------------------------------------------


class TestRegisterAndCleanup:
    def test_registers_files_and_cleans_up(self, tmp_path):
        from obstore.store import LocalStore

        from earthcatalog.pipelines.backfill import register_and_cleanup

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        staging_store = MemoryStore()
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.transform import fan_out, group_by_partition

        fo = fan_out(_STAC_ITEMS[:1], _PARTITIONER)
        for (_c, _y), group in group_by_partition(fo).items():
            d = wh_root / f"grid_partition={_c}" / f"year={_y}"
            d.mkdir(parents=True, exist_ok=True)
            _write_geoparquet(group, str(d / "part_000000.parquet"))

        obstore.put(staging_store, "staging/chunks/chunk_000.parquet", b"data")
        catalog_path = str(tmp_path / "catalog.db")

        register_and_cleanup(
            catalog_path=catalog_path,
            warehouse_root=str(wh_root),
            staging_store=staging_store,
            staging_prefix="staging",
            warehouse_store=LocalStore(str(wh_root)),
            upload=False,
        )

        from earthcatalog.core.catalog import _open_sqlite

        table = _open_sqlite(db_path=catalog_path, warehouse_path=str(wh_root)).load_table(
            "earthcatalog.stac_items"
        )
        assert table.scan().to_arrow().num_rows > 0


class TestRegisterDelta:
    def test_adds_files_to_existing_table(self, tmp_path):
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            _open_sqlite,
        )

        catalog = _open_sqlite(db_path=str(tmp_path / "existing.db"), warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        catalog.create_table(
            identifier=FULL_NAME, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC
        )

        cell_dir = wh_root / "grid_partition=testcell" / "year=2024"
        cell_dir.mkdir(parents=True)
        existing_items = [
            {
                "id": "existing-1",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "grid_partition": "testcell",
                },
                "links": [],
                "assets": {},
            }
        ]
        _write_geoparquet(existing_items, str(cell_dir / "part_000000.parquet"))
        catalog.load_table(FULL_NAME).add_files([str(cell_dir / "part_000000.parquet")])

        new_items = [
            {
                "id": "delta-1",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "grid_partition": "testcell",
                },
                "links": [],
                "assets": {},
            }
        ]
        _write_geoparquet(new_items, str(cell_dir / "part_000001.parquet"))

        catalog_path = str(tmp_path / "existing.db")
        register_delta(
            catalog_path=catalog_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[str(cell_dir / "part_000001.parquet")],
            staging_store=MemoryStore(),
            staging_prefix="staging",
            upload=False,
        )

        result = catalog.load_table(FULL_NAME).scan().to_arrow()
        assert set(result.column("id").to_pylist()) == {"existing-1", "delta-1"}

    def test_sets_hash_index_path(self, tmp_path):
        from earthcatalog.core.catalog import PROP_HASH_INDEX_PATH
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            _open_sqlite,
        )

        db_path = str(tmp_path / "test.db")
        catalog = _open_sqlite(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        catalog.create_table(
            identifier=FULL_NAME, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC
        )

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[],
            staging_store=MemoryStore(),
            staging_prefix="staging",
            upload=False,
        )

        assert (
            catalog.load_table(FULL_NAME).properties.get(PROP_HASH_INDEX_PATH)
            == f"{wh_root}_id_hashes.parquet"
        )


# ---------------------------------------------------------------------------
# End-to-end — run_backfill
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRunBackfillV2:
    @pytest.fixture(autouse=True)
    def _setup_store_config(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_end_to_end_with_mock_fetcher(self, tmp_path):
        from unittest.mock import patch

        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))
        staging_store = MemoryStore()
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        from earthcatalog.core.catalog import _open_sqlite

        table = _open_sqlite(db_path=catalog_path, warehouse_path=str(wh_root)).load_table(
            "earthcatalog.stac_items"
        )
        result = table.scan().to_arrow()
        assert result.num_rows >= len(_STAC_ITEMS)
        ids = set(result.column("id").to_pylist())
        for item in _STAC_ITEMS:
            assert item["id"] in ids

    def test_idempotent_re_run(self, tmp_path):
        from unittest.mock import patch

        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))
        staging_store = MemoryStore()
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        # Second run on fresh catalog — same staging store, chunks from run 1 survive
        wh_root2 = tmp_path / "warehouse2"
        wh_root2.mkdir()
        catalog_path2 = str(tmp_path / "catalog2.db")

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path2,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root2),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
                skip_inventory=True,
            )

        from earthcatalog.core.catalog import _open_sqlite

        table = _open_sqlite(db_path=catalog_path2, warehouse_path=str(wh_root2)).load_table(
            "earthcatalog.stac_items"
        )
        assert table.scan().to_arrow().num_rows >= len(_STAC_ITEMS)


@pytest.mark.e2e
class TestRunBackfillV2Delta:
    @pytest.fixture(autouse=True)
    def _setup_store_config(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_delta_does_not_overwrite_existing_parquets(self, tmp_path):
        from unittest.mock import patch

        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))
        staging_store = MemoryStore()
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())
        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        from earthcatalog.core.catalog import _open_sqlite

        table = _open_sqlite(db_path=catalog_path, warehouse_path=str(wh_root)).load_table(
            "earthcatalog.stac_items"
        )
        first_ids = set(table.scan().to_arrow().column("id").to_pylist())

        # Second delta run with new items
        extra_pairs = [("mock-bucket", "stac/item-extra.stac.json")]
        extra_item = {
            "id": "v2-item-extra",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-10, 60], [10, 60], [10, 70], [-10, 70], [-10, 60]]],
            },
            "properties": {
                "datetime": "2023-01-15T00:00:00Z",
                "platform": "NISAR",
                "percent_valid_pixels": 90.0,
                "date_dt": 15.0,
                "proj:code": "EPSG:32632",
                "sat:orbit_state": "ascending",
                "version": "002",
                "start_datetime": "2023-01-10T00:00:00Z",
                "end_datetime": "2023-01-20T00:00:00Z",
                "updated": "2023-06-01T00:00:00Z",
            },
            "links": [],
            "assets": {},
        }

        inv2 = tmp_path / "inv2.csv"
        inv2.write_text(_make_inventory_csv(extra_pairs))
        obstore.put(staging_store, f"{bucket}/{key}", json.dumps(extra_item).encode())

        catalog_path2 = str(tmp_path / "catalog2.db")
        import shutil

        shutil.copy(catalog_path, catalog_path2)

        all_items = {**_ITEM_MAP, extra_pairs[0]: extra_item}

        async def _mock_fetch_all(store, bucket, key):
            return all_items.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch_all),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv2),
                catalog_path=catalog_path2,
                staging_store=staging_store,
                staging_prefix="ingest2",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
                delta=True,
            )

        table2 = _open_sqlite(db_path=catalog_path2, warehouse_path=str(wh_root)).load_table(
            "earthcatalog.stac_items"
        )
        result2 = table2.scan().to_arrow()
        second_ids = set(result2.column("id").to_pylist())
        for item_id in first_ids:
            assert item_id in second_ids
        assert "v2-item-extra" in second_ids

        # No duplicate part filenames
        for cell_dir in wh_root.glob("grid_partition=*"):
            for year_dir in cell_dir.glob("year=*"):
                parts = [p.name for p in year_dir.glob("part_*.parquet")]
                assert len(parts) == len(set(parts)), f"Duplicate in {year_dir}"


# ---------------------------------------------------------------------------
# Integration tests — real S3
# ---------------------------------------------------------------------------

_REAL_ITEM_KEY = (
    "velocity_image_pair/landsatOLI/v02/N60W030/"
    "LC08_L1TP_232011_20190524_20200828_02_T1_X_LC08_L1TP_232011_20190711_20200827_02_T1_G0120V02_P056.stac.json"
)


@pytest.mark.integration
class TestFetchRealS3:
    @pytest.fixture()
    def public_store(self):
        from obstore.store import S3Store

        return S3Store(bucket="its-live-data", region="us-west-2", skip_signature=True)

    def test_fetch_single_item(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        result = asyncio.run(_fetch_item_async(public_store, "its-live-data", _REAL_ITEM_KEY))
        assert result is not None and result["type"] == "Feature"

    def test_fetch_all_with_real_store(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_all_async

        items, failed = asyncio.run(
            _fetch_all_async(
                [("its-live-data", _REAL_ITEM_KEY)], concurrency=10, store=public_store
            )
        )
        assert len(items) == 1 and len(failed) == 0

    def test_fetch_missing_key_returns_none(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        result = asyncio.run(
            _fetch_item_async(public_store, "its-live-data", "nonexistent/file.stac.json")
        )
        assert result is None
