"""
End-to-end pipeline test: run() with a local inventory CSV and mocked STAC fetches.

No network calls are made.  _fetch_item is patched to return in-memory dicts so
the full run() → fan_out() → write_geoparquet() → add_files → scan path is exercised.

The lock and catalog download/upload are disabled via use_lock=False and a
MemoryStore configured in store_config so no S3 is needed.
"""

import csv
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.pipelines.incremental import run

# ---------------------------------------------------------------------------
# Shared STAC item fixtures
# ---------------------------------------------------------------------------

STAC_ITEMS = [
    {
        "id": f"e2e-item-{i:04d}",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-10 + i, 60],
                    [10 + i, 60],
                    [10 + i, 70],
                    [-10 + i, 70],
                    [-10 + i, 60],
                ]
            ],
        },
        "properties": {
            "datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-15T00:00:00Z",
            "platform": "sentinel-2",
            "percent_valid_pixels": float(70 + i),
            "date_dt": float(5 + i),
            "proj:code": "EPSG:32633",
            "sat:orbit_state": "descending",
        },
        "links": [],
        "assets": {},
    }
    for i in range(6)
]

_ITEM_MAP: dict[tuple[str, str], dict] = {
    ("fake-bucket", f"stac/item-{i:04d}.stac.json"): item for i, item in enumerate(STAC_ITEMS)
}


def _mock_fetch_item(bucket: str, key: str) -> dict | None:
    return _ITEM_MAP.get((bucket, key))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_inventory(path: Path, rows: list[tuple[str, str]]) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "key"])
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def memory_store(tmp_path):
    """Point store_config at a MemoryStore so download/upload are no-ops."""
    store = MemoryStore()
    store_config.set_store(store)
    store_config.set_catalog_key("catalog.db")
    yield store


@pytest.fixture()
def inventory_csv(tmp_path) -> Path:
    inv = tmp_path / "inventory.csv"
    _write_inventory(inv, list(_ITEM_MAP.keys()))
    return inv


@pytest.fixture()
def catalog_dirs(tmp_path):
    return str(tmp_path / "catalog.db"), str(tmp_path / "warehouse")


# ---------------------------------------------------------------------------
# Helper: run pipeline and return the Iceberg table
# ---------------------------------------------------------------------------


def _run_pipeline(inventory_csv, catalog_dirs, **kwargs):
    db, wh = catalog_dirs
    partitioner = H3Partitioner(resolution=2)
    with patch("earthcatalog.pipelines.incremental._fetch_item", side_effect=_mock_fetch_item):
        run(
            inventory_path=str(inventory_csv),
            catalog_path=db,
            warehouse_path=wh,
            chunk_size=10,
            max_workers=2,
            partitioner=partitioner,
            use_lock=False,
            **kwargs,
        )
    from earthcatalog.core.catalog import get_or_create_table, open_catalog

    catalog = open_catalog(db_path=db, warehouse_path=wh)
    return get_or_create_table(catalog)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineE2E:
    def test_rows_written(self, inventory_csv, catalog_dirs):
        """At least one row per STAC item must appear in the Iceberg table."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        assert result.num_rows >= len(STAC_ITEMS)

    def test_all_item_ids_present(self, inventory_csv, catalog_dirs):
        """Every item id from the inventory must appear in the scan result."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        found_ids = set(result.column("id").to_pylist())
        for item in STAC_ITEMS:
            assert item["id"] in found_ids

    def test_snapshot_created(self, inventory_csv, catalog_dirs):
        """run() must produce at least one Iceberg snapshot."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        assert len(table.history()) >= 1

    def test_int_fields_stored_as_int(self, inventory_csv, catalog_dirs):
        """percent_valid_pixels and date_dt must be integers after the round-trip."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        for val in result.column("percent_valid_pixels").to_pylist():
            if val is not None:
                assert isinstance(val, int), f"expected int, got {type(val)}: {val}"
        for val in result.column("date_dt").to_pylist():
            if val is not None:
                assert isinstance(val, int), f"expected int, got {type(val)}: {val}"

    def test_grid_partition_non_null(self, inventory_csv, catalog_dirs):
        """Every row must carry a non-empty grid_partition string."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        partitions = result.column("grid_partition").to_pylist()
        assert all(p and isinstance(p, str) for p in partitions)

    def test_properties_promoted_to_columns(self, inventory_csv, catalog_dirs):
        """STAC properties must appear as top-level columns, not nested dicts."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        for col in ("platform", "proj_code", "sat_orbit_state"):
            assert col in result.schema.names
            vals = [v for v in result.column(col).to_pylist() if v is not None]
            assert len(vals) > 0, f"column {col} is all-null"

    def test_raw_stac_roundtrip(self, inventory_csv, catalog_dirs):
        """raw_stac must deserialise back to the original item dict."""
        table = _run_pipeline(inventory_csv, catalog_dirs)
        result = table.scan().to_arrow()
        stored_ids = {json.loads(r.as_py())["id"] for r in result.column("raw_stac")}
        for item in STAC_ITEMS:
            assert item["id"] in stored_ids

    def test_geoparquet_files_in_warehouse(self, inventory_csv, catalog_dirs):
        """GeoParquet files written to the warehouse must carry geo metadata."""
        import pyarrow.parquet as pq

        db, wh = catalog_dirs
        _run_pipeline(inventory_csv, catalog_dirs)
        parquet_files = list(Path(wh).glob("**/*.parquet"))
        assert len(parquet_files) >= 1
        for f in parquet_files:
            pf = pq.ParquetFile(f)
            assert b"geo" in pf.metadata.metadata, f"{f} missing geo metadata"

    def test_limit_caps_items(self, inventory_csv, catalog_dirs):
        """limit=2 must write rows for at most 2 STAC items."""
        table = _run_pipeline(inventory_csv, catalog_dirs, limit=2)
        result = table.scan().to_arrow()
        unique_ids = set(result.column("id").to_pylist())
        assert len(unique_ids) <= 2

    def test_skips_non_stac_json_keys(self, tmp_path, catalog_dirs):
        """Inventory rows whose key does not end in .stac.json must be ignored."""
        inv = tmp_path / "mixed_inventory.csv"
        rows = list(_ITEM_MAP.keys()) + [
            ("fake-bucket", "stac/README.txt"),
            ("fake-bucket", "stac/metadata.json"),
        ]
        _write_inventory(inv, rows)

        table = _run_pipeline(inv, catalog_dirs)
        result = table.scan().to_arrow()
        found_ids = set(result.column("id").to_pylist())
        for item in STAC_ITEMS:
            assert item["id"] in found_ids

    def test_missing_fetch_returns_no_rows(self, tmp_path, catalog_dirs):
        """Items that _fetch_item cannot resolve (returns None) are skipped."""
        inv = tmp_path / "bad_inventory.csv"
        _write_inventory(inv, [("fake-bucket", "stac/nonexistent.stac.json")])
        db, wh = catalog_dirs
        with patch("earthcatalog.pipelines.incremental._fetch_item", return_value=None):
            run(
                inventory_path=str(inv),
                catalog_path=db,
                warehouse_path=wh,
                chunk_size=10,
                max_workers=1,
                partitioner=H3Partitioner(resolution=2),
                use_lock=False,
            )
        from earthcatalog.core.catalog import get_or_create_table, open_catalog

        catalog = open_catalog(db_path=db, warehouse_path=wh)
        table = get_or_create_table(catalog)
        assert len(table.history()) == 0

    def test_batch_add_files_single_snapshot(self, inventory_csv, catalog_dirs):
        """batch_add_files=True must produce exactly one Iceberg snapshot."""
        table = _run_pipeline(inventory_csv, catalog_dirs, batch_add_files=True)
        assert len(table.history()) == 1, f"expected 1 snapshot, got {len(table.history())}"

    def test_batch_add_files_multi_chunk_single_snapshot(self, inventory_csv, tmp_path):
        """With multiple chunks, batch_add_files=True still produces exactly
        one snapshot (chunk_size=2, 6 items → 3 chunks → 1 snapshot)."""
        from earthcatalog.core.catalog import get_or_create_table, open_catalog

        db = str(tmp_path / "catalog_mc.db")
        wh = str(tmp_path / "wh_mc")
        with patch("earthcatalog.pipelines.incremental._fetch_item", side_effect=_mock_fetch_item):
            run(
                inventory_path=str(inventory_csv),
                catalog_path=db,
                warehouse_path=wh,
                chunk_size=2,
                max_workers=2,
                partitioner=H3Partitioner(resolution=2),
                use_lock=False,
                batch_add_files=True,
            )
        catalog = open_catalog(db_path=db, warehouse_path=wh)
        table = get_or_create_table(catalog)
        assert len(table.history()) == 1, f"expected 1 snapshot, got {len(table.history())}"

    def test_batch_add_files_same_rows_as_incremental(self, inventory_csv, catalog_dirs, tmp_path):
        """batch_add_files=True and False must produce the same set of row IDs."""
        # Non-batch run (default) — uses catalog_dirs
        table_inc = _run_pipeline(inventory_csv, catalog_dirs, batch_add_files=False)
        ids_inc = set(table_inc.scan().to_arrow().column("id").to_pylist())

        # Batch run — fresh catalog in tmp_path subdirs
        db_bat = str(tmp_path / "catalog_bat.db")
        wh_bat = str(tmp_path / "wh_bat")
        table_bat = _run_pipeline(inventory_csv, (db_bat, wh_bat), batch_add_files=True)
        ids_bat = set(table_bat.scan().to_arrow().column("id").to_pylist())

        assert ids_inc == ids_bat
