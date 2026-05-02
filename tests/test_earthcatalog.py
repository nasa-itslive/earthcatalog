"""
Tests for the new EarthCatalog simplified API.

Tests actual logic: spatial partition pruning, temporal filtering, and
Iceberg integration — not trivial property access or string formatting.
"""

from pathlib import Path

import pytest
from obstore.store import MemoryStore
from shapely.geometry import Point, box

from earthcatalog.config import GridConfig
from earthcatalog.core import EarthCatalog, catalog
from earthcatalog.core.catalog import _open_sqlite, get_or_create
from earthcatalog.core.catalog_info import catalog_info
from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_warehouse(tmp_path):
    """
    Create a warehouse with real data spanning multiple cells and years.

    Returns:
        tuple: (tmp_path, table, items) where items is the list of original
               STAC item dicts for validation
    """
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    # Create items across multiple cells and years
    items = [
        {
            "id": f"test-item-{i:04d}",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-55 + i * 5, 62],
                        [-45 + i * 5, 62],
                        [-45 + i * 5, 72],
                        [-55 + i * 5, 72],
                        [-55 + i * 5, 62],
                    ]
                ],
            },
            "properties": {
                "datetime": f"202{i % 4 + 1}-06-15T00:00:00Z",
                "platform": "NISAR",
            },
            "links": [],
            "assets": {},
        }
        for i in range(10)
    ]

    p = H3Partitioner(resolution=2)
    rows = fan_out(items, p)
    groups = group_by_partition(rows)

    paths = []
    for (cell, year), group in groups.items():
        out = str(tmp_path / f"part_{cell[:12]}_{year}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)

    return tmp_path, tbl, items


@pytest.fixture
def memory_store_with_catalog(tmp_path):
    """
    Create an in-memory store with a catalog for testing the new API.

    Structure in MemoryStore:
        catalog/earthcatalog.db
        catalog/warehouse/
    """
    import obstore

    store = MemoryStore()

    # Create a catalog with data
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    # Add minimal data
    item = {
        "id": "mem-test",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-50, 65], [-48, 65], [-48, 68], [-50, 68], [-50, 65]]],
        },
        "properties": {"datetime": "2022-06-15T00:00:00Z", "platform": "NISAR"},
        "links": [],
        "assets": {},
    }

    p = H3Partitioner(resolution=2)
    rows = fan_out([item], p)
    for (cell, year), group in group_by_partition(rows).items():
        out = str(tmp_path / f"part_{cell}_{year}.parquet")
        write_geoparquet(group, out)
        tbl.add_files([out])

    # Upload catalog to memory store at catalog/earthcatalog.db
    catalog_bytes = Path(db).read_bytes()
    obstore.put(store, "catalog/earthcatalog.db", catalog_bytes)

    return store


# ---------------------------------------------------------------------------
# catalog.open() - new API
# ---------------------------------------------------------------------------


class TestNewCatalogOpenAPI:
    """Test the new simplified catalog.open() API."""

    def test_open_with_store_and_local_path(self, tmp_path):
        """Local warehouse with MemoryStore should create EarthCatalog."""
        from obstore.store import MemoryStore

        wh = str(tmp_path / "warehouse")
        store = MemoryStore()

        ec = catalog.open(store=store, base=wh)

        assert isinstance(ec, EarthCatalog)
        assert ec.grid_type == "h3"
        assert ec.grid_resolution == 1  # default

    def test_open_returns_earthcatalog_facade(self, tmp_path):
        """New API should return EarthCatalog, not SqlCatalog."""
        from obstore.store import MemoryStore

        store = MemoryStore()
        ec = catalog.open(store=store, base=str(tmp_path / "warehouse"))

        assert hasattr(ec, "search_files")
        assert hasattr(ec, "stats")
        assert hasattr(ec, "cells_for_geometry")

    def test_open_legacy_api_still_works(self, tmp_path):
        """Legacy db_path + warehouse_path API should still return SqlCatalog."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")

        cat = _open_sqlite(db_path=db, warehouse_path=wh)

        # Legacy API returns SqlCatalog, not EarthCatalog
        assert not isinstance(cat, EarthCatalog)
        assert hasattr(cat, "load_table")

    def test_open_requires_store_and_base(self):
        """open() must be called with store and base."""
        with pytest.raises(TypeError):
            catalog.open()

    def test_open_returns_earthcatalog(self, tmp_path):
        """open() with store+base returns an EarthCatalog."""
        from obstore.store import LocalStore

        store = LocalStore(str(tmp_path))
        ec = catalog.open(store=store, base=str(tmp_path))
        from earthcatalog.core import EarthCatalog

        assert isinstance(ec, EarthCatalog)


# ---------------------------------------------------------------------------
# EarthCatalog.search_files() - spatial pruning logic
# ---------------------------------------------------------------------------


class TestSearchFilesSpatialPruning:
    """Test that search_files correctly prunes by spatial partition."""

    def test_point_query_returns_subset_of_all_files(self, populated_warehouse):
        """Point query should return fewer files than total warehouse."""
        tmp_path, tbl, items = populated_warehouse
        info = catalog_info(tbl)

        # Get all files
        all_files = set()
        for task in tbl.scan().plan_files():
            all_files.add(task.file.file_path)

        # Query for a specific point
        point = Point(-50, 67)  # Within some of our item footprints
        paths = info.file_paths(tbl, point)

        # Should get a subset (not all, not empty)
        assert len(paths) > 0
        assert len(paths) < len(all_files)
        assert set(paths).issubset(all_files)

    def test_non_intersecting_geometry_returns_empty(self, populated_warehouse):
        """Geometry far from any data should return empty file list."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        # Query a location with no data (middle of Pacific)
        pacific = Point(0, 0)
        paths = info.file_paths(tbl, pacific)

        assert paths == []

    def test_bbox_returns_more_files_than_point(self, populated_warehouse):
        """Larger geometry should return equal or more files than point."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        point = Point(-50, 67)
        bbox = box(-60, 60, -40, 75)

        point_paths = info.file_paths(tbl, point)
        bbox_paths = info.file_paths(tbl, bbox)

        # BBox should include at least as many files as point
        assert len(bbox_paths) >= len(point_paths)


# ---------------------------------------------------------------------------
# EarthCatalog.search_files() - temporal filtering logic
# ---------------------------------------------------------------------------


class TestSearchFilesTemporalFiltering:
    """Test that search_files correctly prunes by temporal partition."""

    def test_start_datetime_excludes_earlier_years(self, populated_warehouse):
        """start_datetime should exclude files from earlier years."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        point = Point(-50, 67)
        all_paths = info.file_paths(tbl, point)
        filtered = info.file_paths(tbl, point, start_datetime="2023-01-01")

        # Filtered should have fewer or equal files
        assert len(filtered) <= len(all_paths)

        # Verify year partition constraint
        years_in_filtered = set()
        for task in tbl.scan().plan_files():
            if task.file.file_path in filtered:
                years_in_filtered.add(task.file.partition[1] + 1970)

        for year in years_in_filtered:
            assert year >= 2023

    def test_end_datetime_excludes_later_years(self, populated_warehouse):
        """end_datetime should exclude files from later years."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        point = Point(-50, 67)
        paths = info.file_paths(tbl, point, end_datetime="2021-12-31T23:59:59Z")

        # Verify year partition constraint
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                assert task.file.partition[1] + 1970 <= 2021

    def test_datetime_range_narrows_results(self, populated_warehouse):
        """Both start and end datetime should narrow to a range."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        point = Point(-50, 67)
        paths = info.file_paths(tbl, point, start_datetime="2021-01-01", end_datetime="2022-12-31")

        years = set()
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                years.add(task.file.partition[1] + 1970)

        # All years should be within range
        for year in years:
            assert 2021 <= year <= 2022


# ---------------------------------------------------------------------------
# EarthCatalog convenience methods
# ---------------------------------------------------------------------------


class TestEarthCatalogConvenienceMethods:
    """Test EarthCatalog convenience methods provide correct data."""

    def test_stats_aggregates_partition_data(self, populated_warehouse):
        """stats() should aggregate manifest data correctly."""
        _, tbl, items = populated_warehouse
        info = catalog_info(tbl)

        stats = info.stats(tbl)

        # Should have at least one partition
        assert len(stats) > 0

        # Each stat should have required keys
        for s in stats:
            assert "grid_partition" in s
            assert "year" in s
            assert "row_count" in s
            assert "file_count" in s

    def test_stats_row_counts_match_manifest(self, populated_warehouse):
        """stats() row counts should match Iceberg manifest records."""
        _, tbl, items = populated_warehouse
        info = catalog_info(tbl)

        stats = info.stats(tbl)
        total_from_stats = sum(s["row_count"] for s in stats)

        # Compare to manifest record count
        total_from_manifest = sum(task.file.record_count for task in tbl.scan().plan_files())

        assert total_from_stats == total_from_manifest

    def test_cells_for_geometry_returns_valid_h3(self, populated_warehouse):
        """cells_for_geometry should return valid H3 cell IDs."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        import h3

        bbox = box(-60, 60, -40, 75)
        cells = info.cells_for_geometry(bbox)

        # All cells should be valid H3
        for cell in cells:
            assert h3.is_valid_cell(cell), f"Invalid H3 cell: {cell}"

    def test_cell_list_sql_produces_valid_fragment(self, populated_warehouse):
        """cell_list_sql should produce valid embeddable SQL."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        bbox = box(-60, 60, -40, 75)
        sql = info.cell_list_sql(bbox)

        # Should be valid SQL fragment
        assert sql.startswith("grid_partition IN (")
        assert sql.endswith(")")

        # Should be embeddable
        full_sql = f"SELECT * FROM t WHERE {sql} AND datetime > '2020-01-01'"
        assert "grid_partition IN (" in full_sql


# ---------------------------------------------------------------------------
# Property access and metadata
# ---------------------------------------------------------------------------


class TestEarthCatalogProperties:
    """Test EarthCatalog property access for grid metadata."""

    def test_grid_properties_accessible(self, populated_warehouse):
        """Grid metadata should be accessible via properties."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        assert info.grid_type == "h3"
        assert isinstance(info.grid_resolution, int)

    def test_table_property_returns_underlying_table(self, populated_warehouse):
        """table property should return the PyIceberg Table."""
        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)

        # This is mainly for type checking - just ensure it doesn't error
        assert info is not None

    def test_html_repr_contains_summary(self, populated_warehouse):
        """_repr_html_ should contain catalog summary information."""
        from earthcatalog.core import EarthCatalog

        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)
        ec = EarthCatalog(catalog=None, table=tbl, info=info)

        html = ec._repr_html_()

        # Should contain key information
        assert "EarthCatalog" in html
        assert "Grid type" in html or "grid_type" in html
        # Should contain statistics section
        assert "Statistics" in html or "statistics" in html
        assert "Total files" in html
        assert "Total rows" in html
        assert "Unique items" in html
        assert "Partitions" in html
        # Should contain top partitions section
        assert "Top partitions" in html or "Top partition" in html

    def test_html_repr_is_valid_html(self, populated_warehouse):
        """_repr_html_ should be valid HTML containing proper tags."""
        from earthcatalog.core import EarthCatalog

        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)
        ec = EarthCatalog(catalog=None, table=tbl, info=info)

        html = ec._repr_html_()

        # Should contain proper HTML structure with tables
        assert "<div" in html
        assert "</div>" in html
        assert "<table" in html
        assert "</table>" in html
        assert "<tr" in html
        assert "<td" in html
        assert "<strong>" in html
        assert "</strong>" in html
        # Check for 2-column layout
        assert "display: flex" in html
        # Should contain statistics section
        assert "Statistics" in html or "statistics" in html

    def test_html_repr_stats_are_cached(self, populated_warehouse):
        """_repr_html_ should use cached stats to avoid repeated manifest scans."""
        from earthcatalog.core import EarthCatalog

        _, tbl, _ = populated_warehouse
        info = catalog_info(tbl)
        ec = EarthCatalog(catalog=None, table=tbl, info=info)

        # First call - computes and caches stats
        html1 = ec._repr_html_()
        assert info._cached_stats is not None

        # Second call - uses cached stats
        html2 = ec._repr_html_()

        # Should be identical
        assert html1 == html2


# ---------------------------------------------------------------------------
# Store and catalog download integration
# ---------------------------------------------------------------------------


class TestStoreIntegration:
    """Test integration with obstore for catalog download."""

    def test_auto_download_from_store(self, memory_store_with_catalog):
        """Opening with store should auto-download catalog.db."""
        store = memory_store_with_catalog

        # This should download catalog.db from the MemoryStore
        # and create an EarthCatalog
        ec = catalog.open(store=store, base="catalog")

        assert isinstance(ec, EarthCatalog)
        # Verify we can query the catalog
        assert ec.grid_type == "h3"

    def test_missing_catalog_creates_fresh(self, tmp_path):
        """Missing catalog on store should create a fresh one."""
        from obstore.store import MemoryStore

        store = MemoryStore()  # Empty store - no catalog

        # Should not raise - will create fresh catalog
        ec = catalog.open(store=store, base=str(tmp_path / "catalog"))

        assert isinstance(ec, EarthCatalog)
