"""
Tests for CatalogInfo — grid metadata discovery from Iceberg table properties.
"""

import pytest
from shapely.geometry import Point, Polygon, box

from earthcatalog.config import GridConfig
from earthcatalog.core.catalog import (
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
    get_or_create_table,
    open_catalog,
)
from earthcatalog.core.catalog_info import CatalogInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def h3_table(tmp_path):
    """Iceberg table created with an H3 resolution-2 grid config."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = open_catalog(db_path=db, warehouse_path=wh)
    grid_cfg = GridConfig(type="h3", resolution=2)
    table = get_or_create_table(catalog, grid_config=grid_cfg)
    return table


@pytest.fixture()
def legacy_table(tmp_path):
    """Iceberg table created WITHOUT a grid config (pre-feature catalog)."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = open_catalog(db_path=db, warehouse_path=wh)
    table = get_or_create_table(catalog, grid_config=None)
    return table


# ---------------------------------------------------------------------------
# CatalogInfo.from_table
# ---------------------------------------------------------------------------


class TestFromTable:
    def test_reads_grid_type(self, h3_table):
        info = CatalogInfo.from_table(h3_table)
        assert info.grid_type == "h3"

    def test_reads_resolution(self, h3_table):
        info = CatalogInfo.from_table(h3_table)
        assert info.grid_resolution == 2

    def test_legacy_defaults_to_h3_resolution_1(self, legacy_table):
        info = CatalogInfo.from_table(legacy_table)
        assert info.grid_type == "h3"
        assert info.grid_resolution == 1

    def test_properties_stored_in_table(self, h3_table):
        props = h3_table.properties
        assert props[PROP_GRID_TYPE] == "h3"
        assert props[PROP_GRID_RESOLUTION] == "2"

    def test_backfill_missing_properties_on_existing_table(self, tmp_path):
        """get_or_create_table called twice: second call should backfill props."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        catalog = open_catalog(db_path=db, warehouse_path=wh)
        # First call — no grid config
        get_or_create_table(catalog, grid_config=None)
        # Second call — with grid config on existing table
        grid_cfg = GridConfig(type="h3", resolution=3)
        table = get_or_create_table(catalog, grid_config=grid_cfg)
        info = CatalogInfo.from_table(table)
        assert info.grid_resolution == 3


# ---------------------------------------------------------------------------
# cells_for_geometry — H3
# ---------------------------------------------------------------------------


class TestCellsForGeometry:
    def test_point_returns_one_cell(self, h3_table):
        info = CatalogInfo.from_table(h3_table)
        pt = Point(-45.0, 70.0)  # somewhere in Greenland
        cells = info.cells_for_geometry(pt)
        assert len(cells) == 1
        assert all(isinstance(c, str) for c in cells)

    def test_polygon_returns_multiple_cells(self, h3_table):
        info = CatalogInfo.from_table(h3_table)
        bbox = box(-60, 60, -30, 80)
        cells = info.cells_for_geometry(bbox)
        assert len(cells) > 1

    def test_cells_are_valid_h3_indices(self, h3_table):
        import h3

        info = CatalogInfo.from_table(h3_table)
        bbox = box(-60, 60, -30, 80)
        for cell in info.cells_for_geometry(bbox):
            assert h3.is_valid_cell(cell), f"Invalid H3 cell: {cell}"

    def test_resolution_respected(self, tmp_path):
        """Higher resolution → more and smaller cells."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        catalog = open_catalog(db_path=db, warehouse_path=wh)
        bbox = box(-60, 60, -30, 80)

        table_r1 = get_or_create_table(catalog, grid_config=GridConfig(type="h3", resolution=1))
        cells_r1 = CatalogInfo.from_table(table_r1).cells_for_geometry(bbox)

        # Need a fresh catalog for r2 (different table would share schema)
        db2 = str(tmp_path / "catalog2.db")
        catalog2 = open_catalog(db_path=db2, warehouse_path=wh)
        table_r2 = get_or_create_table(catalog2, grid_config=GridConfig(type="h3", resolution=2))
        cells_r2 = CatalogInfo.from_table(table_r2).cells_for_geometry(bbox)

        assert len(cells_r2) > len(cells_r1)

    def test_unknown_grid_type_raises(self, tmp_path):
        info = CatalogInfo(grid_type="s2", grid_resolution=5, boundaries_path=None, id_field=None)
        with pytest.raises(ValueError, match="Unknown grid type"):
            info.cells_for_geometry(Point(0, 0))


# ---------------------------------------------------------------------------
# cell_list_sql
# ---------------------------------------------------------------------------


class TestCellListSql:
    def test_returns_in_clause(self, h3_table):
        info = CatalogInfo.from_table(h3_table)
        bbox = box(-60, 60, -30, 80)
        sql = info.cell_list_sql(bbox)
        assert sql.startswith("grid_partition IN (")
        assert "'" in sql

    def test_empty_geometry_returns_null_guard(self, h3_table):
        """A geometry that intersects no cells should produce a safe no-match clause."""
        info = CatalogInfo(grid_type="h3", grid_resolution=2, boundaries_path=None, id_field=None)
        # Tiny degenerate polygon at a pole that maps to zero cells at res 2
        # — force the empty-cells branch by monkeypatching
        original = info.cells_for_geometry
        info.cells_for_geometry = lambda g: []  # type: ignore[method-assign]
        sql = info.cell_list_sql(Point(0, 0))
        assert sql == "grid_partition IN (NULL)"
        info.cells_for_geometry = original  # restore

    def test_sql_fragment_embeddable(self, h3_table):
        """SQL fragment should be embeddable in a larger query without syntax errors."""
        info = CatalogInfo.from_table(h3_table)
        sql = info.cell_list_sql(box(-60, 60, -30, 80))
        full = f"SELECT * FROM t WHERE {sql} AND datetime > '2020-01-01'"
        # Just assert it's a valid string with expected structure
        assert "grid_partition IN (" in full
        assert "AND datetime" in full
