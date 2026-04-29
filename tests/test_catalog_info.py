"""
Tests for CatalogInfo — grid metadata discovery from Iceberg table properties.
"""

from datetime import UTC, datetime

import pytest
from shapely.geometry import Point, box

from earthcatalog.config import GridConfig
from earthcatalog.core.catalog import (
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
    get_or_create,
    open,
)
from earthcatalog.core.catalog_info import CatalogInfo, catalog_info

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def h3_table(tmp_path):
    """Iceberg table created with an H3 resolution-2 grid config."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = open(db_path=db, warehouse_path=wh)
    grid_cfg = GridConfig(type="h3", resolution=2)
    table = get_or_create(catalog, grid_config=grid_cfg)
    return table


@pytest.fixture()
def legacy_table(tmp_path):
    """Iceberg table created WITHOUT a grid config (pre-feature catalog)."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = open(db_path=db, warehouse_path=wh)
    table = get_or_create(catalog, grid_config=None)
    return table


# ---------------------------------------------------------------------------
# catalog_info
# ---------------------------------------------------------------------------


class TestFromTable:
    def test_reads_grid_type(self, h3_table):
        assert catalog_info(h3_table).grid_type == "h3"

    def test_reads_resolution(self, h3_table):
        info = catalog_info(h3_table)
        assert info.grid_resolution == 2

    def test_legacy_defaults_to_h3_resolution_1(self, legacy_table):
        info = catalog_info(legacy_table)
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
        catalog = open(db_path=db, warehouse_path=wh)
        # First call — no grid config
        get_or_create(catalog, grid_config=None)
        # Second call — with grid config on existing table
        grid_cfg = GridConfig(type="h3", resolution=3)
        table = get_or_create(catalog, grid_config=grid_cfg)
        info = catalog_info(table)
        assert info.grid_resolution == 3


# ---------------------------------------------------------------------------
# cells_for_geometry — H3
# ---------------------------------------------------------------------------


class TestCellsForGeometry:
    def test_point_returns_one_cell(self, h3_table):
        info = catalog_info(h3_table)
        pt = Point(-45.0, 70.0)  # somewhere in Greenland
        cells = info.cells_for_geometry(pt)
        assert len(cells) == 1
        assert all(isinstance(c, str) for c in cells)

    def test_polygon_returns_multiple_cells(self, h3_table):
        info = catalog_info(h3_table)
        bbox = box(-60, 60, -30, 80)
        cells = info.cells_for_geometry(bbox)
        assert len(cells) > 1

    def test_cells_are_valid_h3_indices(self, h3_table):
        import h3

        info = catalog_info(h3_table)
        bbox = box(-60, 60, -30, 80)
        for cell in info.cells_for_geometry(bbox):
            assert h3.is_valid_cell(cell), f"Invalid H3 cell: {cell}"

    def test_resolution_respected(self, tmp_path):
        """Higher resolution → more and smaller cells."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        catalog = open(db_path=db, warehouse_path=wh)
        bbox = box(-60, 60, -30, 80)

        table_r1 = get_or_create(catalog, grid_config=GridConfig(type="h3", resolution=1))
        cells_r1 = catalog_info(table_r1).cells_for_geometry(bbox)

        # Need a fresh catalog for r2 (different table would share schema)
        db2 = str(tmp_path / "catalog2.db")
        catalog2 = open(db_path=db2, warehouse_path=wh)
        table_r2 = get_or_create(catalog2, grid_config=GridConfig(type="h3", resolution=2))
        cells_r2 = catalog_info(table_r2).cells_for_geometry(bbox)

        assert len(cells_r2) > len(cells_r1)

    def test_unknown_grid_type_raises(self, tmp_path):
        info = CatalogInfo(grid_type="s2", grid_resolution=5, boundaries_path=None, id_field=None)
        with pytest.raises(ValueError, match="Unknown grid type"):
            info.cells_for_geometry(Point(0, 0))


# ---------------------------------------------------------------------------
# cell_list_sql
# ---------------------------------------------------------------------------


class TestStats:
    def test_returns_rows(self, tmp_path):
        from earthcatalog.core.catalog import get_or_create, open
        from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
        from earthcatalog.grids.h3_partitioner import H3Partitioner

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = open(db_path=db, warehouse_path=wh)
        tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

        item = {
            "id": "s-test",
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
        paths = []
        for (cell, year), group in group_by_partition(rows).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = catalog_info(tbl)
        stats = info.stats(tbl)
        assert len(stats) > 0

    def test_row_counts_match_fan_out(self, tmp_path):
        from earthcatalog.core.catalog import get_or_create, open
        from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
        from earthcatalog.grids.h3_partitioner import H3Partitioner

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = open(db_path=db, warehouse_path=wh)
        tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

        items = [
            {
                "id": f"s-{i}",
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
                "properties": {"datetime": "2022-06-15T00:00:00Z", "platform": "NISAR"},
                "links": [],
                "assets": {},
            }
            for i in range(5)
        ]
        p = H3Partitioner(resolution=2)
        rows = fan_out(items, p)
        paths = []
        for (cell, year), group in group_by_partition(rows).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = catalog_info(tbl)
        stats = info.stats(tbl)
        total = sum(s["row_count"] for s in stats)
        assert total == len(rows)

    def test_stats_have_expected_keys(self, h3_table):
        info = catalog_info(h3_table)
        for s in info.stats(h3_table):
            assert "grid_partition" in s
            assert "year" in s
            assert "row_count" in s
            assert "file_count" in s
            assert "total_bytes" in s

    def test_year_is_calendar_year(self, tmp_path):
        from earthcatalog.core.catalog import get_or_create, open
        from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
        from earthcatalog.grids.h3_partitioner import H3Partitioner

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = open(db_path=db, warehouse_path=wh)
        tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

        item = {
            "id": "yr-test",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-50, 65], [-48, 65], [-48, 68], [-50, 68], [-50, 65]]],
            },
            "properties": {"datetime": "2023-06-15T00:00:00Z", "platform": "NISAR"},
            "links": [],
            "assets": {},
        }
        p = H3Partitioner(resolution=2)
        rows = fan_out([item], p)
        paths = []
        for (cell, year), group in group_by_partition(rows).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = catalog_info(tbl)
        years = {s["year"] for s in info.stats(tbl)}
        assert 2023 in years
        assert all(y >= 1970 for y in years)

    def test_returns_in_clause(self, h3_table):
        info = catalog_info(h3_table)
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
        info = catalog_info(h3_table)
        sql = info.cell_list_sql(box(-60, 60, -30, 80))
        full = f"SELECT * FROM t WHERE {sql} AND datetime > '2020-01-01'"
        assert "grid_partition IN (" in full
        assert "AND datetime" in full


# ---------------------------------------------------------------------------
# file_paths — datetime filtering
# ---------------------------------------------------------------------------


def _build_multiyear_warehouse(tmp_path, years):
    from earthcatalog.core.catalog import get_or_create, open
    from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
    from earthcatalog.grids.h3_partitioner import H3Partitioner

    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = open(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    items = []
    for i, year in enumerate(years):
        items.append(
            {
                "id": f"dt-test-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-50, 65], [-48, 65], [-48, 68], [-50, 68], [-50, 65]]],
                },
                "properties": {"datetime": f"{year}-06-15T00:00:00Z", "platform": "NISAR"},
                "links": [],
                "assets": {},
            }
        )

    p = H3Partitioner(resolution=2)
    rows = fan_out(items, p)
    paths = []
    for idx, ((cell, year), group) in enumerate(group_by_partition(rows).items()):
        out = str(tmp_path / f"part_{cell}_{year}_{idx}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)
    return tbl


class TestFilePathsDatetime:
    def test_no_datetime_returns_all_files(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        all_paths = info.file_paths(tbl, pt)
        filtered = info.file_paths(tbl, pt, start_datetime="2022-01-01")
        assert len(filtered) < len(all_paths)

    def test_start_datetime_prunes_earlier_years(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, start_datetime="2022-01-01")
        years_in_result = set()
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                years_in_result.add(task.file.partition[1] + 1970)
        for y in years_in_result:
            assert y >= 2022

    def test_end_datetime_prunes_later_years(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, end_datetime="2021-12-31T23:59:59Z")
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                assert task.file.partition[1] + 1970 <= 2021

    def test_both_datetimes_narrows_range(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, start_datetime="2021-06-01", end_datetime="2022-06-01")
        years_in_result = set()
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                years_in_result.add(task.file.partition[1] + 1970)
        for y in years_in_result:
            assert 2021 <= y <= 2022

    def test_string_and_datetime_both_work(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths_str = info.file_paths(tbl, pt, start_datetime="2022-01-01")
        paths_dt = info.file_paths(tbl, pt, start_datetime=datetime(2022, 1, 1, tzinfo=UTC))
        assert set(paths_str) == set(paths_dt)

    def test_naive_datetime_gets_utc(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, start_datetime=datetime(2022, 1, 1))
        years_in_result = {
            task.file.partition[1] + 1970
            for task in tbl.scan().plan_files()
            if task.file.file_path in paths
        }
        for y in years_in_result:
            assert y >= 2022

    def test_empty_cells_returns_empty_with_datetime(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, box(170, -10, 175, 0), start_datetime="2022-01-01")
        assert paths == []
