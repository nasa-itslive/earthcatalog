"""
Tests for CatalogInfo — grid metadata discovery from Iceberg table properties.
"""

from datetime import UTC, datetime

import pytest
from shapely.geometry import Point, box

from earthcatalog.catalog import (
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
    CatalogInfo,
    _catalog_info,
    _open_sqlite,
    get_or_create,
)
from earthcatalog.config import GridConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def h3_table(tmp_path):
    """Iceberg table created with an H3 resolution-2 grid config."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = _open_sqlite(db_path=db, warehouse_path=wh)
    grid_cfg = GridConfig(type="h3", resolution=2)
    table = get_or_create(catalog, grid_config=grid_cfg)
    return table


@pytest.fixture()
def legacy_table(tmp_path):
    """Iceberg table created WITHOUT a grid config (pre-feature catalog)."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = _open_sqlite(db_path=db, warehouse_path=wh)
    table = get_or_create(catalog, grid_config=None)
    return table


# ---------------------------------------------------------------------------
# catalog_info
# ---------------------------------------------------------------------------


class TestFromTable:
    def test_reads_grid_type(self, h3_table):
        assert _catalog_info(h3_table).grid_type == "h3"

    def test_reads_resolution(self, h3_table):
        info = _catalog_info(h3_table)
        assert info.grid_resolution == 2

    def test_legacy_defaults_to_h3_resolution_1(self, legacy_table):
        info = _catalog_info(legacy_table)
        assert info.grid_type == "h3"
        assert info.grid_resolution == 1

    def test_properties_stored_in_table(self, h3_table):
        props = h3_table.properties
        assert props[PROP_GRID_TYPE] == "h3"
        assert props[PROP_GRID_RESOLUTION] == "2"

    def test_backfill_missing_properties_on_existing_table(self, tmp_path):
        """get_or_create called twice: second call should backfill props."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        catalog = _open_sqlite(db_path=db, warehouse_path=wh)
        # First call — no grid config
        get_or_create(catalog, grid_config=None)
        # Second call — with grid config on existing table
        grid_cfg = GridConfig(type="h3", resolution=3)
        table = get_or_create(catalog, grid_config=grid_cfg)
        info = _catalog_info(table)
        assert info.grid_resolution == 3


# ---------------------------------------------------------------------------
# cells_for_geometry — H3
# ---------------------------------------------------------------------------


class TestCellsForGeometry:
    def test_point_returns_one_cell(self, h3_table):
        info = _catalog_info(h3_table)
        pt = Point(-45.0, 70.0)  # somewhere in Greenland
        cells = info.cells_for_geometry(pt)
        assert len(cells) == 1
        assert all(isinstance(c, str) for c in cells)

    def test_polygon_returns_multiple_cells(self, h3_table):
        info = _catalog_info(h3_table)
        bbox = box(-60, 60, -30, 80)
        cells = info.cells_for_geometry(bbox)
        assert len(cells) > 1

    def test_cells_are_valid_h3_indices(self, h3_table):
        import h3

        info = _catalog_info(h3_table)
        bbox = box(-60, 60, -30, 80)
        for cell in info.cells_for_geometry(bbox):
            assert h3.is_valid_cell(cell), f"Invalid H3 cell: {cell}"

    def test_resolution_respected(self, tmp_path):
        """Higher resolution → more and smaller cells."""
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        catalog = _open_sqlite(db_path=db, warehouse_path=wh)
        bbox = box(-60, 60, -30, 80)

        table_r1 = get_or_create(catalog, grid_config=GridConfig(type="h3", resolution=1))
        cells_r1 = _catalog_info(table_r1).cells_for_geometry(bbox)

        # Need a fresh catalog for r2 (different table would share schema)
        db2 = str(tmp_path / "catalog2.db")
        catalog2 = _open_sqlite(db_path=db2, warehouse_path=wh)
        table_r2 = get_or_create(catalog2, grid_config=GridConfig(type="h3", resolution=2))
        cells_r2 = _catalog_info(table_r2).cells_for_geometry(bbox)

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
        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
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
        for (cell, year), group in group_by_partition(rows, p).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = _catalog_info(tbl)
        stats = info.stats(tbl)
        assert len(stats) > 0

    def test_row_counts_match_fan_out(self, tmp_path):
        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
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
        for (cell, year), group in group_by_partition(rows, p).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = _catalog_info(tbl)
        stats = info.stats(tbl)
        total = sum(s["row_count"] for s in stats)
        assert total == len(rows)

    def test_stats_have_expected_keys(self, h3_table):
        info = _catalog_info(h3_table)
        for s in info.stats(h3_table):
            assert "grid_partition" in s
            assert "year" in s
            assert "row_count" in s
            assert "file_count" in s
            assert "total_bytes" in s

    def test_year_is_calendar_year(self, tmp_path):
        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
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
        for (cell, year), group in group_by_partition(rows, p).items():
            out = str(tmp_path / f"part_{cell}_{year}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        info = _catalog_info(tbl)
        years = {s["year"] for s in info.stats(tbl)}
        assert 2023 in years
        assert all(y >= 1970 for y in years)

    def test_returns_in_clause(self, h3_table):
        info = _catalog_info(h3_table)
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
        info = _catalog_info(h3_table)
        sql = info.cell_list_sql(box(-60, 60, -30, 80))
        full = f"SELECT * FROM t WHERE {sql} AND datetime > '2020-01-01'"
        assert "grid_partition IN (" in full
        assert "AND datetime" in full


# ---------------------------------------------------------------------------
# file_paths — datetime filtering
# ---------------------------------------------------------------------------


def _build_multiyear_warehouse(tmp_path, years):
    from earthcatalog.catalog import _open_sqlite, get_or_create
    from earthcatalog.grids.h3_partitioner import H3Partitioner
    from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
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
                "properties": {
                    "datetime": f"{year}-06-15T00:00:00Z",
                    "start_datetime": f"{year}-01-01T00:00:00Z",
                    "end_datetime": f"{year}-12-31T23:59:59Z",
                    "platform": "NISAR",
                },
                "links": [],
                "assets": {},
            }
        )

    p = H3Partitioner(resolution=2)
    rows = fan_out(items, p)
    paths = []
    for idx, ((cell, year), group) in enumerate(group_by_partition(rows, p).items()):
        out = str(tmp_path / f"part_{cell}_{year}_{idx}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)
    return tbl


class TestFilePathsDatetime:
    @pytest.mark.parametrize(
        ("filter_kwargs", "check_year_fn"),
        [
            ({"start_datetime": "2022-01-01"}, lambda y: y >= 2022),
            ({"end_datetime": "2021-12-31T23:59:59Z"}, lambda y: y <= 2021),
            (
                {"start_datetime": "2021-06-01", "end_datetime": "2022-06-01"},
                lambda y: 2021 <= y <= 2022,
            ),
        ],
    )
    def test_datetime_filters(self, tmp_path, filter_kwargs, check_year_fn):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = _catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, **filter_kwargs)
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                assert check_year_fn(task.file.partition[1] + 1970)

    def test_no_datetime_has_fewer_with_filter(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = _catalog_info(tbl)
        pt = Point(-49, 66.5)
        all_paths = info.file_paths(tbl, pt)
        filtered = info.file_paths(tbl, pt, start_datetime="2022-01-01")
        assert len(filtered) < len(all_paths)

    def test_string_and_datetime_both_work(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = _catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths_str = info.file_paths(tbl, pt, start_datetime="2022-01-01")
        paths_dt = info.file_paths(tbl, pt, start_datetime=datetime(2022, 1, 1, tzinfo=UTC))
        assert set(paths_str) == set(paths_dt)

    def test_naive_datetime_gets_utc(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = _catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, start_datetime=datetime(2022, 1, 1))
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                assert task.file.partition[1] + 1970 >= 2022

    def test_empty_cells_returns_empty_with_datetime(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = _catalog_info(tbl)
        paths = info.file_paths(tbl, box(170, -10, 175, 0), start_datetime="2022-01-01")
        assert paths == []

    @pytest.mark.parametrize("year_str", ["2021-01", "2022"])
    def test_year_formats(self, tmp_path, year_str):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])
        info = _catalog_info(tbl)
        pt = Point(-49, 66.5)
        paths = info.file_paths(tbl, pt, start_datetime=year_str)
        for task in tbl.scan().plan_files():
            if task.file.file_path in paths:
                assert task.file.partition[1] + 1970 >= 2021


# ---------------------------------------------------------------------------
# Datetime parsing
# ---------------------------------------------------------------------------


class TestDatetimeParsing:
    """Test flexible datetime parsing for search_files."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            ("2020-06-15", (2020, 6, 15, 0, 0)),
            ("2020-06-15T10:30:00Z", (2020, 6, 15, 10, 30)),
            ("2020-06", (2020, 6, 1, 0, 0)),
            ("2020", (2020, 1, 1, 0, 0)),
            (datetime(2020, 6, 15, 10, 30), (2020, 6, 15, 10, 30)),
            (datetime(2020, 6, 15, 10, 30, tzinfo=UTC), None),
        ],
    )
    def test_parse_dt(self, input_val, expected):
        from earthcatalog.catalog import _parse_dt

        dt = _parse_dt(input_val)
        if expected is None:
            assert dt.tzinfo == UTC
        else:
            assert (dt.year, dt.month, dt.day, dt.hour, dt.minute) == expected
            assert dt.tzinfo == UTC

    def test_parse_dt_invalid(self):
        from earthcatalog.catalog import _parse_dt

        with pytest.raises(ValueError, match="Unable to parse datetime"):
            _parse_dt("not-a-date")
        with pytest.raises(ValueError, match="Unable to parse datetime"):
            _parse_dt("2020-13")


# ---------------------------------------------------------------------------
# total_files, unique_item_count, top_cells
# ---------------------------------------------------------------------------


class TestCatalogInfoStatsMethods:
    """Test new statistics methods on CatalogInfo."""

    @pytest.mark.parametrize("populated", [True, False])
    def test_total_files(self, tmp_path, h3_table, populated):
        tbl = _build_multiyear_warehouse(tmp_path, [2020]) if populated else h3_table
        info = _catalog_info(tbl)
        count = info.total_files(tbl)
        if populated:
            assert count > 0
        else:
            assert count == 0

    def test_unique_item_count_no_hash_index(self, h3_table):
        """unique_item_count should return 0 when no index is available."""
        info = _catalog_info(h3_table)
        count = info.unique_item_count(h3_table, store=None)
        assert count == 0

    def test_unique_item_count_with_index(self, tmp_path):
        """unique_item_count should count active rows from the unified index."""
        from obstore.store import LocalStore

        from earthcatalog.index import Index

        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])

        index_path = str(tmp_path / "warehouse_index.parquet")
        store = LocalStore(str(tmp_path))
        Index(store, "warehouse_index.parquet").append(
            [
                {
                    "s3_key": f"s3://b/k{i}.stac.json",
                    "stac_id": f"item-{i}",
                    "grid_partition": "cellA",
                    "year": 2020,
                }
                for i in range(10)
            ]
        )

        # Set the index path in table properties.
        with tbl.transaction() as tx:
            tx.set_properties(**{"earthcatalog.index_path": index_path})

        info = _catalog_info(tbl)
        count = info.unique_item_count(tbl, store=None)
        assert count == 10

    def test_unique_item_count_local_file_not_found(self, tmp_path):
        """unique_item_count should return 0 when file doesn't exist."""
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])

        # Set non-existent index path
        with tbl.transaction() as tx:
            tx.set_properties(
                **{"earthcatalog.hash_index_path": str(tmp_path / "nonexistent.parquet")}
            )

        info = _catalog_info(tbl)
        count = info.unique_item_count(tbl, store=None)
        assert count == 0

    def test_unique_item_count_uses_default_path(self, tmp_path):
        """unique_item_count should use default_index_path when table property is not set."""
        from obstore.store import LocalStore

        from earthcatalog.index import Index

        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022])

        index_path = str(tmp_path / "warehouse_index.parquet")
        Index(LocalStore(str(tmp_path)), "warehouse_index.parquet").append(
            [
                {
                    "s3_key": f"s3://b/k{i}.stac.json",
                    "stac_id": f"item-{i}",
                    "grid_partition": "cellA",
                    "year": 2020,
                }
                for i in range(42)
            ]
        )

        # get_or_create stamps earthcatalog.index_path automatically; drop it
        # to simulate a warehouse with no known index location.
        from earthcatalog.schema import PROP_INDEX_PATH

        with tbl.transaction() as tx:
            tx.remove_properties(PROP_INDEX_PATH)
        info = _catalog_info(tbl)

        # Without property or default path, should return 0
        count_no_default = info.unique_item_count(tbl, store=None)
        assert count_no_default == 0

        # With default path, should return the actual count
        count_with_default = info.unique_item_count(tbl, store=None, default_index_path=index_path)
        assert count_with_default == 42

    def test_top_cells_sorted_and_cached(self, tmp_path):
        tbl = _build_multiyear_warehouse(tmp_path, [2020, 2021, 2022, 2023])
        info = _catalog_info(tbl)

        top3 = info.top_cells(tbl, limit=3)
        assert len(top3) <= 3
        for i in range(len(top3) - 1):
            assert top3[i]["row_count"] >= top3[i + 1]["row_count"]
        for cell in top3:
            assert "grid_partition" in cell
            assert "row_count" in cell
            assert "file_count" in cell

        top10 = info.top_cells(tbl, limit=10)
        assert len(top10) >= len(top3)

        top2 = info.top_cells(tbl, limit=5)
        assert top2 == top3  # cache hit
        assert info._cached_top_cells is not None
