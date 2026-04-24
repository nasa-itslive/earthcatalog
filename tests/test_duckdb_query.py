"""
DuckDB Iceberg query tests.

Verifies that after the pipeline writes GeoParquet files and registers them
via ``table.add_files()``, DuckDB can read the Iceberg table directly using
``iceberg_scan()`` against the metadata JSON path.

No network calls are made — the catalog lives entirely in tmp_path.
"""

import duckdb
import pytest

from earthcatalog.core.catalog import get_or_create_table, open_catalog
from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

ITEMS = [
    {
        "id": f"duck-item-{i:04d}",
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
    for i in range(4)
]


@pytest.fixture()
def populated_table(tmp_path):
    """Build and populate an Iceberg table; return (table, duckdb_connection)."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = open_catalog(db_path=db, warehouse_path=wh)
    tbl = get_or_create_table(cat)

    p = H3Partitioner(resolution=2)
    rows = fan_out(ITEMS, p)
    paths = []
    for (cell, year), group in group_by_partition(rows).items():
        year_str = str(year) if year is not None else "unknown"
        out = str(tmp_path / f"part_{cell[:12]}_{year_str}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    return tbl, con


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDuckDBQuery:
    def test_iceberg_scan_returns_rows(self, populated_table):
        """DuckDB iceberg_scan must return at least one row."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchone()
        assert rows[0] > 0

    def test_iceberg_scan_all_ids_present(self, populated_table):
        """Every source item ID must appear in the DuckDB scan result."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT DISTINCT id FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchall()
        found = {r[0] for r in rows}
        for item in ITEMS:
            assert item["id"] in found, f"{item['id']} missing from DuckDB result"

    def test_iceberg_scan_int_columns(self, populated_table):
        """percent_valid_pixels and date_dt must be readable as integers via DuckDB."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT percent_valid_pixels, date_dt "
            f"FROM iceberg_scan('{tbl.metadata_location}') LIMIT 10"
        ).fetchall()
        for pvp, dt in rows:
            if pvp is not None:
                assert isinstance(pvp, int), f"expected int, got {type(pvp)}: {pvp}"
            if dt is not None:
                assert isinstance(dt, int), f"expected int, got {type(dt)}: {dt}"

    def test_iceberg_scan_platform_column(self, populated_table):
        """platform column must be readable and non-null via DuckDB."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT DISTINCT platform FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchall()
        platforms = {r[0] for r in rows if r[0] is not None}
        assert "sentinel-2" in platforms

    def test_iceberg_scan_after_second_insert(self, populated_table, tmp_path):
        """Row count must double after a second add_files call."""
        tbl, con = populated_table
        first_count = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchone()[0]

        # Write a second chunk with the same items
        p = H3Partitioner(resolution=2)
        rows = fan_out(ITEMS, p)
        paths = []
        for (cell, year), group in group_by_partition(rows).items():
            year_str = str(year) if year is not None else "unknown"
            out = str(tmp_path / f"chunk2_{cell[:12]}_{year_str}.parquet")
            write_geoparquet(group, out)
            paths.append(out)
        tbl.add_files(paths)

        # metadata_location changes after add_files — reload
        second_count = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchone()[0]
        assert second_count == first_count * 2

    def test_iceberg_scan_sql_filter(self, populated_table):
        """DuckDB must be able to filter by a string column (platform)."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT id FROM iceberg_scan('{tbl.metadata_location}') WHERE platform = 'sentinel-2'"
        ).fetchall()
        assert len(rows) > 0

    def test_raw_stac_json_parseable_in_duckdb(self, populated_table):
        """raw_stac stored as JSON string must be parseable by DuckDB json_extract."""
        tbl, con = populated_table
        rows = con.execute(
            f"SELECT json_extract(raw_stac, '$.id') "
            f"FROM iceberg_scan('{tbl.metadata_location}') LIMIT 5"
        ).fetchall()
        assert len(rows) > 0
        for (val,) in rows:
            assert val is not None
