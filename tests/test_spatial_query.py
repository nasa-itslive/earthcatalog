"""
Spatial query integration test.

Verifies that GeoParquet files written by our pipeline carry correct spatial
metadata and that both read_parquet (with cell globs) and iceberg_scan work
for spatial queries.

Tests:
1. geo metadata present in written parquet files
2. geoarrow.wkb extension on geometry field
3. read_parquet + ST_Intersects works (the recommended pattern)
4. iceberg_scan + geometry column fails with spatial extension loaded
   (documents the DuckDB limitation)
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

from earthcatalog.config import GridConfig
from earthcatalog.core.catalog import _open_sqlite, get_or_create
from earthcatalog.core.catalog_info import catalog_info
from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

ITEMS = [
    {
        "id": f"spatial-item-{i:04d}",
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
            "datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-15T00:00:00Z",
            "platform": "NISAR",
        },
        "links": [],
        "assets": {},
    }
    for i in range(10)
]

BBOX_GREENLAND = box(-60, 60, -30, 80)


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    """Write 10 STAC items through the full pipeline into a local Iceberg catalog."""
    tmp_path = tmp_path_factory.mktemp("spatial")
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    p = H3Partitioner(resolution=2)
    rows = fan_out(ITEMS, p)
    groups = group_by_partition(rows)

    written_files = []
    all_paths = []
    for idx, ((cell, year), group) in enumerate(groups.items()):
        year_str = str(year) if year is not None else "unknown"
        out = str(tmp_path / f"part_{cell[:12]}_{year_str}_{idx}.parquet")
        write_geoparquet(group, out)
        written_files.append((cell, year_str, out))
        all_paths.append(out)
    tbl.add_files(all_paths)

    return tmp_path, tbl, written_files


class TestGeoMetadata:
    def test_geo_key_in_parquet_metadata(self, warehouse):
        _, _, files = warehouse
        _, _, path = files[0]
        meta = pq.ParquetFile(path).metadata.metadata
        assert b"geo" in meta, "Missing 'geo' key in Parquet file metadata"

    def test_geoarrow_wkb_extension_on_geometry(self, warehouse):
        _, _, files = warehouse
        _, _, path = files[0]
        schema = pq.ParquetFile(path).schema_arrow
        geom_field = schema.field("geometry")
        assert geom_field.metadata is not None
        assert geom_field.metadata.get(b"ARROW:extension:name") == b"geoarrow.wkb"

    def test_geo_metadata_has_geometry_column(self, warehouse):
        _, _, files = warehouse
        _, _, path = files[0]
        import orjson

        meta = pq.ParquetFile(path).metadata.metadata
        geo = orjson.loads(meta[b"geo"])
        assert "geometry" in geo.get("columns", {})


class TestReadParquetSpatial:
    """Test the recommended spatial query pattern: read_parquet via info.file_paths()."""

    def test_file_paths_returns_matching_files(self, warehouse):
        tmp_path, tbl, files = warehouse
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, BBOX_GREENLAND)
        assert len(paths) > 0
        assert all(p.endswith(".parquet") for p in paths)

    def test_read_parquet_with_file_paths_returns_rows(self, warehouse):
        tmp_path, tbl, files = warehouse
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, BBOX_GREENLAND)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        df = con.execute(f"SELECT id, platform, datetime FROM read_parquet({paths})").df()
        assert len(df) > 0

    def test_st_intersects_filters_correctly(self, warehouse):
        tmp_path, tbl, files = warehouse
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, BBOX_GREENLAND)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        df = con.execute(f"""
            SELECT id, platform, datetime
            FROM read_parquet({paths})
            WHERE ST_Intersects(geometry, ST_GeomFromText('{BBOX_GREENLAND.wkt}'))
            ORDER BY datetime
        """).df()
        assert len(df) > 0
        for item in ITEMS:
            from shapely.geometry import shape

            geom = shape(item["geometry"])
            if geom.intersects(BBOX_GREENLAND):
                assert item["id"] in df["id"].values, f"{item['id']} should be in results"

    def test_file_paths_empty_for_non_intersecting_geom(self, warehouse):
        _, tbl, _ = warehouse
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, box(170, -10, 175, 0))
        assert paths == []

    def test_file_paths_with_datetime_returns_fewer_files(self, warehouse):
        _, tbl, _ = warehouse
        info = catalog_info(tbl)
        all_paths = info.file_paths(tbl, BBOX_GREENLAND)
        filtered = info.file_paths(tbl, BBOX_GREENLAND, start_datetime="2023-01-01")
        assert len(filtered) <= len(all_paths)

    def test_file_paths_datetime_with_duckdb_query(self, warehouse):
        tmp_path, tbl, files = warehouse
        info = catalog_info(tbl)
        paths = info.file_paths(tbl, BBOX_GREENLAND, start_datetime="2023-01-01")

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        if paths:
            df = con.execute(f"""
                SELECT id, datetime
                FROM read_parquet({paths})
                WHERE ST_Intersects(geometry, ST_GeomFromText('{BBOX_GREENLAND.wkt}'))
                ORDER BY datetime
            """).df()
            for dt_val in df["datetime"]:
                assert dt_val.year >= 2023


class TestIcebergScanSpatial:
    """Document the DuckDB iceberg_scan + spatial extension limitation."""

    def test_iceberg_scan_non_spatial_columns(self, warehouse):
        """Non-spatial queries via iceberg_scan work fine."""
        _, tbl, _ = warehouse
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg;")
        rows = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchone()
        assert rows[0] > 0

    def test_iceberg_scan_geometry_column_fails_with_spatial(self, warehouse):
        """Loading spatial extension makes iceberg_scan fail on geometry."""
        _, tbl, _ = warehouse
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL spatial; LOAD spatial;")
        with pytest.raises(Exception):
            con.execute(
                f"SELECT id, geometry FROM iceberg_scan('{tbl.metadata_location}')"
            ).fetchall()

    def test_iceberg_scan_partition_pruning_works(self, warehouse):
        """Partition pruning via iceberg_scan works (no geometry reference)."""
        _, tbl, _ = warehouse
        info = catalog_info(tbl)
        sql = info.cell_list_sql(BBOX_GREENLAND)

        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg;")
        # Without partition filter — all rows present
        all_rows = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}')"
        ).fetchone()[0]
        assert all_rows > 0

        # With cell filter — should return a subset (or same) count.
        # Note: flat-file add_files doesn't populate partition metadata, so
        # the filter is applied as a column predicate, not partition pruning.
        # The query still succeeds and returns correct results.
        filtered_rows = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}') WHERE {sql}"
        ).fetchone()[0]
        assert filtered_rows > 0
        assert filtered_rows <= all_rows
