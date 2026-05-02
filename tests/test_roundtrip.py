"""
Round-trip integration test: fan_out + write_geoparquet → add_files → scan.

Uses a fully in-process SQLite catalog (tmp_path) — no S3, no network.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from earthcatalog.core.catalog import _open_sqlite, get_or_create
from earthcatalog.core.transform import fan_out, group_by_partition, write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

ITEMS = [
    {
        "id": f"item-{i:04d}",
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
            "platform": "sentinel-1",
            "percent_valid_pixels": float(80 + i),
            "date_dt": float(10 + i),
            "proj:code": "EPSG:32632",
            "sat:orbit_state": "ascending",
        },
        "links": [{"href": "http://example.com", "rel": "canonical"}],
        "assets": {"data": {"href": f"s3://bucket/item-{i:04d}.tif", "title": f"Data {i}"}},
    }
    for i in range(5)
]


@pytest.fixture()
def iceberg_table(tmp_path):
    db = str(tmp_path / "catalog.db")
    warehouse = str(tmp_path / "warehouse")
    catalog = _open_sqlite(db_path=db, warehouse_path=warehouse)
    return get_or_create(catalog), tmp_path


def _write_and_add(iceberg_table_fixture, items, resolution=2):
    """Helper: fan_out → group_by_partition → write one file per group → add_files."""
    table, tmp_path = iceberg_table_fixture
    p = H3Partitioner(resolution=resolution)
    rows = fan_out(items, p)
    paths = []
    for (cell, year), group in group_by_partition(rows).items():
        year_str = str(year) if year is not None else "unknown"
        out = str(tmp_path / f"part_{cell[:12]}_{year_str}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    table.add_files(paths)
    return table


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRoundTrip:
    def test_append_and_scan_row_count(self, iceberg_table, tmp_path):
        """Rows written must be readable back via PyIceberg scan."""
        table, _ = iceberg_table
        p = H3Partitioner(resolution=2)
        rows = fan_out(ITEMS, p)
        total_n = 0
        paths = []
        for (cell, year), group in group_by_partition(rows).items():
            year_str = str(year) if year is not None else "unknown"
            out = str(tmp_path / f"part_{cell[:12]}_{year_str}.parquet")
            total_n += write_geoparquet(group, out)
            paths.append(out)
        table.add_files(paths)

        result = table.scan().to_arrow()
        assert result.num_rows == total_n

    def test_fan_out_preserved(self, iceberg_table, tmp_path):
        """Each source item must appear as >= 1 row."""
        table = _write_and_add(iceberg_table, ITEMS)
        result = table.scan().to_arrow()
        ids = set(result.column("id").to_pylist())
        for item in ITEMS:
            assert item["id"] in ids

    def test_schema_preserved(self, iceberg_table, tmp_path):
        """Key column types must survive the write/read cycle."""
        table = _write_and_add(iceberg_table, ITEMS)
        result = table.scan().to_arrow()
        assert result.schema.field("id").type == pa.string()
        assert result.schema.field("percent_valid_pixels").type == pa.int64()
        assert result.schema.field("date_dt").type == pa.int64()
        # geometry is binary (WKB) — may come back as binary or large_binary
        assert result.schema.field("geometry").type in (pa.binary(), pa.large_binary())

    def test_int_fields_not_float(self, iceberg_table, tmp_path):
        """percent_valid_pixels and date_dt must be integers, not floats."""
        table = _write_and_add(iceberg_table, ITEMS)
        result = table.scan().to_arrow()
        for val in result.column("percent_valid_pixels").to_pylist():
            if val is not None:
                assert isinstance(val, int)
        for val in result.column("date_dt").to_pylist():
            if val is not None:
                assert isinstance(val, int)

    def test_multiple_chunks_accumulate(self, iceberg_table, tmp_path):
        """Two add_files calls must produce two snapshots and doubled rows."""
        table, _ = iceberg_table
        p = H3Partitioner(resolution=2)

        def _write_chunk(suffix):
            rows = fan_out(ITEMS, p)
            paths = []
            total = 0
            for (cell, year), group in group_by_partition(rows).items():
                year_str = str(year) if year is not None else "unknown"
                out = str(tmp_path / f"{suffix}_{cell[:12]}_{year_str}.parquet")
                total += write_geoparquet(group, out)
                paths.append(out)
            return paths, total

        paths1, n = _write_chunk("chunk1")
        paths2, _ = _write_chunk("chunk2")
        table.add_files(paths1)
        table.add_files(paths2)

        assert len(table.history()) == 2
        result = table.scan().to_arrow()
        assert result.num_rows == n * 2

    def test_assets_links_are_json_strings(self, iceberg_table, tmp_path):
        """assets and links columns must be JSON strings (not structs/lists)."""
        table = _write_and_add(iceberg_table, [ITEMS[0]])
        result = table.scan().to_arrow()
        assert result.schema.field("assets").type == pa.string()
        assert result.schema.field("links").type == pa.string()
        assets = json.loads(result.column("assets")[0].as_py())
        assert "data" in assets
        links = json.loads(result.column("links")[0].as_py())
        assert links[0]["href"] == "http://example.com"

    def test_grid_partition_column_populated(self, iceberg_table, tmp_path):
        """Every row must have a non-null, non-empty grid_partition."""
        table = _write_and_add(iceberg_table, ITEMS)
        result = table.scan().to_arrow()
        partitions = result.column("grid_partition").to_pylist()
        assert all(p and isinstance(p, str) for p in partitions)

    def test_single_item_insert_zero_to_n(self, iceberg_table, tmp_path):
        """Table starts at 0 rows; after inserting one item row count equals fan-out size."""
        table, _ = iceberg_table
        # Confirm truly empty before insert
        assert table.scan().to_arrow().num_rows == 0

        p = H3Partitioner(resolution=2)
        rows = fan_out([ITEMS[0]], p)
        paths = []
        n = 0
        for (cell, year), group in group_by_partition(rows).items():
            year_str = str(year) if year is not None else "unknown"
            out = str(tmp_path / f"single_{cell[:12]}_{year_str}.parquet")
            n += write_geoparquet(group, out)
            paths.append(out)
        assert n > 0, "fan_out produced no rows for a valid item"

        table.add_files(paths)
        result = table.scan().to_arrow()
        assert result.num_rows == n
        assert result.column("id")[0].as_py() == ITEMS[0]["id"]

    def test_geoparquet_file_has_geo_metadata(self, tmp_path):
        """The written Parquet file must carry the ``geo`` key from rustac."""
        p = H3Partitioner(resolution=2)
        rows = fan_out(ITEMS, p)
        out = str(tmp_path / "chunk.parquet")
        write_geoparquet(rows, out)
        pf = pq.ParquetFile(out)
        assert b"geo" in pf.metadata.metadata
