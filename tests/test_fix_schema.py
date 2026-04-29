"""
Tests for earthcatalog.core.fix_schema.fix_schema().

Uses a synthetic legacy parquet file (with a raw_stac column)
so the test suite has no S3 dependency.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from earthcatalog.core.fix_schema import fix_schema

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ITEM = {
    "type": "Feature",
    "stac_version": "1.1.0",
    "id": "legacy-item-0001",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[-10, 60], [10, 60], [10, 70], [-10, 70], [-10, 60]]],
    },
    "properties": {
        "datetime": "2021-06-15T00:00:00Z",
        "platform": "LT05",
        "percent_valid_pixels": 80,
        "date_dt": 32,
        "proj:code": "EPSG:3413",
        "sat:orbit_state": "ascending",
        "scene_1_id": "s1a",
        "scene_2_id": "s1b",
        "scene_1_frame": "f1",
        "scene_2_frame": "f2",
        "version": "002",
        "start_datetime": "2021-06-14T00:00:00Z",
        "grid_partition": "cell0",
    },
    "assets": {"data": {"href": "s3://bucket/item.tif", "title": "Data"}},
    "links": [{"href": "http://example.com", "rel": "canonical"}],
}


def _make_legacy_file(path: str, n: int = 3) -> list[str]:
    """Write a synthetic legacy parquet file and return the item IDs."""
    ids = [f"legacy-{i:04d}" for i in range(n)]
    rows = []
    for i, item_id in enumerate(ids):
        item = {**_ITEM, "id": item_id}
        item["properties"] = {**_ITEM["properties"], "grid_partition": f"cell{i}"}
        rows.append(json.dumps(item))

    pq.write_table(
        pa.table({"raw_stac": pa.array(rows, type=pa.string())}),
        path,
    )
    return ids


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFixSchema:
    def test_returns_correct_row_count(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        ids = _make_legacy_file(inp, n=3)
        n = fix_schema(inp, out)
        assert n == len(ids)

    def test_output_file_exists(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp)
        fix_schema(inp, out)
        assert (tmp_path / "fixed.parquet").exists()

    def test_ids_preserved(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        ids = _make_legacy_file(inp, n=3)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert set(t.column("id").to_pylist()) == set(ids)

    def test_grid_partition_preserved(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=3)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        partitions = t.column("grid_partition").to_pylist()
        assert all(p and isinstance(p, str) for p in partitions)

    def test_assets_column_present(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert "assets" in t.schema.names

    def test_links_column_present(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert "links" in t.schema.names

    def test_no_raw_stac_column(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert "raw_stac" not in t.schema.names

    def test_geo_metadata_present(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        meta = pq.read_metadata(out).metadata
        assert b"geo" in meta
        geo = json.loads(meta[b"geo"])
        assert geo["primary_column"] == "geometry"

    def test_geoarrow_extension_on_geometry(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        field_meta = t.schema.field("geometry").metadata
        assert field_meta is not None
        assert field_meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"

    def test_missing_raw_stac_raises(self, tmp_path):
        inp = str(tmp_path / "bad.parquet")
        out = str(tmp_path / "out.parquet")
        pq.write_table(pa.table({"id": pa.array(["x"])}), inp)
        with pytest.raises(ValueError, match="raw_stac"):
            fix_schema(inp, out)

    def test_compression_is_zstd(self, tmp_path):
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        meta = pq.read_metadata(out)
        for i in range(meta.row_group(0).num_columns):
            assert meta.row_group(0).column(i).compression == "ZSTD", (
                f"column {i} compression is {meta.row_group(0).column(i).compression}, expected ZSTD"
            )

    def test_assets_is_json_string_after_migration(self, tmp_path):
        """assets must be a JSON string (not a struct) after fix_schema."""
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert t.schema.field("assets").type == pa.string()
        parsed = json.loads(t.column("assets")[0].as_py())
        assert "data" in parsed

    def test_links_is_json_string_after_migration(self, tmp_path):
        """links must be a JSON string (not a list) after fix_schema."""
        inp = str(tmp_path / "legacy.parquet")
        out = str(tmp_path / "fixed.parquet")
        _make_legacy_file(inp, n=1)
        fix_schema(inp, out)
        t = pq.ParquetFile(out).read()
        assert t.schema.field("links").type == pa.string()
        parsed = json.loads(t.column("links")[0].as_py())
        assert parsed[0]["href"] == "http://example.com"
