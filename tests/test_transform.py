"""
Tests for earthcatalog.core.transform.

Grouped by component
--------------------
TestToInt               — _to_int() helper
TestFanOut              — fan_out()
TestWriteGeoparquet     — write_geoparquet() (local filesystem)
TestWriteGeoparquetS3   — write_geoparquet_s3() (obstore — MemoryStore mock)
TestGroupByPartition    — group_by_partition()
"""

import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.core.transform import (
    FileMetadata,
    _to_int,
    fan_out,
    group_by_partition,
    write_geoparquet,
    write_geoparquet_s3,
)
from earthcatalog.grids.h3_partitioner import H3Partitioner

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

BASE_ITEM = {
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "item-0001",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-10, 60],
                [10, 60],
                [10, 70],
                [-10, 70],
                [-10, 60],
            ]
        ],
    },
    "properties": {
        "datetime": "2021-06-15T00:00:00Z",
        "platform": "sentinel-1",
        "percent_valid_pixels": 80.0,
        "date_dt": 5.0,
        "proj:code": "EPSG:32632",
        "sat:orbit_state": "ascending",
        "scene_1_id": "s1a",
        "scene_2_id": "s1b",
        "scene_1_frame": "f1",
        "scene_2_frame": "f2",
        "version": "2.0",
        "start_datetime": "2021-06-14T00:00:00Z",
        "mid_datetime": "2021-06-14T12:00:00Z",
        "end_datetime": "2021-06-15T00:00:00Z",
    },
    "links": [],
    "assets": {},
}

POINT_ITEM = {
    **BASE_ITEM,
    "id": "point-item",
    "geometry": {"type": "Point", "coordinates": [0.0, 65.0]},
}

FIVE_ITEMS = [
    {
        **BASE_ITEM,
        "id": f"item-{i:04d}",
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
            **BASE_ITEM["properties"],
            "percent_valid_pixels": float(80 + i),
            "date_dt": float(5 + i),
            "datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-15T00:00:00Z",
        },
    }
    for i in range(5)
]


@pytest.fixture()
def partitioner():
    return H3Partitioner(resolution=2)


# ---------------------------------------------------------------------------
# _to_int helper
# ---------------------------------------------------------------------------


class TestToInt:
    def test_rounds_float(self):
        assert _to_int(5.0) == 5

    def test_rounds_up(self):
        assert _to_int(5.6) == 6

    def test_none_returns_none(self):
        assert _to_int(None) is None

    def test_already_int(self):
        assert _to_int(42) == 42


# ---------------------------------------------------------------------------
# fan_out()
# ---------------------------------------------------------------------------


class TestFanOut:
    def test_produces_at_least_one_row_per_item(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        assert len(rows) >= 1

    def test_fan_out_count_greater_than_one(self, partitioner):
        """Large polygon should intersect multiple H3 cells at res=2."""
        rows = fan_out([BASE_ITEM], partitioner)
        assert len(rows) > 1

    def test_all_rows_have_same_id(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        assert all(r["id"] == "item-0001" for r in rows)

    def test_grid_partition_injected(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        for row in rows:
            assert "grid_partition" in row["properties"]
            assert isinstance(row["properties"]["grid_partition"], str)
            assert row["properties"]["grid_partition"] != ""

    def test_no_duplicate_grid_partitions_per_item(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        cells = [r["properties"]["grid_partition"] for r in rows]
        assert len(cells) == len(set(cells))

    def test_colon_sanitisation(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        props = rows[0]["properties"]
        assert "proj_code" in props
        assert "sat_orbit_state" in props
        assert "proj:code" not in props
        assert "sat:orbit_state" not in props

    def test_int_rounding(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        for row in rows:
            assert isinstance(row["properties"]["percent_valid_pixels"], int)
            assert isinstance(row["properties"]["date_dt"], int)
            assert row["properties"]["percent_valid_pixels"] == 80
            assert row["properties"]["date_dt"] == 5

    def test_raw_stac_is_json_string(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        raw = json.loads(rows[0]["properties"]["raw_stac"])
        assert raw["id"] == BASE_ITEM["id"]
        assert raw["properties"]["platform"] == "sentinel-1"

    def test_point_geometry_handled(self):
        """Point geometry should produce exactly one row (single H3 cell)."""
        p = H3Partitioner(resolution=2)
        rows = fan_out([POINT_ITEM], p)
        assert len(rows) == 1
        assert rows[0]["properties"]["grid_partition"] != ""

    def test_bad_geometry_skipped(self, partitioner):
        bad = {**BASE_ITEM, "geometry": {"type": "Polygon", "coordinates": []}}
        rows = fan_out([bad], partitioner)
        assert rows == []

    def test_multiple_items_accumulate(self, partitioner):
        rows = fan_out(FIVE_ITEMS, partitioner)
        ids = {r["id"] for r in rows}
        assert ids == {f"item-{i:04d}" for i in range(5)}

    def test_geometry_preserved_as_geojson(self, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        assert rows[0]["geometry"]["type"] == "Polygon"


# ---------------------------------------------------------------------------
# write_geoparquet()
# ---------------------------------------------------------------------------


class TestWriteGeoparquet:
    def test_file_created(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        assert (tmp_path / "out.parquet").exists()

    def test_returns_row_count(self, tmp_path, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        n = write_geoparquet(rows, str(tmp_path / "out.parquet"))
        assert n == len(rows)

    def test_empty_input_returns_zero_no_file(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        n = write_geoparquet([], out)
        assert n == 0
        assert not (tmp_path / "out.parquet").exists()

    def test_geo_metadata_present(self, tmp_path, partitioner):
        """rustac must have written the ``geo`` key in the Parquet file metadata."""
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        pf = pq.ParquetFile(out)
        assert b"geo" in pf.metadata.metadata
        geo = json.loads(pf.metadata.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"

    def test_geoarrow_extension_on_geometry(self, tmp_path, partitioner):
        """geometry column must carry the geoarrow.wkb Arrow extension type."""
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        t = pq.read_table(out)
        meta = t.schema.field("geometry").metadata
        assert meta is not None
        assert meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"

    def test_expected_columns_present(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        t = pq.read_table(out)
        required = {
            "id",
            "grid_partition",
            "geometry",
            "datetime",
            "platform",
            "proj_code",
            "sat_orbit_state",
            "percent_valid_pixels",
            "date_dt",
            "raw_stac",
        }
        assert required.issubset(set(t.schema.names))

    def test_int_fields_are_int32(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        t = pq.read_table(out)
        assert t.schema.field("percent_valid_pixels").type == pa.int32()
        assert t.schema.field("date_dt").type == pa.int32()

    def test_int_fields_not_float_values(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        t = pq.read_table(out)
        for val in t.column("percent_valid_pixels").to_pylist():
            if val is not None:
                assert isinstance(val, int)
        for val in t.column("date_dt").to_pylist():
            if val is not None:
                assert isinstance(val, int)

    def test_datetime_is_timestamp(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        t = pq.read_table(out)
        assert pa.types.is_timestamp(t.schema.field("datetime").type)

    def test_missing_optional_props_are_null(self, tmp_path, partitioner):
        minimal = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "min-001",
            "geometry": BASE_ITEM["geometry"],
            "properties": {"datetime": "2021-01-01T00:00:00Z"},
            "links": [],
            "assets": {},
        }
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([minimal], partitioner), out)
        t = pq.read_table(out)
        assert t.schema.field("platform").type == pa.string()
        for val in t.column("platform").to_pylist():
            assert val is None

    def test_no_rustac_tmp_file_left(self, tmp_path, partitioner):
        """Temporary rustac file must be cleaned up after write."""
        out = str(tmp_path / "out.parquet")
        write_geoparquet(fan_out([BASE_ITEM], partitioner), out)
        assert not (tmp_path / "out.parquet.rustac_tmp").exists()

    def test_five_item_batch(self, tmp_path, partitioner):
        out = str(tmp_path / "out.parquet")
        rows = fan_out(FIVE_ITEMS, partitioner)
        n = write_geoparquet(rows, out)
        t = pq.read_table(out)
        assert t.num_rows == n
        ids = set(t.column("id").to_pylist())
        assert ids == {f"item-{i:04d}" for i in range(5)}


# ---------------------------------------------------------------------------
# group_by_partition()
# ---------------------------------------------------------------------------


class TestGroupByPartition:
    def test_each_group_has_single_grid_partition(self, partitioner):
        """Every group must contain items with the same grid_partition value."""
        rows = fan_out([BASE_ITEM], partitioner)
        groups = group_by_partition(rows)
        for (cell, _year), items in groups.items():
            for item in items:
                assert item["properties"]["grid_partition"] == cell

    def test_each_group_has_single_year(self, partitioner):
        """Every item in a group must have the same calendar year in datetime."""
        rows = fan_out([BASE_ITEM], partitioner)
        groups = group_by_partition(rows)
        for (_cell, year), items in groups.items():
            for item in items:
                dt = item["properties"].get("datetime")
                actual_year = int(dt[:4]) if dt else None
                assert actual_year == year

    def test_group_keys_are_cell_year_tuples(self, partitioner):
        """Keys must be (str, int|None) tuples."""
        rows = fan_out([BASE_ITEM], partitioner)
        groups = group_by_partition(rows)
        for key in groups:
            cell, year = key
            assert isinstance(cell, str)
            assert year is None or isinstance(year, int)

    def test_all_rows_accounted_for(self, partitioner):
        """Sum of group sizes must equal the total fan_out row count."""
        rows = fan_out(FIVE_ITEMS, partitioner)
        groups = group_by_partition(rows)
        assert sum(len(v) for v in groups.values()) == len(rows)

    def test_multi_year_items_split_into_separate_groups(self):
        """Items from different years must land in different groups."""
        p = H3Partitioner(resolution=2)
        # Two items with identical geometry but different years
        item_2021 = {
            **BASE_ITEM,
            "id": "a",
            "properties": {**BASE_ITEM["properties"], "datetime": "2021-06-15T00:00:00Z"},
        }
        item_2024 = {
            **BASE_ITEM,
            "id": "b",
            "properties": {**BASE_ITEM["properties"], "datetime": "2024-06-15T00:00:00Z"},
        }
        rows = fan_out([item_2021, item_2024], p)
        groups = group_by_partition(rows)
        years_seen = {year for (_cell, year) in groups}
        assert 2021 in years_seen
        assert 2024 in years_seen

    def test_no_datetime_maps_to_none_year(self, partitioner):
        """Items without datetime must land in year=None groups."""
        no_dt = {**BASE_ITEM, "id": "nodatetime", "properties": {}}
        rows = fan_out([no_dt], partitioner)
        groups = group_by_partition(rows)
        assert any(year is None for (_cell, year) in groups)

    def test_within_group_sorted_by_platform_then_datetime(self):
        """Items inside each group must be sorted by (platform, datetime)."""
        p = H3Partitioner(resolution=1)  # coarse — single cell for a small polygon
        items = [
            {
                **BASE_ITEM,
                "id": f"item-{i}",
                "properties": {
                    **BASE_ITEM["properties"],
                    "platform": plat,
                    "datetime": dt,
                },
            }
            for i, (plat, dt) in enumerate(
                [
                    ("sentinel-2", "2021-03-01T00:00:00Z"),
                    ("sentinel-1", "2021-06-15T00:00:00Z"),
                    ("sentinel-1", "2021-01-01T00:00:00Z"),
                    ("sentinel-2", "2021-01-01T00:00:00Z"),
                ]
            )
        ]
        rows = fan_out(items, p)
        groups = group_by_partition(rows)
        for group_items in groups.values():
            keys = [
                (it["properties"]["platform"], it["properties"]["datetime"]) for it in group_items
            ]
            assert keys == sorted(keys), f"group not sorted: {keys}"


# ---------------------------------------------------------------------------
# write_geoparquet_s3()
# ---------------------------------------------------------------------------


class TestWriteGeoparquetS3:
    """
    Tests for write_geoparquet_s3() using an in-memory obstore MemoryStore.

    No real S3 or filesystem I/O occurs.  The MemoryStore acts as an
    in-process key/value store; ``obstore.get()`` reads bytes written by
    ``obstore.put()`` in the same test.
    """

    @pytest.fixture()
    def store(self):
        return MemoryStore()

    def test_returns_row_and_byte_count(self, store, partitioner):
        rows = fan_out([BASE_ITEM], partitioner)
        n_rows, n_bytes = write_geoparquet_s3(rows, store, "test/out.parquet")
        assert n_rows == len(rows)
        assert n_bytes > 0

    def test_empty_input_returns_zeros_no_upload(self, store):
        n_rows, n_bytes = write_geoparquet_s3([], store, "test/out.parquet")
        assert n_rows == 0
        assert n_bytes == 0
        # Nothing should have been written to the store.
        import obstore

        with pytest.raises(Exception):
            obstore.get(store, "test/out.parquet")

    def test_uploaded_file_is_readable_parquet(self, store, partitioner):
        import obstore as obs

        rows = fan_out([BASE_ITEM], partitioner)
        write_geoparquet_s3(rows, store, "test/out.parquet")
        raw = bytes(obs.get(store, "test/out.parquet").bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.num_rows == len(rows)

    def test_geo_metadata_preserved(self, store, partitioner):
        import obstore as obs

        rows = fan_out([BASE_ITEM], partitioner)
        write_geoparquet_s3(rows, store, "test/out.parquet")
        raw = bytes(obs.get(store, "test/out.parquet").bytes())
        pf = pq.ParquetFile(io.BytesIO(raw))
        assert b"geo" in pf.metadata.metadata
        geo = json.loads(pf.metadata.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"

    def test_int_fields_are_int32(self, store, partitioner):
        import obstore as obs

        rows = fan_out([BASE_ITEM], partitioner)
        write_geoparquet_s3(rows, store, "test/out.parquet")
        raw = bytes(obs.get(store, "test/out.parquet").bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.schema.field("percent_valid_pixels").type == pa.int32()
        assert tbl.schema.field("date_dt").type == pa.int32()

    def test_geoarrow_extension_on_geometry(self, store, partitioner):
        import obstore as obs

        rows = fan_out([BASE_ITEM], partitioner)
        write_geoparquet_s3(rows, store, "test/out.parquet")
        raw = bytes(obs.get(store, "test/out.parquet").bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        meta = tbl.schema.field("geometry").metadata
        assert meta is not None
        assert meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"

    def test_different_keys_are_independent(self, store, partitioner):
        """Two uploads to different keys must not interfere."""
        import obstore as obs

        rows = fan_out([BASE_ITEM], partitioner)
        write_geoparquet_s3(rows, store, "a/part1.parquet")
        write_geoparquet_s3(rows, store, "b/part2.parquet")
        raw1 = bytes(obs.get(store, "a/part1.parquet").bytes())
        raw2 = bytes(obs.get(store, "b/part2.parquet").bytes())
        t1 = pq.ParquetFile(io.BytesIO(raw1)).read()
        t2 = pq.ParquetFile(io.BytesIO(raw2)).read()
        assert t1.num_rows == t2.num_rows == len(rows)

    def test_returns_file_metadata_compatible_fields(self, store, partitioner):
        """Return values are suitable for constructing a FileMetadata."""
        rows = fan_out([BASE_ITEM], partitioner)
        key = "grid_partition=abc/year=2021/part_000000_aabbccdd.parquet"
        n_rows, n_bytes = write_geoparquet_s3(rows, store, key)
        fm = FileMetadata(
            s3_key=key,
            grid_partition="abc",
            year=2021,
            row_count=n_rows,
            file_size_bytes=n_bytes,
        )
        assert fm.row_count == len(rows)
        assert fm.file_size_bytes > 0
