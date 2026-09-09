"""Tests for S2, UTM, and lat/lon partitioners."""

from __future__ import annotations

from shapely import wkb
from shapely.geometry import Point

from earthcatalog.grids.latlon_partitioner import LatLonPartitioner
from earthcatalog.grids.s2_partitioner import S2Partitioner
from earthcatalog.grids.utm_partitioner import UTMPartitioner


class TestS2Partitioner:
    def test_point_maps_to_single_cell(self):
        p = S2Partitioner(resolution=2)
        geom = Point(-100.0, 75.0)
        keys = p.get_intersecting_keys(wkb.dumps(geom))
        assert len(keys) == 1
        # S2 tokens are hex; resolution-2 cells have 3-char tokens.
        assert all(len(k) > 1 for k in keys)

    def test_polygon_maps_to_one_or_more_cells(self):
        from shapely.geometry import box

        p = S2Partitioner(resolution=2)
        geom = box(-100, 70, -80, 80)
        keys = p.get_intersecting_keys(wkb.dumps(geom))
        assert len(keys) >= 1

    def test_resolution_changes_cell_count(self):
        from shapely.geometry import box

        coarse = S2Partitioner(resolution=1)
        fine = S2Partitioner(resolution=3)
        geom = box(-100, 70, -80, 80)
        coarse_keys = coarse.get_intersecting_keys(wkb.dumps(geom))
        fine_keys = fine.get_intersecting_keys(wkb.dumps(geom))
        assert len(fine_keys) >= len(coarse_keys)


class TestUTMPartitioner:
    def test_point_in_zone(self):
        p = UTMPartitioner()
        # lon=-100, lat=70 -> UTM zone 14N (zone 14 covers -102..-96)
        geom = Point(-100.0, 70.0)
        keys = p.get_intersecting_keys(wkb.dumps(geom))
        assert keys == ["14N"]

    def test_southern_hemisphere(self):
        p = UTMPartitioner()
        geom = Point(-100.0, -70.0)
        keys = p.get_intersecting_keys(wkb.dumps(geom))
        assert keys == ["14S"]

    def test_polygon_spans_zones(self):
        from shapely.geometry import box

        p = UTMPartitioner()
        # lon -100..-60 -> zones 14 through 20 at lat 70
        geom = box(-100, 70, -60, 71)
        keys = set(p.get_intersecting_keys(wkb.dumps(geom)))
        assert keys >= {"14N", "17N", "20N"}


class TestLatLonPartitioner:
    def test_point_tile_id(self):
        p = LatLonPartitioner(resolution=2)
        # floor(-118.2/2)=-60, floor(-46.3/2)=-24? No: -46.3/2=-23.15 → -24
        assert p.tile_id(-118.2, -46.3) == "r-24c-60"

    def test_point_on_positive_side(self):
        p = LatLonPartitioner(resolution=2)
        assert p.tile_id(10.5, 20.5) == "r10c5"

    def test_point_returns_single_tile(self):
        p = LatLonPartitioner(resolution=2)
        keys = p.get_intersecting_keys(wkb.dumps(Point(-118.2, -46.3)))
        assert keys == ["r-24c-60"]

    def test_polygon_spans_multiple_tiles(self):
        from shapely.geometry import box

        p = LatLonPartitioner(resolution=2)
        keys = set(p.get_intersecting_keys(wkb.dumps(box(-101.0, 69.0, -99.0, 71.0))))
        # 2° bbox in a 2° grid touches exactly 2x2 = 4 tiles
        assert keys == {"r34c-51", "r34c-50", "r35c-51", "r35c-50"}

    def test_boundary_inclusive(self):
        """A geometry whose edge lies on a tile boundary touches both tiles."""
        from shapely.geometry import box

        p = LatLonPartitioner(resolution=2)
        keys = set(p.get_intersecting_keys(wkb.dumps(box(-120.0, 70.0, -118.0, 72.0))))
        assert "r35c-60" in keys and "r36c-60" in keys and "r35c-59" in keys

    def test_fractional_resolution(self):
        from shapely.geometry import box

        p = LatLonPartitioner(resolution=0.5)
        keys = p.get_intersecting_keys(wkb.dumps(box(0.0, 0.0, 0.99, 0.99)))
        assert len(keys) == 4

    def test_exact_boundary_touches_next_tile(self):
        """A 1° box in a 0.5° grid with edges exactly on boundaries also
        touches the tiles that start at its max corner (boundary-inclusive)."""
        from shapely.geometry import box

        p = LatLonPartitioner(resolution=0.5)
        keys = set(p.get_intersecting_keys(wkb.dumps(box(0.0, 0.0, 1.0, 1.0))))
        assert keys == {
            "r0c0", "r0c1", "r0c2",
            "r1c0", "r1c1", "r1c2",
            "r2c0", "r2c1", "r2c2",
        }

    def test_bad_resolution_rejected(self):
        import pytest

        with pytest.raises(ValueError):
            LatLonPartitioner(resolution=0)
