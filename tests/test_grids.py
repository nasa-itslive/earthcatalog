"""Tests for S2 and UTM partitioners."""

from __future__ import annotations

from shapely import wkb
from shapely.geometry import Point

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
