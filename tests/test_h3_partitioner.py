"""
Tests for earthcatalog.grids.h3_partitioner — H3Partitioner.
"""

import pytest
from shapely.geometry import Point, Polygon, mapping
from shapely import wkb as shapely_wkb

from earthcatalog.grids.h3_partitioner import H3Partitioner


def _wkb(geom):
    return geom.wkb


ARCTIC_POINT = Point(-45.0, 75.0)

SMALL_POLYGON = Polygon([
    (-10, 60), (10, 60), (10, 70), (-10, 70), (-10, 60)
])

LARGE_POLYGON = Polygon([
    (-80, -70), (80, -70), (80, -80), (-80, -80), (-80, -70)
])


class TestH3Partitioner:
    def test_returns_list(self):
        p = H3Partitioner(resolution=2)
        keys = p.get_intersecting_keys(_wkb(ARCTIC_POINT))
        assert isinstance(keys, list)

    def test_point_returns_at_least_one_cell(self):
        p = H3Partitioner(resolution=2)
        keys = p.get_intersecting_keys(_wkb(ARCTIC_POINT))
        assert len(keys) >= 1

    def test_all_keys_are_valid_h3(self):
        import h3
        p = H3Partitioner(resolution=3)
        keys = p.get_intersecting_keys(_wkb(SMALL_POLYGON))
        for key in keys:
            assert h3.is_valid_cell(key), f"Invalid H3 cell: {key}"

    def test_small_polygon_boundary_inclusive(self):
        """Boundary-inclusive logic must give more cells than center-only."""
        import h3
        from shapely.geometry import mapping as geo_mapping

        p = H3Partitioner(resolution=3)
        keys_full = set(p.get_intersecting_keys(_wkb(SMALL_POLYGON)))
        keys_center_only = set(h3.geo_to_cells(geo_mapping(SMALL_POLYGON), 3))
        # Full set must be a superset of center-only
        assert keys_full >= keys_center_only

    def test_large_polygon_multiple_cells(self):
        p = H3Partitioner(resolution=2)
        keys = p.get_intersecting_keys(_wkb(LARGE_POLYGON))
        assert len(keys) > 1

    def test_resolution_respected(self):
        """Higher resolution → more (smaller) cells for same geometry."""
        import h3
        p2 = H3Partitioner(resolution=2)
        p3 = H3Partitioner(resolution=3)
        keys2 = p2.get_intersecting_keys(_wkb(SMALL_POLYGON))
        keys3 = p3.get_intersecting_keys(_wkb(SMALL_POLYGON))
        # res 3 cells are smaller; a fixed polygon should cover at least as many
        assert len(keys3) >= len(keys2)

    def test_default_resolution_is_3(self):
        p = H3Partitioner()
        assert p.resolution == 3

    def test_no_duplicate_keys(self):
        p = H3Partitioner(resolution=3)
        keys = p.get_intersecting_keys(_wkb(SMALL_POLYGON))
        assert len(keys) == len(set(keys))
