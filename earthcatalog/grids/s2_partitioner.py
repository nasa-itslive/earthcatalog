"""
S2 cell partitioner using Google S2 (via the `s2geometry` Python bindings).

Maps geometries to S2 cell ids (token strings) at a configurable level.
Boundary-inclusive: any cell the geometry touches is returned, so there are
no coverage gaps along cell edges.
"""

from __future__ import annotations

from shapely import wkb

from earthcatalog.partitioner import AbstractPartitioner


class S2Partitioner(AbstractPartitioner):
    """Assign geometries to S2 cells at a fixed level."""

    def __init__(self, resolution: int = 2, time_bin: str = "year") -> None:
        super().__init__(time_bin=time_bin)
        self.resolution = resolution

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        import s2geometry as s2

        geom = wkb.loads(geom_wkb)
        if geom.is_empty:
            return []

        # Build an S2 lat/lng region from the geometry.
        if geom.geom_type == "Point":
            region = _point_region(geom, s2)
        else:
            region = _polygon_region(geom, s2)

        coverer = s2.S2RegionCoverer()
        coverer.set_min_level(self.resolution)
        coverer.set_max_level(self.resolution)
        coverer.set_max_cells(16)
        cell_ids = coverer.GetCovering(region)
        return [c.ToToken() for c in cell_ids]


def _point_region(geom, s2) -> object:
    lat, lon = geom.y, geom.x
    ll = s2.S2LatLng.FromDegrees(lat, lon)
    cell_id = s2.S2CellId(ll)
    return s2.S2Cell(cell_id)


def _polygon_region(geom, s2) -> object:
    from s2geometry import S2LatLngRect

    min_lon, min_lat, max_lon, max_lat = geom.bounds
    lo = s2.S2LatLng.FromDegrees(min_lat, min_lon)
    hi = s2.S2LatLng.FromDegrees(max_lat, max_lon)
    return S2LatLngRect(lo, hi)
