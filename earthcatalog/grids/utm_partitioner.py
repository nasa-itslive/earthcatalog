"""
UTM zone partitioner.

Assigns geometries to UTM zone keys (e.g. ``"11N"``, ``"32S"``).  Zone
boundaries are computed directly from longitude/latitude — no reprojection
is needed because the zone is a fixed 6-degree longitude band plus a
hemisphere letter.  A geometry spanning multiple zones returns one key per
zone it touches.
"""

from __future__ import annotations

from shapely import wkb

from earthcatalog.partitioner import AbstractPartitioner

_WORLD_WIDTH = 360.0
_ZONE_WIDTH = 6.0


def _utm_zone(lon: float, lat: float) -> str:
    """Return the UTM zone key (e.g. ``"11N"``) for a lon/lat point."""
    zone = int((lon + 180.0) // _ZONE_WIDTH) + 1
    zone = min(60, max(1, zone))
    hemi = "N" if lat >= 0 else "S"
    return f"{zone}{hemi}"


class UTMPartitioner(AbstractPartitioner):
    """Map geometries to UTM zones by their bounding box."""

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        if geom.is_empty:
            return []
        minx, miny, maxx, maxy = geom.bounds
        zones: set[str] = set()

        # Walk the bounding box in zone-width steps and add every zone the
        # box touches.  This covers polygons spanning multiple zones.
        lat_lo, lat_hi = min(miny, maxy), max(maxy, miny)
        lon = minx
        while lon <= maxx:
            # A bounding box that wraps the antimeridian would have maxx < minx;
            # treat it conservatively by also testing the far side.
            if maxx < minx:
                for extra in (minx, maxx):
                    zones.add(_utm_zone(extra, (lat_lo + lat_hi) / 2.0))
                break
            zones.add(_utm_zone(lon, (lat_lo + lat_hi) / 2.0))
            lon += _ZONE_WIDTH

        return sorted(zones)
