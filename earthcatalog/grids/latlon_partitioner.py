"""
Native lat/lon rectangular grid partitioner.

A fixed-size equirectangular tile scheme: *resolution* is the cell size in
degrees (``resolution=2`` → 2°×2° tiles), and tile ids are
``r{row}c{col}`` with ``row = floor(lat / size)``, ``col = floor(lon / size)``
(e.g. a point at (-118.2, -46.3) in a 2° grid lands in ``r-23c-60``).

Boundary-inclusive: a polygon returns every tile its bounding box touches
(walked in cell-size steps), so geometries are never dropped at tile edges —
final row-level filtering happens downstream in the search engines.
"""

from __future__ import annotations

import math

from shapely import wkb

from earthcatalog.partitioner import AbstractPartitioner


class LatLonPartitioner(AbstractPartitioner):
    """Assign geometries to fixed lat/lon degree tiles."""

    def __init__(self, resolution: float = 2.0, time_bin: str = "year") -> None:
        super().__init__(time_bin=time_bin)
        if resolution <= 0:
            raise ValueError(f"resolution must be positive degrees, got {resolution}")
        self.resolution = resolution

    def tile_id(self, lon: float, lat: float) -> str:
        """Tile key for a point: ``r{row}c{col}`` from floor(coord / size)."""
        size = self.resolution
        return f"r{math.floor(lat / size)}c{math.floor(lon / size)}"

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        if geom.is_empty:
            return []

        minx, miny, maxx, maxy = geom.bounds
        size = self.resolution

        tiles: set[str] = set()
        lat = math.floor(miny / size) * size
        while lat <= maxy:
            lon = math.floor(minx / size) * size
            while lon <= maxx:
                tiles.add(self.tile_id(lon, lat))
                lon += size
            lat += size
        return sorted(tiles)
