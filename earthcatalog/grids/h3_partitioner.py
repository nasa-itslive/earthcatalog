"""
H3 hexagonal grid partitioner.

Uses Uber's `H3 <https://h3geo.org/>`_ library to map geometries to
hexagonal grid cells at a configurable resolution.

Resolution guide
----------------
| Resolution | Avg. cell area | Global cells | Recommended use         |
|:----------:|:--------------:|:------------:|-------------------------|
| 0          | ~4,250,000 km² | 122          | Continental-scale        |
| 1          | ~607,220 km²   | 842          | **Production default**   |
| 2          | ~86,750 km²    | 5,882        | Sub-regional granularity |
| 3          | ~12,390 km²    | 41,162       | Dense urban datasets     |

Boundary-inclusive contract
----------------------------
A polygon whose edge passes through a cell — but whose interior does not
contain that cell's centroid — is still assigned to the edge cell.
:class:`H3Partitioner` achieves this by combining:

1. ``h3.geo_to_cells()`` — cells whose *center* falls inside the polygon.
2. A densified boundary walk (``_boundary_cells``) — cells touched by the
   polygon's exterior ring sampled at ~10 km spacing.

This guarantees no data gap at cell boundaries regardless of item shape.
"""

import h3
import numpy as np
from shapely import wkb
from shapely.geometry import Polygon, mapping

from earthcatalog.core.partitioner import AbstractPartitioner


def _boundary_cells(geom: object, resolution: int) -> set[str]:
    """
    Return all H3 cells touched by the polygon's exterior boundary ring.
    Densify the ring so no cell is skipped between two vertices.
    """
    coords = list(geom.exterior.coords)
    cells: set[str] = set()

    for i in range(len(coords) - 1):
        lon0, lat0 = coords[i]
        lon1, lat1 = coords[i + 1]

        # One sample per ~10 km is more than enough for any H3 resolution.
        n = max(2, int(np.hypot(lon1 - lon0, lat1 - lat0) / 0.1))
        for t in np.linspace(0, 1, n):
            lat = lat0 + t * (lat1 - lat0)
            lon = lon0 + t * (lon1 - lon0)
            cells.add(h3.latlng_to_cell(lat, lon, resolution))

    return cells


class H3Partitioner(AbstractPartitioner):
    """
    Returns every H3 cell that has ANY overlap with the input geometry:
    - Polygon/MultiPolygon: cells whose center falls inside (h3.geo_to_cells)
      UNION cells touched by the boundary (densified ring walk)
    - Point: single cell containing the point (h3.latlng_to_cell)
    """

    def __init__(self, resolution: int = 3) -> None:
        self.resolution = resolution

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)

        if geom.geom_type == "Point":
            lon, lat = geom.x, geom.y
            return [h3.latlng_to_cell(lat, lon, self.resolution)]

        interior = set(h3.geo_to_cells(mapping(geom), self.resolution))
        boundary = _boundary_cells(geom, self.resolution)
        return list(interior | boundary)

    def key_to_wkt(self, key: str) -> str:
        """Return the WKT boundary polygon for an H3 cell."""
        # h3.cell_to_boundary returns [(lat, lng), ...] pairs
        boundary_latlng = h3.cell_to_boundary(key)
        # Convert to (lng, lat) for Shapely / WKT
        coords = [(lng, lat) for lat, lng in boundary_latlng]
        return Polygon(coords).wkt
