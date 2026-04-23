"""
Abstract base class for spatial partitioners.

A partitioner maps a WKB geometry to one or more grid cell keys.  The
boundary-inclusive contract means that a geometry touching a cell boundary is
assigned to that cell, preventing coverage gaps along shared edges.

Built-in implementations
------------------------
- :class:`~earthcatalog.grids.h3_partitioner.H3Partitioner` — Uber H3 hexagonal grid
- :class:`~earthcatalog.grids.geojson_partitioner.GeoJSONPartitioner` — arbitrary polygon regions

Custom partitioners
-------------------
Subclass :class:`AbstractPartitioner` and implement :meth:`get_intersecting_keys`
and :meth:`key_to_wkt`, then pass an instance to
:func:`~earthcatalog.core.transform.fan_out`.
"""

from abc import ABC, abstractmethod


class AbstractPartitioner(ABC):
    """
    Given a WKB geometry, return the set of grid cell keys whose boundaries
    intersect that geometry. A single item may map to multiple keys (the
    Overlap Multiplier).
    """

    @abstractmethod
    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        """Return grid cell IDs that intersect the given WKB geometry."""
        ...

    @abstractmethod
    def key_to_wkt(self, key: str) -> str:
        """Return the WKT boundary polygon of a grid cell (useful for debugging)."""
        ...
