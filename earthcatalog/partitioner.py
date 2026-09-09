"""
Spatial + temporal partitioning.

A partitioner owns BOTH halves of the hive partition key:

- the spatial cell keys from a WKB geometry (:meth:`AbstractPartitioner.get_intersecting_keys`)
- the temporal bin value from an item datetime (:meth:`AbstractPartitioner.bin_value`)

Together they form the warehouse path segment
``grid=<type>/level=<res>/tile=<cell>/<time_bin>=<value>/``.  The
boundary-inclusive contract means that a geometry touching a cell boundary
is assigned to that cell, preventing coverage gaps along shared edges.

Built-in implementations
------------------------
- :class:`~earthcatalog.grids.h3_partitioner.H3Partitioner` — Uber H3 hexagonal grid
- :class:`~earthcatalog.grids.s2_partitioner.S2Partitioner` — Google S2 cells
- :class:`~earthcatalog.grids.utm_partitioner.UTMPartitioner` — UTM zones
- :class:`~earthcatalog.grids.geojson_partitioner.GeoJSONPartitioner` — arbitrary polygon regions

Custom partitioners
-------------------
Subclass :class:`AbstractPartitioner`, implement
:meth:`get_intersecting_keys`, accept ``time_bin`` in ``__init__`` and
forward it to ``super().__init__``, then register the builder with
:func:`~earthcatalog.grids.register_grid` (or add it to the built-in
registry in ``earthcatalog.grids``).
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime

TIME_BINS = ("year", "month", "day")


def bin_value(value: str | datetime | None, time_bin: str = "year") -> str:
    """Format a temporal value for the hive path: ``2025`` / ``2025-12`` /
    ``2025-12-20``.

    Accepts the ISO strings STAC items carry or a datetime.  Missing or
    unparseable values map to ``"unknown"`` — files without a datetime live
    in the ``unknown`` partition and index rows must agree.
    """
    if time_bin not in TIME_BINS:
        raise ValueError(f"unknown time bin: {time_bin!r}")
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        value = value.astimezone(UTC).isoformat()
    s = str(value)
    y, m, d = s[:4], s[5:7], s[8:10]
    if not (y.isdigit() and len(y) == 4):
        return "unknown"
    if time_bin == "year":
        return y
    if m.isdigit() and len(m) == 2:
        if time_bin == "month":
            return f"{y}-{m}"
        if d.isdigit() and len(d) == 2:
            return f"{y}-{m}-{d}"
    return "unknown"


class AbstractPartitioner(ABC):
    """Maps an item to its partition keys — spatial cells AND temporal bin.

    Given a WKB geometry, :meth:`get_intersecting_keys` returns the set of
    grid cell keys whose boundaries intersect that geometry (a single item
    may map to multiple keys — the Overlap Multiplier).  Given an item
    datetime, :meth:`bin_value` returns the formatted temporal bin for this
    partitioner's configured *time_bin*.
    """

    def __init__(self, time_bin: str = "year") -> None:
        if time_bin not in TIME_BINS:
            raise ValueError(f"unknown time bin: {time_bin!r}")
        self.time_bin = time_bin

    @abstractmethod
    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        """Return grid cell IDs that intersect the given WKB geometry."""
        ...

    def bin_value(self, value: str | datetime | None) -> str:
        """Temporal bin (``"2025"`` / ``"2025-12"`` / ``"2025-12-20"``) for an
        item datetime under this partitioner's *time_bin*."""
        return bin_value(value, self.time_bin)
