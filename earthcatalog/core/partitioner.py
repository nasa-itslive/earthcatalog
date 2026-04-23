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
