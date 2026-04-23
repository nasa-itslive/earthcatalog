"""
Partitioner factory.

Usage:
    from earthcatalog.config import GridConfig
    from earthcatalog.grids import build_partitioner

    cfg = GridConfig(type="h3", resolution=3)
    partitioner = build_partitioner(cfg)
"""

from __future__ import annotations

from earthcatalog.config import GridConfig
from earthcatalog.core.partitioner import AbstractPartitioner


def build_partitioner(cfg: GridConfig) -> AbstractPartitioner:
    """Instantiate the correct partitioner from a GridConfig."""
    if cfg.type == "h3":
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        return H3Partitioner(resolution=cfg.resolution or 1)

    if cfg.type == "geojson":
        from earthcatalog.grids.geojson_partitioner import GeoJSONPartitioner
        if not cfg.boundaries_path:
            raise ValueError("GridConfig.boundaries_path is required for type='geojson'")
        return GeoJSONPartitioner(
            boundaries_path=cfg.boundaries_path,
            id_field=cfg.id_field or "id",
        )

    if cfg.type == "s2":
        raise NotImplementedError("S2 partitioner is not yet implemented")

    raise ValueError(f"Unknown grid type: {cfg.type!r}")
