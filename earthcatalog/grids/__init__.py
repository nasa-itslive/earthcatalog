"""
Partitioner factory and grid registry.

Usage:
    from earthcatalog.config import GridConfig
    from earthcatalog.grids import build_partitioner

    cfg = GridConfig(type="h3", resolution=3, time_bin="month")
    partitioner = build_partitioner(cfg)

The factory passes ``cfg.time_bin`` into every partitioner — the built
object owns both the spatial keys and the temporal bin.  New grids are
added by registering a builder (no factory edits needed):

    from earthcatalog.grids import register_grid

    register_grid("my_grid", lambda cfg, **kw: MyPartitioner(resolution=cfg.resolution, **kw))
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from earthcatalog.config import GridConfig
from earthcatalog.partitioner import AbstractPartitioner


def _build_h3(cfg: GridConfig, **kwargs: Any) -> AbstractPartitioner:
    from earthcatalog.grids.h3_partitioner import H3Partitioner

    return H3Partitioner(resolution=cfg.resolution or 1, **kwargs)


def _build_s2(cfg: GridConfig, **kwargs: Any) -> AbstractPartitioner:
    from earthcatalog.grids.s2_partitioner import S2Partitioner

    return S2Partitioner(resolution=cfg.resolution or 2, **kwargs)


def _build_utm(cfg: GridConfig, **kwargs: Any) -> AbstractPartitioner:
    from earthcatalog.grids.utm_partitioner import UTMPartitioner

    return UTMPartitioner(**kwargs)


def _build_geojson(cfg: GridConfig, **kwargs: Any) -> AbstractPartitioner:
    from earthcatalog.grids.geojson_partitioner import GeoJSONPartitioner

    if not cfg.boundaries_path:
        raise ValueError("GridConfig.boundaries_path is required for type='geojson'")
    return GeoJSONPartitioner(
        boundaries_path=cfg.boundaries_path,
        id_field=cfg.id_field or "id",
        **kwargs,
    )


_REGISTRY: dict[str, Callable[..., AbstractPartitioner]] = {
    "h3": _build_h3,
    "s2": _build_s2,
    "utm": _build_utm,
    "geojson": _build_geojson,
}


def register_grid(grid_type: str, builder: Callable[..., AbstractPartitioner]) -> None:
    """Register (or replace) the builder for *grid_type*.

    *builder* is called as ``builder(cfg, time_bin=cfg.time_bin)`` and
    returns an :class:`~earthcatalog.partitioner.AbstractPartitioner`.
    """
    _REGISTRY[grid_type] = builder


def build_partitioner(cfg: GridConfig) -> AbstractPartitioner:
    """Instantiate the partitioner for a GridConfig (grid + temporal bin)."""
    try:
        builder = _REGISTRY[cfg.type]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown grid type: {cfg.type!r} (known: {known})") from None
    return builder(cfg, time_bin=cfg.time_bin)
