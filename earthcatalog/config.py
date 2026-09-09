"""Grid configuration for EarthCatalog."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GridConfig:
    type: str = "h3"
    resolution: float | None = None  # H3/S2 level; lat_lon: degrees per tile
    boundaries_path: str | None = None  # GeoJSON partitioner: path to boundaries file
    id_field: str | None = None  # GeoJSON partitioner: property to use as key
    time_bin: str = "year"  # "year" | "month" | "day" — temporal binning
