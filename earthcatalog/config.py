"""
Configuration dataclasses and YAML loader for EarthCatalog.

Example config file (config/h3_r1.yaml):

    catalog:
      db_path:   /tmp/earthcatalog.db
      warehouse: /tmp/earthcatalog_warehouse

    grid:
      type:       h3
      resolution: 1        # H3 resolution (0=coarse, 15=fine); default 1 (~86k km²/cell)
                           # Resolution 1 → 842 global cells; good partition grain for
                           # spatial predicate pushdown without too many small files.

    ingest:
      chunk_size:       500
      max_workers:      16
      batch_add_files:  false   # set true for initial backfill — one Iceberg snapshot for entire run

Example with GeoJSON boundaries:

    grid:
      type:            geojson
      boundaries_path: /path/to/regions.geojson
      id_field:        region_name
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import yaml


@dataclass
class CatalogConfig:
    db_path: str = "/tmp/earthcatalog.db"
    warehouse: str = "/tmp/earthcatalog_warehouse"


@dataclass
class GridConfig:
    type: Literal["h3", "s2", "geojson"] = "h3"
    resolution: int | None = None  # H3 / S2 resolution level
    boundaries_path: str | None = None  # GeoJSON partitioner: path to boundaries file
    id_field: str | None = None  # GeoJSON partitioner: property to use as key


@dataclass
class IngestConfig:
    chunk_size: int = 500
    max_workers: int = 16
    batch_add_files: bool = False
    """
    When False (default): ``table.add_files()`` is called after every chunk.
    Safe for incremental daily runs where a mid-run crash leaves the catalog in
    a consistent partial state.

    When True: all GeoParquet paths are collected in memory and registered in a
    single ``table.add_files()`` call at the very end of the run — exactly one
    Iceberg snapshot regardless of chunk count.  Use this for initial backfills
    to avoid snapshot explosion (200k+ snapshots for 100M items).
    If the process crashes mid-run, no files are registered; re-run from scratch.
    """


@dataclass
class AppConfig:
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    grid: GridConfig = field(default_factory=GridConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)


def load_config(path: str) -> AppConfig:
    """Load an AppConfig from a YAML file.  Missing sections use defaults."""
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}

    catalog_raw = raw.get("catalog", {})
    grid_raw = raw.get("grid", {})
    ingest_raw = raw.get("ingest", {})

    return AppConfig(
        catalog=CatalogConfig(**catalog_raw),
        grid=GridConfig(**grid_raw),
        ingest=IngestConfig(**ingest_raw),
    )
