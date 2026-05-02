# API Reference

## Top-Level API

Entry points for opening a catalog, ingesting data, and searching.

::: earthcatalog.core.catalog
    options:
      members:
        - open

::: earthcatalog.core.earthcatalog.EarthCatalog
    options:
      members:
        - ingest
        - bulk_ingest
        - search_files
        - info
        - stats
        - unique_item_count

## Modules

| Module | Description |
|---|---|
| [`earthcatalog.core`](core.md) | Schema, transform, lock, EarthCatalog |
| [`earthcatalog.grids`](grids.md) | Spatial partitioners (H3, GeoJSON) |
| [`earthcatalog.pipelines`](pipelines.md) | Incremental and backfill pipelines |
| [`earthcatalog.maintenance`](maintenance.md) | Warehouse compaction |
