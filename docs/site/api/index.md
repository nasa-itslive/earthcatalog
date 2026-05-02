# API Reference

## Top-Level API

Entry points for opening a catalog, ingesting data, and searching.

### Module functions

::: earthcatalog.core.catalog
    options:
      members:
        - open
        - ingest

### EarthCatalog methods

::: earthcatalog.core.earthcatalog.EarthCatalog
    options:
      members:
        - ingest
        - bulk_ingest
        - search_files
        - stats
        - unique_item_count

## Modules

| Module | Description |
|---|---|
| [`earthcatalog.core`](core.md) | Schema, transform, lock, store config, EarthCatalog |
| [`earthcatalog.grids`](grids.md) | Spatial partitioners (H3, GeoJSON) |
| [`earthcatalog.pipelines`](pipelines.md) | Incremental and backfill pipelines |
| [`earthcatalog.maintenance`](maintenance.md) | Warehouse compaction |
