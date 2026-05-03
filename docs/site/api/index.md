# API Reference

## Top-Level API

Entry points for opening a catalog, ingesting data, and searching.

::: earthcatalog.catalog
    options:
      members:
        - open

::: earthcatalog.catalog.EarthCatalog
    options:
      members:
        - ingest
        - bulk_ingest
        - search
        - search_to_arrow
        - search_files
        - info
        - stats
        - unique_item_count

## Modules

| Module | Description |
|---|---|
| [`earthcatalog`](core.md) | Catalog, search, transform, lock |
| [`earthcatalog.grids`](grids.md) | Spatial partitioners (H3, GeoJSON) |
| [`earthcatalog.pipelines`](pipelines.md) | Incremental and backfill pipelines |
| [`earthcatalog.maintenance`](maintenance.md) | Warehouse compaction |
