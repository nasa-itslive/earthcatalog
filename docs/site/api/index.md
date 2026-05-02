# API Reference

## Top-Level API

The `catalog` module is the main entry point — it provides `open()`, `ingest()`,
and related lifecycle functions.

::: earthcatalog.core.catalog

## Modules

| Module | Description |
|---|---|
| [`earthcatalog.core`](core.md) | Schema, transform, lock, store config, EarthCatalog |
| [`earthcatalog.grids`](grids.md) | Spatial partitioners (H3, GeoJSON) |
| [`earthcatalog.pipelines`](pipelines.md) | Incremental and backfill pipelines |
| [`earthcatalog.maintenance`](maintenance.md) | Warehouse compaction |
