# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg and GeoParquet.
- Single-node incremental ingest (`earthcatalog incremental`) with `--since` delta support.
- Dask distributed backfill pipeline with spot-resilient per-file mini-runs.
- Standalone compaction tool (`maintenance/compact.py`).
- H3 and GeoJSON spatial partitioners.
- SQLite-backed PyIceberg catalog with `IdentityTransform(grid_partition)` + `YearTransform(datetime)`.
- `CatalogInfo` — discovers grid type and resolution from table properties; provides `cells_for_geometry()` and `cell_list_sql()` helpers for spatial queries without prior knowledge of the catalog configuration.
- S3 distributed lock via conditional writes (`If-None-Match: *`) — no DynamoDB required.
- 199 tests.

[Unreleased]: https://github.com/nasa-itslive/earthcatalog/commits/main
