# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.2.0] - 2026-04-24

### Added

- **`CatalogInfo`** (`earthcatalog.core.catalog_info`) — discovers grid system metadata
  (type, resolution) directly from Iceberg table properties. No prior knowledge of how
  the catalog was built is required.
  - `CatalogInfo.from_table(table)` — reads grid properties; falls back to `h3 / resolution=1`
    for catalogs predating this release.
  - `cells_for_geometry(geom)` — converts any Shapely geometry to the correct partition
    keys at the catalog's actual resolution.
  - `cell_list_sql(geom)` — returns a ready-to-embed `grid_partition IN (...)` SQL fragment
    for use with `iceberg_scan()`.
- **Grid metadata stored as Iceberg table properties** — `get_or_create_table()` now accepts
  an optional `GridConfig` and writes `earthcatalog.grid.type`, `earthcatalog.grid.resolution`,
  `earthcatalog.grid.boundaries_path`, and `earthcatalog.grid.id_field` to the table.
  Existing tables without these properties are backfilled on next access.
- **`pyproject.toml` URLs** — added `Homepage`, `Repository`, `Documentation`, `Bug Tracker`,
  and `Changelog` links pointing to `github.com/nasa-itslive/earthcatalog`.
- **13 new tests** in `tests/test_catalog_info.py` covering property read/write, legacy
  fallback, resolution handling, valid H3 indices, SQL fragment generation, and empty-geometry guard.

### Changed

- `get_or_create_table(catalog)` signature extended to
  `get_or_create_table(catalog, grid_config=None)` — fully backward-compatible.
- `run()` in `incremental.py` and `run_backfill()` in `backfill.py` both accept and
  forward `grid_config` so table properties are populated automatically during ingest.
- README spatial query example updated to use `CatalogInfo` instead of manual H3 cell
  construction; DuckDB `spatial` extension now loaded alongside `iceberg`.
- Documentation and GitHub Pages URLs updated from `nsidc.github.io` to
  `nasa-itslive.github.io`.

---

## [0.1.0] - 2026-04-23

### Added

- Initial release of the rewritten earthcatalog pipeline.
- **`pipelines/incremental.py`** — single-node ingest from S3 Inventory (CSV, CSV.gz,
  Parquet, manifest.json). Processes items in chunks; one Iceberg snapshot per chunk by
  default. Supports `--since` for delta runs.
- **`pipelines/backfill.py`** — three-level Dask distributed pipeline. Level 0: workers
  fetch STAC items and write GeoParquet directly to S3. Level 1: per-bucket compaction on
  workers. Level 2: single `table.add_files()` on the head node. Spot-resilient via
  per-file mini-runs with `ingest_stats.json` checkpoint.
- **`maintenance/compact.py`** — standalone compaction tool; merges small part files per
  `(grid_partition, year)` bucket, deduplicates by `id`, rebuilds the Iceberg manifest.
- **`core/schema.py`** — 22-column PyArrow schema as the single source of truth.
- **`core/catalog.py`** — SQLite-backed PyIceberg catalog with `IdentityTransform(grid_partition)`
  + `YearTransform(datetime)` partition spec.
- **`core/transform.py`** — `fan_out()`, `group_by_partition()`, `write_geoparquet()`,
  and `_align_schema()` for rustac schema alignment.
- **`core/lock.py`** — `S3Lock` using S3 conditional writes (`If-None-Match: *`) for
  distributed catalog serialization without DynamoDB.
- **`core/store_config.py`** — global obstore singleton; `LocalStore` default for
  zero-config local development.
- **`grids/h3_partitioner.py`** — boundary-inclusive H3 partitioner with densified
  exterior ring walk to prevent coverage gaps.
- **`grids/geojson_partitioner.py`** — GeoJSON region partitioner backed by a Shapely
  STR-tree for O(log n) intersection lookup.
- **`config.py`** — `AppConfig`, `GridConfig`, `IngestConfig`, `CatalogConfig` dataclasses
  with YAML loader.
- Full test suite: 186 tests across 10 test files covering transform, partitioners,
  inventory parsing, lock, catalog lifecycle, round-trip ingest, DuckDB queries, pipeline
  end-to-end, backfill, and compaction.
- Logo, architecture docs, and ingest guide in `docs/site/`.

[Unreleased]: https://github.com/nasa-itslive/earthcatalog/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/nasa-itslive/earthcatalog/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nasa-itslive/earthcatalog/releases/tag/v0.1.0
