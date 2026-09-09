# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Removed
- One-shot processing modules retired now that the store is normalized:
  `earthcatalog/migrate.py` (legacy index migration) and
  `earthcatalog/index_backfill.py`, plus their `migrate-indices` /
  `index-backfill` CLI commands. Production catch-up is a plain
  `earthcatalog ingest --mode auto` diff ingest.
- Orphaned `earthcatalog/tools/` dev utility and the `scripts/ingest.py`
  backward-compat shim.
- Deprecated `EarthCatalog.bulk_ingest()` alias (use `ingest_inventory()`).
- Dead private-name aliases in `inventory.py`; callers use the public
  `iter_inventory*` names.

### Fixed
- CI workflows (`consolidate.yml`, `garbage_collect.yml`) now call
  `earthcatalog info` instead of the deleted `scripts/info.py` (both were
  failing at the catalog-info steps).
- README canonical search example uses `catalog.search()` returning pystac
  Items for clarity.
- `run_backfill` no longer builds a prefix-scoped warehouse store (fixed
  double-prefixed S3 keys on ingest).
- Garbage collection now runs against the unified warehouse index
  (`{warehouse}_index.parquet`) instead of the legacy hash/source index;
  removed `earthcatalog.pipelines.delete`.
- `unique_item_count()` / `scripts/info.py` / `cli info` report active items
  from the unified index (soft-deleted rows excluded).
- Consolidation and `compact_warehouse` support `s3://` warehouses and upload
  the rebuilt catalog to the correct S3 key.

## [0.6.0] - 2026-05-03

### Added
- Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg and GeoParquet.
- Single-node incremental ingest (`earthcatalog incremental`) with `--since` delta support.
- Dask distributed backfill pipeline with spot-resilient per-file mini-runs.
- Standalone compaction tool (`maintenance/compact.py`).
- H3 and GeoJSON spatial partitioners.
- SQLite-backed PyIceberg catalog with `IdentityTransform(grid_partition)` + `YearTransform(datetime)`.
- `CatalogInfo` — discovers grid type and resolution from table properties; provides `cells_for_geometry()` and `cell_list_sql()` helpers for spatial queries without prior knowledge of the catalog configuration.
- S3 distributed lock via conditional writes (`If-None-Match: *`) — no DynamoDB required.
- Lazy pystac_client-compatible search: `search()` returns `EarthCatalogItemSearch` with `items()`, `items_as_dicts()`, `pages()`, `item_collection()`, `matched()`, `stats()`.
- `stats()` returns `{files, rows_upper_bound, bytes_upper_bound}` from Iceberg manifest — zero I/O on Parquet data.
- `_repr_html_` on search results displays params, estimated match count, files, and data size.
- `__version__`, `__commit__`, `__version_full__` package metadata.
- `setuptools-scm` for versioning: version derived from git tags, commit hash baked into dev versions via `local_scheme = "node-and-date"`.
- 275 tests (13 fix_schema tests removed).

### Changed
- `EarthCatalog.search()` now returns `EarthCatalogItemSearch` (lazy, pystac_client-compatible) instead of collecting all results eagerly.
- `list(ec.search(...))` still works via `__iter__` backward compat.
- Per-file `rustac.search_sync` fan-out replaces DuckDB Hive-partitioned glob scan (was 2-3 min on 5000-file S3 warehouse; now metadata-only Iceberg prune + per-file search).
- `assets`, `links`, `bbox` stored as JSON strings in Iceberg and rehydrated by `_rehydrate()` for `pystac.Item.from_dict()` roundtrip.
- `Search` renamed to `_FileSearchEngine` (internal engine); public API is `EarthCatalogItemSearch`.
- `_StderrFilter` wrapper replaces broken `redirect_stderr` that crashed Jupyter kernel on stderr suppression.
- Package dependencies split: `pandas`, `duckdb`, `coiled` moved to dev extras.
- `catalog.py`, `catalog_info.py`, `fix_schema.py` consolidated into `earthcatalog.py` — one file for all catalog concerns.
- Versioning switched from manual `pyproject.toml` version to `setuptools-scm` (git tag based).
- S3 auth simplified: PyIceberg always uses anonymous reads (env credentials no longer needed for `open()`); `_anonymous_s3()` → `_cleared_env_s3()` detects anonymous mode from env directly instead of store config.
- `pages()` now wraps rustac calls with anonymous S3 context (was missing, now consistent with `items_as_dicts()`).
- `ingest()` and `bulk_ingest()` raise clear `RuntimeError` if `AWS_ACCESS_KEY_ID` is missing.
- Public API docs and examples updated to `import earthcatalog as ec` / `catalog = ec.open(...)` pattern.

### Removed
- `key_to_wkt()`, `_dedup_items()`, `open_catalog` alias.
- `tests/fixtures/stac_items.py` (184 unused lines).
- `tests/test_path_resolution.py` (253 lines testing string slicing).
- `environment.yml` (replaced by pyproject.toml).
- `earthcatalog/core/fix_schema.py` and `tests/test_fix_schema.py` (one-off migration tool).

[Unreleased]: https://github.com/nasa-itslive/earthcatalog/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/nasa-itslive/earthcatalog/releases/tag/v0.6.0
