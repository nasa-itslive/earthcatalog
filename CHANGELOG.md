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

### Removed
- `key_to_wkt()`, `_dedup_items()`, `open_catalog` alias.
- `tests/fixtures/stac_items.py` (184 unused lines).
- `tests/test_path_resolution.py` (253 lines testing string slicing).
- `environment.yml` (replaced by pyproject.toml).
- `earthcatalog/core/fix_schema.py` and `tests/test_fix_schema.py` (one-off migration tool).

[Unreleased]: https://github.com/nasa-itslive/earthcatalog/commits/main
