# earthcatalog refactoring: slim facade, partitioner-owned binning, one-shot removal

Guided by your decisions: delete both one-shot modules now; **drop `search_uris`/`duck_search` from the facade** (they move to `search.py` as module-level functions — breaking change, documented); lat/lon grid uses **degrees-per-cell** with `r{row}c{col}` tile ids. Target for `catalog.py` is the `<600` lines already encoded in `test_architecture.py`.

## Phase 0 — Unbreak CI + trivial deletions (quick wins)
1. **Fix broken workflows** (currently failing): `consolidate.yml:68,84` and `garbage_collect.yml:76,97` still call the deleted `scripts/info.py` → replace with `uv run earthcatalog info --catalog … --warehouse …` (CLI has full flag parity).
2. Delete `earthcatalog/tools/` (orphaned, zero references).
3. Delete `scripts/ingest.py` shim; touch up `run.py` docstring + STATE.md note.
4. Remove deprecated `EarthCatalog.bulk_ingest`.
5. `inventory.py`: delete the 11 dead `_underscore` aliases; rename `_iter_inventory` → `iter_inventory` (2 call sites: `gc.py:44`, `test_inventory.py`).
6. Delete dead `_HIVE_RE_V2` from `schema.py`.
7. Delete stale `docs/cleanup_plan.md`, `docs/delete_plan_v2.md`; fix dead script refs in `docs/site/operations/ingest_workflow.md` and `docs/site/pipelines/architecture.md`.

## Phase 1 — Remove one-shot modules
1. Delete `earthcatalog/migrate.py` and `earthcatalog/index_backfill.py`.
2. Remove the `migrate-indices` and `index-backfill` commands from `cli.py`.
3. Delete `tests/test_migrate.py`, `tests/test_index_backfill.py`; update the wheel-smoke line in `tests.yml`.
4. Update STATE.md (RPI-5 resolved) + CHANGELOG.

## Phase 2 — Partitioner owns temporal binning
1. `partitioner.py`: `AbstractPartitioner` gains `time_bin` (constructor arg, default `"year"`) and a concrete `bin_value(dt) -> str` method — the formatting logic moves here from `schema.bin_value` (`schema.py` re-exports for back-compat so existing imports keep working).
2. `grids/__init__.py`: replace the if/elif factory with a registry dict (`h3`, `s2`, `utm`, `geojson`, `lat_lon`); the factory passes `time_bin=cfg.time_bin` into every partitioner.
3. All four partitioners accept `time_bin` and forward to the base class.
4. `transform.group_by_partition(items, partitioner)` — takes the partitioner and calls `partitioner.bin_value(...)` instead of a loose `time_bin` string; thread the partitioner through `ingest.py` workers (the layout tuple stays for path building only).
5. **Fix `rebuild.py`**: rebuild the Iceberg spec via `build_partition_spec(time_bin)` from preserved table properties instead of the hardcoded year-only `PARTITION_SPEC` (currently silently recreates month/day tables with a year spec).
6. Tests: `bin_value` on the partitioner, `group_by_partition` via partitioner, rebuild preserves month/day specs.

## Phase 3 — Read path uses the partitioner (query parity)
1. `CatalogInfo.cells_for_geometry` → lazily build and **cache one partitioner** via `build_partitioner(GridConfig(...))`; delete the duplicated `_h3_cells`/`_geojson_keys` (this also fixes GeoJSONPartitioner being rebuilt per query). Result: **s2/utm/lat_lon catalogs become queryable** (today they raise `ValueError` on any search).
2. Add `time_bin` field to `CatalogInfo` (from `PROP_TIME_BIN`); `file_paths` uses it instead of re-reading table properties.
3. Generic `__repr__` (drop h3/geojson special-casing); drop the h3-specific default resolution in `_catalog_info`.
4. Tests: ingest→search roundtrip for `s2` and `utm` on `LocalStore` (previously impossible).

## Phase 4 — Native lat/lon rectangular grid
1. New `earthcatalog/grids/latlon_partitioner.py`: `LatLonPartitioner(resolution=2)` where resolution = **cell size in degrees**; tile id `r{row}c{col}` with `row=floor(lat/size)`, `col=floor(lon/size)` (e.g. `r-23c-60`); polygons walk the bbox in size-steps, boundary-inclusive like the other partitioners.
2. Register `"lat_lon"` in the factory; add it to `run.py` argparse choices and `cli.py --grid` help.
3. Tests: point/polygon/multi-tile/boundary cases + the config-only roundtrip `GridConfig(type="lat_lon", resolution=2, time_bin="month")` → `grid=lat_lon/level=2/tile=…/month=…` with search finding items back (the stated goal of `test_layout.py`'s docstring).

## Phase 5 — Slim `EarthCatalog` to the basic operations
Final public surface: `open()`, `search`, `search_to_arrow`, `search_files`, `ingest_inventory`, `garbage_collect`, `download_catalog`/`upload_catalog`, `grid_type`/`grid_resolution`.
1. **Search**: move the `search_uris`/`duck_search` bodies into `search.py` as one shared DuckDB implementation plus two module-level functions `search_uris(catalog, …)` / `duck_search(catalog, …)`; move `_search_prune`/`_cleared_env_s3` there too. Update all call sites (tests: `test_search`, `test_duckdb_query`, `test_earthcatalog`, `test_spatial_query`, `test_roundtrip`; `run.py`/`cli.py` where used) and `docs/site/api/core.md`.
2. **GC**: move the ~120-line post-GC orchestration (warehouse-prefix derivation, `_strip`, Iceberg rebuild+upload, stats refresh) into `gc.py`; `EarthCatalog.garbage_collect` becomes a thin wrapper. Also kills the `_strip` duplicate of `migrate._strip`.
3. **Display**: extract the ~90-line `_repr_html_` body into a render helper in `stats.py`; keep a one-line delegate.
4. **Stats**: move `_build_stats_cache` + the `_ensure_stats`/`stats`/`top_cells`/`total_files` machinery into `stats.py` (merging with `recompute_warehouse` as flagged in SIMPLIFICATION_PROGRESS).
5. `unique_item_count`'s index-opening → helper in `index.py`.
6. **Encapsulation**: `IngestPipeline` (pipeline.py:67-68) stops reaching into `catalog._catalog`/`._table` — expose read-only properties or pass explicitly.
7. Tighten `test_architecture.py`: `CATALOG_PY_MAX_LINES` 1120 → 600; raise `search.py`/`gc.py`/`stats.py` budgets to absorb the moved code.

## Phase 6 — Consolidate shared helpers
1. New `earthcatalog/uris.py`: canonical `parse_s3_uri()` (moved from `stats.py`, re-exported there); replace the ~20 inline `removeprefix("s3://").split("/", 1)` copies across catalog/gc/pipeline/stats/rebuild/inventory/diff/run/cli/`scripts/run_gc.py`.
2. New `earthcatalog/stores.py`: one `make_store(...)` authenticated-store factory folding `run._make_s3_store` + `inventory.get_store`/`get_authenticated_store` + ad-hoc `S3Store(...)` in cli. (`store_config.py` globals stay for the allowlisted legacy modules; full removal is a separate pass.)

## Phase 7 — Docs + verification
1. Update `docs/site/api/core.md` (search function homes, removed CLI commands), CHANGELOG, STATE.md, `SIMPLIFICATION_PROGRESS.md`.
2. Full hermetic suite (`pytest -m "not integration and not performance and not e2e"`), ruff + pyright clean, and per-grid ingest→search characterization roundtrips (h3, s2, utm, geojson, lat_lon) before/after.

## Risks / notes
- **Breaking change**: removing `search_uris`/`duck_search` methods affects external callers/notebooks — mitigated by same-name functions in `earthcatalog.search`, changelog + docs updates.
- S2 polygon coverage is bbox-based (over-inclusive); that's safe for pruning since rustac/DuckDB apply final row filtering — noted, not redesigned here.
- The cached partitioner on `CatalogInfo` matters for geojson (STRtree build cost per query today).
- One commit per phase, suite green at every commit; architecture-test budget tightening lands last within each phase so it's easy to revert.