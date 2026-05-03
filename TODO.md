# EarthCatalog Refactoring — DONE

## Goal Achieved
Coherent top-level API: `catalog.open()` → EarthCatalog with `.ingest()` (single-node), `.bulk_ingest()` (Dask/Coiled), `.search_files()` (spatial), `.stats()` (catalog info).

## Completed

### Phase A — store_config eliminated
- `catalog.py`: `download_catalog`/`upload_catalog` accept explicit store+key params
- `lock.py`: `S3Lock` accepts explicit store+key params (fallback kept for compat)
- `EarthCatalog`: `lock()`, `download_catalog()`, `upload_catalog()` use its own store

### Phase B — `catalog.ingest()` unified API
- Single-node: `ThreadPoolExecutor` fetch → fan-out → write → register
- Full mode: drop+recreate table, uuid filenames
- Delta mode: append, hash index update, `since=` datetime filtering
- Module-level `catalog.ingest()` convenience wrapper

### Phase C — Uniform Obstore I/O
- `compact_cell_year` + `compact_cell_year_s3` → single `compact_cell_year`
- `compact_cell_year_delta` + `compact_cell_year_delta_s3` → single `compact_cell_year_delta`
- `_next_part_index_local` + `_next_part_index_s3` → single `_next_part_index`
- `register_and_cleanup` uses store-based listing with `_list_warehouse_keys`

### Phase D — Deduplication
- `catalog_info.py:_h3_boundary_cells` → delegates to `H3Partitioner`
- `backfill.py:_update_hash_index_from_parquets` → uniform obstore paths

### Phase E — Dead code removed
- 10 one-off scripts → `scripts/archive/`
- Backfill 4-phase scaffolding PRESERVED (user needs spot-instance resilience)

### Phase F — Test simplification
- `test_backfill.py`: 1365→282 lines, removed 27 trivial helper tests
- Kept: compact, register, e2e backfill+delta, real S3 integration

### Phase G — Documentation
- Quickstart rewritten with `catalog.open()` + `.ingest()` + `.search_files()`
- Ingest workflow updated with both API paths

### `bulk_ingest()` — Distributed Dask/Coiled entry point
- Thin wrapper (~30 lines) around `backfill.run_backfill()`
- Derives catalog path, warehouse root, partitioner from EarthCatalog state
- Maps simplified params to the 4-phase pipeline
- Temporary `store_config` bridge for `run_backfill` internals

## Current State
- 279 tests pass (34 deselected integration/e2e)
- Pre-commit hooks pass (ruff lint + format + pytest)
- Docs build clean

## Next: `catalog.search()` — spatial/temporal query API
