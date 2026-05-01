# EarthCatalog Refactoring — TODO

## Overall Goal
Refactor EarthCatalog into a coherent API (`catalog.open()`, `catalog.ingest()`, `catalog.search()`) by collapsing `backfill.py` and `incremental.py` into a single pipeline, eliminating storage duality, removing global mutable state, and deduplicating code — all without changing observable behavior.

## Phase A — Eliminate store_config globals
- [x] Create `improvements` branch
- [ ] Make `store_config` deprecated; thread store/catalog_key/lock_key explicitly through EarthCatalog
- [ ] Update `lock.py` to accept store/key from caller instead of reading globals
- [ ] Update `catalog.py` `download_catalog`/`upload_catalog` to accept store/key
- [ ] Remove `store_config.set_store()` calls from tests; pass stores explicitly
- [ ] All tests pass

## Phase B — Unified `catalog.ingest()` API
- [ ] Design `EarthCatalog.ingest()` signature (inventory, delta, chunk_size, compact_rows, limit, since, update_hash_index)
- [ ] Implement `ingest()`: read inventory → fetch → fan-out → write parquet → register + hash index
- [ ] Handle both full backfill (drop+recreate table) and delta (append + file numbering)
- [ ] Make `backfill.py` CLI delegate to `catalog.ingest()`
- [ ] Make `incremental.py` CLI delegate to `catalog.ingest()`
- [ ] Verify hash index update works the same way
- [ ] All tests pass

## Phase C — Abstract storage I/O
- [ ] Eliminate `compact_cell_year` + `compact_cell_year_s3` pairs: one function behind a store abstraction
- [ ] Eliminate `compact_cell_year_delta` + `compact_cell_year_delta_s3` pairs
- [ ] Eliminate `_next_part_index_local` + `_next_part_index_s3`
- [ ] Eliminate `register_and_cleanup` + `register_delta` branching — one register function
- [ ] All tests pass

## Phase D — Deduplication
- [ ] Remove `catalog_info.py:_h3_boundary_cells` — delegate to `h3_partitioner.py`
- [ ] Remove `backfill.py:_update_hash_index_from_parquets` local-path reimplementation — use `hash_index.py` uniformly
- [ ] Consolidate `maintenance/compact.py` with backfill compact phase
- [ ] All tests pass

## Phase E — Remove dead code
- [ ] Remove backfill 4-phase scaffolding (NDJSON, completion markers, pending-chunk retry) — not needed for single-node
- [ ] Remove unused import in `incremental.py` (`H3Partitioner`)
- [ ] Archive one-off scripts (audit_inventory, compare_catalogs, diff_manifests, etc.)
- [ ] Remove `scripts/compact_stragglers.py`, `final_compact.py`, `migrate_schema.py`, `repair_catalog.py`
- [ ] All tests pass

## Phase F — Test fixtures
- [ ] Migrate inline STAC item definitions in 10 test files to `tests/fixtures/stac_items.py`
- [ ] All tests pass

## Phase G — Documentation
- [ ] Update `docs/site/operations/ingest_workflow.md`
- [ ] Update `docs/site/operations/query_catalog.md`
- [ ] Update `docs/site/index.md` (if needed)
- [ ] Verify `zensical build` succeeds
