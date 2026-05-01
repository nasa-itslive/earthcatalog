# Progress

## 2026-05-01 — Phase A ✓, Phase B ✓

**Phase A — store_config eliminated:**
- `catalog.py`: `download_catalog`/`upload_catalog` accept explicit store+key params
- `lock.py`: `S3Lock` accepts explicit store+key params
- `EarthCatalog`: added `lock()`, `download_catalog()`, `upload_catalog()` methods
- All backward compat preserved; 309 tests pass

**Phase B — unified `catalog.ingest()` API:**
- `EarthCatalog.ingest()`: handles both full (drop+recreate) and delta (append) modes
- `catalog.ingest()` module-level convenience wrapper
- Exported from `earthcatalog.core`
- Hash index update, inventory reading, rustac GeoParquet writing all preserved
- 309 tests pass

## Done
- Phase A: store_config globals eliminated (catalog.py, lock.py, EarthCatalog)
- Phase B: catalog.ingest() unified API (full + delta, hash index, inventory)
- Phase C: Uniform Obstore I/O — all compact/register functions use stores,
  no local/S3 branching (-37 lines)
- Phase D: Deduplicated _h3_boundary_cells, _update_hash_index_from_parquets

## Remaining
- Phase E: Remove dead one-off scripts (audit_inventory, compare_catalogs, etc.)
- Phase F: Adopt shared test fixtures for simple cases (deferred)
- Phase G: Documentation update
