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

## All Phases Complete (306 tests pass)

| Phase | What | Outcome |
|-------|------|---------|
| **A** | store_config globals eliminated | `catalog.py`, `lock.py`, `EarthCatalog` accept explicit store+key |
| **B** | `catalog.ingest()` unified API | Full + delta modes, hash index, inventory — on `EarthCatalog` + module level |
| **C** | Uniform Obstore I/O | `compact_cell_year`/`_delta`/`_next_part_index` — one function each, no local/S3 pairs. -37 lines |
| **D** | Deduplication | `_h3_boundary_cells` → `H3Partitioner`; `_update_hash_index_from_parquets` → uniform obstore |
| **E** | Remove dead scripts | 10 one-offs → `scripts/archive/` |
| **F** | Test fixtures | Scaffold exists; deferred for complex cases (per-item assets/links) |
| **G** | Documentation | Updated `ingest_workflow.md` with `catalog.ingest()` API |
