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

## Remaining
- Phase C: Abstract storage I/O (eliminate local/S3 pairs in backfill.py)
- Phase E: Remove dead code / archive one-off scripts  
- Phase F: Adopt shared test fixtures across test files (deferred — low impact)
- Phase G: Documentation update
