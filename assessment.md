# EarthCatalog Project Assessment

## Project Shape (what exists)

```
earthcatalog/
  core/
    catalog.py         — PyIceberg catalog open/create/add_files/download/upload
    catalog_info.py    — CatalogInfo: H3 cell→path resolution, stats, grid metadata
    earthcatalog.py    — EarthCatalog facade (search_files, stats, _repr_html_)
    transform.py       — fan_out, group_by_partition, write_geoparquet, _normalize_for_iceberg
    schema.py          — canonical PyArrow schema (28 fields)
    hash_index.py      — hash_id, read_hashes, merge_hashes_from_parquets, write_hashes
    lock.py            — S3Lock (atomic if-none-match)
    partitioner.py     — AbstractPartitioner (ABC with get_intersecting_keys, key_to_wkt)
    store_config.py    — global mutable module-level store/catalog_key/lock_key
    fix_schema.py      — legacy raw_stac→rustac migration, imports _normalize_for_iceberg
  grids/
    __init__.py        — build_partitioner factory
    h3_partitioner.py  — H3Partitioner (get_intersecting_keys via geo_to_cells + _boundary_cells)
    geojson_partitioner.py — GeoJSONPartitioner (STRtree-based)
  pipelines/
    backfill.py        — 4-phase Dask pipeline (1431 lines, normal + delta × local + S3)
    incremental.py     — single-node ThreadPool pipeline (737 lines, superset of inventory readers)
  maintenance/
    compact.py         — local-only warehouse compaction + catalog rebuild (419 lines)
  config.py            — AppConfig/CatalogConfig/GridConfig/IngestConfig + YAML loader
  cli.py               — typer CLI wrapping incremental.run / incremental.run_from_config
  tools/make_test_inventory.py — generate test S3 Inventory CSV
scripts/               — 16 one-off/utility scripts
tests/                 — 21 test files, 10 with inline STAC item definitions
```

## Finding 1: Two parallel pipeline codebases

**`backfill.py`** (1431 lines) and **`incremental.py`** (737 lines) are two parallel implementations of the same fundamental operation: read inventory → fetch STAC JSON → fan-out → write GeoParquet → register with Iceberg.

| Aspect | `backfill.py` | `incremental.py` |
|--------|---------------|------------------|
| Architecture | 4 phases (chunks → NDJSON → compact → register) | Single loop: fetch → fan-out → write → add_files |
| Parallelism | Dask distributed | `ThreadPoolExecutor` local |
| Intermediate format | NDJSON staging files | In-memory batching |
| Idempotency | Per-chunk markers, skip-file tracking | S3Lock + retry |
| Delta mode | Separate code paths (delta=True) | `since=` datetime filter |
| Storage coupling | Local/S3 function pairs everywhere | Single loop, path-based |

The project has effectively migrated from Dask/Coiled to single-node GitHub Actions, but both pipelines remain. **`backfill.py` carries significant complexity from the Dask era** — NDJSON intermediate files, completion markers, pending-chunk retry, per-chunk idempotency — none of which are needed in the current single-node mode.

### What to do

Collapse the two pipelines into one. The `incremental.py` loop structure is simpler and sufficient. `backfill.py`'s notable unique features (delta file numbering, hash index update, inventory reader) should be preserved, but the 4-phase scaffolding can go.

---

## Finding 2: Local/S3 duplication — the 2× multiplier

`backfill.py` defines dual implementations for every compact operation:

```
compact_cell_year              (local FS)
compact_cell_year_s3           (S3 store)
compact_cell_year_delta        (local FS)
compact_cell_year_delta_s3     (S3 store)
_next_part_index_local         (local FS)
_next_part_index_s3            (S3 store)
register_and_cleanup           (local + S3 inline branching)
register_delta                 (local + S3 inline branching)
_update_hash_index_from_parquets (local + S3 inline branching)
```

Each pair differs only in *how a file is read/written* (local path vs. obstore put/get). The business logic is identical. This is the textbook OCP violation: adding a third backend (GCS, Azure Blob) requires doubling every function again.

### What to do

Abstract file I/O behind a thin `WarehouseStore` wrapper (or just standardize on obstore with a uniform URI scheme). The `store_config` pattern already exists but it's inconsistently applied — some functions read from it, some accept both `warehouse_store` and `warehouse_root` parameters.

---

## Finding 3: `store_config.py` — global mutable state

```python
# store_config.py
_store = LocalStore("/tmp/earthcatalog_store")
_catalog_key = "catalog.db"
_lock_key = ".lock"
```

This module-level mutable state is read by `lock.py`, `catalog.py` (download/upload_catalog), and many pipeline functions. Every test resets it with `store_config.set_store(MemoryStore())`, creating hidden ordering dependencies between tests.

### What to do

Make store configuration explicit. The `EarthCatalog` facade already encapsulates store + catalog + info, but large portions of the code (backfill, incremental, scripts) ignore it and use store_config directly. Either:
- Thread an explicit `StoreConfig` object through pipeline functions, or
- Make `EarthCatalog` the single entry point that everything else derives state from.

---

## Finding 4: `run_backfill()` — 22 parameters

```python
def run_backfill(
    inventory_path, catalog_path,
    staging_store, staging_prefix,
    warehouse_store, warehouse_root,
    partitioner=None, h3_resolution=None,
    chunk_size=100_000, compact_rows=100_000,
    fetch_concurrency=256,
    limit=None, since=None,
    use_lock=True, upload=True,
    skip_inventory=False, skip_ingest=False,
    retry_pending=False, delta=False,
    create_client=None,
    hash_index_path=None, update_hash_index=False,
) -> None:
```

22 parameters — 8 of which are boolean flags that switch between entirely different modes (`delta`, `skip_inventory`, `skip_ingest`, `retry_pending`). This is an ISP violation: every caller depends on parameters it doesn't use.

### What to do

Split into separate entry points: `run_backfill()` (initial full ingest) and `run_delta()` (incremental). Extract mode flags into a small `IngestOptions` dataclass.

---

## Finding 5: Duplicate `_boundary_cells` implementations

Two independent implementations of the same H3 edge-cell algorithm:

- **`h3_partitioner.py:_boundary_cells`** (line 37–56): uses `numpy.linspace` for densification
- **`catalog_info.py:_h3_boundary_cells`** (line 296–315): uses manual `for t in range(n): t/n` loop

```python
# h3_partitioner.py
n = max(2, int(np.hypot(lon1 - lon0, lat1 - lat0) / 0.1))
for t in np.linspace(0, 1, n):
    cells.add(h3.latlng_to_cell(lat0 + t*(lat1-lat0), lon0 + t*(lon1-lon0), resolution))

# catalog_info.py  (same algorithm, different API)
dist = ((lon1 - lon0)**2 + (lat1 - lat0)**2)**0.5
n = max(2, int(dist / 0.1))
for i in range(n):
    t = i / n
    cells.add(h3.latlng_to_cell(lat0 + t*(lat1-lat0), lon0 + t*(lon1-lon0), resolution))
```

### What to do

`h3_partitioner.py` is the natural home. `catalog_info.py` should delegate to `H3Partitioner` instead of reimplementing.

---

## Finding 6: `_update_hash_index_from_parquets` duplicates `hash_index.py`

`backfill.py:_update_hash_index_from_parquets` (lines 879–945) is a 67-line function that reimplements the read-merge-write cycle that already exists in `hash_index.py` (`read_hashes`, `merge_hashes_from_parquets`, `write_hashes`). The only difference is a local-path fallback that reads/writes files directly instead of through obstore.

### What to do

Add a `read_hashes_local` / `write_hashes_local` pair to `hash_index.py`, or standardize all hash index I/O through obstore with a LocalStore. Then delete the duplicate code.

---

## Finding 7: `maintenance/compact.py` duplicates backfill Phase 3

`maintenance/compact.py:_compact_group_impl` (lines 91–165) duplicates:
- `compact_cell_year` / `compact_cell_year_s3` read-merge-dedup-write logic
- The `FileMetadata` grouping from `backfill.py`
- `_scan_warehouse` duplicates obstore.list + hive-regex parsing already in `register_and_cleanup`

### What to do

Consolidate. Either make `compact.py` delegate to backfill's compact functions, or extract the shared compaction logic to `core/`.

---

## Finding 8: `incremental.py` — one unused import, dead-end `_get_store`

```python
from earthcatalog.grids.h3_partitioner import H3Partitioner  # line 101 — unused
```

`H3Partitioner` is imported at module level in `incremental.py` but never referenced directly (`run` creates it via `H3Partitioner(resolution=h3_resolution)` when no partitioner is passed, and `run_from_config` uses `build_partitioner`).

`_get_store` creates a `skip_signature=True` S3Store, but `_get_authenticated_store` does the credential-resolution dance. Both are called inconsistently.

---

## Finding 9: Test fixture duct-tape

10 test files define inline STAC items with 6+ different shapes. The `tests/fixtures/stac_items.py` module exists (from my earlier work) but hasn't been adopted yet. This isn't a structural problem but it creates noise in diffs and makes test maintenance harder.

---

## Finding 10: Scripts directory bloat

`scripts/` has 16 files. Many are clearly one-off investigative tools:

| Script | Status |
|--------|--------|
| `run_backfill.py` | Active — wraps backfill CLI |
| `update_hash_index.py` | Active — thin wrapper over hash_index.py |
| `info.py` | Active — catalog info CLI |
| `daily_delta.py` | Active — daily delta pipeline |
| `consolidate.py` | Active — consolidation pipeline |
| `inject_build_metadata.py` | Active — docs build |
| `catalog_stats.py` | Probably superseded by `info.py` |
| `audit_inventory.py` | One-off investigative |
| `compare_catalogs.py` | One-off |
| `diff_manifests.py` | One-off |
| `dump_inventory_ids.py` | One-off |
| `extract_manifest_ids.py` | One-off |
| `compact_stragglers.py` | One-off |
| `final_compact.py` | One-off |
| `migrate_schema.py` | One-off |
| `repair_catalog.py` | One-off |

The one-off scripts accumulate because there's no shared investigative CLI.

---

## SOLID Scorecard

| Principle | Grade | Reasoning |
|-----------|-------|-----------|
| **S**ingle Responsibility | **D** | `backfill.py`: 1431 lines, 4 phases, 2 modes, 2 backends, hash index management, staging cleanup |
| **O**pen/Closed | **D** | Adding backend = 4 new functions; adding mode = 4 more |
| **L**iskov Substitution | **B** | `AbstractPartitioner` ABC is clean; but `obstore` stores are duck-typed with no formal protocol |
| **I**nterface Segregation | **C** | `run_backfill(**22_kwargs)`; EarthCatalog facade is good, but pipelines ignore it |
| **D**ependency Inversion | **C** | `store_config` global mutables couple everything; concrete store/path types everywhere |

---

## Recommended Actions (by impact)

1. **Collapse the two pipelines** — delete `backfill.py`'s 4-phase scaffolding; migrate delta-file numbering + hash-index update into `incremental.py` (or a unified module)
2. **Abstract storage I/O** — eliminate the local/S3 function pairs; one compact function, one register function
3. **Eliminate `store_config` globals** — pass store explicitly; let `EarthCatalog` be the dependency root
4. **Deduplicate `_boundary_cells`** — `h3_partitioner.py` is the canonical home
5. **Deduplicate hash index update** — delete the local-path reimplementation in `backfill.py`
6. **Consolidate compaction** — merge `maintenance/compact.py` logic with backfill's compact phase
7. **Clean up scripts** — archive one-offs, keep active pipelines
8. **Adopt shared test fixtures** — already scaffolded in `tests/fixtures/`

Items 1–3 are structural and would require careful refactoring but would eliminate the majority of the project's accidental complexity. Items 4–8 are mechanical.
