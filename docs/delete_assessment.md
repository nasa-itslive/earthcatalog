# Delete Plan Assessment — Codebase Soundness Review

## Methodology

Each assertion in `docs/delete_plan.md` was validated against the actual
source code at the file/line level.  No code was executed; this is a
static analysis.

---

## Assertion-by-Assertion Validation

### 1. "Queries use Iceberg only for file-level pruning, not row-level filtering"

**VERIFIED — Correct.**

`earthcatalog/search.py:100-152` (`_FileSearchEngine`): Iceberg pruning
(`_search_prune` → `CatalogInfo.file_paths()`) returns a list of file
paths via `table.scan(row_filter=expr).plan_files()` at
`earthcatalog/catalog.py:245`.  Each path is then passed individually to
`rustac.search_sync()` in `iter_items()` at `search.py:146`.  DuckDB path
at `search_uris()` (`search.py:771`) and `duck_search()` (`search.py:860`)
follow the same pattern: Iceberg selects files, DuckDB does row-level
filtering via `read_parquet([paths]) WHERE ...`.

**No row-level filter is applied at the Iceberg level.**  A "deleted"
column would not help Iceberg skip files, because `IdentityTransform` on
`grid_partition` and `YearTransform` on `datetime` are the only partition
transforms (`catalog.py:95-98`).  A boolean column cannot be used for
partition pruning without a bucket or identity transform explicitly added
to the partition spec.

**Conclusion**: The plan's rejection of the "deleted" column approach is
sound.

---

### 2. "Hash index tracks all item IDs in catalog"

**VERIFIED — Correct.**

`earthcatalog/hash_index.py:41-43`: `hash_id(item_id)` produces an
`xxh3_128` 16-byte digest.  `write_hashes()` at `hash_index.py:103-115`
writes a sorted Parquet file with a single `id_hash` column.
`read_hashes()` at `hash_index.py:46-61` loads it into a `set[bytes]`.

The hash index is updated during ingest at `catalog.py:1064-1085` via
`merge_hashes_from_parquets()`, which reads the `id` column from newly
written GeoParquet files, hashes each value, and unions into the existing
set.

**Conclusion**: The hash index correctly tracks all item IDs.  The plan's
use of it as catalog-truth is valid.

---

### 3. "Hash index has no file→ID mapping"

**VERIFIED — Correct.**

The hash index schema (`hash_index.py:110`) is a single column:
`pa.binary(16)` named `id_hash`.  There are no columns for file paths,
item IDs (plaintext), or any other metadata.  The current hash index
cannot answer "which files contain item X?" — it can only answer "is item
X in the catalog?"

**Conclusion**: The plan correctly identifies this as a gap requiring new
code (file metadata tracking).  This is a prerequisite for targeted file
rewrites.

---

### 4. "S3 Inventory as source of truth for objects in S3"

**VERIFIED — Correct.**

`earthcatalog/pipelines/incremental.py:427-460` (`_iter_inventory`):
Supports CSV, CSV.gz, Parquet, and manifest.json S3 inventory formats.
Yields `(bucket, key)` pairs.  The `since` parameter filters on
`last_modified_date`.

The ingest loop at `catalog.py:1043-1056` consumes these pairs, fetches
the STAC JSON for each `.stac.json` key, and processes it.

**Conclusion**: The plan's reliance on S3 Inventory as the S3-truth
source is correct.  However, see **Critical Gap #1** below.

---

### 5. "File→ID mapping needs to be tracked during ingest"

**VERIFIED — Correct.**

Current ingest at `catalog.py:1033-1039` writes GeoParquet files via
`write_geoparquet_s3()`.  The written keys are collected in `written_keys`
and later registered with `table.add_files()` at line 1061.  No metadata
about which item IDs went into which file is persisted anywhere.

**Conclusion**: The plan correctly identifies the need to add file
metadata tracking to the ingest pipeline.  A natural insertion point is
after `write_geoparquet_s3()` returns at `catalog.py:1037`, where both the
file key and the group items (with their IDs) are in scope.

---

### 6. "Compaction is independent of delete workflow"

**VERIFIED — Correct.**

`earthcatalog/maintenance/compact.py:200-356` (`compact_warehouse`):
Operates purely on GeoParquet files in the warehouse.  Reads files,
deduplicates by `id`, sorts, writes compacted output
(`compact.py:113-140`), deletes old part files (`compact.py:151-155`),
and rebuilds the Iceberg catalog by drop+recreate
(`compact.py:330-346`).

The compaction code has zero awareness of S3 Inventory, hash index, or
file metadata.  It is strictly a warehouse-level operation.

**Conclusion**: Running compaction before or after deletion is safe.
Both operations are idempotent and operate on independent state.
Compaction's drop+recreate of the Iceberg table is a concern (see
**Critical Gap #2** below).

---

### 7. "Iceberg properties store hash index path"

**VERIFIED — Correct.**

`catalog.py:60`: `PROP_HASH_INDEX_PATH = "earthcatalog.hash_index_path"`
is set as a table property.  At `catalog.py:1065-1069`, during ingest with
`update_hash_index=True`, the property is written via
`tx.set_properties()`.  At `catalog.py:290`, `unique_item_count()` reads it
from `table.properties`.

The hash index path default is `{warehouse_root}_id_hashes.parquet`
(`catalog.py:924`, `backfill.py:889`).

**Conclusion**: The plan's assumption that the hash index path is
accessible from table properties is correct.

---

### 8. "Three-phase approach with rollback"

**PARTIALLY VERIFIED — Conceptually sound, but implementation details
need refinement.**

The three phases are:
1. **Plan** (read-only) — safe to retry
2. **Backup** — snapshot current state
3. **Execute** — write operations with rollback on failure

The plan correctly identifies that `find_orphaned_items()` (read-only)
and `rewrite_file_without_orphans()` (independent per-file) don't need
rollback.  The `update_hash_index()` step is correctly identified as the
critical point requiring rollback.

**Issues**:

a) **Backup granularity**: The plan backs up the entire hash index and
   file metadata before any writes.  For a large catalog with billions of
   items, this could consume significant memory/time.  A lighter
   alternative: backup only the orphaned entries (remove-backup), since
   those are the only things being modified.

b) **Partial execution recovery**: If `rewrite_file_without_orphans()`
   writes 3 out of 5 files before failing, the plan rolls back the hash
   index but does NOT restore the already-rewritten GeoParquet files.
   This means the files on disk have been modified (orphans removed), but
   the hash index still includes them.  On the next garbage collection
   run, the files will be re-read and the already-removed orphans will
   have no effect (idempotent), so this is safe but wasteful.

c) **Iceberg table registration**: The plan does not address how
   `table.add_files()` is called for rewritten files.  After rewriting a
   file at the same path, PyIceberg may not recognize it as a new file.
   Current code in `compact.py` handles this by drop+recreate of the
   table.  The plan should specify whether to:
   - Overwrite in-place (same path, no Iceberg change needed — lazy,
     relies on snapshot isolation), OR
   - Write to a new path, add to Iceberg, delete old path (safe, atomic)

   **Recommendation**: Overwrite in-place is simpler and correct because
   Iceberg's MVCC model means existing snapshots keep pointing at old
   file metadata.  After rewrite, a new snapshot should be created via
   `table.add_files()` with the new files (same paths, new content).

**Conclusion**: The three-phase structure is sound.  The rollback
mechanism is adequate — partial file rewrites are idempotent, and hash
index restoration prevents orphaned items from being permanently lost in
the metadata.

---

### 9. "Idempotency of each step"

**VERIFIED — Correct.**

- `find_orphaned_items()` — read-only, always idempotent.
- `rewrite_file_without_orphans()` — filtering removes the same IDs each
  time. If orphans are already removed, the filter is a no-op.
- `update_hash_index()` — set difference `{k:v for k,v in hashes if k not in orphaned}`
  is idempotent. Removing an already-removed hash is a no-op.
- `update_file_metadata()` — same logic, idempotent.

**Conclusion**: Correct.  Retry safety is guaranteed.

---

### 10. "Store interface uses `obstore`"

**VERIFIED — Correct.**

All I/O in the codebase goes through `obstore`:
- `obstore.get(store, key)` — `hash_index.py:53`, `catalog.py:397`
- `obstore.put(store, key, data)` — `hash_index.py:114`, `catalog.py:413`
- `obstore.delete(store, key)` — `lock.py:136`, `compact.py:153`
- `obstore.list(store, prefix=...)` — `compact.py:178`

Tests use `MemoryStore` from `obstore.store` (`test_hash_index.py:10`).
The plan's pseudo-code uses `obstore.put(store, key, data)` which matches
the codebase pattern.

**Conclusion**: Consistent with existing codebase conventions.

---

### 11. "Iceberg table schema and partition spec"

**VERIFIED — Correct.**

`catalog.py:63-92` defines `ICEBERG_SCHEMA` with 28 fields including
`id`, `grid_partition`, `geometry`, `datetime`, etc.
`catalog.py:95-98` defines `PARTITION_SPEC` with `IdentityTransform` on
`grid_partition` and `YearTransform` on `datetime`.

The plan correctly references these when discussing why a "deleted"
column wouldn't help with partition pruning.

**Conclusion**: Correct.

---

### 12. "Test patterns"

**VERIFIED — Correct.**

Existing tests use `MemoryStore` and `tmp_path` fixtures.  For example,
`test_hash_index.py:40` uses `store = MemoryStore()` and
`write_hashes(hashes, store, "idx.parquet")`.  Integration tests like
`test_hash_index.py:126` use `_open_sqlite()` and `register_delta()`.

The plan's test proposals follow these patterns.  However, Test 11
(`test_rollback_on_failure`) uses an assertion pattern (`assert "orphaned"
in result`) that doesn't match the plan's own `run_garbage_collection()`
implementation which re-raises exceptions rather than returning a result
dict on failure.

**Conclusion**: Test patterns match, but Test 11's expected behavior
needs to be aligned with the actual rollback implementation.

---

## Critical Gaps in the Plan

### Critical Gap #1: Keyspace Mismatch Between Hash Index and S3 Inventory

The hash index stores `xxh3_128(id)` hashes of STAC item `id` properties
(e.g., `"landsat_oli_lc08_L1TP_045029_20200715_20200722_01_T1"`).
The S3 Inventory yields `(bucket, key)` pairs of `.stac.json` object paths
(e.g., `"its-live-data"`, `"inventory/.../item.stac.json"`).

**These are different keyspaces and cannot be directly compared.**

The plan says:
> "compare current hash index vs current S3 inventory"

This is impossible without a mapping.  There are two approaches:

**Approach A (expensive)**: On every garbage collection run, iterate
through the entire S3 inventory, fetch every STAC JSON, extract its `id`,
hash it, and build `inventory_hash_set`.  Then `catalog_hashes -
inventory_hashes` = orphaned hashes.  This is prohibitively expensive for
catalogs with billions of items.

**Approach B (requires new code)**: During ingest, store a mapping from
S3 key → STAC item ID (or hash).  On deletion, read current inventory
keys, find stored keys that are absent, look up their IDs → orphaned.
This requires adding a mapping file during ingest.

**Recommendation**: Approach B is preferred.  Add a `key_to_id.parquet`
or similar that is written during ingest.  The delete pipeline then:
1. Read current S3 inventory keys → `current_keys`
2. Read stored key→ID mapping → `stored_keys`, `stored_mapping`
3. `missing_keys = stored_keys - current_keys`
4. `orphaned_ids = {stored_mapping[k] for k in missing_keys}`

This avoids fetching any STAC items during garbage collection.

---

### Critical Gap #2: Iceberg Catalog Registration After File Rewrite

The plan says:
> "Rewrite those files (filter out deleted IDs)"
> "Register new files in Iceberg (new snapshot)"

The current `compact.py` handles catalog updates by **drop+recreate** of
the entire Iceberg table and re-registering all surviving files
(`compact.py:330-346`).  This is a "repair table" pattern.

For the delete workflow, the plan should specify one of:

**Option 1 (in-place, MVCC)**: Overwrite files at the same path.  Iceberg
readers will see the old content until a new snapshot is created.  Call
`table.add_files()` with the same paths (which should create a new entry
pointing at the new file content since the files were rewritten).
*Risk: PyIceberg may cache file-level metadata and not detect the change.*

**Option 2 (new path, drop old)**: Write filtered content to a new path
(`_filtered` suffix), call `table.add_files()` with new paths, call
`table.delete_files()` with old paths, then delete old files from store.
*Risk: more complex, but explicitly safe.*

**Recommendation**: Option 2 is safer.  The plan should address this
explicitly, including the Iceberg catalog update procedure.

---

### Critical Gap #3: Reverse-Lookup from Hash to ID

The hash index stores one-way hashes.  After finding orphaned hashes
(`catalog_hashes - inventory_hashes`), the plan needs to map these
hashes back to original STAC item IDs in order to:
a) Filter them out of GeoParquet files (need the original `id` string)
b) Remove them from file metadata (need the original `id` string)

The plan's file metadata (`file_to_items`) stores original IDs per file,
which solves this.  But the orphan detection step in the plan is:
> `orphaned = find_orphaned_items(inventory_path, catalog_path, store)`

It's unclear whether `find_orphaned_items` returns hashes or original IDs.
The plan's later code uses `orphaned` as both:
- A set of hashes for hash index update: `if k not in orphaned`
- A set of IDs for file filtering: `df[~df['id'].isin(orphaned_ids)]`

These must be the same type.  If `orphaned` is a set of IDs (not hashes),
then the hash index update needs to be: `if hash_id(k) not in
orphaned_hashes`.  The plan needs to clarify this.

**Recommendation**: Store original IDs alongside hashes in the hash index
(schema: `[id_hash, id]`), or maintain a separate ID→hash lookup.  This
way orphan detection returns IDs, and the hash index update can
reconstruct hashes from IDs.

Alternatively, store the file metadata with both IDs and hashes so that
the entire flow works with IDs, only converting to hashes at the final
hash index write step.

---

### Critical Gap #4: No Mechanism to Obtain STAC IDs from S3 Inventory

During a garbage collection run, you need the set of STAC item IDs that
currently exist in S3.  The S3 Inventory gives `(bucket, key)` pairs, not
STAC IDs.  To get STAC IDs from the inventory, you must fetch every
`.stac.json` file and parse its `id` field.

This means the garbage collection I/O cost scales with the **size of the
current inventory**, not the number of orphans.  For a 1B-item catalog,
reading all STAC JSONs to find 100 orphans wastes enormous resources.

**Recommendation**: Store the mapping from S3 key → STAC item ID during
ingest.  This is a one-time write per ingested item, and makes deletion a
set-difference on keys (cheap), not a full re-scan.

---

## Summary

| Assertion | Verdict | Notes |
|-----------|---------|-------|
| Iceberg is file-level pruning only | **PASS** | `search.py:100-152` |
| Hash index tracks all item IDs | **PASS** | `hash_index.py:41-115` |
| No file→ID mapping exists | **PASS** | Hash index is single-column |
| S3 Inventory as truth | **PASS** | `incremental.py:427-460` |
| File metadata needed | **PASS** | Gap in current codebase |
| Compaction is independent | **PASS** | `compact.py:200-356` |
| Hash index path in properties | **PASS** | `catalog.py:60,1065` |
| Three-phase with rollback | **PASS** (needs detail) | Conceptually sound |
| Idempotency of each step | **PASS** | Set-difference is idempotent |
| Store interface is obstore | **PASS** | Consistent with codebase |
| Schema/partition spec | **PASS** | `catalog.py:63-98` |
| Test patterns | **PASS** (minor fix needed) | Test 11 assertion mismatch |

**Overall**: The plan is architecturally sound and correctly identifies
the key gaps in the current codebase.  The four critical gaps above
must be resolved during implementation, but they are implementation
details rather than fundamental flaws in the approach.

### Priority Fixes Before Implementation

1. **Keyspace mapping** (Critical Gap #1): Add S3 key → STAC ID mapping
   during ingest.  Without this, garbage collection requires
   re-downloading all STAC items on every run.

2. **Iceberg registration** (Critical Gap #2): Specify the
   `table.add_files()` / `table.delete_files()` protocol for rewritten
   files, or adopt the drop+recreate pattern from compact.py.

3. **Hash→ID reverse lookup** (Critical Gap #3): Clarify whether
   `find_orphaned_items()` returns IDs or hashes, and ensure the
   subsequent steps use consistent types.

4. **Stored ID retrieval** (Critical Gap #4): Add a `key_to_id.parquet`
   written during ingest so the delete pipeline can detect orphans
   without fetching STAC items.
