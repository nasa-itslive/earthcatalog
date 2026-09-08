# Delete / Garbage Collection Plan — v2 (Bloom Filter)

## Problem Restated

- **Catalog**: 45M STAC items in ~3,000 GeoParquet files (hive-partitioned by
  `grid_partition` + `year`).  ~15k rows/file average.
- **Ingest rate**: ~20k new items/day → ~140k/week → ~10 new GeoParquet
  files/week.
- **Deletion rate**: ~500–2,000 items/month, spatially clustered (same
  geographic area).  Rare compared to catalog size.
- **Hardware**: GitHub Actions runner (2 cores, ~7 GB RAM, limited CPU).
- **Schedule**: Both ingest and garbage collection run weekly, same job.

## Why the Original Plan (v1) Doesn't Fit

The v1 plan assumed comparing "catalog IDs" vs "inventory IDs" directly.
This requires either:

1. Fetching 45M STAC JSONs from S3 on every run to get IDs → **impossible
   on a GitHub runner** (hours of I/O, terabytes of data).
2. Maintaining a full ID→ID mapping in memory → **45M string IDs exceed
   available RAM**.
3. Maintaining file→ID metadata for all 45M items → **complex, needs
   constant maintenance, and doesn't solve the core problem**.

The v1 plan also didn't address: how do you map S3 inventory keys
(`s3://bucket/path/item.stac.json`) to STAC item IDs
(`landsat_oli_lc08_L1TP_...`)? These are independent namespaces — the S3
key is where the item lives, the STAC ID is what it calls itself.  You
need a stored mapping to bridge them without re-downloading every STAC
JSON.

## Key Insight

**We don't need to know which STAC IDs exist in S3.  We need to know
which S3 _keys_ still exist.**  If a key we ingested from is no longer in
the inventory, its STAC item is orphaned regardless of the STAC ID.

This shifts the problem from "compare 45M STAC IDs" to "compare 45M S3
keys" — same cardinality, but the S3 key is already available from the
inventory without fetching any JSON.

The challenge: 45M keys × ~100 bytes/key → 4.5 GB — doesn't fit in GitHub
runner memory.  Solution: **Bloom filter**.

## Architecture

### New Data: `source_index.parquet`

Written during ingest, one extra call per chunk.  Schema:

| Column          | Type   | Description                                    |
|-----------------|--------|------------------------------------------------|
| `s3_key`        | string | Full `s3://bucket/key` path to the STAC JSON   |
| `stac_id`       | string | The STAC item `id` property                     |
| `grid_partition`| string | H3 cell (partition key)                         |
| `year`          | int32  | Year from datetime (for partition path lookup)  |
| `ingested_at`   | string | ISO timestamp of ingest                         |
| `deleted`       | bool   | `True` if confirmed deleted (default `False`)   |

**Size**: ~140k rows/week appended.  After 1 year: ~7M rows.  After 5
years: ~36M rows.  Compressed Parquet: ~50–100 bytes/row → ~2–4 GB
total.  Colocated with the hash index at
`{warehouse_root}_source_index.parquet`.

### GC Algorithm (runs weekly, after ingest)

```
Phase 1 — Build Bloom filter (in-memory, ~80 MB)
  Stream S3 Inventory CSV/Parquet →
    for each .stac.json key, insert s3://bucket/key into Bloom filter
  Parameters: 45M expected items, 0.001% false-positive rate
  → ~80 MB memory, ~30 seconds I/O

Phase 2 — Find candidate deletions
  Stream source_index.parquet (skip rows WHERE deleted = true) →
    for each s3_key:
      if NOT in Bloom filter → candidate
  Expected: ~500 true deletions + ~45 false positives
  → ~30 seconds I/O

Phase 3 — Confirm candidates (eliminate Bloom false positives)
  For each candidate s3_key:
    HEAD request to S3 (obstore.head)
    If 404 → confirmed orphan
  Concurrency: 64 simultaneous HEADs
  → ~1 second for 545 candidates

Phase 4 — Execute cleanup (only if orphans found)
  For each confirmed orphan (stac_id, grid_partition, year):
    a) Find affected GeoParquet files:
       List files under warehouse/grid_partition=<cell>/year=<year>/
       Read each file's id column → check if stac_id is present
       → affected_files set
    b) For each affected file:
       Read full file → filter out orphaned stac_ids → write to new path
       Register new file in Iceberg (table.add_files)
       Delete old file from store + Iceberg
    c) Update hash index: remove hash_id(stac_id) from set → write back
    d) Mark source_index rows as deleted = true
  → Seconds to minutes (depends on #affected files)

Phase 5 — Periodic index compaction (rare, manual)
  Read source_index.parquet → filter deleted=true → write back
  Only needed after many deletion cycles (10k+ deleted rows)
```

### Memory Budget (GitHub Runner)

| Component            | Size      | Notes                              |
|----------------------|-----------|------------------------------------|
| Bloom filter         | ~80 MB    | 45M items, 0.001% FPR, 10 hash fns |
| Parquet batch buffer | ~8 MB     | 100k-row streaming batches         |
| Candidate set        | ~1 KB     | ~545 candidates                    |
| PyIceberg catalog    | ~50 MB    | In-memory SQLite                   |
| **Total**            | **~140 MB**| Well within 7 GB limit             |

### Time Budget (GitHub Runner)

| Phase                | Time    | Bottleneck    |
|----------------------|---------|---------------|
| Build Bloom filter   | ~30 s   | I/O (read inventory) |
| Scan source_index    | ~30 s   | I/O (read Parquet)   |
| Confirm candidates   | ~1 s    | Network (HEADs)      |
| Rewrite files        | ~5 s    | I/O per file         |
| **Total (typical)**  | **~1–2 min** |              |

## Implementation Changes

### 1. `earthcatalog/source_index.py` (new)

```python
def append_source_index(rows, store, key):
    """Append rows to source_index.parquet. Creates if not exists."""
    # rows: list of (s3_key, stac_id, grid_partition, year)
    import io
    import obstore
    import pyarrow as pa
    import pyarrow.parquet as pq

    tbl = pa.table({
        "s3_key":       [r[0] for r in rows],
        "stac_id":      [r[1] for r in rows],
        "grid_partition": [r[2] for r in rows],
        "year":         pa.array([r[3] for r in rows], type=pa.int32()),
        "ingested_at":  pa.array([datetime.now(UTC).isoformat()] * len(rows)),
        "deleted":      pa.array([False] * len(rows)),
    })

    try:
        raw = bytes(obstore.get(store, key).bytes())
        existing = pq.ParquetFile(io.BytesIO(raw)).read()
        merged = pa.concat_tables([existing, tbl])
        buf = io.BytesIO()
        pq.write_table(merged, buf, compression="zstd")
        obstore.put(store, key, buf.getvalue())
    except FileNotFoundError:
        buf = io.BytesIO()
        pq.write_table(tbl, buf, compression="zstd")
        obstore.put(store, key, buf.getvalue())

def mark_deleted(stac_ids, store, key):
    """Mark rows as deleted=true for given stac_ids."""
    import obstore
    raw = bytes(obstore.get(store, key).bytes())
    tbl = pq.ParquetFile(io.BytesIO(raw)).read()
    id_col = tbl.column("stac_id").to_pylist()
    deleted_col = list(tbl.column("deleted").to_pylist())
    for i, sid in enumerate(id_col):
        if sid in stac_ids:
            deleted_col[i] = True
    tbl = tbl.set_column(
        tbl.schema.get_field_index("deleted"),
        "deleted",
        pa.array(deleted_col, type=pa.bool_())
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())

def compact_source_index(store, key):
    """Remove deleted rows, rewrite file."""
    raw = bytes(obstore.get(store, key).bytes())
    tbl = pq.ParquetFile(io.BytesIO(raw)).read()
    mask = pc.invert(pc.equal(tbl.column("deleted"), True))  # noqa
    tbl = tbl.filter(mask)
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
```

### 2. `earthcatalog/catalog.py` — Ingest Hook

After `write_geoparquet_s3()` at `catalog.py:1037`, add:

```python
# Track source keys for garbage collection
if written_keys and self._store:
    source_rows = []
    for item in group_items:
        # item stores source bucket/key during fan_out + fetch
        s3_key = f"s3://{item['_source_bucket']}/{item['_source_key']}"
        source_rows.append((s3_key, item["id"], cell, year_int))
    from earthcatalog.source_index import append_source_index
    si_key = f"{warehouse_root.rstrip('/')}_source_index.parquet"
    append_source_index(source_rows, self._store, si_key)
```

Note: This requires threading the source `(bucket, key)` through
`fan_out()` and `group_by_partition()`.  Currently `_fetch_item()` returns
just the JSON dict — it needs to also return the source metadata.

### 3. `earthcatalog/pipelines/delete.py` (new)

Main GC pipeline:

```python
def run_garbage_collection(inventory_path, store, warehouse_root,
                           hash_index_key, source_index_key):
    """
    Weekly garbage collection using Bloom filter approach.

    Returns {"orphaned": N, "files_rewritten": N, "bloom_size_mb": N}
    """
    # Phase 1: Build Bloom filter from S3 Inventory
    bloom = build_inventory_bloom(inventory_path)

    # Phase 2: Find candidates from source index
    candidates = find_deletion_candidates(store, source_index_key, bloom)

    # Phase 3: Confirm via HEAD requests
    orphans = confirm_deletions(candidates, concurrency=64)

    if not orphans:
        return {"orphaned": 0, "files_rewritten": 0}

    # Phase 4: Execute cleanup
    _execute_cleanup(orphans, store, warehouse_root,
                     hash_index_key, source_index_key)
```

### 4. Iceberg Registration After Rewrite

For each affected GeoParquet file, use **write-to-new-path +
swap** to avoid in-place overwrite race conditions:

```python
def rewrite_file_without_orphans(file_key, orphaned_ids, store):
    """Rewrite file to new path, return (new_key, old_key)."""
    new_key = file_key.replace(".parquet", "_gc.parquet")

    raw = bytes(obstore.get(store, file_key).bytes())
    df = pq.ParquetFile(io.BytesIO(raw)).read()
    mask = pc.invert(pc.is_in(df.column("id"),
                     pa.array(list(orphaned_ids))))
    cleaned = df.filter(mask)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        pq.write_table(cleaned, tmp.name, compression="zstd")
        data = Path(tmp.name).read_bytes()
    Path(tmp.name).unlink(missing_ok=True)

    obstore.put(store, new_key, data)
    return new_key, file_key
```

Then register with Iceberg:
```python
def _execute_cleanup(orphans, store, warehouse_root,
                     hash_index_key, source_index_key):
    # 1. Group orphans by (grid_partition, year)
    # 2. For each (cell, year): find affected GeoParquet files
    # 3. For each affected file: rewrite → new path
    # 4. table.add_files(new_paths)
    # 5. table.delete_files(old_paths)
    # 6. obstore.delete(old_paths)
    # 7. Update hash index
    # 8. Mark source_index rows as deleted=true
```

## Failure Handling (Simplified)

Since each step is idempotent and GitHub runners are ephemeral, we don't
need full three-phase rollback.  Instead:

- **Phase 1–2**: Read-only, safe to retry.
- **Phase 3 (HEAD confirmations)**: Read-only, safe to retry.
- **Phase 4 (rewrite files)**: Each file rewrite is atomic (write to new
  key, then register).  If the runner dies mid-way:
  - Already-rewritten files have `_gc.parquet` suffix — safe orphan.
  - Iceberg hasn't been updated yet — catalog still points at old files.
  - On retry, the old `_gc.parquet` files are overwritten (idempotent).
- **Phase 4 (hash index)**: If this fails, the hash index still contains
  orphaned hashes — safe, just means dedup won't catch them.  Next run
  will retry.
- **Phase 4 (source index)**: `mark_deleted` is idempotent.

**No rollback needed** — every step is safe to retry or no-op.

## What Gets Removed from v1

- ~~Three-phase rollback with backup/restore~~ — not needed with
  idempotent steps and new-path writes.
- ~~`file_to_items` metadata~~ — replaced by `source_index.parquet` which
  already contains `grid_partition` + `year` for direct file lookup.
- ~~`get_files_for_ids()`~~ — instead, list files in affected partitions
  and check `id` column.
- ~~`update_file_metadata_after_delete()`~~ — source index uses a
  `deleted` boolean column instead.
- ~~Full hash index backup~~ — only backup the orphaned hashes (set of ~500
  bytes), not the entire 45M-row index.

## Tests

### `tests/test_source_index.py`

- `test_append_creates_file` — first append creates source_index.parquet
- `test_append_extends_file` — second append adds rows to existing file
- `test_mark_deleted` — marks specific rows, others unchanged
- `test_compact_removes_deleted` — compact filters deleted=true rows
- `test_stream_skip_deleted` — streaming skips deleted rows

### `tests/test_delete.py`

- `test_bloom_build_from_inventory` — builds filter, checks known keys
- `test_find_candidates_finds_orphans` — missing keys → candidates
- `test_find_candidates_skips_deleted` — deleted rows are skipped
- `test_confirm_deletions_404` — HEAD 404 → confirmed orphan
- `test_confirm_deletions_200` — HEAD 200 → false positive (Bloom)
- `test_rewrite_removes_orphans` — file rewritten without orphan IDs
- `test_e2e_gc_pipeline` — full cycle: inventory → bloom → candidates →
  confirm → rewrite → hash update
- `test_no_orphans_is_noop` — clean inventory → zero orphaned
- `test_idempotent_rewrite` — rewriting without orphans is safe to retry

All tests use `MemoryStore` + `tmp_path`, consistent with existing tests.

## Scheduling

```bash
# Weekly: ingest then GC (same job)
0 2 * * 0 earthcatalog ingest --inventory s3://.../inventory/... \
    --base s3://bucket/catalog --update-hash-index \
    --update-source-index \
    && earthcatalog gc --base s3://bucket/catalog
```

Or from Python:
```python
ec = EarthCatalog.open(store=store, base="s3://bucket/catalog")
ec.ingest(inventory_path, update_hash_index=True, update_source_index=True)
ec.garbage_collect(inventory_path)
```

## Idempotency

| Step | Safe to retry? | Why |
|------|---------------|-----|
| Build Bloom filter | Yes | Read-only |
| Find candidates | Yes | Read-only |
| HEAD confirmations | Yes | Read-only |
| Rewrite file | Yes | Writes to `_gc.parquet`, overwrites on retry |
| Iceberg add/delete | Yes | Idempotent — adding same path twice is a no-op |
| Delete old file from store | Yes | Missing file is not an error |
| Update hash index | Yes | Set difference is idempotent |
| Mark source index deleted | Yes | Already-deleted rows are no-op |

## Compaction Safety

Compaction (`compact_warehouse`) is independent.  If it runs between
ingest and GC:

- **Ingest → Compact → GC**: Compacted files contain both existing and
  newly-ingested items.  GC will find orphans and rewrite the compacted
  files.  Safe.
- **GC → Compact**: GC rewrites files with `_gc.parquet` suffix.  Compactor
  won't touch these (they don't match the hive pattern).  Next cycle, the
  `_gc` files are the canonical ones.  Before compaction, GC should clean
  up stale `_gc` files from previous runs.

**Recommendation**: Add a pre-GC step to delete any leftover `_gc.parquet`
files (from a crashed previous run) and ensure the Iceberg table only
points at canonical (non-`_gc`) paths.

## Migration Path

For the 45M items already in the catalog that lack `source_index.parquet`
entries:

1. **Backfill script** (run once): Stream all GeoParquet files, read
   `id` column, populate `source_index.parquet` with empty `s3_key`
   (we don't know the original S3 keys for pre-existing items).
2. **Grace period**: For the first few weeks, only detect orphans from
   items ingested AFTER the source index was enabled (non-empty `s3_key`).
3. After the grace period (~1 month, since deletions are monthly), all
   items in the catalog have `source_index` entries with real S3 keys.

**Risk**: Items ingested before the source index existed cannot be
detected as deleted.  This is acceptable because:
- Most deletions are recent data (the user said "a few thousand a month").
- After 1 month, the window is closed — all catalog items have entries.
- If a backfill script is run to populate `s3_key` from the original
  inventory data, this gap disappears entirely.

## Implementation Plan

### Build Order (dependency chain)

```
Step 1: earthcatalog/source_index.py       ← no deps, pure I/O
Step 2: Inject source metadata into fetch   ← unblocks Step 3
Step 3: Hook source_index append into ingest ← needs Step 1 + 2
Step 4: earthcatalog/pipelines/delete.py    ← needs Step 1
Step 5: earthcatalog/catalog.py GC method   ← wraps Step 4
Step 6: tests/test_source_index.py          ← needs Step 1
Step 7: tests/test_delete.py               ← needs Step 4
Step 8: Backfill source_index for 45M items ← needs Step 1
```

---

### Step 1: `earthcatalog/source_index.py`

New file.  Independent of everything else — pure Parquet I/O via `obstore`.

Functions:

| Function | Signature | Description |
|----------|-----------|-------------|
| `append_source_index` | `(rows: list[tuple[str,str,str,int]], store, key) -> int` | Append rows to source_index.parquet. Creates if not exists. Returns new row count. |
| `mark_deleted` | `(stac_ids: set[str], store, key) -> int` | Set `deleted=True` for matching stac_ids. Returns number of rows marked. |
| `compact_source_index` | `(store, key) -> int` | Remove `deleted=True` rows, rewrite. Returns rows kept. |
| `stream_active` | `(store, key) -> Iterator[dict]` | Generator yielding non-deleted rows as dicts: `{s3_key, stac_id, grid_partition, year}`. |

Schema: `s3_key: string, stac_id: string, grid_partition: string, year: int32, ingested_at: string, deleted: bool`

Implementation: Append is read+concat+write (inefficient for large files, but
140k rows/week is small).  If perf becomes an issue, switch to appending
separate part files and compacting them.

---

### Step 2: Inject source bucket/key into fetch functions

**Why**: `source_index.parquet` needs `s3_key` for every item.  Currently
`_fetch_item` returns only the JSON body — the source `(bucket, key)` is
discarded.

**What**: Inject `_source_bucket` and `_source_key` as top-level keys in
the returned dict.  These propagate through `fan_out()` (copied by
`{**item, ...}` at `transform.py:122`) and `group_by_partition()` (no
filtering at `transform.py:159`).

**Files to modify**:

| File | Line | Change |
|------|------|--------|
| `earthcatalog/pipelines/incremental.py` | 471 | Replace `return json.loads(bytes(raw))` with augmented dict |
| `earthcatalog/pipelines/backfill.py` | 319 | Same for `_fetch_item_async` |
| `earthcatalog/pipelines/backfill.py` | 411 | In `ingest_chunk`, pairs from chunk Parquet carry `(bucket, key)` — already threaded to `_fetch_all_async` |

**Change in `incremental.py:468-474`**:
```python
def _fetch_item(bucket: str, key: str) -> dict | None:
    try:
        raw = obstore.get(_get_store(bucket), key).bytes()
        item = json.loads(bytes(raw))
        item["_source_bucket"] = bucket
        item["_source_key"] = key
        return item
    except Exception as exc:
        print(f"WARN: failed to fetch s3://{bucket}/{key}: {exc}")
        return None
```

**Change in `backfill.py:318-319`** (inside `_fetch_item_async`):
```python
        item = orjson.loads(raw)
        item["_source_bucket"] = bucket
        item["_source_key"] = key
        return item
```

`bucket` and `key` are already parameters of `_fetch_item_async` at line 296.
The `bucket` comes from `pairs[0][0]` at line 426 and is the same for all
items in a chunk (single-bucket chunks).

---

### Step 3: Hook source_index into ingest pipelines

Three integration points, one helper call each.

#### 3a. `catalog.py` — `EarthCatalog.ingest()` (single-node path)

**Location**: `catalog.py:1038`, after `write_geoparquet_s3()` returns.

Variables in scope: `s3_key` (warehouse key), `group_items` (list of dicts with
`id`, `_source_bucket`, `_source_key`), `cell`, `year`, `self._store`.

```python
# After line 1037, inside the if n > 0 block at line 1038-1040:
if n > 0:
    written_keys.append(s3_key)
    total_rows += n
    # NEW: track source keys
    if self._source_index_store is not None:
        source_rows = [
            (f"s3://{it['_source_bucket']}/{it['_source_key']}",
             it["id"], cell, int(year or 0))
            for it in group_items if "_source_key" in it
        ]
        if source_rows:
            from earthcatalog.source_index import append_source_index
            append_source_index(source_rows, self._source_index_store,
                              self._source_index_key)
```

New attribute on `EarthCatalog`: `_source_index_store` and `_source_index_key`,
initialized in `__init__` or `open()`.

#### 3b. `incremental.py` — `run()` (legacy single-node path)

**Location**: `incremental.py:574`, after `write_geoparquet()`.

Variables in scope: `out_path` (local path), `group_items`, `cell`, `year`.

Same logic as 3a but using `LocalStore` instead of S3 store.  This path is
legacy but should be updated for consistency.

#### 3c. `backfill.py` — `_stream_compact()` (Dask/bulk path)

**Location**: `backfill.py:560`, inside `write_fn(batch, out_path)` call.

The `compact_cell_year` / `compact_cell_year_delta` callers pass a custom
`write_fn`. Wrap the existing write function:

```python
def _write_with_source_index(batch, key, store, source_index_store, source_index_key, cell, year):
    n = _write_parquet_to_store(batch, store, key)
    if n > 0:
        source_rows = [
            (f"s3://{it['_source_bucket']}/{it['_source_key']}",
             it["id"], cell, int(year or 0))
            for it in batch if "_source_key" in it
        ]
        if source_rows:
            from earthcatalog.source_index import append_source_index
            append_source_index(source_rows, source_index_store, source_index_key)
    return n
```

Passed as `write_fn` in `compact_cell_year` (line 622) and
`compact_cell_year_delta` (line 683).

---

### Step 4: `earthcatalog/pipelines/delete.py`

New file.  Core GC pipeline.

```python
def run_garbage_collection(
    inventory_path: str,
    catalog_path: str = None,
    *,
    store: object,
    warehouse_root: str,
    hash_index_key: str,
    source_index_key: str,
    bloom_error_rate: float = 0.0001,
    head_concurrency: int = 64,
    dry_run: bool = False,
) -> dict:
```

Calls (in order):

1. `_build_inventory_bloom(inventory_path, error_rate)` → `pybloom_live.BloomFilter`
2. `_find_candidates(store, source_index_key, bloom)` → `list[(s3_key, stac_id, grid_partition, year)]`
3. `_confirm_with_head(candidates, concurrency)` → `list[(stac_id, grid_partition, year)]`
4. `_execute_cleanup(orphans, store, warehouse_root, hash_index_key, source_index_key, catalog_path, dry_run)` → `dict`

Helper functions per file:

```python
def _build_inventory_bloom(inventory_path, error_rate):
    """Stream S3 Inventory, insert every .stac.json key into Bloom filter."""
    ...

def _find_candidates(store, source_index_key, bloom):
    """Stream source_index, return rows where s3_key NOT in bloom."""
    ...

async def _head_one(store, s3_url):
    """Return True if object exists."""
    ...

def _confirm_with_head(candidates, concurrency):
    """HEAD each candidate s3_key. Return only confirmed-absent ones."""
    ...

def _execute_cleanup(orphans, store, warehouse_root,
                     hash_index_key, source_index_key, catalog_path, dry_run):
    """Group orphans by (grid_partition, year), rewrite files, update indices."""
    ...
```

File rewriting uses the existing `_write_parquet_to_store` pattern from
`backfill.py:585` — write to local temp file, upload via obstore.

Iceberg registration: `table.add_files(new_paths)` then `table.delete_files(old_paths)`,
same pattern as existing code but without drop+recreate.

---

### Step 5: `earthcatalog/catalog.py` — `EarthCatalog.garbage_collect()`

New method on `EarthCatalog`:

```python
def garbage_collect(self, inventory_path: str, *, dry_run: bool = False) -> dict:
    from earthcatalog.pipelines.delete import run_garbage_collection

    warehouse_root = self._catalog.properties.get("warehouse", "")
    hash_index_key = self._table.properties.get("earthcatalog.hash_index_path", "")
    if hash_index_key.startswith("s3://"):
        hash_index_key = hash_index_key.removeprefix("s3://").split("/", 1)[1]

    source_index_key = f"{warehouse_root.rstrip('/')}_source_index.parquet"
    if source_index_key.startswith("s3://"):
        source_index_key = source_index_key.removeprefix("s3://").split("/", 1)[1]

    catalog_path = self._catalog.properties.get("uri", "").removeprefix("sqlite:///")

    return run_garbage_collection(
        inventory_path=inventory_path,
        catalog_path=catalog_path,
        store=self._store,
        warehouse_root=warehouse_root,
        hash_index_key=hash_index_key,
        source_index_key=source_index_key,
        dry_run=dry_run,
    )
```

---

### Step 6-7: Tests

All tests use `MemoryStore` + `tmp_path` fixtures (consistent with
`test_hash_index.py:10,40`).  See test specs in "Tests" section above.

### Step 8: Backfill source_index for existing 45M items

One-time script.  Since source bucket/key are lost for pre-existing items,
set `s3_key` and `deleted` to empty/true so they're skipped by the GC
Bloom filter.  After a full reingest, every item has a real `s3_key`.

```python
def backfill_source_index(store, warehouse_root, source_index_key):
    """Scan all warehouse files, populate source_index with placeholder keys."""
    from earthcatalog.maintenance.compact import _scan_warehouse
    import pyarrow.parquet as pq

    wh_store = LocalStore(str(Path(warehouse_root)))
    buckets = _scan_warehouse(wh_store)

    rows = []
    for (cell, year_str), fms in buckets.items():
        year = int(year_str) if year_str.isdigit() else 0
        for fm in fms:
            raw = bytes(obstore.get(wh_store, fm.s3_key).bytes())
            for batch in pq.ParquetFile(io.BytesIO(raw)).iter_batches(columns=["id"]):
                for stac_id in batch.column("id").to_pylist():
                    if stac_id is None:
                        continue
                    rows.append(("", stac_id, cell, year))

    from earthcatalog.source_index import append_source_index
    append_source_index(rows, store, source_index_key)
```

After reingest, `s3_key` will be populated and `deleted` set to false —
the GC Bloom filter will then cover these items.

But since the user says they'll do a full reingest anyway, this step can be
skipped — the reingest populates `source_index.parquet` naturally via the
ingest hooks from Step 3.

---

### Timing Summary (GitHub Runner)

| Step | Build? | Run? | One-time or weekly? |
|------|--------|------|----------------------|
| 1 — `source_index.py` | Write new file | — | One-time |
| 2 — Inject source metadata | Modify 2 lines | — | One-time |
| 3 — Hook into ingest | ~10 lines per hook | — | One-time |
| 4 — `delete.py` | Write new file | — | One-time |
| 5 — `garbage_collect()` | ~20 lines | — | One-time |
| 6-7 — Tests | Write test files | — | One-time |
| 8 — Backfill script | Write + run once | ~5 min (scan 3k files) | One-time |
| Full reingest | — | ~2-4 hours (Dask, 45M items) | One-time |
| Weekly GC | — | ~1-2 min | Weekly |
| Weekly ingest | — | ~10 min (140k items) | Weekly |

---

## Summary of Differences from v1

| Aspect | v1 | v2 |
|--------|----|----|
| Orphan detection | Compare catalog IDs vs inventory IDs | Bloom filter on S3 keys |
| Key mapping | Requires key→ID mapping (45M rows) | Same, but via `source_index.parquet` |
| File location | file→ID metadata | `grid_partition` + `year` from source_index |
| Memory on GitHub runner | Requires loading all IDs or hashes | ~80 MB Bloom filter |
| STAC fetching during GC | Would need to fetch STACs to get IDs | Zero STAC fetches |
| Rollback | Three-phase backup/restore | Not needed (idempotent steps) |
| Iceberg update | Unclear | Write to new path + add/delete files |
| False positives | None (exact comparison) | Bloom filter → HEAD confirmation |
