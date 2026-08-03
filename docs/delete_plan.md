# Delete / Garbage Collection Plan

## Goal

Add a periodic garbage collection step that removes STAC items from the catalog
whose source objects no longer exist in S3. The approach compares the current
hash index (catalog truth) against the current S3 inventory (S3 truth) —
items in the index but not in S3 are orphaned.

## Why Not a "deleted" Column?

- No spatial pruning benefit (Iceberg can't skip files based on a boolean flag)
- Every query would need to scan and filter deleted items
- GeoParquet doesn't support arbitrary boolean predicate pushdown
- Storage overhead on every file

## Strategy

**Periodic cleanup** (weekly/monthly cron job):
1. Read hash index (all item IDs in catalog)
2. Read current S3 inventory (all objects in S3)
3. Find orphaned items: `catalog_ids - inventory_ids`
4. For each orphaned item, look up which files contain it (via file metadata)
5. Rewrite those files (filter out deleted IDs)
6. Register new files in Iceberg (new snapshot)
7. Update hash index (remove orphaned hashes)
8. Update file metadata (remove orphaned IDs from mapping)

**Single point-in-time comparison** — no need to store previous inventories.

## Idempotency & Compaction

### Compaction Safety

The compaction step (`compact_warehouse`) is **independent** of the delete workflow:

- **Compaction before delete**: Compacts files that still contain orphaned items → fine, delete workflow cleans them up later
- **Compaction after delete**: Compacts already-cleaned files → fine, produces clean output
- **Order doesn't matter** — both operations are idempotent and produce correct results

### DB Theory on Idempotency

In database theory, an operation is **idempotent** if running it multiple times has the same effect as running it once. This is crucial for:
- **Fault tolerance** — retries don't cause inconsistencies
- **Distributed systems** — operations can be retried safely
- **ACID transactions** — operations within a transaction are atomic

Each step in our workflow is individually idempotent:
- `find_orphaned_items()` — read-only, safe to retry
- `rewrite_file_without_orphans()` — filtering is deterministic, safe to retry
- `update_hash_index()` — set difference is idempotent
- `update_file_metadata()` — set difference is idempotent

## Files to Change

### New Files

#### 1. `earthcatalog/pipelines/delete.py`

Main delete pipeline module. Exports:
- `find_orphaned_items(inventory_path, catalog_path, store)` — identify orphans
- `run_garbage_collection(inventory_path, catalog_path, warehouse_path)` — full cleanup
- `rewrite_file_without_orphans(file_path, orphaned_ids)` — rewrite single file
- `update_file_metadata_after_delete(file_to_items, orphaned_ids)` — update mapping
- `backup_and_restore_hash_index(backup_hashes, store, key)` — rollback helper

#### 2. `tests/test_delete.py`

Tests for the delete pipeline using MemoryStore and mocked inventory.

### Modified Files

#### 3. `earthcatalog/hash_index.py`

Add file metadata tracking:
- `write_file_metadata(file_to_items, store, key)` — write file→ID mapping
- `read_file_metadata(store, key)` — read file→ID mapping
- `update_file_metadata_after_delete(file_to_items, orphaned_ids)` — remove orphaned IDs
- `get_files_for_ids(file_to_items, ids)` — find files containing specific IDs

#### 4. `earthcatalog/pipelines/__init__.py`

Export new functions from `earthcatalog.pipelines.delete`.

#### 5. `earthcatalog/pipelines/incremental.py` (or `backfill.py`)

Track file→ID mapping during ingest:
- After writing each GeoParquet file, record which item IDs it contains
- Store in file metadata alongside hash index

## Implementation Details

### File Metadata Format

```python
# file_to_items: {file_path: [item_id1, item_id2, ...]}
# Or inverted: {item_id: [file_path1, file_path2, ...]}

file_to_items = {
    "grid_partition=8001/year=2020/part_000000.parquet": ["item-1", "item-2", ...],
    "grid_partition=8002/year=2020/part_000001.parquet": ["item-3", "item-4", ...],
}
```

### `write_file_metadata`

```python
def write_file_metadata(file_to_items, store, key):
    """Write file→ID mapping as Parquet file."""
    ids_list = []
    for file_path, item_ids in file_to_items.items():
        ids_list.append((file_path, item_ids))
    
    tbl = pa.table({
        "file_path": [f[0] for f in ids_list],
        "item_id": [id for f, ids in ids_list for id in ids],
    })
    
    # Write to store
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
```

### `get_files_for_ids`

```python
def get_files_for_ids(file_to_items, ids):
    """Find files containing any of the given IDs."""
    affected_files = set()
    for file_path, item_ids in file_to_items.items():
        if any(item_id in ids for item_id in item_ids):
            affected_files.add(file_path)
    return affected_files
```

### `rewrite_file_without_orphans`

```python
def rewrite_file_without_orphans(file_path, orphaned_ids):
    """Read file, filter out orphaned IDs, write back."""
    # Read file
    df = read_parquet(file_path)
    
    # Filter out deleted IDs
    df = df[~df['id'].isin(orphaned_ids)]
    
    # Write back (same path)
    write_parquet(df, file_path)
    
    return len(df)  # rows remaining
```

### `run_garbage_collection` — Three-Phase with Rollback

```python
def run_garbage_collection(inventory_path, catalog_path, warehouse_path, store):
    """
    Three-phase garbage collection with rollback support:
    
    Phase 1 — Plan (read-only, safe to retry)
    Phase 2 — Backup current state
    Phase 3 — Execute (write operations, in order)
    
    If Phase 3 fails, Phase 2 backup is restored automatically.
    """
    
    # Phase 1: Plan (read-only, safe to retry)
    orphaned = find_orphaned_items(inventory_path, catalog_path, store)
    if not orphaned:
        return {"orphaned": 0}
    
    file_to_items = read_file_metadata(store, hash_index_path)
    affected_files = get_files_for_ids(file_to_items, orphaned)
    
    # Phase 2: Backup current state (for rollback)
    backup_hash_index = read_hashes(store, hash_index_path)
    backup_file_metadata = dict(file_to_items)
    
    # Phase 3: Execute (write operations, in order)
    try:
        # 3a: Rewrite files
        for file_path in affected_files:
            rows_remaining = rewrite_file_without_orphans(file_path, orphaned)
            print(f"Rewrote {file_path}: {rows_remaining} rows remaining")
        
        # 3b: Update hash index (critical — must succeed)
        existing = read_hashes(store, hash_index_path)
        remaining = {k: v for k, v in existing.items() if k not in orphaned}
        write_hashes(remaining, store, hash_index_path)
        
        # 3c: Update file metadata
        update_file_metadata_after_delete(file_to_items, orphaned)
        write_file_metadata(file_to_items, store, hash_index_path)
        
    except Exception as e:
        # Roll back to backup state
        write_hashes(backup_hash_index, store, hash_index_path)
        write_file_metadata(backup_file_metadata, store, hash_index_path)
        print(f"ERROR: Rollback to pre-deletion state due to: {e}")
        raise e  # Re-raise for logging
    
    return {"orphaned": len(orphaned), "files_affected": len(affected_files)}
```

## Failure Scenarios & Handling

| Step | Failure Impact | Rollback Needed? |
|------|---------------|-------------------|
| `find_orphaned_items()` | No state changed (read-only) | No |
| `rewrite_file_without_orphans()` | Partial file cleanup | No (files are independent) |
| `update_hash_index()` | Hash index inconsistent | **Yes** — restore from backup |
| `update_file_metadata()` | Partial metadata update | No (less critical, retry on next run) |

## Tests to Write

### `tests/test_delete.py`

#### Test 1: `test_find_orphaned_items_basic`

```python
def test_find_orphaned_items_basic(self, tmp_path, memory_store):
    """Items in catalog but not in inventory should be detected as orphaned."""
    # 1. Create a catalog with items A, B, C
    # 2. Create an inventory with items A, B (C is missing)
    # 3. Run find_orphaned_items
    # 4. Verify C is in orphaned set
    orphaned = find_orphaned_items(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        store=memory_store,
    )
    assert "item-C" in orphaned
    assert "item-A" not in orphaned
    assert "item-B" not in orphaned
```

#### Test 2: `test_find_orphaned_items_empty`

```python
def test_find_orphaned_items_empty(self, tmp_path, memory_store):
    """No orphans when all catalog items exist in inventory."""
    orphaned = find_orphaned_items(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        store=memory_store,
    )
    assert len(orphaned) == 0
```

#### Test 3: `test_find_orphaned_items_no_catalog`

```python
def test_find_orphaned_items_no_catalog(self, tmp_path, memory_store):
    """Empty catalog should return empty orphaned set."""
    orphaned = find_orphaned_items(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        store=memory_store,
    )
    assert len(orphaned) == 0
```

#### Test 4: `test_get_files_for_ids_basic`

```python
def test_get_files_for_ids_basic(self, tmp_path, memory_store):
    """get_files_for_ids should return files containing the given IDs."""
    file_to_items = {
        "file1.parquet": ["item-A", "item-B"],
        "file2.parquet": ["item-C", "item-D"],
        "file3.parquet": ["item-E"],
    }
    
    affected = get_files_for_ids(file_to_items, {"item-A", "item-C"})
    assert affected == {"file1.parquet", "file2.parquet"}
```

#### Test 5: `test_get_files_for_ids_empty`

```python
def test_get_files_for_ids_empty(self, file_to_items):
    """get_files_for_ids with empty set should return no files."""
    affected = get_files_for_ids(file_to_items, set())
    assert affected == set()
```

#### Test 6: `test_rewrite_file_without_orphans`

```python
def test_rewrite_file_without_orphans(self, tmp_path):
    """Rewrite should remove orphaned IDs from file."""
    # 1. Create a test file with items A, B, C
    # 2. Rewrite with orphaned_ids=["item-B"]
    # 3. Verify file now contains only A and C
    file_path = tmp_path / "test.parquet"
    write_parquet_with_ids(["item-A", "item-B", "item-C"], file_path)
    
    rows_before = read_parquet(file_path)['id'].tolist()
    assert len(rows_before) == 3
    
    rows_after = rewrite_file_without_orphans(str(file_path), {"item-B"})
    assert rows_after == 2
    
    remaining_ids = read_parquet(file_path)['id'].tolist()
    assert "item-A" in remaining_ids
    assert "item-B" not in remaining_ids
    assert "item-C" in remaining_ids
```

#### Test 7: `test_rewrite_file_no_orphans`

```python
def test_rewrite_file_no_orphans(self, tmp_path):
    """Rewrite with no orphaned IDs should not change the file."""
    file_path = tmp_path / "test.parquet"
    write_parquet_with_ids(["item-A", "item-B"], file_path)
    
    rows_before = read_parquet(file_path)['id'].tolist()
    rows_after = rewrite_file_without_orphans(str(file_path), set())
    
    assert rows_before == rows_after
    assert len(rows_after) == 2
```

#### Test 8: `test_run_garbage_collection_full`

```python
def test_run_garbage_collection_full(self, tmp_path, memory_store):
    """Full garbage collection should rewrite files and update catalog."""
    # 1. Create catalog with items A, B, C
    # 2. Create inventory with items A, B
    # 3. Run garbage collection
    # 4. Verify C is removed from catalog
    # 5. Verify file containing C is rewritten without C
    # 6. Verify hash index updated
    result = run_garbage_collection(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        warehouse_path=str(tmp_path / "warehouse"),
        store=memory_store,
    )
    assert result["orphaned"] == 1
    assert result["files_affected"] == 1
```

#### Test 9: `test_run_garbage_collection_no_op`

```python
def test_run_garbage_collection_no_op(self, tmp_path, memory_store):
    """No orphans should result in no changes."""
    result = run_garbage_collection(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        warehouse_path=str(tmp_path / "warehouse"),
        store=memory_store,
    )
    assert result["orphaned"] == 0
    assert result["files_affected"] == 0
```

#### Test 10: `test_e2e_delete_pipeline`

```python
def test_e2e_delete_pipeline(self, tmp_path, memory_store):
    """End-to-end: ingest items, delete some, verify cleanup."""
    # 1. Ingest items A, B, C into catalog
    # 2. Create inventory with only A, B
    # 3. Run garbage collection
    # 4. Verify C is removed from catalog
    # 5. Verify queries don't return C
```

#### Test 11: `test_rollback_on_failure`

```python
def test_rollback_on_failure(self, tmp_path, memory_store):
    """If hash index update fails, should roll back to pre-deletion state."""
    # 1. Create catalog with items A, B, C
    # 2. Create inventory with items A, B
    # 3. Mock rewrite_file_without_orphans to succeed
    # 4. Mock write_hashes to raise an error
    # 5. Verify that hash index is restored to pre-deletion state
    # 6. Verify that file metadata is also restored
    result = run_garbage_collection(
        inventory_path=str(tmp_path / "inventory.csv"),
        catalog_path=str(tmp_path / "catalog.db"),
        warehouse_path=str(tmp_path / "warehouse"),
        store=memory_store,
    )
    # Should raise an error and roll back
    assert "orphaned" in result  # Or verify error was raised and state restored
```

## Scheduling

Add to cron or Airflow:

```bash
# Weekly garbage collection (Sunday 2 AM)
0 2 * * 0 /path/to/earthcatalog/pipelines/delete.py \
    --inventory s3://bucket/inventory/current \
    --catalog /path/to/catalog.db \
    --warehouse s3://bucket/warehouse
```

## Idempotency

- File rewrite is idempotent — rewriting without orphaned IDs is safe
- Hash index update is idempotent — removing non-existent hashes is safe
- Running multiple times won't cause issues

## Error Handling

- If inventory is missing or corrupted, skip deletion
- If file metadata is missing, scan all files to find orphans
- If file rewrite fails, log error and continue with other files
- Never partially delete — either all or nothing (three-phase with rollback ensures this)
