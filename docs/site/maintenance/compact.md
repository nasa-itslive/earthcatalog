# Warehouse Compaction

The incremental pipeline writes **one new part file per `(cell, year)` bucket
per run**.  After N incremental runs a bucket has N part files, each
potentially containing duplicate `id` rows if the same STAC item appeared in
more than one inventory delta.

Compaction merges all part files in each bucket into a single, deduplicated,
sorted file and rebuilds the Iceberg catalog.

---

## When to run

- **After each incremental run** if strict deduplication is required.
- **Weekly or monthly** for most use cases — the incremental pipeline can run
  without compaction; compaction just cleans up the accumulation.
- **After a large backfill** to collapse all per-chunk part files into one file
  per bucket (this is done automatically inside `run_backfill` via Level 1).

---

## What compaction does

For each `(grid_partition, year)` bucket with ≥ `threshold` part files:

1. **Orphan sweep** — delete any `.parquet` files in the prefix that are not
   in the known part-file list (cleanup from interrupted previous runs).
2. **Download** all part files.
3. **Merge** into a single Arrow table (schema metadata preserved from the first file).
4. **Deduplicate** on `id`: sort by `(id ASC, updated DESC)`, keep the first
   occurrence of each id (i.e. the most recently updated version).
5. **Sort** by `(platform, datetime)` for optimal Parquet predicate pushdown.
6. **Write** one consolidated GeoParquet file to the warehouse store.
7. **Delete** all input part files.

After all buckets are compacted, the Iceberg catalog is **rebuilt from scratch**
(drop table → recreate → `add_files(all_surviving_paths)`), producing exactly
one clean snapshot.

!!! note "Dedup scope"
    Deduplication is *within* each `(cell, year)` file, not globally unique
    across the warehouse.  Because of the fan-out, one `id` legitimately
    appears in multiple cell files (one per intersecting H3 cell).

---

## CLI usage

```bash
# Dry run — see what would be compacted without touching any files
python -m earthcatalog.maintenance.compact \
    --warehouse /tmp/warehouse \
    --catalog   /tmp/catalog.db \
    --dry-run

# Compact all buckets with ≥ 2 part files (default)
python -m earthcatalog.maintenance.compact \
    --warehouse /tmp/warehouse \
    --catalog   /tmp/catalog.db

# Custom threshold (only compact buckets with ≥ 5 files)
python -m earthcatalog.maintenance.compact \
    --warehouse /tmp/warehouse \
    --catalog   /tmp/catalog.db \
    --threshold 5

# With S3 lock (recommended for production)
python -m earthcatalog.maintenance.compact \
    --warehouse /tmp/warehouse \
    --catalog   /tmp/catalog.db \
    --use-lock
```

---

## Python API

```python
from earthcatalog.maintenance.compact import compact_warehouse

summary = compact_warehouse(
    warehouse_path="/tmp/warehouse",
    catalog_path="/tmp/catalog.db",
    threshold=2,       # compact any bucket with ≥ 2 files
    use_lock=False,    # set True in production
    dry_run=False,
)

print(summary)
# {
#     "buckets_scanned":   150,
#     "buckets_compacted":  45,
#     "files_before":      195,
#     "files_after":       150,
# }
```

---

## Return value

`compact_warehouse()` returns a dict:

| Key | Type | Description |
|---|---|---|
| `buckets_scanned` | int | Total `(cell, year)` buckets found in the warehouse |
| `buckets_compacted` | int | Buckets that met the threshold and were compacted |
| `files_before` | int | Total part files before compaction |
| `files_after` | int | Total part files after compaction |

---

## Scale considerations

At H3 resolution 1, compacting the **most recent year** involves ~100–200
buckets and runs sequentially in a few minutes.  This is fast enough for a
weekly maintenance job.

A **full warehouse compaction** (all 44 years × ~150 cells ≈ ~6,600 buckets)
takes longer sequentially.  For that scale, use the Level-1 compaction inside
the [backfill pipeline](../pipelines/backfill.md) which runs all buckets in
parallel with Dask.

---

## Iceberg catalog rebuild

After physical compaction, `compact_warehouse` performs a full catalog rebuild:

1. **Drop** the existing Iceberg table (removes all stale manifest entries).
2. **Recreate** the table with the same schema and partition spec.
3. **`table.add_files(all_surviving_paths)`** — register every surviving
   Parquet file in a single new snapshot.

The result is exactly **1 Iceberg snapshot** regardless of how many incremental
runs produced the input files.

!!! warning "Brief table-not-found window"
    Between dropping and recreating the Iceberg table, the catalog has no table.
    Any concurrent reader will see a `NoSuchTableError`.  Use `--use-lock` in
    production to prevent concurrent access during this window.
