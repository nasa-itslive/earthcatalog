# Backfill Pipeline

The backfill pipeline processes the **full historical catalog** (ITS_LIVE
velocity pairs since 1982) using a three-level Dask DAG.  It is designed to
run on a Dask cluster (local or [Coiled](https://coiled.io/)) and is
spot-instance resilient via per-file checkpoint/resume.

---

## When to use it

- **Initial full ingest** of the complete ITS_LIVE archive (~millions of items).
- **Re-ingesting** a large prefix after a schema change.
- Any time `earthcatalog incremental` would take too long on a single node.

For day-to-day updates use the [incremental pipeline](incremental.md) instead.

---

## Three-level architecture

```
Level 0 — ingest_chunk  (one Dask task per inventory chunk)
┌──────────────────────────────────────────────────────────────┐
│ • fetch STAC JSON from S3 (obstore, parallel)                │
│ • fan_out → group_by_partition                               │
│ • write one GeoParquet file per (cell, year) to warehouse    │
│ • return list[FileMetadata]  (~200 B per file)               │
│   ↳ Parquet data never crosses the network to the head node  │
└──────────────────────────────────────────────────────────────┘

Level 1 — compact_group  (one Dask task per (cell, year) bucket)
┌──────────────────────────────────────────────────────────────┐
│ • download all part files for the bucket                     │
│ • merge → deduplicate on id → sort by (platform, datetime)   │
│ • write one consolidated GeoParquet file                     │
│ • delete the input part files                                │
│ • return single FileMetadata                                 │
└──────────────────────────────────────────────────────────────┘

Level 2 — catalog registration  (head node only)
┌──────────────────────────────────────────────────────────────┐
│ • table.add_files(all_compacted_paths)                       │
│ • one Iceberg snapshot for the entire backfill run           │
│ • upload catalog.db to S3                                    │
└──────────────────────────────────────────────────────────────┘
```

**Key invariant**: Parquet data never travels through the Dask scheduler or
head node.  Workers own all I/O; the head node only handles inventory
streaming, metadata collection (~200 B/file), and the final catalog snapshot.

---

## Spot-instance resilience

The pipeline runs in **per-file mini-run mode** by default.  Each inventory
chunk is processed as an independent Dask task with its own `ingest_stats.json`
checkpoint file.  If a spot instance is preempted mid-run:

1. Completed tasks' `ingest_stats.json` files remain in the warehouse.
2. Re-running the pipeline with the same inventory skips already-processed chunks.
3. The final Level-2 catalog registration collects all surviving files.

---

## Usage

### Synchronous (single process — good for debugging)

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory /tmp/inventory.csv \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse \
    --scheduler synchronous \
    --limit     200
```

### Local Dask cluster

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory /tmp/inventory.parquet \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse \
    --scheduler local \
    --workers   4
```

### Coiled cloud cluster

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory       s3://its-live-data/inventory/latest.parquet \
    --catalog         /tmp/catalog.db \
    --warehouse       /tmp/warehouse \
    --scheduler       coiled \
    --coiled-n-workers   20 \
    --coiled-software    itslive-ingest \
    --coiled-region      us-west-2
```

---

## Compaction scale

At H3 resolution 1, the ITS_LIVE glacier coverage occupies roughly
**100–200 cells** globally.  Multiplied by **44 years** (1982–2025) gives
approximately **4,400–8,800 `(cell, year)` buckets** for a full backfill.

Each Level-1 task is independent and runs in parallel on the Dask cluster —
`compact_group = dask.delayed(_compact_group_impl)`.

For the **typical maintenance case** (compacting only the most recent year
after an incremental update), ~100–200 tasks are run by the standalone
[compaction tool](../maintenance/compact.md), sequentially in a few minutes.

---

## Python API

```python
from earthcatalog.pipelines.backfill import run_backfill

run_backfill(
    inventory_path="s3://its-live-data/inventory/latest.parquet",
    catalog_path="/tmp/catalog.db",
    warehouse_path="/tmp/warehouse",
    scheduler="local",
    n_workers=4,
    chunk_size=500,
    limit=None,
)
```
