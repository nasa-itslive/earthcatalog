# Incremental Pipeline

The incremental pipeline is the day-to-day workhorse for keeping the catalog
up to date.  It runs on a single node (laptop, EC2, GitHub Actions runner) and
processes a delta inventory — only the STAC items that changed since the last run.

---

## When to use it

- **Daily / weekly catalog updates** — pass `--since <yesterday>` to ingest only new or changed items.
- **First-time small-scale ingest** — good for smoke-testing up to ~50,000 items.
- **Incremental backfill** — run without `--since` to process a full inventory file.

For very large backhills (millions of items, full history since 1982) use the
[Dask backfill pipeline](backfill.md) instead.

---

## How it works

```
Inventory file
    │ _iter_inventory()           streaming; bounded memory
    ▼
ThreadPoolExecutor               obstore.get() per item (parallel)
    │ STAC JSON dicts
    ▼
fan_out(items, partitioner)      H3 fan-out; boundary-inclusive
    │
    ▼
group_by_partition(rows)         one group per (cell, year)
    │
    ▼
write_geoparquet(group, path)    rustac; one .parquet per group
    │
    ▼
table.add_files(paths)           PyIceberg SQLite registration
    │
    ▼
upload_catalog()                 catalog.db → S3 (with S3Lock)
```

---

## Supported inventory formats

| Format | Notes |
|---|---|
| CSV (`.csv`) | Standard AWS S3 Inventory format — `bucket,key` columns |
| CSV.gz (`.csv.gz`) | Gzip-compressed CSV |
| Parquet (`.parquet`) | Preferred at scale — typed, compressed, row-group streaming |
| `manifest.json` | AWS S3 Inventory manifest referencing multiple Parquet files |

All formats are auto-detected from the file extension.

---

## Delta ingest with `--since`

```bash
earthcatalog incremental \
    --inventory /tmp/full_inventory.parquet \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse \
    --since     2026-04-21
```

The pipeline reads the full inventory but skips any row where
`last_modified_date < since`.  This is done at read time in row-group
batches, so memory usage stays bounded even for very large inventory files.

!!! note "Future improvement"
    When an Athena cron job is configured to produce a pre-filtered delta
    Parquet file, pass that file as `--inventory` and drop `--since`
    entirely — no pipeline changes are needed.

---

## Usage

```bash
# Minimal
earthcatalog incremental \
    --inventory /tmp/inventory.csv \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse

# With delta filter and item limit (smoke test)
earthcatalog incremental \
    --inventory /tmp/inventory.parquet \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse \
    --since     2026-04-01 \
    --limit     1000

# With YAML config
earthcatalog incremental \
    --config    config/h3_r1.yaml \
    --inventory /tmp/inventory.csv
```

---

## Python API

```python
from earthcatalog.pipelines.incremental import run
from earthcatalog.grids.h3_partitioner import H3Partitioner

run(
    inventory_path="/tmp/inventory.csv",
    catalog_path="/tmp/catalog.db",
    warehouse_path="/tmp/warehouse",
    partitioner=H3Partitioner(resolution=1),
    chunk_size=500,
    max_workers=8,
    use_lock=True,
)
```

---

## Memory model

- Inventory is streamed row by row — one row-group batch in memory at a time.
- STAC JSON is fetched in parallel but each batch is processed and written
  before the next batch begins (`chunk_size` controls batch size).
- `catalog.db` is downloaded once at startup and uploaded once at the end.
- Peak memory ≈ `chunk_size × avg_item_size × overlap_multiplier`
  (typically < 1 GB for `chunk_size=500` at H3 resolution 1).
