# Ingest Workflow

GitHub Actions workflow for daily delta ingest with isolated staging and automatic hash index updates.

## Overview

The ingest workflow runs daily to process new ITS_LIVE granules from the S3 Inventory and register them in the catalog.

**Schedule:** 04:00 UTC daily (after `daily_delta` at 02:00 UTC)

## Two APIs

EarthCatalog provides two ingest paths:

### 1. Unified API: `catalog.ingest()`

Simple, single-function entry point for both full backfill and delta:

```python
from earthcatalog.core import catalog
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
ec = catalog.open(store=store, base="s3://my-bucket/catalog")

# Full backfill: drop + recreate table
ec.ingest("s3://bucket/inventory/full.parquet", mode="full")

# Delta: append new files to existing table
ec.ingest("s3://bucket/inventory/delta.parquet", mode="delta",
          update_hash_index=True)
```

Key features:
- Mode auto-detection (`mode="auto"` → delta if table has data)
- Consistent store-based I/O (no local/S3 branching)
- Optional hash index update (`update_hash_index=True`)
- `since` parameter for datetime-based filtering

### 2. Fault-tolerant pipeline: `scripts/run_backfill.py`

4-phase pipeline designed for spot-instance resilience:

| Step | Action | Output |
|------|--------|--------|
| 1 | `daily_delta.yml` produces new delta inventory | `s3://…/delta/pending/delta_YYYY-MM-DD.parquet` |
| 2 | `ingest.yml` downloads catalog | Local SQLite Iceberg db |
| 3 | `run_backfill.py --delta --update-hash-index` | New warehouse parquets + hash index merge |
| 4 | Move delta to `ingested/` | `s3://…/delta/ingested/delta_YYYY-MM-DD.done` |

## Key Features

- **Isolated staging**: Each run uses `s3://…/ingest/delta_{date}` — no collisions with other runs
- **Integrated hash index**: `--update-hash-index` merges new item IDs in Phase 4 (no separate step)
- **Spot resilient**: Phases are idempotent; chunks and NDJSON survive interruption

## Workflow Inputs

| Input | Description | Default |
|-------|-------------|---------|
| `delta_date` | Date of delta to ingest (YYYY-MM-DD) | Yesterday |
| `scheduler` | `local`, `coiled`, or `synchronous` | `local` |
| `workers` | Dask workers (local scheduler) | `4` |
| `chunk_size` | Items per chunk | `10000` |

## Configuration

```yaml
env:
  AWS_REGION: us-west-2
  WAREHOUSE: s3://its-live-data/test-space/stac/catalog/warehouse
  WAREHOUSE_HASH: s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet
  STAGING: s3://its-live-data/test-space/stac/catalog/ingest
  DELTA_PREFIX: s3://its-live-data/test-space/stac/catalog/delta
```

## Running Manually

### Full command with all parameters

```bash
python scripts/run_backfill.py \
  --inventory s3://its-live-data/test-space/stac/catalog/delta/pending/delta_2026-04-28.parquet \
  --catalog /tmp/earthcatalog.db \
  --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
  --staging s3://its-live-data/test-space/stac/catalog/ingest/delta_2026-04-28 \
  --delta \
  --update-hash-index \
  --hash-index s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet \
  --scheduler local \
  --workers 4
```

### Smoke test (synchronous, 1000 items)

```bash
python scripts/run_backfill.py \
  --inventory s3://.../delta/pending/delta_2026-04-28.parquet \
  --limit 1000 \
  --scheduler synchronous \
  --delta \
  --update-hash-index \
  --skip-upload
```

### Parameters explained

| Flag | Purpose |
|------|---------|
| `--inventory` | Delta parquet from `daily_delta` workflow |
| `--staging` | Isolated path per run (`delta_{date}`) — avoids chunk collisions |
| `--delta` | Append mode: adds files without dropping the table |
| `--update-hash-index` | Merge new item IDs from warehouse parquets into hash index |
| `--hash-index` | Location of the hash index parquet |
| `--skip-upload` | Skip uploading catalog.db to S3 (for testing) |
| `--skip-inventory` | Re-use existing chunks in staging (after crash) |
| `--no-lock` | Skip S3 lock (safe for single-operator runs) |

## Hash Index Update (Plan B)

When `--update-hash-index` is passed, Phase 4 (`register_delta`) does:

1. Reads existing hash index from `s3://…/warehouse_id_hashes.parquet` (~40M hashes)
2. Reads `id` column from each newly written warehouse parquet file
3. Hashes each item ID with xxh3_128 (seed=42)
4. Merges into existing set (union, deduplicates)
5. Writes updated index back to S3

**Why this is the correct approach:**
- Reads from actual warehouse files (what's actually in the catalog)
- No scan of existing files (~5k parquets avoided)
- Fast: ~160k items takes seconds
- Exact: only includes successfully ingested items

## Verifying After Ingest

```bash
python scripts/info.py \
  --catalog /tmp/earthcatalog.db \
  --warehouse s3://its-live-data/test-space/stac/catalog/warehouse
```

Expected output:
```
Unique items  : 40,176,531 (from hash index)  # +60k from delta
Total files   : 5,024                        # +new files
Years        : 1982–2026
```

## Troubleshooting

### Delta file not found

```bash
# Check if daily_delta ran successfully
aws s3 ls s3://its-live-data/test-space/stac/catalog/delta/pending/
```

### Re-run after crash (use existing chunks)

```bash
python scripts/run_backfill.py \
  --inventory ... \
  --skip-inventory \
  --no-lock \
  --scheduler local \
  ...
```

### Re-run from Phase 3 (skip Phase 1 & 2)

```bash
python scripts/run_backfill.py \
  --inventory ... \
  --skip-inventory \
  --skip-ingest \
  ...
```