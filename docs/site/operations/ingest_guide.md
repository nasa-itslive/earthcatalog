# Ingest Guide

Operational reference for bulk backfill and daily delta ingest.

## Quick reference

```python
import earthcatalog as ec
from obstore.store import S3Store

store = S3Store(bucket="my-bucket", region="us-west-2")
catalog = ec.open(store=store, base="s3://my-bucket/catalog")

# Bulk backfill (Dask/Coiled, spot-resilient 4-phase pipeline)
catalog.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
                     create_client=lambda: coiled.Client(n_workers=100))

# Single-node backfill (no Dask)
catalog.ingest("s3://bucket/inventory/full.parquet", mode="full")

# Daily delta
catalog.ingest("s3://bucket/delta/daily.parquet", mode="delta",
          update_hash_index=True)
```

## 1. Bulk backfill

First-time ingest of the full catalog. Two paths:

### With Dask/Coiled (`ec.bulk_ingest`)

Runs the 4-phase staging pipeline: chunk files → async S3 fetch → NDJSON intermediates
→ compact to GeoParquet → register with Iceberg. Spot-resilient — interrupted chunks
are retried on restart, NDJSON survives, completion markers track progress.

```python
catalog.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
               create_client=lambda: coiled.Client(
                   n_workers=20,
                   vm_type="m6i.xlarge",
               ))
```

Pass `create_client=None` (default) to use an existing Dask client or fall back
to sequential execution if no cluster is available.

### Single-node (`ec.ingest`)

Simpler path using `ThreadPoolExecutor`. No intermediate files — fetch →
fan-out → write → register. Suitable for inventories under ~1M items.

```python
catalog.ingest("s3://bucket/inventory/full.parquet", mode="full",
          chunk_size=10000, update_hash_index=True)
```

## 2. Daily delta

Process new items since the last ingest. Appends files without overwriting,
updates the hash index for duplicate detection.

```python
catalog.ingest("s3://bucket/delta/2026-04-28.parquet",
          mode="delta",
          update_hash_index=True)
```

Filter by modification date to skip old items:

```python
from datetime import UTC, datetime, timedelta

catalog.ingest("s3://bucket/delta.parquet", mode="delta",
          since=datetime.now(UTC) - timedelta(days=2),
          update_hash_index=True)
```

## 3. Verification

```python
catalog.stats()              # per-partition row/file counts from Iceberg manifests
catalog.unique_item_count()  # unique STAC items (hash index footer, no full scan)
catalog.info()               # grid metadata and catalog info object
```

DuckDB query with spatial pruning:

```python
from shapely.geometry import box
import duckdb

greenland = box(-60, 60, -20, 85)
paths = catalog.search_files(greenland, start_datetime="2020-01-01")

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{greenland.wkt}'))
    LIMIT 10
""").df()
```

## CLI reference

The preferred entry point is the `earthcatalog backfill` CLI — it routes
through the resumable `bulk_ingest` / `Ingester` pipeline and the unified
index (no hash/source-index knobs to manage):

```bash
# Fresh full build with an S2 grid
earthcatalog backfill --inventory s3://bucket/inventory/full.parquet \
    --mode full --grid s2 --resolution 2

# Daily delta ingest (diff produced by scripts/daily_delta.py)
earthcatalog backfill --inventory s3://…/delta/pending/delta_2026-04-28.parquet \
    --mode delta --scheduler local --workers 4

# Resume a failed run — already-ingested source keys are skipped automatically.
# Stage-only (compact later) or compact staged NDJSON:
earthcatalog backfill --inventory … --mode delta --skip-compact
earthcatalog backfill --inventory … --mode delta --skip-fetch
```

Use `earthcatalog backfill --help` for the full option list.
