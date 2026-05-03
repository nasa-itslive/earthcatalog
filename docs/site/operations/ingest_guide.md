# Ingest Guide

Operational reference for bulk backfill and daily delta ingest.

## Quick reference

```python
import earthcatalog as ea
from obstore.store import S3Store

store = S3Store(bucket="my-bucket", region="us-west-2")
ec = ea.open(store=store, base="s3://my-bucket/catalog")

# Bulk backfill (Dask/Coiled, spot-resilient 4-phase pipeline)
ec.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
               create_client=lambda: coiled.Client(n_workers=100))

# Single-node backfill (no Dask)
ec.ingest("s3://bucket/inventory/full.parquet", mode="full")

# Daily delta
ec.ingest("s3://bucket/delta/daily.parquet", mode="delta",
          update_hash_index=True)
```

## 1. Bulk backfill

First-time ingest of the full catalog. Two paths:

### With Dask/Coiled (`ec.bulk_ingest`)

Runs the 4-phase staging pipeline: chunk files → async S3 fetch → NDJSON intermediates
→ compact to GeoParquet → register with Iceberg. Spot-resilient — interrupted chunks
are retried on restart, NDJSON survives, completion markers track progress.

```python
ec.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
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
ec.ingest("s3://bucket/inventory/full.parquet", mode="full",
          chunk_size=10000, update_hash_index=True)
```

## 2. Daily delta

Process new items since the last ingest. Appends files without overwriting,
updates the hash index for duplicate detection.

```python
ec.ingest("s3://bucket/delta/2026-04-28.parquet",
          mode="delta",
          update_hash_index=True)
```

Filter by modification date to skip old items:

```python
from datetime import UTC, datetime, timedelta

ec.ingest("s3://bucket/delta.parquet", mode="delta",
          since=datetime.now(UTC) - timedelta(days=2),
          update_hash_index=True)
```

## 3. Verification

```python
ec.stats()              # per-partition row/file counts from Iceberg manifests
ec.unique_item_count()  # unique STAC items (hash index footer, no full scan)
ec.info()               # grid metadata and catalog info object
```

DuckDB query with spatial pruning:

```python
from shapely.geometry import box
import duckdb

greenland = box(-60, 60, -20, 85)
paths = ec.search_files(greenland, start_datetime="2020-01-01")

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

The old `python -m earthcatalog.pipelines.backfill` CLI is still available for
backward compatibility but new deployments should use the Python API above.
