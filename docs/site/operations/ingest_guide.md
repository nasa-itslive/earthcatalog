# Ingest Guide

Operational reference for full (re)builds and daily delta ingest.

## Quick reference

```python
from earthcatalog.ingest_config import IngestConfig
import earthcatalog as ec
from obstore.store import S3Store

store = S3Store(bucket="my-bucket", region="us-west-2")
catalog = ec.open(store=store, base="s3://my-bucket/catalog")

# Full ingest (Dask/Coiled, resumable NDJSON-staged pipeline)
catalog.ingest_inventory(
    "s3://bucket/inventory/full.parquet",
    mode="full",
    config=IngestConfig(create_client=lambda: coiled.Client(n_workers=100)),
)

# Single-node full ingest (no Dask) — resumable by default
catalog.ingest_inventory("s3://bucket/inventory/full.parquet", mode="full")

# Daily delta (appends, updates the unified index)
catalog.ingest_inventory("s3://bucket/delta/daily.parquet", mode="delta")
```

## 1. Full ingest

First-time ingest of the full catalog. Both paths route through
`ingest_inventory` → `IngestPipeline`/`Ingester` and the unified index.

### With Dask/Coiled

Runs the resumable staging pipeline: chunk the inventory → fetch STAC JSONs
(parallel workers) → write NDJSON intermediates → compact to GeoParquet
(memory-bounded) → register with Iceberg + update the unified index.
Spot-resilient — interrupted chunks are retried on restart and the staged
NDJSON survives.

```python
catalog.ingest_inventory(
    "s3://bucket/inventory/full.parquet",
    mode="full",
    config=IngestConfig(
        create_client=lambda: coiled.Client(n_workers=20, vm_type="m6i.xlarge"),
    ),
)
```

### Single-node (no Dask)

`create_client=None` (the default) runs the same pipeline on one process,
streaming NDJSON and compacting in bounded memory. Suitable for inventories
under ~1M items.

```python
catalog.ingest_inventory("s3://bucket/inventory/full.parquet", mode="full")
```

## 2. Daily delta

Process new items since the last ingest. Appends files without overwriting
and updates the unified index for duplicate detection.

```python
catalog.ingest_inventory("s3://bucket/delta/2026-04-28.parquet", mode="delta")
```

Filter by modification date to skip old items:

```python
from datetime import UTC, datetime, timedelta

catalog.ingest_inventory(
    "s3://bucket/delta.parquet",
    mode="delta",
    config=IngestConfig(since=datetime.now(UTC) - timedelta(days=2)),
)
```

## 3. Verification

```python
catalog.stats()              # per-partition row/file counts from Iceberg manifests
catalog.unique_item_count()  # active STAC items (streamed from the unified index)
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

The preferred entry point is the `earthcatalog ingest` CLI — it routes
through the resumable `ingest_inventory` / `IngestPipeline` and the unified
index (no hash/source-index knobs to manage):

```bash
# Fresh full build with an S2 grid
uv run earthcatalog ingest --inventory s3://bucket/inventory/full.parquet \
    --mode full --grid s2 --resolution 2

# Daily delta ingest (diff produced by scripts/daily_delta.py)
uv run earthcatalog ingest --inventory s3://…/delta/pending/delta_2026-04-28.parquet \
    --mode delta --scheduler local --workers 4

# Resume a failed run — already-ingested source keys are skipped automatically.
# Stage-only (compact later) or compact staged NDJSON:
uv run earthcatalog ingest --inventory … --mode delta --skip-compact
uv run earthcatalog ingest --inventory … --mode delta --skip-fetch
```

Use `uv run earthcatalog ingest --help` for the full option list.
