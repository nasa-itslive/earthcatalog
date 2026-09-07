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

Distributed ingest runs as **scatter → map → reduce**:

1. **Scatter** (head): the head streams the inventory once, filters
   `.stac.json` items, and writes fixed-row shard parquets (one per
   `chunk_size` items) to `{warehouse}/staging/shards/<run_id>/` plus a
   `scatter.json` manifest.  Head memory stays bounded; every shard is the
   same size regardless of source part-file skew.
2. **Map** (workers): each worker reads its own shard URL, fetches the STAC
   JSONs, and fans them out to per-(cell, year) NDJSON.
3. **Reduce** (workers): each `(cell, year)` bucket is compacted to GeoParquet
   by a worker (one task per bucket); the head then commits to Iceberg + the
   unified index exactly once.  Shard files are deleted after a successful run.

Each phase shows a `tqdm` progress bar.  The map/reduce can also be split so
you can watch (or resume) the two phases independently — `skip_compact=True`
runs only the NDJSON scatter, `skip_fetch=True` runs only the consolidation:

```python
# Phase A — scatter NDJSON (fetch → per-(cell,year) NDJSON), no GeoParquet yet
catalog.ingest_inventory(
    scatter_json,
    mode="delta",
    config=IngestConfig(create_client=lambda: client, skip_compact=True),
)

# Phase B — consolidate NDJSON → GeoParquet, no re-fetch
catalog.ingest_inventory(
    scatter_json,
    mode="delta",
    config=IngestConfig(create_client=lambda: client, skip_fetch=True),
)
```

To keep workers from idling behind the head's inventory read, run scatter
and map/reduce as **two separate steps**:

```python
# Step 1 — scatter only (no cluster needed)
catalog.ingest_inventory(
    "s3://bucket/inventory/full.parquet",
    mode="full",
    config=IngestConfig(create_client=lambda: client, scatter_only=True),
)
# → prints: scatter_only set — skipping map/reduce.
# → returns {"scatter": "s3://.../staging/shards/<run_id>/scatter.json"}

# Step 2 — consume the pre-scattered shards (workers start immediately)
catalog.ingest_inventory(
    "s3://.../staging/shards/<run_id>/scatter.json",
    mode="full",
    config=IngestConfig(create_client=lambda: client),
)
```

A failed map/reduce keeps the shard files — re-run step 2 with the same
`scatter.json` to resume (the unified index dedups already-ingested keys).

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

# Daily update — exact diff, then ingest only what is new.
# Step 1: DuckDB EXCEPT of the two inventory days (out-of-core, string-exact);
# writes new/changed keys; --out-old captures disappeared keys (GC input).
uv run earthcatalog diff \
    --current  s3://…/inventory/dt=2026-09-06/manifest.json \
    --previous s3://…/inventory/dt=2026-09-05/manifest.json \
    --out      s3://…/diffs/new-20260905-20260906.parquet \
    --out-old  s3://…/diffs/old-20260905-20260906.parquet

# Step 2: anti-join the diff against the unified index (known keys skip),
# fetch the rest with a bounded pool, commit, write _last_run.json.
uv run earthcatalog ingest --diff s3://…/diffs/new-20260905-20260906.parquet \
    --mode delta --scheduler synchronous --fetch-workers 16

# Counts only, no writes:
uv run earthcatalog ingest --diff s3://…/diffs/new-20260905-20260906.parquet --dry-run

# Two-step scatter → map/reduce (workers don't idle behind the head read):
# Step 1: scatter only (no cluster needed)
uv run earthcatalog ingest --inventory s3://bucket/inventory/full.parquet \
    --mode full --scatter-only
# → prints: scatter_only set — skipping map/reduce.
# → prints: Scatter: N shard file(s) … (s3://…/staging/shards/<run_id>/scatter.json)
# Step 2: consume the pre-scattered shards on the cluster
uv run earthcatalog ingest --inventory s3://…/staging/shards/<run_id>/scatter.json \
    --mode delta --scheduler coiled

# Resume a failed run — already-ingested source keys are skipped automatically.
# Stage-only (compact later) or compact staged NDJSON:
uv run earthcatalog ingest --inventory … --mode delta --skip-compact
uv run earthcatalog ingest --inventory … --mode delta --skip-fetch
```

Use `uv run earthcatalog ingest --help` for the full option list.
