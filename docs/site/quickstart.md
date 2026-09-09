# Quick Start

EarthCatalog ingests STAC items from S3 into a spatially-partitioned GeoParquet
catalog backed by Apache Iceberg. Instead of a database, Parquet files sit on S3
and a small SQLite file tracks the Iceberg schema. DuckDB reads them directly —
no serialization overhead, no infrastructure.

## Daily update (the normal path)

Two exact steps. The diff is an out-of-core DuckDB `EXCEPT` between two
inventory days; the ingest anti-joins the result against the unified index, so
already-ingested keys are skipped even if a previous run was missed.

```bash
# 1. What changed between yesterday and today?  (exact, string comparison)
earthcatalog diff \
    --current  s3://…/inventory/2026-09-06T01-00Z/manifest.json \
    --previous s3://…/inventory/2026-09-05T01-00Z/manifest.json \
    --out      s3://…/diffs/new-20260905-20260906.parquet \
    --out-old  s3://…/diffs/old-20260905-20260906.parquet   # disappeared keys (GC input)

# 2. Ingest what is actually new (~40k keys ≈ 15 min on a GitHub runner)
earthcatalog ingest \
    --diff s3://…/diffs/new-20260905-20260906.parquet \
    --warehouse s3://my-bucket/catalog/warehouse \
    --mode delta --fetch-workers 16
```

`earthcatalog ingest --diff` is:

- **exact** — new = string anti-join against the unified index, no hash
  collisions, no false "already ingested";
- **idempotent** — re-running the same command does nothing once everything
  is indexed;
- **self-healing** — a crash leaves a small journal; the next run recovers
  it automatically, and a stale index catches up from the backlog instead of
  skipping days;
- **resumable** — every batch commits journal-first; a crash loses at most
  one batch.

Add `--dry-run` to count `considered / new / already indexed` without writing
anything, and `--limit N` to bound a test run.

## Full (re)build

First-time build from a complete inventory. Drops the existing table, index,
and staging area, then ingests everything.

```bash
earthcatalog ingest \
  --inventory s3://bucket/inventory/full.parquet \
  --warehouse s3://my-bucket/catalog/warehouse \
  --mode full --grid h3 --resolution 1
```

`--mode full` resets the table **and** the index and sweeps `_staging/` — a
populated warehouse re-ingests from zero.

## Bulk ingest (Dask/Coiled — not for the daily path)

Large historical builds can fan out to a cluster. Stop after the scatter step
so workers never idle behind the head's inventory read, then run the
map/reduce:

```bash
# Step 1: scatter into fixed-row shard files (no cluster needed)
earthcatalog ingest --inventory s3://bucket/inventory/full.parquet \
    --mode full --scatter-only
# Step 2: map/reduce on the cluster
earthcatalog ingest --inventory s3://…/staging/shards/<run_id>/scatter.json \
    --mode full --scheduler coiled
```

See the [Ingest Guide](operations/ingest_guide.md) for scheduler options.

## Catching up a stale catalog

If the catalog index is behind the inventory (missed runs), the daily flow
self-corrects: the diff still only carries the days' changes, and the
anti-join ingests everything the index is missing. For a large gap, diff
against the index directly:

```bash
earthcatalog diff --current s3://…/today.manifest.json \
    --against-index s3://…/catalog/warehouse_index.parquet \
    --out s3://…/catchup.parquet
earthcatalog ingest --diff s3://…/catchup.parquet --warehouse s3://…/catalog/warehouse
```

## Search

Iceberg pruning narrows the search to relevant files, then DuckDB or
rustac applies spatial, temporal, and CQL2 filters per file.

### Fastest — `duck_search()`

Uses DuckDB's parallel I/O — **~2× faster** than the other methods
across all query types.  Returns a ``pandas.DataFrame`` (no pystac
conversion overhead).  It is a module function in ``earthcatalog.search``:

```python
import cql2
from earthcatalog.search import duck_search

df = duck_search(
    catalog,
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('platform = "sentinel-1"').to_json(),
    max_items=100,
)
# df is a pandas.DataFrame with flat columns
```

### Lazy / pystac — `search()`

Returns a lazy ``EarthCatalogItemSearch`` that yields ``pystac.Item``
objects.  Same speed as ``search_to_arrow()``.  Best for interactive
use with ``max_items=100`` where early exit avoids wasted work.

```python
results = catalog.search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('platform = "sentinel-1"').to_json(),
    max_items=100,
)
for item in results.items():
    print(item.id, item.properties["platform"])
```

### PyArrow — `search_to_arrow()`

Returns a ``pyarrow.Table``.  Useful for zero-copy interchange with
other Arrow-native tools.

```python
table = catalog.search_to_arrow(
    bbox=[-60, 60, -20, 85],
    datetime="2020-01/..",
)
```

### Bulk URIs — `search_uris()`

Returns a ``pandas.DataFrame`` with ``(id, uri)`` — the data download
URLs extracted from the ``assets`` column.  Reads only 2 columns from
S3, making it the fastest method for URL-only workflows.

```python
from earthcatalog.search import search_uris

df = search_uris(
    catalog,
    intersects={"type": "Point", "coordinates": [-45, 70]},
    filter=cql2.parse_text('percent_valid_pixels >= 80').to_json(),
    max_items=1000,
)
for _, row in df.iterrows():
    print(row.id, row.uri)
```

## Query with DuckDB

Lower-level: Iceberg partition pruning finds the relevant Parquet file paths,
then DuckDB reads them directly.

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

## Catalog info

```python
catalog.stats()              # per-partition row/file counts
catalog.unique_item_count()  # active STAC items (from the unified index)
catalog.info()               # grid metadata (type, resolution, boundaries)
```
