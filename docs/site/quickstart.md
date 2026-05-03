# Quick Start

EarthCatalog ingests STAC items from S3 into a spatially-partitioned GeoParquet
catalog backed by Apache Iceberg. Instead of a database, Parquet files sit on S3
and a small SQLite file tracks the Iceberg schema. DuckDB reads them directly —
no serialization overhead, no infrastructure.

## Bulk ingest

First-time full backfill from an S3 Inventory file. Drops any existing table and
recreates it from scratch.

```python
import earthcatalog as ec
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
catalog = ec.open(store=store, base="s3://my-bucket/catalog")

catalog.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
                     create_client=lambda: coiled.Client(n_workers=100))
```

For smaller inventories the single-node path works without Dask:

```python
catalog.ingest("s3://bucket/inventory/full.parquet", mode="full")
```

## Delta ingest

Daily incremental updates. Appends new files to the existing table without
overwriting, and updates the hash index for duplicate detection.

```python
catalog.ingest("s3://bucket/delta/2026-04-28.parquet",
          mode="delta",
          update_hash_index=True)
```

Optionally filter by modification date:

```python
from datetime import UTC, datetime, timedelta

catalog.ingest("delta.parquet", mode="delta",
          since=datetime.now(UTC) - timedelta(days=2))
```

## Search with rustac

Higher-level search using `rustac.search` semantics with automatic file pruning.
Accepts CQL2 filters, spatial predicates, and temporal ranges — same kwargs as
`rustac.search()`.

```python
# Spatial + temporal + CQL2 filter — Iceberg prunes files, rustac filters rows
results = catalog.search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter={"op": "=", "args": [{"property": "platform"}, "sentinel-1"]},
    max_items=100,
)

# Results as a PyArrow table (zero-copy via Arrow PyCapsule protocol)
table = catalog.search_to_arrow(
    bbox=[-60, 60, -20, 85],
    datetime="2020-01/..",
)
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
catalog.unique_item_count()  # unique STAC items (from hash index)
catalog.info()               # grid metadata (type, resolution, boundaries)
```
