# Quick Start

EarthCatalog ingests STAC items from S3 into a spatially-partitioned GeoParquet
catalog backed by Apache Iceberg. Instead of a database, Parquet files sit on S3
and a small SQLite file tracks the Iceberg schema. DuckDB reads them directly —
no serialization overhead, no infrastructure.

## Bulk ingest

First-time full backfill from an S3 Inventory file. Drops any existing table and
recreates it from scratch.

```python
import earthcatalog as ea
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
ec = ea.open(store=store, base="s3://my-bucket/catalog")

ec.bulk_ingest("s3://bucket/inventory/full.parquet", mode="full",
               create_client=lambda: coiled.Client(n_workers=100))
```

For smaller inventories the single-node path works without Dask:

```python
ec.ingest("s3://bucket/inventory/full.parquet", mode="full")
```

## Delta ingest

Daily incremental updates. Appends new files to the existing table without
overwriting, and updates the hash index for duplicate detection.

```python
ec.ingest("s3://bucket/delta/2026-04-28.parquet",
          mode="delta",
          update_hash_index=True)
```

Optionally filter by modification date:

```python
from datetime import UTC, datetime, timedelta

ec.ingest("delta.parquet", mode="delta",
          since=datetime.now(UTC) - timedelta(days=2))
```

## Query with DuckDB

Iceberg partition pruning finds the relevant Parquet files for a geometry.
DuckDB reads them with spatial filtering — no full scan.

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

## Catalog info

```python
ec.stats()              # per-partition row/file counts
ec.unique_item_count()  # unique STAC items (from hash index)
ec.info()               # grid metadata (type, resolution, boundaries)
```
