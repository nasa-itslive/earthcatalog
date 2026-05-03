<img src="docs/images/earthcatalog.png" alt="earthcatalog" width="200">

# earthcatalog

Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg. Transforms STAC items from S3 into a queryable GeoParquet catalog with spatial + temporal partition pruning.

```python
import earthcatalog as ea
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
ec = ea.open(store=store, base="s3://bucket/catalog")

# Ingest
ec.ingest("delta.parquet", mode="delta", update_hash_index=True)
ec.bulk_ingest("full_inventory.parquet", create_client=coiled.Client)

# Search
paths = ec.search_files(geometry, start_datetime="2020-01-01")
```

## Install

```bash
pip install earthcatalog
```

## Why

Traditional STAC databases (PostgreSQL + PostGIS) are fine for lookups but struggle with bulk exports. earthcatalog skips the database — spatially partitioned GeoParquet files sit on S3, indexed by an Iceberg table through a single SQLite file. DuckDB reads them directly with zero serialization overhead.

- No moving parts: Parquet files on S3, no database to maintain
- Spatial pruning: Iceberg partition filtering via H3 cells — queries open only relevant files
- Zero infra: SQLite catalog lives on S3, no Glue, no REST server
- Public data accessible without AWS credentials (`skip_signature=True`)

## Documentation

See the [hosted docs](https://nasa-itslive.github.io/earthcatalog/) for the full guide.
