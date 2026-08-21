<center><img src="https://raw.githubusercontent.com/nasa-itslive/earthcatalog/refs/heads/main/docs/images/earthcatalog.png" width="300px" /></center>


# earthcatalog

**Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg.**

earthcatalog ingests STAC item catalogs from AWS S3 into a spatially-partitioned [Apache Iceberg](https://iceberg.apache.org/) table. The resulting catalog can be queried with [DuckDB](https://duckdb.org/) or any Iceberg-compatible engine using efficient spatial and temporal predicate pushdown.

> **Project Status: Alpha** — The schema, partition spec, and CLI are stable for the ITS_LIVE velocity-pair catalog. Public bucket access requires no AWS credentials.

---

## What it does

earthcatalog transforms STAC items from public S3 buckets into a queryable Parquet catalog. Each STAC item is mapped to a DGGS (H3 by default) cells, then grouped by cell and year into Parquet files:

- **Input**: S3 Inventory manifest with `.stac.json` keys, the stac items
- **Spatial partitioning**: One row per (item × H3 cell) — a point near a cell boundary maps to multiple cells
- **Output**: One Parquet file per `(grid_partition, year)` bucket
- **Catalog**: PyIceberg table backed by SQLite, hosted on S3

[Why spatial partitioning matters →](https://www.architecture-performance.fr/ap_blog/spatial-queries-in-duckdb-with-r-tree-and-h3-indexing/)

**File pruning happens at read time**: A DuckDB query on a point queries only the Parquet files for that cell + year — no full scan required.

---

## Why earthcatalog

Traditional STAC implementations use databases (PostgreSQL with PostGIS, Cloud SQL, etc.) to serve API queries. While fine for single-item lookups, they struggle with bulk exports — retrieving 100K+ rows means streaming through a database cursor with all the serialization overhead.

earthcatalog takes a different approach — spatially partitioned GeoParquet:

- **No moving parts**: Parquet files sit on S3, no database to maintain or sync
- **Spatial partitioning**: Queries with spatial filters open only relevant files — typically 2-10 files out of 5,000
- **Zero serialization overhead**: DuckDB reads directly from S3; bulk exports are limited only by network bandwidth
- **SQLite on S3**: No infrastructure (no RDS, no Glue, no REST API) — the catalog is a single SQLite file on S3

---

## How to use

### 1. Install

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

### 2. Quick start

```python
import earthcatalog
from obstore.store import S3Store
from shapely.geometry import box
import duckdb

# Open — returns EarthCatalog
store = S3Store(bucket="its-live-data", region="us-west-2", skip_signature=True)
catalog = earthcatalog.open(store=store, base="s3://its-live-data/test-space/stac/catalog")

# Search — rustac-powered with Iceberg file pruning
search = catalog.search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter={"op": "=", "args": [{"property": "platform"}, "sentinel-1"]},
    max_items=10,
)

# we get pystac items back!
for items in search.items():
    print(items)

# Or get results as a PyArrow table
table = catalog.search_to_arrow(bbox=[-60, 60, -20, 85])
```

### 3. Ingest

```python
# Daily delta (single-node) — resumable, NDJSON-staged
catalog.ingest_inventory("s3://bucket/delta.parquet", mode="delta")

# Large ingest (Dask/Coiled)
from earthcatalog.ingest_config import IngestConfig
catalog.ingest_inventory(
    "s3://bucket/full.parquet",
    config=IngestConfig(create_client=coiled.Client),
)
```

---

## Key features

| Feature | Detail |
|---|---|
| **obstore** for all S3 I/O | No credentials needed for public buckets (`skip_signature=True`) |
| **H3 spatial partitioning** | Resolution 1 hex grid — 842 global cells at ~5M km² each |
| **rustac GeoParquet** | `geo` metadata and `geoarrow.wkb` extension handled automatically |
| **PyIceberg + SQLite** | Zero-infra catalog — no Glue, no REST server |
| **Iceberg partition pruning** | `IdentityTransform(grid_partition)` + `YearTransform(datetime)` |
| **S3 atomic lock** | `If-None-Match: *` conditional write — no DynamoDB required |
| **Incremental delta ingest** | Delta parquet with new items only |
| **Hash index** | xxh3_128 for O(log n) duplicate detection |

---

---

<i>Built from commit <a href="https://github.com/nasa-itslive/earthcatalog/commit/7a0a3f6">7a0a3f6</a> (2026-05-01)</i>
