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
- **H3 resolution 1**: 842 global cells at roughly equal area (~5M km² each); works with any grid, not tied to H3 specifically
- **Zero serialization overhead**: DuckDB reads directly from S3; bulk exports are limited only by network bandwidth
- **No credentials needed**: Public ITS_LIVE bucket accessible without AWS keys
- **SQLite on S3**: No infrastructure (no RDS, no Glue, no REST API) — the catalog is a single SQLite file on S3

---

## How to use

### 1. Install

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

### 2. Query the catalog

The fastest way to explore the catalog is with the EarthCatalog facade and DuckDB. No credentials needed:

```python
from earthcatalog.core import catalog
from obstore.store import S3Store
from shapely.geometry import Point
import duckdb

# Open — returns EarthCatalog with Iceberg table + grid metadata
store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
ec = catalog.open(store=store,
                  base='s3://its-live-data/test-space/stac/catalog')

# Iceberg partition pruning — zero I/O on irrelevant files
point = Point(-133.99, 58.74)
paths = ec.search_files(point, start_datetime='2020-01-01')

# Query with DuckDB
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{point.wkt}'))
    LIMIT 10
""").df()
```

See [Query Catalog](operations/query_catalog.md) for DuckDB + rustac + CQL2 examples and [`search_files()` API docs](api/core.md).

### 3. Ingest a delta

Daily delta ingest from an S3 Inventory file, single-node:

```python
ec.ingest("s3://bucket/delta/2026-04-28.parquet",
          mode="delta",
          update_hash_index=True)
```

For large backfills on Dask/Coiled:

```python
ec.bulk_ingest("s3://bucket/full_inventory.parquet",
               mode="full",
               create_client=lambda: coiled.Client(n_workers=100))
```

See the [`ingest()`](api/core.md) and [`bulk_ingest()`](api/core.md) docstrings for all parameters, or the [Ingest Workflow](operations/ingest_workflow.md) for the GitHub Actions configuration.

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

## Project status

> **Alpha** — The schema, partition spec, and CLI are stable for the ITS_LIVE velocity-pair catalog (~40M items). API may change.

What's working:
- Full backfill from S3 Inventory
- Daily delta ingest with isolated staging
- H3 cell + year partition pruning
- DuckDB + rustac queries with spatial filters
- Hash index auto-update in Phase 4

What's not yet:
- Pypi package publishing
- Hosted documentation (GitHub Pages build pending)
- Automated compaction schedule

---

## Learn more

- **[Quick Start](quickstart.md)** — Install and run in five minutes
- **[Architecture](architecture.md)** — Deep dive into the pipeline design
- **[Configuration](configuration.md)** — YAML config reference
- **[Query Catalog](operations/query_catalog.md)** — DuckDB, rustac, and CQL2 examples
- **[Ingest Workflow](operations/ingest_workflow.md)** — GitHub Actions daily delta + ingest
- **[Maintenance](maintenance/compact.md)** — Warehouse compaction
- **[API Reference](api/index.md)** — Python API docs

---

<i>Built from commit <a href="https://github.com/nasa-itslive/earthcatalog/commit/9083f82">9083f82</a> (2026-05-01)</i>
