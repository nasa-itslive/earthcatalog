# earthcatalog

**Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg.**

earthcatalog ingests STAC item catalogs from AWS S3 into a spatially-partitioned [Apache Iceberg](https://iceberg.apache.org/) table. The resulting catalog can be queried with [DuckDB](https://duckdb.org/) or any Iceberg-compatible engine using efficient spatial and temporal predicate pushdown.

> **Project Status: Alpha** — The schema, partition spec, and CLI are stable for the ITS_LIVE velocity-pair catalog. Public bucket access requires no AWS credentials.

---

## What it does

earthcatalog transforms STAC items from the public ITS_LIVE S3 bucket into a queryable Parquet catalog. Each STAC item is fanned out to one row per intersecting H3 cell at resolution 1 (~100km hexagons), then grouped by cell and year into Parquet files:

- **Input**: S3 Inventory manifest with `.stac.json` keys
- **Fan-out**: One row per (item × H3 cell) — a point near a cell boundary maps to multiple cells
- **Output**: One Parquet file per `(grid_partition, year)` bucket
- **Catalog**: PyIceberg table backed by SQLite, hosted on S3

**File pruning happens at read time**: A DuckDB query on a point queries only the Parquet files for that cell + year — no full scan required.

---

## Why earthcatalog

Traditional STAC catalogs either:
- Store one row per item (not spatially partitioned) — queries scan everything
- Use naive tiling (lat/lon grids) — cells near poles have wildly different areas

earthcatalog uses H3 resolution 1:
- 842 global cells at roughly equal area (~5M km² each)
- Files are compact — each cell typically has 50-200K rows
- Spatial queries open only the relevant files

Other benefits:
- **No AWS credentials needed** for the public ITS_LIVE bucket
- **No infrastructure** — SQLite catalog on S3, no Glue NoCatalog, no REST servers
- **Incremental updates** — only new items from the S3 Inventory delta

---

## How to use

### 1. Install

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

### 2. Query the catalog

The fastest way to explore the catalog is with DuckDB. No credentials needed:

```python
import duckdb
from shapely.geometry import Point
from earthcatalog.core import catalog, store_config
from obstore.store import S3Store

# Connect (no credentials for public bucket)
store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
catalog.download_catalog('/tmp/earthcatalog.db')

c = catalog.open('/tmp/earthcatalog.db', 
               's3://its-live-data/test-space/stac/catalog/warehouse')
table = catalog.get_or_create(c)
info = catalog.info(table)

# Prune to relevant files by H3 cell + year
point = Point(-133.99, 58.74)
paths = info.file_paths(table, point, start_datetime='2020-01-01', 
                      end_datetime='2022-12-31')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{point.wkt}'))
    LIMIT 10
""").df()
```

See [Query Catalog](operations/query_catalog.md) for rustac + CQL2 examples.

### 3. Ingest an increment

Daily increments come from the S3 Inventory delta. The GitHub Actions workflow handles this:

```bash
# Daily at 04:00 UTC
python scripts/run_backfill.py \
  --inventory s3://its-live-data/test-space/stac/catalog/delta/pending/delta_2026-04-28.parquet \
  --catalog /tmp/earthcatalog.db \
  --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
  --staging s3://its-live-data/test-space/stac/catalog/ingest/delta_2026-04-28 \
  --delta \
  --update-hash-index \
  --scheduler local \
  --workers 4
```

Key flags:
- `--delta`: Append mode (add files without overwriting)
- `--update-hash-index`: Merge new item IDs into the hash index in Phase 4
- `--staging`: Date-specific path for isolated runs

See [Ingest Workflow](operations/ingest_workflow.md) for the full workflow and GitHub Actions configuration.

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