<img src="docs/images/earthcatalog.png" alt="earthcatalog" width="200">

# earthcatalog

EarthCatalog is a Python library that transforms STAC (SpatioTemporal Asset Catalog) items into a
cloud-native, spatially partitioned GeoParquet catalog, enabling fast bulk spatial queries and
scalable analysis of large Earth observation datasets.

---

## When to use EarthCatalog

**Ideal for:**

- Large-scale Earth observation data (>1M STAC items)
- Asset distribution is sparse and global, e.g. Sentinel or Landsat scenes
- Frequent spatial queries on specific regions
- Time-series analysis of locations over time
- Multi-sensor data fusion from different providers
- Real-time data ingestion with incremental updates

**Not ideal for:**

- Small datasets (<10K items) — overhead outweighs benefits
- Simple one-time processing without query needs
- Regional datasets with similar geometries/footprints
- Non-spatial data without geographic components

---

## Quick start

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

### 1. Initial bulk ingest

Use the backfill pipeline for a first-time full ingest from an S3 Inventory manifest.
This fans out across Dask workers — each worker fetches STAC items from S3, writes
GeoParquet files directly to the warehouse, and returns only lightweight metadata to
the head node. Expect ~1M items/hour on a modest Dask cluster.

```bash
python scripts/run_backfill.py \
    --inventory  s3://my-bucket/inventory/manifest.json \
    --catalog    /tmp/catalog.db \
    --warehouse  s3://my-bucket/warehouse \
    --scheduler  local \
    --workers    32
```

### 2. Daily delta ingest

After the initial ingest, use the daily delta pipeline to detect and ingest new items.
The delta producer reads the S3 Inventory manifest, hashes item IDs with xxh3_128,
anti-joins against the warehouse hash index (~2 seconds for 40M items), and writes
a delta parquet with only new items.

```bash
# Produce delta (runs daily via GitHub Actions)
python scripts/daily_delta.py \
    s3://log-bucket/inventory/.../manifest.json \
    --warehouse-hash s3://my-bucket/warehouse_id_hashes.parquet \
    --delta-prefix s3://my-bucket/delta

# Ingest delta (runs weekly via GitHub Actions)
python scripts/run_backfill.py \
    --inventory s3://my-bucket/delta/pending/delta_2026-04-27.parquet \
    --delta \
    --scheduler local \
    --workers 4
```

### 3. Spatial query

EarthCatalog stores grid metadata (type and resolution) as Iceberg table
properties at ingest time. Use `CatalogInfo` to discover the grid system,
prune files via Iceberg partition filtering, and query with DuckDB's spatial
extension — no prior knowledge required.

```python
import duckdb
from shapely.geometry import box
from earthcatalog.core import catalog

c = catalog.open(db_path="/tmp/catalog.db", warehouse_path="/tmp/warehouse")
table   = catalog.get_or_create(c)
info    = catalog.info(table)

bbox  = box(-60, 60, -30, 80)                                    # Greenland
paths = info.file_paths(table, bbox)                              # Iceberg-partition-pruned file list

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE datetime >= '2022-01-01'
      AND ST_Intersects(geometry, ST_GeomFromText('{bbox.wkt}'))
    ORDER BY datetime
""").df()
# Iceberg partition pruning limits files scanned;
# ST_Intersects then does exact geometry intersection.
```

## Documentation

See the [hosted documentation](https://nasa-itslive.github.io/earthcatalog/) for the full user guide,
architecture overview, and API reference.

---

## Requirements

- Python 3.12+
- See `environment.yml` for the full dependency list

Primary dependencies: `pyarrow`, `pyiceberg`, `obstore`, `rustac`, `h3-py`, `shapely`, `duckdb`
