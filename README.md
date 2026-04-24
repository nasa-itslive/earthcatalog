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
earthcatalog backfill \
    --inventory  s3://my-bucket/inventory/manifest.json \
    --catalog    s3://my-bucket/catalog/catalog.db \
    --warehouse  s3://my-bucket/warehouse \
    --workers    32
```

### 2. Incremental updates

After the initial ingest, run the incremental pipeline whenever a new S3 Inventory
is available. It fetches only keys modified since the last run (`--since`), writes
new GeoParquet files, and appends a new Iceberg snapshot. Typical runs complete in
minutes for daily deltas.

```bash
earthcatalog incremental \
    --inventory  /tmp/s3_inventory.csv \
    --catalog    /tmp/catalog.db \
    --warehouse  /tmp/warehouse \
    --since      2024-01-01
```

### 3. Spatial query

EarthCatalog partitions data by H3 cell and year. Pass the H3 cells that cover your
region of interest to `WHERE grid_partition IN (...)` — Iceberg skips every file
outside those cells with zero I/O.

```python
import duckdb, h3
from shapely.geometry import box, mapping
from earthcatalog.core.catalog import open_catalog, get_or_create_table

# Convert a bounding box to H3 cells at the catalog's resolution (default 1)
bbox = box(-60, 60, -30, 80)  # Greenland
cells = h3.geo_to_cells(mapping(bbox), res=1)
cell_list = ", ".join(f"'{c}'" for c in cells)

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/warehouse")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg; INSTALL spatial; LOAD spatial;")

df = con.execute(f"""
    SELECT id, platform, datetime, geometry
    FROM iceberg_scan('{table.metadata_location}')
    WHERE grid_partition IN ({cell_list})
      AND datetime >= '2022-01-01'
      AND ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{bbox.wkt}'))
    ORDER BY datetime
""").df()
# grid_partition prunes to candidate files (zero I/O on the rest);
# ST_Intersects then does exact geometry intersection within those files.
```

---

## Documentation

See the [hosted documentation](https://nsidc.github.io/earthcatalog/) for the full user guide,
architecture overview, and API reference.

---

## Requirements

- Python 3.12+
- See `environment.yml` for the full dependency list

Primary dependencies: `pyarrow`, `pyiceberg`, `obstore`, `rustac`, `h3-py`, `shapely`, `duckdb`
