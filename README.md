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

```bash
earthcatalog incremental \
    --inventory  /tmp/s3_inventory.csv \
    --catalog    /tmp/catalog.db \
    --warehouse  /tmp/warehouse
```

```python
import duckdb
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/warehouse")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute(f"""
    SELECT id, platform, grid_partition, datetime
    FROM iceberg_scan('{table.metadata_location}')
    WHERE platform = 'sentinel-2'
    LIMIT 20
""").df()
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
