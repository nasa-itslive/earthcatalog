# Querying the Catalog

How to query the EarthCatalog with DuckDB, rustac, and spatial/temporal filters.

---

## EarthCatalog Search API

The simplest way to find files is through the EarthCatalog facade:

```python
from earthcatalog.core import catalog
from obstore.store import S3Store
from shapely.geometry import Point

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
ec = catalog.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Iceberg partition pruning — zero I/O on irrelevant files
point = Point(-133.99, 58.74)
paths = ec.search_files(point, start_datetime='2020-01-01', end_datetime='2022-12-31')
# paths is a list of S3 URIs ready for DuckDB read_parquet()
```

See the [`search_files()`][earthcatalog.core.earthcatalog.EarthCatalog.search_files] docstring for details.

## Quick Query (DuckDB)

### Spatial + temporal query with H3 cell pruning

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE datetime >= '2020-01-01'
      AND datetime <= '2022-12-31T23:59:59Z'
      AND ST_Intersects(geometry, ST_GeomFromText('{point.wkt}'))
    ORDER BY datetime
    LIMIT 10
""").df()
print(df)
```

---

## rustac (STAC-native)

### Query with CQL2 filters

```python
import rustac, cql2, json, pystac
from earthcatalog.core import catalog, store_config
from obstore.store import S3Store
from shapely.geometry import Point

# Connect to catalog
store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
catalog.download_catalog('/tmp/earthcatalog.db')

c = catalog.open('/tmp/earthcatalog.db',
               's3://its-live-data/test-space/stac/catalog/warehouse')
table = catalog.get_or_create(c)
info = catalog.info(table)

# Get file paths (Iceberg pruning)
point = Point(-133.99, 58.74)
paths = info.file_paths(table, point, start_datetime='2020-01-01',
                       end_datetime='2022-12-31')

# Query with CQL2
client = rustac.DuckdbClient()
cql2_filter = cql2.parse_text('percent_valid_pixels > 50').to_json()

# rustac.DuckdbClient.search() takes one href at a time
items = []
for path in paths:
    items.extend(client.search(path, filter=cql2_filter))

# Hydrate to pystac.Item (assets/links stored as JSON strings)
def to_stac(raw: dict) -> pystac.Item:
    for key in ('assets', 'links', 'bbox'):
        if isinstance(raw.get(key), str):
            raw[key] = json.loads(raw[key])
    return pystac.Item.from_dict(raw)

stac_items = [to_stac(i) for i in items]
```

### CQL2 Examples

```python
import cql2

# Simple property filter
cql2.parse_text('platform = "sentinel-2"')

# Range filter
cql2.parse_text('percent_valid_pixels > 50')

# Temporal filter
cql2.parse_text('datetime >= "2020-01-01" AND datetime <= "2022-12-31"')

# Combined
cql2.parse_text('platform = "landsat-8" AND percent_valid_pixels > 70')

# In list
cql2.parse_text('platform IN ("sentinel-2", "landsat-8", "landsat-9")')
```

---

## Spatial Query Options

### ST_Intersects (polygon or point)

```python
# Point intersection
ST_Intersects(geometry, ST_GeomFromText('POINT(-133.99 58.74)'))

# Polygon intersection  
ST_Intersects(geometry, ST_GeomFromText('POLYGON((-140 55, -130 55, -130 60, -140 60, -140 55))'))
```

### Bounding box filter (faster, no geometry parsing)

```python
# Using DuckDB's bbox extension
SELECT id, platform, datetime
FROM read_parquet({paths})
WHERE ST_XMin(geometry) >= -140
  AND ST_XMax(geometry) <= -130
  AND ST_YMin(geometry) >= 55
  AND ST_YMax(geometry) <= 60
```

---

## Benchmarks

| Query Type | Files Opened | Rows Returned | Latency |
|-----------|-------------|--------------|--------|
| Full scan (no filter) | 5,024 | 63.4M | ~30s |
| Cell filter (10 cells) | ~50 | 5.2M | ~5s |
| Cell + year (2020) | ~10 | 1.1M | ~1s |
| Cell + year + platform | ~5 | 150K | ~0.5s |
| Point intersection | ~2 | ~100 | ~0.3s |

> **Note**: Benchmarks are from the production catalog (~63M rows, 5k files). Actual times vary by network, instance type, and S3 throughput.

### Performance Tips

1. **Use Iceberg partition pruning**: `info.file_paths()` filters by H3 cell and year first
2. **Prefer temporal filters**: Year partition is heavily pruned
3. **Avoid ST_Intersects on full scans**: Use bbox filters when possible
4. **Limit result sets**: Always use `LIMIT` for exploratory queries

---

## Full Examples

### Find all Sentinel-2 scenes over Greenland in 2020

```python
import duckdb
from earthcatalog.core import catalog, store_config
from obstore.store import S3Store

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
catalog.download_catalog('/tmp/earthcatalog.db')

c = catalog.open('/tmp/earthcatalog.db',
               's3://its-live-data/test-space/stac/catalog/warehouse')
table = catalog.get_or_create(c)
info = catalog.info(table)

# Greenland bbox
from shapely.geometry import Polygon
greenland = Polygon([(-60, 60), (-20, 60), (-20, 85), (-60, 85), (-60, 60)])
paths = info.file_paths(table, greenland, start_datetime='2020-01-01', end_datetime='2020-12-31')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

df = con.execute(f"""
    SELECT id, platform, datetime, 
           ST_XMin(geometry) AS xmin, ST_YMin(geometry) AS ymin
    FROM read_parquet({paths})
    WHERE platform = 'sentinel-2'
      AND ST_YMax(geometry) >= 60
    ORDER BY datetime
    LIMIT 100
""").df()
```

### Time series for a single location

```python
import duckdb
from earthcatalog.core import catalog, store_config
from obstore.store import S3Store
from shapely.geometry import Point

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
catalog.download_catalog('/tmp/earthcatalog.db')

c = catalog.open('/tmp/earthcatalog.db',
               's3://its-live-data/test-space/stac/catalog/warehouse')
table = catalog.get_or_create(c)
info = catalog.info(table)

# Point in Alaska
point = Point(-149.5, 63.5)
paths = info.file_paths(table, point, start_datetime='2018-01-01', end_datetime='2023-12-31')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

# Time series by month
df = con.execute(f"""
    SELECT DATE_TRUNC('month', datetime) AS month, COUNT(*) AS scenes
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{point.wkt}'))
    GROUP BY DATE_TRUNC('month', datetime)
    ORDER BY month
""").df()
print(df)
```

---

## API Reference

### catalog.info(table)

Returns `CatalogInfo` with grid metadata:

```python
info = catalog.info(table)
info.grid_type        # 'h3'
info.h3_resolution  # 1
info.hash_index_path  # 's3://.../warehouse_id_hashes.parquet'
```

### catalog.info().file_paths(table, geometry, start_datetime, end_datetime)

Prunes files by H3 cell + year partition:

```python
paths = info.file_paths(
    table,
    geometry=Point(-133.99, 58.74),  # shapely geometry
    start_datetime='2020-01-01',
    end_datetime='2022-12-31'
)
# Returns: ['s3://.../warehouse/grid_partition=.../year=2020/part_000001.parquet', ...]
```

Returns a list of parquet file paths that can be passed directly to `read_parquet()`.