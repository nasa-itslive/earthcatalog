# Querying the Catalog

How to query the EarthCatalog with DuckDB, rustac, and spatial/temporal filters.

---

## EarthCatalog Search API

The simplest way to find files is through the EarthCatalog facade:

```python
import earthcatalog as ec
from obstore.store import S3Store
from shapely.geometry import Point

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Iceberg partition pruning — zero I/O on irrelevant files
point = Point(-133.99, 58.74)
paths = catalog.search_files(point, start_datetime='2020-01-01', end_datetime='2022-12-31')
# paths is a list of S3 URIs ready for DuckDB read_parquet()
```

See the [`search_files()`][earthcatalog.catalog.EarthCatalog.search_files] docstring for details.

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
import earthcatalog as ec
from earthcatalog.catalog import download_catalog, get_or_create
from earthcatalog.core import store_config
from obstore.store import S3Store
from shapely.geometry import Point

# Connect to catalog
store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
download_catalog('/tmp/earthcatalog.db')

catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Get file paths (Iceberg pruning)
point = Point(-133.99, 58.74)
paths = catalog.info().file_paths(catalog.table, point, start_datetime='2020-01-01',
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

## Performance

Estimated latency for a region like northern Greenland, based on the
production catalog (63.4M rows, 5,024 files, ~12,600 items/file):

| Scenario | Files after pruning | Est. latency | Items returned |
|---|---|---|---|
| Spatial only, no filter | ~80 | ~9s | ~1M |
| + year=2020 | ~10 | ~1s | ~126K |
| + year=2020 + CQL2 filter + max_items=100 | ~1 | ~0.1–0.2s | 100 |
| + year=2020 + highly selective CQL2 + max_items=100 | ~2–3 | ~0.3–0.4s | ~50 |

**Why `max_items=100` is fast**: the search reads files sequentially and
stops as soon as enough matching items are found.  Since each warehouse
file averages ~12,600 items, a `max_items=100` query almost always
finishes after the first file regardless of filter selectivity —
provided the filter matches at least 100 rows in that file.

**Why latency scales with files, not total catalog size**: Iceberg
partition pruning narrows the search to only the H3 cells and years
that intersect your query geometry.  A point in Greenland resolves to
~1-2 cells; a large polygon to ~5-10.  Each cell has 5-10 years of
data, so even a full-catalog spatial query reads only ~25-100 files
(out of 5,024).  The remaining 98% of files are never opened.

### DuckDB Parquet predicate pushdown

When a CQL2 filter (e.g. ``percent_valid_pixels >= 1``) is present,
DuckDB's Parquet reader uses column chunk statistics (min, max)
from the Parquet footer to skip row groups *before* decompressing
any data:

- Reads the Parquet footer (~1-10 KB per file)
- For each row group, checks if ``stats.max >= 1``
- Skips row groups that can't possibly match
- Reads only the columns needed for the query from qualifying groups

For regional-scale queries the files are small enough (single row group)
that the footer read + full scan cost is similar regardless of filter
selectivity.  Predicate pushdown becomes significant for large files
with many row groups.

### Benchmarks (test data: 1,000 items, 4 files)

| Query | Items/s | Notes |
|---|---|---|
| No filter, max_items=100 | ~600 | Baseline |
| `percent_valid_pixels >= 1`, max_items=100 | ~750 | Nearly all rows qualify |
| `percent_valid_pixels <= 50`, max_items=100 | ~860 | ~50% qualify |
| No limit, no filter | ~2,200 | Full sequential scan |
| `pages()` | 4 pages | One per Iceberg partition |

### Performance Tips

1. **Always use `max_items`** for exploratory queries — keeps latency under 0.2s
2. **Prefer temporal filters** — the `year` partition is heavily pruned
3. **Spatial + temporal = fastest** — both partitions are pruned before any file is opened
4. **Highly selective CQL2 filters** (e.g. `percent_valid_pixels >= 95`) skip row groups via Parquet statistics but don't reduce the number of files opened — pair them with `max_items` for consistent latency

---

## Full Examples

### Find all Sentinel-2 scenes over Greenland in 2020

```python
import duckdb
import earthcatalog as ec
from earthcatalog.catalog import download_catalog, get_or_create
from earthcatalog.core import store_config
from obstore.store import S3Store

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
download_catalog('/tmp/earthcatalog.db')

catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Greenland bbox
from shapely.geometry import Polygon
greenland = Polygon([(-60, 60), (-20, 60), (-20, 85), (-60, 85), (-60, 60)])
paths = catalog.info().file_paths(catalog.table, greenland, start_datetime='2020-01-01', end_datetime='2020-12-31')

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
import earthcatalog as ec
from earthcatalog.catalog import download_catalog, get_or_create
from earthcatalog.core import store_config
from obstore.store import S3Store
from shapely.geometry import Point

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
store_config.set_store(store)
store_config.set_catalog_key('test-space/stac/catalog/earthcatalog.db')
download_catalog('/tmp/earthcatalog.db')

catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Point in Alaska
point = Point(-149.5, 63.5)
paths = catalog.info().file_paths(catalog.table, point, start_datetime='2018-01-01', end_datetime='2023-12-31')

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

### EarthCatalog.info()

Returns `CatalogInfo` with grid metadata:

```python
info = catalog.info()
info.grid_type        # 'h3'
info.grid_resolution  # 1
```

### CatalogInfo.file_paths(table, geometry, start_datetime, end_datetime)

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