# Querying the Catalog

Three search methods, from fastest to most flexible.

---

## Fastest — `duck_search(format="native")`

Uses DuckDB internally for parallel Parquet I/O.  **~2× faster** than
the other methods across all query types.  Returns a ``pandas.DataFrame``
with flat columns — no pystac conversion overhead.

```python
import earthcatalog as ec
import cql2
from obstore.store import S3Store

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

df = catalog.duck_search(
    format="native",
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('platform = "sentinel-1"').to_json(),
    max_items=100,
)
# df is a pandas.DataFrame — iterate or convert as needed
for _, row in df.iterrows():
    print(row["id"], row["platform"])
```

### `max_items` note

DuckDB's SQL ``LIMIT`` triggers a **7× slower query plan** for multi-file
scans.  ``duck_search()`` avoids this by fetching all matching rows and
truncating in Python.  For ``max_items ≤ 100,000`` the overhead is
negligible; for larger result sets use ``search_files()`` + hand-written
DuckDB SQL (see BYO section).

### Return a list of pystac Items

```python
items = catalog.duck_search(format="pystac", ...)
# list[pystac.Item] — slower due to conversion overhead
```

---

## Lazy iteration — `search()`

Returns a lazy ``EarthCatalogItemSearch`` that yields ``pystac.Item``
objects.  Comparable to ``search_to_arrow()`` in speed.  Best for
interactive use with ``max_items=100`` where early exit avoids wasted
work (sequential per-file processing stops as soon as enough items
are found).

```python
results = catalog.search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('platform = "sentinel-1"').to_json(),
    max_items=100,
)
for item in results.items():
    print(item.id, item.properties["platform"])
```

### CQL2 filters

Filters use ``cql2.parse_text()`` for a natural SQL-like syntax:

```python
import cql2

cql2.parse_text('platform = "sentinel-2"')
cql2.parse_text('percent_valid_pixels > 50')
cql2.parse_text('platform IN ("sentinel-2", "landsat-8", "landsat-9")')
cql2.parse_text('platform = "landsat-8" AND percent_valid_pixels > 70')
```

Temporal filtering uses the top-level ``datetime`` kwarg (STAC-standard).
Do **not** reference ``datetime`` inside CQL2 — rustac generates broken
SQL when ``datetime`` appears in a CQL2 expression:

```python
# ✅ Correct
results = catalog.search(
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('percent_valid_pixels >= 80').to_json(),
)
```

Raw CQL2 JSON dicts are also accepted:

```python
filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 80]}
```

### Pagination and metadata

```python
# pages() — one batch per file
for i, page in enumerate(results.pages()):
    print(f"Page {i}: {len(page)} items")

# matched() — estimated upper bound from Iceberg manifest (no Parquet I/O)
print(f"Up to {results.matched():,} matching rows")

# stats() — file count and data volume from manifests
s = results.stats()
print(f"{s['files']} files, ~{s['rows_upper_bound']:,} rows")
```

---

## PyArrow — `search_to_arrow()`

Returns a ``pyarrow.Table``.  Useful for zero-copy interchange with
other Arrow-native tools.  Same speed as ``search()``.

```python
table = catalog.search_to_arrow(
    bbox=[-60, 60, -20, 85],
    datetime="2020-01/..",
)
```

---

## Performance

See [`search_performance.md`](search_performance.md) for detailed
benchmarks across all methods against the production catalog.

| Method | Narrow query (2 files) | Wide query (32 files) | Wide + 100k limit |
|---|---|---|---|
| `search()` | ~2s | ~36s | ~55s |
| `duck_search(pystac)` | ~2s | ~4s | ~57s |
| `duck_search(native)` | **~2s** | **~3s (11×)** | **~28s (2×)** |
| `search_to_arrow()` | ~2s | ~34s | ~47s |

### How it works

1. **Iceberg partition pruning** resolves the spatial/temporal query
   to a list of file paths (zero I/O on non-matching files).
2. **DuckDB or rustac** then reads those files and applies CQL2 filters.

For narrow queries (few files), all methods are bottlenecked by the
S3 download + Parquet scan time (~1s per file).  For wide queries with
sparse data spread across many files, ``duck_search(native)`` benefits
from DuckDB's internal parallel I/O while rustac reads files sequentially.

### Performance Tips

1. **Use ``duck_search(format="native")``** for fastest results
2. **Use ``search()`` for lazy iteration** with ``max_items=100`` — early exit avoids wasted work
3. **Prefer temporal filters** — the ``year`` partition is heavily pruned
4. **Spatial + temporal = fastest** — both partitions are pruned before any file is opened
5. **Highly selective CQL2 filters** pair with ``max_items`` for consistent latency

---

## BYO Query Engine: DuckDB

If you need raw SQL access (aggregations, joins, arbitrary expressions),
use ``search_files()`` to get the file list, then query with DuckDB
directly.  Remember to configure anonymous S3 access:

```python
import duckdb
import earthcatalog as ec
from obstore.store import S3Store
from shapely.geometry import Polygon

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

greenland = Polygon([(-60, 60), (-20, 60), (-20, 85), (-60, 85), (-60, 60)])
paths = catalog.search_files(greenland, start_datetime='2020-01-01', end_datetime='2020-12-31')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
# Anonymous S3 — DuckDB inherits AWS creds from env; explicitly clear them
con.execute("SET s3_access_key_id='';")
con.execute("SET s3_secret_access_key='';")
con.execute("SET s3_session_token='';")

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
from shapely.geometry import Point

point = Point(-149.5, 63.5)
paths = catalog.search_files(point, start_datetime='2018-01-01', end_datetime='2023-12-31')

df = con.execute(f"""
    SELECT DATE_TRUNC('month', datetime) AS month, COUNT(*) AS scenes
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{point.wkt}'))
    GROUP BY month
    ORDER BY month
""").df()
```

### Spatial query options

```python
# ST_Intersects
ST_Intersects(geometry, ST_GeomFromText('POINT(-133.99 58.74)'))

# Bounding box filter (faster, no geometry parsing)
SELECT id, platform, datetime
FROM read_parquet({paths})
WHERE ST_XMin(geometry) >= -140
  AND ST_XMax(geometry) <= -130
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

### catalog.search_files(geometry, start_datetime, end_datetime)

Prunes files by H3 cell + year partition:

```python
paths = catalog.search_files(
    geometry=Point(-133.99, 58.74),
    start_datetime='2020-01-01',
    end_datetime='2022-12-31'
)
# Returns: ['s3://.../warehouse/grid_partition=.../year=2020/part_000001.parquet', ...]
```
