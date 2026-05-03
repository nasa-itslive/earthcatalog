# Querying the Catalog

How to search the catalog using the high-level API or directly with DuckDB.

---

## EarthCatalog Search API

The primary way to search.  Iceberg partition pruning narrows the search
to relevant files (zero I/O), then rustac applies spatial, temporal, and
CQL2 filters per file.

```python
import earthcatalog as ec
from obstore.store import S3Store

store = S3Store(bucket='its-live-data', region='us-west-2', skip_signature=True)
catalog = ec.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

# Simple spatial query
results = catalog.search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    max_items=100,
)
for item in results.items():
    print(item.id, item.properties["platform"])
```

### CQL2 filters

Filters use `cql2.parse_text()` for a natural SQL-like syntax:

```python
import cql2

results = catalog.search(
    intersects={"type": "Point", "coordinates": [-45, 70]},
    filter=cql2.parse_text('percent_valid_pixels >= 80').to_json(),
    max_items=100,
)
```

CQL2 expressions support standard comparisons, `AND`/`OR`, `IN`, etc.:

```python
cql2.parse_text('platform = "sentinel-2"')
cql2.parse_text('percent_valid_pixels > 50')
cql2.parse_text('datetime >= "2020-01-01" AND datetime <= "2022-12-31"')
cql2.parse_text('platform IN ("sentinel-2", "landsat-8", "landsat-9")')
cql2.parse_text('platform = "landsat-8" AND percent_valid_pixels > 70')
```

For callers that already have CQL2 JSON (e.g. from a UI builder), the
raw JSON format is also accepted:

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

### Items as PyArrow

```python
table = catalog.search_to_arrow(
    bbox=[-60, 60, -20, 85],
    datetime="2020-01/..",
)
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

**Why `max_items=100` is fast**: files are read sequentially and the
search stops as soon as enough matching items are found.  Since each
warehouse file averages ~12,600 items, a `max_items=100` query almost
always finishes after the first file regardless of filter selectivity —
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

## BYO Query Engine: DuckDB

If you need raw SQL access (aggregations, joins, arbitrary expressions),
use Iceberg pruning to get the file list, then query with DuckDB directly.

### Greenland 2020, Sentinel-2

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
# ST_Intersects (polygon or point)
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

Returns a list of parquet file paths that can be passed directly to
DuckDB's ``read_parquet()``.
