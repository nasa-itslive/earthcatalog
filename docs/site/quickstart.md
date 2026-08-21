# Quick Start

EarthCatalog ingests STAC items from S3 into a spatially-partitioned GeoParquet
catalog backed by Apache Iceberg. Instead of a database, Parquet files sit on S3
and a small SQLite file tracks the Iceberg schema. DuckDB reads them directly —
no serialization overhead, no infrastructure.

## Bulk ingest

First-time full ingest from an S3 Inventory file. Drops any existing table and
recreates it from scratch.

```python
from earthcatalog.ingest_config import IngestConfig

catalog.ingest_inventory(
    "s3://bucket/inventory/full.parquet",
    mode="full",
    config=IngestConfig(create_client=lambda: coiled.Client(n_workers=100)),
)
```

For smaller inventories the single-node path works without Dask (the default
``stage="ndjson"`` writes resumable NDJSON first, then compacts in bounded
memory):

```python
catalog.ingest_inventory("s3://bucket/inventory/full.parquet", mode="full")
```

Or from the CLI:

```bash
uv run earthcatalog ingest \
  --inventory s3://bucket/inventory/full.parquet \
  --warehouse s3://my-bucket/catalog/warehouse \
  --mode full
```

## Delta ingest

Daily incremental updates. Appends new files to the existing table without
overwriting, and updates the unified index for duplicate detection.

```python
catalog.ingest_inventory("s3://bucket/delta/2026-04-28.parquet", mode="delta")
```

Or from the CLI:

```bash
uv run earthcatalog ingest \
  --inventory s3://bucket/delta/2026-04-28.parquet \
  --warehouse s3://my-bucket/catalog/warehouse \
  --mode delta
```

Ingest is resumable: `--skip-fetch` resumes compaction of already-staged
NDJSON, and `--skip-compact` only stages NDJSON for a later run.

## Search

Iceberg pruning narrows the search to relevant files, then DuckDB or
rustac applies spatial, temporal, and CQL2 filters per file.

### Fastest — `duck_search()`

Uses DuckDB's parallel I/O — **~2× faster** than the other methods
across all query types.  Returns a ``pandas.DataFrame`` (no pystac
conversion overhead).

```python
import cql2

df = catalog.duck_search(
    intersects={"type": "Point", "coordinates": [0, 60]},
    datetime="2020-01-01/2020-12-31",
    filter=cql2.parse_text('platform = "sentinel-1"').to_json(),
    max_items=100,
)
# df is a pandas.DataFrame with flat columns
```

### Lazy / pystac — `search()`

Returns a lazy ``EarthCatalogItemSearch`` that yields ``pystac.Item``
objects.  Same speed as ``search_to_arrow()``.  Best for interactive
use with ``max_items=100`` where early exit avoids wasted work.

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

### PyArrow — `search_to_arrow()`

Returns a ``pyarrow.Table``.  Useful for zero-copy interchange with
other Arrow-native tools.

```python
table = catalog.search_to_arrow(
    bbox=[-60, 60, -20, 85],
    datetime="2020-01/..",
)
```

### Bulk URIs — `search_uris()`

Returns a ``pandas.DataFrame`` with ``(id, uri)`` — the data download
URLs extracted from the ``assets`` column.  Reads only 2 columns from
S3, making it the fastest method for URL-only workflows.

```python
df = catalog.search_uris(
    intersects={"type": "Point", "coordinates": [-45, 70]},
    filter=cql2.parse_text('percent_valid_pixels >= 80').to_json(),
    max_items=1000,
)
for _, row in df.iterrows():
    print(row.id, row.uri)
```

## Query with DuckDB

Lower-level: Iceberg partition pruning finds the relevant Parquet file paths,
then DuckDB reads them directly.

```python
from shapely.geometry import box
import duckdb

greenland = box(-60, 60, -20, 85)
paths = catalog.search_files(greenland, start_datetime="2020-01-01")

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{greenland.wkt}'))
    LIMIT 10
""").df()
```

## Catalog info

```python
catalog.stats()              # per-partition row/file counts
catalog.unique_item_count()  # active STAC items (from the unified index)
catalog.info()               # grid metadata (type, resolution, boundaries)
```
