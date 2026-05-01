# Quick Start

Get earthcatalog up and running in five minutes.

---

## Prerequisites

- Python 3.12+
- AWS credentials (only needed to write to a private S3 bucket; reading ITS_LIVE data is public)

---

## 1. Install

```bash
pip install earthcatalog
```

Or for development with [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/nasa-itslive/earthcatalog.git
cd earthcatalog
uv sync --extra dev
```

---

## 2. Open a catalog

The `catalog` module is the main entry point.  You provide an obstore-compatible
store and a base path; EarthCatalog wires everything together.

```python
from earthcatalog.core import catalog
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
ec = catalog.open(
    store=store,
    base="s3://its-live-data/test-space/stac/catalog",
)
print(ec)
# EarthCatalog(grid_type='h3', resolution=1)
```

The store handles all I/O — `S3Store`, `LocalStore`, and `MemoryStore` are
interchangeable.  No special handling for different URI schemes needed.

---

## 3. Full ingestion

Ingest STAC items from an S3 Inventory file into a fresh catalog:

```python
ec.ingest(
    "s3://my-bucket/inventory/full.parquet",
    mode="full",
    chunk_size=10000,
    update_hash_index=True,
)
```

- **`mode="full"`**: drops any existing Iceberg table and recreates it
- Files are written to the warehouse store in hive-style layout:
  `grid_partition={cell}/year={year}/part_{uuid}.parquet`
- rustac writes valid stac-geoparquet with proper geo metadata
- The optional `limit` parameter caps the number of items (useful for testing)

---

## 4. Delta ingestion

Append new items to an existing catalog without overwriting:

```python
ec.ingest(
    "s3://my-bucket/inventory/delta_2026-04-28.parquet",
    mode="delta",
    update_hash_index=True,
)
```

- **`mode="delta"`**: opens the existing Iceberg table and adds files
- New part files are numbered sequentially after existing ones per partition
- The hash index is updated with new item IDs (deduplicates across runs)
- Inventory rows have a `last_modified_date` column for `since=` filtering:
  ```python
  from datetime import UTC, datetime, timedelta
  ec.ingest(inventory, mode="delta", since=datetime.now(UTC) - timedelta(days=2))
  ```

---

## 5. Quick search

Use Iceberg partition pruning to find files intersecting a geometry:

```python
from shapely.geometry import box

greenland = box(-60, 60, -20, 85)
paths = ec.search_files(greenland, start_datetime="2020-01-01")
print(f"{len(paths)} files intersect Greenland")
```

The result is a list of file paths suitable for DuckDB:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute(f"""
    SELECT id, platform, datetime, percent_valid_pixels
    FROM iceberg_scan('{ec.table.metadata_location}')
    WHERE grid_partition IN (
        SELECT DISTINCT grid_partition
        FROM read_parquet({paths!r})
    )
    AND percent_valid_pixels > 50
    LIMIT 20
""").df()
print(df)
```

Or use the convenience method to generate the SQL cell filter:

```python
cell_filter = ec.cell_list_sql(greenland)
# → "grid_partition IN ('8206d7fffffffff', '820677fffffffff', ...)"
```

---

## 6. Inspect the catalog

```python
# Per-partition stats (from Iceberg manifests, no Parquet I/O)
for s in ec.stats()[:3]:
    print(f"{s['grid_partition']}/{s['year']}: {s['row_count']} rows")

# Unique item count (from hash index)
print(f"Unique items: {ec.unique_item_count():,}")
```

Or from the command line:

```bash
python scripts/info.py \
    --catalog   /tmp/catalog.db \
    --warehouse /tmp/warehouse
```

---

## Next steps

- [Architecture](../architecture.md) — how the pipeline works
- [Configuration](../configuration.md) — full YAML reference
- [Operations: Ingest Workflow](../operations/ingest_workflow.md) — production runbook
- [Query Catalog](../operations/query_catalog.md) — DuckDB + rustac + CQL2 examples
