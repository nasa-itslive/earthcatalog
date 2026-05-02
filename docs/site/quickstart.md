# Quick Start

```python
import earthcatalog as ea
from obstore.store import S3Store
from shapely.geometry import box
from datetime import UTC, datetime, timedelta

# Open — any obstore store works (S3Store, LocalStore, MemoryStore)
store = S3Store(bucket="its-live-data", region="us-west-2")
ec = ea.open(store=store, base="s3://bucket/catalog")
print(ec)  # EarthCatalog(grid_type='h3', resolution=1)

# Ingest
# --- daily delta (single-node, ThreadPoolExecutor)
ec.ingest("s3://bucket/delta.parquet", mode="delta", update_hash_index=True)
ec.ingest("delta.parquet", mode="delta", since=datetime.now(UTC) - timedelta(days=2))

# --- large backfill (Dask/Coiled, 4-phase spot-resilient)
ec.bulk_ingest("s3://bucket/full.parquet", mode="full",
               create_client=lambda: coiled.Client(n_workers=100))

# Search — Iceberg partition pruning, zero I/O on irrelevant files
greenland = box(-60, 60, -20, 85)
paths = ec.search_files(greenland, start_datetime="2020-01-01")

import duckdb
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
df = con.execute(f"""
    SELECT id, platform, datetime
    FROM read_parquet({paths})
    WHERE ST_Intersects(geometry, ST_GeomFromText('{greenland.wkt}'))
    LIMIT 10
""").df()

# Stats — from Iceberg manifests, no Parquet data read
ec.stats()
ec.unique_item_count()
```
