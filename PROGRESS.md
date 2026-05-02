# Progress

## All Phases Complete (279 tests pass)

| Phase | What | Lines ∆ | Status |
|-------|------|---------|--------|
| **A** | store_config globals eliminated | +45 | ✅ |
| **B** | `catalog.ingest()` unified API | +289 | ✅ |
| **C** | Uniform Obstore I/O (no local/S3 pairs) | -37 | ✅ |
| **D** | Deduplication (_h3_boundary_cells, hash index) | -34 | ✅ |
| **E** | Archive one-off scripts | - | ✅ |
| **F** | Test simplification (27 tests removed) | -918 | ✅ |
| **G** | Documentation (quickstart, ingest workflow) | +111 | ✅ |
| | **`bulk_ingest()`** — Dask/Coiled wrapper | +162 | ✅ |

## Top-Level API

```python
from earthcatalog.core import catalog
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2")
ec = catalog.open(store=store, base="s3://my-bucket/catalog")

# Single-node daily delta
ec.ingest("delta.parquet", mode="delta", update_hash_index=True)

# Large backfill on Coiled/Dask
ec.bulk_ingest("full_inventory.parquet", mode="full",
               create_client=lambda: coiled.Client(n_workers=100))

# Spatial search (Iceberg partition-pruned)
paths = ec.search_files(bbox, start_datetime="2020-01-01")
```
