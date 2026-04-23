# Plan: Smoke-test against real S3 data, then implement Dask backfill

**Status**: Phase 1 not started  
**Last updated**: 2026-04-23

---

## Overview

Two sequential phases:

1. **Phase 1 — Smoke-test the library** against the real ITS_LIVE STAC items in
   `s3://its-live-data/test-space/velocity_image_pair/nisar/v02`.  Run the
   single-node pipeline with a small item limit, inspect every layer of the
   output (files on disk, Iceberg catalog, DuckDB queries), and fix any issues
   found before scaling up.

2. **Phase 2 — Dask backfill** — implement the three-level distributed pipeline
   described in this document only after Phase 1 is clean.

---

## Phase 1 — Smoke-test the single-node pipeline

### Goal

Verify that the full stack — `make_test_inventory` → `run()` →
GeoParquet files → Iceberg catalog → DuckDB `iceberg_scan()` — works correctly
end-to-end on real NISAR STAC data before scaling up.

### Step 1.1 — Generate a small inventory

```bash
python -m earthcatalog.tools.make_test_inventory \
    --prefix test-space/velocity_image_pair/nisar/v02 \
    --limit  200 \
    --output /tmp/test_inventory_200.csv
```

Inspect the first few rows:

```bash
head -5 /tmp/test_inventory_200.csv
```

Expected: two-column CSV with `bucket=its-live-data` and keys ending in
`.stac.json`.

**Pass criterion**: 200 `.stac.json` keys written; no error.

---

### Step 1.2 — Fetch one item manually and inspect its schema

Before running the full pipeline, verify that the raw STAC JSON from the real
bucket matches the schema we have in `schema.py`.  Run in a Python shell:

```python
import json
import obstore
from obstore.store import S3Store

store = S3Store(bucket="its-live-data", region="us-west-2",
                skip_signature=True)
raw   = obstore.get(store,
    "test-space/velocity_image_pair/nisar/v02/<pick_a_key>.stac.json"
).bytes()
item  = json.loads(bytes(raw))
print(json.dumps(item["properties"], indent=2))
```

**Check**:
- `datetime` field present and ISO-8601 formatted.
- `percent_valid_pixels` and `date_dt` are present (may be float or int).
- `proj:code` and `sat:orbit_state` present — colon sanitisation needed.
- Note any unexpected property names that are not in `schema.py`; add them if
  they are universally present, otherwise they land in `raw_stac`.

**Pass criterion**: no crash; `datetime`, `platform`, and at least one of
`percent_valid_pixels` / `date_dt` present.

---

### Step 1.3 — Run the single-node pipeline with a small limit

```bash
mamba run -n itslive-ingest \
    python -m earthcatalog.pipelines.incremental \
        --inventory   /tmp/test_inventory_200.csv \
        --catalog     /tmp/smoke_catalog.db \
        --warehouse   /tmp/smoke_warehouse \
        --h3-resolution 1 \
        --chunk-size  50 \
        --limit       200 \
        --batch-add-files
```

Watch stdout for per-chunk progress lines.

**Pass criteria**:
- No Python exceptions.
- Final line shows `200 items → N rows` (N ≥ 200, likely 200–5 000 after
  H3 fan-out at res=1).
- `Snapshots in catalog: 1` (because `--batch-add-files` is set).

---

### Step 1.4 — Inspect the warehouse layout

```bash
find /tmp/smoke_warehouse -name "*.parquet" | head -20
```

Expected hive layout:

```
/tmp/smoke_warehouse/
  grid_partition=81003ffffffffff/
    year=2021/
      part_000000_3a7f1b2c.parquet
    year=2022/
      part_000000_9d4e2a1f.parquet
  grid_partition=81013ffffffffff/
    ...
```

**Pass criteria**:
- Files exist inside hive subdirectories (not in the root).
- H3 cell strings look valid (`8` prefix + 15 hex chars for res=1).
- File sizes are non-zero.

---

### Step 1.5 — Inspect a GeoParquet file

```python
import pyarrow.parquet as pq
from pathlib import Path

f = next(Path("/tmp/smoke_warehouse").glob("**/*.parquet"))
pf = pq.ParquetFile(f)

# Check geo metadata
assert b"geo" in pf.metadata.metadata, "missing geo key"
print("geo metadata present ✓")

# Check schema
tbl = pf.read()
print(tbl.schema)
print(tbl.to_pandas().head(3))

# Check int rounding
for col in ("percent_valid_pixels", "date_dt"):
    if col in tbl.schema.names:
        vals = [v for v in tbl.column(col).to_pylist() if v is not None]
        assert all(isinstance(v, int) for v in vals), f"{col} not int"
        print(f"{col}: int ✓")
```

**Pass criteria**:
- `geo` key present.
- `grid_partition` column non-null, all values are the same H3 cell string
  (single-partition contract).
- `percent_valid_pixels` and `date_dt` are `int32` not `float`.
- `geometry` column non-null and non-empty.
- `raw_stac` column is a valid JSON string.

---

### Step 1.6 — Verify the Iceberg catalog

```python
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(
    db_path="/tmp/smoke_catalog.db",
    warehouse_path="/tmp/smoke_warehouse",
)
table = get_or_create_table(catalog)

print("Snapshots :", len(table.history()))     # should be 1
print("Files     :", len(table.scan().plan_files()))
result = table.scan().to_arrow()
print("Rows      :", result.num_rows)
print("Unique IDs:", result.column("id").n_unique)
```

**Pass criteria**:
- Exactly 1 snapshot.
- `num_rows` matches the pipeline output.
- `id` count equals the number of unique source items × H3 fan-out (some items
  may span more than one cell).

---

### Step 1.7 — DuckDB end-to-end query

```python
import duckdb, h3

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

# Full scan
count = con.execute(
    f"SELECT count(*) FROM iceberg_scan('{table.metadata_location}')"
).fetchone()[0]
print("DuckDB row count:", count)

# Spatial filter — convert a bounding box to H3 cells at res=1
test_polygon = {
    "type": "Polygon",
    "coordinates": [[[-10, 55], [10, 55], [10, 75], [-10, 75], [-10, 55]]],
}
cells = list(h3.geo_to_cells(test_polygon, res=1))
cell_list = ", ".join(f"'{c}'" for c in cells)

rows = con.execute(f"""
    SELECT id, platform, grid_partition, datetime
    FROM iceberg_scan('{table.metadata_location}')
    WHERE grid_partition IN ({cell_list})
    LIMIT 10
""").fetchall()
print(f"Spatial filter returned {len(rows)} rows")
print(rows)
```

**Pass criteria**:
- `count` matches the PyIceberg scan row count.
- Spatial filter returns only rows whose `grid_partition` is in `cells`.
- No DuckDB errors.

---

### Step 1.8 — Scale up to 2 000 items

Repeat steps 1.3–1.7 with `--limit 2000`.  This exercises multiple chunks
(chunk_size=50 → 40 flush calls) with `batch_add_files` keeping them as a
single snapshot, and gives a meaningful sample of the real data distribution
across H3 cells and years.

```bash
mamba run -n itslive-ingest \
    python -m earthcatalog.pipelines.incremental \
        --inventory   /tmp/test_inventory_200.csv \
        --catalog     /tmp/smoke_catalog_2k.db \
        --warehouse   /tmp/smoke_warehouse_2k \
        --h3-resolution 1 \
        --chunk-size  200 \
        --batch-add-files
```

**Additional check**: count files per `(grid_partition, year)` bucket:

```python
from collections import Counter
from pathlib import Path

counts = Counter()
for f in Path("/tmp/smoke_warehouse_2k").glob("**/*.parquet"):
    parts = f.parts
    cell = [p for p in parts if p.startswith("grid_partition=")][0]
    year = [p for p in parts if p.startswith("year=")][0]
    counts[(cell, year)] += 1

print("Buckets with >1 file (candidates for compaction):",
      sum(1 for v in counts.values() if v > 1))
print("Max files in one bucket:", max(counts.values()))
```

This tells us how much fragmentation exists before compaction — input to
sizing the Level 1 compact step.

---

### Phase 1 exit criteria (all must pass before starting Phase 2)

| Check | Command / location |
|---|---|
| Real STAC schema matches `schema.py` | Step 1.2 |
| Pipeline runs without exceptions on 200 items | Step 1.3 |
| Hive directory layout correct | Step 1.4 |
| `geo` metadata + int columns in output files | Step 1.5 |
| Single Iceberg snapshot, correct row count | Step 1.6 |
| DuckDB `iceberg_scan` + spatial filter work | Step 1.7 |
| Runs clean at 2 000 items | Step 1.8 |

If any step fails, fix the issue and re-run from that step before proceeding.
Document findings (unexpected fields, schema mismatches, performance notes) in
a new `docs/progress_smoke_test.md` file.

---

## Phase 2 — Dask distributed backfill

Implement only after all Phase 1 exit criteria are met.

### Architecture (three levels)

```
S3 Inventory (Parquet, row-group streamed by head node)
        │  chunks of ~20k (bucket, key) pairs
        ▼
┌──────────────────────────────────────────────────────┐
│  Level 0: dask.delayed(ingest_chunk) × N workers    │
│                                                      │
│  Each worker (no shared state):                      │
│    _fetch_items_parallel()  — obstore, threaded      │
│    fan_out()                — pure function          │
│    group_by_partition()     — pure function          │
│    write_geoparquet() per (cell, year)               │
│        → write to local temp → obstore.put() to S3  │
│    return list[FileMetadata]  (~200 B per file)      │
└──────────────────────┬───────────────────────────────┘
                       │  collect metadata dicts (head node)
                       │  no Parquet data crosses the network
                       ▼
           group by (grid_partition, year)
           ≤ 842 × N_years groups at H3 res=1
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  Level 1: dask.delayed(compact_group) × ≤ 4 210     │
│                                                      │
│  Each worker (one per (cell, year) bucket):          │
│    obstore.get() all part files for this bucket      │
│    pa.concat_tables()                                │
│    sort_by (platform, datetime)                      │
│    write_geoparquet() → obstore.put() to S3          │
│    delete original part files (obstore.delete)       │
│    return final_path                                 │
└──────────────────────┬───────────────────────────────┘
                       │  collect final_paths (head node)
                       ▼
           table.add_files(final_paths)  — 1 snapshot
           upload_catalog()
```

**Key invariant**: Parquet data never travels to or through the head node.
Workers own all I/O.  The head node only handles:
- Inventory streaming and chunk dispatch (~200 B/item)
- Metadata collection from Level 0 (~200 B/file)
- Metadata grouping (in-memory dict)
- `table.add_files()` + `upload_catalog()` (catalog operations only)

---

### Step 2.1 — S3 write support for `write_geoparquet`

Currently `write_geoparquet(items, path)` writes to a local filesystem path.
Workers on a Dask cluster need to write directly to S3.

**Design**: `write_geoparquet` stays local-first (rustac requires a local path).
Add a helper `write_geoparquet_s3` that:

1. Writes to a local temp file (same as now).
2. Calls `obstore.put(store, s3_key, data)` to upload.
3. Deletes the local temp file.

```python
# earthcatalog/core/transform.py — new public function
def write_geoparquet_s3(
    fan_out_items: list[dict],
    store,           # obstore Store (S3Store or MemoryStore for tests)
    s3_key: str,     # key within the store, e.g. "warehouse/grid_partition=.../part.parquet"
) -> tuple[int, int]:
    """Write GeoParquet to S3 via obstore.  Returns (row_count, byte_count)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        n = write_geoparquet(fan_out_items, tmp_path)
        data = Path(tmp_path).read_bytes()
        obstore.put(store, s3_key, data)
        return n, len(data)
    finally:
        Path(tmp_path).unlink(missing_ok=True)
```

The store is injected (via `store_config.get_store()` or passed explicitly),
keeping the function testable with `MemoryStore`.

**Test**: add to `test_transform.py` — write via `MemoryStore`, read back,
verify `geo` metadata and row count.

---

### Step 2.2 — `FileMetadata` dataclass

Define a small dataclass (or TypedDict) for the metadata each Level 0 worker
returns.  Lives in `earthcatalog/core/transform.py` or a new
`earthcatalog/core/metadata.py`.

```python
from dataclasses import dataclass

@dataclass
class FileMetadata:
    path:            str    # full s3:// URI
    grid_partition:  str    # H3 cell string
    year:            int | None
    row_count:       int
    file_size_bytes: int
```

Keeping this a plain dataclass (no PyArrow, no Iceberg imports) ensures it
is trivially serialisable by Dask's task graph and safe to pass between
workers and the scheduler.

---

### Step 2.3 — `ingest_chunk` Dask task

New file: `earthcatalog/pipelines/backfill.py`.

```python
@dask.delayed
def ingest_chunk(
    bucket_key_pairs: list[tuple[str, str]],
    partitioner,
    warehouse_s3_prefix: str,
    chunk_id: int,
    max_workers: int = 16,
) -> list[FileMetadata]:
    """
    Level 0 worker: fetch → fan_out → group_by_partition → write to S3.
    Returns a list of FileMetadata — no Parquet data leaves this function.
    """
    ...
```

**Note**: `partitioner` must be serialisable by Dask (cloudpickle).  Both
`H3Partitioner` and `GeoJSONPartitioner` are pure Python — this should work
without modification.

**Test**: call `ingest_chunk(...).compute()` with a `MemoryStore` and a
mocked fetch; assert returned metadata matches the written files.

---

### Step 2.4 — `compact_group` Dask task

```python
@dask.delayed
def compact_group(
    file_metas: list[FileMetadata],
    out_key: str,
    store,
) -> FileMetadata:
    """
    Level 1 worker: read all part files for one (cell, year) bucket,
    merge, sort by (platform, datetime), write one consolidated file.
    Deletes the input part files after a successful write.
    """
    ...
```

**Compaction is optional per group**: if a bucket already has only one file
(common for cells with sparse coverage), skip it and pass through the existing
`FileMetadata` unchanged.

**Test**: create 3 small part files for one `(cell, year)`, call
`compact_group(...).compute()`, assert output has merged row count and single
file.

---

### Step 2.5 — `run_backfill` orchestrator

```python
def run_backfill(
    inventory_path: str,
    catalog_path: str,
    warehouse_s3_prefix: str,
    partitioner,
    chunk_size: int = 20_000,
    max_workers_per_task: int = 16,
    compact: bool = True,
    compact_threshold: int = 2,   # compact buckets with >= this many part files
    use_lock: bool = True,
) -> None:
    """
    Three-level Dask backfill:
      Level 0: ingest_chunk tasks (one per chunk)
      Level 1: compact_group tasks (one per (cell, year) bucket, if compact=True)
      Level 2: table.add_files() + upload_catalog() on head node
    """
```

**Inventory streaming**: the head node streams the inventory and dispatches
chunks without loading the full inventory into RAM — same
`_iter_inventory()` used by the single-node pipeline.

---

### Step 2.6 — Integration test with small real data

Once steps 2.1–2.5 are implemented and unit-tested:

```bash
mamba run -n itslive-ingest python - <<'EOF'
import dask
dask.config.set(scheduler="synchronous")   # single-thread for debugging

from earthcatalog.pipelines.backfill import run_backfill
from earthcatalog.grids.h3_partitioner import H3Partitioner

run_backfill(
    inventory_path="s3://its-live-data/test-space/...",
    catalog_path="/tmp/backfill_test.db",
    warehouse_s3_prefix="s3://its-live-data/test-space/catalog/warehouse",
    partitioner=H3Partitioner(resolution=1),
    chunk_size=200,
    compact=True,
    use_lock=False,
)
EOF
```

Use `dask.config.set(scheduler="synchronous")` first so stack traces are
readable.  Switch to `LocalCluster` once it is clean.

---

### Step 2.7 — Dask `LocalCluster` smoke test

```python
from dask.distributed import Client, LocalCluster

cluster = LocalCluster(n_workers=4, threads_per_worker=1)
client  = Client(cluster)

run_backfill(
    ...,
    chunk_size=1000,
)
```

Verify:
- No worker OOM (each worker holds at most one chunk of fan-out rows at a time).
- No SQLite contention (catalog ops only on head node).
- Final snapshot count = 1.

---

### Open questions to resolve during Phase 2

| Question | Notes |
|---|---|
| **Worker S3 credentials** | `its-live-data` is public (`skip_signature=True`). For writing to a production warehouse bucket, workers need IAM role or environment credentials. Injected via `store_config.set_store()` before `client.submit()`. |
| **Compact threshold** | How many part files per `(cell, year)` before compaction is worth it? Measure from Phase 1.8 output. |
| **Level 1 read cost** | Each compact worker downloads all part files for its bucket. At 20k items/chunk and 842 cells, a popular cell may have hundreds of part files. Measure total bytes read in the compact step. |
| **Temp file cleanup** | Workers must delete local temp files after S3 upload. Ensure `finally` blocks are in place; add a cleanup task if workers can crash mid-write. |
| **REST catalog** | SQLite `add_files()` is single-writer and fast enough for the Level 2 call (≤ 4 210 files). Switch to a REST catalog only if Level 2 becomes a bottleneck. |
| **Dask task graph size** | At 100M items / 20k per chunk = 5 000 Level 0 tasks + ≤ 4 210 Level 1 tasks = ~9 210 total tasks. Well within Dask's scheduler limits. |

---

## File layout after Phase 2

```
earthcatalog/
  core/
    transform.py          # + write_geoparquet_s3(), FileMetadata
  pipelines/
    incremental.py        # unchanged
    backfill.py           # NEW — ingest_chunk, compact_group, run_backfill
tests/
  test_transform.py       # + write_geoparquet_s3 tests
  test_backfill.py        # NEW — unit + integration tests for backfill pipeline
docs/
  plan_smoke_test_and_dask.md   # this document
  progress_smoke_test.md        # to be written during Phase 1
```
