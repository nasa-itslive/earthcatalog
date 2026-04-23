# EarthCatalog Ingest Guide

Operational reference for running the three pipeline stages:
initial bulk backfill, daily incremental update, and compaction/cleanup.

**Status (2026-04-23)**: End-to-end S3 warehouse run verified — 20 000 items
ingested, 6 compacted GeoParquet files written to S3, `catalog.db` uploaded,
spatial queries with H3 cell pruning confirmed working.
Spot-resilient per-file mini-run architecture implemented and tested.

---

## Prerequisites

```bash
# AWS credentials — export before any command in this guide
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-west-2

# Paths used throughout this guide — adjust to your environment
MANIFEST="s3://pds-buckets-its-live-logbucket-70tr3aw5f2op/inventory/\
velocity_image_pair/its-live-data/VelocityGranuleInventory/2026-04-22T01-00Z/manifest.json"
CATALOG="$HOME/earthcatalog/catalog.db"
WAREHOUSE="$HOME/earthcatalog/warehouse"
```

---

## 1. Initial Bulk Backfill

Ingest the entire ITS_LIVE NISAR catalog from a fresh S3 Inventory manifest.
This is a one-time operation.

### Smoke test (local, 500 items, single process)

Validate the pipeline end-to-end before committing to a full run.

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory  "$MANIFEST" \
    --catalog    /tmp/catalog-smoke.db \
    --warehouse  /tmp/warehouse-smoke \
    --scheduler  synchronous \
    --limit      500 \
    --no-lock
```

Expect output like:
```
Manifest: 96 data file(s) in pds-buckets-its-live-logbucket-70tr3aw5f2op
Reading inventory …
  chunk 1: 500 items → 762 rows in 24 partition files
Done. 500 items → 762 rows
```

### Full backfill to S3 warehouse (recommended — verified 2026-04-23)

Write Parquet files and `catalog.db` directly to S3.  Workers read credentials
from env vars; `--no-lock` skips the S3 conditional-write lock (safe for a
single-operator run; remove it for multi-operator production use).

```bash
S3_CATALOG="s3://its-live-data/test-space/stac/catalog/catalog.db"
S3_WAREHOUSE="s3://its-live-data/test-space/stac/catalog/data"

python -m earthcatalog.pipelines.backfill \
    --inventory         "$MANIFEST" \
    --catalog           "$S3_CATALOG" \
    --warehouse         "$S3_WAREHOUSE" \
    --scheduler         local \
    --workers           4 \
    --threads-per-worker 1 \
    --chunk-size        2000 \
    --h3-resolution     1 \
    --no-lock
```

Expected output (20 000 items):

```
Manifest: 126 data file(s) in pds-buckets-its-live-logbucket-70tr3aw5f2op
Level 0: 10 ingest_chunk tasks (20000 items)
Level 0 done: 60 files, 56113 rows
Level 1: 6 compact tasks, 0 files passed through
Level 2: registering 6 files in one snapshot
Catalog uploaded → s3://its-live-data/test-space/stac/catalog/catalog.db
Ingest stats: s3://its-live-data/test-space/stac/catalog/ingest_stats.json
Backfill done. 20000 items → 56113 rows across 6 files
Snapshots in catalog: 1
```

Output layout in S3:

```
s3://its-live-data/test-space/stac/catalog/
├── catalog.db                         # Iceberg SQLite catalog
├── ingest_stats.json                  # run history (one record per run, appended)
└── data/
    ├── earthcatalog/stac_items/metadata/  # Iceberg metadata
    └── grid_partition=<cell>/year=<year>/compacted_*.parquet
```

### Ingest stats report

`ingest_stats.json` is a JSON array — one record appended **per manifest data
file** (in per-file mini-run mode) or per full run (in monolithic mode).
It always lands next to `catalog.db` by default. Override with
`--report-location`:

```bash
# Write stats to a different S3 prefix
python -m earthcatalog.pipelines.backfill ... \
    --report-location "s3://my-ops-bucket/earthcatalog/reports/ingest_stats.json"

# Write stats to a local file regardless of catalog location
python -m earthcatalog.pipelines.backfill ... \
    --report-location "/var/log/earthcatalog/ingest_stats.json"
```

Example record (per-file mini-run mode):

```json
{
  "run_id":         "2026-04-23T20:01:04+00:00",
  "inventory":      "s3://.../manifest.json",
  "inventory_file": "inventory/data/abc123.parquet",
  "since":          null,
  "h3_resolution":  1,
  "source_items":   450,
  "total_rows":     1260,
  "unique_cells":   3,
  "unique_years":   2,
  "files_written":  6,
  "avg_fan_out":    2.8,
  "overhead_pct":   180.0,
  "fan_out_distribution": {"2": 150, "3": 300}
}
```

The `inventory_file` field is the S3 key of the manifest data file that was
processed in this mini-run.  It is `null` for monolithic (non-manifest) runs.

---

### Spot resilience: per-file mini-runs (default)

When the inventory is a `manifest.json`, each of the 126 manifest data files
is processed as an independent mini-run. The pipeline:

1. Reads `ingest_stats.json` to find already-processed data files.
2. For each remaining data file: acquires the lock → downloads the catalog →
   ingests → compacts → `add_files` + `upload_catalog` → appends stats record →
   releases the lock.

**If a spot instance is interrupted**, the pipeline resumes from the last
successfully written stats record on the next run — no re-processing of files
that already committed their `add_files` call.

**Failure matrix**:

| Interrupted at | Catalog updated? | Stats record written? | On restart |
|---|---|---|---|
| Before `add_files` | No | No | Re-process file (dedup by `id` is safe) |
| `add_files` OK but `upload_catalog` failed | Local only | No | Re-process file (dedup safe) |
| Catalog uploaded but stats not yet written | Yes | No | Re-process file (dedup safe) |
| Stats record written | Yes | Yes | File skipped ✓ |

Worst case: one data file (~450 items) re-processed. Dedup during compaction
guarantees no duplicate rows in the final table.

To opt out and use the original monolithic behaviour (one lock, one `add_files`
at the end):

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory         "$MANIFEST" \
    ... \
    --no-per-file-mini-runs
```

### Full backfill (local filesystem)

Suitable for a workstation or a large EC2 instance.
Expect several hours for the full ~10M-item catalog.

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory         "$MANIFEST" \
    --catalog           "$CATALOG" \
    --warehouse         "$WAREHOUSE" \
    --scheduler         local \
    --workers           4 \
    --threads-per-worker 1 \
    --chunk-size        500 \
    --h3-resolution     1
```

- `--h3-resolution 1` gives 842 global H3 cells — the production default.
- Omit `--no-lock`; the S3 lock protects the catalog.db upload.
- Compaction runs automatically after each chunk (default threshold: 2 part
  files per `(cell, year)` bucket). Use `--no-compact` to skip it and run
  compaction separately afterward.

### Full backfill (Coiled cloud cluster)

For fastest turnaround. Requires a Coiled account (`coiled login`).

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory         "$MANIFEST" \
    --catalog           "$CATALOG" \
    --warehouse         "$WAREHOUSE" \
    --scheduler         coiled \
    --coiled-n-workers  20 \
    --coiled-vm-type    m6i.xlarge \
    --coiled-software   itslive-ingest \
    --coiled-region     us-west-2 \
    --chunk-size        500
```

> **Note on `since` and the inventory manifest**: The manifest contains all
> 96 Parquet data files sorted by S3 key, not by modification date. New
> granules for historical acquisition years are scattered across all files.
> For the initial backfill do **not** pass `--since` — process everything.

---

## 2. Daily Incremental Update

Run after each new S3 Inventory is generated (AWS publishes daily).
Only processes objects modified on or after the cutoff date.

### Get the latest manifest

Find the most recent inventory timestamp prefix:

```bash
aws s3 ls s3://pds-buckets-its-live-logbucket-70tr3aw5f2op/inventory/\
velocity_image_pair/its-live-data/VelocityGranuleInventory/ \
    --profile default | tail -5
```

Set `MANIFEST` to the latest timestamp directory, e.g.:

```bash
MANIFEST="s3://pds-buckets-its-live-logbucket-70tr3aw5f2op/inventory/\
velocity_image_pair/its-live-data/VelocityGranuleInventory/2026-04-23T01-00Z/manifest.json"
```

### Run incremental ingest

```bash
SINCE=$(date -u -d "2 days ago" +%Y-%m-%d)   # Linux
# SINCE=$(date -u -v-2d +%Y-%m-%d)           # macOS

python -m earthcatalog.pipelines.backfill \
    --inventory         "$MANIFEST" \
    --catalog           "$CATALOG" \
    --warehouse         "$WAREHOUSE" \
    --scheduler         local \
    --workers           4 \
    --since             "$SINCE" \
    --chunk-size        500
```

**Why `--since` does not reduce network I/O**: All 96 inventory Parquet files
are still downloaded in full because AWS does not record per-file date ranges
in the manifest and new granules are scattered across all files by key order.
The `since` filter skips rows in Python after reading — it reduces ingest work
(STAC fetches, fan-out, writes) but not manifest download volume.

**Alternative — single-node incremental** (simpler, no Dask):

```bash
python -m earthcatalog.pipelines.incremental \
    --inventory     "$MANIFEST" \
    --catalog       "$CATALOG" \
    --warehouse     "$WAREHOUSE" \
    --since         "$SINCE" \
    --workers       16 \
    --batch-add-files
```

Use `--batch-add-files` to register all new Parquet files in a single Iceberg
snapshot instead of one snapshot per chunk. Recommended for incremental runs
to prevent snapshot accumulation.

---

## 3. Compaction and Cleanup

Each ingest chunk writes several small part files per `(cell, year)` bucket.
Compaction merges them into one file per bucket, deduplicates by `id`, and
deletes orphaned part files.

### Automatic compaction (default, runs inside backfill)

The backfill pipeline compacts each `(cell, year)` bucket after it accumulates
`--compact-threshold` (default: 2) part files. This is the recommended mode —
no separate step needed.

To tune aggressiveness:

```bash
python -m earthcatalog.pipelines.backfill \
    --inventory          "$MANIFEST" \
    ... \
    --compact-threshold  1    # compact after every chunk (most aggressive)
    # --no-compact             # disable entirely; run manually afterward
```

### Manual compaction (standalone)

If you ran with `--no-compact` or want to re-compact after multiple incremental
runs have accumulated part files, use the standalone compaction tool:

```bash
# Dry run — see what would be compacted without touching any files
python -m earthcatalog.maintenance.compact \
    --warehouse "$WAREHOUSE" \
    --catalog   "$CATALOG" \
    --dry-run

# Compact all buckets with ≥ 2 part files (default) and rebuild catalog
python -m earthcatalog.maintenance.compact \
    --warehouse "$WAREHOUSE" \
    --catalog   "$CATALOG" \
    --use-lock

# S3 warehouse
python -m earthcatalog.maintenance.compact \
    --warehouse "$S3_WAREHOUSE" \
    --catalog   "$S3_CATALOG" \
    --use-lock
```

Or via the Python API:

```python
from earthcatalog.maintenance.compact import compact_warehouse

summary = compact_warehouse(
    warehouse_path="$HOME/earthcatalog/warehouse",
    catalog_path="$HOME/earthcatalog/catalog.db",
    threshold=2,
    use_lock=True,
)
print(summary)
# {"buckets_scanned": 150, "buckets_compacted": 45,
#  "files_before": 195, "files_after": 150}
```

### What compaction does

For each `(grid_partition=<cell>, year=<year>)` bucket:

1. Lists all `.parquet` files under that prefix via `obstore.list()`.
2. Reads and concatenates all part files into a single Arrow table.
3. Deduplicates rows by `id`, keeping the most-recent `updated` value.
4. Sorts rows by `(platform, datetime)`.
5. Writes one merged file: `part_NNNNNN_<uuid>.parquet`.
6. Deletes all old part files (orphan sweep).
7. Registers only the merged file in the Iceberg catalog via `table.add_files()`.

H3 fan-out rows with the same `id` but different `grid_partition` (a point
near a cell boundary that maps to multiple cells) are preserved — dedup is
within-partition only.

---

## 4. Verifying the Catalog

### Option A — Query a local catalog

```python
import duckdb
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/wh")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
df = con.execute(f"""
    SELECT grid_partition, YEAR(datetime) AS yr, COUNT(*) AS n
    FROM iceberg_scan('{table.metadata_location}')
    GROUP BY grid_partition, yr ORDER BY n DESC
""").df()
print(df)
```

### Option B — Query the S3 catalog (with H3 cell pruning)

Download `catalog.db` from S3, open the Iceberg table, then run a
cell-pruned spatial + temporal query. DuckDB skips files for cells
not in the `IN` list — file-level pruning confirmed working (2026-04-23).

```python
import os, tempfile, duckdb, h3, obstore
from obstore.store import S3Store
from earthcatalog.core.catalog import open_catalog, get_or_create_table

region  = os.environ["AWS_DEFAULT_REGION"]
key_id  = os.environ["AWS_ACCESS_KEY_ID"]
secret  = os.environ["AWS_SECRET_ACCESS_KEY"]

# 1. Download catalog.db from S3
store = S3Store(bucket="its-live-data", region=region,
                aws_access_key_id=key_id, aws_secret_access_key=secret)
tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp.write(bytes(obstore.get(store, "test-space/stac/catalog/catalog.db").bytes()))
tmp.close()

# 2. Open the Iceberg table
catalog = open_catalog(tmp.name, "s3://its-live-data/test-space/stac/catalog/data")
table   = get_or_create_table(catalog)

# 3. Configure DuckDB with S3 credentials
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute(f"""
    CREATE SECRET s3_creds (
        TYPE S3, KEY_ID '{key_id}', SECRET '{secret}', REGION '{region}'
    )
""")

# 4. H3 cell-pruned spatial query
query_geojson = {
    "type": "Polygon",
    "coordinates": [[[-55, 60], [-10, 60], [-10, 85], [-55, 85], [-55, 60]]]
}
query_cells = list(h3.geo_to_cells(query_geojson, res=1))
cell_list   = ", ".join(f"'{c}'" for c in query_cells)

df = con.execute(f"""
    SELECT id, platform, datetime, percent_valid_pixels
    FROM iceberg_scan('{table.metadata_location}')
    WHERE grid_partition IN ({cell_list})   -- Iceberg skips non-matching files
      AND datetime BETWEEN '2020-01-01' AND '2020-12-31'
    ORDER BY datetime
    LIMIT 20
""").df()
print(df)
```

**Observed performance (20k-item test data, S3, res=1)**:

| Query | Files opened | Rows returned | Latency |
|---|---|---|---|
| Full scan (no filter) | 6/6 | 56 113 | ~2 s |
| Greenland cells only | 2/6 | 18 178 | ~1 s |
| Greenland + year=2020 | 2/6 | 1 810 | ~1 s |

> Partition pruning relies on Iceberg's `IdentityTransform(grid_partition)`
> manifest. DuckDB reads the manifest, identifies which files belong to each
> cell, and opens only those files — no full scan required.
