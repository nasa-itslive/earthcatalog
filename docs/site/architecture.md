# Architecture

earthcatalog is a spatially-partitioned STAC ingest pipeline.  This page
explains every layer from S3 inventory to Iceberg catalog.

---

## Pipeline overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Source                                                          │
│  AWS S3 Inventory  (CSV / CSV.gz / Parquet / manifest.json)     │
└──────────────────────────────┬──────────────────────────────────┘
                               │ (bucket, key) pairs
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  Fetch                                                           │
│  ThreadPoolExecutor + obstore.get()                              │
│  • anonymous for public ITS_LIVE bucket                          │
│  • --since filter: skip keys unchanged since last run            │
└──────────────────────────────┬──────────────────────────────────┘
                               │ STAC item dicts
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  fan_out(items, partitioner)                                     │
│  • map each item to every intersecting H3 cell (boundary-incl.) │
│  • sanitise property names  (proj:code → proj_code)             │
│  • round float fields to int32                                   │
│  • inject grid_partition into each synthetic item                │
└──────────────────────────────┬──────────────────────────────────┘
                               │ list[synthetic items]
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  group_by_partition(rows)                                        │
│  • group by (grid_partition, year)                               │
│  • sort within each group by (platform, datetime)                │
│  • each group → exactly one Parquet file                         │
│  ↳ required for Iceberg add_files() single-value constraint      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ dict[(cell, year) → [items]]
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  write_geoparquet(group, path)                                   │
│  • rustac.GeoparquetWriter                                       │
│  • full GeoParquet compliance (geo key, geoarrow.wkb extension) │
│  • hive path: grid_partition=<cell>/year=<year>/part_N.parquet   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ .parquet files on disk / S3
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  table.add_files(paths)                                          │
│  • PyIceberg SQLite catalog  (catalog.db)                        │
│  • one Iceberg snapshot per run                                  │
│  • catalog.db uploaded to S3 with S3Lock (If-None-Match: *)      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technology stack

| Library | Role |
|---|---|
| **obstore** | All S3 and local I/O — fetch STAC items, write/read Parquet, upload `catalog.db`, acquire S3 lock |
| **rustac** | Converts STAC item dicts to GeoParquet with correct `geo` metadata and `geoarrow.wkb` extension type |
| **PyIceberg** | Manages the SQLite-backed file manifest: partition spec, snapshot history, schema enforcement at registration |
| **PyArrow** | In-memory schema definition, type casting (`_align_schema`), Parquet read/write, compaction merges |
| **H3** | Spatial partitioning index — maps item geometries to one or more hexagonal cells at a configurable resolution |

obstore is used exclusively for all storage operations (no boto3, s3fs, or fsspec). rustac is used exclusively for GeoParquet production; PyIceberg's own writer (`table.append()`) is not used — see [Iceberg usage](#iceberg-usage) below.

---

## Spatial partitioning

### Fan-out (Overlap Multiplier)

A single STAC item whose bounding polygon spans multiple H3 cells is written
to **each** of those cells.  This is the *Overlap Multiplier*: one source item
→ N rows (one per intersecting cell).

This design enables **zero-scan spatial queries**: filter by
`grid_partition IN (candidate_cells)` and Iceberg skips every file outside
the query region entirely.

### H3 resolution

| Resolution | Avg. cell area | Global cells | Use              |
|:---:       |:---:           |:---:         |---               |
| 0          | ~4,250,000 km² | 122          | Continental      |
| **1**      | **~607,220 km²**| **842**     | **Production**   |
| 2          | ~86,750 km²    | 5,882        | Sub-regional     |
| 3          | ~12,390 km²    | 41,162       | Dense datasets   |

Resolution 1 gives ~100–200 occupied cells for the ITS_LIVE glacier coverage
(Greenland, Antarctica, Arctic, mountain ranges).

### Boundary-inclusive contract

`H3Partitioner` uses:

1. `h3.geo_to_cells()` — cells whose *centroid* is inside the polygon
2. A densified boundary walk (`_boundary_cells`) — cells touched by the exterior ring, sampled at ~10 km intervals

Together these guarantee that a polygon touching a cell edge is assigned to
that cell, preventing gaps along shared boundaries.

---

## Schema

22-column PyArrow schema defined in `earthcatalog/core/schema.py` — the
**single source of truth** imported by both `transform.py` and `catalog.py`.

| Column | Arrow type | Notes |
|---|---|---|
| `id` | `string` (not null) | STAC item ID |
| `grid_partition` | `string` (not null) | H3 cell index |
| `geometry` | `binary` | WKB geometry (`geoarrow.wkb` extension) |
| `datetime` | `timestamp[us, UTC]` | |
| `start_datetime` | `timestamp[us, UTC]` | |
| `mid_datetime` | `timestamp[us, UTC]` | |
| `end_datetime` | `timestamp[us, UTC]` | |
| `created` | `timestamp[us, UTC]` | |
| `updated` | `timestamp[us, UTC]` | |
| `percent_valid_pixels` | `int32` | Rounded from float |
| `date_dt` | `int32` | Rounded from float |
| `latitude` | `float64` | |
| `longitude` | `float64` | |
| `platform` | `string` | e.g. `sentinel-2`, `landsat-8` |
| `version` | `string` | |
| `proj_code` | `string` | From `proj:code` |
| `sat_orbit_state` | `string` | From `sat:orbit_state` |
| `scene_1_id` | `string` | |
| `scene_2_id` | `string` | |
| `scene_1_frame` | `string` | |
| `scene_2_frame` | `string` | |
| `raw_stac` | `string` | Full STAC item JSON |

---

## Iceberg catalog

earthcatalog uses a **SQLite-backed PyIceberg catalog** (`catalog.db`).

- No Glue, no Hive Metastore, no REST server required.
- `catalog.db` is a single file that can be stored on S3.
- **Partition spec**: `IdentityTransform(grid_partition)` +
  `YearTransform(datetime)` — enables Iceberg file pruning on both spatial
  and temporal predicates with zero additional metadata.
- `table.add_files()` registers each GeoParquet file in exactly one Iceberg
  snapshot.  Files must contain exactly one `grid_partition` value and one
  `year` value (enforced by `group_by_partition()`).

---

## Iceberg usage

### What we use

| Feature | How |
|---|---|
| `SqlCatalog` (SQLite) | `catalog.db` is the catalog — no Glue, no REST server |
| `IdentityTransform(grid_partition)` + `YearTransform(datetime)` | Partition spec enables file pruning on spatial and temporal predicates |
| `table.add_files(paths)` | Register pre-written Parquet files atomically in one snapshot |
| Schema with stable field IDs (1–22) | Type mismatch raises `ValueError` at registration time, catching drift early |
| `table.history()` | Audit trail of snapshots |

### What we don't use and why

| Feature | Reason not used |
|---|---|
| `table.append()` / `table.overwrite()` | Dask workers write directly to S3 (obstore); routing data through the head node would be a bottleneck |
| `table.append()` for GeoParquet | PyIceberg's writer does not produce the `geo` Parquet key or `geoarrow.wkb` extension type; rustac does. PyIceberg GeometryType support is not yet implemented upstream. |
| Schema evolution | `raw_stac` (full STAC JSON string) is the escape hatch — new fields can be recovered from it without a catalog migration |
| Time travel, row deletes, merge-on-read | Not needed; deduplication is handled during compaction |
| REST catalog / Glue / Hive Metastore | SQLite is sufficient for a single-writer workload |

---

## Compaction

Over many incremental runs each `(grid_partition, year)` bucket accumulates many small part files. `maintenance/compact.py` merges them.

```
compact_warehouse(warehouse_path, catalog_path, threshold=2)

1. Scan warehouse for all .parquet files grouped by (cell, year)
2. For each bucket with >= threshold files:
   - Download all part files via obstore
   - pa.concat_tables → deduplicate on id (keep latest by updated DESC)
   - Write one compacted file → obstore.put → S3
   - Delete input part files
3. Drop and recreate the Iceberg table
4. table.add_files(all surviving files) → one clean snapshot
5. upload_catalog
```

Step 3 (drop + recreate) is required because `table.add_files()` can only add
files — it cannot remove stale references. After compaction the old manifest
points to deleted files; dropping the table rebuilds it from what physically
exists on disk.

Run compaction periodically after many incremental runs, or after a backfill
that produced a large number of small part files.

---

## Atomic catalog updates (S3 lock)

Because `catalog.db` is a shared file on S3, concurrent writers (multiple
Dask workers, parallel incremental runs) could corrupt it.  earthcatalog
prevents this with an **S3 conditional write lock**:

```
PUT catalog/.lock
    If-None-Match: *        ← only succeeds if the key does not exist
```

- If the PUT succeeds → you hold the lock.
- If S3 returns 412 Precondition Failed → another writer holds it; retry.
- On release → DELETE `catalog/.lock`.

This uses S3 Conditional Writes (GA as of 2024-11-05) and requires no
DynamoDB table or external coordination service.

---

## Warehouse layout

```
warehouse/
├── grid_partition=820957fffffffff/
│   ├── year=2021/
│   │   └── part_000000_<uuid>.parquet
│   └── year=2022/
│       └── part_000000_<uuid>.parquet
├── grid_partition=820977fffffffff/
│   └── year=2021/
│       └── part_000000_<uuid>.parquet
└── ...
```

This hive-style layout is:

- Readable by any Parquet-aware tool (DuckDB, Spark, pandas, pyarrow).
- Required by PyIceberg's `IdentityTransform` + `YearTransform` partition spec.
- Compatible with the standalone compaction tool.

---

## Query example (DuckDB + H3 spatial pruning)

```python
import duckdb, h3
from shapely.geometry import box, mapping

# 1. Define query region and convert to H3 cells
query_bbox  = box(-60, 60, -30, 80)   # Greenland-ish
candidate_cells = h3.geo_to_cells(mapping(query_bbox), resolution=1)
cell_list = ", ".join(f"'{c}'" for c in candidate_cells)

# 2. Scan only the relevant Iceberg files
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute(f"""
    SELECT id, platform, datetime, grid_partition
    FROM iceberg_scan('{metadata_location}')
    WHERE grid_partition IN ({cell_list})
      AND datetime >= '2022-01-01'
    ORDER BY datetime
""").df()
```

Iceberg skips every file whose `grid_partition` is not in `candidate_cells` —
the query reads only the physically relevant files.
