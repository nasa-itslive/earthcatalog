# earthcatalog

Ingest STAC catalogs from AWS S3 into a spatially-partitioned
[Apache Iceberg](https://iceberg.apache.org/) table, queryable with
[DuckDB](https://duckdb.org/) or any other Iceberg-compatible engine.

---

## What it does

1. **Reads** an AWS S3 Inventory file (CSV, CSV.gz, or Parquet) to find `.stac.json` keys.
2. **Fetches** each STAC item directly from S3 using
   [obstore](https://github.com/developmentseed/obstore)
3. **Fan-out** — assigns each item to every grid cell it intersects
   (boundary-inclusive) via `fan_out()`.
4. **Fan-out + partition grouping** — assigns each item to every grid cell it
   intersects (boundary-inclusive) via `fan_out()`, then groups the results by
   `(grid_partition, year)` via `group_by_partition()` so each GeoParquet file
   covers exactly one partition value for both Iceberg transforms.
5. **Writes** one GeoParquet file per `(grid_partition, year)` group using
   [rustac](https://github.com/stac-utils/rustac) into a hive-style directory
   layout (`grid_partition=<cell>/year=<year>/`) — full GeoParquet compliance
   (`geo` key, `geoarrow.wkb` geometry extension) with no manual metadata
   construction.
5. **Registers** each file in a local SQLite-backed PyIceberg catalog via
   `table.add_files()`.
6. **Persists** the `catalog.db` to/from S3 with an atomic S3 lock
   (`If-None-Match: *`) so concurrent runners cannot corrupt it.

---

## Quick start

### 1. Create the environment

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

### 2. Run the incremental pipeline

```bash
# With a YAML config file
earthcatalog incremental \
    --config config/h3_r3.yaml \
    --inventory /tmp/s3_inventory.parquet

# Or with flags only
earthcatalog incremental \
    --inventory  /tmp/s3_inventory.csv \
    --catalog    /tmp/catalog.db \
    --warehouse  /tmp/warehouse \
    --limit      1000
```

### 3. Query with DuckDB

```python
import duckdb
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/warehouse")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute(f"""
    SELECT id, platform, grid_partition, datetime
    FROM iceberg_scan('{table.metadata_location}')
    WHERE platform = 'sentinel-2'
    LIMIT 20
""").df()
print(df)
```

---

## Architecture overview

```
S3 Inventory (CSV / CSV.gz / Parquet)
        │
        ▼
_iter_inventory()           streaming; one row in memory at a time
        │ (bucket, key) pairs
        ▼
ThreadPoolExecutor          obstore.get() per item — anonymous public S3
        │ JSON dicts
        ▼
fan_out(items, partitioner) H3 fan-out; colon-sanitised props; raw_stac JSON
        │ list[synthetic STAC items]
        ▼
group_by_partition(rows)    group by (grid_partition, year); sort by (platform, datetime)
        │ dict[(cell, year) → list[items]]
        ▼
write_geoparquet(group, path) rustac GeoParquetWriter + schema alignment (one file per group)
        │ hive-style .parquet files
        ▼
table.add_files(paths)      PyIceberg SQLite catalog registration (batch, all groups at once)
        │
        ▼
DuckDB / PyIceberg scan
```

See [docs/DESIGN_v3.md](docs/DESIGN_v3.md) for the full design.

---

## Key design choices

| Choice | Rationale |
|---|---|
| **obstore** for all S3 I/O | No credentials for public buckets; `skip_signature=True` |
| **rustac** for GeoParquet | Handles `geo` metadata and `geoarrow.wkb` extension automatically |
| **PyIceberg + SQLite** | Zero-infra catalog; no Glue, no REST server |
| **`IdentityTransform(grid_partition)` + `YearTransform(datetime)`** | One file per `(cell, year)` group satisfies the single-partition-value constraint of `add_files()`; enables Iceberg file pruning on spatial + temporal predicates |
| **Hive directory layout** | `grid_partition=<cell>/year=<year>/part_N.parquet` — compatible with Iceberg partition spec and readable by any Parquet-aware tool |
| **Unpartitioned Iceberg spec** ~~(old)~~ | Superseded — `IdentityTransform` now re-enabled |
| **S3 conditional writes** for lock | `If-None-Match: *` → atomic; no DynamoDB required |
| **schema.py** as single source of truth | `transform.py` derives `_COLUMNS` from it; `catalog.py` mirrors it with stable integer field IDs |

---

## Schema (22 columns)

| Column | Type | Notes |
|---|---|---|
| `id` | `string` (not null) | STAC item ID |
| `grid_partition` | `string` (not null) | H3 cell index |
| `geometry` | `binary` (WKB) | GeoParquet geometry |
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
| `platform` | `string` | |
| `version` | `string` | |
| `proj_code` | `string` | From `proj:code` |
| `sat_orbit_state` | `string` | From `sat:orbit_state` |
| `scene_1_id` | `string` | |
| `scene_2_id` | `string` | |
| `scene_1_frame` | `string` | |
| `scene_2_frame` | `string` | |
| `raw_stac` | `string` | Full original STAC item JSON |

---

## Tests

```bash
mamba run -n itslive-ingest pytest --tb=short -q
# 95 passed
```

| File | Tests | Covers |
|---|---|---|
| `test_transform.py` | 31 | `fan_out`, `write_geoparquet`, `_to_int`, `group_by_partition` |
| `test_h3_partitioner.py` | 8 | H3 boundary-inclusive, Point-safe |
| `test_inventory.py` | 14 | CSV, CSV.gz, Parquet streaming |
| `test_lock.py` | 8 | S3 lock acquire/release/race |
| `test_catalog_lifecycle.py` | 3 | catalog download/upload via MemoryStore |
| `test_roundtrip.py` | 8 | 0→N insert, multi-append, schema preservation |
| `test_duckdb_query.py` | 7 | DuckDB `iceberg_scan` — row count, filters, JSON extract |
| `test_pipeline_e2e.py` | 11 | Full `run()` with mocked S3 fetch |

---

## Configuration

```yaml
# config/h3_r3.yaml
catalog:
  db_path:   /tmp/earthcatalog.db
  warehouse: /tmp/earthcatalog_warehouse

grid:
  type:       h3
  resolution: 1        # H3 resolution; default 1 (sub continental scale)

ingest:
  chunk_size:  5000     # items per ThreadPoolExecutor batch
  max_workers: 16      # concurrent S3 fetch threads
```

---

## Requirements

- Python 3.11+
- mamba (conda-forge)
- See `environment.yml` for the full dependency list

Primary runtime dependencies: `pyarrow`, `pyiceberg[pyiceberg-core]`,
`obstore`, `rustac`, `h3-py`, `shapely`, `duckdb`, `typer`
