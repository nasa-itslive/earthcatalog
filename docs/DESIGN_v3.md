# EarthCatalog Python Package – Design Document (v3)

This document describes the current architecture of the `earthcatalog` Python
package, which ingests massive STAC catalogs into a lakehouse built on Apache
Iceberg. It reflects the **actual working implementation** as of April 2026.

**Latest milestone (2026-04-23)**: End-to-end S3 warehouse run verified —
20 000 STAC items ingested from a real S3 Inventory manifest, 6 compacted
GeoParquet files written to `s3://its-live-data/test-space/stac/catalog/data/`,
`catalog.db` uploaded to S3, and spatial queries with H3 cell pruning confirmed
working via DuckDB `iceberg_scan()` against the live S3 table.

**Spot-resilience milestone (2026-04-23)**: Per-file mini-run architecture
implemented in `run_backfill` — each manifest data file is an independent
mini-run with its own lock cycle and catalog upload, enabling automatic
checkpoint/resume via `ingest_stats.json`.

---

## Table of Contents

1. [Overview & Goals](#1-overview--goals)
2. [Package Directory Structure](#2-package-directory-structure)
3. [Dependencies & Environment](#3-dependencies--environment)
4. [Configuration (YAML)](#4-configuration-yaml)
5. [Pluggable Grid Architecture](#5-pluggable-grid-architecture)
6. [The Vectorized Schema (`core/schema.py`)](#6-the-vectorized-schema-coreschemapy)
7. [Catalog Management (`core/catalog.py`)](#7-catalog-management-corecatalogpy)
8. [The ETL Engine (`core/transform.py`)](#8-the-etl-engine-coretransformpy)
9. [Execution Pipelines](#9-execution-pipelines)
10. [Store Configuration (`core/store_config.py`)](#10-store-configuration-corestore_configpy)
11. [Catalog Lifecycle: SQLite-in-S3](#11-catalog-lifecycle-sqlite-in-s3)
12. [S3 Lock File (`core/lock.py`)](#12-s3-lock-file-corelockpy)
13. [CLI Interface (`cli.py`)](#13-cli-interface-clipy)
14. [Maintenance: Sort & Compact](#14-maintenance-sort--compact)
15. [Testing Strategy](#15-testing-strategy)
16. [Open Questions & Future Work](#16-open-questions--future-work)

---

## 1. Overview & Goals

**EarthCatalog** ingests STAC item catalogs (starting with ITS_LIVE velocity
pairs) into a spatially-partitioned Iceberg table stored on S3. Primary goals:

- **Scale**: Handle 100M+ STAC items in an initial backfill via Dask, then
  keep up with daily additions via GitHub Actions.
- **Query speed**: Sub-second spatial + temporal range queries via DuckDB over
  GeoParquet-compatible Iceberg files.
- **Flexibility**: Swap spatial grid backends (H3, S2, custom GeoJSON
  boundaries) without touching the ETL core.
- **Portability**: The exact same `process_chunk()` function runs on a laptop,
  a GitHub Runner, or a 100-node Dask cluster.
- **No proprietary services**: All I/O via `obstore`; no boto3, no s3fs, no
  Glue, no DynamoDB.

### High-Level Data Flow (single-node, current)

```
S3 Inventory CSV / CSV.gz / Parquet / manifest.json
        │  streamed line-by-line or row-group by row-group
        ▼
  _iter_inventory()  →  (bucket, key) pairs
        │  chunked into batches of chunk_size (default 500)
        ▼
  ThreadPoolExecutor  →  obstore.get() per item  →  JSON dicts
        │
        ▼
  fan_out()  →  list[synthetic STAC items] with grid_partition injected (~14× fan-out)
        │
        ▼
  write_geoparquet()  →  rustac writes GeoParquet file (geo metadata + geoarrow.wkb)
        │
        ▼
  table.add_files()  →  register Parquet file in Iceberg catalog (SQLite)
```

### High-Level Data Flow (Dask backfill, planned)

```
S3 Inventory
        │
        ▼
  Head node: _iter_inventory() → chunks dispatched as Dask tasks
        │
        ▼
  Workers: obstore.get() → process_chunk() → write Parquet directly to S3
           return: tiny metadata dict (path, rows, bytes)
        │
        ▼
  Head node: PyIceberg transaction.add_file() → commit
             (zero Parquet data ever touches the head node)
```

---

## 2. Package Directory Structure

```
earthcatalog/                        # repo root
├── pyproject.toml                   # package; CLI entry points
├── environment.yml                  # mamba env (itslive-ingest)
├── config/                          # example YAML configs (not committed)
│   ├── h3_r3.yaml
│   └── geojson_regions.yaml
├── docs/
│   └── DESIGN_v3.md                 # this document
├── tests/
│   ├── test_transform.py            # 24 unit tests  (fan_out, write_geoparquet, _to_int)
│   ├── test_h3_partitioner.py       # 8 unit tests
│   ├── test_lock.py                 # 8 unit tests
│   ├── test_catalog_lifecycle.py    # 3 unit tests
│   ├── test_inventory.py            # 14 unit tests (CSV, CSV.gz, Parquet)
│   ├── test_roundtrip.py            # 8 integration tests (fan_out → write_geoparquet → add_files → scan)
│   ├── test_duckdb_query.py         # 7 integration tests (DuckDB iceberg_scan against local catalog)
│   └── test_pipeline_e2e.py         # 11 end-to-end tests (inventory → run() → scan)
└── earthcatalog/                    # Python package
    ├── cli.py                       # Typer CLI — incremental command
    ├── config.py                    # AppConfig / GridConfig / IngestConfig / load_config()
    ├── core/
    │   ├── schema.py                # Canonical PyArrow schema (22 fields) + GEOMETRY_FIELDS
    │   ├── partitioner.py           # AbstractPartitioner ABC
    │   ├── transform.py             # fan_out() + write_geoparquet() + _align_schema()
    │   ├── catalog.py               # SqlCatalog open/create + download/upload (obstore)
    │   ├── lock.py                  # S3Lock via obstore conditional writes
    │   └── store_config.py          # global store (default: LocalStore)
    ├── grids/
    │   ├── __init__.py              # build_partitioner(GridConfig) factory
    │   ├── h3_partitioner.py        # boundary-inclusive H3; key_to_wkt()
    │   └── geojson_partitioner.py   # STRtree R-tree; loads boundaries via obstore
    ├── pipelines/
    │   └── incremental.py           # single-node pipeline; run() + run_from_config()
    └── tools/
        └── make_test_inventory.py   # lists public S3 prefix → writes inventory CSV
```

Implemented and tested:

```
    ├── pipelines/
    │   └── backfill.py              # Dask distributed MapReduce — DONE
    └── maintenance/
        └── compact.py               # Iceberg sort/compact — TODO
```

---

## 3. Dependencies & Environment

All dependencies are managed via `mamba` (conda-forge). Key packages:

| Package | Role |
|---|---|
| `pyarrow` | In-memory columnar format & schema enforcement |
| `pyiceberg[pyiceberg-core]` | Iceberg table writes, catalog management, `YearTransform` |
| `obstore` | **All S3 I/O** — anonymous public buckets via `skip_signature=True`; conditional writes via `mode="create"` |
| `h3-py` | H3 hexagonal grid partitioning |
| `shapely` | Geometry operations, WKB serialization, STRtree |
| `duckdb` | Local analytics queries over Iceberg/Parquet |
| `pyyaml` | YAML config loading |
| `typer` | CLI framework |
| `boto3` | Present in env **only** to satisfy PyIceberg internal extras — never called directly |
| `dask` / `distributed` | Distributed backfill orchestration — installed, not yet used |

Install:

```bash
mamba env create -f environment.yml
mamba activate itslive-ingest
pip install -e .
```

---

## 4. Configuration (YAML)

Grid type and catalog settings are driven by a YAML config file.

```yaml
# config/h3_r3.yaml
catalog:
  db_path:   /tmp/earthcatalog.db       # local SQLite path during job
  warehouse: /tmp/earthcatalog_warehouse

grid:
  type:       h3
  resolution: 3       # H3 resolution (0=coarse, 15=fine); default 3

ingest:
  chunk_size:  500    # STAC items fetched per ThreadPoolExecutor batch
  max_workers: 16     # concurrent S3 fetch threads
```

```yaml
# config/geojson_regions.yaml
catalog:
  db_path:   /tmp/earthcatalog_regions.db
  warehouse: /tmp/earthcatalog_regions_warehouse

grid:
  type:            geojson
  boundaries_path: /path/to/polar_regions.geojson  # or s3:// URI
  id_field:        region_name

ingest:
  chunk_size:  500
  max_workers: 16
```

`config.py` implements three dataclasses and a loader:

```python
# earthcatalog/config.py
from dataclasses import dataclass, field
from typing import Literal
import yaml

@dataclass
class CatalogConfig:
    db_path:   str = "/tmp/earthcatalog.db"
    warehouse: str = "/tmp/earthcatalog_warehouse"

@dataclass
class GridConfig:
    type:            Literal["h3", "s2", "geojson"] = "h3"
    resolution:      int | None = None      # H3 / S2 level
    boundaries_path: str | None = None      # GeoJSON only
    id_field:        str | None = None      # GeoJSON only

@dataclass
class IngestConfig:
    chunk_size:  int = 500
    max_workers: int = 16

@dataclass
class AppConfig:
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    grid:    GridConfig    = field(default_factory=GridConfig)
    ingest:  IngestConfig  = field(default_factory=IngestConfig)

def load_config(path: str) -> AppConfig:
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}
    return AppConfig(
        catalog=CatalogConfig(**raw.get("catalog", {})),
        grid=GridConfig(**raw.get("grid", {})),
        ingest=IngestConfig(**raw.get("ingest", {})),
    )
```

---

## 5. Pluggable Grid Architecture

### 5.1 Abstract Base Class (`core/partitioner.py`)

Every grid backend implements this two-method interface. The ETL engine depends
only on this contract — swapping grid backends requires zero changes to
`transform.py`.

```python
from abc import ABC, abstractmethod

class AbstractPartitioner(ABC):
    @abstractmethod
    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        """Return grid cell IDs that intersect the given WKB geometry."""
        ...

    @abstractmethod
    def key_to_wkt(self, key: str) -> str:
        """Return the WKT boundary polygon of a grid cell (for debugging)."""
        ...
```

### 5.2 H3 Implementation (`grids/h3_partitioner.py`)

Boundary-inclusive: interior cells via `h3.geo_to_cells` **UNION** boundary
cells via a densified ring walk. Points use `h3.latlng_to_cell` directly
(`h3.geo_to_cells` crashes on Point geometry).

```python
class H3Partitioner(AbstractPartitioner):
    def __init__(self, resolution: int = 3):
        self.resolution = resolution

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        if geom.geom_type == "Point":
            return [h3.latlng_to_cell(geom.y, geom.x, self.resolution)]
        interior = set(h3.geo_to_cells(mapping(geom), self.resolution))
        boundary = _boundary_cells(geom, self.resolution)  # densified ring walk
        return list(interior | boundary)

    def key_to_wkt(self, key: str) -> str:
        coords = [(lng, lat) for lat, lng in h3.cell_to_boundary(key)]
        return Polygon(coords).wkt
```

`_boundary_cells` densifies each edge of the exterior ring (~10 km steps) and
maps every sample point to its H3 cell — no cell is missed when two vertices
straddle a cell boundary.

### 5.3 GeoJSON / R-tree Implementation (`grids/geojson_partitioner.py`)

Supports arbitrary polygon boundaries (drainage basins, polar regions, etc.)
via an in-memory STRtree index. The boundaries file is loaded via **obstore**,
supporting both local paths and `s3://` URIs.

```python
class GeoJSONPartitioner(AbstractPartitioner):
    def __init__(self, boundaries_path: str, id_field: str = "id"):
        raw = _load_bytes(boundaries_path)   # obstore-backed, local or S3
        features = json.loads(raw)["features"]
        self._geometries = [shape(feat["geometry"]) for feat in features]
        self._keys = [str(feat["properties"][id_field]) for feat in features]
        self._tree = STRtree(self._geometries)

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        return [self._keys[i]
                for i in self._tree.query(geom, predicate="intersects")]

    def key_to_wkt(self, key: str) -> str:
        return self._geometries[self._keys.index(key)].wkt
```

### 5.4 Partitioner Factory (`grids/__init__.py`)

```python
from earthcatalog.config import GridConfig

def build_partitioner(cfg: GridConfig) -> AbstractPartitioner:
    if cfg.type == "h3":
        return H3Partitioner(resolution=cfg.resolution or 1)
    if cfg.type == "geojson":
        return GeoJSONPartitioner(
            boundaries_path=cfg.boundaries_path,
            id_field=cfg.id_field or "id",
        )
    if cfg.type == "s2":
        raise NotImplementedError("S2 partitioner not yet implemented")
    raise ValueError(f"Unknown grid type: {cfg.type!r}")
```

---

## 6. The Vectorized Schema (`core/schema.py`)

`schema.py` is the **single source of truth** for the PyArrow column layout.
It is imported by `transform.py` (GeoParquet writing) and serves as the
reference for the parallel PyIceberg schema in `catalog.py`.

All relevant STAC properties are promoted to top-level typed columns. Colon
characters in property names are sanitised (`proj:code` → `proj_code`).
Missing fields coerce to Arrow nulls.

`GEOMETRY_FIELDS` is a frozenset of field names that carry WKB binary geometry
and must be annotated with the `geoarrow.wkb` Arrow extension type when the
GeoParquet file is written.

```python
# earthcatalog/core/schema.py
import pyarrow as pa

GEOMETRY_FIELDS: frozenset[str] = frozenset({"geometry"})

schema = pa.schema([
    # Identity
    pa.field("id",                   pa.string(),              nullable=False),
    pa.field("grid_partition",       pa.string(),              nullable=False),
    pa.field("geometry",             pa.binary(),              nullable=True),   # WKB; optional

    # Timestamps
    pa.field("datetime",             pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("start_datetime",       pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("mid_datetime",         pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("end_datetime",         pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("created",              pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("updated",              pa.timestamp("us", tz="UTC"), nullable=True),

    # Metrics — catalog sends floats (e.g. 5.0), always stored as int32
    pa.field("percent_valid_pixels", pa.int32(),  nullable=True),
    pa.field("date_dt",              pa.int32(),  nullable=True),

    # Coordinates
    pa.field("latitude",             pa.float64(), nullable=True),
    pa.field("longitude",            pa.float64(), nullable=True),

    # Categoricals
    pa.field("platform",             pa.string(), nullable=True),
    pa.field("version",              pa.string(), nullable=True),
    pa.field("proj_code",            pa.string(), nullable=True),   # "proj:code"
    pa.field("sat_orbit_state",      pa.string(), nullable=True),   # "sat:orbit_state"
    pa.field("scene_1_id",           pa.string(), nullable=True),
    pa.field("scene_2_id",           pa.string(), nullable=True),
    pa.field("scene_1_frame",        pa.string(), nullable=True),
    pa.field("scene_2_frame",        pa.string(), nullable=True),

    # Fallback — full original JSON always recoverable
    pa.field("raw_stac",             pa.string(), nullable=True),
])
```

---

## 7. Catalog Management (`core/catalog.py`)

The Iceberg table is partitioned by **`(IdentityTransform(grid_partition),
YearTransform(datetime))`**. Each registered GeoParquet file must contain
exactly one value for every `IdentityTransform` field — enforced by writing
one file per `(grid_partition, year)` group (see `group_by_partition()` in §8).
This enables Iceberg file-level pruning: a spatial query converts its geometry
to H3 cells and filters `WHERE grid_partition IN (<cells>)` — Iceberg skips all
files whose cell is not in the set.

The PyIceberg schema mirrors `schema.py` with stable integer field IDs (never
reordered or reused — required for Iceberg schema evolution).

```python
PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=2,   # grid_partition
        field_id=1000,
        transform=IdentityTransform(),
        name="grid_partition",
    ),
    PartitionField(
        source_id=4,   # datetime
        field_id=1001,
        transform=YearTransform(),
        name="datetime_year",
    ),
)
```

Key functions:

| Function | Description |
|---|---|
| `open_catalog(db_path, warehouse_path)` | Open/create SQLite-backed SqlCatalog |
| `get_or_create_table(catalog)` | Load or create `earthcatalog.stac_items` |
| `download_catalog(local_path)` | Pull `catalog.db` from configured store; silent if missing (first run) |
| `upload_catalog(local_path)` | Push updated `catalog.db` back to configured store |

All store I/O goes through `store_config.get_store()` — no hardcoded S3 URIs.

---

## 8. The ETL Engine (`core/transform.py`)

The ETL engine exposes three public functions. All are **pure functions** —
no I/O, no global state — so they run identically on a laptop or inside a
Dask worker.

### `fan_out(stac_items, partitioner) -> list[dict]`

Produces one synthetic STAC item per (source_item × grid_cell) pair:

1. Parse geometry with Shapely; call `partitioner.get_intersecting_keys(wkb)`.
2. Sanitise property names (`proj:code` → `proj_code`).
3. Round float fields `percent_valid_pixels` and `date_dt` to Python `int`.
4. Inject `grid_partition` key into each synthetic item's `properties`.
5. Serialise the original item to `raw_stac` JSON for full recoverability.

Items with unparseable geometry are skipped. Items outside all grid cells get
sentinel key `"__none__"`.

### `group_by_partition(fan_out_items) -> dict[tuple[str, int|None], list[dict]]`

Groups `fan_out()` output by `(grid_partition, year)` and sorts each group by
`(platform, datetime)`. This ensures:

- Each `write_geoparquet()` call produces a file with exactly one
  `grid_partition` value (required by `IdentityTransform`).
- Each file spans at most one calendar year (required by `YearTransform`).
- Within each file, rows are sorted for run-length encoding and scan locality.

### `write_geoparquet(fan_out_items, path) -> int`

Writes fan-out items to a GeoParquet file using `rustac.GeoparquetWriter`.
**Contract**: all items in the list must have the same `grid_partition` value
and the same calendar year.

1. rustac writes a temp file — it handles the `geo` Parquet key and
   `geoarrow.wkb` Arrow extension on the geometry column automatically.
2. `pq.ParquetFile(tmp).read()` reads the file directly (bypasses PyArrow's
   hive-partition schema inference which would conflict with the `grid_partition`
   column in the hive directory name).
3. `_align_schema()` casts all columns to the target types from `schema.py`,
   fills missing columns with typed nulls, and forwards the `geo` key from the
   rustac temp file into the output schema metadata unchanged.
4. Returns the number of rows written (0 if input is empty).

### Schema alignment details

`_COLUMNS` is derived from `schema.py` at import time — no duplication:

```python
from .schema import GEOMETRY_FIELDS, schema as ARROW_SCHEMA

_COLUMNS = [
    (f.name, f.type, f.nullable, f.name in GEOMETRY_FIELDS)
    for f in ARROW_SCHEMA
]
```

rustac infers column types from Python values:

| Python value | rustac Arrow type | Target (schema.py) | Action |
|---|---|---|---|
| `int` | `int64` | `int32` | cast |
| `float` | `double` | `double` | identity |
| STAC `datetime` string | `timestamp[ms, UTC]` | `timestamp[us, UTC]` | cast |
| `mid_datetime` (non-standard) | `string` | `timestamp[us, UTC]` | cast via string |
| missing column | absent | target type | fill with nulls |

### `asyncio` usage

`rustac.GeoparquetWriter.open()` is async. A fresh `asyncio` event loop is
created and closed inside `write_geoparquet()` — no `async`/`await` propagates
to callers.

```python
loop = asyncio.new_event_loop()
try:
    loop.run_until_complete(_async_write(items, tmp_path))
finally:
    loop.close()
```

---

## 9. Execution Pipelines

### Phase 1: Single-Node Incremental (`pipelines/incremental.py`)

**Status**: Fully implemented and tested.

**Input**: S3 Inventory file in CSV, CSV.gz, or Parquet format.

```
_iter_inventory()          — streaming; never loads the full file into RAM
    ↓ (bucket, key) pairs
ThreadPoolExecutor         — obstore.get() per item, max_workers=16 by default
    ↓ JSON dicts
fan_out()                  — H3 fan-out, property sanitisation, raw_stac serialisation
    ↓ list[synthetic STAC items]
group_by_partition()       — group by (grid_partition, year); sort by (platform, datetime)
    ↓ dict[(cell, year) → list[items]]
write_geoparquet()         — one file per group, hive-style directory layout
    ↓ list[paths]
table.add_files(paths)     — register all files in one PyIceberg call (SQLite)
```

#### Inventory streaming

| Format | How it is read |
|---|---|
| `.csv` (local) | `open()` + `csv.reader` — one row in memory |
| `.csv.gz` (local) | `gzip.open()` + `csv.reader` — one row in memory |
| `.csv` / `.csv.gz` (S3) | Downloaded once via obstore; stream-decompressed via `gzip.open(BytesIO(...))` |
| `.parquet` (local or S3) | `pq.ParquetFile.iter_batches(batch_size=65536)` — one row-group in memory |

AWS S3 Inventory Parquet files contain extra columns (`size`, `storage_class`,
etc.) that are silently ignored; only `bucket` and `key` are read.

#### Entry points

```python
# Direct (argparse CLI, existing behaviour)
run(inventory_path, catalog_path, warehouse_path, ...)

# Config-driven (Typer CLI, YAML file)
run_from_config(inventory_path, config: AppConfig, limit=None)
```

### Phase 2: Dask Distributed Backfill (`pipelines/backfill.py`)

**Status**: Implemented, tested, and **verified against real S3** (2026-04-23).
Supports local Dask cluster and Coiled.
Features: `since=` delta filter, per-`(cell, year)` compaction with dedup and
orphan sweep, Coiled pass-through CLI flags, S3 warehouse + catalog support,
**spot-resilient per-file mini-runs with checkpoint/resume**.

**Verified S3 run** (20 000 items, `--no-lock`, local 4-worker cluster):

```
Manifest: 126 data file(s) in pds-buckets-its-live-logbucket-70tr3aw5f2op
Level 0: 10 ingest_chunk tasks (20000 items)
Level 0 done: 60 files, 56113 rows
Level 1: 6 compact tasks, 0 files passed through
Level 2: registering 6 files in one snapshot
Catalog uploaded → s3://its-live-data/test-space/stac/catalog/catalog.db
Backfill done. 20000 items → 56113 rows across 6 files
Snapshots in catalog: 1
```

Output layout in S3:

```
s3://its-live-data/test-space/stac/catalog/
├── catalog.db                                     # Iceberg SQLite catalog
└── data/
    ├── earthcatalog/stac_items/metadata/          # Iceberg metadata files
    ├── grid_partition=81007ffffffffff/year=2019/compacted_*.parquet
    ├── grid_partition=81007ffffffffff/year=2020/compacted_*.parquet
    ├── grid_partition=81037ffffffffff/year=2019/compacted_*.parquet
    ├── grid_partition=81037ffffffffff/year=2020/compacted_*.parquet
    ├── grid_partition=8107bffffffffff/year=2019/compacted_*.parquet
    └── grid_partition=8107bffffffffff/year=2020/compacted_*.parquet
```

**Key bug fixes applied during S3 run**:

| Bug | Cause | Fix |
|---|---|---|
| `UnboundLocalError: warehouse_path` | `main()` referenced `warehouse_path` in the S3 branch where only `warehouse_root` was set | Use `warehouse_root` (always set) at call site |
| `Path(s3://...)` mangled URI | `str(Path(warehouse_root) / fm.s3_key)` produced `/its-live-data/...` | Added `_join_path(root, key)` helper that string-joins for `s3://` URIs |
| Workers writing to bucket root | `S3Store(bucket=...)` had no prefix; keys were `grid_partition=.../...` at bucket root | Pass `prefix=wh_prefix` to `S3Store` so the store is rooted at the warehouse prefix |

**Golden Rule — Scheduler RAM Budget**:

> Workers own all I/O. The scheduler owns only metadata.

- Workers fetch STAC JSON from S3, call `fan_out()` + `write_geoparquet()`,
  write Parquet **directly** to S3 in their output paths.
- Workers return only a **tiny metadata dict** (~1 KB) back to the scheduler:

```python
{
    "path":      "s3://bucket/warehouse/earthcatalog/stac_items/data/part_042.parquet",
    "file_size": 134_217_728,
    "row_count": 487_302,
}
```

- The head node calls `table.add_files([path])` to register paths —
  **zero Parquet data ever touches the head node**.

#### Spot-resilient execution: per-file mini-runs

**Problem**: a spot instance interrupted after 80 of 126 inventory data files
loses all 80 files' worth of Iceberg registration — they are written to S3 but
never recorded in the catalog. On restart, all 126 files are re-processed from
scratch.

**Solution**: `per_file_mini_runs=True` (default). When the inventory is a
`manifest.json`, the pipeline processes each manifest data file as an
independent mini-run:

```
For each manifest data file (e.g. 126 files):
    1. Check ingest_stats.json — skip if this file's key is already recorded
    2. Acquire S3 lock
    3. download_catalog()  ←  pull the latest catalog.db from S3
    4. Level 0: ingest this file's items  →  write part files to S3
    5. Level 1: compact (cell, year) buckets within this file's items
    6. Level 2: table.add_files() + upload_catalog()  ←  commit to catalog
    7. Append one record to ingest_stats.json (inventory_file = data_key)
    8. Release S3 lock
```

**Checkpoint mechanism**: `ingest_stats.json` stores `inventory_file` (the
S3 key of each processed data file). On restart, `_load_processed_files()`
reads this file and skips any data files already recorded. The catalog IS the
source of truth; the stats file is the resume checkpoint.

**Failure scenarios**:

| Interruption point | Data written to S3? | Catalog updated? | On restart |
|---|---|---|---|
| Before step 6 | Yes (part files) | No | Re-process file (dedup handles duplicates) |
| During step 6 (`add_files` succeeded, `upload_catalog` failed) | Yes | In local memory only | `catalog.db` not uploaded — re-process file |
| After step 6, before step 7 | Yes | Yes | stats record not written — re-process file (dedup handles) |
| After step 7 | Yes | Yes | File skipped on restart ✓ |

Worst case: one data file's items are re-processed. Dedup by `id` during
compaction guarantees correctness. Maximum wasted work ≈ 1 file (a few hundred
STAC items).

**`ingest_stats.json` record** (per mini-run):

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

**Monolithic mode** (opt-out): pass `--no-per-file-mini-runs` on the CLI or
`per_file_mini_runs=False` in Python. One lock, one `add_files` call at the
end. Suitable for short smoke tests on non-spot machines.

---

## 10. Store Configuration (`core/store_config.py`)

All I/O (catalog download/upload, lock file, GeoJSON boundaries) goes through a
single **injected store** rather than constructing S3 clients directly. This
makes every component testable without AWS credentials.

```python
# Default: local disk, zero config
_store       = LocalStore("/tmp/earthcatalog_store")
_catalog_key = "catalog.db"
_lock_key    = ".lock"

def get_store() / set_store(store)
def get_catalog_key() / set_catalog_key(key)
def get_lock_key() / set_lock_key(key)
```

**Production override** (before any job):

```python
from obstore.store import S3Store
from earthcatalog.core import store_config

store_config.set_store(S3Store(bucket="my-bucket", region="us-west-2"))
store_config.set_catalog_key("catalog/catalog.db")
store_config.set_lock_key("catalog/.lock")
```

**Test override** (pytest fixture):

```python
from obstore.store import MemoryStore
store_config.set_store(MemoryStore())
```

> **Note**: S3 conditional writes (`mode="create"`, i.e. `If-None-Match: *`)
> require AWS Signature V4 — they work on any authenticated S3 bucket but
> **not** on anonymous public buckets. `MemoryStore` and `LocalStore` support
> `mode="create"` natively for all tests.

---

## 11. Catalog Lifecycle: SQLite-in-S3

PyIceberg's `SqlCatalog` (SQLite) is zero-infrastructure — no Glue, no Hive
Metastore, no REST server. The `catalog.db` file is shuttled to/from the
configured store for the duration of each job.

```
Job starts
    │
    ▼
1. Acquire lock  (see §12)
2. download_catalog("/tmp/catalog.db")   ← obstore.get(store, catalog_key)
3. open_catalog(db_path="/tmp/catalog.db", warehouse_path=...)
    │
    ▼
[ ... process_chunk() → table.append() loop ... ]
    │
    ▼
4. upload_catalog("/tmp/catalog.db")     ← obstore.put(store, catalog_key, data)
5. Release lock
Job ends
```

First run: `download_catalog` silently skips if the key does not exist; a fresh
catalog is created by `get_or_create_table`.

---

## 12. S3 Lock File (`core/lock.py`)

### Why the naive read-then-write approach is wrong

```
Process A: GET .lock  → 404 (no lock)
Process B: GET .lock  → 404 (no lock)       ← both see it free
Process A: PUT .lock  ← writes lock
Process B: PUT .lock  ← overwrites A's lock ← both now "hold" the lock
```

### The fix: obstore conditional writes (`mode="create"`)

`obstore.put(store, key, data, mode="create")` maps to `If-None-Match: *` on
S3. Exactly one concurrent PUT wins; the other raises `AlreadyExistsError`
immediately — guaranteed atomic, no DynamoDB required.

```python
class S3Lock:
    def __init__(self, owner: str, ttl_hours: int = 12): ...

    def acquire(self) -> None:
        # Single atomic call — no prior GET needed
        obstore.put(store, key, payload, mode="create")
        # On AlreadyExistsError: read existing lock; override if stale (age ≥ ttl)

    def release(self) -> None:
        obstore.delete(store, key)  # exception swallowed if already gone
```

### Failure modes

| Scenario | Behaviour |
|---|---|
| Two jobs start simultaneously | One `PUT mode="create"` wins; the other raises `CatalogLocked` before doing any work |
| Job crashes mid-run | Lock stays; next job overrides after `ttl_hours` |
| Lock key deleted externally | Next job's `PUT mode="create"` succeeds immediately |
| S3 outage during `release()` | Exception swallowed; lock becomes stale, overridden after TTL |

### GitHub Actions integration

```yaml
- name: Run incremental ingest
  run: earthcatalog incremental --config config/h3_r3.yaml --inventory /tmp/delta.csv
```

The Python `S3Lock` handles all locking logic; no separate shell check needed.

---

## 13. CLI Interface (`cli.py`)

Built with Typer. The `--config` flag loads a YAML `AppConfig`; individual
flags override any config-file value.

```python
app = typer.Typer(name="earthcatalog")

@app.command()
def incremental(
    inventory:     str           = typer.Option(...,  "--inventory", "-i"),
    config:        Optional[str] = typer.Option(None, "--config",    "-c"),
    catalog:       Optional[str] = typer.Option(None, "--catalog"),
    warehouse:     Optional[str] = typer.Option(None, "--warehouse"),
    chunk_size:    Optional[int] = typer.Option(None, "--chunk-size"),
    workers:       Optional[int] = typer.Option(None, "--workers"),
    h3_resolution: Optional[int] = typer.Option(None, "--h3-resolution"),
    limit:         Optional[int] = typer.Option(None, "--limit"),
): ...
```

Example usage:

```bash
# With a config file
earthcatalog incremental --config config/h3_r3.yaml --inventory /tmp/delta.csv

# Flags only (no config file needed)
earthcatalog incremental \
    --inventory /tmp/delta.parquet \
    --catalog   /tmp/ec.db \
    --warehouse /tmp/ec_wh \
    --limit     1000

# Legacy argparse entry point (also available)
earthcatalog-ingest --inventory /tmp/delta.csv --limit 500
```

---

## 14. Maintenance: Sort & Compact (`maintenance/compact.py`)

**Status**: Designed, not yet implemented.

Because daily ingest produces many small, spatially-random Parquet files, a
weekly maintenance job should re-sort and compact them for optimal DuckDB
query performance.

**Planned sort order** (low-cardinality first for run-length encoding):

```sql
ORDER BY platform ASC NULLS LAST,
         percent_valid_pixels DESC NULLS LAST,
         datetime ASC
```

Implementation will use PyIceberg's `rewrite_data_files` action rather than
Athena, to stay consistent with the obstore-only I/O policy.

---

## 15. Testing Strategy

### Test inventory (95 tests, all passing)

| File | Tests | What it covers |
|---|---|---|
| `test_transform.py` | 31 | `_to_int`, `fan_out` (sanitisation, colon rename, int rounding, fan-out multiplier), `write_geoparquet` (schema, types, geo metadata), `group_by_partition` (grouping, sorting, year extraction) |
| `test_h3_partitioner.py` | 8 | boundary-inclusive, Point-safe, resolution, no duplicates, `key_to_wkt` |
| `test_inventory.py` | 42 | CSV with/without header, CSV.gz, quoted values, Parquet (basic, order, batch boundary, extra columns, empty), dispatch, manifest.json, `_coerce_last_modified` |
| `test_lock.py` | 8 | acquire/release, context manager, stale override, race — all via `MemoryStore` |
| `test_catalog_lifecycle.py` | 3 | download/upload roundtrip via `MemoryStore` |
| `test_roundtrip.py` | 8 | `fan_out → group_by_partition → write_geoparquet → add_files → scan` cycle (schema, types, fan-out, 0→N insert, multi-append) |
| `test_duckdb_query.py` | 7 | DuckDB `iceberg_scan()` against local catalog: row count, all IDs present, int column types, platform filter, cumulative append, SQL filter, JSON extract |
| `test_pipeline_e2e.py` | 11 | Full `run()`: local inventory CSV → mocked fetch → Iceberg scan (row count, IDs, snapshot, int types, grid_partition, column promotion, raw_stac, geo metadata, limit, key filter, missing fetch) |
| `test_backfill.py` | 52 | `run_backfill`, `_ingest_chunk_impl`, `compact_group`, `_write_ingest_stats`, `_load_processed_files`, per-file mini-run checkpoint, fan-out Counter aggregation, S3 path helpers |

**Total: 178 tests passing, 4 skipped (integration)**

### Key design rules for tests

- **No real S3 access** — all store I/O uses `MemoryStore` or `LocalStore`
  (via `tmp_path`).
- **H3 resolution 2** in tests (resolution **1** in production) to keep fan-out
  manageable.
- `fan_out` and `write_geoparquet` are pure functions / side-effect-only —
  testable with no fixtures beyond `tmp_path`.
- DuckDB queries use `table.metadata_location` (the Iceberg metadata JSON path)
  which is always a local file path in tests — no S3 config needed.

### Spatial query with partition pruning (example)

With `IdentityTransform(grid_partition)`, Iceberg skips all files whose cell is
not in the query set. DuckDB does not yet propagate Iceberg partition predicates
automatically, so the recommended pattern is to convert the query geometry to H3
cells and pass them as an `IN` filter:

```python
import h3, duckdb

# Convert a GeoJSON query polygon to H3 cells at resolution 1
query_geojson = {
    "type": "Polygon",
    "coordinates": [[[-55, 60], [-10, 60], [-10, 85], [-55, 85], [-55, 60]]]
}
query_cells = list(h3.geo_to_cells(query_geojson, res=1))
cell_list = ", ".join(f"'{c}'" for c in query_cells)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
# Configure S3 credentials once
con.execute(f"""
    CREATE SECRET s3_creds (
        TYPE S3, KEY_ID '...', SECRET '...', REGION 'us-west-2'
    )
""")

# Spatial + temporal query — Iceberg file pruning skips non-matching cells
df = con.execute(f"""
    SELECT id, platform, datetime, percent_valid_pixels
    FROM iceberg_scan('{table.metadata_location}')
    WHERE grid_partition IN ({cell_list})
      AND datetime BETWEEN '2020-01-01' AND '2020-12-31'
    ORDER BY datetime
""").df()
```

**Verified against S3 data (2026-04-23)**:

```
Query: Greenland bounding box [-55,60 → -10,85] at H3 res=1
H3 cells: ['81067ffffffffff', '81063ffffffffff', '8107bffffffffff', ...]  (7 cells)

Full table:  3 cells × 2 years = 6 files, 56 113 rows
After cell pruning:  1 matching cell (8107bffffffffff) → 2 files scanned
  Greenland 2019: 16 368 rows
  Greenland 2020:  1 810 rows
Spatial + temporal (2020 only): 1 810 rows returned in < 1 s
```

Files for the 5 non-matching cells are **never opened** by DuckDB — file-level
pruning works end-to-end via Iceberg partition metadata.

---

### Verifying the catalog with DuckDB (example)

```python
import duckdb
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/wh")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
df = con.execute(
    f"SELECT id, platform, grid_partition "
    f"FROM iceberg_scan('{table.metadata_location}') "
    f"WHERE platform = 'sentinel-2' LIMIT 10"
).df()
print(df)
```

---

## 16. Open Questions & Future Work

| Topic | Status | Notes |
|---|---|---|
| **Dask backfill** (`pipelines/backfill.py`) | **Done** | `since=` delta filter, dedup+orphan sweep, Coiled pass-through CLI |
| **S3 warehouse + catalog** | **Done** | Full end-to-end verified 2026-04-23; `S3Store(prefix=...)`, `_join_path()`, `warehouse_root` fixes |
| **Manifest.json support** (`incremental.py`) | **Done** | `_iter_inventory_manifest` reads all Parquet data files from real S3 Inventory manifest; authenticated via `~/.aws/credentials` or env vars |
| **Spatial query + H3 pruning** | **Done** | Verified via DuckDB `iceberg_scan()` + `WHERE grid_partition IN (...)` against live S3 table |
| **`ingest_stats.json` on S3** | **Done** | Auto-uploaded to `<catalog_prefix>/ingest_stats.json`; `--report-location` overrides; records appended across runs; `inventory_file` field enables checkpoint |
| **Spot-resilient per-file mini-runs** | **Done** | `per_file_mini_runs=True` (default); each manifest data file is an independent mini-run; `ingest_stats.json` + `_load_processed_files()` checkpoint; `--no-per-file-mini-runs` reverts to monolithic |
| **Iceberg compaction** (`maintenance/compact.py`) | Not started | Use PyIceberg `rewrite_data_files`, not Athena |
| **S2 partitioner** | Not started | Evaluate `s2geometry` conda-forge package |
| **Overlap deduplication** | Open | Document DuckDB query pattern: `SELECT DISTINCT ON (id) ...` |
| **Schema evolution** | Open | Use PyIceberg `update_schema()` with nullability guarantee |
| **GeoParquet compliance** | Done | `geo` metadata + `geoarrow.wkb` extension written by rustac; verified by `test_duckdb_query.py` |
| **Delta check** | Open | Before ingesting, skip items whose `id` already exists in the table (pushdown filter via DuckDB) |
| **Iceberg partitioning** | **Done** | `IdentityTransform(grid_partition)` + `YearTransform(datetime)` re-enabled; enforced by `group_by_partition()` writing one file per `(cell, year)` group |
