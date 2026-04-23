# EarthCatalog Python Package – Flexible Grid Design (v2)

This document describes the architecture for the `earthcatalog` Python package, which ingests massive STAC catalogs into a lakehouse built on Apache Iceberg. It integrates the **Pluggable Grid Architecture**, the **Strict Vectorized Schema**, and the **Hierarchical Sort Compaction** into a single, cohesive strategy.

---

## Table of Contents

1. [Overview & Goals](#1-overview--goals)
2. [Package Directory Structure](#2-package-directory-structure)
3. [Dependencies & Environment](#3-dependencies--environment)
4. [Configuration (YAML)](#4-configuration-yaml)
5. [Pluggable Grid Architecture](#5-pluggable-grid-architecture)
6. [The Vectorized Schema (`core/schema.py`)](#6-the-vectorized-schema-coreschemapy)
7. [Storage Routing (`core/catalog.py`)](#7-storage-routing-corecatalogpy)
8. [The ETL Engine (`core/transform.py`)](#8-the-etl-engine-coretransformpy)
9. [Execution Pipelines](#9-execution-pipelines)
10. [Catalog Lifecycle: SQLite-in-S3](#10-catalog-lifecycle-sqlite-in-s3)
11. [S3 Lock File (`core/lock.py`)](#11-s3-lock-file-corelockpy)
12. [CLI Interface](#12-cli-interface)
13. [Maintenance: Sort & Compact](#13-maintenance-sort--compact)
14. [Testing Strategy](#14-testing-strategy)
15. [Open Questions & Future Work](#15-open-questions--future-work)

---

## 1. Overview & Goals

**EarthCatalog** ingests STAC item catalogs (starting with ITS_LIVE velocity pairs) into a spatially-partitioned Iceberg table stored on S3. The primary goals are:

- **Scale**: Handle 100M+ STAC items in an initial backfill via Dask, then keep up with ~20k daily additions via GitHub Actions.
- **Query speed**: Sub-second spatial + temporal range queries via DuckDB over GeoParquet-compatible Iceberg files.
- **Flexibility**: Swap spatial grid backends (H3, S2, custom GeoJSON boundaries) without touching the ETL core.
- **Portability**: The exact same transform code runs on a laptop, a GitHub Runner, or a 100-node Dask cluster.

### High-Level Data Flow

```
S3 Inventory (URIs)
        │
        ▼
  ┌─────────────┐    chunk URIs     ┌────────────────────────────────────┐
  │  Dask Head  │ ───────────────►  │   Dask Workers (spot instances)    │
  │  (scheduler)│                   │  1. fetch STAC JSON                │
  └─────────────┘                   │  2. process_chunk()                │
        ▲                           │  3. write Parquet → S3 directly    │
        │  tiny metadata dicts      │     grid_partition=.../year=.../   │
        │  (~1 KB per task)         └────────────────────────────────────┘
        │                                          │
        │◄─────────────────────────────────────────┘
        │  { path, row_count, file_size, col_stats }
        │
        ▼
  PyIceberg transaction.add_file()
  (head node registers paths — zero data in memory)
        │
        ▼
   Iceberg table (queryable via DuckDB / Athena)
```

---

## 2. Package Directory Structure

The codebase strictly separates spatial business logic from compute orchestration so the same transformations run identically on a GitHub Runner and a 100-node Dask cluster.

```
earthcatalog/
├── cli.py                     # Typer CLI (--config, --inventory, overrides)
├── config.py                  # YAML parser → AppConfig / GridConfig / IngestConfig
├── core/
│   ├── catalog.py             # PyIceberg SqlCatalog + download/upload via obstore
│   ├── schema.py              # Strict PyArrow schema definitions
│   ├── partitioner.py         # AbstractPartitioner ABC (get_intersecting_keys, key_to_wkt)
│   ├── transform.py           # ETL engine: JSON → Grid Fan-out → Arrow Table
│   ├── lock.py                # S3 lockfile via obstore conditional writes
│   └── store_config.py        # Global obstore-compatible store (default: LocalStore)
├── grids/                     # Pluggable grid implementations
│   ├── __init__.py            # build_partitioner(GridConfig) factory
│   ├── h3_partitioner.py
│   └── geojson_partitioner.py # R-tree indexed custom boundaries (loads via obstore)
├── pipelines/
│   ├── backfill.py            # Dask distributed MapReduce (100M items) — TODO
│   └── incremental.py         # Single-node delta ingest; run() + run_from_config()
└── maintenance/
    └── compact.py             # Weekly Iceberg rewriteDataFiles — TODO
```

---

## 3. Dependencies & Environment

All dependencies are pinned in `environment.yml` and managed via `conda-forge`. Key packages:

| Package | Role |
|---|---|
| `pyarrow` | In-memory columnar format & schema enforcement |
| `pyiceberg[pyiceberg-core]` | Iceberg table writes, catalog management, YearTransform |
| `dask` / `distributed` | Distributed backfill orchestration |
| `h3-py` | H3 hexagonal grid partitioning |
| `shapely` | Geometry operations and WKB serialization |
| `duckdb` | Local analytics queries over Iceberg/Parquet |
| `obstore` | All S3 I/O (anonymous public buckets via `skip_signature=True`) |
| `pyyaml` | YAML config loading |
| `typer` | CLI framework |
| `boto3` | Present in env only to satisfy PyIceberg internal extras — **never called directly** |

Install:

```bash
conda env create -f environment.yml
conda activate itslive-ingest
```

---

## 4. Configuration (YAML)

Grid type and catalog settings are driven by a YAML config file, keeping the CLI simple and reproducible runs auditable.

```yaml
# config/h3_r5.yaml
catalog:
  uri: "s3://my-datalake/iceberg/"
  warehouse: "earth_catalog"
  table: "stac_items"

grid:
  type: "h3"
  resolution: 5           # H3 resolution (0=coarse, 15=fine)

ingest:
  data_prefix: "s3://my-datalake/iceberg/data/"  # workers write Parquet here directly
  chunk_size: 5000        # STAC items per Dask task
  max_workers: 8          # For concurrent.futures incremental mode
```

```yaml
# config/geojson_regions.yaml
catalog:
  uri: "s3://my-datalake/iceberg/"
  warehouse: "earth_catalog"
  table: "stac_items_regions"

grid:
  type: "geojson"
  boundaries_path: "s3://my-datalake/grids/polar_regions.geojson"
  id_field: "region_name"  # GeoJSON property to use as partition key

ingest:
  data_prefix: "s3://my-datalake/iceberg/data/"  # workers write Parquet here directly
```

`config.py` loads and validates these:

```python
# core/config.py
import yaml
from dataclasses import dataclass
from typing import Literal

@dataclass
class CatalogConfig:
    uri: str
    warehouse: str
    table: str

@dataclass
class GridConfig:
    type: Literal["h3", "s2", "geojson"]
    resolution: int | None = None
    boundaries_path: str | None = None
    id_field: str | None = None

@dataclass
class AppConfig:
    catalog: CatalogConfig
    grid: GridConfig
    ingest: dict

def load_config(path: str) -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return AppConfig(
        catalog=CatalogConfig(**raw["catalog"]),
        grid=GridConfig(**raw["grid"]),
        ingest=raw.get("ingest", {}),
    )
```

---

## 5. Pluggable Grid Architecture

### 5.1 Abstract Base Class (`core/partitioner.py`)

Every grid backend implements this interface. The ETL engine only depends on this contract.

```python
# core/partitioner.py
from abc import ABC, abstractmethod

class AbstractPartitioner(ABC):
    """
    Given a geometry (as WKB bytes), return the set of grid cell keys
    whose boundaries intersect that geometry.  A single item may map to
    multiple keys (the "Overlap Multiplier").
    """

    @abstractmethod
    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        """Return grid cell IDs that intersect the given WKB geometry."""
        ...

    @abstractmethod
    def key_to_wkt(self, key: str) -> str:
        """Return the WKT boundary of a grid cell (useful for debugging)."""
        ...
```

### 5.2 H3 Implementation (`grids/h3_partitioner.py`)

```python
class H3Partitioner(AbstractPartitioner):
    def __init__(self, resolution: int = 3):
        self.resolution = resolution

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)

        # h3.geo_to_cells only accepts Polygon / MultiPolygon.
        # Points are handled separately via latlng_to_cell.
        if geom.geom_type == "Point":
            return [h3.latlng_to_cell(geom.y, geom.x, self.resolution)]

        interior = set(h3.geo_to_cells(geometry.mapping(geom), self.resolution))
        boundary = _boundary_cells(geom, self.resolution)   # densified ring walk
        return list(interior | boundary)
```

`_boundary_cells` densifies each edge of the exterior ring (~10 km steps) and
maps every sample point to its H3 cell, ensuring no cell is missed even when
two polygon vertices straddle a cell boundary.

### 5.3 GeoJSON / R-tree Implementation (`grids/geojson_partitioner.py`)

This implementation supports arbitrary polygon boundaries (e.g., ITS_LIVE drainage basins, polar regions) via an in-memory R-tree index for fast intersection lookups.

```python
# grids/geojson_partitioner.py
import json
from shapely import wkb
from shapely.geometry import shape
from shapely.strtree import STRtree
from .core.partitioner import AbstractPartitioner

class GeoJSONPartitioner(AbstractPartitioner):
    def __init__(self, boundaries_path: str, id_field: str = "id"):
        with open(boundaries_path) as f:
            features = json.load(f)["features"]

        self._geometries = [shape(feat["geometry"]) for feat in features]
        self._keys = [str(feat["properties"][id_field]) for feat in features]
        self._tree = STRtree(self._geometries)

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        candidate_idxs = self._tree.query(geom, predicate="intersects")
        return [self._keys[i] for i in candidate_idxs]

    def key_to_wkt(self, key: str) -> str:
        idx = self._keys.index(key)
        return self._geometries[idx].wkt
```

### 5.4 Partitioner Factory

A factory function wires config → implementation cleanly:

```python
# grids/__init__.py
from .h3_partitioner import H3Partitioner
from .s2_partitioner import S2Partitioner
from .geojson_partitioner import GeoJSONPartitioner
from ..core.config import GridConfig

def build_partitioner(cfg: GridConfig) -> "AbstractPartitioner":
    if cfg.type == "h3":
        return H3Partitioner(resolution=cfg.resolution or 5)
    elif cfg.type == "s2":
        return S2Partitioner(level=cfg.resolution or 10)
    elif cfg.type == "geojson":
        return GeoJSONPartitioner(
            boundaries_path=cfg.boundaries_path,
            id_field=cfg.id_field or "id",
        )
    else:
        raise ValueError(f"Unknown grid type: {cfg.type!r}")
```

---

## 6. The Vectorized Schema (`core/schema.py`)

All relevant STAC properties are promoted to top-level columns for millisecond analytics via DuckDB. Missing JSON fields are safely coerced to Parquet nulls.

```python
# core/schema.py
import pyarrow as pa

catalog_schema = pa.schema([
    # Routing & Core
    pa.field("id",              pa.string(),                  nullable=False),
    pa.field("grid_partition",  pa.string(),                  nullable=False),  # from grid plugin
    pa.field("geometry",        pa.binary(),                  nullable=False),  # WKB → GeoParquet

    # Time (vectorized for fast filtering)
    pa.field("datetime",        pa.timestamp('us', tz='UTC')),
    pa.field("start_datetime",  pa.timestamp('us', tz='UTC')),
    pa.field("mid_datetime",    pa.timestamp('us', tz='UTC')),
    pa.field("end_datetime",    pa.timestamp('us', tz='UTC')),
    pa.field("created",         pa.timestamp('us', tz='UTC')),
    pa.field("updated",         pa.timestamp('us', tz='UTC')),

    # Metrics (vectorized for fast aggregations)
    pa.field("percent_valid_pixels", pa.int32()),
    pa.field("latitude",        pa.float64()),
    pa.field("longitude",       pa.float64()),
    pa.field("date_dt",         pa.int32()),

    # Categoricals
    pa.field("platform",        pa.string()),
    pa.field("version",         pa.string()),
    pa.field("proj:code",       pa.string()),
    pa.field("sat:orbit_state", pa.string()),
    pa.field("scene_1_id",      pa.string()),
    pa.field("scene_2_id",      pa.string()),
    pa.field("scene_1_frame",   pa.string()),
    pa.field("scene_2_frame",   pa.string()),

    # Fallback
    pa.field("raw_stac",        pa.string()),  # complete original JSON
])
```

---

## 7. Storage Routing (`core/catalog.py`)

The Iceberg table uses a **Space-First** partition spec to optimize deep-time analytical queries over specific geographic regions.

- **Partition spec**: `PartitionSpec(grid_partition, year(datetime))`
- **Physical layout**: `s3://datalake/grid_partition=8a2a.../year=2026/`

```python
# core/catalog.py
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, BinaryType, TimestamptzType,
    IntegerType, DoubleType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, YearTransform
from .schema import catalog_schema  # PyArrow schema → converted below

def get_or_create_table(catalog_uri: str, warehouse: str, table_name: str):
    catalog = load_catalog("glue", uri=catalog_uri, warehouse=warehouse)
    full_name = f"{warehouse}.{table_name}"

    if catalog.table_exists(full_name):
        return catalog.load_table(full_name)

    # Convert PyArrow schema to PyIceberg schema (abbreviated)
    iceberg_schema = Schema(
        NestedField(1,  "id",             StringType(),    required=True),
        NestedField(2,  "grid_partition",  StringType(),    required=True),
        NestedField(3,  "geometry",        BinaryType(),    required=True),
        NestedField(4,  "datetime",        TimestamptzType()),
        # ... remaining fields ...
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=100,
                       transform=IdentityTransform(), name="grid_partition"),
        PartitionField(source_id=4, field_id=101,
                       transform=YearTransform(),     name="year"),
    )

    return catalog.create_table(
        identifier=full_name,
        schema=iceberg_schema,
        partition_spec=partition_spec,
        location=f"s3://my-datalake/iceberg/{table_name}",
    )
```

---

## 8. The ETL Engine (`core/transform.py`)

This is the single source of truth for data mutation. It handles the **Overlap Multiplier** (duplicating records that cross grid boundaries) and enforces the strict PyArrow schema.

```python
# core/transform.py
import json
from datetime import datetime, timezone

import pyarrow as pa
from shapely.geometry import shape

from .schema import catalog_schema
from .partitioner import AbstractPartitioner


def _parse_ts(value: str | None) -> datetime | None:
    """Parse ISO-8601 strings to timezone-aware datetimes; return None on failure."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def process_chunk(stac_items: list[dict], partitioner: AbstractPartitioner) -> pa.Table:
    """
    Transform a list of raw STAC item dicts into a PyArrow Table.

    Each item may produce MULTIPLE rows if its geometry overlaps more than
    one grid cell (the Overlap Multiplier).  All rows share the same raw_stac
    value so the original record is always recoverable.
    """
    records = []

    for item in stac_items:
        props = item.get("properties", {})

        try:
            geom_wkb = shape(item["geometry"]).wkb
        except Exception:
            # Malformed geometry: skip item, optionally log
            continue

        grid_keys = partitioner.get_intersecting_keys(geom_wkb)
        if not grid_keys:
            # Item falls outside all known grid cells; store under sentinel key
            grid_keys = ["__none__"]

        raw_json = json.dumps(item)

        for key in grid_keys:
            records.append({
                "id":                    item["id"],
                "grid_partition":        key,
                "geometry":              geom_wkb,
                "datetime":              _parse_ts(props.get("datetime")),
                "start_datetime":        _parse_ts(props.get("start_datetime")),
                "mid_datetime":          _parse_ts(props.get("mid_datetime")),
                "end_datetime":          _parse_ts(props.get("end_datetime")),
                "created":               _parse_ts(props.get("created")),
                "updated":               _parse_ts(props.get("updated")),
                "percent_valid_pixels":  props.get("percent_valid_pixels"),
                "latitude":              props.get("latitude"),
                "longitude":             props.get("longitude"),
                "date_dt":               props.get("date_dt"),
                "platform":              props.get("platform"),
                "version":               props.get("version"),
                "proj:code":             props.get("proj:code"),
                "sat:orbit_state":       props.get("sat:orbit_state"),
                "scene_1_id":            props.get("scene_1_id"),
                "scene_2_id":            props.get("scene_2_id"),
                "scene_1_frame":         props.get("scene_1_frame"),
                "scene_2_frame":         props.get("scene_2_frame"),
                "raw_stac":              raw_json,
            })

    return pa.Table.from_pylist(records, schema=catalog_schema)
```

---

## 9. Execution Pipelines

### Scheduler RAM Budget — the Golden Rule

> **Workers own all I/O. The scheduler owns only metadata.**

The Dask head node has limited RAM.  Any design that routes row data through it will OOM.  The rule is:

- Workers write Parquet files **directly** to S3 in their spatially-correct locations.
- Workers return only a **tiny metadata dict** (file path, byte size, row count, column statistics) back to the scheduler.
- The head node collects those dicts and calls PyIceberg's transaction API to register the files — no data ever touches it.

```
Worker payload returned to scheduler (~1 KB per task, not 256 MB):

{
  "path":      "s3://bucket/iceberg/data/grid_partition=8a2a1b/year=2026/part_0042.parquet",
  "file_size": 134217728,   # bytes
  "row_count": 487_302,
  "col_stats": {
    "datetime":   {"min": "2026-01-01T00:00:00Z", "max": "2026-12-31T23:59:59Z"},
    "grid_partition": {"min": "8a2a1b", "max": "8a2a1b"},
  }
}
```

### Phase 1: The Initial Backfill (`pipelines/backfill.py`)

**Input**: A master S3 inventory file listing 100M STAC item URIs.

```
Map    → Dask workers: download JSON → process_chunk() → write Parquet to S3
           └─ return: tiny file-metadata dict (path, rows, bytes, col stats)
Commit → Head node: collect metadata dicts → PyIceberg transaction.add_file() → commit
```

The key difference from a naive design: **the head node never reads Parquet data**.
It only registers file paths that workers already wrote.

```python
# pipelines/backfill.py
import json
import dask.bag as db
import pyarrow.parquet as pq
import obstore
from obstore.store import S3Store
from ..core.transform import process_chunk
from ..grids import build_partitioner
from ..core.catalog import get_or_create_table


def _get_store(bucket: str) -> S3Store:
    return S3Store(bucket=bucket, region="us-west-2", skip_signature=True)


def _worker_task(chunk: list[tuple[str, str]], partitioner, data_prefix: str) -> dict:
    """Runs entirely on a Dask worker.  Returns a small metadata dict."""
    from dask.base import tokenize

    items = []
    for bucket, key in chunk:
        try:
            raw = bytes(obstore.get(_get_store(bucket), key).bytes())
            items.append(json.loads(raw))
        except Exception:
            pass

    arrow_table = process_chunk(items, partitioner)
    if arrow_table.num_rows == 0:
        return {}

    grid_key  = arrow_table["grid_partition"][0].as_py()
    year      = arrow_table["datetime"][0].as_py().year
    file_name = f"part_{tokenize(chunk)}.parquet"
    s3_path   = f"{data_prefix}grid_partition={grid_key}/year={year}/{file_name}"

    # Write directly to S3 via obstore
    buf = pq.write_table(arrow_table, use_pyarrow=True)  # in-memory buffer
    # (use pyarrow.BufferOutputStream + obstore.put for real impl)

    return {"path": s3_path, "row_count": arrow_table.num_rows}
```


def run_backfill(uri_list_path: str, config):
    partitioner = build_partitioner(config.grid)
    table = get_or_create_table(
        config.catalog.uri, config.catalog.warehouse, config.catalog.table
    )
    data_prefix = f"{config.ingest['data_prefix']}"
    chunk_size  = config.ingest.get("chunk_size", 5000)

    uris = open(uri_list_path).read().splitlines()
    bag  = db.from_sequence(uris, partition_size=chunk_size)

    # Workers run _worker_task; scheduler receives a bag of small dicts
    file_metadata_bag = bag.map_partitions(
        _worker_task,
        partitioner=partitioner,
        data_prefix=data_prefix,
    )
    file_metadata: list[dict] = [m for m in file_metadata_bag.compute() if m]

    # Head node commit: register files with Iceberg — zero data in memory
    print(f"Committing {len(file_metadata)} Parquet files to Iceberg catalog...")
    with table.transaction() as tx:
        for meta in file_metadata:
            tx.add_files(data_file=meta["path"])  # PyIceberg >=0.7 API
    print("Backfill committed.")
```

> **Note — pre-grouping chunks by grid cell**: If URI chunks are pre-sorted so
> all items in one chunk map to the same `grid_partition`, each worker writes
> exactly one Parquet file and the partition path is unambiguous.  If chunks
> are spatially random (as in a raw S3 inventory), a worker may produce
> multiple grid keys; in that case, call `pq.write_to_dataset()` with
> `partitioning=["grid_partition", "year"]` and return one metadata dict per
> output file.

### Phase 2: The Daily Incremental (`pipelines/incremental.py`)

**Input**: Daily S3 inventory file (~20k new URIs), run via GitHub Actions.

The same scheduler-RAM principle applies: `process_chunk()` runs in-process on
the single runner, but the delta check must **not** pull all 100M existing IDs
into memory.  Instead, query Iceberg with a pushdown filter — only the
`id` column, no data files scanned unnecessarily.

```python
# pipelines/incremental.py  (config-driven entry point)
from earthcatalog.config import load_config
from earthcatalog.pipelines.incremental import run_from_config

def run_incremental(inventory_path: str, config_path: str, limit=None):
    cfg = load_config(config_path)
    run_from_config(inventory_path, cfg, limit=limit)
```

---

## 10a. Store Configuration (`core/store_config.py`)

All I/O (catalog download/upload and lock file) goes through a single
**injected store** rather than constructing S3 clients directly.  This
makes every component fully testable without any AWS credentials.

```python
# core/store_config.py
from pathlib import Path
from obstore.store import LocalStore

_DEFAULT_ROOT = "/tmp/earthcatalog_store"
Path(_DEFAULT_ROOT).mkdir(parents=True, exist_ok=True)

_store      = LocalStore(_DEFAULT_ROOT)  # ← default: local disk, zero config
_catalog_key = "catalog.db"
_lock_key    = ".lock"

def get_store(): ...
def set_store(store): ...        # swap in S3Store, MemoryStore, etc.
def get_catalog_key(): ...
def set_catalog_key(key): ...
def get_lock_key(): ...
def set_lock_key(key): ...
```

**Production override** (run once before any job):

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

> Note: S3 conditional writes (`mode='create'`, i.e. `If-None-Match: *`)
> require AWS Signature V4 — they work against any authenticated S3 bucket
> but **not** against anonymous public buckets.  `MemoryStore` and
> `LocalStore` support `mode='create'` natively for testing.

---

## 10. Catalog Lifecycle: SQLite-in-S3

PyIceberg's `SqlCatalog` (backed by SQLite) is a zero-infrastructure catalog — no Glue, no Hive Metastore, no REST server required. Because every write job (backfill and incremental) runs exclusively and sequentially, we can store the `catalog.db` file on S3 and shuttle it to local disk for the duration of each job.

### Lifecycle per Job Run

```
Job starts
    │
    ▼
1. Acquire S3 lock (see §11)
2. Download  s3://bucket/catalog/catalog.db  →  /tmp/catalog.db
3. Point PyIceberg at  sqlite:////tmp/catalog.db
    │
    ▼
[ ... do all work: process chunks, workers write Parquet, head commits ... ]
    │
    ▼
4. Upload  /tmp/catalog.db  →  s3://bucket/catalog/catalog.db  (overwrite)
5. Release S3 lock
Job ends
```

```python
# core/catalog.py  (updated to support SQLite-in-S3 lifecycle)
import obstore
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog
from earthcatalog.core.store_config import get_store, get_catalog_key

LOCAL_CATALOG_PATH = "/tmp/earthcatalog.db"

def download_catalog() -> None:
    """Pull the SQLite catalog from the configured store to local disk."""
    store = get_store()
    key   = get_catalog_key()
    try:
        data = bytes(obstore.get(store, key).bytes())
        Path(LOCAL_CATALOG_PATH).write_bytes(data)
        print(f"Catalog downloaded ({len(data):,} bytes)")
    except FileNotFoundError:
        print("No existing catalog in store — will create a fresh one.")

def upload_catalog() -> None:
    """Push the updated SQLite catalog back to the configured store."""
    store = get_store()
    key   = get_catalog_key()
    data  = Path(LOCAL_CATALOG_PATH).read_bytes()
    obstore.put(store, key, data)
    print(f"Catalog uploaded ({len(data):,} bytes)")

def open_local_catalog(warehouse_path: str) -> SqlCatalog:
    """Open PyIceberg against the local SQLite file."""
    return SqlCatalog(
        "earthcatalog",
        uri=f"sqlite:///{LOCAL_CATALOG_PATH}",
        warehouse=warehouse_path,
    )
```

The two pipeline entry points (`run_backfill`, `run_incremental`) wrap their
entire body in this lifecycle:

```python
from .lock import S3Lock
from .catalog import download_catalog, upload_catalog, open_local_catalog

def run_backfill(uri_list_path: str, config):
    catalog_s3  = config.catalog.s3_uri          # e.g. s3://bucket/catalog/catalog.db
    lock_s3     = config.catalog.lock_uri         # e.g. s3://bucket/catalog/.lock

    with S3Lock(lock_s3, owner="backfill"):
        download_catalog(catalog_s3)
        catalog = open_local_catalog(config.catalog.warehouse_path)
        # ... all the Dask work ...
        upload_catalog(catalog_s3)
```

---

## 11. S3 Lock File (`core/lock.py`)

### Design

The lock is a single small JSON file at a well-known key (e.g.
`catalog/.lock`) in the **configured store** (see §10a).

#### Why the naive read-then-write approach is wrong

A read-then-write implementation has a classic **TOCTOU race** (Time Of Check /
Time Of Use):

```
Process A: GET .lock  → 404 (no lock)
Process B: GET .lock  → 404 (no lock)          ← both see it free
Process A: PUT .lock  ← writes lock
Process B: PUT .lock  ← overwrites A's lock    ← both now "hold" the lock
```

Both processes proceed, both commit to `catalog.db`, whoever uploads last
silently wins and destroys the other's work.

#### The fix: S3 Conditional Writes (`If-None-Match: *`)

AWS released **S3 Conditional Writes** in August 2024. A `PUT` with the header
`If-None-Match: *` is atomic at the S3 level: it succeeds only if the key does
**not** exist at the moment of the write. If two processes race, exactly one
gets `200 OK` and the other gets `412 Precondition Failed` — guaranteed, with
no external database or DynamoDB table required.

```
Process A: PUT .lock (If-None-Match: *)  → 200 OK      ← wins the lock
Process B: PUT .lock (If-None-Match: *)  → 412 Failed  ← immediately aborted
```

The acquire step is now a **single atomic call** with no read beforehand.

```
s3://my-datalake/catalog/.lock contents:
{
  "owner":     "backfill",
  "pid":       12345,
  "hostname":  "dask-head-0",
  "acquired":  "2026-04-22T03:00:00Z",
  "ttl_hours": 12
}
```

### Implementation

```python
# core/lock.py
import json
import socket
import os
from datetime import datetime, timezone, timedelta

import obstore
from obstore.exceptions import AlreadyExistsError
from earthcatalog.core.store_config import get_store, get_lock_key


class CatalogLocked(RuntimeError):
    """Raised when the lock is held by another process."""


class S3Lock:
    """
    Atomic lockfile using obstore conditional writes (If-None-Match: *).
    Works with LocalStore and MemoryStore for testing; with S3Store for prod.
    """

    def __init__(self, owner: str, ttl_hours: int = 12):
        self._owner    = owner
        self._ttl      = ttl_hours

    def __enter__(self):
        self.acquire(); return self

    def __exit__(self, *_):
        self.release()

    def acquire(self) -> None:
        store   = get_store()
        key     = get_lock_key()
        payload = self._make_payload()
        try:
            obstore.put(store, key, payload, mode="create")
        except AlreadyExistsError:
            existing = self._read_lock()
            if existing is None:
                obstore.put(store, key, payload, mode="create"); return
            acquired_at = datetime.fromisoformat(existing["acquired"])
            age = datetime.now(timezone.utc) - acquired_at
            ttl = timedelta(hours=existing.get("ttl_hours", self._ttl))
            if age >= ttl:
                obstore.delete(store, key)
                obstore.put(store, key, payload, mode="create"); return
            raise CatalogLocked(
                f"Catalog locked by '{existing['owner']}' since {existing['acquired']}. "
                f"Expires in {ttl - age}."
            )

    def release(self) -> None:
        store = get_store()
        key   = get_lock_key()
        try:
            obstore.delete(store, key)
        except FileNotFoundError:
            pass

    def _make_payload(self) -> bytes:
        return json.dumps({
            "owner":     self._owner,
            "pid":       os.getpid(),
            "hostname":  socket.gethostname(),
            "acquired":  datetime.now(timezone.utc).isoformat(),
            "ttl_hours": self._ttl,
        }).encode()

    def _read_lock(self) -> dict | None:
        store = get_store()
        key   = get_lock_key()
        try:
            return json.loads(bytes(obstore.get(store, key).bytes()))
        except FileNotFoundError:
            return None
```

### Failure modes

| Scenario | Behaviour |
|---|---|
| Two jobs start simultaneously | Only one `PUT If-None-Match` wins (`200`); the other gets `412` and raises `CatalogLocked` before doing any work — **no race condition** |
| Job crashes mid-run (lock not released) | Lock stays on S3; next job checks age against `ttl_hours`, deletes stale lock, re-acquires atomically |
| Lock key accidentally deleted | Next job's `PUT If-None-Match` succeeds immediately — proceeds normally |
| S3 outage during `release()` | Exception is swallowed; lock becomes stale and is overridden after TTL |
| boto3 < 1.35 (no `IfNoneMatch` param) | `TypeError` on `put_object` — version check in CI will catch this |

### GitHub Actions integration

Add a lock check as the very first step, before any Python code runs:

```yaml
# .github/workflows/daily_ingest.yml
- name: Check catalog lock
  run: |
    aws s3api head-object \
      --bucket my-datalake \
      --key catalog/.lock 2>/dev/null \
    && echo "LOCKED=true" >> $GITHUB_ENV \
    || echo "LOCKED=false" >> $GITHUB_ENV

- name: Abort if locked
  if: env.LOCKED == 'true'
  run: |
    echo "Catalog is locked by another process. Aborting."
    exit 1

- name: Run incremental ingest
  run: earthcatalog incremental --config config/h3_r5.yaml --inventory /tmp/delta.txt
```

---

## 12. CLI Interface

```python
# cli.py
import typer
from .core.config import load_config
from .pipelines.backfill import run_backfill
from .pipelines.incremental import run_incremental

app = typer.Typer(help="EarthCatalog STAC ingest tool")

@app.command()
def backfill(
    config: str = typer.Option(..., "--config", "-c", help="Path to YAML config"),
    inventory: str = typer.Option(..., "--inventory", "-i", help="S3 URI list file"),
):
    """Run full distributed backfill via Dask."""
    cfg = load_config(config)
    run_backfill(inventory, cfg)

@app.command()
def incremental(
    config: str = typer.Option(..., "--config", "-c", help="Path to YAML config"),
    inventory: str = typer.Option(..., "--inventory", "-i", help="Daily delta URI list"),
):
    """Run single-node incremental ingest (for GitHub Actions)."""
    cfg = load_config(config)
    uris = open(inventory).read().splitlines()
    run_incremental(uris, cfg)

if __name__ == "__main__":
    app()
```

Example usage:

```bash
# Initial backfill (Dask cluster assumed running)
earthcatalog backfill --config config/h3_r5.yaml --inventory s3://bucket/inventory_full.txt

# Daily incremental (GitHub Actions)
earthcatalog incremental --config config/h3_r5.yaml --inventory /tmp/daily_delta.txt
```

---

## 13. Maintenance: Sort & Compact (`maintenance/compact.py`)

Because daily ingest files are small and unordered, a weekly maintenance script re-bins and sorts Parquet files to optimize DuckDB query performance.

**Sort order rationale**: Low-cardinality columns first maximizes run-length encoding compression; `percent_valid_pixels DESC` sorts best-quality data first for early-termination queries.

```python
# maintenance/compact.py
import boto3

def run_compaction(catalog_name: str, database: str, table: str, region: str = "us-west-2"):
    """
    Submit Iceberg compaction via Athena.  Requires the Iceberg connector
    and CALL syntax support (Athena engine v3 or Spark).
    """
    client = boto3.client("athena", region_name=region)
    sql = f"""
    CALL {catalog_name}.system.rewrite_data_files(
      table => '{database}.{table}',
      options => map(
        'target-file-size-bytes', '268435456',
        'min-file-size-bytes',    '67108864'
      ),
      strategy   => 'sort',
      sort_order => 'platform ASC NULLS LAST,
                     percent_valid_pixels DESC NULLS LAST,
                     datetime ASC'
    )
    """
    response = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": "s3://my-datalake/athena-results/"},
    )
    print(f"Compaction query submitted: {response['QueryExecutionId']}")
```

---

## 14. Testing Strategy

### Unit Tests

Each component is tested in isolation with small synthetic inputs.

```python
# tests/test_transform.py
import pyarrow as pa
from earthcatalog.core.transform import process_chunk
from earthcatalog.grids.h3_partitioner import H3Partitioner

SAMPLE_ITEM = {
    "id": "LC08_L2SP_001001_20230601",
    "geometry": {
        "type": "Point",
        "coordinates": [-70.0, -77.5]  # Antarctic point
    },
    "properties": {
        "datetime": "2023-06-01T00:00:00Z",
        "platform": "landsat-8",
        "percent_valid_pixels": 95,
    }
}

def test_process_chunk_produces_valid_table():
    partitioner = H3Partitioner(resolution=3)
    table = process_chunk([SAMPLE_ITEM], partitioner)
    assert isinstance(table, pa.Table)
    assert table.num_rows >= 1
    assert "id" in table.schema.names
    assert "grid_partition" in table.schema.names

def test_overlap_multiplier():
    """Items whose geometry spans multiple cells produce multiple rows."""
    large_polygon_item = {
        "id": "large_scene",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-80, -70], [80, -70], [80, -80], [-80, -80], [-80, -70]]]
        },
        "properties": {"datetime": "2023-01-01T00:00:00Z"}
    }
    partitioner = H3Partitioner(resolution=2)
    table = process_chunk([large_polygon_item], partitioner)
    assert table.num_rows > 1  # Should span multiple H3 cells
```

### Integration Test: Round-trip

```python
# tests/test_roundtrip.py
import duckdb
from earthcatalog.core.transform import process_chunk
from earthcatalog.grids.h3_partitioner import H3Partitioner

def test_duckdb_can_query_output(tmp_path):
    partitioner = H3Partitioner(resolution=3)
    table = process_chunk([SAMPLE_ITEM], partitioner)

    parquet_path = tmp_path / "test.parquet"
    import pyarrow.parquet as pq
    pq.write_table(table, parquet_path)

    con = duckdb.connect()
    result = con.execute(
        f"SELECT id, platform FROM read_parquet('{parquet_path}')"
    ).fetchall()
    assert result[0][1] == "landsat-8"
```

---

## 15. Open Questions & Future Work

| Topic | Question | Options |
|---|---|---|
| **Iceberg catalog backend** | Glue vs. REST catalog vs. local for dev? | Start with local `SqlCatalog` for dev, Glue for prod |
| **S2 partitioner** | Which Python S2 binding? (`s2geometry`, `s2cell`) | Evaluate `s2geometry` conda-forge package |
| **Overlap deduplication** | Should queries deduplicate by `id` or is fan-out intentional? | Document query pattern; add `DISTINCT ON (id)` example |
| **Schema evolution** | How to add new STAC extension fields over time? | Use PyIceberg `update_schema()` with nullability guarantee |
| **Backfill resume** | How to resume a partial backfill after failure? | Track completed staging prefixes in a SQLite manifest |
| **GeoParquet compliance** | Add `geo` metadata to Parquet files for GDAL compatibility? | Use `geoarrow` / `stac-geoparquet` for final writes |
