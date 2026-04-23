# EarthCatalog Python Package – Flexible Grid Design (Corrected)

This document describes the architecture for the `earthcatalog` Python package, which ingests massive STAC catalogs.  

This plan integrates the Pluggable Grid Architecture, the Strict Vectorized Schema, and the Hierarchical Sort Compaction into a single, cohesive lakehouse strategy.
🏗️ EarthCatalog Architecture Blueprint
1. Package Directory Structure

The codebase strictly separates the spatial business logic from the compute orchestration to ensure the exact same transformations run on a GitHub Runner as on a 100-node Dask cluster.

earthcatalog/
├── cli.py                     # Typer/Click interface (--grid, --mode)
├── config.py                  # YAML parser for grid configurations
├── core/                      
│   ├── catalog.py             # PyIceberg connection & schema/partition definitions
│   ├── schema.py              # Strict PyArrow schema definitions
│   ├── partitioner.py         # AbstractPartitioner base class
│   └── transform.py           # The ETL engine: JSON -> Grid Fan-out -> Arrow Table
├── grids/                     # Pluggable grid implementations
│   ├── h3_partitioner.py      
│   ├── s2_partitioner.py      
│   └── geojson_partitioner.py # R-tree indexed custom boundaries
├── pipelines/                 
│   ├── backfill.py            # Dask distributed MapReduce (100M items)
│   └── incremental.py         # Single-node delta ingest (20k daily items)
└── maintenance/
    └── compact.py             # Weekly Iceberg rewriteDataFiles procedure

2. The Vectorized Schema (core/schema.py)

To enable millisecond analytics via DuckDB, all relevant STAC properties are promoted to top-level columns. Missing JSON fields will be safely coerced into Parquet Nulls.
Python

import pyarrow as pa

catalog_schema = pa.schema([
    # Routing & Core
    pa.field("id", pa.string(), nullable=False),
    pa.field("grid_partition", pa.string(), nullable=False), # Populated by grid plugin
    pa.field("geometry", pa.binary(), nullable=False),       # WKB for GeoParquet
    
    # Time (Vectorized for fast filtering)
    pa.field("datetime", pa.timestamp('us', tz='UTC')),
    pa.field("start_datetime", pa.timestamp('us', tz='UTC')),
    pa.field("mid_datetime", pa.timestamp('us', tz='UTC')),
    pa.field("end_datetime", pa.timestamp('us', tz='UTC')),
    pa.field("created", pa.timestamp('us', tz='UTC')),
    pa.field("updated", pa.timestamp('us', tz='UTC')),
    
    # Metrics (Vectorized for fast aggregations)
    pa.field("percent_valid_pixels", pa.int32()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("date_dt", pa.int32()),
    
    # Categoricals
    pa.field("platform", pa.string()),
    pa.field("version", pa.string()),
    pa.field("proj:code", pa.string()),
    pa.field("sat:orbit_state", pa.string()),
    pa.field("scene_1_id", pa.string()),
    pa.field("scene_2_id", pa.string()),
    pa.field("scene_1_frame", pa.string()),
    pa.field("scene_2_frame", pa.string()),
    
    # Fallback
    pa.field("raw_stac", pa.string()) # Complete original JSON
])

3. Storage Routing (core/catalog.py)

The table is initialized with a Space-First partition specification to optimize for deep-time analytical queries over specific geographic regions.

    Spec: PartitionSpec(<grid_column>, year(datetime))

    Physical Result: s3://datalake/grid_partition=8a2a.../year=2026/

4. The ETL Engine (core/transform.py)

This is the single source of truth for data mutation. It handles the "Overlap Multiplier" (duplicating records that cross grid boundaries) and enforces the strict PyArrow schema.
Python

from shapely.geometry import shape
import pyarrow as pa
import json
from .schema import catalog_schema

def process_chunk(stac_items: list[dict], partitioner) -> pa.Table:
    records = []
    
    for item in stac_items:
        props = item.get("properties", {})
        geom_wkb = shape(item['geometry']).wkb
        
        # 1. Resolve Grid Overlaps
        grid_keys = partitioner.get_intersecting_keys(geom_wkb)
        
        for key in grid_keys:
            # 2. Flatten & Map
            records.append({
                "id": item["id"],
                "grid_partition": key,
                "geometry": geom_wkb,
                "datetime": props.get("datetime"),
                "start_datetime": props.get("start_datetime"),
                # ... [map remaining fields] ...
                "platform": props.get("platform"),
                "percent_valid_pixels": props.get("percent_valid_pixels"),
                "raw_stac": json.dumps(item)
            })
            
    # 3. Enforce Schema strictness
    return pa.Table.from_pylist(records, schema=catalog_schema)

5. Execution Pipelines
Phase 1: The Initial Backfill (Dask)

    Input: A master S3 inventory file (100M URIs).

    Map: Dask Head Node chunks URIs. Dask Workers download JSONs, execute process_chunk(), and write independent .ndjson files to S3 staging folders (staging/grid_key/year/worker_id.ndjson).

    Reduce: Head Node reads staging folders via pyarrow.dataset, consolidates them into memory, and performs atomic Iceberg appends via pyiceberg.

Phase 2: The Daily Incremental (GitHub Actions)

    Input: Daily S3 inventory file.

    Delta Check: Headless script performs an Anti-Join between the S3 Inventory and the existing Iceberg catalog to identify new URIs.

    Commit: Uses Python's native concurrent.futures to download JSONs, executes process_chunk() in memory, and performs a single PyArrow append to Iceberg.

6. Maintenance: The "Sort & Compact" Job

Because daily ingest files are small and unordered, a weekly maintenance script (maintenance/compact.py) must be run to bin-pack the Parquet files and optimize them for DuckDB queries.

Execution: Submits the following SQL to an engine capable of Iceberg maintenance (e.g., Athena, Spark, or a managed service like Tabular):
SQL

CALL my_catalog.system.rewrite_data_files(
  table => 'earth_catalog.stac_items',
  options => map(
    'target-file-size-bytes', '268435456',  -- 256MB target
    'min-file-size-bytes', '67108864'       -- 64MB min
  ),
  strategy => 'sort',
  -- Hierarchical linear sort: Low cardinality -> Quality -> Time
  sort_order => 'platform ASC NULLS LAST, percent_valid_pixels DESC NULLS LAST, datetime ASC'
);
