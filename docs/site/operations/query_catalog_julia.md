# Querying the Catalog from Julia

How to query the EarthCatalog with DuckDB.jl directly from Julia, including
H3 file pruning without the Python `earthcatalog` package.

---

## Overview

The catalog is stored as a set of GeoParquet files in S3, spatially partitioned
by H3 cell (resolution 1) and year. The Python library does two things before
running a query:

1. **H3 pruning** — converts a geometry to a list of H3 cell IDs and reads only
   the relevant partition files from the Iceberg manifest.
2. **DuckDB query** — runs SQL with spatial filters over the pruned file list.

Both steps are fully reproducible in Julia using `DuckDB.jl`, `H3.jl`, and
`DataFrames.jl`. No Python or `earthcatalog` package is required.

---

## Setup

```julia
using Pkg
Pkg.add(["DuckDB", "DataFrames", "H3", "Arrow"])
```

---

## Step 1 — Compute H3 cells for a geometry

The catalog uses H3 **resolution 1** (`grid_partition` column). Given a point
or bounding box, compute the covering cells:

```julia
using H3.API

# --- Point query ---
# H3.API uses radians internally
lat, lon = 58.74, -133.99
cell = latLngToCell(LatLng(deg2rad(lat), deg2rad(lon)), 1)  # res = 1

cells_point = [string(cell, base=16)]  # e.g. ["8206d7fffffffff"]

# --- Bounding box query (disk approximation) ---
# Get all resolution-1 cells that cover a rough bbox
# Use gridDisk with a radius of 1 around the centre cell for small areas
centre = latLngToCell(LatLng(deg2rad(58.74), deg2rad(-133.99)), 1)
cells_box = [string(c, base=16) for c in gridDisk(centre, 1)]

# --- Alaska / large area ---
# For larger regions, enumerate a grid of sample points and collect unique cells
function cells_for_bbox(south, west, north, east; res=1, step=5.0)
    cells = Set{String}()
    lat = south
    while lat <= north
        lon = west
        while lon <= east
            c = latLngToCell(LatLng(deg2rad(lat), deg2rad(lon)), res)
            push!(cells, string(c, base=16))
            lon += step
        end
        lat += step
    end
    return collect(cells)
end

cells_alaska = cells_for_bbox(54.0, -170.0, 72.0, -130.0)
```

> **Tip**: Resolution 1 cells are very large (~86,000 km²). A single point
> typically maps to exactly one cell. For a bounding box the coarse grid
> usually covers 1–10 cells, so pruning is extremely effective.

---

## Step 2 — Build the file list from S3

Partition paths follow the Hive convention:

```
s3://its-live-data/test-space/stac/catalog/warehouse/
  grid_partition=8206d7fffffffff/
    year=2020/
      part_000001.parquet
      part_000002.parquet
    year=2021/
      part_000001.parquet
```

Enumerate them programmatically by reading the directory listing with DuckDB's
`httpfs` extension:

```julia
using DuckDB, DataFrames

con = DBInterface.connect(DuckDB.DB, ":memory:")

# Load the httpfs extension (auto-installs on first use)
DBInterface.execute(con, "INSTALL httpfs; LOAD httpfs;")
DBInterface.execute(con, "SET s3_region = 'us-west-2';")

const WAREHOUSE = "s3://its-live-data/test-space/stac/catalog/warehouse"

# Build glob patterns for the pruned cells + year range
function parquet_globs(cells::Vector{String};
                       start_year::Int=1900, end_year::Int=2100)
    globs = String[]
    for cell in cells
        for year in start_year:end_year
            push!(globs, "$WAREHOUSE/grid_partition=$cell/year=$year/*.parquet")
        end
    end
    return globs
end

cells  = ["8206d7fffffffff"]
globs  = parquet_globs(cells, start_year=2020, end_year=2022)

# Build a SQL list literal:  ['s3://...', 's3://...']
paths_sql = "[" * join(repr.(globs), ", ") * "]"
```

> **Alternative — read all years for a cell and let DuckDB skip via predicate
> pushdown:**
>
> ```julia
> paths_sql = "[$(join(repr.([\"$WAREHOUSE/grid_partition=$c/*.parquet\" for c in cells]), \", \"))]"
> ```
>
> This is simpler but opens more files. Explicit year enumeration is faster for
> narrow time windows.

---

## Step 3 — Query with spatial filter

```julia
using DuckDB, DataFrames

con = DBInterface.connect(DuckDB.DB, ":memory:")
DBInterface.execute(con, "INSTALL httpfs;   LOAD httpfs;")
DBInterface.execute(con, "INSTALL spatial;  LOAD spatial;")
DBInterface.execute(con, "SET s3_region = 'us-west-2';")

lat, lon   = 58.74, -133.99
start_date = "2020-01-01"
end_date   = "2022-12-31"

# paths_sql from Step 2
query = """
    SELECT
        id,
        platform,
        datetime,
        percent_valid_pixels
    FROM read_parquet($paths_sql, hive_partitioning = true)
    WHERE datetime >= TIMESTAMPTZ '$start_date'
      AND datetime <= TIMESTAMPTZ '$(end_date)T23:59:59Z'
      AND ST_Intersects(
            geometry,
            ST_GeomFromText('POINT($lon $lat)')
          )
    ORDER BY datetime
    LIMIT 50
"""

df = DBInterface.execute(con, query) |> DataFrame
println(df)
```

---

## Complete Self-Contained Example

Point query over south-central Alaska, 2020–2022:

```julia
using DuckDB, DataFrames, H3.API

# ── 1. H3 pruning ──────────────────────────────────────────────────────────
lat, lon = 58.74, -133.99
cell     = latLngToCell(LatLng(deg2rad(lat), deg2rad(lon)), 1)
cells    = [string(cell, base=16)]

const WAREHOUSE = "s3://its-live-data/test-space/stac/catalog/warehouse"
start_year, end_year = 2020, 2022

globs = [
    "$WAREHOUSE/grid_partition=$c/year=$y/*.parquet"
    for c in cells
    for y in start_year:end_year
]
paths_sql = "[" * join(repr.(globs), ", ") * "]"

# ── 2. DuckDB query ────────────────────────────────────────────────────────
con = DBInterface.connect(DuckDB.DB, ":memory:")
for ext in ("httpfs", "spatial")
    DBInterface.execute(con, "INSTALL $ext; LOAD $ext;")
end
DBInterface.execute(con, "SET s3_region = 'us-west-2';")

df = DBInterface.execute(con, """
    SELECT id, platform, datetime, percent_valid_pixels
    FROM   read_parquet($paths_sql, hive_partitioning = true)
    WHERE  datetime >= TIMESTAMPTZ '2020-01-01'
      AND  datetime <= TIMESTAMPTZ '2022-12-31T23:59:59Z'
      AND  ST_Intersects(geometry, ST_GeomFromText('POINT($lon $lat)'))
    ORDER BY datetime
    LIMIT 50
""") |> DataFrame

println(df)
DBInterface.close!(con)
```

---

## Bounding Box Query (no geometry parsing)

Faster than `ST_Intersects` for large result sets — uses column statistics for
row-group skipping:

```julia
west, south, east, north = -140.0, 55.0, -130.0, 62.0

cells = cells_for_bbox(south, west, north, east)  # from Step 1
globs = [
    "$WAREHOUSE/grid_partition=$c/year=$y/*.parquet"
    for c in cells for y in 2020:2022
]
paths_sql = "[" * join(repr.(globs), ", ") * "]"

df = DBInterface.execute(con, """
    SELECT id, platform, datetime
    FROM   read_parquet($paths_sql, hive_partitioning = true)
    WHERE  ST_XMin(geometry) >= $west
      AND  ST_XMax(geometry) <= $east
      AND  ST_YMin(geometry) >= $south
      AND  ST_YMax(geometry) <= $north
      AND  datetime >= TIMESTAMPTZ '2020-01-01'
      AND  datetime <= TIMESTAMPTZ '2022-12-31T23:59:59Z'
    ORDER BY datetime
""") |> DataFrame
```

---

## Time Series Aggregation

Count scenes per month at a single location:

```julia
df = DBInterface.execute(con, """
    SELECT
        DATE_TRUNC('month', datetime) AS month,
        COUNT(*)                      AS scenes,
        AVG(percent_valid_pixels)     AS avg_valid_pct
    FROM read_parquet($paths_sql, hive_partitioning = true)
    WHERE ST_Intersects(geometry, ST_GeomFromText('POINT($lon $lat)'))
      AND datetime BETWEEN TIMESTAMPTZ '2020-01-01'
                       AND TIMESTAMPTZ '2022-12-31T23:59:59Z'
    GROUP BY DATE_TRUNC('month', datetime)
    ORDER BY month
""") |> DataFrame
```

---

## Get Arrow Output

If you need zero-copy interop with other Julia packages (e.g., GeoDataFrames.jl):

```julia
using Arrow

result     = DBInterface.execute(con, "SELECT id, datetime FROM read_parquet($paths_sql) LIMIT 1000")
arrow_buf  = Arrow.tobuffer(result)   # serialize to Arrow IPC in memory
tbl        = Arrow.Table(arrow_buf)   # deserialize as columnar Arrow.Table

# Arrow.Table is column-oriented — access columns directly:
tbl.id
tbl.datetime
```

---

## Schema Reference

Key columns available in every partition file:

| Column | Type | Notes |
|---|---|---|
| `id` | `String` | STAC item ID |
| `geometry` | `WKB Blob` | Footprint in WKB; use `ST_GeomFromWKB()` or DuckDB spatial auto-detection |
| `datetime` | `TIMESTAMPTZ` | Scene acquisition time |
| `platform` | `String` | e.g. `"sentinel-2"`, `"landsat-8"` |
| `percent_valid_pixels` | `Int64` | 0–100 |
| `date_dt` | `Int64` | Days since epoch (integer form of `datetime`) |
| `assets` | `String` | JSON-encoded asset dictionary |
| `links` | `String` | JSON-encoded links array |
| `grid_partition` | `String` | H3 cell ID (Hive partition column) |
| `year` | `Int32` | Year (Hive partition column) |

> `assets` and `links` are stored as JSON strings. Parse them in DuckDB with
> `json_extract(assets, '$.B04.href')` or hydrate in Julia with `JSON3.read`.

---

## Performance Tips

1. **Always prune by H3 cell first** — the catalog has 5,024 files; a point
   query at resolution 1 typically opens ≤ 10 files.
2. **Add year bounds** — the `year` Hive column enables DuckDB to skip entire
   year directories at the filesystem level.
3. **Prefer bbox filters over `ST_Intersects`** for large result sets — avoids
   geometry deserialisation on every row.
4. **Use `hive_partitioning = true`** in `read_parquet` so DuckDB can push
   `grid_partition` and `year` predicates down without reading file footers.
5. **Reuse the connection** — extension loading (`INSTALL / LOAD`) is per-session;
   keep `con` alive across queries in a notebook or script.

---

## Comparison with Python Client

| Feature | Julia (DuckDB.jl) | Python (earthcatalog) |
|---|---|---|
| H3 pruning | Manual (`H3.jl`) | `info.file_paths()` |
| SQL query | `DBInterface.execute` | `duckdb.connect().execute()` |
| DataFrame output | `\|> DataFrame` | `.df()` |
| Arrow output | Via `Arrow.tobuffer` | `.arrow()` (zero-copy) |
| STAC hydration | `JSON3.read(row.assets)` | `pystac.Item.from_dict()` |
| CQL2 filters | Not available (use SQL) | `cql2` + `rustac.DuckdbClient` |
