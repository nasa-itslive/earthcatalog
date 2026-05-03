# Search Performance

Comparison of the search methods against the production catalog
(63.4M rows, 5,024 files, H3 resolution 1).  All times are from
isolated single-query runs against the real S3 catalog.

## Search methods

| Method | Engine | Returns | I/O |
|---|---|---|---|
| `search()` | rustac (DuckDB per file) | lazy `EarthCatalogItemSearch` → `pystac.Item` | Sequential |
| `duck_search(format="pystac")` | DuckDB `read_parquet` | eager `list[pystac.Item]` | Parallel |
| `duck_search(format="native")` | DuckDB `read_parquet` | eager `pandas.DataFrame` | Parallel |
| `search_to_arrow()` | rustac → Arrow | eager `pyarrow.Table` | Sequential |

## Dense region — small polygon, northern Greenland

The densest cell cluster.  Queries hit many files with abundant
matching items across 34+ years.

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| 1980–2017, no filter, max=100k | 55.5s | 57.0s | **26.7s** (2.1×) | 47.2s |
| 1980–2017, pvp>=1, max=100k | 53.9s | 56.6s | **28.8s** (1.9×) | 57.2s |
| 1980–2026, no filter, max=100k | 53.1s | 58.7s | **28.0s** (1.9×) | 46.2s |
| 1980–2026, pvp>=1, max=100k | — | — | — | — |

`duck_search(native)` is **~2× faster** across the board.  The pystac
conversion overhead (~25s) erases DuckDB's parallel-read advantage
for `duck_search(pystac)`, making it comparable to `search()`.

The `pvp>=1` filter has negligible effect — almost all items satisfy it.

## Sparse region — small polygon, Alaska

126 files after pruning, ~1.4M est. rows.  `pvp<50` is selective
(~5–10% of items satisfy it), forcing all methods to scan more rows
before hitting the 100k limit.

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| 1980–2026, no filter, max=100k | 62.0s | 60.0s | **30.3s** (2.0×) | 54.6s |
| 1980–2026, pvp<50, max=100k | 108.2s | 93.6s | **62.5s** (1.7×) | 99.1s |
| 1980–2026, date_dt>=50, max=100k | T/O | T/O | T/O | T/O |

`duck_search(native)` is consistently **~2× faster**.  The selective
`pvp<50` filter reduces the gap slightly because DuckDB must scan
all rows regardless of parallelism (the filter is applied after read).

The `date_dt>=50` query timed out (>10 min) with all methods — highly
selective filters over a wide temporal range can be expensive regardless
of approach.  Add a narrower temporal range or use `max_items` sparingly
for such cases.

## Sparse query — single point, 1980–2015

Few files per cell (32 files), each with sparse matching items.

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| Point Greenland, 1980–2015, pvp>=1, 642 items | **36.5s** | **4.1s** (8.9×) | **3.3s** (11×) | 33.6s |

DuckDB's advantage is maximized when items are spread thinly across
many files — it reads them in parallel while rustac reads them one
at a time.

## Narrow queries — year-targeted

Iceberg prunes to 2 files regardless of geometry size.  All methods
are bounded by the S3 download + scan time for one or two files
(~1s/file).

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| Point Greenland, year=2020, max=100 | 2.7s | 2.8s | 2.1s | 2.1s |
| + pvp>=80, max=100 | 2.2s | 2.2s | 2.0s | 2.2s |
| Bbox Greenland, year=2020, max=100 | 2.5s | 1.7s | 1.8s | 1.9s |
| Polygon Iceland, year=2020, max=100 | 2.2s | 1.5s | 1.4s | 1.6s |

## When to use each

| Scenario | Recommended |
|---|---|
| Interactive exploration, max_items=100 | `search()` — same speed, lazy |
| Wide query, need pystac Items | `search()` (lazy) or `duck_search(native)` + manual conversion |
| Wide query, fastest total time | `duck_search(format="native")` — 2× faster |
| PyArrow table output | `search_to_arrow()` |
| Raw SQL / aggregations / joins | `search_files()` + DuckDB directly |

## Key insight

`duck_search(format="native")` is consistently **~2× faster** across
all query types due to DuckDB's parallel I/O.  The `pystac` conversion
overhead (~25–30s for 100k items) erases this advantage, making
`duck_search(pystac)` comparable to `search()`.

DuckDB's parallelism gives the biggest win when items are **spread
across many files** (sparse queries across wide temporal ranges).
For a point query over 34 years with sparse matching items, the gain
reaches **8–11×** because DuckDB reads all files concurrently while
rustac reads them one at a time.

Selective filters (`pvp<50`, `date_dt>=50`) reduce the gain because
DuckDB must scan all rows regardless of parallelism — the filter is
applied after the read.  For extreme cases (highly selective + wide
temporal range), the query may time out with all methods.
