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

DuckDB's parallel I/O gives the biggest win when items are **spread
across many files** (sparse queries across wide temporal ranges).
For dense queries (abundant items in every file) or narrow queries
(few files), `search()` matches `duck_search()` because rustac's
sequential scan is bottlenecked by the same S3 download time anyway.
`search_to_arrow()` is similar to `search()` since both use rustac.
