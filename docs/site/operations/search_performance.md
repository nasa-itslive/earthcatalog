# Search Performance

Comparison of the three search methods against the production catalog
(63.4M rows, 5,024 files, H3 resolution 1).

## Search methods

| Method | Engine | Returns | I/O |
|---|---|---|---|
| `search()` | rustac (DuckDB per file) | lazy `EarthCatalogItemSearch` → `pystac.Item` | Sequential per file |
| `duck_search()` | DuckDB `read_parquet` | eager `list[pystac.Item]` or `pandas.DataFrame` (with `format="native"`) | Parallel across files |
| `search_to_arrow()` | rustac → Arrow | eager `pyarrow.Table` | Sequential per file |

## Benchmarks

All times are against the real S3 catalog from a single machine.
Results vary with network, S3 region, and instance type.

### Narrow queries (2 files typically)

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| Point Greenland, year=2020, max=100 | 2.7s | 2.8s | 2.1s | 2.1s |
| + pvp>=80, max=100 | 2.2s | 2.2s | 2.0s | 2.2s |
| Bbox Greenland, year=2020, max=100 | 2.5s | 1.7s | 1.8s | 1.9s |
| Polygon Iceland, year=2020, max=100 | 2.2s | 1.5s | 1.4s | 1.6s |

All methods comparable for narrow queries — the bottleneck is S3
download + Parquet scan of the first file (~1s/file).

### Wide queries (32 files)

| Query | search | duck_search (pystac) | duck_search (native) | search_to_arrow |
|---|---|---|---|---|
| Point, 1980–2015, pvp>=1 (642 items) | **36.5s** | **4.1s** (8.9×) | **3.3s** (11×) | 33.6s |
| Bbox Greenland, 1980–2015, pvp>=1, 10k limit | 64.1s | ~42s | ~39s | 60.9s |

Results are from isolated single-query runs.  Batched benchmarks
can show inflated times due to cumulative S3 I/O congestion.

`duck_search()` is **8-11× faster** for queries spanning many sparse files
because DuckDB reads Parquet files in parallel while rustac processes
them sequentially.  The native format avoids pystac conversion overhead
(~1s per query).

## When to use each

| Scenario | Recommended method |
|---|---|
| Interactive exploration, max_items=100 | `search()` — same speed, lazy iteration |
| Wide temporal range, many files | `duck_search(format="native")` — fastest |
| Need pystac Items | `search()` (lazy) or `duck_search()` (eager, faster) |
| PyArrow table output | `search_to_arrow()` |
| Raw SQL / aggregations / joins | `search_files()` + DuckDB directly |

## Key insight

The 1s/file bottleneck is the Parquet read + rustac schema inference.
DuckDB's internal parallelism helps when many files must be scanned,
but for queries where the first file already satisfies `max_items`,
`search()` is equally fast with no wasted work.
