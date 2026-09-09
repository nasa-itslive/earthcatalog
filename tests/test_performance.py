"""
Performance benchmarks for search queries with CQL2 filters.

Measures latency and item throughput for different filter selectivity
against an Iceberg-pruned warehouse.

DuckDB Parquet predicate pushdown
----------------------------------
DuckDB's Parquet reader uses column chunk statistics (min, max, null count)
from the Parquet footer to skip irrelevant row groups *before* decompressing
any data.  For a filter like ``percent_valid_pixels >= 1``:

- DuckDB reads the Parquet footer (metadata only, ~1-10 KB per file)
- For each row group, checks if ``stats.max >= 1``
- Skips row groups whose max is below the threshold
- For qualifying row groups, reads only the columns referenced in the
  query + filter columns (``SELECT *`` = all columns)

With ``percent_valid_pixels <= 50``:
- Skips row groups whose min > 50
- If most row groups have min > 50 (high-quality data), most are skipped

For the ``>= 1`` case nearly all row groups qualify (only truly empty
or zero-value groups are skipped), so DuckDB reads most of the data.

Note: ``percent_valid_pixels`` is stored as ``LongType`` (int64) with
Parquet statistics enabled.  Single-row-group files (common at this
scale) are either fully read or fully skipped — no partial row-group
skipping within a file.

Real-world extrapolation (ITS_LIVE catalog)
--------------------------------------------
Production catalog: 63.4M rows, 5,024 files, ~12,600 items/file.

The benchmark above uses 4 files, 250 items/file (= 1,000 items total).
With sequential per-file processing, total latency scales linearly with
the number of files read — not with the total catalog size.

Northern Greenland estimate (H3 resolution 1, cells ~842H+ etc.):

  Scenario                              Files after   Est. latency   Est. items
                                         pruning      (sequential)    returned
  ────────────────────────────────────────────────────────────────────────────
  Spatial only, no filter                    ~80        80×0.115s ≈ 9s      1M
  + year=2020                                ~10        10×0.115s ≈ 1s    126K
  + year=2020 + max_items=100                 ~1         0.12s             100
  + year=2020 + pvp>=1 + max_items=100        ~1         0.13s             100
  + year=2020 + pvp>=95 + max_items=100       ~2-3       0.3-0.4s         ~54

  Per-file latency ~0.115s (from benchmark: 0.459s / 4 files).
  Spatial pruning: H3 cell filters via Iceberg ``In(grid_partition, ...)``.
  Temporal pruning: Iceberg ``year`` partition filter.

Key insight: ``max_items=N`` reads at most *one file* when the first file
already contains N matching items.  Since warehouse files average ~12,600
items, a ``max_items=100`` query always stops after the first file regardless
of filter selectivity — provided the filter matches at least 100 rows in
that file.  This makes bounded searches consistently fast (~0.1-0.2s).

Without ``max_items`` the latency is proportional to the number of pruned
files, not the total catalog size.  A full-year scan of a single H3 cell
(~10 files, ~126K items) takes ~1s.
"""

from __future__ import annotations

import time

import pytest

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_warehouse(tmp_path, n_items: int = 1000, n_years: int = 4):
    """Create an Iceberg warehouse with *n_items* across *n_years*.

    Items are spread across H3 cells and years, with ``percent_valid_pixels``
    uniformly distributed from 0 to 100.
    """
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    items = [
        {
            "id": f"perf-{i:06d}",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-60, 60], [-20, 60], [-20, 80], [-60, 80], [-60, 60]]],
            },
            "properties": {
                "datetime": f"202{i % n_years + 1}-06-15T00:00:00Z",
                "platform": "sentinel-1" if i % 2 == 0 else "sentinel-2",
                "percent_valid_pixels": i % 101,
            },
            "links": [],
            "assets": {},
        }
        for i in range(n_items)
    ]

    p = H3Partitioner(resolution=2)
    rows = fan_out(items, p)
    paths = []
    for (cell, year), group in group_by_partition(rows, p).items():
        out = str(tmp_path / f"part_{cell[:12]}_{year}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)
    return tbl


def _search(catalog_table, **kwargs):
    """Run a rustac search and return (items, elapsed_seconds)."""
    from earthcatalog.search import EarthCatalogItemSearch, _FileSearchEngine

    engine = _FileSearchEngine(prune_fn=_make_prune_fn(catalog_table))
    sr = EarthCatalogItemSearch(params=kwargs, engine=engine, table=catalog_table)
    t0 = time.perf_counter()
    items = list(sr.items())
    elapsed = time.perf_counter() - t0
    return items, elapsed


def _make_prune_fn(table):
    from earthcatalog.catalog import _catalog_info

    info = _catalog_info(table)

    def _prune(geom, start_datetime=None, end_datetime=None):
        return info.file_paths(
            table, geom, start_datetime=start_datetime, end_datetime=end_datetime
        )

    return _prune


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSearchPerformance:
    """Benchmark search latency and throughput with varying filter selectivity."""

    @pytest.fixture(scope="class")
    def warehouse(self, tmp_path_factory):
        tmp_path = tmp_path_factory.mktemp("perf_warehouse")
        return _build_warehouse(tmp_path, n_items=1000, n_years=4)

    def test_no_filter(self, warehouse):
        """Baseline: no filter, max_items=100."""
        items, elapsed = _search(
            warehouse,
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2026-12-31",
            max_items=100,
        )
        assert len(items) == 100
        print(
            f"\n  no filter, max_items=100: {len(items)} items in {elapsed:.3f}s "
            f"({len(items) / elapsed:.0f} items/s)"
        )

    def test_filter_high_selectivity(self, warehouse):
        """percent_valid_pixels >= 1 — nearly all rows qualify."""
        items, elapsed = _search(
            warehouse,
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2026-12-31",
            filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 1]},
            max_items=100,
        )
        assert len(items) == 100
        print(
            f"\n  percent_valid_pixels >= 1, max_items=100: {len(items)} items in {elapsed:.3f}s "
            f"({len(items) / elapsed:.0f} items/s)"
        )

    def test_filter_low_selectivity(self, warehouse):
        """percent_valid_pixels <= 50 — ~50% of rows qualify."""
        items, elapsed = _search(
            warehouse,
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2026-12-31",
            filter={"op": "<=", "args": [{"property": "percent_valid_pixels"}, 50]},
            max_items=100,
        )
        assert len(items) == 100
        print(
            f"\n  percent_valid_pixels <= 50, max_items=100: {len(items)} items in {elapsed:.3f}s "
            f"({len(items) / elapsed:.0f} items/s)"
        )

    def test_filter_highly_selective(self, warehouse):
        """percent_valid_pixels >= 95 — ~5% of rows qualify, may not reach max_items."""
        items, elapsed = _search(
            warehouse,
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2026-12-31",
            filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 95]},
            max_items=100,
        )
        print(
            f"\n  percent_valid_pixels >= 95, max_items=100: {len(items)} items in {elapsed:.3f}s "
            f"({len(items) / elapsed:.0f} items/s)"
            if items
            else "\n  percent_valid_pixels >= 95: 0 items (no matching rows)"
        )

    def test_no_limit(self, warehouse):
        """No max_items — read all matching items from pruned files."""
        items, elapsed = _search(
            warehouse,
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2026-12-31",
        )
        print(
            f"\n  no limit, no filter: {len(items)} items in {elapsed:.3f}s "
            f"({len(items) / elapsed:.0f} items/s)"
        )

    def test_paginated_search(self, warehouse):
        """Materialize via pages() — measure per-page latency."""
        from earthcatalog.search import EarthCatalogItemSearch, _FileSearchEngine

        engine = _FileSearchEngine(prune_fn=_make_prune_fn(warehouse))
        sr = EarthCatalogItemSearch(
            params={
                "intersects": {"type": "Point", "coordinates": [-45, 70]},
                "datetime": "1980-01-01/2026-12-31",
            },
            engine=engine,
            table=warehouse,
        )
        pages = list(sr.pages())
        n_pages = len(pages)
        n_total = sum(len(p) for p in pages)
        print(f"\n  pages(): {n_pages} pages, {n_total} items total")
