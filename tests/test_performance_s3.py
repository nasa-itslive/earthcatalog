"""
Performance benchmarks against the real ITS_LIVE catalog on S3.

These are integration tests (``pytest -m integration``) that require
anonymous read access to the public ITS_LIVE bucket.

Measures actual S3 latency for common query patterns against the
production catalog (63.4M rows, 5,024 files).  Results are printed
but not asserted — use as a baseline reference.

Catalog: s3://its-live-data/test-space/stac/catalog
"""

from __future__ import annotations

import time

import pytest
from obstore.store import S3Store

BUCKET = "its-live-data"
BASE = "s3://its-live-data/test-space/stac/catalog"

# Common query geometries
GREENLAND_POINT = {"type": "Point", "coordinates": [-45, 70]}
GREENLAND_BBOX = [-60, 60, -20, 85]
ALASKA_POINT = {"type": "Point", "coordinates": [-149.5, 63.5]}


@pytest.fixture(scope="module")
def catalog():
    import earthcatalog as ec

    store = S3Store(bucket=BUCKET, region="us-west-2", skip_signature=True)
    return ec.open(store=store, base=BASE)


class TestProductionPerformance:
    """Latency benchmarks against the real S3 catalog.

    These tests are informational — they print timing data but do not
    assert specific thresholds (latency varies with network, S3 load,
    and instance type).
    """

    def test_spatial_only(self, catalog):
        """Point query, no temporal or CQL2 filter, max_items=100."""
        t0 = time.perf_counter()
        results = catalog.search(intersects=GREENLAND_POINT, max_items=100)
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        n = len(items)
        info = results.stats()
        print(f"\n  spatial only, max_items=100: {n} items in {elapsed:.3f}s"
              f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})")

    def test_spatial_and_year(self, catalog):
        """Point in Greenland + single year, max_items=100."""
        t0 = time.perf_counter()
        results = catalog.search(
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        n = len(items)
        info = results.stats()
        print(f"\n  spatial + year=2020, max_items=100: {n} items in {elapsed:.3f}s"
              f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})")

    def test_spatial_year_cql2(self, catalog):
        """Point + year + CQL2 percent_valid_pixels >= 80, max_items=100."""
        import cql2

        t0 = time.perf_counter()
        results = catalog.search(
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            filter=cql2.parse_text("percent_valid_pixels >= 80").to_json(),
            max_items=100,
        )
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        n = len(items)
        info = results.stats()
        print(f"\n  spatial + year=2020 + pvp>=80, max_items=100: {n} items in {elapsed:.3f}s"
              f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})")

    def test_wide_temporal_range(self, catalog):
        """Full temporal range (1980–2026) with no item limit.

        This exercises the sequential per-file scan across many years.
        """
        t0 = time.perf_counter()
        results = catalog.search(
            intersects=GREENLAND_POINT,
            datetime="1980-01-01/2026-12-31",
        )
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        n = len(items)
        info = results.stats()
        print(f"\n  spatial + full range, no limit: {n} items in {elapsed:.3f}s"
              f"  ({n/elapsed:.0f} items/s)"
              f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})")

    def test_pages(self, catalog):
        """Materialize via pages(), measure per-page breakdown."""
        results = catalog.search(
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
        )
        t0 = time.perf_counter()
        pages = list(results.pages())
        elapsed = time.perf_counter() - t0
        n_pages = len(pages)
        n_total = sum(len(p) for p in pages)
        page_sizes = [len(p) for p in pages[:5]]
        print(f"\n  pages(): {n_pages} pages, {n_total} items in {elapsed:.3f}s"
              f"  (first page sizes: {page_sizes}...)")

    def test_out_of_range_datetime(self, catalog):
        """Datetime range that matches no data — should be fast (Iceberg prunes to 0 files).

        The real catalog has items from ~2016 onward (no data before 1980).
        """
        t0 = time.perf_counter()
        results = catalog.search(
            intersects=GREENLAND_POINT,
            datetime="1970-01-01/1979-12-31",
            max_items=100,
        )
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        n = len(items)
        info = results.stats()
        print(f"\n  out-of-range datetime, max_items=100: {n} items in {elapsed:.4f}s"
              f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})")

    def test_bbox_vs_point(self, catalog):
        """Compare bbox vs point query — bbox should prune more files."""
        t0 = time.perf_counter()
        r_bbox = catalog.search(bbox=GREENLAND_BBOX, datetime="2020-01-01/2020-12-31", max_items=100)
        items_bbox = list(r_bbox.items())
        t_bbox = time.perf_counter() - t0
        s_bbox = r_bbox.stats()

        t0 = time.perf_counter()
        r_point = catalog.search(intersects=GREENLAND_POINT, datetime="2020-01-01/2020-12-31", max_items=100)
        items_point = list(r_point.items())
        t_point = time.perf_counter() - t0
        s_point = r_point.stats()

        print(f"\n  bbox  (Greenland): {len(items_bbox)} items in {t_bbox:.3f}s"
              f"  (files={s_bbox['files']}, est.rows={s_bbox['rows_upper_bound']:,})")
        print(f"  point (Greenland): {len(items_point)} items in {t_point:.3f}s"
              f"  (files={s_point['files']}, est.rows={s_point['rows_upper_bound']:,})")
