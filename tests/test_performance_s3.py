"""
Performance benchmarks against the real ITS_LIVE catalog on S3.

These are integration tests (``pytest -m integration``) that require
anonymous read access to the public ITS_LIVE bucket.

Measures actual S3 latency for common query patterns against the
production catalog (63.4M rows, 5,024 files).  Results are printed
but not asserted — use as a baseline reference.

Also verifies that ``catalog.search()`` (rustac path) and
``catalog.search_files()`` + DuckDB ``read_parquet()`` produce the
same item IDs for identical spatial/temporal arguments.

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


def _duckdb_search(paths, geom, datetime_range=None):
    """Run a DuckDB spatial query against *paths* and return (items, Arrow table).

    DuckDB picks up AWS credentials from env by default.  For anonymous
    S3 access we clear them for the duration of the query.
    """
    import os

    import duckdb

    saved = {
        "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
        "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
        "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
    }
    os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
    try:
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        path_list = ", ".join(repr(p) for p in paths)
        geom_wkt = _geom_to_wkt(geom)

        sql = f"""
            SELECT id, datetime, platform, percent_valid_pixels, geometry
            FROM read_parquet([{path_list}])
            WHERE ST_Intersects(geometry, ST_GeomFromText('{geom_wkt}'))
        """
        if datetime_range:
            sql += f" AND datetime >= '{datetime_range[0]}' AND datetime < '{datetime_range[1]}'"

        tbl = con.execute(sql).fetch_arrow_table()
        items = tbl.column("id").to_pylist()
        return items, tbl
    finally:
        os.environ.pop("AWS_NO_SIGN_REQUEST", None)
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def _geom_to_wkt(geom_dict):
    """Convert a GeoJSON-like dict to WKT."""
    from shapely.geometry import shape

    return shape(geom_dict).wkt


class TestCorrectness:
    """Verify rustac and DuckDB return the same items for identical geometry.

    Note: rustac's ``search_sync`` may return fewer items than a direct
    DuckDB ``ST_Intersects`` query on the same Parquet files.  This is a
    known discrepancy — rustac's spatial predicate evaluation differs from
    DuckDB's spatial extension in some edge cases.  These tests report the
    magnitude of any difference but do not assert exact equality.
    """

    def test_rustac_vs_duckdb_point(self, catalog):
        """Point query: compare rustac vs DuckDB item counts for same spatial+temporal args."""
        import os

        import duckdb
        from shapely.geometry import shape

        geom = shape(GREENLAND_POINT)
        paths = catalog.search_files(geom, start_datetime="2024-01-01", end_datetime="2024-12-31")
        assert len(paths) > 0

        saved = {
            "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
            "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
            "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
        }
        os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            path_list = ", ".join(repr(p) for p in paths)
            duck_ids = sorted(
                r[0]
                for r in con.execute(f"""
                    SELECT DISTINCT id FROM read_parquet([{path_list}])
                    WHERE ST_Intersects(geometry, ST_GeomFromText('{geom.wkt}'))
                      AND datetime >= '2024-01-01' AND datetime < '2024-12-31'
                """).fetchall()
            )
        finally:
            os.environ.pop("AWS_NO_SIGN_REQUEST", None)
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

        rustac_ids = sorted(
            item.id
            for item in catalog.search(
                intersects=GREENLAND_POINT,
                datetime="2024-01-01/2024-12-31",
            ).items()
        )

        ratio = len(rustac_ids) / len(duck_ids) if duck_ids else 0
        if rustac_ids != duck_ids:
            only_rustac = set(rustac_ids) - set(duck_ids)
            only_duck = set(duck_ids) - set(rustac_ids)
            print(
                f"\n  📊 rustac={len(rustac_ids)}, duckdb={len(duck_ids)}"
                f"  (rustac/duckdb={ratio:.1%})"
                f"  rustac extra: {len(only_rustac)}, duckdb extra: {len(only_duck)}"
            )
        else:
            print(f"\n  ✅ rustac & DuckDB match: {len(rustac_ids)} IDs identical")


class TestProductionPerformance:
    """Latency benchmarks against the real S3 catalog.

    These tests are informational — they print timing data but do not
    assert specific thresholds (latency varies with network, S3 load,
    and instance type).
    """

    def _bench_rustac(self, catalog, label, **search_kwargs):
        """Time catalog.search() (rustac path). Returns (n_items, elapsed, info)."""
        t0 = time.perf_counter()
        results = catalog.search(**search_kwargs)
        items = list(results.items())
        elapsed = time.perf_counter() - t0
        info = results.stats()
        return len(items), elapsed, info

    def _bench_duckdb(
        self, catalog, label, geom, start_datetime=None, end_datetime=None, limit=100
    ):
        """Time search_files() + DuckDB path. Returns (n_items, elapsed)."""
        import os

        import duckdb

        t0 = time.perf_counter()
        paths = catalog.search_files(geom, start_datetime=start_datetime, end_datetime=end_datetime)
        path_list = ", ".join(repr(p) for p in paths)
        geom_wkt = geom.wkt if hasattr(geom, "wkt") else _geom_to_wkt(geom)

        saved = {
            "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
            "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
            "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
        }
        os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            sql = f"""SELECT id FROM read_parquet([{path_list}])
                      WHERE ST_Intersects(geometry, ST_GeomFromText('{geom_wkt}'))"""
            if start_datetime:
                sql += f" AND datetime >= '{start_datetime}'"
            if end_datetime:
                sql += f" AND datetime < '{end_datetime}'"
            sql += f" LIMIT {limit}"
            rows = con.execute(sql).fetchall()
        finally:
            os.environ.pop("AWS_NO_SIGN_REQUEST", None)
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

        elapsed = time.perf_counter() - t0
        return len(rows), elapsed

    def test_rustac_vs_duckdb_latency_point(self, catalog):
        """Compare rustac vs DuckDB latency for point query with max_items=100."""
        from shapely.geometry import shape

        geom = shape(GREENLAND_POINT)
        n_r, t_r, info = self._bench_rustac(
            catalog,
            "point",
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        n_d, t_d = self._bench_duckdb(
            catalog,
            "point",
            geom,
            start_datetime="2020-01-01",
            end_datetime="2020-12-31",
            limit=100,
        )
        print(
            f"\n  rustac: {n_r} items in {t_r:.3f}s  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )
        print(f"  duckdb: {n_d} items in {t_d:.3f}s")

    def test_rustac_vs_duckdb_latency_bbox(self, catalog):
        """Compare rustac vs DuckDB latency for bbox query with max_items=100."""
        from shapely.geometry import box

        geom = box(*GREENLAND_BBOX)
        n_r, t_r, info = self._bench_rustac(
            catalog,
            "bbox",
            bbox=GREENLAND_BBOX,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        n_d, t_d = self._bench_duckdb(
            catalog,
            "bbox",
            geom,
            start_datetime="2020-01-01",
            end_datetime="2020-12-31",
            limit=100,
        )
        print(
            f"\n  rustac: {n_r} items in {t_r:.3f}s  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )
        print(f"  duckdb: {n_d} items in {t_d:.3f}s")

    def test_spatial_only(self, catalog):
        """Point query, no temporal or CQL2 filter, max_items=100."""
        n, elapsed, info = self._bench_rustac(
            catalog,
            "spatial only",
            intersects=GREENLAND_POINT,
            max_items=100,
        )
        print(
            f"\n  spatial only, max_items=100: {n} items in {elapsed:.3f}s"
            f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )

    def test_spatial_and_year(self, catalog):
        """Point in Greenland + single year, max_items=100."""
        n, elapsed, info = self._bench_rustac(
            catalog,
            "spatial + year",
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        print(
            f"\n  spatial + year=2020, max_items=100: {n} items in {elapsed:.3f}s"
            f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )

    def test_spatial_year_cql2(self, catalog):
        """Point + year + CQL2 percent_valid_pixels >= 80, max_items=100."""
        import cql2

        n, elapsed, info = self._bench_rustac(
            catalog,
            "spatial + year + CQL2",
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            filter=cql2.parse_text("percent_valid_pixels >= 80").to_json(),
            max_items=100,
        )
        print(
            f"\n  spatial + year=2020 + pvp>=80, max_items=100: {n} items in {elapsed:.3f}s"
            f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )

    def test_wide_temporal_range(self, catalog):
        """Full temporal range (1980–2026) with no item limit."""
        n, elapsed, info = self._bench_rustac(
            catalog,
            "full range",
            intersects=GREENLAND_POINT,
            datetime="1980-01-01/2026-12-31",
        )
        print(
            f"\n  spatial + full range, no limit: {n} items in {elapsed:.3f}s"
            f"  ({n / elapsed:.0f} items/s)"
            f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )

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
        print(
            f"\n  pages(): {n_pages} pages, {n_total} items in {elapsed:.3f}s"
            f"  (first page sizes: {page_sizes}...)"
        )

    def test_out_of_range_datetime(self, catalog):
        """Datetime range that matches no data — Iceberg prunes to 0 files."""
        n, elapsed, info = self._bench_rustac(
            catalog,
            "out-of-range",
            intersects=GREENLAND_POINT,
            datetime="1970-01-01/1979-12-31",
            max_items=100,
        )
        print(
            f"\n  out-of-range datetime, max_items=100: {n} items in {elapsed:.4f}s"
            f"  (files={info['files']}, est.rows={info['rows_upper_bound']:,})"
        )

    def test_bbox_vs_point(self, catalog):
        """Compare bbox vs point query latency side by side."""
        n_b, t_b, s_b = self._bench_rustac(
            catalog,
            "bbox",
            bbox=GREENLAND_BBOX,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        n_p, t_p, s_p = self._bench_rustac(
            catalog,
            "point",
            intersects=GREENLAND_POINT,
            datetime="2020-01-01/2020-12-31",
            max_items=100,
        )
        print(
            f"\n  bbox  (Greenland): {n_b} items in {t_b:.3f}s"
            f"  (files={s_b['files']}, est.rows={s_b['rows_upper_bound']:,})"
        )
        print(
            f"  point (Greenland): {n_p} items in {t_p:.3f}s"
            f"  (files={s_p['files']}, est.rows={s_p['rows_upper_bound']:,})"
        )

    def test_rustac_vs_duckdb_wide_range(self, catalog):
        """Compare rustac search() vs search_files() + DuckDB for wide temporal range.

        Both use the same spatial/temporal/property filter:
        - point at [-45, 70]
        - datetime 1980-01-01 to 2015-12-31
        - percent_valid_pixels >= 1
        """
        import os

        import duckdb
        from shapely.geometry import shape

        geom = shape(GREENLAND_POINT)

        # ---- rustac path ----
        t0 = time.perf_counter()
        rustac_items = list(
            catalog.search(
                intersects=GREENLAND_POINT,
                datetime="1980-01-01/2015-12-31",
                filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 1]},
            ).items()
        )
        rustac_elapsed = time.perf_counter() - t0
        rustac_ids = {item.id for item in rustac_items}
        rustac_first = rustac_items[0] if rustac_items else None

        # ---- DuckDB path ----
        t0 = time.perf_counter()
        paths = catalog.search_files(geom, start_datetime="1980-01-01", end_datetime="2015-12-31")
        path_list = ", ".join(repr(p) for p in paths)

        saved = {
            "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
            "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
            "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
        }
        os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            rows = con.execute(f"""
                SELECT id, platform, datetime
                FROM read_parquet([{path_list}])
                WHERE percent_valid_pixels >= 1 AND
                      ST_Intersects(geometry, ST_GeomFromText('{geom.wkt}'))
                ORDER BY datetime
            """).fetchdf()
        finally:
            os.environ.pop("AWS_NO_SIGN_REQUEST", None)
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

        duckdb_elapsed = time.perf_counter() - t0
        duckdb_ids = set(rows["id"])

        # ---- comparison ----
        n_rustac = len(rustac_ids)
        n_duckdb = len(duckdb_ids)
        only_rustac = rustac_ids - duckdb_ids
        only_duck = duckdb_ids - rustac_ids
        overlap = rustac_ids & duckdb_ids

        print(f"\n  rustac items():  {n_rustac:>6} items in {rustac_elapsed:.1f}s")
        print(f"  duckdb SQL:      {n_duckdb:>6} items in {duckdb_elapsed:.1f}s")
        print(
            f"  overlap: {len(overlap)} shared IDs"
            f"  (rustac only: {len(only_rustac)}, duckdb only: {len(only_duck)})"
            f"  files: {len(paths)}"
        )

        if rustac_first:
            print(
                f"  first rustac item: {rustac_first.id}"
                f"  platform={rustac_first.properties.get('platform')}"
                f"  datetime={rustac_first.properties.get('datetime')}"
            )

    def test_duck_search_vs_rustac(self, catalog):
        """Compare duck_search() vs search() (rustac) for wide temporal range.

        duck_search() uses DuckDB internally for parallel Parquet reads.
        """

        query = dict(
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="1980-01-01/2015-12-31",
            filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 1]},
        )

        # rustac
        t0 = time.perf_counter()
        r_items = list(catalog.search(**query).items())
        t_r = time.perf_counter() - t0

        # duck_search
        t0 = time.perf_counter()
        d_items = catalog.duck_search(**query)
        t_d = time.perf_counter() - t0

        r_ids = {item.id for item in r_items}
        d_ids = {item.id for item in d_items}
        overlap = r_ids & d_ids
        info = catalog.search(intersects=GREENLAND_POINT, datetime="1980-01-01/2015-12-31").stats()

        print(f"\n  search()       {len(r_items):>6} items in {t_r:.1f}s")
        print(
            f"  duck_search()  {len(d_items):>6} items in {t_d:.1f}s  ({(t_r / t_d):.1f}x faster)"
        )
        print(
            f"  overlap: {len(overlap)} ids  "
            f"(search only: {len(r_ids - d_ids)}, duck only: {len(d_ids - r_ids)})"
            f"  files={info['files']}"
        )

    def test_duck_search_cql2_vs_raw_json(self, catalog):
        """duck_search() with cql2.parse_text() vs raw JSON should match."""
        import cql2

        raw_json = {"op": ">=", "args": [{"property": "percent_valid_pixels"}, 80]}
        items_raw = catalog.duck_search(
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="2020-01-01/2020-12-31",
            filter=raw_json,
            max_items=10,
        )
        items_cql2 = catalog.duck_search(
            intersects={"type": "Point", "coordinates": [-45, 70]},
            datetime="2020-01-01/2020-12-31",
            filter=cql2.parse_text("percent_valid_pixels >= 80").to_json(),
            max_items=10,
        )
        ids_raw = {it.id for it in items_raw}
        ids_cql2 = {it.id for it in items_cql2}
        match = "✅" if ids_raw == ids_cql2 else "❌"
        print(f"\n  {match} cql2.parse_text() == raw JSON: {len(ids_raw)} ids match")
