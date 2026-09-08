"""Tests for _FileSearchEngine and EarthCatalogItemSearch."""

from __future__ import annotations

import pytest

from earthcatalog.search import (
    EarthCatalogItemSearch,
    _extract_datetime_range,
    _extract_geometry,
    _FileSearchEngine,
    build_query,
)


class TestBuildQuery:
    def test_where_true_when_no_filters(self):
        sql = build_query(["s3://b/p1.parquet"])
        assert sql == "SELECT * FROM read_parquet(['s3://b/p1.parquet']) WHERE TRUE"

    def test_geometry_filter(self):
        from shapely.geometry import Point

        sql = build_query(["p.parquet"], geom=Point(0, 0))
        assert "ST_Intersects(geometry, ST_GeomFromText(" in sql

    def test_datetime_filters(self):
        sql = build_query(["p.parquet"], start_dt="2020-01-01", end_dt="2020-12-31")
        assert "datetime >= '2020-01-01'" in sql
        assert "datetime <= '2020-12-31'" in sql

    def test_cql2_filter(self):
        import cql2

        sql = build_query(["p.parquet"], raw_filter=cql2.parse_text("id = 'x'").to_json())
        assert "id" in sql

    def test_select_column_subset(self):
        sql = build_query(["p.parquet"], select="id, assets")
        assert sql.startswith("SELECT id, assets FROM read_parquet(")


class TestExtractGeometry:
    @pytest.mark.parametrize(
        "kwargs,check",
        [
            ({"intersects": {"type": "Point", "coordinates": [0, 60]}}, "Point"),
            ({"bbox": [-10, 50, 10, 70]}, "Polygon"),
            ({"datetime": "2020"}, None),
        ],
    )
    def test_extract_geometry(self, kwargs, check):
        g = _extract_geometry(**kwargs)
        if check is None:
            assert g is None
        else:
            assert g is not None


class TestExtractDatetime:
    @pytest.mark.parametrize(
        ("kwargs", "expected_start", "expected_end"),
        [
            ({"datetime": "2020-01-01/2020-12-31"}, "2020-01-01", "2020-12-31"),
            ({"datetime": "../2020-12-31"}, None, "2020-12-31"),
            ({"datetime": "2020-01-01/.."}, "2020-01-01", None),
            ({"intersects": {"type": "Point", "coordinates": [0, 0]}}, None, None),
        ],
    )
    def test_extract_datetime_range(self, kwargs, expected_start, expected_end):
        s, e = _extract_datetime_range(**kwargs)
        assert s == expected_start
        assert e == expected_end


class TestFileSearchEngine:
    def test_prune_called_with_geom_and_datetime(self, monkeypatch):
        calls = []

        def fake_prune(geom, start_datetime=None, end_datetime=None):
            calls.append((geom, start_datetime, end_datetime))
            return ["file.parquet"]

        eng = _FileSearchEngine(prune_fn=fake_prune)
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [])

        eng.search(
            intersects={"type": "Point", "coordinates": [0, 60]}, datetime="2020-01-01/2020-12-31"
        )
        assert len(calls) == 1
        assert calls[0][0] is not None
        assert calls[0][1] == "2020-01-01"
        assert calls[0][2] == "2020-12-31"

    def test_no_spatial_returns_empty(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["should-not-run"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: ["should-not-run"])
        assert eng.search(datetime="2020") == []

    def test_merges_results(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}"}])
        results = eng.search(intersects={"type": "Point", "coordinates": [0, 60]})
        assert len(results) == 2

    def test_max_items(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        monkeypatch.setattr(
            rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}"} for _ in range(5)]
        )
        results = eng.search(intersects={"type": "Point", "coordinates": [0, 60]}, max_items=3)
        assert len(results) == 3

    def test_empty_prune_returns_empty(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: [])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: ["should-not-run"])
        assert eng.search(intersects={"type": "Point", "coordinates": [0, 60]}) == []


class TestEarthCatalogItemSearch:
    """pystac_client-compatible lazy search result."""

    def test_returns_lazy(self):
        """Construction does not trigger prune or rustac."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        sr = EarthCatalogItemSearch(params={"max_items": 10}, engine=eng)
        assert sr._cached_files is None
        assert sr._cached_collection is None

    def test_items_as_dicts(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}"}])
        sr = EarthCatalogItemSearch(
            params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng
        )
        results = list(sr.items_as_dicts())
        assert len(results) == 2
        assert results[0]["id"] == "item-a.parquet"

    def test_items(self, monkeypatch):
        """items() yields pystac.Item objects."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet"])
        import rustac

        monkeypatch.setattr(
            rustac,
            "search_sync",
            lambda href, **kw: [
                {
                    "id": "test-id",
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "geometry": {"type": "Point", "coordinates": [0, 60]},
                    "properties": {"datetime": "2020-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            ],
        )
        sr = EarthCatalogItemSearch(
            params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng
        )
        items = list(sr.items())
        assert len(items) == 1
        assert items[0].id == "test-id"

    def test_iter_backward_compat(self, monkeypatch):
        """list(search) works (backward compat)."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": "item"}])
        sr = EarthCatalogItemSearch(
            params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng
        )
        assert list(sr) == [{"id": "item"}]

    def test_pages(self, monkeypatch):
        """Each file produces one page."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        def fake(href, **kw):
            return [{"id": f"item-{href}-{i}"} for i in range(2)]

        monkeypatch.setattr(rustac, "search_sync", fake)
        sr = EarthCatalogItemSearch(
            params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng
        )
        pages = list(sr.pages())
        assert len(pages) == 2
        assert len(pages[0]) == 2
        assert pages[0][0]["id"] == "item-a.parquet-0"

    def test_max_items_in_items_as_dicts(self, monkeypatch):
        """max_items stops iteration early."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        monkeypatch.setattr(
            rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}-{i}"} for i in range(5)]
        )
        sr = EarthCatalogItemSearch(
            params={"intersects": {"type": "Point", "coordinates": [0, 60]}, "max_items": 3},
            engine=eng,
        )
        results = list(sr.items_as_dicts())
        assert len(results) == 3

    @pytest.mark.parametrize(
        ("method", "expected"),
        [
            ("matched", None),
            ("stats", None),
        ],
    )
    def test_default_return_values(self, monkeypatch, method, expected):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        sr = EarthCatalogItemSearch(params={}, engine=eng)
        assert getattr(sr, method)() == expected

    def test_get_parameters(self):
        """get_parameters returns a copy of the params."""
        eng = _FileSearchEngine()
        sr = EarthCatalogItemSearch(params={"max_items": 5, "collections": ["test"]}, engine=eng)
        params = sr.get_parameters()
        assert params == {"max_items": 5, "collections": ["test"]}
        params["max_items"] = 999  # should not affect original
        assert sr._params["max_items"] == 5

    @pytest.mark.parametrize(
        ("method", "attr", "expected"),
        [
            ("repr", "__repr__", "EarthCatalogItemSearch"),
            ("html", "_repr_html_", "max_items"),
        ],
    )
    def test_repr_contains_params(self, method, attr, expected):
        eng = _FileSearchEngine()
        sr = EarthCatalogItemSearch(
            params={"max_items": 10, "bbox": [-120, 35, -119, 36]}, engine=eng
        )
        text = getattr(sr, attr)()
        assert expected in str(text)


class TestSearchToArrow:
    def test_returns_table(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet"])
        import rustac

        monkeypatch.setattr(
            rustac,
            "search_sync",
            lambda href, **kw: [
                {
                    "id": "test",
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "geometry": {"type": "Point", "coordinates": [0, 60]},
                    "properties": {"datetime": "2020-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            ],
        )
        result = eng.search_to_arrow(intersects={"type": "Point", "coordinates": [0, 60]})
        assert result.num_rows == 1
