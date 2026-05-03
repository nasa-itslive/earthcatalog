"""Tests for _FileSearchEngine and EarthCatalogItemSearch."""

from __future__ import annotations

from earthcatalog.search import (
    EarthCatalogItemSearch,
    _extract_datetime_range,
    _extract_geometry,
    _FileSearchEngine,
)


class TestExtractGeometry:
    def test_intersects_to_geometry(self):
        from shapely.geometry import shape

        g = _extract_geometry(intersects={"type": "Point", "coordinates": [0, 60]})
        assert g is not None
        assert g.equals(shape({"type": "Point", "coordinates": [0, 60]}))

    def test_bbox_to_box(self):
        from shapely.geometry import box

        g = _extract_geometry(bbox=[-10, 50, 10, 70])
        assert g is not None
        assert g.equals(box(-10, 50, 10, 70))

    def test_no_spatial(self):
        assert _extract_geometry(datetime="2020") is None


class TestExtractDatetime:
    def test_range(self):
        s, e = _extract_datetime_range(datetime="2020-01-01/2020-12-31")
        assert s == "2020-01-01"
        assert e == "2020-12-31"

    def test_open_start(self):
        s, e = _extract_datetime_range(datetime="../2020-12-31")
        assert s is None
        assert e == "2020-12-31"

    def test_open_end(self):
        s, e = _extract_datetime_range(datetime="2020-01-01/..")
        assert s == "2020-01-01"
        assert e is None

    def test_no_datetime(self):
        s, e = _extract_datetime_range(intersects={"type": "Point", "coordinates": [0, 0]})
        assert s is None
        assert e is None


class TestFileSearchEngine:
    def test_prune_called_with_geom_and_datetime(self, monkeypatch):
        calls = []

        def fake_prune(geom, start_datetime=None, end_datetime=None):
            calls.append((geom, start_datetime, end_datetime))
            return ["file.parquet"]

        eng = _FileSearchEngine(prune_fn=fake_prune)
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [])

        eng.search(intersects={"type": "Point", "coordinates": [0, 60]}, datetime="2020-01-01/2020-12-31")
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

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}"} for _ in range(5)])
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
        sr = EarthCatalogItemSearch(params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng)
        results = list(sr.items_as_dicts())
        assert len(results) == 2
        assert results[0]["id"] == "item-a.parquet"

    def test_items(self, monkeypatch):
        """items() yields pystac.Item objects."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{
            "id": "test-id",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [0, 60]},
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [],
            "assets": {},
        }])
        sr = EarthCatalogItemSearch(params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng)
        items = list(sr.items())
        assert len(items) == 1
        assert items[0].id == "test-id"

    def test_iter_backward_compat(self, monkeypatch):
        """list(search) works (backward compat)."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": "item"}])
        sr = EarthCatalogItemSearch(params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng)
        assert list(sr) == [{"id": "item"}]

    def test_pages(self, monkeypatch):
        """Each file produces one page."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        def fake(href, **kw):
            return [{"id": f"item-{href}-{i}"} for i in range(2)]

        monkeypatch.setattr(rustac, "search_sync", fake)
        sr = EarthCatalogItemSearch(params={"intersects": {"type": "Point", "coordinates": [0, 60]}}, engine=eng)
        pages = list(sr.pages())
        assert len(pages) == 2
        assert len(pages[0]) == 2
        assert pages[0][0]["id"] == "item-a.parquet-0"

    def test_max_items_in_items_as_dicts(self, monkeypatch):
        """max_items stops iteration early."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet", "b.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{"id": f"item-{href}-{i}"} for i in range(5)])
        sr = EarthCatalogItemSearch(params={"intersects": {"type": "Point", "coordinates": [0, 60]}, "max_items": 3}, engine=eng)
        results = list(sr.items_as_dicts())
        assert len(results) == 3

    def test_matched_returns_none_without_table(self, monkeypatch):
        """Without an Iceberg table, matched() returns None."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        sr = EarthCatalogItemSearch(params={}, engine=eng)
        assert sr.matched() is None

    def test_get_parameters(self):
        """get_parameters returns a copy of the params."""
        eng = _FileSearchEngine()
        sr = EarthCatalogItemSearch(params={"max_items": 5, "collections": ["test"]}, engine=eng)
        params = sr.get_parameters()
        assert params == {"max_items": 5, "collections": ["test"]}
        params["max_items"] = 999  # should not affect original
        assert sr._params["max_items"] == 5

    def test_repr_shows_params(self):
        eng = _FileSearchEngine()
        sr = EarthCatalogItemSearch(params={"max_items": 10, "collections": ["test"], "bbox": [-120, 35, -119, 36]}, engine=eng)
        r = repr(sr)
        assert "EarthCatalogItemSearch" in r
        assert "max_items=10" in r
        assert "collections=" in r

    def test_stats_returns_dict_without_table(self):
        """Without Iceberg table, stats() returns None."""
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["f.parquet"])
        sr = EarthCatalogItemSearch(params={}, engine=eng)
        assert sr.stats() is None

    def test_html_repr_contains_params(self):
        eng = _FileSearchEngine()
        sr = EarthCatalogItemSearch(params={"max_items": 10, "bbox": [-120, 35, -119, 36]}, engine=eng)
        html = sr._repr_html_()
        assert "EarthCatalogItemSearch" in html
        assert "max_items" in html
        assert "-120" in html


class TestSearchToArrow:
    def test_returns_table(self, monkeypatch):
        eng = _FileSearchEngine(prune_fn=lambda geom, **kw: ["a.parquet"])
        import rustac

        monkeypatch.setattr(rustac, "search_sync", lambda href, **kw: [{
            "id": "test",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [0, 60]},
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [],
            "assets": {},
        }])
        result = eng.search_to_arrow(intersects={"type": "Point", "coordinates": [0, 60]})
        assert result.num_rows == 1
