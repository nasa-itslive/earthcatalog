"""
Exploration: verify Iceberg pruning + per-file rustac search both work.

Builds a known warehouse with items at [0,60] and tests the exact query
pattern that the user reported (intersects + datetime + CQL2 filter).
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner


@pytest.fixture
def warehouse(tmp_path):
    items_2020 = [
        {
            "id": "good-2020",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [0, 60]},
            "properties": {
                "datetime": "2020-06-15T00:00:00Z",
                "platform": "sentinel-1",
                "percent_valid_pixels": 95,
                "date_dt": 15,
            },
            "links": [],
            "assets": {},
        },
        {
            "id": "bad-2020",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [0, 60]},
            "properties": {
                "datetime": "2020-06-15T00:00:00Z",
                "platform": "sentinel-1",
                "percent_valid_pixels": 0,
                "date_dt": 15,
            },
            "links": [],
            "assets": {},
        },
    ]
    items_2021 = [
        {
            "id": "good-2021",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [0, 60]},
            "properties": {
                "datetime": "2021-06-15T00:00:00Z",
                "platform": "sentinel-1",
                "percent_valid_pixels": 99,
                "date_dt": 15,
            },
            "links": [],
            "assets": {},
        },
    ]

    p = H3Partitioner(resolution=2)
    wh = tmp_path / "warehouse"
    for year_label, items in [("2020", items_2020), ("2021", items_2021)]:
        for (cell, year), group in group_by_partition(fan_out(items, p)).items():
            d = wh / f"grid_partition={cell}" / f"year={year or year_label}"
            d.mkdir(parents=True)
            write_geoparquet(group, str(d / "part.parquet"))

    return wh, p


class TestQueryReturnsResults:
    """The user's exact query pattern should return items."""

    def _prune_fn(self, wh, p, geom, start_datetime=None, end_datetime=None):
        from shapely import wkb
        cells = set(p.get_intersecting_keys(wkb.dumps(geom)))
        import os
        paths = []
        for root, dirs, files in os.walk(wh):
            for f in files:
                if f.endswith(".parquet") and any(c in root for c in cells):
                    paths.append(os.path.join(root, f))
        return paths

    def test_query_returns_good_2020(self, warehouse):
        wh, p = warehouse
        from earthcatalog.search import _FileSearchEngine

        s = _FileSearchEngine(prune_fn=lambda geom, **kw: self._prune_fn(wh, p, geom, **kw))

        results = s.search(
            intersects={"type": "Point", "coordinates": [0, 60]},
            datetime="2020-01-01/2020-12-31",
            filter={"op": ">=", "args": [{"property": "percent_valid_pixels"}, 1]},
            max_items=100,
        )

        assert len(results) == 1
        assert results[0]["id"] == "good-2020"

    def test_without_filter_returns_both(self, warehouse):
        wh, p = warehouse
        from earthcatalog.search import _FileSearchEngine

        s = _FileSearchEngine(prune_fn=lambda geom, **kw: self._prune_fn(wh, p, geom, **kw))

        results = s.search(
            intersects={"type": "Point", "coordinates": [0, 60]},
            datetime="2020-01-01/2020-12-31",
        )

        assert len(results) == 2
        ids = {r["id"] for r in results}
        assert ids == {"good-2020", "bad-2020"}


class TestColumnPredicatePushdown:
    """Verify Parquet column statistics exist for filter pushdown."""

    def test_percent_column_stats_exist(self, warehouse):
        wh, _ = warehouse
        pf = pq.ParquetFile(list(wh.rglob("*.parquet"))[0])
        has_stats = False
        for rg in range(pf.metadata.num_row_groups):
            for ci in range(pf.metadata.row_group(rg).num_columns):
                cm = pf.metadata.row_group(rg).column(ci)
                if "percent_valid_pixels" in str(cm.path_in_schema):
                    if cm.statistics and cm.statistics.min is not None:
                        has_stats = True
                    break
        assert has_stats
