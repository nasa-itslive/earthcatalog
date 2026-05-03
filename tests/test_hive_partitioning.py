"""DuckDB Hive partition pruning — direct DuckdbClient queries."""

from __future__ import annotations

import pytest


@pytest.fixture
def hive_warehouse(tmp_path):
    from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet
    from earthcatalog.grids.h3_partitioner import H3Partitioner

    items = []
    for i, (lon, lat, yr) in enumerate([
        (0, 60, 2020), (10, 65, 2021), (-10, 55, 2020), (20, 70, 2022),
    ]):
        items.append({
            "id": f"item-{i:04d}",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"datetime": f"{yr}-06-15T00:00:00Z", "platform": "sentinel-1"},
            "links": [],
            "assets": {},
        })

    p = H3Partitioner(resolution=2)
    wh = tmp_path / "warehouse"
    for (cell, year), group in group_by_partition(fan_out(items, p)).items():
        d = wh / f"grid_partition={cell}" / f"year={year or 'unknown'}"
        d.mkdir(parents=True)
        write_geoparquet(group, str(d / "part.parquet"))
    return wh


class TestHivePruning:
    def test_unfiltered(self, hive_warehouse):
        from rustac import DuckdbClient
        client = DuckdbClient(use_hive_partitioning=True)
        assert len(client.search(f"{hive_warehouse}/*/*/*.parquet")) == 4

    def test_single_cell_filter(self, hive_warehouse):
        from rustac import DuckdbClient
        client = DuckdbClient(use_hive_partitioning=True)
        results = client.search(
            f"{hive_warehouse}/*/*/*.parquet",
            filter={"op": "=", "args": [{"property": "grid_partition"}, "820807fffffffff"]},
        )
        assert len(results) == 1
        assert results[0]["properties"]["grid_partition"] == "820807fffffffff"

    def test_multi_cell_filter(self, hive_warehouse):
        from rustac import DuckdbClient
        client = DuckdbClient(use_hive_partitioning=True)
        cells = ["820807fffffffff", "82094ffffffffff"]
        results = client.search(
            f"{hive_warehouse}/*/*/*.parquet",
            filter={"op": "in", "args": [{"property": "grid_partition"}, cells]},
        )
        assert len(results) == 2
        for r in results:
            assert r["properties"]["grid_partition"] in cells
