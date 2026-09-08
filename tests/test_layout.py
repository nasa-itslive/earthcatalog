"""Schema-driven warehouse layout: ``grid=/level=/tile=/{bin}=``.

The layout is derived from the catalog configuration (table properties),
not hardcoded: the default is h3/level-1/yearly, but lat_lon/level-2/monthly
or s2/level-4/daily must work by config alone.  Legacy
``grid_partition=.../year=...`` files stay readable (gc / rebuild / stats).
"""

from __future__ import annotations

import datetime as dt

import pytest
import shapely.geometry
from obstore.store import LocalStore

from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester
from earthcatalog.schema import bin_value, layout_of, partition_prefix, partition_year


class TestBinValue:
    def test_year(self):
        assert bin_value("2025-12-20T00:00:00Z", "year") == "2025"

    def test_month(self):
        assert bin_value("2025-12-20T00:00:00Z", "month") == "2025-12"

    def test_day(self):
        assert bin_value("2025-12-20T05:00:00Z", "day") == "2025-12-20"

    def test_none_is_unknown(self):
        assert bin_value(None, "year") == "unknown"

    def test_unparseable_is_unknown(self):
        assert bin_value("not-a-date", "year") == "unknown"

    def test_datetime_object(self):
        assert bin_value(dt.datetime(2026, 3, 5, tzinfo=dt.UTC), "month") == "2026-03"

    def test_unknown_bin_raises(self):
        with pytest.raises(ValueError):
            bin_value("2025-12-20", "week")


class TestPartitionYear:
    def test_year_transform(self):
        assert partition_year("year", 55) == 2025

    def test_month_transform(self):
        # 2026-01 is month 672 after 1970-01.
        assert partition_year("month", 672) == 2026

    def test_day_transform(self):
        assert partition_year("day", 20442) == 2025  # 2025-12-20


class TestLayoutOf:
    def test_defaults(self):
        assert layout_of({}) == ("h3", "1", "year")

    def test_from_properties(self):
        props = {
            "earthcatalog.grid.type": "s2",
            "earthcatalog.grid.resolution": "4",
            "earthcatalog.time_bin": "day",
        }
        assert layout_of(props) == ("s2", "4", "day")


def test_partition_prefix_v2():
    assert (
        partition_prefix("warehouse", "h3", "2", "810fbffffffffff", "year", "2025")
        == "warehouse/grid=h3/level=2/tile=810fbffffffffff/year=2025/"
    )


def _point_item(item_id: str, when: str, end: str | None = None) -> dict:
    props = {
        "datetime": when,
        "platform": "sentinel-1",
        "grid_partition": "placeholder",
    }
    if end is not None:
        props["start_datetime"] = when
        props["end_datetime"] = end
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [-0.5, -0.5, 0.5, 0.5],
        "properties": props,
        "_source_bucket": "data-bucket",
        "_source_key": f"dir/{item_id}.stac.json",
    }


def _ingest_one(tmp_path, grid_config: GridConfig, item: dict):
    store = LocalStore(str(tmp_path))
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=grid_config)

    partitioner = H3Partitioner(resolution=grid_config.resolution)
    item["properties"]["grid_partition"] = partitioner.get_intersecting_keys(
        shapely.geometry.shape(item["geometry"]).wkb
    )[0]
    ing = Ingester(
        store=store,
        index=Index(store, "warehouse_index.parquet"),
        table=table,
        fetch_fn=lambda b, k: item,
        partitioner=partitioner,
        warehouse_prefix="warehouse",
        warehouse_root=str(wh),
        batch_size=10,
    )
    ing.run([("data-bucket", f"dir/{item['id']}.stac.json")])
    return cat, table, store, partitioner


def test_writer_emits_v2_layout_for_h3_resolution_2(tmp_path):
    """GridConfig(type=h3, resolution=2) → grid=h3/level=2/tile=<cell>/year=..."""
    item = _point_item("v2-item", "2025-12-20T00:00:00Z")
    _cat, table, store, _partitioner = _ingest_one(
        tmp_path, GridConfig(type="h3", resolution=2), item
    )
    assert layout_of(table.properties) == ("h3", "2", "year")

    keys = [obj["path"] for batch in store.list(prefix="warehouse/") for obj in batch]
    parquet = [k for k in keys if k.endswith(".parquet")]
    assert len(parquet) == 1
    cell = item["properties"]["grid_partition"]
    assert f"grid=h3/level=2/tile={cell}/year=2025/" in parquet[0]
    assert "grid_partition=" not in parquet[0]


def test_month_bin_table(tmp_path):
    """time_bin="month" → month=2025-12 partition, and search still finds it."""
    item = _point_item("monthly-item", "2025-12-20T00:00:00Z")
    cat, table, store, _partitioner = _ingest_one(
        tmp_path, GridConfig(type="h3", resolution=2, time_bin="month"), item
    )
    keys = [obj["path"] for batch in store.list(prefix="warehouse/") for obj in batch]
    parquet = [k for k in keys if k.endswith(".parquet")]
    cell = item["properties"]["grid_partition"]
    assert any(f"tile={cell}/month=2025-12/" in k for k in parquet)

    # Iceberg derived the month ordinal from the data column; the stats cache
    # and the search prune must convert it back to a calendar year.
    stats = _catalog_info(table)._ensure_stats(table)
    assert stats[0]["year"] == 2025

    from earthcatalog.catalog import EarthCatalog

    ec = EarthCatalog(
        catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None
    )
    result = ec.search(bbox=[-1, -1, 1, 1], datetime="2025-12-01/2025-12-31")
    ids = {it.id for it in result.item_collection()}
    assert "monthly-item" in ids
