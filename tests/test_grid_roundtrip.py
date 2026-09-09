"""Ingest → search roundtrips for every built-in grid on LocalStore.

These prove the read path prunes with exactly the grid the data was written
with — including grids that historically raised ValueError on the query side
(s2, utm) or silently rebuilt a per-query partitioner (geojson).
"""

from __future__ import annotations

import json

import pytest
from obstore.store import LocalStore

from earthcatalog.catalog import EarthCatalog, _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.grids import build_partitioner
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester


def _point_item(item_id: str, lon: float, lat: float, when: str) -> dict:
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "bbox": [lon, lat, lon, lat],
        "properties": {
            "datetime": when,
            "platform": "sentinel-1",
            "grid_partition": "placeholder",
        },
        "_source_bucket": "data-bucket",
        "_source_key": f"dir/{item_id}.stac.json",
    }


def _ingest_and_search(tmp_path, grid_config: GridConfig, item: dict, query_bbox: list[float]):
    store = LocalStore(str(tmp_path))
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=grid_config)

    partitioner = build_partitioner(grid_config)
    item["properties"]["grid_partition"] = partitioner.get_intersecting_keys(
        _wkb_of(item)
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
    ing.run([("data-bucket", item["_source_key"])])

    ec = EarthCatalog(catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None)
    result = ec.search(bbox=query_bbox, datetime="2025-01-01/2026-01-01")
    return {it.id for it in result.item_collection()}


def _wkb_of(item: dict) -> bytes:
    from shapely.geometry import shape

    return shape(item["geometry"]).wkb


@pytest.mark.parametrize(
    "grid_config",
    [
        GridConfig(type="s2", resolution=2),
        GridConfig(type="utm"),
        GridConfig(type="h3", resolution=2),
    ],
)
def test_roundtrip_point_query_finds_item(tmp_path, grid_config):
    item = _point_item("rt-item", -100.0, 70.0, "2025-06-15T00:00:00Z")
    ids = _ingest_and_search(tmp_path, grid_config, item, [-101.0, 69.0, -99.0, 71.0])
    assert "rt-item" in ids


def test_roundtrip_geojson_boundaries(tmp_path):
    boundaries = tmp_path / "tiles.geojson"
    boundaries.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": "tile-glacier"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [[-101.0, 69.0], [-99.0, 69.0], [-99.0, 71.0], [-101.0, 71.0], [-101.0, 69.0]]
                            ],
                        },
                    }
                ],
            }
        )
    )
    grid_config = GridConfig(type="geojson", boundaries_path=str(boundaries), id_field="id")
    item = _point_item("geo-item", -100.0, 70.0, "2025-06-15T00:00:00Z")
    ids = _ingest_and_search(tmp_path, grid_config, item, [-101.0, 69.0, -99.0, 71.0])
    assert "geo-item" in ids


def test_catalog_info_reuses_one_partitioner(tmp_path):
    """The read-side partitioner is built once and cached."""
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    info = _catalog_info(table)
    assert info.partitioner() is info.partitioner()
    assert info.time_bin == "year"
