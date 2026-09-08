"""Year-boundary temporal search (the itslive-branch bug, accounted for).

ITS_LIVE velocity pairs span ~500 days: `datetime` is the pair midpoint,
`start_datetime`/`end_datetime` bound the observation window.  Two things
must use OVERLAP semantics, not point semantics:

1. the Iceberg prune (start_datetime/end_datetime predicates + a year
   window that reaches back past the query start), and
2. the row-level temporal filter.

A pair that crosses a year boundary must still be found by a query for the
year its extent extends into.
"""

from __future__ import annotations

import pytest
import shapely.geometry
from obstore.store import LocalStore

from earthcatalog.catalog import EarthCatalog, _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester


def _pair_item(item_id: str, cell: str) -> dict:
    """A velocity-pair-like item spanning 2025-12 into 2026-03."""
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [-0.5, -0.5, 0.5, 0.5],
        "properties": {
            "datetime": "2025-12-20T00:00:00Z",
            "start_datetime": "2025-12-01T00:00:00Z",
            "end_datetime": "2026-03-10T00:00:00Z",
            "grid_partition": cell,
            "platform": "sentinel-1",
        },
        "_source_bucket": "data-bucket",
        "_source_key": f"dir/{item_id}.stac.json",
    }


@pytest.fixture()
def catalog_with_boundary_pair(tmp_path):
    """A warehouse holding ONE pair that crosses the 2025→2026 boundary."""
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    store = LocalStore(str(tmp_path))
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))

    class _FakeTable:
        files: list[str] = []

        def add_files(self, paths):
            self.files.extend(paths)

    item = _pair_item("boundary-pair", "boundary-pair")
    # Production fan-out: the item's cell comes from the partitioner.
    partitioner = H3Partitioner(resolution=2)
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
    ing.run([("data-bucket", "dir/boundary-pair.stac.json")])

    return (
        EarthCatalog(
            catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None
        ),
        store,
    )


def test_pair_crossing_year_boundary_found_by_next_year_query(catalog_with_boundary_pair):
    ec, _ = catalog_with_boundary_pair
    result = ec.search(bbox=[-1, -1, 1, 1], datetime="2026-01-05/2026-02-05")
    ids = {it.id for it in result.item_collection()}
    assert "boundary-pair" in ids


def test_pair_still_found_in_its_own_year(catalog_with_boundary_pair):
    ec, _ = catalog_with_boundary_pair
    result = ec.search(bbox=[-1, -1, 1, 1], datetime="2025-12-20/2025-12-31")
    ids = {it.id for it in result.item_collection()}
    assert "boundary-pair" in ids
