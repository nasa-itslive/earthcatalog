"""
Tests for the new EarthCatalog simplified API.

Tests actual logic: spatial partition pruning, temporal filtering, and
Iceberg integration — not trivial property access or string formatting.
"""

from pathlib import Path

import pytest
from obstore.store import MemoryStore
from shapely.geometry import Point, box

from earthcatalog import EarthCatalog
from earthcatalog import open as ec_open
from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_warehouse(tmp_path):
    """
    Create a warehouse with real data spanning multiple cells and years.

    Returns:
        tuple: (tmp_path, table, items) where items is the list of original
               STAC item dicts for validation
    """
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    # Create items across multiple cells and years
    items = [
        {
            "id": f"test-item-{i:04d}",
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-55 + i * 5, 62],
                        [-45 + i * 5, 62],
                        [-45 + i * 5, 72],
                        [-55 + i * 5, 72],
                        [-55 + i * 5, 62],
                    ]
                ],
            },
            "properties": {
                "datetime": f"202{i % 4 + 1}-06-15T00:00:00Z",
                "platform": "NISAR",
            },
            "links": [],
            "assets": {},
        }
        for i in range(10)
    ]

    p = H3Partitioner(resolution=2)
    rows = fan_out(items, p)
    groups = group_by_partition(rows, p)

    paths = []
    for (cell, year), group in groups.items():
        out = str(tmp_path / f"part_{cell[:12]}_{year}.parquet")
        write_geoparquet(group, out)
        paths.append(out)
    tbl.add_files(paths)

    return tmp_path, tbl, items


@pytest.fixture
def memory_store_with_catalog(tmp_path):
    """
    Create an in-memory store with a catalog for testing the new API.

    Structure in MemoryStore:
        catalog/earthcatalog.db
        catalog/warehouse/
    """
    import obstore

    store = MemoryStore()

    # Create a catalog with data
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    tbl = get_or_create(cat, grid_config=GridConfig(resolution=2))

    # Add minimal data
    item = {
        "id": "mem-test",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-50, 65], [-48, 65], [-48, 68], [-50, 68], [-50, 65]]],
        },
        "properties": {"datetime": "2022-06-15T00:00:00Z", "platform": "NISAR"},
        "links": [],
        "assets": {},
    }

    p = H3Partitioner(resolution=2)
    rows = fan_out([item], p)
    for (cell, year), group in group_by_partition(rows, p).items():
        out = str(tmp_path / f"part_{cell}_{year}.parquet")
        write_geoparquet(group, out)
        tbl.add_files([out])

    # Upload catalog to memory store at catalog/earthcatalog.db
    catalog_bytes = Path(db).read_bytes()
    obstore.put(store, "catalog/earthcatalog.db", catalog_bytes)

    return store


# ---------------------------------------------------------------------------
# ec_open() - new API
# ---------------------------------------------------------------------------


class TestNewCatalogOpenAPI:
    """Test the new simplified ec_open() API."""

    def test_open_with_store_and_local_path(self, tmp_path):
        from obstore.store import MemoryStore

        wh = str(tmp_path / "warehouse")
        ec = ec_open(store=MemoryStore(), base=wh)
        assert isinstance(ec, EarthCatalog)
        assert ec.grid_type == "h3"
        assert ec.grid_resolution == 1
        assert hasattr(ec, "search_files")
        assert hasattr(ec, "search")

    def test_open_legacy_api_still_works(self, tmp_path):
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db_path=db, warehouse_path=wh)
        assert not isinstance(cat, EarthCatalog)
        assert hasattr(cat, "load_table")


# ---------------------------------------------------------------------------
# EarthCatalog.search_files() - spatial pruning logic
# ---------------------------------------------------------------------------


class TestSearchFilesSpatialPruning:
    """Test that search_files correctly prunes by spatial partition."""

    def test_point_query_returns_subset_of_all_files(self, populated_warehouse):
        """Point query should return fewer files than total warehouse."""
        tmp_path, tbl, items = populated_warehouse
        info = _catalog_info(tbl)

        # Get all files
        all_files = set()
        for task in tbl.scan().plan_files():
            all_files.add(task.file.file_path)

        # Query for a specific point
        point = Point(-50, 67)  # Within some of our item footprints
        paths = info.file_paths(tbl, point)

        # Should get a subset (not all, not empty)
        assert len(paths) > 0
        assert len(paths) < len(all_files)
        assert set(paths).issubset(all_files)

    def test_non_intersecting_geometry_returns_empty(self, populated_warehouse):
        """Geometry far from any data should return empty file list."""
        _, tbl, _ = populated_warehouse
        info = _catalog_info(tbl)

        # Query a location with no data (middle of Pacific)
        pacific = Point(0, 0)
        paths = info.file_paths(tbl, pacific)

        assert paths == []

    def test_bbox_returns_more_files_than_point(self, populated_warehouse):
        """Larger geometry should return equal or more files than point."""
        _, tbl, _ = populated_warehouse
        info = _catalog_info(tbl)

        point = Point(-50, 67)
        bbox = box(-60, 60, -40, 75)

        point_paths = info.file_paths(tbl, point)
        bbox_paths = info.file_paths(tbl, bbox)

        # BBox should include at least as many files as point
        assert len(bbox_paths) >= len(point_paths)


# ---------------------------------------------------------------------------
# EarthCatalog convenience methods
# ---------------------------------------------------------------------------


class TestEarthCatalogConvenienceMethods:
    """Test EarthCatalog convenience methods provide correct data."""

    def test_stats_row_counts_match_manifest(self, populated_warehouse):
        """stats() row counts should match Iceberg manifest records."""
        _, tbl, items = populated_warehouse
        info = _catalog_info(tbl)

        stats = info.stats(tbl)
        total_from_stats = sum(s["row_count"] for s in stats)

        # Compare to manifest record count
        total_from_manifest = sum(task.file.record_count for task in tbl.scan().plan_files())

        assert total_from_stats == total_from_manifest


# ---------------------------------------------------------------------------
# Property access and metadata
# ---------------------------------------------------------------------------


class TestEarthCatalogProperties:
    """Test EarthCatalog property access for grid metadata."""

    def test_grid_properties_accessible(self, populated_warehouse):
        """Grid metadata should be accessible via properties."""
        _, tbl, _ = populated_warehouse
        info = _catalog_info(tbl)

        assert info.grid_type == "h3"
        assert isinstance(info.grid_resolution, int)

    def test_html_repr(self, populated_warehouse):
        from earthcatalog import EarthCatalog

        _, tbl, _ = populated_warehouse
        info = _catalog_info(tbl)
        ec = EarthCatalog(catalog=None, table=tbl, info=info)
        html = ec._repr_html_()
        assert "EarthCatalog" in html
        assert "Total files" in html
        assert "Total rows" in html
        assert "<div" in html and "</div>" in html
        assert "<table" in html and "</table>" in html
        assert info._cached_stats is not None


# ---------------------------------------------------------------------------
# Store and catalog download integration
# ---------------------------------------------------------------------------


class TestStoreIntegration:
    """Test integration with obstore for catalog download."""

    def test_auto_download_from_store(self, memory_store_with_catalog):
        """Opening with store should auto-download catalog.db."""
        store = memory_store_with_catalog

        # This should download catalog.db from the MemoryStore
        # and create an EarthCatalog
        ec = ec_open(store=store, base="catalog")

        assert isinstance(ec, EarthCatalog)
        # Verify we can query the catalog
        assert ec.grid_type == "h3"

    def test_missing_catalog_creates_fresh(self, tmp_path):
        """Missing catalog on store should create a fresh one."""
        from obstore.store import MemoryStore

        store = MemoryStore()  # Empty store - no catalog

        # Should not raise - will create fresh catalog
        ec = ec_open(store=store, base=str(tmp_path / "catalog"))

        assert isinstance(ec, EarthCatalog)


class TestIngestInventory:
    """Minimal tests for EarthCatalog.ingest_inventory()."""

    def test_ingest_inventory_derives_params(self, tmp_path, monkeypatch):
        """ingest_inventory builds an Index and runs a single-node Ingester."""
        from earthcatalog import EarthCatalog
        from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig

        store = MemoryStore()
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db, wh)
        tbl = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
        ec = EarthCatalog(
            catalog=cat,
            table=tbl,
            info=_catalog_info(tbl),
            store=store,
            catalog_key="catalog.db",
        )

        # Mock credentials — ingest_inventory() requires AWS_ACCESS_KEY_ID
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")

        # Patch the Ingester to capture construction instead of executing.
        captured = {}

        def fake_ingester_init(self, store, index, table, **kwargs):
            captured["store"] = store
            captured["index"] = index
            captured["table"] = table
            captured["kwargs"] = kwargs
            self._store = store

        def fake_run(self, inventory):
            captured["inventory"] = inventory
            return {"items": 0, "rows": 0}

        import earthcatalog.ingest as _ingmod

        class _FakeIngester:
            def __init__(self, store, index, table, **kwargs):
                captured["store"] = store
                captured["index"] = index
                captured["table"] = table
                captured["kwargs"] = kwargs

            def run(self, inventory):
                captured["inventory"] = inventory
                return {"items": 0, "rows": 0}

        monkeypatch.setattr(_ingmod, "Ingester", _FakeIngester)
        monkeypatch.setattr(_ingmod, "DaskIngester", _FakeIngester)

        # Call ingest_inventory in full mode
        ec.ingest_inventory("inventory.parquet", mode="full")

        assert captured.get("kwargs", {}).get("partitioner") is not None
        assert captured.get("store") == store

    def test_scatter_only_then_resume_from_scatter_manifest(self, tmp_path, monkeypatch):
        """Two-step distributed workflow: scatter_only writes shards + a
        scatter.json and does NOT ingest; a second call pointing at the
        scatter.json consumes the shards as-is (no re-read)."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from earthcatalog import EarthCatalog
        from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.ingest_config import IngestConfig
        from earthcatalog.inventory import is_scatter_manifest

        store = MemoryStore()
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db, wh)
        tbl = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
        ec = EarthCatalog(
            catalog=cat,
            table=tbl,
            info=_catalog_info(tbl),
            store=store,
            catalog_key="catalog.db",
        )
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")

        keys = [f"k{i}.stac.json" for i in range(5)]
        inv = tmp_path / "inv.parquet"
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["b"] * 5, type=pa.string()),
                    "key": pa.array(keys, type=pa.string()),
                }
            ),
            str(inv),
        )

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

        captured = {}

        class _FakeDaskIngester:
            def __init__(self, **kwargs):
                captured["kwargs"] = kwargs

            def run(self, shards, *, client=None):
                captured["shards"] = shards
                captured["n_shards"] = len(shards)
                return {"items": len(shards), "rows": len(shards)}

        import earthcatalog.ingest as _ingmod

        monkeypatch.setattr(_ingmod, "DaskIngester", _FakeDaskIngester)

        # Step 1: scatter only — no cluster, no DaskIngester call.
        cfg = IngestConfig(create_client=lambda: _FakeClient(), chunk_size=2, scatter_only=True)
        result = ec.ingest_inventory(str(inv), mode="full", config=cfg)
        assert "scatter" in result and is_scatter_manifest(result["scatter"])
        assert "n_shards" not in captured, "scatter_only must not ingest"

        # The scatter manifest + 3 shard files (5 keys, chunk_size=2) exist.
        manifest_key = result["scatter"]
        shard_dir = manifest_key.rsplit("/", 1)[0] + "/"
        paths = [obj["path"] for batch in store.list(prefix=shard_dir) for obj in batch]
        assert any(p.endswith("scatter.json") for p in paths), paths
        assert sum(p.endswith(".parquet") for p in paths) == 3

        # Step 2: point at the scatter.json — shards consumed, no re-read.
        cfg2 = IngestConfig(create_client=lambda: _FakeClient(), chunk_size=2)
        ec.ingest_inventory(manifest_key, mode="delta", config=cfg2)
        assert captured["n_shards"] == 3
        # Each loaded shard is file-backed and yields its rows.
        assert all(not s.pairs and s.files for s in captured["shards"])

        # Cleanup after success: the manifest is gone (shards too).
        import obstore as _obstore

        with pytest.raises(Exception):
            _obstore.get(store, manifest_key).bytes()

    def test_failed_ingest_keeps_shards_for_resume(self, tmp_path, monkeypatch):
        """A failed map/reduce keeps the shard files so the run can be
        resumed by re-pointing at the same scatter manifest."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from earthcatalog import EarthCatalog
        from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.ingest_config import IngestConfig

        store = MemoryStore()
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db, wh)
        tbl = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
        ec = EarthCatalog(
            catalog=cat,
            table=tbl,
            info=_catalog_info(tbl),
            store=store,
            catalog_key="catalog.db",
        )
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")

        inv = tmp_path / "inv.parquet"
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["b", "b"], type=pa.string()),
                    "key": pa.array(["a.stac.json", "b.stac.json"], type=pa.string()),
                }
            ),
            str(inv),
        )

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

        class _FailingDaskIngester:
            def __init__(self, **kwargs):
                pass

            def run(self, shards, *, client=None):
                raise RuntimeError("simulated worker failure")

        import earthcatalog.ingest as _ingmod

        monkeypatch.setattr(_ingmod, "DaskIngester", _FailingDaskIngester)

        cfg = IngestConfig(create_client=lambda: _FakeClient(), chunk_size=10)
        with pytest.raises(RuntimeError, match="simulated"):
            ec.ingest_inventory(str(inv), mode="full", config=cfg)

        # Shards + manifest kept for resume (list everything — local-path
        # keys are absolute under MemoryStore in tests).
        all_paths = [obj["path"] for batch in store.list(prefix="") for obj in batch]
        assert any(p.endswith("scatter.json") for p in all_paths), all_paths
        assert any(p.endswith(".parquet") for p in all_paths), all_paths

    def test_scatter_only_is_idempotent(self, tmp_path, monkeypatch):
        """Re-running the scatter step with the same inventory reuses the
        existing shards instead of re-reading the inventory."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from earthcatalog import EarthCatalog
        from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.ingest_config import IngestConfig

        store = MemoryStore()
        db = str(tmp_path / "catalog.db")
        wh = str(tmp_path / "warehouse")
        cat = _open_sqlite(db, wh)
        tbl = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
        ec = EarthCatalog(
            catalog=cat,
            table=tbl,
            info=_catalog_info(tbl),
            store=store,
            catalog_key="catalog.db",
        )
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")

        inv = tmp_path / "inv.parquet"
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["b", "b"], type=pa.string()),
                    "key": pa.array(["a.stac.json", "b.stac.json"], type=pa.string()),
                }
            ),
            str(inv),
        )

        import earthcatalog.inventory as _invmod

        calls = {"n": 0}
        real_write = _invmod.write_inventory_shards

        def counting_write(*args, **kwargs):
            calls["n"] += 1
            return real_write(*args, **kwargs)

        monkeypatch.setattr(_invmod, "write_inventory_shards", counting_write)

        cfg = IngestConfig(chunk_size=100, scatter_only=True)
        r1 = ec.ingest_inventory(str(inv), mode="full", config=cfg)
        r2 = ec.ingest_inventory(str(inv), mode="full", config=cfg)

        assert calls["n"] == 1, "second scatter run must reuse, not re-scatter"
        assert r1["scatter"] == r2["scatter"]
