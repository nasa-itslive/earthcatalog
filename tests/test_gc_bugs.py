"""
Regression tests for the GC / Iceberg-rebuild correctness issues.

The only remaining legacy GC bug worth guarding: after garbage collection
rewrites warehouse Parquet files, the Iceberg table still points at the
now-deleted ``part_*.parquet`` paths.  ``EarthCatalog.garbage_collect`` must
rebuild and re-upload the catalog so subsequent searches don't reference
deleted paths.
"""

from __future__ import annotations

import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Iceberg catalog rebuilt after GC via EarthCatalog.garbage_collect
# ---------------------------------------------------------------------------


class TestIcebergRebuildAfterGC:
    """
    EarthCatalog.garbage_collect must rebuild the Iceberg table after rewriting
    warehouse files so subsequent searches do not reference deleted paths.
    """

    def _make_item(self, item_id: str, lon: float = -50.0, lat: float = 70.0) -> dict:
        return {
            "id": item_id,
            "type": "Feature",
            "stac_version": "1.0.0",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
            "properties": {"datetime": "2020-06-15T00:00:00Z"},
            "links": [],
            "assets": {},
        }

    def test_iceberg_table_rebuilt_after_gc(self, tmp_path):
        """
        After garbage_collect rewrites a partition file the Iceberg table
        must reference the new gc_*.parquet, not the deleted part_*.parquet.
        """

        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.pipelines.backfill import rebuild_iceberg_from_warehouse
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        # Build a small catalog with two items in the same partition.
        item_keep = self._make_item("keep", lon=-50.0, lat=70.0)
        item_del = self._make_item("deleted", lon=-50.0, lat=70.0)

        db_path = str(tmp_path / "earthcatalog.db")
        wh_path = str(tmp_path / "warehouse")
        Path(wh_path).mkdir(parents=True, exist_ok=True)

        cat = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        grid_cfg = GridConfig(type="h3", resolution=2)
        tbl = get_or_create(cat, grid_config=grid_cfg)

        partitioner = H3Partitioner(resolution=2)
        rows = fan_out([item_keep, item_del], partitioner)
        groups = group_by_partition(rows)
        part_paths = []
        for (cell, year), group in groups.items():
            year_str = str(year) if year is not None else "unknown"
            out_dir = Path(wh_path) / f"grid_partition={cell}" / f"year={year_str}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = str(out_dir / "part_000000.parquet")
            write_geoparquet(group, out_path)
            part_paths.append(out_path)
        tbl.add_files(part_paths)

        # Verify Iceberg currently points at part_000000.parquet.
        registered_before = [
            task.file.file_path for task in tbl.scan().plan_files()
        ]
        assert any("part_000000.parquet" in p for p in registered_before)

        # Simulate GC: rewrite part_000000 → gc_*.parquet (drop "deleted"),
        # then delete the original.  This mimics what execute_cleanup does.
        from obstore.store import LocalStore as _LS

        from earthcatalog.gc import rewrite_file_without_orphans

        wh_store = _LS(wh_path)
        # Map full path to key relative to wh_store.
        for full_p in part_paths:
            rel = str(Path(full_p).relative_to(wh_path))
            new_key, _ = rewrite_file_without_orphans(rel, {"deleted"}, wh_store)
            Path(full_p).unlink()  # delete original (mimics execute_cleanup)

        # Now rebuild the Iceberg catalog via the helper.
        rebuild_iceberg_from_warehouse(
            catalog_path=db_path,
            warehouse_root=wh_path,
            warehouse_store=_LS(wh_path),
            upload=False,
        )

        # Re-open catalog and check that it now points at gc_*.parquet.
        cat2 = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        from earthcatalog.catalog import FULL_NAME

        tbl2 = cat2.load_table(FULL_NAME)
        registered_after = [task.file.file_path for task in tbl2.scan().plan_files()]

        assert not any(
            "part_000000.parquet" in p for p in registered_after
        ), "deleted part_000000 still registered"
        assert any(
            "gc_" in p for p in registered_after
        ), "gc_* file not registered after rebuild"

    def test_gc_dry_run_does_not_rebuild(self, tmp_path, monkeypatch):
        """dry_run=True must skip the Iceberg rebuild entirely."""
        from earthcatalog import open as ec_open
        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        item = self._make_item("some-item", lon=-50.0, lat=70.0)
        db_path = str(tmp_path / "earthcatalog.db")
        wh_path = str(tmp_path / "warehouse")
        Path(wh_path).mkdir(parents=True, exist_ok=True)

        cat = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        tbl = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
        partitioner = H3Partitioner(resolution=2)
        rows = fan_out([item], partitioner)
        for (cell, year), group in group_by_partition(rows).items():
            year_str = str(year) if year is not None else "unknown"
            out_dir = Path(wh_path) / f"grid_partition={cell}" / f"year={year_str}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = str(out_dir / "part_000000.parquet")
            write_geoparquet(group, out_path)
            tbl.add_files([out_path])

        # Track whether rebuild_iceberg_from_warehouse is called.
        rebuild_called = []
        import earthcatalog.pipelines.backfill as _bfmod

        orig = _bfmod.rebuild_iceberg_from_warehouse

        def _spy(*args, **kwargs):
            rebuild_called.append(True)
            return orig(*args, **kwargs)

        monkeypatch.setattr(_bfmod, "rebuild_iceberg_from_warehouse", _spy)

        from obstore.store import LocalStore as _LS

        store = _LS(str(tmp_path))
        ec = ec_open(store=store, base=str(tmp_path))

        inv_path = tmp_path / "inv.csv"
        with inv_path.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["bucket", "key"])
            # inventory is empty → nothing to delete

        # dry_run=True → no rebuild should happen.
        ec.garbage_collect(str(inv_path), dry_run=True)

        assert rebuild_called == [], "rebuild must not be called during dry_run"

    def test_properties_preserved_after_rebuild(self, tmp_path):
        """
        rebuild_iceberg_from_warehouse must carry over all existing table
        properties (grid_type, grid_resolution, hash_index_path, etc.).
        """
        from obstore.store import LocalStore

        from earthcatalog.catalog import (
            FULL_NAME,
            PROP_GRID_RESOLUTION,
            PROP_GRID_TYPE,
            PROP_HASH_INDEX_PATH,
            _open_sqlite,
            get_or_create,
        )
        from earthcatalog.config import GridConfig
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.pipelines.backfill import rebuild_iceberg_from_warehouse
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        item = self._make_item("prop-item", lon=-50.0, lat=70.0)
        db_path = str(tmp_path / "catalog.db")
        wh_path = str(tmp_path / "warehouse")
        Path(wh_path).mkdir(parents=True, exist_ok=True)

        cat = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        grid_cfg = GridConfig(type="h3", resolution=3)
        tbl = get_or_create(cat, grid_config=grid_cfg)
        # Manually set hash_index_path property.
        with tbl.transaction() as tx:
            tx.set_properties(**{PROP_HASH_INDEX_PATH: "s3://bucket/path/hashes.parquet"})

        partitioner = H3Partitioner(resolution=3)
        rows = fan_out([item], partitioner)
        for (cell, year), group in group_by_partition(rows).items():
            year_str = str(year) if year is not None else "unknown"
            out_dir = Path(wh_path) / f"grid_partition={cell}" / f"year={year_str}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = str(out_dir / "part_000000.parquet")
            write_geoparquet(group, out_path)
            tbl.add_files([out_path])

        # Rebuild.
        rebuild_iceberg_from_warehouse(
            catalog_path=db_path,
            warehouse_root=wh_path,
            warehouse_store=LocalStore(wh_path),
            upload=False,
        )

        # Reload and verify properties survived.
        cat2 = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        tbl2 = cat2.load_table(FULL_NAME)
        assert tbl2.properties.get(PROP_GRID_TYPE) == "h3"
        assert tbl2.properties.get(PROP_GRID_RESOLUTION) == "3"
        assert tbl2.properties.get(PROP_HASH_INDEX_PATH) == "s3://bucket/path/hashes.parquet"
