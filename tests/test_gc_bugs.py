"""
Regression tests for the three GC / delete bugs identified in the assessment.

Bug 1 — _list_partition_files excluded gc_* files
    After a successful GC run the canonical warehouse file is gc_*.parquet.
    A second GC pass on the same partition must still be able to find and
    rewrite it; previously the exclusion filter made those partitions invisible.

Bug 2 — garbage_collect passed empty warehouse_prefix to run_garbage_collection
    With a bucket-level S3Store the warehouse files live under a key prefix
    like "test-space/stac/catalog/warehouse/".  Passing an empty prefix caused
    _list_partition_files to search from the bucket root and find nothing, so
    GC rewrote zero files even when orphans were confirmed.

Bug 3 — Iceberg catalog not rebuilt after GC
    execute_cleanup rewrites warehouse Parquet files and deletes the originals
    but never updated the Iceberg table manifests.  After GC the table still
    pointed at the deleted part_*.parquet paths; any subsequent search that
    touched those partitions raised FileNotFoundError.
    EarthCatalog.garbage_collect must rebuild and re-upload the catalog.
"""

from __future__ import annotations

import csv
import io
import tempfile
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.hash_index import hash_id, read_hashes, write_hashes
from earthcatalog.pipelines.delete import (
    _list_partition_files,
    execute_cleanup,
    run_garbage_collection,
)
from earthcatalog.source_index import append_source_index


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _s3_key(key: str) -> str:
    return f"s3://data-bucket/{key}"


def _make_parquet(item_ids: list[str], cell: str = "cellA", year: int = 2020) -> bytes:
    tbl = pa.table(
        {
            "id": pa.array(item_ids, type=pa.string()),
            "grid_partition": pa.array([cell] * len(item_ids), type=pa.string()),
            "year": pa.array([year] * len(item_ids), type=pa.int32()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _write_inventory_csv(path: Path, keys: list[str]) -> Path:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "key"])
        for k in keys:
            writer.writerow(["data-bucket", k])
    return path


def _ids_in(store, key: str) -> list[str]:
    raw = bytes(obstore.get(store, key).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read().column("id").to_pylist()


def _list_store(store, prefix: str = "") -> list[str]:
    keys: list[str] = []
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            keys.append(obj["path"])
    return keys


# ---------------------------------------------------------------------------
# Bug 1 — _list_partition_files must include gc_* files
# ---------------------------------------------------------------------------


class TestListPartitionFilesIncludesGcFiles:
    """_list_partition_files must return gc_* files so a second GC pass works."""

    def test_lists_gc_files(self):
        store = MemoryStore()
        obstore.put(
            store,
            "grid_partition=cellA/year=2020/gc_abcd1234.parquet",
            _make_parquet(["item-A"]),
        )
        keys = _list_partition_files(store, "", "cellA", 2020)
        assert any("gc_" in k for k in keys), "gc_* file was not returned"

    def test_lists_part_files(self):
        store = MemoryStore()
        obstore.put(
            store,
            "grid_partition=cellA/year=2020/part_000000.parquet",
            _make_parquet(["item-A"]),
        )
        keys = _list_partition_files(store, "", "cellA", 2020)
        assert len(keys) == 1
        assert "part_000000.parquet" in keys[0]

    def test_lists_both_gc_and_part(self):
        """Mixed partition — both file types appear (transitional state after failed run)."""
        store = MemoryStore()
        obstore.put(
            store,
            "grid_partition=cellA/year=2020/part_000000.parquet",
            _make_parquet(["item-A"]),
        )
        obstore.put(
            store,
            "grid_partition=cellA/year=2020/gc_deadbeef.parquet",
            _make_parquet(["item-B"]),
        )
        keys = _list_partition_files(store, "", "cellA", 2020)
        assert len(keys) == 2

    def test_second_gc_pass_can_find_first_gc_output(self, tmp_path):
        """
        Simulate two consecutive GC runs on the same partition.

        First run:  part_000000 (item-keep + item-del-1) -> gc_first;
                    part deleted, item-del-1 removed.
        Second run: gc_first (item-keep + item-del-2) -> gc_second;
                    gc_first deleted, item-del-2 removed.

        item-del-2 is added to the source index only before the second run so
        the first run produces exactly one confirmed deletion.
        """
        store = MemoryStore()

        # Partition has one file with two items.
        obstore.put(
            store,
            "grid_partition=cellA/year=2020/part_000000.parquet",
            _make_parquet(["item-keep", "item-del-1"]),
        )
        # Source index for first run: only item-keep and item-del-1.
        append_source_index(
            [
                (_s3_key("keep.stac.json"), "item-keep", "cellA", 2020),
                (_s3_key("gone1.stac.json"), "item-del-1", "cellA", 2020),
            ],
            store,
            "source.parquet",
        )
        write_hashes(
            {hash_id("item-keep"), hash_id("item-del-1")},
            store,
            "hash.parquet",
        )

        inv1 = _write_inventory_csv(tmp_path / "inv1.csv", ["keep.stac.json"])
        head_fn = lambda k: k == _s3_key("keep.stac.json")  # noqa: E731

        # First GC run — removes item-del-1, writes gc_*.parquet.
        r1 = run_garbage_collection(
            str(inv1),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            warehouse_prefix="",
            head_fn=head_fn,
        )
        assert r1["confirmed"] == 1
        assert r1["files_rewritten"] == 1

        # At this point only gc_*.parquet exists; part_000000 is deleted.
        part_keys = [k for k in _list_store(store, "grid_partition=cellA/") if "part_" in k]
        assert part_keys == [], "original part file should be deleted after first GC"
        gc_keys = [k for k in _list_store(store, "grid_partition=cellA/") if "gc_" in k]
        assert len(gc_keys) == 1

        # Inject item-del-2 into the gc file and add it to the source index
        # so the second GC run has a new orphan to detect.
        gc_key = gc_keys[0]
        obstore.put(store, gc_key, _make_parquet(["item-keep", "item-del-2"]))
        append_source_index(
            [(_s3_key("gone2.stac.json"), "item-del-2", "cellA", 2020)],
            store,
            "source.parquet",
        )
        write_hashes(
            {hash_id("item-keep"), hash_id("item-del-2")},
            store,
            "hash.parquet",
        )

        # Second GC run — must find gc_*.parquet and remove item-del-2.
        inv2 = _write_inventory_csv(tmp_path / "inv2.csv", ["keep.stac.json"])
        r2 = run_garbage_collection(
            str(inv2),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            warehouse_prefix="",
            head_fn=head_fn,
        )
        assert r2["confirmed"] == 1, "second GC pass must detect item-del-2"
        assert r2["files_rewritten"] == 1, "second GC pass must rewrite the gc_* file"

        # Final state: only item-keep remains.
        final_gc = [k for k in _list_store(store, "grid_partition=cellA/") if "gc_" in k]
        assert len(final_gc) == 1
        assert _ids_in(store, final_gc[0]) == ["item-keep"]


# ---------------------------------------------------------------------------
# Bug 2 — warehouse_prefix must be passed correctly by garbage_collect
# ---------------------------------------------------------------------------


class TestWarehousePrefixDerivation:
    """
    EarthCatalog.garbage_collect must derive warehouse_prefix from warehouse_root
    and pass it to run_garbage_collection so _list_partition_files searches at
    the correct path within the bucket-level store.
    """

    def test_gc_finds_files_under_nested_prefix(self, tmp_path):
        """
        Warehouse lives at a non-root key prefix within the store.

        Simulates: store = S3Store(bucket="my-bucket")
                   warehouse = "s3://my-bucket/project/catalog/warehouse"

        With the old code (warehouse_prefix="") the list call searched from
        the bucket root and found nothing.  With the fix it searches from
        "project/catalog/warehouse/" and finds the right files.
        """
        store = MemoryStore()
        warehouse_prefix = "project/catalog/warehouse/"

        # Place warehouse files under the nested prefix.
        obstore.put(
            store,
            f"{warehouse_prefix}grid_partition=cellA/year=2020/part_000000.parquet",
            _make_parquet(["item-A", "item-B"]),
        )
        append_source_index(
            [
                (_s3_key("a.stac.json"), "item-A", "cellA", 2020),
                (_s3_key("gone.stac.json"), "item-B", "cellA", 2020),
            ],
            store,
            "source.parquet",
        )
        write_hashes({hash_id("item-A"), hash_id("item-B")}, store, "hash.parquet")

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["a.stac.json"])
        head_fn = lambda k: k == _s3_key("a.stac.json")  # noqa: E731

        result = run_garbage_collection(
            str(inv),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            warehouse_prefix=warehouse_prefix,
            head_fn=head_fn,
        )

        assert result["confirmed"] == 1
        assert result["files_rewritten"] == 1
        assert result["rows_removed"] == 1

        # The rewritten gc file must live under the same prefix.
        gc_keys = [
            k
            for k in _list_store(store, warehouse_prefix)
            if k.endswith(".parquet") and "gc_" in k
        ]
        assert len(gc_keys) == 1
        assert _ids_in(store, gc_keys[0]) == ["item-A"]

    def test_empty_prefix_still_works(self, tmp_path):
        """
        warehouse_prefix="" is the degenerate case where the store is already
        scoped to the warehouse root (e.g. a LocalStore or a prefixed S3Store).
        Must continue to work unchanged.
        """
        store = MemoryStore()
        obstore.put(
            store,
            "grid_partition=cellB/year=2021/part_000000.parquet",
            _make_parquet(["item-X", "item-gone"], cell="cellB", year=2021),
        )
        append_source_index(
            [
                (_s3_key("x.stac.json"), "item-X", "cellB", 2021),
                (_s3_key("gone.stac.json"), "item-gone", "cellB", 2021),
            ],
            store,
            "source.parquet",
        )
        write_hashes({hash_id("item-X"), hash_id("item-gone")}, store, "hash.parquet")

        inv = _write_inventory_csv(tmp_path / "inv.csv", ["x.stac.json"])
        result = run_garbage_collection(
            str(inv),
            store=store,
            source_index_key="source.parquet",
            hash_index_key="hash.parquet",
            warehouse_prefix="",
            head_fn=lambda k: k == _s3_key("x.stac.json"),
        )

        assert result["files_rewritten"] == 1


# ---------------------------------------------------------------------------
# Bug 3 — Iceberg catalog rebuilt after GC via EarthCatalog.garbage_collect
# ---------------------------------------------------------------------------


class TestIcebergRebuildAfterGC:
    """
    EarthCatalog.garbage_collect must rebuild the Iceberg table after rewriting
    warehouse files so subsequent searches do not reference deleted paths.
    """

    def _make_catalog(self, tmp_path, items: list[dict], h3_resolution: int = 2):
        """
        Build a small local EarthCatalog with real warehouse files and return
        the EarthCatalog facade.  Uses LocalStore so no S3 credentials needed.
        """
        from obstore.store import LocalStore

        from earthcatalog import open as ec_open
        from earthcatalog.catalog import _open_sqlite, get_or_create
        from earthcatalog.config import GridConfig
        from earthcatalog.grids.h3_partitioner import H3Partitioner
        from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

        db_path = str(tmp_path / "earthcatalog.db")
        wh_path = str(tmp_path / "warehouse")
        Path(wh_path).mkdir(parents=True, exist_ok=True)

        cat = _open_sqlite(db_path=db_path, warehouse_path=wh_path)
        grid_cfg = GridConfig(type="h3", resolution=h3_resolution)
        tbl = get_or_create(cat, grid_config=grid_cfg)

        partitioner = H3Partitioner(resolution=h3_resolution)
        rows = fan_out(items, partitioner)
        groups = group_by_partition(rows)
        paths = []
        for (cell, year), group in groups.items():
            year_str = str(year) if year is not None else "unknown"
            out_dir = Path(wh_path) / f"grid_partition={cell}" / f"year={year_str}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = str(out_dir / "part_000000.parquet")
            write_geoparquet(group, out_path)
            paths.append(out_path)
        tbl.add_files(paths)

        store = LocalStore(str(tmp_path))
        ec = ec_open(store=store, base=str(tmp_path))
        return ec, db_path, wh_path, store

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
        from obstore.store import LocalStore

        from earthcatalog import open as ec_open
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
        from earthcatalog.pipelines.delete import rewrite_file_without_orphans

        from obstore.store import LocalStore as _LS

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
        import csv

        from obstore.store import LocalStore

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

        # Set up source + hash index with a "deleted" item.
        si_path = str(tmp_path / "warehouse_source_index.parquet")
        hi_path = str(tmp_path / "warehouse_id_hashes.parquet")
        from obstore.store import LocalStore as _LS

        si_store = _LS(str(tmp_path))
        append_source_index(
            [(_s3_key("gone.stac.json"), "some-item", "cellA", 2020)],
            si_store,
            "warehouse_source_index.parquet",
        )
        write_hashes({hash_id("some-item")}, si_store, "warehouse_id_hashes.parquet")

        # Track whether rebuild_iceberg_from_warehouse is called.
        rebuild_called = []
        import earthcatalog.pipelines.backfill as _bfmod

        orig = _bfmod.rebuild_iceberg_from_warehouse

        def _spy(*args, **kwargs):
            rebuild_called.append(True)
            return orig(*args, **kwargs)

        monkeypatch.setattr(_bfmod, "rebuild_iceberg_from_warehouse", _spy)

        store = _LS(str(tmp_path))
        ec = ec_open(store=store, base=str(tmp_path))

        inv_path = tmp_path / "inv.csv"
        with inv_path.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["bucket", "key"])
            # inventory is empty → "gone.stac.json" is absent

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
