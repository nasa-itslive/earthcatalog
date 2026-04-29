"""Tests for earthcatalog.core.hash_index."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.core.hash_index import (
    hash_id,
    merge_hashes_from_parquets,
    read_hashes,
    write_hashes,
)


def _make_warehouse_parquet(item_ids: list[str]) -> bytes:
    """Write a minimal warehouse parquet with an ``id`` column."""
    tbl = pa.table({"id": pa.array(item_ids, type=pa.string())})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


class TestHashId:
    def test_deterministic(self):
        assert hash_id("item-1") == hash_id("item-1")

    def test_16_bytes(self):
        assert len(hash_id("item-1")) == 16

    def test_different_ids_differ(self):
        assert hash_id("item-1") != hash_id("item-2")


class TestReadWriteHashes:
    def test_roundtrip(self, tmp_path):
        store = MemoryStore()
        hashes = {hash_id("a"), hash_id("b"), hash_id("c")}
        write_hashes(hashes, store, "idx.parquet")
        result = read_hashes(store, "idx.parquet")
        assert result == hashes

    def test_read_missing_returns_empty(self):
        store = MemoryStore()
        assert read_hashes(store, "nonexistent.parquet") == set()

    def test_write_returns_count(self):
        store = MemoryStore()
        hashes = {hash_id("x"), hash_id("y")}
        n = write_hashes(hashes, store, "idx.parquet")
        assert n == 2

    def test_written_file_is_sorted(self):
        store = MemoryStore()
        hashes = {hash_id(str(i)) for i in range(20)}
        write_hashes(hashes, store, "idx.parquet")
        import obstore

        raw = bytes(obstore.get(store, "idx.parquet").bytes())
        pf = pq.ParquetFile(io.BytesIO(raw))
        values = pf.read().column("id_hash").to_pylist()
        assert values == sorted(values)


class TestMergeHashesFromParquets:
    def test_merges_new_ids(self, tmp_path):
        path = tmp_path / "part_0.parquet"
        path.write_bytes(_make_warehouse_parquet(["item-1", "item-2", "item-3"]))

        existing: set[bytes] = {hash_id("item-1")}
        updated, n_new = merge_hashes_from_parquets([str(path)], existing)

        assert n_new == 2
        assert hash_id("item-1") in updated
        assert hash_id("item-2") in updated
        assert hash_id("item-3") in updated

    def test_no_duplicates(self, tmp_path):
        path = tmp_path / "part_0.parquet"
        path.write_bytes(_make_warehouse_parquet(["item-1", "item-1", "item-1"]))

        existing: set[bytes] = set()
        updated, n_new = merge_hashes_from_parquets([str(path)], existing)

        assert n_new == 1
        assert len(updated) == 1

    def test_empty_paths(self):
        existing = {hash_id("item-x")}
        updated, n_new = merge_hashes_from_parquets([], existing)
        assert n_new == 0
        assert updated == existing

    def test_multiple_parquet_files(self, tmp_path):
        p1 = tmp_path / "part_0.parquet"
        p2 = tmp_path / "part_1.parquet"
        p1.write_bytes(_make_warehouse_parquet(["a", "b"]))
        p2.write_bytes(_make_warehouse_parquet(["c", "d"]))

        existing: set[bytes] = set()
        updated, n_new = merge_hashes_from_parquets([str(p1), str(p2)], existing)
        assert n_new == 4
        for item_id in ("a", "b", "c", "d"):
            assert hash_id(item_id) in updated

    def test_none_ids_skipped(self, tmp_path):
        tbl = pa.table({"id": pa.array([None, "item-1", None], type=pa.string())})
        path = tmp_path / "part.parquet"
        buf = io.BytesIO()
        pq.write_table(tbl, buf)
        path.write_bytes(buf.getvalue())

        existing: set[bytes] = set()
        updated, n_new = merge_hashes_from_parquets([str(path)], existing)
        assert n_new == 1
        assert hash_id("item-1") in updated


class TestRegisterDeltaUpdatesHashIndex:
    """Integration test: register_delta with update_hash_index=True."""

    def test_hash_index_updated_from_new_parquets(self, tmp_path):
        """register_delta should merge IDs from new parquet files into the hash index."""
        from obstore.store import MemoryStore

        from earthcatalog.core import store_config
        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        db_path = str(tmp_path / "test.db")
        cat = open_catalog(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            cat.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        cat.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )

        # Write two warehouse parquet files with item IDs
        part_dir = wh_root / "grid_partition=8001fffffffffff" / "year=2020"
        part_dir.mkdir(parents=True)
        p1 = part_dir / "part_000000.parquet"
        p2 = part_dir / "part_000001.parquet"
        p1.write_bytes(_make_warehouse_parquet(["item-A", "item-B"]))
        p2.write_bytes(_make_warehouse_parquet(["item-C"]))

        hash_index_path = str(tmp_path / "hash_index.parquet")

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[str(p1), str(p2)],
            staging_store=MemoryStore(),
            staging_prefix="staging",
            upload=False,
            hash_index_path=hash_index_path,
            update_hash_index=True,
        )

        # Verify hash index was written and contains exactly the right IDs
        pf = pq.ParquetFile(hash_index_path)
        written = {bytes(h) for h in pf.read().column("id_hash").to_pylist()}
        assert written == {hash_id("item-A"), hash_id("item-B"), hash_id("item-C")}

    def test_hash_index_merged_with_existing(self, tmp_path):
        """register_delta should union new IDs with pre-existing hash index."""
        from obstore.store import MemoryStore

        from earthcatalog.core import store_config
        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        # Pre-populate hash index with existing item
        hash_index_path = str(tmp_path / "hash_index.parquet")
        existing_hashes = {hash_id("pre-existing")}
        buf = io.BytesIO()
        pq.write_table(
            pa.table({"id_hash": pa.array(sorted(existing_hashes), type=pa.binary(16))}),
            buf,
            compression="zstd",
        )
        Path(hash_index_path).write_bytes(buf.getvalue())

        db_path = str(tmp_path / "test.db")
        cat = open_catalog(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            cat.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        cat.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )

        part_dir = wh_root / "grid_partition=8001fffffffffff" / "year=2020"
        part_dir.mkdir(parents=True)
        p1 = part_dir / "part_000000.parquet"
        p1.write_bytes(_make_warehouse_parquet(["new-item-1", "new-item-2"]))

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[str(p1)],
            staging_store=MemoryStore(),
            staging_prefix="staging",
            upload=False,
            hash_index_path=hash_index_path,
            update_hash_index=True,
        )

        pf = pq.ParquetFile(hash_index_path)
        written = {bytes(h) for h in pf.read().column("id_hash").to_pylist()}
        assert hash_id("pre-existing") in written
        assert hash_id("new-item-1") in written
        assert hash_id("new-item-2") in written
        assert len(written) == 3

    def test_no_update_when_flag_false(self, tmp_path):
        """register_delta should NOT touch hash index when update_hash_index=False."""
        from obstore.store import MemoryStore

        from earthcatalog.core import store_config
        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        hash_index_path = str(tmp_path / "hash_index.parquet")

        db_path = str(tmp_path / "test.db")
        cat = open_catalog(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            cat.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        cat.create_table(identifier=FULL_NAME, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[],
            staging_store=MemoryStore(),
            staging_prefix="staging",
            upload=False,
            hash_index_path=hash_index_path,
            update_hash_index=False,
        )

        assert not Path(hash_index_path).exists()
