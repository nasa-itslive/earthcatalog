"""
Tests for scripts/daily_delta.py and scripts/update_hash_index.py.

Uses MemoryStore — no real S3 traffic.
"""

import io
import json

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xxhash
from obstore.store import MemoryStore

from earthcatalog.core import store_config

_HASH_SEED = 42


def _hash_id(item_id: str) -> bytes:
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


def _make_inventory_parquet(pairs: list[tuple[str, str]]) -> bytes:
    tbl = pa.table(
        {
            "bucket": [b for b, _ in pairs],
            "key": [k for _, k in pairs],
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _make_manifest(data_keys: list[str]) -> bytes:
    manifest = {
        "sourceBucket": "its-live-data",
        "destinationBucket": "arn:aws:s3:::log-bucket",
        "fileFormat": "Parquet",
        "files": [{"key": k, "MD5checksum": "abc123"} for k in data_keys],
    }
    return json.dumps(manifest).encode()


def _make_hash_index_parquet(item_ids: list[str]) -> bytes:
    hashes = sorted(_hash_id(i) for i in item_ids)
    tbl = pa.table({"id_hash": pa.array(hashes, type=pa.binary(16))})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _make_delta_parquet(
    pairs: list[tuple[str, str]], item_ids: list[str]
) -> bytes:
    hashes = [_hash_id(i) for i in item_ids]
    tbl = pa.table(
        {
            "bucket": [b for b, _ in pairs],
            "key": [k for _, k in pairs],
            "id_hash": pa.array(hashes, type=pa.binary(16)),
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# daily_delta._hash_id
# ---------------------------------------------------------------------------


class TestHashId:
    def test_deterministic(self):
        from scripts.daily_delta import _hash_id as hash_fn

        assert hash_fn("item-1") == hash_fn("item-1")

    def test_different_inputs_different_hashes(self):
        from scripts.daily_delta import _hash_id as hash_fn

        assert hash_fn("item-1") != hash_fn("item-2")

    def test_returns_16_bytes(self):
        from scripts.daily_delta import _hash_id as hash_fn

        assert len(hash_fn("item-1")) == 16


# ---------------------------------------------------------------------------
# daily_delta._stream_inventory_hashes
# ---------------------------------------------------------------------------


class TestStreamInventoryHashes:
    def test_filters_stac_json_only(self):
        from scripts.daily_delta import _stream_inventory_hashes

        store = MemoryStore()
        pairs = [
            ("bucket", "path/to/item-1.stac.json"),
            ("bucket", "path/to/item-1.nc"),
            ("bucket", "path/to/item-1.meta"),
            ("bucket", "path/to/item-2.stac.json"),
        ]
        data = _make_inventory_parquet(pairs)
        obstore.put(store, "data/inv.parquet", data)

        manifest = {"files": [{"key": "data/inv.parquet"}]}
        result_pairs, result_hashes = _stream_inventory_hashes(manifest, store)

        assert len(result_pairs) == 2
        assert result_pairs[0] == ("bucket", "path/to/item-1.stac.json")
        assert result_pairs[1] == ("bucket", "path/to/item-2.stac.json")

    def test_extracts_correct_hashes(self):
        from scripts.daily_delta import _stream_inventory_hashes

        store = MemoryStore()
        pairs = [
            ("b", "dir/item-abc.stac.json"),
            ("b", "dir/item-def.stac.json"),
        ]
        data = _make_inventory_parquet(pairs)
        obstore.put(store, "inv.parquet", data)

        manifest = {"files": [{"key": "inv.parquet"}]}
        result_pairs, result_hashes = _stream_inventory_hashes(manifest, store)

        assert result_hashes[0] == _hash_id("item-abc")
        assert result_hashes[1] == _hash_id("item-def")


# ---------------------------------------------------------------------------
# daily_delta._build_hash_set
# ---------------------------------------------------------------------------


class TestBuildHashSet:
    def test_dedup(self):
        from scripts.daily_delta import _build_hash_set

        hashes = [_hash_id("a"), _hash_id("b"), _hash_id("a")]
        result = _build_hash_set(hashes)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# daily_delta._list_pending_deltas
# ---------------------------------------------------------------------------


class TestListPendingDeltas:
    def test_finds_parquets(self):
        from scripts.daily_delta import _list_pending_deltas

        store = MemoryStore()
        obstore.put(store, "pending/delta_2026-04-27.parquet", b"data")
        obstore.put(store, "pending/delta_2026-04-28.parquet", b"data")
        obstore.put(store, "pending/readme.txt", b"text")

        result = _list_pending_deltas(store, "")
        assert len(result) == 2
        assert all(k.endswith(".parquet") for k in result)

    def test_empty(self):
        from scripts.daily_delta import _list_pending_deltas

        store = MemoryStore()
        result = _list_pending_deltas(store, "")
        assert result == []


# ---------------------------------------------------------------------------
# daily_delta._write_delta_parquet / _read_pending_delta
# ---------------------------------------------------------------------------


class TestDeltaParquetRoundTrip:
    def test_write_and_read(self):
        from scripts.daily_delta import _read_pending_delta, _write_delta_parquet

        store = MemoryStore()
        rows = [
            ("bucket-a", "path/item-1.stac.json", _hash_id("item-1")),
            ("bucket-b", "path/item-2.stac.json", _hash_id("item-2")),
        ]
        n = _write_delta_parquet(rows, store, "pending/delta.parquet")
        assert n == 2

        read_back = _read_pending_delta(store, "pending/delta.parquet")
        assert len(read_back) == 2
        assert read_back[0][0] == "bucket-a"
        assert read_back[1][2] == _hash_id("item-2")

    def test_empty_rows(self):
        from scripts.daily_delta import _write_delta_parquet

        store = MemoryStore()
        n = _write_delta_parquet([], store, "pending/delta.parquet")
        assert n == 0


# ---------------------------------------------------------------------------
# daily_delta.run_daily_delta (integration)
# ---------------------------------------------------------------------------


class TestRunDailyDelta:
    @pytest.fixture(autouse=True)
    def _setup(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_produces_delta_for_new_items(self):
        from scripts.daily_delta import run_daily_delta

        store = MemoryStore()

        inv_pairs = [
            ("its-live-data", "dir/item-1.stac.json"),
            ("its-live-data", "dir/item-2.stac.json"),
            ("its-live-data", "dir/item-3.stac.json"),
        ]
        inv_data = _make_inventory_parquet(inv_pairs)
        obstore.put(store, "data/inv.parquet", inv_data)

        manifest = _make_manifest(["data/inv.parquet"])
        obstore.put(store, "manifest.json", manifest)

        wh_data = _make_hash_index_parquet(["item-1", "item-2"])
        obstore.put(store, "warehouse_id_hashes.parquet", wh_data)

        delta_store = MemoryStore()
        obstore.put(delta_store, "pending/.keep", b"")

        from unittest.mock import patch

        with (
            patch("scripts.daily_delta._get_store", side_effect=lambda bucket, **kw: store if bucket == "log-bucket" else delta_store),
        ):
            result = run_daily_delta(
                manifest_uri="s3://log-bucket/manifest.json",
                warehouse_hash_uri="s3://log-bucket/warehouse_id_hashes.parquet",
                delta_prefix="s3://delta-bucket/delta",
                date_str="2026-04-28",
            )

        assert result["new_items"] == 1
        assert result["accumulated_total"] == 1
        assert result["inventory_items"] == 3
        assert result["warehouse_hashes"] == 2
        assert result["skipped"] is False
        assert result["inventory_cached"] is False

    def test_uses_inventory_cache_when_available(self, tmp_path):
        from scripts.daily_delta import (
            _write_inventory_cache_local,
            run_daily_delta,
        )

        delta_dir = tmp_path / "delta"
        pending = delta_dir / "pending"
        pending.mkdir(parents=True)

        inv_rows = [
            ("b", "dir/item-1.stac.json", _hash_id("item-1")),
            ("b", "dir/item-2.stac.json", _hash_id("item-2")),
            ("b", "dir/item-3.stac.json", _hash_id("item-3")),
        ]
        inv_path = pending / "inventory_2026-04-28.parquet"
        _write_inventory_cache_local(inv_rows, str(inv_path))

        wh_path = tmp_path / "warehouse_id_hashes.parquet"
        tbl = pa.table({"id_hash": pa.array(
            [_hash_id("item-1"), _hash_id("item-2")], type=pa.binary(16)
        )})
        pq.write_table(tbl, str(wh_path))

        result = run_daily_delta(
            manifest_uri="s3://log-bucket/manifest.json",
            warehouse_hash_uri=str(wh_path),
            delta_prefix=str(delta_dir),
            date_str="2026-04-28",
        )

        assert result["skipped"] is False
        assert result["inventory_cached"] is True
        assert result["inventory_items"] == 3
        assert result["new_items"] == 1
        assert result["warehouse_hashes"] == 2
        assert result["accumulated_total"] == 1
        assert (pending / "delta_2026-04-28.parquet").exists()

    def test_no_new_items_produces_empty_delta(self):
        from scripts.daily_delta import run_daily_delta

        store = MemoryStore()

        inv_pairs = [("b", "dir/item-1.stac.json")]
        obstore.put(store, "data/inv.parquet", _make_inventory_parquet(inv_pairs))
        obstore.put(store, "manifest.json", _make_manifest(["data/inv.parquet"]))

        wh_data = _make_hash_index_parquet(["item-1"])
        obstore.put(store, "warehouse_id_hashes.parquet", wh_data)

        delta_store = MemoryStore()

        from unittest.mock import patch

        with (
            patch("scripts.daily_delta._get_store", side_effect=lambda bucket, **kw: store if bucket == "log-bucket" else delta_store),
        ):
            result = run_daily_delta(
                manifest_uri="s3://log-bucket/manifest.json",
                warehouse_hash_uri="s3://log-bucket/warehouse_id_hashes.parquet",
                delta_prefix="s3://delta-bucket/delta",
                date_str="2026-04-28",
            )

        assert result["new_items"] == 0
        assert result["accumulated_total"] == 0
        assert result["inventory_cached"] is False


# ---------------------------------------------------------------------------
# daily_delta local I/O
# ---------------------------------------------------------------------------


class TestIsLocal:
    def test_s3_uri_is_not_local(self):
        from scripts.daily_delta import _is_local

        assert _is_local("s3://bucket/key") is False

    def test_absolute_path_is_local(self):
        from scripts.daily_delta import _is_local

        assert _is_local("/tmp/hashes.parquet") is True

    def test_relative_path_is_local(self):
        from scripts.daily_delta import _is_local

        assert _is_local("./data/hashes.parquet") is True


class TestLocalIO:
    def test_hash_index_local_roundtrip(self, tmp_path):
        from scripts.daily_delta import (
            _download_hash_index_local,
        )

        hashes = sorted([_hash_id("item-1"), _hash_id("item-2")])
        tbl = pa.table({"id_hash": pa.array(hashes, type=pa.binary(16))})
        pq.write_table(tbl, str(tmp_path / "hashes.parquet"))

        result = _download_hash_index_local(str(tmp_path / "hashes.parquet"))
        assert len(result) == 2
        assert set(result) == set(hashes)

    def test_list_pending_deltas_local(self, tmp_path):
        from scripts.daily_delta import _list_pending_deltas_local

        pending = tmp_path / "pending"
        pending.mkdir()
        (pending / "delta_2026-04-27.parquet").write_bytes(b"data")
        (pending / "delta_2026-04-28.parquet").write_bytes(b"data")
        (pending / "readme.txt").write_bytes(b"text")

        result = _list_pending_deltas_local(str(tmp_path))
        assert len(result) == 2
        assert all(k.endswith(".parquet") for k in result)

    def test_list_pending_deltas_local_empty(self, tmp_path):
        from scripts.daily_delta import _list_pending_deltas_local

        result = _list_pending_deltas_local(str(tmp_path / "nonexistent"))
        assert result == []

    def test_write_and_read_delta_local(self, tmp_path):
        from scripts.daily_delta import (
            _read_pending_delta_local,
            _write_delta_parquet_local,
        )

        rows = [
            ("bucket-a", "path/item-1.stac.json", _hash_id("item-1")),
            ("bucket-b", "path/item-2.stac.json", _hash_id("item-2")),
        ]
        out_path = str(tmp_path / "pending" / "delta.parquet")
        n = _write_delta_parquet_local(rows, out_path)
        assert n == 2

        read_back = _read_pending_delta_local(out_path)
        assert len(read_back) == 2
        assert read_back[0][0] == "bucket-a"
        assert read_back[1][2] == _hash_id("item-2")

    def test_write_delta_local_empty_rows(self, tmp_path):
        from scripts.daily_delta import _write_delta_parquet_local

        out_path = str(tmp_path / "pending" / "delta.parquet")
        n = _write_delta_parquet_local([], out_path)
        assert n == 0

    def test_inventory_cache_local_roundtrip(self, tmp_path):
        from scripts.daily_delta import (
            _read_inventory_cache_local,
            _write_inventory_cache_local,
        )

        rows = [
            ("b", "dir/item-1.stac.json", _hash_id("item-1")),
            ("b", "dir/item-2.stac.json", _hash_id("item-2")),
            ("b", "dir/item-3.stac.json", _hash_id("item-3")),
        ]
        out_path = str(tmp_path / "pending" / "inventory_2026-04-27.parquet")
        _write_inventory_cache_local(rows, out_path)

        pairs, hashes = _read_inventory_cache_local(out_path)
        assert len(pairs) == 3
        assert pairs[0] == ("b", "dir/item-1.stac.json")
        assert hashes[0] == _hash_id("item-1")
        assert hashes[2] == _hash_id("item-3")


class TestRunDailyDeltaSkip:
    def test_skips_when_delta_exists_locally(self, tmp_path):
        from scripts.daily_delta import run_daily_delta

        existing_rows = [
            ("b", "dir/item-new.stac.json", _hash_id("item-new")),
            ("b", "dir/item-prev.stac.json", _hash_id("item-prev")),
        ]
        delta_dir = tmp_path / "delta"
        delta_dir.mkdir()
        pending = delta_dir / "pending"
        pending.mkdir()
        out_path = pending / "delta_2026-04-27.parquet"

        tbl = pa.table({
            "bucket": pa.array([r[0] for r in existing_rows], type=pa.string()),
            "key": pa.array([r[1] for r in existing_rows], type=pa.string()),
            "id_hash": pa.array([r[2] for r in existing_rows], type=pa.binary(16)),
        })
        pq.write_table(tbl, str(out_path))

        result = run_daily_delta(
            manifest_uri="s3://log-bucket/manifest.json",
            warehouse_hash_uri="/tmp/warehouse_id_hashes.parquet",
            delta_prefix=str(delta_dir),
            date_str="2026-04-27",
        )

        assert result["skipped"] is True
        assert result["accumulated_total"] == 2
        assert result["inventory_items"] == 0
        assert result["new_items"] == 0
        assert result["delta_key"] == str(out_path)

    def test_does_not_skip_when_delta_missing(self, tmp_path):
        from scripts.daily_delta import run_daily_delta

        store = MemoryStore()

        inv_pairs = [("b", "dir/item-1.stac.json")]
        obstore.put(store, "data/inv.parquet", _make_inventory_parquet(inv_pairs))
        obstore.put(store, "manifest.json", _make_manifest(["data/inv.parquet"]))

        wh_data = _make_hash_index_parquet(["item-1"])
        obstore.put(store, "warehouse_id_hashes.parquet", wh_data)

        delta_dir = tmp_path / "delta"

        wh_path = tmp_path / "warehouse_id_hashes.parquet"
        tbl = pa.table({"id_hash": pa.array([_hash_id("item-1")], type=pa.binary(16))})
        pq.write_table(tbl, str(wh_path))

        from unittest.mock import patch

        with (
            patch("scripts.daily_delta._get_store", return_value=store),
        ):
            result = run_daily_delta(
                manifest_uri="s3://log-bucket/manifest.json",
                warehouse_hash_uri=str(wh_path),
                delta_prefix=str(delta_dir),
                date_str="2026-04-28",
            )

        assert result["skipped"] is False
        assert result["new_items"] == 0
        assert result["accumulated_total"] == 0


# ---------------------------------------------------------------------------
# update_hash_index
# ---------------------------------------------------------------------------


class TestUpdateHashIndex:
    def test_appends_new_hashes(self):
        from scripts.update_hash_index import update_from_delta

        delta_data = _make_delta_parquet(
            [("b", "dir/item-3.stac.json"), ("b", "dir/item-4.stac.json")],
            ["item-3", "item-4"],
        )

        store = MemoryStore()
        obstore.put(store, "delta/delta_2026-04-28.parquet", delta_data)

        existing = _make_hash_index_parquet(["item-1", "item-2"])
        obstore.put(store, "warehouse_id_hashes.parquet", existing)

        from unittest.mock import patch

        with patch("scripts.update_hash_index._get_store", return_value=store):
            result = update_from_delta(
                "s3://test-bucket/delta/delta_2026-04-28.parquet",
                "s3://test-bucket/warehouse_id_hashes.parquet",
            )

        assert result["new"] == 2
        assert result["total"] == 4

        raw = bytes(obstore.get(store, "warehouse_id_hashes.parquet").bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.num_rows == 4
        hashes_in_file = set(tbl.column("id_hash").to_pylist())
        assert _hash_id("item-3") in hashes_in_file
        assert _hash_id("item-4") in hashes_in_file

    def test_deduplicates_existing(self):
        from scripts.update_hash_index import update_from_delta

        delta_data = _make_delta_parquet(
            [("b", "dir/item-1.stac.json")],
            ["item-1"],
        )

        store = MemoryStore()
        obstore.put(store, "delta/delta.parquet", delta_data)
        existing = _make_hash_index_parquet(["item-1", "item-2"])
        obstore.put(store, "warehouse_id_hashes.parquet", existing)

        from unittest.mock import patch

        with patch("scripts.update_hash_index._get_store", return_value=store):
            result = update_from_delta(
                "s3://test-bucket/delta/delta.parquet",
                "s3://test-bucket/warehouse_id_hashes.parquet",
            )

        assert result["new"] == 0
        assert result["total"] == 2
