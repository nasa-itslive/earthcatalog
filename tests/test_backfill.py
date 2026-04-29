"""
Tests for earthcatalog.pipelines.backfill.

Uses MemoryStore for unit tests — no real S3 traffic.
Integration tests (marked ``pytest.mark.integration``) fetch from the
public ITS_LIVE bucket and are skipped unless ``-m integration`` is passed.
"""

import io
import json

import dask
import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.transform import write_geoparquet as _write_geoparquet
from earthcatalog.grids.h3_partitioner import H3Partitioner

_PARTITIONER = H3Partitioner(resolution=2)

_STAC_ITEMS = [
    {
        "id": f"v2-item-{i:04d}",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-10 + i, 60],
                    [10 + i, 60],
                    [10 + i, 70],
                    [-10 + i, 70],
                    [-10 + i, 60],
                ]
            ],
        },
        "properties": {
            "datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-15T00:00:00Z",
            "platform": "NISAR",
            "percent_valid_pixels": float(80 + i),
            "date_dt": float(12 + i),
            "proj:code": "EPSG:32632",
            "sat:orbit_state": "ascending",
            "scene_1_id": f"scene1_{i}",
            "scene_2_id": f"scene2_{i}",
            "scene_1_frame": f"frame1_{i}",
            "scene_2_frame": f"frame2_{i}",
            "version": "002",
            "start_datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-10T00:00:00Z",
            "end_datetime": f"202{i % 4 + 1}-0{i % 9 + 1}-20T00:00:00Z",
            "updated": f"202{i % 4 + 1}-06-01T00:00:00Z",
        },
        "links": [],
        "assets": {},
    }
    for i in range(8)
]

_ITEM_MAP = {
    ("mock-bucket", f"stac/item-{i:04d}.stac.json"): item for i, item in enumerate(_STAC_ITEMS)
}

_ALL_PAIRS = list(_ITEM_MAP.keys())


def _seed_item_store() -> MemoryStore:
    """Create a MemoryStore pre-loaded with mock STAC JSON items."""
    store = MemoryStore()
    for (bucket, key), item in _ITEM_MAP.items():
        full_key = f"{bucket}/{key}"
        obstore.put(store, full_key, json.dumps(item).encode())
    return store


def _mock_fetch_item(bucket: str, key: str) -> dict | None:
    return _ITEM_MAP.get((bucket, key))


def _make_inventory_csv(pairs: list[tuple[str, str]]) -> str:
    """Return CSV content for an inventory."""
    lines = ["bucket,key"]
    for b, k in pairs:
        lines.append(f"{b},{k}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 1 — Scheduler tests
# ---------------------------------------------------------------------------


class TestWriteChunks:
    def test_writes_chunk_parquets(self, tmp_path):
        from earthcatalog.pipelines.backfill import write_chunks

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        store = MemoryStore()
        keys = write_chunks(str(inv), store, "chunks", chunk_size=4)

        assert len(keys) == 2  # 8 items / 4 per chunk = 2 chunks

        for key in keys:
            raw = memoryview(obstore.get(store, key).bytes()).tobytes()
            tbl = pq.ParquetFile(io.BytesIO(raw)).read()
            assert tbl.num_rows == 4
            assert set(tbl.schema.names) == {"bucket", "key"}

    def test_skips_already_written_chunks(self, tmp_path):
        from earthcatalog.pipelines.backfill import write_chunks

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        store = MemoryStore()
        keys1 = write_chunks(str(inv), store, "chunks", chunk_size=4)
        keys2 = write_chunks(str(inv), store, "chunks", chunk_size=4)

        assert keys1 == keys2
        assert all(keys1)

    def test_limit_caps_items(self, tmp_path):
        from earthcatalog.pipelines.backfill import write_chunks

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        store = MemoryStore()
        keys = write_chunks(str(inv), store, "chunks", chunk_size=100, limit=3)

        assert len(keys) == 1
        raw = memoryview(obstore.get(store, keys[0]).bytes()).tobytes()
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.num_rows == 3

    def test_filters_non_stac_json(self, tmp_path):
        from earthcatalog.pipelines.backfill import write_chunks

        mixed_pairs = _ALL_PAIRS + [("mock-bucket", "data/file.parquet")]
        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(mixed_pairs))

        store = MemoryStore()
        keys = write_chunks(str(inv), store, "chunks", chunk_size=100)

        raw = memoryview(obstore.get(store, keys[0]).bytes()).tobytes()
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        all_keys = tbl.column("key").to_pylist()
        assert all(k.endswith(".stac.json") for k in all_keys)
        assert len(all_keys) == len(_ALL_PAIRS)


# ---------------------------------------------------------------------------
# Phase 2 — Ingest worker tests
# ---------------------------------------------------------------------------


class TestFetchItemAsync:
    def test_fetches_from_real_memory_store(self):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        store = MemoryStore()
        bucket, key = _ALL_PAIRS[0]
        obstore.put(store, key, json.dumps(_ITEM_MAP[(bucket, key)]).encode())

        result = asyncio.run(_fetch_item_async(store, bucket, key))
        assert result is not None
        assert result["id"] == "v2-item-0000"

    def test_missing_key_returns_none(self):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        store = MemoryStore()
        result = asyncio.run(_fetch_item_async(store, "bucket", "nonexistent.json"))
        assert result is None

    def test_403_raises_immediately(self):
        import asyncio
        from unittest.mock import patch

        from earthcatalog.pipelines.backfill import _fetch_item_async

        async def _raise_403(store, key):
            raise PermissionError("403 Forbidden")

        with patch("earthcatalog.pipelines.backfill.obstore.get_async", side_effect=_raise_403):
            with pytest.raises(PermissionError):
                asyncio.run(_fetch_item_async(object(), "bucket", "secret.json"))


class TestFetchAllAsync:
    def test_fetches_multiple_items(self):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_all_async

        store = MemoryStore()
        pairs = _ALL_PAIRS[:4]
        for bucket, key in pairs:
            obstore.put(store, key, json.dumps(_ITEM_MAP[(bucket, key)]).encode())

        items, failed = asyncio.run(_fetch_all_async(pairs, concurrency=10, store=store))

        assert len(items) == 4
        assert len(failed) == 0

    def test_tracks_failures(self):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_all_async

        store = MemoryStore()
        bucket, key = _ALL_PAIRS[0]
        obstore.put(store, key, json.dumps(_ITEM_MAP[(bucket, key)]).encode())

        items, failed = asyncio.run(_fetch_all_async(_ALL_PAIRS, concurrency=10, store=store))

        assert len(items) == 1
        assert len(failed) == 7


class TestWriteNdjson:
    def test_writes_ndjson_to_store(self):
        from earthcatalog.pipelines.backfill import _write_ndjson

        store = MemoryStore()
        items = [{"id": "a"}, {"id": "b"}]
        n = _write_ndjson(items, store, "test.ndjson")

        assert n == 2
        raw = memoryview(obstore.get(store, "test.ndjson").bytes()).tobytes().decode()
        lines = [ln for ln in raw.split("\n") if ln.strip()]
        assert len(lines) == 2
        assert json.loads(lines[0])["id"] == "a"

    def test_empty_list_returns_zero(self):
        from earthcatalog.pipelines.backfill import _write_ndjson

        store = MemoryStore()
        n = _write_ndjson([], store, "test.ndjson")
        assert n == 0

    def test_appends_to_existing_ndjson(self):
        import orjson

        from earthcatalog.pipelines.backfill import _write_ndjson

        store = MemoryStore()
        _write_ndjson([{"id": "a"}, {"id": "b"}], store, "test.ndjson")
        n = _write_ndjson([{"id": "c"}], store, "test.ndjson")
        assert n == 1
        raw = memoryview(obstore.get(store, "test.ndjson").bytes()).tobytes().decode()
        lines = [ln for ln in raw.split("\n") if ln.strip()]
        assert len(lines) == 3
        assert orjson.loads(lines[2])["id"] == "c"


class TestListCompletedChunkIds:
    def test_returns_empty_when_no_markers(self):
        from earthcatalog.pipelines.backfill import _list_completed_chunk_ids

        store = MemoryStore()
        result = _list_completed_chunk_ids(store, "staging", "pending_chunks")
        assert result == set()

    def test_returns_ids_with_marker_and_no_pending(self):
        from earthcatalog.pipelines.backfill import _list_completed_chunk_ids

        store = MemoryStore()
        obstore.put(store, "staging/_completed/chunk_000001.done", b"ok")
        obstore.put(store, "staging/_completed/chunk_000002.done", b"ok")

        result = _list_completed_chunk_ids(store, "staging", "pending_chunks")
        assert result == {1, 2}

    def test_excludes_ids_with_pending_file(self):
        from earthcatalog.pipelines.backfill import _list_completed_chunk_ids

        store = MemoryStore()
        obstore.put(store, "staging/_completed/chunk_000001.done", b"ok")
        obstore.put(store, "staging/_completed/chunk_000002.done", b"ok")
        obstore.put(store, "pending_chunks/chunk_000002.parquet", b"pending")

        result = _list_completed_chunk_ids(store, "staging", "pending_chunks")
        assert result == {1}

    def test_ndjson_without_marker_means_incomplete(self):
        from earthcatalog.pipelines.backfill import _list_completed_chunk_ids

        store = MemoryStore()
        obstore.put(store, "staging/8001/2020/chunk_000005.ndjson", b'{"id":"a"}')

        result = _list_completed_chunk_ids(store, "staging", "pending_chunks")
        assert result == set()

    def test_ignores_non_chunk_markers(self):
        from earthcatalog.pipelines.backfill import _list_completed_chunk_ids

        store = MemoryStore()
        obstore.put(store, "staging/_completed/not_a_chunk.done", b"ok")
        obstore.put(store, "staging/_completed/chunk_notanumber.done", b"ok")

        result = _list_completed_chunk_ids(store, "staging", "pending_chunks")
        assert result == set()


class TestIngestChunk:
    def test_reads_chunk_and_writes_ndjson(self, tmp_path):
        from unittest.mock import patch

        from earthcatalog.pipelines.backfill import ingest_chunk

        store = MemoryStore()

        # Write a chunk parquet
        tbl = pa.table({"bucket": [b for b, _ in _ALL_PAIRS], "key": [k for _, k in _ALL_PAIRS]})
        buf = io.BytesIO()
        pq.write_table(tbl, buf)
        obstore.put(store, "chunks/chunk_000000.parquet", buf.getvalue())

        async def _mock(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock):
            report = ingest_chunk(
                chunk_key="chunks/chunk_000000.parquet",
                staging_store=store,
                staging_prefix="staging",
                pending_prefix="pending_chunks",
                partitioner=_PARTITIONER,
            )

        assert report["source_items"] == 8
        assert report["fetched_items"] == 8
        assert report["fetch_failures"] == 0
        assert report["groups_written"] > 0
        assert len(report["ndjson_keys"]) > 0

        for nk in report["ndjson_keys"]:
            assert nk.startswith("staging/")
            assert nk.endswith(".ndjson")


# ---------------------------------------------------------------------------
# Phase 3 — Compact tests
# ---------------------------------------------------------------------------


class TestReadNdjsonFiles:
    def test_reads_multiple_files(self):
        from earthcatalog.pipelines.backfill import _read_ndjson_files

        store = MemoryStore()
        items_a = [{"id": "1", "properties": {"updated": "2024-01-01T00:00:00Z"}}]
        items_b = [{"id": "2", "properties": {"updated": "2024-01-01T00:00:00Z"}}]

        obstore.put(store, "a.ndjson", "\n".join(json.dumps(i) for i in items_a).encode())
        obstore.put(store, "b.ndjson", "\n".join(json.dumps(i) for i in items_b).encode())

        result = _read_ndjson_files(store, ["a.ndjson", "b.ndjson"])
        assert len(result) == 2

    def test_skips_missing_files(self):
        from earthcatalog.pipelines.backfill import _read_ndjson_files

        store = MemoryStore()
        result = _read_ndjson_files(store, ["nonexistent.ndjson"])
        assert result == []


class TestDedupItems:
    def test_deduplicates_by_id(self):
        from earthcatalog.pipelines.backfill import _dedup_items

        items = [
            {"id": "a", "properties": {"updated": "2024-01-01T00:00:00Z"}},
            {"id": "a", "properties": {"updated": "2024-06-01T00:00:00Z"}},
            {"id": "b", "properties": {"updated": "2024-01-01T00:00:00Z"}},
        ]
        result = _dedup_items(items)
        assert len(result) == 2
        by_id = {i["id"]: i for i in result}
        assert by_id["a"]["properties"]["updated"] == "2024-06-01T00:00:00Z"

    def test_keeps_all_unique(self):
        from earthcatalog.pipelines.backfill import _dedup_items

        items = [{"id": f"item-{i}", "properties": {}} for i in range(10)]
        result = _dedup_items(items)
        assert len(result) == 10


class TestCompactCellYear:
    def test_reads_ndjson_and_writes_parquet(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()

        from earthcatalog.core.transform import fan_out, group_by_partition

        fo = fan_out(_STAC_ITEMS, _PARTITIONER)
        groups = group_by_partition(fo)

        (cell, year), items = list(groups.items())[0]

        for item in items:
            item["properties"]["updated"] = "2024-01-01T00:00:00Z"

        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        ndjson_key = f"staging/{cell}/{year}/worker_abc123.ndjson"
        obstore.put(store, ndjson_key, ndjson_data)

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        report = compact_cell_year(
            cell=cell,
            year=str(year),
            staging_store=store,
            staging_prefix="staging",
            warehouse_root=str(wh_root),
            compact_rows=100_000,
        )

        assert report["cell"] == cell
        assert report["year"] == str(year)
        assert report["input_files"] == 1
        assert report["input_items"] == len(items)
        assert report["unique_items"] == len(items)
        assert report["output_rows"] > 0
        assert len(report["output_files"]) == 1

        parquet_path = report["output_files"][0]
        assert parquet_path.startswith(str(wh_root))
        tbl = pq.ParquetFile(parquet_path).read()
        assert tbl.num_rows == len(items)

    def test_empty_prefix_returns_empty_report(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()
        report = compact_cell_year(
            cell="nocell",
            year="2024",
            staging_store=store,
            staging_prefix="staging",
            warehouse_root=str(tmp_path),
        )
        assert report["input_files"] == 0
        assert report["output_rows"] == 0

    def test_splits_into_multiple_parts(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year

        store = MemoryStore()
        cell = "testcell"
        year = "2024"

        items = [
            {
                "id": f"item-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-01-01T00:00:00Z",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
            for i in range(5)
        ]

        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        obstore.put(store, f"staging/{cell}/{year}/worker_001.ndjson", ndjson_data)

        report = compact_cell_year(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_root=str(tmp_path),
            compact_rows=2,
        )

        assert len(report["output_files"]) == 3  # 5 items / 2 per part = 3 parts
        assert report["output_rows"] == 5


class TestCompactCellYearS3:
    def test_writes_parquet_to_store(self):
        from earthcatalog.pipelines.backfill import compact_cell_year_s3

        store = MemoryStore()
        wh_store = MemoryStore()
        cell = "tests3cell"
        year = "2024"

        items = [
            {
                "id": f"s3item-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-01-01T00:00:00Z",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
            for i in range(3)
        ]

        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        obstore.put(store, f"staging/{cell}/{year}/worker_001.ndjson", ndjson_data)

        report = compact_cell_year_s3(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=wh_store,
            compact_rows=100_000,
        )

        assert report["output_rows"] == 3
        assert len(report["output_files"]) == 1

        out_key = report["output_files"][0]
        raw = memoryview(obstore.get(wh_store, out_key).bytes()).tobytes()
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        assert tbl.num_rows == 3


# ---------------------------------------------------------------------------
# Phase 4 — Register tests
# ---------------------------------------------------------------------------


class TestRegisterAndCleanup:
    def test_registers_files_and_cleans_up(self, tmp_path):
        from earthcatalog.pipelines.backfill import register_and_cleanup

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        staging_store = MemoryStore()

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        cell_dir = wh_root / "grid_partition=testcell" / "year=2024"
        cell_dir.mkdir(parents=True)

        from earthcatalog.core.transform import fan_out as _fan_out
        from earthcatalog.core.transform import write_geoparquet as _write_gp

        items = [_STAC_ITEMS[0]]
        fo = _fan_out(items, _PARTITIONER)
        if fo:
            from earthcatalog.core.transform import group_by_partition as _gp

            groups = _gp(fo)
            for (_c, _y), group in groups.items():
                d = wh_root / f"grid_partition={_c}" / f"year={_y}"
                d.mkdir(parents=True, exist_ok=True)
                _write_gp(group, str(d / "part_000000.parquet"))

        # Put some staging files to verify cleanup
        obstore.put(staging_store, "staging/chunks/chunk_000.parquet", b"data")
        obstore.put(staging_store, "staging/testcell/2024/worker_001.ndjson", b"ndjson")

        catalog_path = str(tmp_path / "catalog.db")

        register_and_cleanup(
            catalog_path=catalog_path,
            warehouse_root=str(wh_root),
            staging_store=staging_store,
            staging_prefix="staging",
            upload=False,
        )

        from earthcatalog.core.catalog import open_catalog

        catalog = open_catalog(db_path=catalog_path, warehouse_path=str(wh_root))
        table = catalog.load_table("earthcatalog.stac_items")
        result = table.scan().to_arrow()
        assert result.num_rows > 0

        # Staging files should be deleted
        remaining = []
        for batch in obstore.list(staging_store, prefix="staging"):
            for obj in batch:
                remaining.append(obj["path"])
        assert "staging/chunks/chunk_000.parquet" in remaining
        assert "staging/testcell/2024/worker_001.ndjson" in remaining


# ---------------------------------------------------------------------------
# End-to-end — run_backfill
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRunBackfillV2:
    @pytest.fixture(autouse=True)
    def _setup_store_config(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_end_to_end_with_mock_fetcher(self, tmp_path):
        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        staging_store = MemoryStore()

        # Seed item data into the staging store so _fetch_item_async can find it
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        from unittest.mock import patch

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        from earthcatalog.core.catalog import open_catalog

        catalog = open_catalog(db_path=catalog_path, warehouse_path=str(wh_root))
        table = catalog.load_table("earthcatalog.stac_items")
        result = table.scan().to_arrow()
        assert result.num_rows >= len(_STAC_ITEMS)

        ids = set(result.column("id").to_pylist())
        for item in _STAC_ITEMS:
            assert item["id"] in ids

    def test_idempotent_re_run(self, tmp_path):
        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        staging_store = MemoryStore()
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        from unittest.mock import patch

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        # Run again — same staging store, chunks and NDJSON from run 1 still there
        wh_root2 = tmp_path / "warehouse2"
        wh_root2.mkdir()
        catalog_path2 = str(tmp_path / "catalog2.db")

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path2,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root2),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
                skip_inventory=True,
            )

        from earthcatalog.core.catalog import open_catalog

        catalog = open_catalog(db_path=catalog_path2, warehouse_path=str(wh_root2))
        table = catalog.load_table("earthcatalog.stac_items")
        result = table.scan().to_arrow()
        assert result.num_rows >= len(_STAC_ITEMS)


# ---------------------------------------------------------------------------
# Delta mode tests
# ---------------------------------------------------------------------------


class TestNextPartIndexLocal:
    def test_returns_zero_when_no_files(self, tmp_path):
        from earthcatalog.pipelines.backfill import _next_part_index_local

        assert _next_part_index_local(str(tmp_path), "cell1", "2024") == 0

    def test_returns_next_after_existing(self, tmp_path):
        from earthcatalog.pipelines.backfill import _next_part_index_local

        d = tmp_path / "grid_partition=cell1" / "year=2024"
        d.mkdir(parents=True)
        (d / "part_000000.parquet").touch()
        (d / "part_000001.parquet").touch()

        assert _next_part_index_local(str(tmp_path), "cell1", "2024") == 2

    def test_handles_gaps(self, tmp_path):
        from earthcatalog.pipelines.backfill import _next_part_index_local

        d = tmp_path / "grid_partition=cell1" / "year=2024"
        d.mkdir(parents=True)
        (d / "part_000000.parquet").touch()
        (d / "part_000005.parquet").touch()

        assert _next_part_index_local(str(tmp_path), "cell1", "2024") == 6

    def test_ignores_non_parquet(self, tmp_path):
        from earthcatalog.pipelines.backfill import _next_part_index_local

        d = tmp_path / "grid_partition=cell1" / "year=2024"
        d.mkdir(parents=True)
        (d / "part_000000.parquet").touch()
        (d / "notes.txt").touch()

        assert _next_part_index_local(str(tmp_path), "cell1", "2024") == 1


class TestNextPartIndexS3:
    def test_returns_zero_when_empty(self):
        from earthcatalog.pipelines.backfill import _next_part_index_s3

        store = MemoryStore()
        assert _next_part_index_s3(store, "cell1", "2024") == 0

    def test_returns_next_after_existing(self):
        from earthcatalog.pipelines.backfill import _next_part_index_s3

        store = MemoryStore()
        obstore.put(store, "grid_partition=cell1/year=2024/part_000000.parquet", b"data")
        obstore.put(store, "grid_partition=cell1/year=2024/part_000001.parquet", b"data")

        assert _next_part_index_s3(store, "cell1", "2024") == 2


class TestCompactCellYearDelta:
    def test_writes_new_files_without_overwriting(self, tmp_path):
        from earthcatalog.pipelines.backfill import compact_cell_year_delta

        store = MemoryStore()
        cell = "deltacell"
        year = "2024"

        existing_items = [
            {
                "id": f"old-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-01-01T00:00:00Z",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
            for i in range(3)
        ]

        wh_root = tmp_path / "warehouse"
        cell_dir = wh_root / f"grid_partition={cell}" / f"year={year}"
        cell_dir.mkdir(parents=True)
        _write_geoparquet(existing_items, str(cell_dir / "part_000000.parquet"))

        new_items = [
            {
                "id": f"new-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-06-01T00:00:00Z",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
            for i in range(2)
        ]
        ndjson_data = "\n".join(json.dumps(i) for i in new_items).encode()
        obstore.put(store, f"staging/{cell}/{year}/chunk_000.ndjson", ndjson_data)

        report = compact_cell_year_delta(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_root=str(wh_root),
            compact_rows=100_000,
        )

        assert report["output_rows"] == 2
        assert len(report["output_files"]) == 1

        new_path = report["output_files"][0]
        assert "part_000001.parquet" in new_path

        existing_tbl = pq.ParquetFile(str(cell_dir / "part_000000.parquet")).read()
        assert existing_tbl.num_rows == 3

        new_tbl = pq.ParquetFile(new_path).read()
        assert new_tbl.num_rows == 2


class TestCompactCellYearDeltaS3:
    def test_writes_new_files_with_offset(self):
        from earthcatalog.pipelines.backfill import compact_cell_year_delta_s3

        store = MemoryStore()
        wh_store = MemoryStore()
        cell = "s3delta"
        year = "2024"

        obstore.put(wh_store, f"grid_partition={cell}/year={year}/part_000000.parquet", b"old")
        obstore.put(wh_store, f"grid_partition={cell}/year={year}/part_000001.parquet", b"old")

        items = [
            {
                "id": f"delta-s3-{i}",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-01-01T00:00:00Z",
                    "grid_partition": cell,
                },
                "links": [],
                "assets": {},
            }
            for i in range(2)
        ]
        ndjson_data = "\n".join(json.dumps(i) for i in items).encode()
        obstore.put(store, f"staging/{cell}/{year}/chunk_000.ndjson", ndjson_data)

        report = compact_cell_year_delta_s3(
            cell=cell,
            year=year,
            staging_store=store,
            staging_prefix="staging",
            warehouse_store=wh_store,
            compact_rows=100_000,
        )

        assert report["output_rows"] == 2
        assert len(report["output_files"]) == 1
        assert "part_000002.parquet" in report["output_files"][0]

        old_data = memoryview(
            obstore.get(wh_store, f"grid_partition={cell}/year={year}/part_000000.parquet").bytes()
        ).tobytes()
        assert old_data == b"old"


class TestRegisterDelta:
    def test_adds_files_to_existing_table(self, tmp_path):
        from earthcatalog.pipelines.backfill import register_delta

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )

        catalog = open_catalog(db_path=str(tmp_path / "existing.db"), warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )

        cell_dir = wh_root / "grid_partition=testcell" / "year=2024"
        cell_dir.mkdir(parents=True)

        existing_items = [
            {
                "id": "existing-1",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-01-01T00:00:00Z",
                    "grid_partition": "testcell",
                },
                "links": [],
                "assets": {},
            }
        ]
        existing_path = str(cell_dir / "part_000000.parquet")
        _write_geoparquet(existing_items, existing_path)

        catalog.load_table(FULL_NAME).add_files([existing_path])

        new_items = [
            {
                "id": "delta-1",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "datetime": "2024-01-15T00:00:00Z",
                    "platform": "TEST",
                    "updated": "2024-06-01T00:00:00Z",
                    "grid_partition": "testcell",
                },
                "links": [],
                "assets": {},
            }
        ]
        new_path = str(cell_dir / "part_000001.parquet")
        _write_geoparquet(new_items, new_path)

        staging_store = MemoryStore()

        register_delta(
            catalog_path=str(tmp_path / "existing.db"),
            warehouse_root=str(wh_root),
            new_parquet_paths=[new_path],
            staging_store=staging_store,
            staging_prefix="staging",
            upload=False,
        )

        result = catalog.load_table(FULL_NAME).scan().to_arrow()
        assert result.num_rows == 2
        ids = set(result.column("id").to_pylist())
        assert "existing-1" in ids
        assert "delta-1" in ids

    def test_register_delta_sets_hash_index_path(self, tmp_path):
        from earthcatalog.core import store_config
        from earthcatalog.core.catalog import PROP_HASH_INDEX_PATH

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )

        db_path = str(tmp_path / "test.db")
        catalog = open_catalog(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )

        staging_store = MemoryStore()

        from earthcatalog.pipelines.backfill import register_delta

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[],
            staging_store=staging_store,
            staging_prefix="staging",
            upload=False,
        )

        table = catalog.load_table(FULL_NAME)
        assert table.properties.get(PROP_HASH_INDEX_PATH) == f"{wh_root}_id_hashes.parquet"

    def test_register_delta_custom_hash_index_path(self, tmp_path):
        from earthcatalog.core import store_config
        from earthcatalog.core.catalog import PROP_HASH_INDEX_PATH

        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()

        from earthcatalog.core.catalog import (
            FULL_NAME,
            ICEBERG_SCHEMA,
            NAMESPACE,
            PARTITION_SPEC,
            open_catalog,
        )

        db_path = str(tmp_path / "test.db")
        catalog = open_catalog(db_path=db_path, warehouse_path=str(wh_root))
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )

        staging_store = MemoryStore()
        custom_path = "s3://my-bucket/custom_hashes.parquet"

        from earthcatalog.pipelines.backfill import register_delta

        register_delta(
            catalog_path=db_path,
            warehouse_root=str(wh_root),
            new_parquet_paths=[],
            staging_store=staging_store,
            staging_prefix="staging",
            upload=False,
            hash_index_path=custom_path,
        )

        table = catalog.load_table(FULL_NAME)
        assert table.properties.get(PROP_HASH_INDEX_PATH) == custom_path


@pytest.mark.e2e
class TestRunBackfillV2Delta:
    @pytest.fixture(autouse=True)
    def _setup_store_config(self):
        store_config.set_store(MemoryStore())
        store_config.set_catalog_key("catalog.db")
        yield

    def test_delta_does_not_overwrite_existing_parquets(self, tmp_path):
        from earthcatalog.pipelines.backfill import run_backfill

        inv = tmp_path / "inv.csv"
        inv.write_text(_make_inventory_csv(_ALL_PAIRS))

        staging_store = MemoryStore()
        for (bucket, key), item in _ITEM_MAP.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())

        wh_root = tmp_path / "warehouse"
        wh_root.mkdir()
        catalog_path = str(tmp_path / "catalog.db")

        from unittest.mock import patch

        async def _mock_fetch(store, bucket, key):
            return _ITEM_MAP.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv),
                catalog_path=catalog_path,
                staging_store=staging_store,
                staging_prefix="ingest",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
            )

        from earthcatalog.core.catalog import open_catalog

        catalog = open_catalog(db_path=catalog_path, warehouse_path=str(wh_root))
        table = catalog.load_table("earthcatalog.stac_items")
        result1 = table.scan().to_arrow()
        first_run_count = result1.num_rows
        first_run_ids = set(result1.column("id").to_pylist())

        inv2 = tmp_path / "inv2.csv"
        extra_pairs = [
            ("mock-bucket", "stac/item-extra-0.stac.json"),
            ("mock-bucket", "stac/item-extra-1.stac.json"),
        ]
        extra_items = [
            {
                "id": "v2-item-extra-0",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-10, 60], [10, 60], [10, 70], [-10, 70], [-10, 60]]],
                },
                "properties": {
                    "datetime": "2023-01-15T00:00:00Z",
                    "platform": "NISAR",
                    "percent_valid_pixels": 90.0,
                    "date_dt": 15.0,
                    "proj:code": "EPSG:32632",
                    "sat:orbit_state": "ascending",
                    "scene_1_id": "s1",
                    "scene_2_id": "s2",
                    "scene_1_frame": "f1",
                    "scene_2_frame": "f2",
                    "version": "002",
                    "start_datetime": "2023-01-10T00:00:00Z",
                    "end_datetime": "2023-01-20T00:00:00Z",
                    "updated": "2023-06-01T00:00:00Z",
                },
                "links": [],
                "assets": {},
            },
            {
                "id": "v2-item-extra-1",
                "type": "Feature",
                "stac_version": "1.0.0",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-10, 60], [10, 60], [10, 70], [-10, 70], [-10, 60]]],
                },
                "properties": {
                    "datetime": "2023-02-15T00:00:00Z",
                    "platform": "NISAR",
                    "percent_valid_pixels": 85.0,
                    "date_dt": 14.0,
                    "proj:code": "EPSG:32632",
                    "sat:orbit_state": "ascending",
                    "scene_1_id": "s3",
                    "scene_2_id": "s4",
                    "scene_1_frame": "f3",
                    "scene_2_frame": "f4",
                    "version": "002",
                    "start_datetime": "2023-02-10T00:00:00Z",
                    "end_datetime": "2023-02-20T00:00:00Z",
                    "updated": "2023-06-01T00:00:00Z",
                },
                "links": [],
                "assets": {},
            },
        ]

        extra_item_map = dict(zip(extra_pairs, extra_items))
        all_item_map = {**_ITEM_MAP, **extra_item_map}

        for (bucket, key), item in extra_item_map.items():
            obstore.put(staging_store, f"{bucket}/{key}", json.dumps(item).encode())
        inv2.write_text(_make_inventory_csv(extra_pairs))

        catalog_path2 = str(tmp_path / "catalog2.db")
        import shutil

        shutil.copy(catalog_path, catalog_path2)

        async def _mock_fetch_all(store, bucket, key):
            return all_item_map.get((bucket, key))

        with (
            patch("earthcatalog.pipelines.backfill._fetch_item_async", side_effect=_mock_fetch_all),
            dask.config.set(scheduler="synchronous"),
        ):
            run_backfill(
                inventory_path=str(inv2),
                catalog_path=catalog_path2,
                staging_store=staging_store,
                staging_prefix="ingest2",
                warehouse_store=MemoryStore(),
                warehouse_root=str(wh_root),
                partitioner=_PARTITIONER,
                chunk_size=4,
                use_lock=False,
                delta=True,
            )

        catalog2 = open_catalog(db_path=catalog_path2, warehouse_path=str(wh_root))
        table2 = catalog2.load_table("earthcatalog.stac_items")
        result2 = table2.scan().to_arrow()
        second_run_count = result2.num_rows
        second_run_ids = set(result2.column("id").to_pylist())

        assert second_run_count == first_run_count + 56
        for item_id in first_run_ids:
            assert item_id in second_run_ids
        assert "v2-item-extra-0" in second_run_ids
        assert "v2-item-extra-1" in second_run_ids

        for cell_dir in (wh_root).glob("grid_partition=*"):
            for year_dir in cell_dir.glob("year=*"):
                part_files = sorted(year_dir.glob("part_*.parquet"))
                part_names = [p.name for p in part_files]
                assert len(part_names) == len(set(part_names)), (
                    f"Duplicate parquet names in {year_dir}"
                )


# ---------------------------------------------------------------------------
# Integration tests — fetch real STAC items from public ITS_LIVE bucket
# ---------------------------------------------------------------------------

_REAL_ITEM_KEY = (
    "velocity_image_pair/landsatOLI/v02/N60W030/"
    "LC08_L1TP_232011_20190524_20200828_02_T1_X_LC08_L1TP_232011_20190711_20200827_02_T1_G0120V02_P056.stac.json"
)


@pytest.mark.integration
class TestFetchRealS3:
    """Fetch real STAC items from the public its-live-data bucket."""

    @pytest.fixture()
    def public_store(self):
        from obstore.store import S3Store

        return S3Store(bucket="its-live-data", region="us-west-2", skip_signature=True)

    def test_fetch_single_item(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        result = asyncio.run(_fetch_item_async(public_store, "its-live-data", _REAL_ITEM_KEY))
        assert result is not None
        assert result["type"] == "Feature"
        assert "geometry" in result
        assert "properties" in result

    def test_fetch_all_with_real_store(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_all_async

        pairs = [("its-live-data", _REAL_ITEM_KEY)]
        items, failed = asyncio.run(_fetch_all_async(pairs, concurrency=10, store=public_store))

        assert len(items) == 1
        assert len(failed) == 0
        assert items[0]["type"] == "Feature"

    def test_fetch_missing_key_returns_none(self, public_store):
        import asyncio

        from earthcatalog.pipelines.backfill import _fetch_item_async

        result = asyncio.run(
            _fetch_item_async(public_store, "its-live-data", "nonexistent/file.stac.json")
        )
        assert result is None
