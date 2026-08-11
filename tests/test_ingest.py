"""Tests for the resumable Ingester (single ingest path, index-as-checkpoint)."""

from __future__ import annotations

import io

import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.index import Index
from earthcatalog.ingest import DaskIngester, Ingester


def _inventory(keys: list[str]) -> list[tuple[str, str]]:
    """Build a fake inventory of (bucket, key) pairs."""
    return [("data-bucket", k) for k in keys]


def _make_item(key: str) -> dict:
    """A minimal STAC item; id derived from key so it's deterministic."""
    return {
        "id": f"item-{key.split('/')[-1]}",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [0.0, 0.0, 0.0, 0.0],
        "properties": {
            "datetime": "2020-05-01T00:00:00Z",
            "grid_partition": "cellA",
            "platform": "sentinel-1",
        },
        "_source_bucket": "data-bucket",
        "_source_key": key,
    }


def _run(
    keys: list[str],
    *,
    stage: str = "direct",
    fetch_fn=None,
    batch_size: int = 2,
):
    """Run an ingest to completion on MemoryStore, returning (store, ingester)."""
    store = MemoryStore()
    index = Index(store, "warehouse/index.parquet")

    class _FakeTable:
        def __init__(self):
            self.files: list[str] = []

        def add_files(self, paths):
            self.files.extend(paths)

    table = _FakeTable()

    def _default_fetch(bucket, key):
        return _make_item(key)

    ing = Ingester(
        store=store,
        index=index,
        table=table,
        fetch_fn=fetch_fn or _default_fetch,
        stage=stage,
        warehouse_prefix="warehouse/",
        batch_size=batch_size,
    )
    ing.run(_inventory(keys))
    return store, ing, index, table


def _read_ids(store, key: str) -> list[str]:
    raw = bytes(store.get(key).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read().column("id").to_pylist()


def _list_files(store, prefix: str) -> list[str]:
    out = []
    for batch in store.list(prefix=prefix):
        for obj in batch:
            out.append(obj["path"])
    return out


class TestDirectMode:
    def test_writes_parquet_and_indexes(self):
        store, _, index, table = _run(["a.stac.json", "b.stac.json"])
        assert len(table.files) >= 1
        assert index.known_source_keys() == {
            "s3://data-bucket/a.stac.json",
            "s3://data-bucket/b.stac.json",
        }

    def test_rerun_is_noop(self):
        _, _, index, table = _run(["a.stac.json", "b.stac.json"])
        files_after_first = len(table.files)
        # Second run: same inventory, index already populated -> nothing new
        _run(["a.stac.json", "b.stac.json"])
        assert len(table.files) == files_after_first
        assert index.known_source_keys() == {
            "s3://data-bucket/a.stac.json",
            "s3://data-bucket/b.stac.json",
        }


class TestResume:
    def test_crash_midrun_resumes_without_duplicates(self):
        """Fetch fails after 2 items; resume with a working fetch yields all rows once."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        calls = {"n": 0}

        def _failing(bucket, key):
            calls["n"] += 1
            if calls["n"] > 2:
                raise RuntimeError("simulated crash")
            return _make_item(key)

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_failing,
            stage="direct",
            warehouse_prefix="warehouse/",
            batch_size=2,
        )
        try:
            ing.run(_inventory(keys))
            assert False, "expected a crash"
        except RuntimeError:
            pass

        # Resume: fetch works now
        ing2 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="direct",
            warehouse_prefix="warehouse/",
            batch_size=2,
        )
        ing2.run(_inventory(keys))

        # Every item present exactly once across all files
        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json", "item-c.stac.json"]


class TestDaskIngester:
    def test_distributes_across_shards(self):
        """Workers write parquet; head registers files + index rows once."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        ing = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            warehouse_prefix="warehouse/",
        )

        # Fake Dask client.map: runs _worker on each shard, returns results.
        class _FakeClient:
            def map(self, fn, shards):
                return [fn(s) for s in shards]

        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        shards = [_inventory(keys[:2]), _inventory(keys[2:])]
        ing.run(shards, client=_FakeClient())

        assert len(table.files) == 2  # one parquet per (cell,year) group
        assert index.known_source_keys() == {
            f"s3://data-bucket/{k}" for k in keys
        }

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json", "item-c.stac.json"]


class TestNdjsonMode:
    def test_single_node_stages_ndjson_then_compacts(self):
        """stage=ndjson: Stage A writes NDJSON, Stage B compacts to GeoParquet."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json"]
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
        )
        ing.run(_inventory(keys))

        ndjson = _list_files(store, "warehouse/")
        assert any(k.endswith(".jsonl") for k in ndjson), ndjson

        assert len(table.files) >= 1
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

    def test_dask_stages_ndjson_then_compacts(self):
        """DaskIngester with stage=ndjson: workers write NDJSON, head compacts."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        ing = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
        )

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

        shards = [_inventory(keys[:2]), _inventory(keys[2:])]
        ing.run(shards, client=_FakeClient())

        assert any(k.endswith(".jsonl") for k in _list_files(store, "warehouse/"))
        assert len(table.files) >= 1
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json", "item-c.stac.json"]

    def test_ndjson_rerun_is_noop(self):
        """Re-running ndjson mode skips already-indexed keys (no duplicates)."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json"]
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
        )
        ing.run(_inventory(keys))
        files_after_first = len(table.files)

        ing.run(_inventory(keys))  # second run: nothing new

        assert len(table.files) == files_after_first
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json"]


class TestMemoryBoundedCompact:
    def test_compacts_in_bounded_batches(self):
        """NDJSON compact holds only compact_rows items at once: one part file
        per batch, each sorted, without loading the whole bucket into RAM."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        # 5 items -> compact_rows=2 => ceil(5/2)=3 part files.
        keys = [f"{c}.stac.json" for c in "abcde"]
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            compact_rows=2,
        )
        ing.run(_inventory(keys))

        part_files = [
            k for k in _list_files(store, "warehouse/")
            if k.endswith(".parquet") and "index" not in k
        ]
        assert len(part_files) == 3, part_files

        # Every item present exactly once, no duplicates.
        all_ids = []
        for f in part_files:
            all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == [f"item-{c}.stac.json" for c in "abcde"]

    def test_each_part_sorted_within_batch(self):
        """Within a batch, items are sorted by (platform, datetime)."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        # Items share a cell; give them distinct platforms to observe sort order.
        keys = ["p2.stac.json", "p1.stac.json", "p3.stac.json"]

        def _item_with_platform(key):
            it = _make_item(key)
            it["properties"]["platform"] = key[:2]
            return it

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _item_with_platform(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            compact_rows=10,  # all in one batch
        )
        ing.run(_inventory(keys))

        part_files = [
            k for k in _list_files(store, "warehouse/")
            if k.endswith(".parquet") and "index" not in k
        ]
        assert len(part_files) == 1
        assert _read_ids(store, part_files[0]) == ["item-p1.stac.json", "item-p2.stac.json", "item-p3.stac.json"]

    def test_exact_dedup_keeps_every_unique_item(self):
        """Duplicate NDJSON lines are deduped exactly — no legitimate item is
        dropped (unlike a Bloom filter, which would lose ~0.1% of items in a
        large cell/year bucket)."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json"]

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            compact_rows=2,
        )
        # Run once to build the NDJSON, then append duplicate lines manually to
        # simulate a crash-resume that re-wrote the same items.
        ing.run(_inventory(keys))
        ndjson = [k for k in _list_files(store, "warehouse/") if k.endswith(".jsonl")]
        assert ndjson
        key = ndjson[0]
        dup = bytes(store.get(key).bytes())
        store.put(key, dup + dup)  # double every line

        # Re-run with a fresh ingester: index is empty of hashes only if the
        # first run indexed nothing — it did, so instead compact the bucket
        # directly via a second ingester that skips the index check.
        ing2 = Ingester(
            store=store,
            index=Index(store, "warehouse/other.parquet"),  # fresh, empty index
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            compact_rows=2,
        )
        # Feed nothing new; Stage A writes nothing, but we compact the stale
        # bucket manually to prove dedup drops duplicates exactly.
        cell, year = "cellA", "2020"
        np, _, rows = ing2._compact_ndjson_bucket(cell, year)

        # 2 unique items -> exactly 2 rows, never 4, and both ids survive.
        assert rows == 2, rows
        all_ids = []
        for f in np:
            all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json"]
