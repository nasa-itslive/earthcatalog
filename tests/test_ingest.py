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
