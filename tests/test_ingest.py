"""Tests for the resumable Ingester (single ingest path, index-as-checkpoint)."""

from __future__ import annotations

import io
from unittest.mock import patch

import obstore
import pyarrow.parquet as pq
from obstore.store import MemoryStore

from earthcatalog.index import Index
from earthcatalog.ingest import DaskIngester, Ingester
from earthcatalog.inventory import InventoryShard


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

            def gather(self, results):
                return results

        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        shards = [_inventory(keys[:2]), _inventory(keys[2:])]
        ing.run(shards, client=_FakeClient())

        assert len(table.files) == 2  # one parquet per (cell,year) group
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json", "item-c.stac.json"]


class TestDaskIngesterShardSpecs:
    """DaskIngester accepts InventoryShard specs, not just plain pair lists."""

    def _make(self, store, index, table, *, stage="direct"):
        return DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage=stage,
            warehouse_prefix="warehouse/",
        )

    class _FakeClient:
        def map(self, fn, shards):
            return [fn(s) for s in shards]

        def gather(self, results):
            return results

    def test_pair_spec_shards(self):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        ing = self._make(store, index, table)
        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        shards = [
            InventoryShard(pairs=tuple(_inventory(keys[:2]))),
            InventoryShard(pairs=tuple(_inventory(keys[2:]))),
        ]
        summary = ing.run(shards, client=self._FakeClient())

        assert summary["items"] == 3
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

    def test_file_backed_shard_streams_pairs_on_worker(self):
        """A file-backed shard (inventory part file in a store) is read by
        the worker itself — the head never materialises the pairs."""
        import pyarrow as pa

        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json", "notes.txt"]
        buf = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["data-bucket"] * len(keys), type=pa.string()),
                    "key": pa.array(keys, type=pa.string()),
                }
            ),
            buf,
        )
        store.put("inv/part_0.parquet", buf.getvalue())

        ing = self._make(store, index, table)
        shard = InventoryShard(
            files=("inv/part_0.parquet",),
            store=store,
            suffix=".stac.json",
        )
        ing.run([shard], client=self._FakeClient())

        # notes.txt filtered out by the shard's suffix, on the "worker".
        assert index.known_source_keys() == {
            "s3://data-bucket/a.stac.json",
            "s3://data-bucket/b.stac.json",
        }


class TestScatterMapReduce:
    """End-to-end distributed shape: head scatters fixed-row shard files,
    workers (fake client) read their own shard URLs and stage NDJSON, head
    reduces to GeoParquet + commits once, shard files are cleaned up."""

    def test_scatter_then_map_reduce(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from earthcatalog.inventory import write_inventory_shards

        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()

        # Source inventory: a single parquet with a non-STAC row mixed in.
        keys = ["a.stac.json", "b.stac.json", "c.stac.json", "notes.txt"]
        inv = tmp_path / "inv.parquet"
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["data-bucket"] * len(keys), type=pa.string()),
                    "key": pa.array(keys, type=pa.string()),
                }
            ),
            str(inv),
        )

        # Scatter on the head: fixed 2-row shard files, suffix-filtered.
        shards = write_inventory_shards(
            str(inv),
            store,
            staging_prefix="warehouse/staging/shards/run1",
            chunk_size=2,
            suffix=".stac.json",
        )
        assert [len(list(s.iter_pairs())) for s in shards] == [2, 1]

        # Map: workers read their shard URLs, stage NDJSON.
        ing = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse",
        )

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

            def gather(self, results):
                return results

        summary = ing.run(shards, client=_FakeClient())

        # Reduce happened: single commit, every STAC item indexed once.
        assert summary["items"] == 3
        assert index.known_source_keys() == {
            "s3://data-bucket/a.stac.json",
            "s3://data-bucket/b.stac.json",
            "s3://data-bucket/c.stac.json",
        }

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f and "shard" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == [
            "item-a.stac.json",
            "item-b.stac.json",
            "item-c.stac.json",
        ]

        # Cleanup: shard files + manifest gone, warehouse data files remain.
        from earthcatalog.inventory import delete_scatter

        assert (
            delete_scatter(store, "warehouse/staging/shards/run1", shards) == 3
        )  # 2 shards + manifest
        leftovers = [k for k in _list_files(store, "warehouse/staging/shards/")]
        assert leftovers == []
        data_files = [k for k in _list_files(store, "warehouse/") if k.endswith(".parquet")]
        assert all("shard" not in k for k in data_files) and data_files


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

        files = _list_files(store, "warehouse/")
        # NDJSON is staged, compacted, then deleted after the commit — a full
        # ndjson run must leave only GeoParquet behind (resumable/idempotent).
        assert not any(k.endswith(".jsonl") for k in files), files
        assert len(table.files) >= 1

    def test_skip_compact_only_stages(self):
        """skip_compact=True: Stage A writes NDJSON but does NOT compact."""
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
            skip_compact=True,
        )
        ing.run(_inventory(keys))

        # NDJSON staged, no GeoParquet registered.
        ndjson = [k for k in _list_files(store, "warehouse/") if k.endswith(".jsonl")]
        assert len(ndjson) >= 1
        assert table.files == []

    def test_skip_fetch_only_compacts_existing_staging(self):
        """skip_fetch=True: no fetching; compacts whatever NDJSON is staged."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json"]

        # Stage A only (skip_compact) to create the NDJSON.
        ing1 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            skip_compact=True,
        )
        ing1.run(_inventory(keys))
        assert table.files == []

        # Resume: skip_fetch — no fetch calls, just compact the staged NDJSON.
        fetch_calls = {"n": 0}

        def _counting_fetch(b, k):
            fetch_calls["n"] += 1
            return _make_item(k)

        ing2 = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_counting_fetch,
            stage="ndjson",
            warehouse_prefix="warehouse/",
            skip_fetch=True,
        )
        ing2.run(_inventory(keys))

        assert fetch_calls["n"] == 0
        assert len(table.files) >= 1
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}
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

            def gather(self, results):
                return results

        shards = [_inventory(keys[:2]), _inventory(keys[2:])]
        ing.run(shards, client=_FakeClient())

        # NDJSON is consumed (deleted) after the head commits; only parquet remains.
        assert not any(k.endswith(".jsonl") for k in _list_files(store, "warehouse/"))
        assert len(table.files) >= 1
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

        all_ids = []
        for f in _list_files(store, "warehouse/"):
            if f.endswith(".parquet") and "index" not in f:
                all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json", "item-c.stac.json"]

    def test_dask_split_stages_scatter_then_consolidate(self):
        """The distributed ndjson pipeline can run Stage A (scatter NDJSON,
        skip_compact) and Stage B (consolidate, skip_fetch) as separate calls."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

            def gather(self, results):
                return results

        shards = [_inventory(keys[:2]), _inventory(keys[2:])]

        # Stage A: scatter NDJSON only — no GeoParquet, no index rows.
        ing_a = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            skip_compact=True,
        )
        summary_a = ing_a.run(shards, client=_FakeClient())
        assert summary_a["items"] == 3
        assert summary_a["rows"] == 0
        assert table.files == []
        assert index.known_source_keys() == set()
        assert any(k.endswith(".jsonl") for k in _list_files(store, "warehouse/"))

        # Stage B: consolidate the staged NDJSON — skip fetch entirely.
        fetch_calls = {"n": 0}

        def _counting_fetch(b, k):
            fetch_calls["n"] += 1
            return _make_item(k)

        ing_b = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=_counting_fetch,
            stage="ndjson",
            warehouse_prefix="warehouse/",
            skip_fetch=True,
        )
        summary_b = ing_b.run([], client=_FakeClient())
        assert fetch_calls["n"] == 0, "Stage B must not fetch"
        assert summary_b["rows"] == 3
        assert len(table.files) >= 1
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}
        assert not any(k.endswith(".jsonl") for k in _list_files(store, "warehouse/"))

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


class TestNdjsonCompact:
    def test_compacts_to_single_part(self):
        """One (cell, year) bucket compacts to a single deterministic part
        file holding every staged item exactly once."""
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        keys = [f"{c}.stac.json" for c in "abcde"]
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
        )
        ing.run(_inventory(keys))

        part_files = [
            k
            for k in _list_files(store, "warehouse/")
            if k.endswith(".parquet") and "index" not in k
        ]
        assert len(part_files) == 1, part_files

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
        )
        ing.run(_inventory(keys))

        part_files = [
            k
            for k in _list_files(store, "warehouse/")
            if k.endswith(".parquet") and "index" not in k
        ]
        assert len(part_files) == 1
        assert _read_ids(store, part_files[0]) == [
            "item-p1.stac.json",
            "item-p2.stac.json",
            "item-p3.stac.json",
        ]

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
            skip_compact=True,  # keep the NDJSON staged for the dedup step
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
        )
        # Feed nothing new; Stage A writes nothing, but we compact the stale
        # bucket manually to prove dedup drops duplicates exactly.
        cell, year = "cellA", "2020"
        np, _, rows, _ = ing2._compact_ndjson_bucket(cell, year)

        # 2 unique items -> exactly 2 rows, never 4, and both ids survive.
        assert rows == 2, rows
        all_ids = []
        for f in np:
            all_ids.extend(_read_ids(store, f))
        assert sorted(all_ids) == ["item-a.stac.json", "item-b.stac.json"]

    def test_compaction_streams_ndjson(self):
        """Compaction reads NDJSON via stream(), never loading the whole file
        into RAM (raw.decode().splitlines() would spike to ~file size)."""
        import obstore as _obstore_mod

        from earthcatalog.ingest import Ingester as _Ing
        from earthcatalog.ingest import iter_ndjson_lines

        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        ing = _Ing(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse/",
            skip_compact=True,  # keep the NDJSON staged for the stream() assertion
        )
        ing.run(_inventory(["a.stac.json", "b.stac.json"]))

        # Compact must use .stream() (a full-object .bytes() read is the
        # anti-pattern that spikes RAM). We wrap get() and assert stream()
        # was invoked on the result for the NDJSON read.
        real_get = _obstore_mod.get
        stream_used = {"yes": False}

        class _SpyResult:
            def __init__(self, inner):
                self._inner = inner

            def stream(self):
                stream_used["yes"] = True
                return self._inner.stream()

            def bytes(self):
                return self._inner.bytes()

        def _fake_get(store_obj, key):
            return _SpyResult(real_get(store_obj, key))

        with patch.object(_obstore_mod, "get", side_effect=_fake_get):
            ing._compact_ndjson_bucket("cellA", "2020")

        assert stream_used["yes"], "compaction did not read NDJSON via stream()"

        # The streaming helper yields items without materializing the file.
        ndjson = [k for k in _list_files(store, "warehouse/") if k.endswith(".jsonl")]
        st = real_get(store, ndjson[0]).stream()
        lines = list(iter_ndjson_lines(st))
        assert [line["id"] for line in lines] == ["item-a.stac.json", "item-b.stac.json"]

    def test_iter_ndjson_lines_splits_across_chunks(self):
        """Lines split across stream chunks are reassembled correctly."""
        from earthcatalog.ingest import iter_ndjson_lines

        class _FakeStream:
            def __init__(self, chunks):
                self._chunks = iter(chunks)

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._chunks)

        # 'a' split so the newline lands at a chunk boundary.
        chunks = [b'{"id": "item-a", "props":', b' {"x": 1}}\n{"id": "item-b"', b"}\n"]
        items = list(iter_ndjson_lines(_FakeStream(chunks)))
        assert [i["id"] for i in items] == ["item-a", "item-b"]


class TestDeterministicPartNaming:
    def _stage(self, keys, *, delta=False):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse",
            skip_compact=True,
            delta=delta,
        )
        ing.run(_inventory(keys))
        return store, index, table, ing

    def test_full_mode_names_parts_deterministically(self):
        """Full compaction writes part_000000 — a single deterministic part
        file per bucket, no uuids."""
        store, index, table, ing = self._stage([f"{c}.stac.json" for c in "abcde"])
        np, ir, rows, _ = ing._compact_ndjson_bucket("cellA", "2020")
        assert rows == 5
        names = [p.rsplit("/", 1)[-1] for p in np]
        assert names == ["part_000000.parquet"]

    def test_delta_mode_continues_from_next_part_index(self):
        """Delta compaction appends after existing part_N files, never clobbers."""
        store, index, table, ing = self._stage(
            ["a.stac.json", "b.stac.json", "c.stac.json"], delta=True
        )
        # Pre-existing partition files from a prior full ingest.
        store.put("warehouse/grid_partition=cellA/year=2020/part_000000.parquet", b"x")
        store.put("warehouse/grid_partition=cellA/year=2020/part_000001.parquet", b"x")

        np, ir, rows, _ = ing._compact_ndjson_bucket("cellA", "2020")
        names = [p.rsplit("/", 1)[-1] for p in np]
        assert names == ["part_000002.parquet"]


class TestPerBucketCommitSkip:
    """Per-bucket commit: a committed bucket's NDJSON is deleted, so a re-run
    (skip_fetch) only rediscover + redoes buckets that still have NDJSON."""

    def _item(self, key, cell):
        it = _make_item(key)
        it["properties"]["grid_partition"] = cell
        return it

    class _C:
        def map(self, fn, args):
            return [fn(a) for a in args]

        def gather(self, results):
            return results

    def test_rerun_skips_committed_bucket(self):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        table = _FakeTable()

        def fetch(b, k):
            cell = "cellA" if k in ("a.stac.json", "b.stac.json") else "cellB"
            return self._item(k, cell)

        keys = ["a.stac.json", "b.stac.json", "c.stac.json", "d.stac.json"]

        # Stage NDJSON for cellA and cellB.
        ing_a = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=fetch,
            stage="ndjson",
            warehouse_prefix="warehouse",
            skip_compact=True,
        )
        ing_a.run([_inventory(keys)], client=self._C())
        staged = [k for k in _list_files(store, "warehouse/") if k.endswith(".jsonl")]
        assert any("cellA" in k for k in staged)
        assert any("cellB" in k for k in staged)

        # Simulate cellA's bucket finishing: compact + commit + delete its NDJSON.
        np, ir, _, ndjson_keys = ing_a._compact_ndjson_bucket("cellA", "2020")
        table.add_files(np)
        index.append(ir)
        for k in ndjson_keys:
            obstore.delete(store, k)

        remaining = [k for k in _list_files(store, "warehouse/") if k.endswith(".jsonl")]
        assert all("cellA" not in k for k in remaining)
        assert any("cellB" in k for k in remaining)

        # Re-run Stage B (skip_fetch): only cellB is rediscovered + compacted.
        ing_b = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=fetch,
            stage="ndjson",
            warehouse_prefix="warehouse",
            skip_fetch=True,
        )
        summary = ing_b.run([], client=self._C())

        assert summary["rows"] == 2  # only cellB's 2 items
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}


class TestHeadPreFilter:
    """Bulk profile: the head filters shards against the index before
    client.map, so re-runs never re-fetch known keys (A5)."""

    def _scatter(self, tmp_path, store, keys, chunk=2):
        import pyarrow as pa

        from earthcatalog.inventory import write_inventory_shards

        inv = tmp_path / "inv.parquet"
        pq.write_table(
            pa.table(
                {
                    "bucket": pa.array(["data-bucket"] * len(keys), type=pa.string()),
                    "key": pa.array(keys, type=pa.string()),
                }
            ),
            str(inv),
        )
        return write_inventory_shards(
            str(inv),
            store,
            staging_prefix="warehouse/staging/shards/run1",
            chunk_size=chunk,
            suffix=".stac.json",
        )

    class _RecordingClient:
        def __init__(self):
            self.tasks: list = []
            self.fetched: list = []

        def map(self, fn, args):
            self.tasks.append(args)
            out = []
            for a in args:
                pairs = a.iter_pairs() if hasattr(a, "iter_pairs") else a
                self.fetched.extend(pairs)
                out.append(fn(a))
            return out

        def gather(self, results):
            return results

    class _FakeTable:
        def __init__(self):
            self.files: list[str] = []

        def add_files(self, paths):
            self.files.extend(paths)

    def _dask(self, store, index, table):
        return DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="direct",
            warehouse_prefix="warehouse",
        )

    def test_prefilters_known_keys(self, tmp_path):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = self._FakeTable()
        keys = ["a.stac.json", "b.stac.json", "c.stac.json"]
        shards = self._scatter(tmp_path, store, keys)

        index.append(
            [
                {"stac_id": "a", "s3_key": "s3://data-bucket/a.stac.json",
                 "grid_partition": "cellA", "year": 2020}
            ]
        )
        client = self._RecordingClient()
        summary = self._dask(store, index, table).run(shards, client=client)

        # 'a' was known: workers never saw it; the other two shipped.
        shipped = [k for _, k in client.fetched]
        assert shipped == ["b.stac.json", "c.stac.json"]
        assert summary["items"] == 2
        assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

    def test_fully_known_ships_nothing(self, tmp_path):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = self._FakeTable()
        keys = ["a.stac.json", "b.stac.json"]
        shards = self._scatter(tmp_path, store, keys)

        index.append(
            [
                {"stac_id": k.rsplit(".", 1)[0], "s3_key": f"s3://data-bucket/{k}",
                 "grid_partition": "cellA", "year": 2020}
                for k in keys
            ]
        )
        client = self._RecordingClient()
        summary = self._dask(store, index, table).run(shards, client=client)

        assert client.fetched == []
        assert summary["items"] == 0


class TestUnknownYearSentinel:
    def test_item_without_datetime_matches_gc_partition(self, tmp_path):
        """A11: items without datetime go to year=unknown/ physically; the
        index row must carry year=NULL (not 0) so GC's partition lookup
        finds the same directory and can actually retire the file."""
        import csv as _csv

        from earthcatalog.gc import run_garbage_collection

        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")

        class _FakeTable:
            def __init__(self):
                self.files: list[str] = []

            def add_files(self, paths):
                self.files.extend(paths)

        def no_datetime_item(key):
            item = _make_item(key)
            del item["properties"]["datetime"]
            return item

        ing = Ingester(
            store=store,
            index=index,
            table=_FakeTable(),
            fetch_fn=lambda b, k: no_datetime_item(k),
            stage="direct",
            warehouse_prefix="warehouse",
            batch_size=10,
        )
        summary = ing.run(_inventory(["x.stac.json"]))
        assert summary["items"] == 1

        # Physically in year=unknown/.
        unknown_files = [
            k for k in _list_files(store, "warehouse/")
            if "year=unknown" in k and k.endswith(".parquet")
        ]
        assert unknown_files, _list_files(store, "warehouse/")

        # Index row year is NULL, not 0.
        row = next(index.stream_active())
        assert row["year"] is None

        # GC against an inventory without the item retires the file — the
        # partition lookup (unknown) matches where the file actually lives.
        inv_csv = tmp_path / "inv.csv"
        with inv_csv.open("w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["bucket", "key"])

        gc_result = run_garbage_collection(
            str(inv_csv),
            store=store,
            index=Index(store, "warehouse/index.parquet"),
            warehouse_prefix="warehouse/",
            head_fn=lambda k: False,
        )
        assert gc_result["confirmed"] == 1, gc_result
        assert gc_result["orphaned"] == 1, gc_result
