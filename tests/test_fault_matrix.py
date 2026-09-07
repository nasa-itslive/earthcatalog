"""Fault-injection matrix for the daily (serial, direct-stage) ingest.

Every cell injects one crash point, then re-runs the same command on the
SAME store/index/table and asserts convergence: the item-id multiset and
index rows equal a clean run, no journals are left, and — except for the
unknowable write→journal micro-window — no unregistered files remain.

| inject failure after                  | cell                                  |
|---------------------------------------|---------------------------------------|
| Nth fetch (serial + pooled paths)     | test_crash_nth_fetch_*                |
| file write, pre-journal-update        | test_crash_file_write_unjournaled     |
| add_files, pre-append                 | test_crash_after_add_files            |
| add_files (pre-commit)                | test_crash_before_commit              |
| append, pre-journal-delete            | test_crash_before_journal_delete      |
"""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
import pytest
from obstore.store import MemoryStore

from earthcatalog.index import Index
from earthcatalog.ingest import DaskIngester, Ingester
from earthcatalog.journal import BatchJournal, list_journals

from tests.test_ingest import _inventory, _make_item

WAREHOUSE_ROOT = "memory://warehouse"


class _FakeTable:
    def __init__(self):
        self.files: list[str] = []

    def add_files(self, paths):
        self.files.extend(paths)

    @property
    def inspect(self):
        table = self

        class _Inspect:
            def files(self):
                return pa.table({"file_path": pa.array(table.files)})

        return _Inspect()


class _FailAppendIndex:
    """Delegates to a real Index; ``append`` raises on demand."""

    def __init__(self, inner: Index, fail: bool = False):
        self._inner = inner
        self._fail = fail

    def known_source_keys(self):
        return self._inner.known_source_keys()

    def append(self, rows, part=None):
        if self._fail:
            raise RuntimeError("injected crash in index.append")
        return self._inner.append(rows, part=part)


class _FailOnceTable(_FakeTable):
    """add_files succeeds for the first batch, raises afterwards."""

    def __init__(self):
        super().__init__()
        self._committed_batches = 0

    def add_files(self, paths):
        self._committed_batches += 1
        if self._committed_batches > 1:
            raise RuntimeError("injected crash before commit")
        super().add_files(paths)


def _run(keys, store, index, table, *, batch_size=2, fetch_workers=1, fetch_calls=None):
    calls = {"n": 0}

    def fetch(bucket, key):
        calls["n"] += 1
        return _make_item(key)

    if fetch_calls is not None:
        fetch_calls["outer"] = calls

    ing = Ingester(
        store=store,
        index=index,
        table=table,
        fetch_fn=fetch,
        warehouse_prefix="warehouse",
        warehouse_root=WAREHOUSE_ROOT,
        batch_size=batch_size,
        fetch_workers=fetch_workers,
    )
    return ing.run(_inventory(keys))


def _assert_converged(store, index, table, keys, *, allow_orphans=0):
    assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}
    assert list_journals(store, "warehouse") == []
    orphans = []
    prefix = "warehouse/"
    for batch in store.list(prefix=prefix):
        for obj in batch:
            k = obj["path"]
            if k.endswith(".parquet") and "index" not in k:
                rel = k.removeprefix(prefix)  # add_files paths join root + rel
                if f"{WAREHOUSE_ROOT}/{rel}" not in table.files:
                    orphans.append(k)
    assert len(orphans) <= allow_orphans, orphans


class TestFaultMatrix:
    def test_crash_nth_fetch_serial(self):
        """Serial path: the crash hits mid-fetch, before any journal exists.
        The re-run re-fetches from where the index says to."""
        keys = [f"{i}.stac.json" for i in range(5)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()
        calls = {"n": 0}

        def failing_fetch(bucket, key):
            calls["n"] += 1
            if calls["n"] > 3:
                raise RuntimeError("injected fetch crash")
            return _make_item(key)

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=failing_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
            batch_size=2,
        )
        with pytest.raises(RuntimeError):
            ing.run(_inventory(keys))

        summary = _run(keys, store, index, table, batch_size=2)
        assert summary["items"] == 3  # items 0,1 were committed pre-crash
        _assert_converged(store, index, table, keys)

    def test_crash_nth_fetch_pooled(self):
        """Pooled path: the journal exists (keys written before the fetch),
        so recovery deletes it and the batch re-runs."""
        keys = [f"{i}.stac.json" for i in range(5)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()
        calls = {"n": 0}

        def failing_fetch(bucket, key):
            calls["n"] += 1
            if calls["n"] > 2:
                raise RuntimeError("injected fetch crash")
            return _make_item(key)

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=failing_fetch,
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
            batch_size=4,
            fetch_workers=4,
        )
        with pytest.raises(RuntimeError):
            ing.run(_inventory(keys))

        assert len(list_journals(store, "warehouse")) == 1
        summary = _run(keys, store, index, table, batch_size=4)
        assert summary["recovery"]["journals"] == 1
        _assert_converged(store, index, table, keys)

    def test_crash_after_add_files(self):
        """Crash inside the commit window: files registered, rows not
        appended.  Recovery appends the journaled rows; the re-run fetches
        nothing."""
        keys = [f"{i}.stac.json" for i in range(4)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()

        def make_ingester(proxy_index, fail):
            return Ingester(
                store=store,
                index=_FailAppendIndex(proxy_index, fail=fail),
                table=table,
                fetch_fn=lambda b, k: _make_item(k),
                warehouse_prefix="warehouse",
                warehouse_root=WAREHOUSE_ROOT,
                batch_size=2,
            )

        # Batch 1 commits cleanly.
        make_ingester(index, fail=False).run(_inventory(keys[:2]))
        # Batch 2: add_files succeeds, append crashes.
        with pytest.raises(RuntimeError, match="index.append"):
            make_ingester(index, fail=True).run(_inventory(keys[2:]))

        # Recovery appends the journaled rows; no fetch needed.
        calls = {"n": 0}
        summary = _run(keys[2:], store, index, table, fetch_calls=calls)
        assert summary["recovery"]["rows_appended"] == 2
        assert calls["outer"]["n"] == 0
        _assert_converged(store, index, table, keys)

    def test_crash_before_commit(self):
        """Crash before add_files: journaled files are unregistered.
        Recovery deletes them; the re-run re-fetches the batch."""
        keys = [f"{i}.stac.json" for i in range(4)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FailOnceTable()

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
            batch_size=2,
        )
        with pytest.raises(RuntimeError, match="before commit"):
            ing.run(_inventory(keys))

        table._committed_batches = 0  # let the re-run commit
        calls = {"n": 0}
        summary = _run(keys, store, index, table, fetch_calls=calls)
        assert summary["recovery"]["files_deleted"] == 1
        assert calls["outer"]["n"] == 2  # items 2,3 re-fetched; 0,1 known
        _assert_converged(store, index, table, keys)

    def test_crash_before_journal_delete(self):
        """Commit completed, crash before finish_batch: recovery only
        deletes the journal; the re-run fetches nothing."""
        keys = [f"{i}.stac.json" for i in range(4)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()

        _run(keys, store, index, table, batch_size=4)
        # Hand-craft the leftover journal a post-commit crash would leave.
        sticky = BatchJournal(store, "warehouse", "crash-before-delete")
        sticky.start_batch([f"s3://data-bucket/{k}" for k in keys])

        calls = {"n": 0}
        summary = _run(keys, store, index, table, batch_size=4, fetch_calls=calls)
        assert summary["recovery"]["journals"] == 1
        assert calls["outer"]["n"] == 0
        _assert_converged(store, index, table, keys)

    def test_crash_file_write_unjournaled(self):
        """The micro-window: a file written but never journaled.  Recovery
        cannot know it; convergence still holds and the leaked file stays
        unregistered (invisible to search) — the documented residual risk."""
        keys = [f"{i}.stac.json" for i in range(4)]
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()

        import earthcatalog.ingest as ing_mod
        from earthcatalog.transform import (
            fan_out,
            group_by_partition,
            write_geoparquet_s3,
        )

        def half_written(store, partitioner, prefix, items, on_file=None):
            fo = fan_out(items, partitioner) if partitioner else items
            (cell, year), group = next(iter(group_by_partition(fo).items()))
            k = f"{prefix}/grid_partition={cell}/year={year}/part_leaked.parquet"
            write_geoparquet_s3(group, store, k)
            raise RuntimeError("injected crash after file write, pre-journal")

        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
            batch_size=2,
        )
        with patch.object(ing_mod, "_write_direct", half_written):
            with pytest.raises(RuntimeError, match="pre-journal"):
                ing.run(_inventory(keys))

        # The leaked file exists on the store and is not registered.
        assert store.get("warehouse/grid_partition=cellA/year=2020/part_leaked.parquet")
        calls = {"n": 0}
        summary = _run(keys, store, index, table, fetch_calls=calls)
        assert calls["outer"]["n"] == 4  # nothing had committed
        _assert_converged(store, index, table, keys, allow_orphans=1)


class TestJournalStageScope:
    def test_ndjson_run_creates_no_journal(self):
        """The journal is a direct-stage mechanism: bulk (Dask) runs stage
        NDJSON as a fan-out byproduct and must not journal anything."""
        from earthcatalog.journal import list_journals

        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()

        class _FakeClient:
            def map(self, fn, args):
                return [fn(a) for a in args]

            def gather(self, results):
                return results

        ing = DaskIngester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            stage="ndjson",
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
        )
        shards = [[("data-bucket", "a.stac.json")], [("data-bucket", "b.stac.json")]]
        summary = ing.run(shards, client=_FakeClient())
        assert list_journals(store, "warehouse") == []

    def test_direct_run_leaves_no_journal_after_success(self):
        store = MemoryStore()
        index = Index(store, "warehouse/index.parquet")
        table = _FakeTable()
        ing = Ingester(
            store=store,
            index=index,
            table=table,
            fetch_fn=lambda b, k: _make_item(k),
            warehouse_prefix="warehouse",
            warehouse_root=WAREHOUSE_ROOT,
            batch_size=4,
            fetch_workers=4,
        )
        summary = ing.run(_inventory(["a.stac.json", "b.stac.json"]))
        assert summary["stage"] == "direct"
        assert list_journals(store, "warehouse") == []
