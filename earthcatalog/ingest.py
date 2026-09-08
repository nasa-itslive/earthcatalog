"""
Resumable STAC ingest — the single ingest path for earthcatalog.  The
unified :class:`earthcatalog.index.Index` is the resume checkpoint: every
run skips source keys already recorded there, so crashes waste nothing and
re-runs are idempotent.  ``stage="direct"`` writes fan-out items straight
to GeoParquet; ``stage="ndjson"`` stages per-partition NDJSON first (bulk
profile, PGSTAC interchange) and compacts in a second pass.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Iterator
from functools import partial

import obstore
from obstore.store import ObjectStore
from pyiceberg.table import Table

from earthcatalog import inventory as _inventory
from earthcatalog.index import Index
from earthcatalog.journal import BatchJournal, new_run_id, recover_journals
from earthcatalog.schema import layout_of, partition_prefix
from earthcatalog.transform import (
    _sort_key,
    fan_out,
    group_by_partition,
    write_geoparquet_s3,
)


class Ingester:
    """One resumable ingest pipeline over an S3 inventory."""

    def __init__(
        self,
        store: ObjectStore,
        index: Index,
        table: Table,
        *,
        fetch_fn=None,
        partitioner=None,
        warehouse_prefix: str = "",
        warehouse_root: str | None = None,
        batch_size: int = 10_000,
        fetch_workers: int = 1,
        delta: bool = False,
        dedupe: Callable[[Iterator], Iterator] | None = None,
        upload_db: Callable[[], None] | None = None,
    ) -> None:
        self._store = store
        self._index = index
        self._table = table
        self._fetch_fn = fetch_fn or _inventory.fetch_item
        self._partitioner = partitioner
        self._warehouse_prefix = warehouse_prefix.rstrip("/")
        self._warehouse_root = warehouse_root
        self._batch_size = batch_size
        self._fetch_workers = fetch_workers
        self._delta = delta
        self._dedupe = dedupe
        # Upload the catalog db after each durable batch (crash safety).
        self._upload_db = upload_db
        # Schema-driven hive layout (grid, level, time_bin), worker-safe.
        self._layout = layout_of(table.properties)
        # Resume filter: production injects the DuckDB anti-join
        # (diff.anti_join); without one the run falls back to a set filter.

    def _full_path(self, rel_key: str) -> str:
        """Map a store-relative key to the full URI Iceberg ``add_files`` needs."""
        if not self._warehouse_root:
            return rel_key
        # rel_key already includes warehouse_prefix; strip it so we join the
        # root exactly once.
        rel = rel_key
        if self._warehouse_prefix and rel.startswith(self._warehouse_prefix + "/"):
            rel = rel[len(self._warehouse_prefix) + 1 :]
        return f"{self._warehouse_root.rstrip('/')}/{rel}"

    # -- public ---------------------------------------------------------------

    def run(self, inventory) -> dict:
        """Ingest ``(bucket, key)`` pairs from *inventory*; return a summary.

        Direct stage only — items are fetched through a bounded pool and
        written straight to GeoParquet, journal-first per batch.  (Distributed
        bulk runs use :class:`DaskIngester`, whose workers stage NDJSON as a
        byproduct of the fan-out.)
        """
        total = 0
        rows = 0
        considered = 0
        pending: list[dict] = []

        # Close any crash window left by a previous run, then journal this
        # run's own batches.
        recovery = recover_journals(
            self._store,
            self._warehouse_prefix,
            self._index,
            self._table,
            full_path=self._full_path,
        )
        journal = BatchJournal(self._store, self._warehouse_prefix, new_run_id())

        def _counted():
            nonlocal considered
            for pair in inventory:
                considered += 1
                yield pair

        if True:
            # The index is the resume checkpoint: it is consulted once per
            # pair-stream, never per item.  Production injects the DuckDB
            # anti-join (diff.anti_join); the fallback set-filter is exact
            # but loads the index into RAM — tests and small runs only.
            dedupe = self._dedupe or self._set_dedupe()
            new_pairs = dedupe(_counted())
            if self._fetch_workers > 1:
                # Daily-path bounded fetch pool: one batch is fully fetched
                # before it is flushed, so batch semantics (and the crash
                # windows) are unchanged.
                from itertools import islice

                while True:
                    chunk = list(islice(new_pairs, self._batch_size))
                    if not chunk:
                        break
                    seq = (
                        journal.start_batch([f"s3://{b}/{k}" for b, k in chunk])
                        if journal is not None
                        else None
                    )
                    pending = _fetch_many(chunk, self._fetch_fn, self._fetch_workers)
                    total += len(pending)
                    if pending:
                        rows += self._flush_direct(pending, journal, seq)
                pending = []
            else:
                for bucket, key in new_pairs:
                    item = self._fetch_fn(bucket, key)
                    if item is None:
                        continue
                    pending.append(item)
                    total += 1
                    if len(pending) >= self._batch_size:
                        rows += self._flush_direct(pending, journal)
                        pending = []

            if pending:
                rows += self._flush_direct(pending, journal)

        summary = {
            "items": total,
            "rows": rows,
            "considered": considered,
            "stage": "direct",
        }
        if recovery["journals"]:
            summary["recovery"] = recovery
        return summary

    def _set_dedupe(self) -> Callable[[Iterator], Iterator]:
        """Fallback resume filter: set membership against the whole index."""
        known = self._index.known_source_keys()

        def _filter(pairs: Iterator) -> Iterator:
            for bucket, key in pairs:
                if f"s3://{bucket}/{key}" not in known:
                    yield bucket, key

        return _filter

    # -- internals ------------------------------------------------------------

    def _flush_direct(
        self,
        items: list[dict],
        journal: BatchJournal | None = None,
        seq: int | None = None,
    ) -> int:
        """Journal-first commit of one batch: files -> add_files -> index part."""
        if journal is not None and seq is None:
            seq = journal.start_batch(
                [f"s3://{it['_source_bucket']}/{it['_source_key']}" for it in items]
            )
        on_file: Callable[[str, list[dict]], None] | None = None
        if journal is not None:
            assert seq is not None

            def on_file(rel_key: str, rows: list[dict]) -> None:
                journal.record_file(seq, rel_key, rows)

        new_paths, index_rows, rows = self._write_direct(items, on_file=on_file)
        if new_paths:
            # Iceberg commits first, the index part second: a crash between
            # the two leaves the rows in the journal, recovered as this
            # exact part (deterministic {run_id}/{seq} name).
            self._table.add_files([self._full_path(k) for k in new_paths])
            if index_rows:
                part = f"{journal.run_id}/{seq:04d}" if journal is not None else None
                self._index.append(index_rows, part=part)
            if self._upload_db is not None:
                self._upload_db()
        if journal is not None:
            journal.finish_batch(seq or 0)
        return rows

    def _write_direct(self, items: list[dict], on_file=None) -> tuple[list[str], list[dict], int]:
        """Write items to GeoParquet — see :func:`_write_direct`."""
        return _write_direct(
            self._store,
            self._partitioner,
            self._warehouse_prefix,
            items,
            on_file=on_file,
            layout=self._layout,
        )


class DaskIngester(Ingester):
    """Distributed variant: shards the inventory and writes via ``client.map``.

    Each *shard* is an :class:`earthcatalog.inventory.InventoryShard` spec
    (preferred — the worker streams its own inventory part files, so only
    file keys cross the wire) or a plain sequence of ``(bucket, key)``
    pairs.  In ``"ndjson"`` stage, workers write per-shard NDJSON and then
    compact it to GeoParquet (one task per ``(cell, year)`` bucket); only
    the final Iceberg/index commit stays on the head.  In ``"direct"`` stage,
    each worker runs :meth:`Ingester._write_direct` (write-only GeoParquet)
    and returns ``(new_paths, index_rows, rows)``; the head node then calls
    ``table.add_files()`` and ``index.append()`` exactly once, so the shared
    index file is never written concurrently.

    *client* must expose ``map(fn, shards)`` (a Dask ``Client`` works).
    """

    def __init__(
        self,
        store: ObjectStore,
        index: Index,
        table: Table,
        *,
        fetch_fn=None,
        partitioner=None,
        stage: str = "ndjson",
        warehouse_prefix: str = "",
        warehouse_root: str | None = None,
        batch_size: int = 10_000,
        skip_fetch: bool = False,
        skip_compact: bool = False,
        fetch_concurrency: int = 256,
        delta: bool = False,
        dedupe: Callable[[Iterator], Iterator] | None = None,
    ) -> None:
        super().__init__(
            store=store,
            index=index,
            table=table,
            fetch_fn=fetch_fn,
            partitioner=partitioner,
            warehouse_prefix=warehouse_prefix,
            warehouse_root=warehouse_root,
            batch_size=batch_size,
            delta=delta,
            dedupe=dedupe,
        )
        # Bulk-only knobs: the NDJSON staging format is a byproduct of the
        # worker fan-out, compacted per (cell, year) bucket afterwards.
        self._stage = stage
        self._skip_fetch = skip_fetch
        self._skip_compact = skip_compact
        self._fetch_concurrency = fetch_concurrency
        self._ndjson_prefix = (
            f"{self._warehouse_prefix}/staging/ndjson"
            if self._warehouse_prefix
            else "staging/ndjson"
        )

    def _prefilter_shards(self, inventory: list) -> list:
        """Head-side pre-filter: drop keys the index already has before
        shipping shards to workers, so a re-run never re-fetches.  A shard
        whose keys are all known vanishes entirely; shard specs stream at
        the head (cheap parquet reads) and ship as plain pair lists.
        """
        dedupe = self._dedupe or self._set_dedupe()
        shipped: list = []
        for shard in inventory:
            pairs = shard.iter_pairs() if hasattr(shard, "iter_pairs") else iter(shard)
            kept = list(dedupe(pairs))
            if kept:
                shipped.append(kept)
        return shipped

    def _discover_staged_buckets(self) -> set[tuple[str, str]]:
        """List every (cell, bin_value) bucket that has staged NDJSON files."""
        buckets: set[tuple[str, str]] = set()
        for batch in obstore.list(self._store, prefix=self._ndjson_prefix + "/"):
            for obj in batch:
                k: str = obj["path"]
                if not k.endswith(".jsonl"):
                    continue
                parts = k.split("/")
                # v2: .../tile=<cell>/<bin>=<value>/<file>.jsonl
                # v1: .../grid_partition=<cell>/year=<year>/<file>.jsonl
                for i, part in enumerate(parts):
                    if part.startswith(("tile=", "grid_partition=")):
                        cell = part.split("=", 1)[1]
                        bv = parts[i + 1].split("=", 1)[1] if i + 1 < len(parts) else "unknown"
                        buckets.add((cell, bv))
        return buckets

    def _compact_ndjson_bucket(
        self, cell: str, bin_val: str
    ) -> tuple[list[str], list[dict], int, list[str]]:
        """Compact one ``(cell, bin_value)`` bucket — see :func:`_compact_bucket`."""
        return _compact_bucket(
            self._store,
            self._ndjson_prefix,
            self._warehouse_prefix,
            (cell, bin_val),
            delta=self._delta,
            layout=self._layout,
        )

    def run(self, inventory, *, client=None) -> dict:  # type: ignore[override]
        """Ingest each shard in *inventory* in parallel; workers stream their own pairs."""
        if client is None:
            raise ValueError("DaskIngester.run requires a client exposing map(fn, shards)")
        if self._stage == "ndjson":
            return self._run_ndjson(self._prefilter_shards(list(inventory)), client=client)

        # Ship only plain state to workers (not the Iceberg table/index) via a
        # module-level function, so it pickles/tokenizes deterministically.
        write_fn = partial(
            _write_direct_shard,
            self._store,
            self._fetch_fn,
            self._partitioner,
            self._warehouse_prefix,
            layout=self._layout,
            fetch_concurrency=self._fetch_concurrency,
        )
        shards = self._prefilter_shards(list(inventory))
        results = _collect(client, write_fn, shards, desc="Write GeoParquet")

        new_paths: list[str] = []
        index_rows: list[dict] = []
        total = 0
        for new_paths_i, index_rows_i, rows in results:
            new_paths.extend(new_paths_i)
            index_rows.extend(index_rows_i)
            total += rows

        if new_paths:
            self._table.add_files([self._full_path(k) for k in new_paths])
            if index_rows:
                self._index.append(index_rows)

        return {"items": total, "rows": total}

    def _run_ndjson(self, shards, *, client) -> dict:
        """Distributed NDJSON mode, splittable into two steps.

        Stage A (scatter NDJSON): workers fetch each shard and fan out to
        per-(cell, year) NDJSON buckets.  Stage B (consolidate): workers
        compact each bucket to GeoParquet.  ``skip_compact=True`` runs only
        Stage A; ``skip_fetch=True`` runs only Stage B against already-staged
        NDJSON.

        Each bucket is committed as it finishes — ``add_files`` +
        ``index.append`` + delete its NDJSON — so a failed/OOM run only loses
        the in-flight bucket: re-running (``skip_fetch=True``) discovers only
        buckets that still have staged NDJSON and skips the rest.  The head is
        the single writer (appends are sequential), so the shared index file is
        never written concurrently.
        """
        buckets: set[tuple[str, str]] = set()
        total = 0

        # Stage A — fetch → NDJSON (distributed).
        if not self._skip_fetch:
            stage_fn = partial(
                _write_ndjson_shard,
                self._store,
                self._fetch_fn,
                self._partitioner,
                self._ndjson_prefix,
                layout=self._layout,
                fetch_concurrency=self._fetch_concurrency,
            )
            results = _collect(client, stage_fn, list(enumerate(shards)), desc="Stage NDJSON")
            for n_items, touched in results:
                total += n_items
                buckets.update(touched)

        # Stage B — compact NDJSON → GeoParquet (distributed).
        if self._skip_compact:
            return {"items": total, "rows": 0}

        if self._skip_fetch:
            buckets = self._discover_staged_buckets()

        compact_fn = partial(
            _compact_bucket,
            self._store,
            self._ndjson_prefix,
            self._warehouse_prefix,
            delta=self._delta,
            layout=self._layout,
        )

        rows = 0

        def _commit(result: tuple[list[str], list[dict], int, list[str]]) -> None:
            nonlocal rows
            np, ir, n, ndjson_keys = result
            if np:
                self._table.add_files([self._full_path(k) for k in np])
            if ir:
                self._index.append(ir)
            for key in ndjson_keys:
                try:
                    obstore.delete(self._store, key)
                except Exception:
                    pass
            rows += n

        _for_each_result(
            client, compact_fn, sorted(buckets), desc="Compact GeoParquet", on_result=_commit
        )

        return {"items": total, "rows": rows}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _shard_iter_pairs(shard) -> Iterator[tuple[str, str]]:
    """Iterate a shard: an :class:`InventoryShard` spec or a plain pair list."""
    if isinstance(shard, _inventory.InventoryShard):
        return shard.iter_pairs()
    return iter(shard)


def _collect(client, fn, iterable, *, desc: str):
    """``client.map`` + collect results with a tqdm progress bar.

    Works with a real Dask ``Client`` (Futures, collected via
    ``as_completed`` so the bar advances as tasks finish) and with the
    in-process fake test client (whose ``map`` returns plain results).
    """
    from tqdm import tqdm

    items = list(iterable)
    futures = client.map(fn, items)

    if futures and hasattr(futures[0], "result"):
        from dask.distributed import as_completed

        results: list = [None] * len(futures)
        order = {f: i for i, f in enumerate(futures)}
        with tqdm(total=len(futures), desc=desc) as pbar:
            for fut, res in as_completed(futures, with_results=True):
                results[order[fut]] = res
                pbar.update(1)
        return results

    with tqdm(total=len(items), desc=desc) as pbar:
        for _ in futures:
            pbar.update(1)
    return futures


def _for_each_result(client, fn, iterable, *, desc: str, on_result) -> None:
    """``client.map`` + invoke *on_result* per result as it completes.

    Like :func:`_collect` but streaming — each result is handed to
    ``on_result`` in completion order (so the head can commit each bucket as
    soon as its workers finish), with a tqdm progress bar.  Works with a real
    Dask ``Client`` (via ``as_completed``) and the in-process fake test client.
    """
    from tqdm import tqdm

    items = list(iterable)
    futures = client.map(fn, items)

    if futures and hasattr(futures[0], "result"):
        from dask.distributed import as_completed

        with tqdm(total=len(futures), desc=desc) as pbar:
            for _fut, res in as_completed(futures, with_results=True):
                on_result(res)
                pbar.update(1)
        return

    with tqdm(total=len(items), desc=desc) as pbar:
        for res in futures:
            on_result(res)
            pbar.update(1)


def _append_ndjson(store: ObjectStore, key: str, items: list[dict]) -> None:
    """Append items to an NDJSON object, creating or extending it."""
    lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
    try:
        raw = bytes(obstore.get(store, key).bytes())
        merged = raw.decode("utf-8") + lines
        obstore.put(store, key, merged.encode("utf-8"))
    except FileNotFoundError:
        obstore.put(store, key, lines.encode("utf-8"))


def _put_ndjson(store: ObjectStore, key: str, items: list[dict]) -> None:
    """Write items to a fresh NDJSON object (single PUT, no read-modify-write)."""
    lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
    obstore.put(store, key, lines.encode("utf-8"))


def _write_direct(
    store: ObjectStore,
    partitioner,
    warehouse_prefix: str,
    items: list[dict],
    on_file=None,
    layout: tuple[str, str, str] = ("h3", "1", "year"),
) -> tuple[list[str], list[dict], int]:
    """Write items to GeoParquet without touching the table/index.

    Worker-safe (plain state only) so a distributed caller can fan out this
    function and commit once on the head.  *on_file(rel_key, index_rows)*
    fires after each durably-written file (the journal's per-file update);
    keys follow the schema-driven *layout*.  Returns the 3-tuple
    ``(new_paths, index_rows, rows)``.
    """
    grid, level, time_bin = layout
    fo = fan_out(items, partitioner) if partitioner else items
    if not fo:
        return [], [], 0

    rows = 0
    new_paths: list[str] = []
    # Index rows come from the FAN-OUT items: only those carry
    # grid_partition (fetched STAC items never do), and GC locates the
    # files to rewrite through it — so a multi-cell item yields one row
    # per (source key, cell), and GC cleans every partition it touched.
    index_rows = []
    seen_pairs: set[tuple[str, str]] = set()
    for it in fo:
        if not it.get("_source_key"):
            continue
        pair = (it["_source_key"], it.get("properties", {}).get("grid_partition", "__none__"))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        index_rows.append(_to_index_row(it))

    for (cell, bin_val), group in group_by_partition(fo, time_bin).items():
        prefix = partition_prefix(warehouse_prefix, grid, level, cell, time_bin, bin_val)
        key = f"{prefix}part_{uuid.uuid4().hex[:8]}.parquet"
        n, _ = write_geoparquet_s3(group, store, key)
        if n > 0:
            new_paths.append(key)
            rows += n
            if on_file is not None:
                on_file(key, [_to_index_row(it) for it in group if it.get("_source_key")])

    return new_paths, index_rows, rows


def _fetch_items(fetch_fn, pairs: list[tuple[str, str]], concurrency: int) -> list:
    """Fetch STAC items for (bucket, key) pairs concurrently, dropping Nones.

    The default fetcher uses the async obstore path (true I/O concurrency,
    no thread per request); a custom ``fetch_fn`` (tests) uses a thread pool.
    """
    if fetch_fn is _inventory.fetch_item:
        return _inventory.fetch_items_async(pairs, concurrency=concurrency)

    if concurrency > 1 and len(pairs) > 1:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            return [it for it in ex.map(lambda bk: fetch_fn(bk[0], bk[1]), pairs) if it is not None]
    return [it for it in (fetch_fn(b, k) for b, k in pairs) if it is not None]


def _write_direct_shard(
    store: ObjectStore,
    fetch_fn,
    partitioner,
    warehouse_prefix: str,
    shard,
    fetch_concurrency: int = 256,
    layout: tuple[str, str, str] = ("h3", "1", "year"),
) -> tuple[list[str], list[dict], int]:
    """Fetch each pair in *shard* (concurrent), then write-only fan-out."""
    pairs = list(_shard_iter_pairs(shard))
    items = _fetch_items(fetch_fn, pairs, fetch_concurrency)
    return _write_direct(store, partitioner, warehouse_prefix, items, layout=layout)


def _write_ndjson_shard(
    store: ObjectStore,
    fetch_fn,
    partitioner,
    ndjson_prefix: str,
    shard_with_index,
    fetch_concurrency: int = 256,
    layout: tuple[str, str, str] = ("h3", "1", "year"),
) -> tuple[int, list[tuple[str, str]]]:
    """Fetch a shard (concurrent), fan out to NDJSON (worker task).

    Returns ``(n_items, touched_buckets)`` — the number of items staged and
    the ``(cell, bin_value)`` buckets touched, so the head can compact each
    bucket exactly once.
    """
    shard_index, shard = shard_with_index
    pairs = list(_shard_iter_pairs(shard))
    items = _fetch_items(fetch_fn, pairs, fetch_concurrency)

    grid, level, time_bin = layout
    fo = fan_out(items, partitioner) if partitioner else items
    buckets = group_by_partition(fo, time_bin)

    for (cell, bin_val), group in buckets.items():
        prefix = partition_prefix(ndjson_prefix, grid, level, cell, time_bin, bin_val)
        # The shard index is a deterministic integer: concurrent workers
        # never collide on the same file.
        key = f"{prefix}shard_{shard_index}.jsonl"
        _put_ndjson(store, key, group)

    return len(items), list(buckets.keys())


def _year_from_item(item: dict) -> int | None:
    dt = item.get("properties", {}).get("datetime")
    if dt:
        try:
            return int(str(dt)[:4])
        except ValueError:
            pass
    return None


def _to_index_row(item: dict) -> dict:
    props = item.get("properties", {})
    return {
        "s3_key": f"s3://{item['_source_bucket']}/{item['_source_key']}",
        "stac_id": item.get("id") or "",
        "grid_partition": props.get("grid_partition", "__none__"),
        # Null, not 0: files without a datetime live in year=unknown/, and
        # GC's partition lookup maps None → unknown — the row must agree.
        "year": _year_from_item(item),
    }


def iter_ndjson_lines(stream) -> Iterator[dict]:
    """Yield parsed dicts from a byte-stream of newline-delimited JSON.

    Streams chunk-by-chunk, keeping only the partial trailing line across
    chunk boundaries — never materializing the whole file.  Handles lines
    split at arbitrary chunk boundaries.
    """
    pending = b""
    for chunk in stream:
        pending += bytes(chunk)
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if line.strip():
                yield json.loads(line)
    if pending.strip():
        yield json.loads(pending)


_PART_RE = re.compile(r"part_(\d+)\.parquet$")


def _fetch_many(pairs: list[tuple[str, str]], fetch_fn, workers: int) -> list[dict]:
    """Fetch *pairs* with a bounded pool; drops failures (None contract).

    The default fetcher goes through the async obstore path (one light
    event loop, no thread per request); a custom *fetch_fn* (tests) runs
    in a thread pool with the same None-on-error semantics.
    """
    if fetch_fn is _inventory.fetch_item:
        return _inventory.fetch_items_async(pairs, concurrency=workers)
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=workers) as ex:
        return [it for it in ex.map(lambda p: fetch_fn(*p), pairs) if it is not None]


def _next_part_index(store: ObjectStore, prefixes: list[str]) -> int:
    """Next free ``part_N`` index across *prefixes* (max + 1, or 0)."""
    indices: list[int] = []
    for prefix in prefixes:
        try:
            for listing in obstore.list(store, prefix=prefix):
                for obj in listing:
                    m = _PART_RE.search(obj["path"].rsplit("/", 1)[-1])
                    if m:
                        indices.append(int(m.group(1)))
        except Exception:
            pass
    return (max(indices) + 1) if indices else 0


def _compact_bucket(
    store: ObjectStore,
    ndjson_prefix: str,
    warehouse_prefix: str,
    bucket: tuple[str, str],
    delta: bool = False,
    layout: tuple[str, str, str] = ("h3", "1", "year"),
) -> tuple[list[str], list[dict], int, list[str]]:
    """Compact one ``(cell, bin_value)`` NDJSON bucket into a single GeoParquet file.

    Worker-safe: takes only plain state (store, string prefixes, int) rather
    than the whole :class:`Ingester`, so it can be shipped to Dask workers
    without pickling the Iceberg table or unified index.  *bucket* is a
    single ``(cell, bin_value)`` tuple so it maps cleanly over ``client.map``.

    Like main's warehouse consolidation, the whole partition is held in
    memory: staged NDJSON is streamed line-by-line, deduped exactly by item
    ID, sorted by ``(platform, datetime)``, and written as ONE deterministic
    ``part_{idx:06d}.parquet`` (delta mode continues from the next free
    ``part_N``).  A hot cell with ~500k items needs a few GB — scale the
    worker VM instead of batching.

    Returns ``(new_paths, index_rows, rows, ndjson_keys)`` — the caller (head
    node) commits to Iceberg + index once, then deletes the consumed NDJSON.
    """
    grid, level, time_bin = layout
    cell, bin_val = bucket
    bucket_dir = partition_prefix(ndjson_prefix, grid, level, cell, time_bin, bin_val)
    jsonl_keys: list[str] = []
    try:
        for listing in obstore.list(store, prefix=bucket_dir):
            for obj in listing:
                k: str = obj["path"]
                if k.endswith(".jsonl"):
                    jsonl_keys.append(k)
    except Exception:
        return [], [], 0, []
    if not jsonl_keys:
        return [], [], 0, []

    seen: set[str] = set()
    items: list[dict] = []
    for key in sorted(jsonl_keys):
        result = obstore.get(store, key)
        for item in iter_ndjson_lines(result.stream()):
            item_id = item.get("id")
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            items.append(item)

    if not items:
        return [], [], 0, jsonl_keys

    out_prefix = partition_prefix(warehouse_prefix, grid, level, cell, time_bin, bin_val)
    if delta:
        idx = _next_part_index(store, [out_prefix])
    else:
        idx = 0
    out_key = f"{out_prefix}part_{idx:06d}.parquet"
    n, _ = write_geoparquet_s3(sorted(items, key=_sort_key), store, out_key)
    if n == 0:
        return [], [], 0, jsonl_keys
    index_rows = [_to_index_row(it) for it in items if it.get("_source_key")]
    return [out_key], index_rows, n, jsonl_keys
