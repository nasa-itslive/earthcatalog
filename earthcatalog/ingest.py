"""
Resumable STAC ingest — the single ingest path for earthcatalog.

Replaces the three legacy entry points (``incremental.run``,
``backfill.run_backfill``, ``EarthCatalog.ingest``) with one pipeline whose
checkpoint is the unified :class:`earthcatalog.index.Index`.

Resume model
------------
The index records every source key (``s3://bucket/key``) that has been
durably ingested.  On every run we skip source keys already in the index —
a crash wastes nothing because re-running simply continues from where the
last successful ``index.append`` left off.  A crash *before* ``index.append``
is recovered by compaction (which dedups on ``id_hash``), so the system is
eventually consistent and idempotent.

Modes
-----
``stage="direct"`` — fan-out items are written straight to GeoParquet.
``stage="ndjson"`` — items are first fanned out to per-(cell, year) NDJSON
    (useful for PGSTAC interchange), then compacted to GeoParquet in a
    second pass.
"""

from __future__ import annotations

import json
import re
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterator
from functools import partial

import obstore

from earthcatalog import inventory as _inventory
from earthcatalog.index import Index
from earthcatalog.journal import BatchJournal, new_run_id, recover_journals
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
        store: object,
        index: Index,
        table: object,
        *,
        fetch_fn=None,
        partitioner=None,
        stage: str = "direct",
        warehouse_prefix: str = "",
        warehouse_root: str | None = None,
        batch_size: int = 10_000,
        skip_fetch: bool = False,
        skip_compact: bool = False,
        fetch_concurrency: int = 256,
        fetch_workers: int = 1,
        delta: bool = False,
        dedupe: Callable[[Iterator], Iterator] | None = None,
    ) -> None:
        self._store = store
        self._index = index
        self._table = table
        self._fetch_fn = fetch_fn or _inventory.fetch_item
        self._partitioner = partitioner
        self._stage = stage
        self._warehouse_prefix = warehouse_prefix.rstrip("/")
        self._warehouse_root = warehouse_root
        self._batch_size = batch_size
        self._skip_fetch = skip_fetch
        self._skip_compact = skip_compact
        self._fetch_concurrency = fetch_concurrency
        self._fetch_workers = fetch_workers
        self._delta = delta
        self._dedupe = dedupe
        # Resume filter: pairs -> pairs with known keys removed.  Production
        # runs inject the DuckDB anti-join (diff.anti_join); without one the
        # run falls back to a set-based filter against the index.
        self._ndjson_prefix = (
            f"{self._warehouse_prefix}/staging/ndjson"
            if self._warehouse_prefix
            else "staging/ndjson"
        )

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

        With ``skip_fetch=True`` the fetch loop is skipped entirely and only
        already-staged NDJSON is compacted (Stage B resume).  With
        ``skip_compact=True`` only Stage A (fetch + NDJSON staging) runs,
        leaving GeoParquet compaction for a later resume.
        """
        total = 0
        rows = 0
        considered = 0
        pending: list[dict] = []
        touched: set[tuple[str, str]] = set()

        # Close any crash window left by a previous run (no-op list when
        # there are no journals), then journal this run's own batches.  The
        # journal is a direct-stage mechanism: ndjson's staged buckets are
        # re-compactable on their own, so ndjson runs journal nothing.
        recovery = recover_journals(
            self._store,
            self._warehouse_prefix,
            self._index,
            self._table,
            full_path=self._full_path,
        )
        journal = (
            BatchJournal(self._store, self._warehouse_prefix, new_run_id())
            if self._stage == "direct"
            else None
        )

        def _counted():
            nonlocal considered
            for pair in inventory:
                considered += 1
                yield pair

        if not self._skip_fetch:
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
                        rows += self._flush(pending, touched, journal, seq)
                pending = []
            else:
                for bucket, key in new_pairs:
                    item = self._fetch_fn(bucket, key)
                    if item is None:
                        continue
                    pending.append(item)
                    total += 1
                    if len(pending) >= self._batch_size:
                        rows += self._flush(pending, touched, journal)
                        pending = []

            if pending:
                rows += self._flush(pending, touched, journal)

        if self._stage == "ndjson" and not self._skip_compact:
            # Discover staged buckets even after a skip_fetch resume.
            if self._skip_fetch:
                touched = self._discover_staged_buckets()
            rows += self._compact_all(touched)

        summary = {
            "items": total,
            "rows": rows,
            "considered": considered,
            "stage": self._stage,
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

    def _discover_staged_buckets(self) -> set[tuple[str, str]]:
        """List every (cell, year) bucket that has staged NDJSON files."""
        buckets: set[tuple[str, str]] = set()
        prefix = self._ndjson_prefix + "/"
        for batch in obstore.list(self._store, prefix=prefix):
            for obj in batch:
                k: str = obj["path"]
                if not k.endswith(".jsonl"):
                    continue
                parts = k.split("/")
                # .../grid_partition=<cell>/year=<year>/<file>.jsonl
                for i, part in enumerate(parts):
                    if part.startswith("grid_partition="):
                        cell = part.split("=", 1)[1]
                        year = parts[i + 1].split("=", 1)[1] if i + 1 < len(parts) else "unknown"
                        buckets.add((cell, year))
        return buckets

    # -- internals ------------------------------------------------------------

    def _flush(
        self,
        items: list[dict],
        touched: set[tuple[str, str]],
        journal: BatchJournal | None = None,
        seq: int | None = None,
    ) -> int:
        if self._stage == "ndjson":
            return self._flush_ndjson(items, touched)
        return self._flush_direct(items, journal, seq)

    def _flush_direct(
        self,
        items: list[dict],
        journal: BatchJournal | None = None,
        seq: int | None = None,
    ) -> int:
        if journal is not None and seq is None:
            seq = journal.start_batch(
                [f"s3://{it['_source_bucket']}/{it['_source_key']}" for it in items]
            )
        on_file = (
            (lambda rel_key, rows: journal.record_file(seq, rel_key, rows))
            if journal is not None
            else None
        )
        new_paths, index_rows, rows = self._write_direct(items, on_file=on_file)
        if new_paths:
            # Iceberg commits first, the index part second: a crash between
            # the two leaves the rows in the journal, and recovery writes
            # exactly this part (deterministic {run_id}/{seq} name).
            self._table.add_files([self._full_path(k) for k in new_paths])
            if index_rows:
                part = f"{journal.run_id}/{seq:04d}" if journal is not None else None
                self._index.append(index_rows, part=part)
        if journal is not None:
            journal.finish_batch(seq)
        return rows

    def _write_direct(
        self, items: list[dict], on_file=None
    ) -> tuple[list[str], list[dict], int]:
        """Write items to GeoParquet — see :func:`_write_direct`."""
        return _write_direct(
            self._store, self._partitioner, self._warehouse_prefix, items, on_file=on_file
        )

    def _flush_ndjson(self, items: list[dict], touched: set[tuple[str, str]]) -> int:
        """Stage A only — fan out to per-(cell, year) NDJSON buckets.

        Records the buckets touched so the caller can compact each of them
        exactly once after all batches (Stage B).
        """
        fo = fan_out(items, self._partitioner) if self._partitioner else items
        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in fo:
            cell = item.get("properties", {}).get("grid_partition", "__none__")
            year = str(_year_from_item(item) or "unknown")
            buckets[(cell, year)].append(item)

        for (cell, year), group in buckets.items():
            touched.add((cell, year))
            self._append_ndjson(self._ndjson_key(cell, year), group)
        return 0

    def _ndjson_key(self, cell: str, year: str) -> str:
        return f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/staging.jsonl"

    def _compact_all(self, touched: set[tuple[str, str]]) -> int:
        """Stage B — compact every touched bucket once, then commit once.

        The staged NDJSON is only deleted *after* the Iceberg/index commit,
        so ``skip_fetch`` resumes (or a crash) re-compact without duplicating
        index rows; any GeoParquet written before a crash is simply garbage
        collected (it is never registered in Iceberg).
        """
        new_paths: list[str] = []
        index_rows: list[dict] = []
        consumed: list[str] = []
        total = 0
        for cell, year in sorted(touched):
            np, ir, n, ndjson_keys = self._compact_ndjson_bucket(cell, year)
            new_paths.extend(np)
            index_rows.extend(ir)
            consumed.extend(ndjson_keys)
            total += n

        if new_paths:
            self._table.add_files([self._full_path(k) for k in new_paths])
            if index_rows:
                self._index.append(index_rows)
        for key in consumed:
            try:
                obstore.delete(self._store, key)
            except Exception:
                pass
        return total

    def _compact_ndjson_bucket(
        self, cell: str, year: str
    ) -> tuple[list[str], list[dict], int, list[str]]:
        """Compact one ``(cell, year)`` bucket — see :func:`_compact_bucket`."""
        return _compact_bucket(
            self._store,
            self._ndjson_prefix,
            self._warehouse_prefix,
            (cell, year),
            delta=self._delta,
        )

    def _append_ndjson(self, key: str, items: list[dict]) -> None:
        _append_ndjson(self._store, key, items)


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


def _append_ndjson(store: object, key: str, items: list[dict]) -> None:
    """Append items to an NDJSON object, creating or extending it."""
    lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
    try:
        raw = bytes(obstore.get(store, key).bytes())
        merged = raw.decode("utf-8") + lines
        obstore.put(store, key, merged.encode("utf-8"))
    except FileNotFoundError:
        obstore.put(store, key, lines.encode("utf-8"))


def _put_ndjson(store: object, key: str, items: list[dict]) -> None:
    """Write items to a fresh NDJSON object (single PUT, no read-modify-write)."""
    lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
    obstore.put(store, key, lines.encode("utf-8"))


def _write_direct(
    store: object,
    partitioner,
    warehouse_prefix: str,
    items: list[dict],
    on_file=None,
) -> tuple[list[str], list[dict], int]:
    """Write items to GeoParquet without touching the table/index.

    Worker-safe (plain state only) so a distributed caller can fan out this
    function and commit once on the head.  *on_file(rel_key, index_rows)*,
    when given, fires after each file is durably written — the journal's
    per-file update.  Returns ``(new_paths, index_rows, rows)``.
    """
    fo = fan_out(items, partitioner) if partitioner else items
    if not fo:
        return [], [], 0

    rows = 0
    new_paths: list[str] = []
    index_rows = [_to_index_row(it) for it in items if it.get("_source_key")]

    for (cell, year), group in group_by_partition(fo).items():
        year_str = str(year) if year is not None else "unknown"
        key = f"{warehouse_prefix}/grid_partition={cell}/year={year_str}/part_{uuid.uuid4().hex[:8]}.parquet"
        n, _ = write_geoparquet_s3(group, store, key)
        if n > 0:
            new_paths.append(key)
            rows += n
            if on_file is not None:
                on_file(key, [_to_index_row(it) for it in group if it.get("_source_key")])

    return new_paths, index_rows, rows


def _fetch_items(fetch_fn, pairs: list[tuple[str, str]], concurrency: int) -> list:
    """Fetch STAC items for (bucket, key) pairs concurrently, dropping Nones.

    The default fetch (``earthcatalog.inventory.fetch_item``) runs through the
    async path (``obstore.get_async`` + orjson, like ``main``) — true I/O
    concurrency without a thread per request.  A custom ``fetch_fn`` (tests)
    falls back to a thread pool.
    """
    if fetch_fn is _inventory.fetch_item:
        return _inventory.fetch_items_async(pairs, concurrency=concurrency)

    if concurrency > 1 and len(pairs) > 1:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            return [it for it in ex.map(lambda bk: fetch_fn(bk[0], bk[1]), pairs) if it is not None]
    return [it for it in (fetch_fn(b, k) for b, k in pairs) if it is not None]


def _write_direct_shard(
    store: object,
    fetch_fn,
    partitioner,
    warehouse_prefix: str,
    shard,
    fetch_concurrency: int = 256,
) -> tuple[list[str], list[dict], int]:
    """Fetch each pair in *shard* (concurrent), then write-only fan-out."""
    pairs = list(_shard_iter_pairs(shard))
    items = _fetch_items(fetch_fn, pairs, fetch_concurrency)
    return _write_direct(store, partitioner, warehouse_prefix, items)


def _write_ndjson_shard(
    store: object,
    fetch_fn,
    partitioner,
    ndjson_prefix: str,
    shard_with_index,
    fetch_concurrency: int = 256,
) -> tuple[int, list[tuple[str, str]]]:
    """Fetch a shard (concurrent), fan out to NDJSON (worker task).

    Returns ``(n_items, touched_buckets)`` — the number of items staged and
    the ``(cell, year)`` buckets touched, so the head can compact each bucket
    exactly once.
    """
    shard_index, shard = shard_with_index
    pairs = list(_shard_iter_pairs(shard))
    items = _fetch_items(fetch_fn, pairs, fetch_concurrency)

    fo = fan_out(items, partitioner) if partitioner else items
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in fo:
        cell = item.get("properties", {}).get("grid_partition", "__none__")
        year = str(_year_from_item(item) or "unknown")
        buckets[(cell, year)].append(item)

    for (cell, year), group in buckets.items():
        # grid_partition (geometry) + year (time) + shard index: the index is a
        # deterministic integer (like main's chunk_id), so each shard writes a
        # unique file and concurrent workers never collide.
        key = f"{ndjson_prefix}/grid_partition={cell}/year={year}/shard_{shard_index}.jsonl"
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


def _next_part_index(store: object, warehouse_prefix: str, cell: str, year: str) -> int:
    """Next free ``part_N`` index for a (cell, year) partition (max + 1, or 0)."""
    prefix = f"{warehouse_prefix}/grid_partition={cell}/year={year}/"
    indices: list[int] = []
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
    store: object,
    ndjson_prefix: str,
    warehouse_prefix: str,
    bucket: tuple[str, str],
    delta: bool = False,
) -> tuple[list[str], list[dict], int, list[str]]:
    """Compact one ``(cell, year)`` NDJSON bucket into a single GeoParquet file.

    Worker-safe: takes only plain state (store, string prefixes, int) rather
    than the whole :class:`Ingester`, so it can be shipped to Dask workers
    without pickling the Iceberg table or unified index.  *bucket* is a
    single ``(cell, year)`` tuple so it maps cleanly over ``client.map``.

    Like main's warehouse consolidation, the whole partition is held in
    memory: every staged NDJSON file is streamed line-by-line (never a full
    ``.bytes()`` read), deduped exactly by item ID, sorted by
    ``(platform, datetime)``, and written as ONE deterministic
    ``part_{idx:06d}.parquet``.  A hot cell with ~500k items needs a few GB —
    scale the worker VM instead of batching.

    The output name is deterministic so a re-run overwrites the same file
    instead of orphaning ``part_<uuid>`` copies.  In full mode (*delta* False)
    the index is 0 (idempotent re-run); in delta mode (*delta* True) it
    continues from the next free ``part_N`` so existing files are never
    clobbered.

    Returns ``(new_paths, index_rows, rows, ndjson_keys)`` — the caller (head
    node) commits to Iceberg + index once, then deletes the consumed NDJSON.
    """
    cell, year = bucket
    bucket_dir = f"{ndjson_prefix}/grid_partition={cell}/year={year}/"
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

    idx = _next_part_index(store, warehouse_prefix, cell, year) if delta else 0
    out_key = f"{warehouse_prefix}/grid_partition={cell}/year={year}/part_{idx:06d}.parquet"
    n, _ = write_geoparquet_s3(sorted(items, key=_sort_key), store, out_key)
    if n == 0:
        return [], [], 0, jsonl_keys
    index_rows = [_to_index_row(it) for it in items if it.get("_source_key")]
    return [out_key], index_rows, n, jsonl_keys
