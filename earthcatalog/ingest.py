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
import uuid
from collections import defaultdict
from collections.abc import Iterator

import obstore

from earthcatalog import inventory as _inventory
from earthcatalog.index import Index
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
        compact_rows: int = 100_000,
        skip_fetch: bool = False,
        skip_compact: bool = False,
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
        self._compact_rows = compact_rows
        self._skip_fetch = skip_fetch
        self._skip_compact = skip_compact
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
        pending: list[dict] = []
        touched: set[tuple[str, str]] = set()

        if not self._skip_fetch:
            for bucket, key in inventory:
                src = f"s3://{bucket}/{key}"
                if self._index.contains_source_key(src):
                    continue
                item = self._fetch_fn(bucket, key)
                if item is None:
                    continue
                pending.append(item)
                total += 1
                if len(pending) >= self._batch_size:
                    rows += self._flush(pending, touched)
                    pending = []

            if pending:
                rows += self._flush(pending, touched)

        if self._stage == "ndjson" and not self._skip_compact:
            # Discover staged buckets even after a skip_fetch resume.
            if self._skip_fetch:
                touched = self._discover_staged_buckets()
            rows += self._compact_all(touched)

        return {"items": total, "rows": rows}

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

    def _flush(self, items: list[dict], touched: set[tuple[str, str]]) -> int:
        if self._stage == "ndjson":
            return self._flush_ndjson(items, touched)
        return self._flush_direct(items)

    def _flush_direct(self, items: list[dict]) -> int:
        new_paths, index_rows, rows = self._write_direct(items)
        if new_paths:
            self._table.add_files([self._full_path(k) for k in new_paths])
            if index_rows:
                self._index.append(index_rows)
        return rows

    def _write_direct(self, items: list[dict]) -> tuple[list[str], list[dict], int]:
        """Write items to GeoParquet without touching the table/index.

        Returns ``(new_paths, index_rows, rows)`` so a distributed caller can
        gather results from many workers and commit once on the head node.
        """
        fo = fan_out(items, self._partitioner) if self._partitioner else items
        if not fo:
            return [], [], 0

        rows = 0
        new_paths: list[str] = []
        index_rows = [_to_index_row(it) for it in items if it.get("_source_key")]

        for (cell, year), group in group_by_partition(fo).items():
            year_str = str(year) if year is not None else "unknown"
            key = f"{self._warehouse_prefix}/grid_partition={cell}/year={year_str}/part_{uuid.uuid4().hex[:8]}.parquet"
            n, _ = write_geoparquet_s3(group, self._store, key)
            if n > 0:
                new_paths.append(key)
                rows += n

        return new_paths, index_rows, rows

    def _flush_ndjson(self, items: list[dict], touched: set[tuple[str, str]]) -> int:
        """Stage A only — fan out to per-(cell, year) NDJSON buckets.

        Records the buckets touched so the caller can compact each of them
        exactly once after all batches (memory-bounded Stage B).
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
        """Memory-bounded compact: stream NDJSON → dedup → write GeoParquet.

        Only ``compact_rows`` items are held in memory at a time.  Dedup uses
        an exact ``set`` of item IDs — for the largest cells (~500k items per
        cell/year) this is ~25–50 MB, and unlike a Bloom filter it never
        drops a legitimate item (a Bloom filter's ~0.1% false-positive rate
        would lose ~500 real items per hot cell).  Each batch is sorted by
        ``(platform, datetime)`` and written to its own ``part_<uuid>.parquet``
        (uuid names keep incremental runs from clobbering prior files).

        Returns ``(new_paths, index_rows, rows, ndjson_keys)`` — the caller
        commits once, then deletes the consumed NDJSON.
        """
        bucket_dir = f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/"
        jsonl_keys: list[str] = []
        try:
            for listing in obstore.list(self._store, prefix=bucket_dir):
                for obj in listing:
                    k: str = obj["path"]
                    if k.endswith(".jsonl"):
                        jsonl_keys.append(k)
        except Exception:
            return [], [], 0, []
        if not jsonl_keys:
            return [], [], 0, []

        seen: set[str] = set()
        batch: list[dict] = []
        index_rows: list[dict] = []
        new_paths: list[str] = []
        rows = 0

        def _write_batch() -> None:
            nonlocal rows
            if not batch:
                return
            out_key = (
                f"{self._warehouse_prefix}/grid_partition={cell}/year={year}/"
                f"part_{uuid.uuid4().hex[:8]}.parquet"
            )
            sorted_batch = sorted(batch, key=_sort_key)
            n, _ = write_geoparquet_s3(sorted_batch, self._store, out_key)
            if n > 0:
                new_paths.append(out_key)
                rows += n
                index_rows.extend(_to_index_row(it) for it in batch if it.get("_source_key"))
            batch.clear()

        for key in sorted(jsonl_keys):
            result = obstore.get(self._store, key)
            for item in iter_ndjson_lines(result.stream()):
                item_id = item.get("id")
                if not item_id or item_id in seen:
                    continue
                seen.add(item_id)
                batch.append(item)
                if len(batch) >= self._compact_rows:
                    _write_batch()

        _write_batch()
        return new_paths, index_rows, rows, jsonl_keys

    def _append_ndjson(self, key: str, items: list[dict]) -> None:
        lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
        try:
            raw = bytes(obstore.get(self._store, key).bytes())
            merged = raw.decode("utf-8") + lines
            obstore.put(self._store, key, merged.encode("utf-8"))
        except FileNotFoundError:
            obstore.put(self._store, key, lines.encode("utf-8"))


class DaskIngester(Ingester):
    """Distributed variant: shards the inventory and writes via ``client.map``.

    Each *shard* is an :class:`earthcatalog.inventory.InventoryShard` spec
    (preferred — the worker streams its own inventory part files, so only
    file keys cross the wire) or a plain sequence of ``(bucket, key)``
    pairs.  In ``"ndjson"`` stage, workers write per-shard NDJSON and the
    head compacts + commits once.  In ``"direct"`` stage, each worker runs
    :meth:`Ingester._write_direct` (write-only GeoParquet) and returns
    ``(new_paths, index_rows, rows)``; the head node then calls
    ``table.add_files()`` and ``index.append()`` exactly once, so the shared
    index file is never written concurrently.

    *client* must expose ``map(fn, shards)`` (a Dask ``Client`` works).
    """

    def run(self, inventory, *, client=None) -> dict:  # type: ignore[override]
        """Ingest each shard in *inventory* in parallel; workers stream their own pairs."""
        if client is None:
            raise ValueError("DaskIngester.run requires a client exposing map(fn, shards)")
        if self._stage == "ndjson":
            return self._run_ndjson(inventory, client=client)

        results = client.map(self._write_direct_with_fetch, list(inventory))

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

    def _write_direct_with_fetch(self, shard):
        """Fetch each pair in *shard*, then write-only fan-out."""
        items = [self._fetch_fn(b, k) for b, k in _shard_iter_pairs(shard)]
        items = [it for it in items if it is not None]
        return self._write_direct(items)

    def _run_ndjson(self, shards, *, client) -> dict:
        """Distributed NDJSON mode: workers write NDJSON, head compacts once.

        Each worker fans out its shard into per-(cell, year) NDJSON buckets,
        writing to a shard-unique key so concurrent workers never clobber.
        The head then reads every bucket, dedups by item ID, and writes
        GeoParquet + commits ``add_files``/``index.append`` once, then
        deletes the staged NDJSON so the run is resumable and idempotent.
        """
        results = client.map(self._write_ndjson_with_fetch, enumerate(list(shards)))

        # Collect every (cell, year) bucket produced by any worker.
        buckets: set[tuple[str, str]] = set()
        for touched in results:
            buckets.update(touched)

        new_paths: list[str] = []
        index_rows: list[dict] = []
        consumed: list[str] = []
        total = 0
        for cell, year in sorted(buckets):
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

        return {"items": total, "rows": total}

    def _write_ndjson_with_fetch(self, shard_with_index):
        """Fetch a shard, fan out to NDJSON; return the (cell, year) buckets touched."""
        shard_index, shard = shard_with_index
        items = [self._fetch_fn(b, k) for b, k in _shard_iter_pairs(shard)]
        items = [it for it in items if it is not None]

        fo = fan_out(items, self._partitioner) if self._partitioner else items
        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in fo:
            cell = item.get("properties", {}).get("grid_partition", "__none__")
            year = str(_year_from_item(item) or "unknown")
            buckets[(cell, year)].append(item)

        for (cell, year), group in buckets.items():
            key = (
                f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/shard_{shard_index}.jsonl"
            )
            self._append_ndjson(key, group)

        return list(buckets.keys())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _shard_iter_pairs(shard) -> Iterator[tuple[str, str]]:
    """Iterate a shard: an :class:`InventoryShard` spec or a plain pair list."""
    if isinstance(shard, _inventory.InventoryShard):
        return shard.iter_pairs()
    return iter(shard)


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
        "year": _year_from_item(item) or 0,
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
