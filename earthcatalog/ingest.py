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

import obstore

from earthcatalog import inventory as _inventory
from earthcatalog.index import Index, hash_id
from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet_s3


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
        batch_size: int = 10_000,
    ) -> None:
        self._store = store
        self._index = index
        self._table = table
        self._fetch_fn = fetch_fn or _inventory.fetch_item
        self._partitioner = partitioner
        self._stage = stage
        self._warehouse_prefix = warehouse_prefix.rstrip("/")
        self._batch_size = batch_size
        self._ndjson_prefix = f"{self._warehouse_prefix}/staging/ndjson" if self._warehouse_prefix else "staging/ndjson"

    # -- public ---------------------------------------------------------------

    def run(self, inventory) -> dict:
        """Ingest ``(bucket, key)`` pairs from *inventory*; return a summary."""
        total = 0
        rows = 0
        pending: list[dict] = []

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
                rows += self._flush(pending)
                pending = []

        if pending:
            rows += self._flush(pending)

        return {"items": total, "rows": rows}

    # -- internals ------------------------------------------------------------

    def _flush(self, items: list[dict]) -> int:
        if self._stage == "ndjson":
            return self._flush_ndjson(items)
        return self._flush_direct(items)

    def _flush_direct(self, items: list[dict]) -> int:
        new_paths, index_rows, rows = self._write_direct(items)
        if new_paths:
            self._table.add_files([f"{key}" for key in new_paths])
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

    def _flush_ndjson(self, items: list[dict]) -> int:
        # Stage A — fan out to per-(cell, year) NDJSON buckets.
        fo = fan_out(items, self._partitioner) if self._partitioner else items
        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in fo:
            cell = item.get("properties", {}).get("grid_partition", "__none__")
            year = str(_year_from_item(item) or "unknown")
            buckets[(cell, year)].append(item)

        for (cell, year), group in buckets.items():
            self._append_ndjson(self._ndjson_key(cell, year), group)

        # Stage B — compact the buckets touched in this batch to GeoParquet.
        new_paths: list[str] = []
        index_rows: list[dict] = []
        rows = 0
        for (cell, year), _ in buckets.items():
            np, ir, n = self._compact_ndjson_bucket(cell, year)
            new_paths.extend(np)
            index_rows.extend(ir)
            rows += n

        if new_paths:
            self._table.add_files([f"{k}" for k in new_paths])
            if index_rows:
                self._index.append(index_rows)
        return rows

    def _ndjson_key(self, cell: str, year: str) -> str:
        return f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/staging.jsonl"

    def _compact_ndjson_bucket(self, cell: str, year: str) -> tuple[list[str], list[dict], int]:
        """Read a bucket's NDJSON, dedup vs the index, and write GeoParquet.

        Returns ``(new_paths, index_rows, rows)`` in the same shape as
        :meth:`_write_direct` so a distributed head can commit once.
        """
        # Gather every .jsonl file in the bucket directory (single-node uses
        # staging.jsonl; distributed workers write shard_<i>.jsonl).
        bucket_dir = f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/"
        jsonl_keys: list[str] = []
        try:
            for batch in obstore.list(self._store, prefix=bucket_dir):
                for obj in batch:
                    k: str = obj["path"]
                    if k.endswith(".jsonl"):
                        jsonl_keys.append(k)
        except Exception:
            return [], [], 0
        if not jsonl_keys:
            return [], [], 0

        items: list[dict] = []
        for key in sorted(jsonl_keys):
            raw = bytes(obstore.get(self._store, key).bytes())
            items.extend(
                json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()
            )
        if not items:
            return [], [], 0

        # Dedup against the index's known id hashes (idempotent re-runs).
        known = self._index.hash_set()
        unique: list[dict] = []
        seen: set[str] = set()
        for it in items:
            sid = it.get("id")
            if not sid or sid in seen:
                continue
            seen.add(sid)
            if hash_id(sid) in known:
                continue
            unique.append(it)

        # Write as one GeoParquet file for this (cell, year) bucket.
        fo = fan_out(unique, self._partitioner) if self._partitioner else unique
        index_rows = [_to_index_row(it) for it in unique if it.get("_source_key")]
        new_paths: list[str] = []
        rows = 0
        for (gcell, gyear), group in group_by_partition(fo).items():
            year_str = str(gyear) if gyear is not None else "unknown"
            out_key = f"{self._warehouse_prefix}/grid_partition={gcell}/year={year_str}/part_{uuid.uuid4().hex[:8]}.parquet"
            n, _ = write_geoparquet_s3(group, self._store, out_key)
            if n > 0:
                new_paths.append(out_key)
                rows += n

        return new_paths, index_rows, rows

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

    Each worker runs :meth:`Ingester._write_direct` (write-only GeoParquet)
    and returns ``(new_paths, index_rows, rows)``; the head node then calls
    ``table.add_files()`` and ``index.append()`` exactly once, so the shared
    index file is never written concurrently.

    *client* must expose ``map(fn, shards)`` (a Dask ``Client`` works).
    """

    def run(self, shards, *, client) -> dict:
        """Ingest each *shard* (a list of ``(bucket, key)`` pairs) in parallel."""
        if self._stage == "ndjson":
            return self._run_ndjson(shards, client=client)

        results = client.map(self._write_direct_with_fetch, list(shards))

        new_paths: list[str] = []
        index_rows: list[dict] = []
        total = 0
        for new_paths_i, index_rows_i, rows in results:
            new_paths.extend(new_paths_i)
            index_rows.extend(index_rows_i)
            total += rows

        if new_paths:
            self._table.add_files([f"{k}" for k in new_paths])
            if index_rows:
                self._index.append(index_rows)

        return {"items": total, "rows": total}

    def _write_direct_with_fetch(self, shard):
        """Fetch each (bucket, key) in *shard*, then write-only fan-out."""
        items = [self._fetch_fn(b, k) for b, k in shard]
        items = [it for it in items if it is not None]
        return self._write_direct(items)

    def _run_ndjson(self, shards, *, client) -> dict:
        """Distributed NDJSON mode: workers write NDJSON, head compacts once.

        Each worker fans out its shard into per-(cell, year) NDJSON buckets,
        writing to a shard-unique key so concurrent workers never clobber.
        The head then reads every bucket, dedups against the index, and
        writes GeoParquet + commits ``add_files``/``index.append`` once.
        """
        results = client.map(self._write_ndjson_with_fetch, enumerate(list(shards)))

        # Collect every (cell, year) bucket produced by any worker.
        buckets: set[tuple[str, str]] = set()
        for touched in results:
            buckets.update(touched)

        new_paths: list[str] = []
        index_rows: list[dict] = []
        total = 0
        for cell, year in sorted(buckets):
            np, ir, n = self._compact_ndjson_bucket(cell, year)
            new_paths.extend(np)
            index_rows.extend(ir)
            total += n

        if new_paths:
            self._table.add_files([f"{k}" for k in new_paths])
            if index_rows:
                self._index.append(index_rows)

        return {"items": total, "rows": total}

    def _write_ndjson_with_fetch(self, shard_with_index):
        """Fetch a shard, fan out to NDJSON; return the (cell, year) buckets touched."""
        shard_index, shard = shard_with_index
        items = [self._fetch_fn(b, k) for b, k in shard]
        items = [it for it in items if it is not None]

        fo = fan_out(items, self._partitioner) if self._partitioner else items
        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in fo:
            cell = item.get("properties", {}).get("grid_partition", "__none__")
            year = str(_year_from_item(item) or "unknown")
            buckets[(cell, year)].append(item)

        for (cell, year), group in buckets.items():
            key = f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/shard_{shard_index}.jsonl"
            self._append_ndjson(key, group)

        return list(buckets.keys())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
