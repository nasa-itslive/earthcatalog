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
from earthcatalog.index import Index
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
        fo = fan_out(items, self._partitioner) if self._partitioner else items
        if not fo:
            return 0

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

        if new_paths:
            self._table.add_files([f"{key}" for key in new_paths])
            if index_rows:
                self._index.append(index_rows)
        return rows

    def _flush_ndjson(self, items: list[dict]) -> int:
        # Stage A — fan out to per-(cell, year) NDJSON buckets.
        fo = fan_out(items, self._partitioner) if self._partitioner else items
        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in fo:
            cell = item.get("properties", {}).get("grid_partition", "__none__")
            year = str(_year_from_item(item) or "unknown")
            buckets[(cell, year)].append(item)

        for (cell, year), group in buckets.items():
            key = f"{self._ndjson_prefix}/grid_partition={cell}/year={year}/staging.jsonl"
            self._append_ndjson(key, group)
        return len(fo)

    def _append_ndjson(self, key: str, items: list[dict]) -> None:
        lines = "\n".join(json.dumps(it, default=str) for it in items) + "\n"
        try:
            raw = bytes(obstore.get(self._store, key).bytes())
            merged = raw.decode("utf-8") + lines
            obstore.put(self._store, key, merged.encode("utf-8"))
        except FileNotFoundError:
            obstore.put(self._store, key, lines.encode("utf-8"))


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
