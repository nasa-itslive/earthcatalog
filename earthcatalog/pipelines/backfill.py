"""
Staging-based backfill pipeline for earthcatalog.

Architecture (four phases, fully idempotent)
--------------------------------------------

Phase 1 — Scheduler (head node)
    Reads the inventory manifest/CSV/Parquet and writes fixed-size chunk
    files (Parquet, one row per ``.stac.json`` item) to a staging prefix.
    Each chunk contains up to *chunk_size* ``(bucket, key)`` pairs.
    Already-written chunks are skipped on restart (idempotent).

Phase 2 — Ingest (one Dask task per chunk)
    Each worker:
    1. Reads its chunk Parquet from the staging store.
    2. Async-fetches all STAC JSONs using ``obstore.get_async`` with a
       ``TaskGroup`` + ``Semaphore``.
    3. Accumulates items in memory, fans out through the H3 partitioner.
    4. Writes each ``(cell, year)`` group as NDJSON to:
       ``staging/{cell}/{year}/chunk_{id}.ndjson``

Phase 3 — Compact (one Dask task per (cell, year))
    Each worker:
    1. Scans the staging prefix for ALL ``.ndjson`` files in its bucket.
    2. Reads them into memory, deduplicates by ``id``.
    3. Writes up to *compact_rows* rows per GeoParquet file.
    4. Does NOT delete NDJSON staging files (Phase 4 does that).

Phase 4 — Register (head node)
    1. Drops and recreates the Iceberg table.
    2. Registers all warehouse Parquet files via ``table.add_files()``.
    3. Uploads the catalog.
    4. Deletes all staging files.

Delta mode (``delta=True``)
---------------------------
Lightweight append-only path for small incremental ingests.

Phase 3 — Delta Compact
    Same as normal compact (NDJSON → GeoParquet) but output files are
    numbered starting from the next available index in the warehouse
    partition, so existing parquets are never overwritten.

Phase 4 — Delta Register
    Opens the existing Iceberg table (no drop) and calls
    ``table.add_files()`` with only the newly written parquets.

Spot resilience
---------------
Every phase is individually idempotent.  If a spot instance is killed:

- Phase 1: chunks already written survive; scheduler skips them.
- Phase 2: NDJSON files written by dead workers survive and are picked
  up by Phase 3 on the next run.
- Phase 3: each (cell, year) task scans S3 fresh.
- Phase 4: catalog is rebuilt from scratch (drop + recreate).
"""

from __future__ import annotations

import asyncio
import io
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import obstore
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

from earthcatalog.core.catalog import (
    open_catalog,
    upload_catalog,
)
from earthcatalog.core.transform import (
    fan_out,
    group_by_partition,
)
from earthcatalog.core.transform import (
    write_geoparquet as _write_geoparquet,
)
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.pipelines.incremental import _iter_inventory

_FETCH_RETRIES = 3
_FETCH_BACKOFF_BASE = 0.5
_FETCH_CONCURRENCY = 256


# ---------------------------------------------------------------------------
# Phase 1 — Scheduler: write chunk files to staging
# ---------------------------------------------------------------------------


def _list_existing_chunks(
    staging_store: object,
    staging_prefix: str,
) -> set[int]:
    """Return set of chunk IDs already present in the staging store."""
    prefix = staging_prefix.rstrip("/") + "/"
    existing: set[int] = set()
    try:
        result = obstore.list(staging_store, prefix=prefix)
        for objs in result:
            for obj in objs:
                name = str(obj["path"])
                if "/" in name:
                    name = name.rsplit("/", 1)[-1]
                if name.startswith("chunk_") and name.endswith(".parquet"):
                    existing.add(int(name.removeprefix("chunk_").removesuffix(".parquet")))
    except FileNotFoundError:
        pass
    return existing


def _list_completed_chunk_ids(
    staging_store: object,
    staging_prefix: str,
    pending_prefix: str,
) -> set[int]:
    """Return chunk IDs that have a completion marker and no pending file."""
    completed: set[int] = set()
    marker_prefix = f"{staging_prefix}/_completed/"
    try:
        result = obstore.list(staging_store, prefix=marker_prefix)
        for objs in result:
            for obj in objs:
                name = str(obj["path"])
                fname = name.rsplit("/", 1)[-1]
                if fname.startswith("chunk_") and fname.endswith(".done"):
                    cid_str = fname.removeprefix("chunk_").removesuffix(".done")
                    if cid_str.isdigit():
                        completed.add(int(cid_str))
    except FileNotFoundError:
        pass
    pending_ids = _list_existing_chunks(staging_store, pending_prefix)
    return completed - pending_ids


def _write_one_chunk(
    chunk_id: int,
    batch: list[tuple[str, str]],
    staging_store: object,
    staging_prefix: str,
) -> str:
    """Write a single chunk Parquet. Returns the chunk key."""
    chunk_key = f"{staging_prefix}/chunk_{chunk_id:06d}.parquet"
    tbl = pa.table({"bucket": [b for b, _ in batch], "key": [k for _, k in batch]})
    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    obstore.put(staging_store, chunk_key, buf.getvalue())
    return chunk_key


def write_chunks(
    inventory_path: str,
    staging_store: object,
    staging_prefix: str,
    chunk_size: int = 100_000,
    limit: int | None = None,
    since: datetime | None = None,
    write_concurrency: int = 8,
) -> list[str]:
    """
    Read inventory and write chunk Parquet files to the staging store.

    Streams from the inventory iterator into chunk-sized buffers and writes
    each chunk as soon as it's full — no need to materialize the entire
    inventory in memory.

    Already-written chunks are detected via a single ``obstore.list`` call
    upfront. Items belonging to existing chunks are still consumed from the
    inventory iterator but not buffered or written.

    Writes are parallelized with a thread pool.

    Returns list of chunk keys (including pre-existing ones).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import tqdm

    existing_ids = _list_existing_chunks(staging_store, staging_prefix)
    if existing_ids:
        max_existing = max(existing_ids)
        print(f"Phase 1: {len(existing_ids)} existing chunks found (up to chunk {max_existing})")
    else:
        max_existing = -1

    chunk_keys: dict[int, str] = {}
    written = 0
    skipped = len(existing_ids)
    chunk_id = 0
    buf: list[tuple[str, str]] = []
    total_items = 0

    for cid in existing_ids:
        chunk_keys[cid] = f"{staging_prefix}/chunk_{cid:06d}.parquet"

    contiguous_max = -1
    for i in range(max_existing + 1):
        if i in existing_ids:
            contiguous_max = i
        else:
            break

    items_to_skip = (contiguous_max + 1) * chunk_size

    with ThreadPoolExecutor(max_workers=write_concurrency) as pool:
        pending: dict[object, int] = {}
        pbar = tqdm.tqdm(desc="Phase 1", unit="chunk")

        def _collect(done_futures):
            nonlocal written
            for f in done_futures:
                key = f.result()
                cid = pending[f]
                chunk_keys[cid] = key
                written += 1
                del pending[f]
                pbar.update(1)

        inv_iter = _iter_inventory(inventory_path, since=since)

        if items_to_skip > 0:
            pbar.set_postfix_str(f"fast-forwarding {items_to_skip:,} items")
            chunk_id = contiguous_max + 1
            for b, k in inv_iter:
                if not k.endswith(".stac.json"):
                    continue
                total_items += 1
                if total_items >= items_to_skip:
                    break
            pbar.set_postfix_str(f"fast-forward done, at item {total_items:,}")

        for b, k in inv_iter:
            if not k.endswith(".stac.json"):
                continue
            total_items += 1

            buf.append((b, k))

            if len(buf) >= chunk_size:
                future = pool.submit(_write_one_chunk, chunk_id, buf, staging_store, staging_prefix)
                pending[future] = chunk_id
                chunk_id += 1
                buf = []

                done = [f for f in list(pending) if f.done()]
                _collect(done)

                pbar.set_postfix(
                    items=f"{total_items:,}",
                    written=written,
                    skipped=skipped,
                    pending=len(pending),
                    refresh=False,
                )

            if limit is not None and total_items >= limit:
                break

        if buf and chunk_id not in existing_ids:
            future = pool.submit(_write_one_chunk, chunk_id, buf, staging_store, staging_prefix)
            pending[future] = chunk_id

        for f in as_completed(pending):
            _collect([f])

        pbar.close()

    max_id = max(chunk_keys) if chunk_keys else -1
    result = [chunk_keys[i] for i in range(max_id + 1)]

    print(
        f"Scheduler: {total_items:,} .stac.json items → "
        f"{written} chunks written, {skipped} skipped (already exist)"
    )
    return result


# ---------------------------------------------------------------------------
# Phase 2 — Ingest worker: fetch + fan-out + write NDJSON
# ---------------------------------------------------------------------------


async def _fetch_item_async(store: object, bucket: str, key: str) -> dict | None:
    """
    Fetch one STAC JSON from S3 via obstore.get_async with retry.

    Uses pre-created *store* (one per chunk, not one per item).
    404 → None (skip).  403 → raise.  Other → retry 3× then raise.
    """
    last_exc: Exception | None = None

    for attempt in range(_FETCH_RETRIES + 1):
        try:
            result = await obstore.get_async(store, key)
            buf = await result.bytes_async()
            raw = memoryview(buf).tobytes()
            if not raw or raw[0:1] != b"{":
                body_preview = raw[:200].decode("utf-8", errors="replace")
                if b"SlowDown" in raw or b"<Error>" in raw:
                    raise OSError(f"S3 error response: {body_preview}")
                print(
                    f"WARN: unexpected content for s3://{bucket}/{key}: {body_preview}",
                    file=sys.stderr,
                )
                return None
            return orjson.loads(raw)
        except FileNotFoundError:
            print(f"WARN: s3://{bucket}/{key} not found — skipping", file=sys.stderr)
            return None
        except PermissionError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < _FETCH_RETRIES:
                await asyncio.sleep(_FETCH_BACKOFF_BASE * (2**attempt))

    raise RuntimeError(
        f"Failed to fetch s3://{bucket}/{key} after {_FETCH_RETRIES} retries: {last_exc}"
    ) from last_exc


async def _fetch_all_async(
    pairs: list[tuple[str, str]],
    concurrency: int,
    store: object,
) -> tuple[list[dict], list[tuple[str, str, str]]]:
    """
    Fetch multiple STAC items concurrently via obstore.get_async.

    Returns (items, failed) where failed is a list of
    ``(bucket, key, error_message)`` tuples.
    """
    from asyncio import Semaphore, TaskGroup

    sem = Semaphore(concurrency)
    results: dict[int, dict | None] = {}
    errors: dict[int, str] = {}

    async def _fetch(index: int, bucket: str, key: str) -> None:
        async with sem:
            try:
                results[index] = await _fetch_item_async(store, bucket, key)
            except Exception as exc:
                print(f"ERROR: failed to fetch s3://{bucket}/{key}: {exc}", file=sys.stderr)
                results[index] = None
                errors[index] = str(exc)

    async with TaskGroup() as tg:
        for i, (bucket, key) in enumerate(pairs):
            tg.create_task(_fetch(i, bucket, key))

    items: list[dict] = []
    failed: list[tuple[str, str, str]] = []
    for i, (bucket, key) in enumerate(pairs):
        if results.get(i) is not None:
            items.append(results[i])
        else:
            failed.append((bucket, key, errors.get(i, "unknown")))
    return items, failed


def _write_ndjson(items: list[dict], store: object, key: str) -> int:
    """Write a list of dicts as NDJSON to the store. Appends if file exists."""
    if not items:
        return 0
    new_lines = [orjson.dumps(item) for item in items]
    try:
        existing = memoryview(obstore.get(store, key).bytes()).tobytes()
        data = existing + b"\n" + b"\n".join(new_lines)
    except FileNotFoundError:
        data = b"\n".join(new_lines)
    obstore.put(store, key, data)
    return len(items)


def ingest_chunk(
    chunk_key: str,
    staging_store: object,
    staging_prefix: str,
    pending_prefix: str,
    partitioner: object,
    fetch_concurrency: int = _FETCH_CONCURRENCY,
) -> dict[str, Any]:
    """
    Phase 2 worker: read chunk → fetch items → fan-out → write NDJSON groups.

    Failed items are written to ``pending_prefix/chunk_{id}.parquet`` for
    retry on the next run.  If all items succeed, no pending file is created.

    Returns a report dict with counts and the list of NDJSON keys written.
    """
    raw = memoryview(obstore.get(staging_store, chunk_key).bytes()).tobytes()
    tbl = pq.ParquetFile(io.BytesIO(raw)).read()
    pairs = [
        (str(b), str(k))
        for b, k in zip(tbl.column("bucket").to_pylist(), tbl.column("key").to_pylist())
    ]

    source_items = len(pairs)
    if not pairs:
        return {
            "chunk_key": chunk_key,
            "source_items": 0,
            "fetched_items": 0,
            "fetch_failures": 0,
            "groups_written": 0,
            "fan_out_counter": {},
            "ndjson_keys": [],
        }

    from obstore.store import S3Store as _S3Store

    bucket = pairs[0][0]
    source_store = _S3Store(bucket=bucket, region="us-west-2", skip_signature=True)
    items, failed = asyncio.run(
        _fetch_all_async(pairs, concurrency=fetch_concurrency, store=source_store)
    )
    fetch_failures = len(failed)

    chunk_id = chunk_key.rsplit("/", 1)[-1].replace("chunk_", "").replace(".parquet", "")

    if failed:
        now = datetime.now(UTC).isoformat()
        pending_key = f"{pending_prefix}/chunk_{chunk_id}.parquet"
        fail_tbl = pa.table(
            {
                "bucket": [b for b, _, _ in failed],
                "key": [k for _, k, _ in failed],
                "error": [e for _, _, e in failed],
                "timestamp": [now] * len(failed),
            }
        )
        buf = io.BytesIO()
        pq.write_table(fail_tbl, buf)
        obstore.put(staging_store, pending_key, buf.getvalue())
    else:
        pending_key = f"{pending_prefix}/chunk_{chunk_id}.parquet"
        try:
            obstore.delete(staging_store, pending_key)
        except (FileNotFoundError, Exception):
            pass

    empty_report = {
        "chunk_key": chunk_key,
        "source_items": source_items,
        "fetched_items": len(items),
        "fetch_failures": fetch_failures,
        "groups_written": 0,
        "fan_out_counter": {},
        "ndjson_keys": [],
    }

    if not items:
        empty_report["fetched_items"] = 0
        return empty_report

    fan_out_items = fan_out(items, partitioner)
    if not fan_out_items:
        return empty_report

    cells_per_id: dict[str, set[str]] = defaultdict(set)
    for item in fan_out_items:
        cells_per_id[item["id"]].add(item["properties"].get("grid_partition", "__none__"))
    fan_out_counter: dict[int, int] = dict(Counter(len(cells) for cells in cells_per_id.values()))

    groups = group_by_partition(fan_out_items)
    ndjson_keys: list[str] = []

    for (cell, year), group_items in groups.items():
        ndjson_key = f"{staging_prefix}/{cell}/{year}/chunk_{chunk_id}.ndjson"
        n = _write_ndjson(group_items, staging_store, ndjson_key)
        if n > 0:
            ndjson_keys.append(ndjson_key)

    marker_key = f"{staging_prefix}/_completed/chunk_{chunk_id}.done"
    obstore.put(staging_store, marker_key, b"ok")

    return {
        "chunk_key": chunk_key,
        "source_items": source_items,
        "fetched_items": len(items),
        "fetch_failures": fetch_failures,
        "groups_written": len(ndjson_keys),
        "fan_out_counter": fan_out_counter,
        "ndjson_keys": ndjson_keys,
    }


# ---------------------------------------------------------------------------
# Phase 3 — Compact: NDJSON → GeoParquet
# ---------------------------------------------------------------------------


def _read_ndjson_files(store: object, keys: list[str]) -> list[dict]:
    """Read and parse multiple NDJSON files from the store."""
    items: list[dict] = []
    for key in keys:
        try:
            raw = memoryview(obstore.get(store, key).bytes()).tobytes()
        except FileNotFoundError:
            continue
        for line in raw.decode("utf-8").split("\n"):
            line = line.strip()
            if line:
                items.append(orjson.loads(line))
    return items


def _dedup_items(items: list[dict]) -> list[dict]:
    """Deduplicate by id, keeping the one with the most-recent `updated`."""
    best: dict[str, dict] = {}
    for item in items:
        item_id = item.get("id", "")
        updated = item.get("properties", {}).get("updated", "")
        existing = best.get(item_id)
        if existing is None or updated > (existing.get("properties", {}).get("updated", "")):
            best[item_id] = item
    return list(best.values())


def _scan_ndjson(store: object, prefix: str) -> list[str]:
    """List all .ndjson files under *prefix* in *store*."""
    keys: list[str] = []
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".ndjson"):
                keys.append(k)
    return keys


def _stream_compact(
    cell: str,
    year: str,
    staging_store: object,
    ndjson_keys: list[str],
    compact_rows: int,
    write_fn: Callable[[list[dict], str], int],
    out_key_fn: Callable[[int], str],
) -> dict[str, Any]:
    """
    Stream NDJSON → dedup → write GeoParquet in batches.

    Only holds ``compact_rows`` items + a set of seen IDs in memory.
    """
    seen_ids: set[str] = set()
    batch: list[dict] = []
    input_count = 0
    unique_count = 0
    out_keys: list[str] = []
    total_rows = 0
    part_idx = 0

    for key in ndjson_keys:
        try:
            raw = memoryview(obstore.get(staging_store, key).bytes()).tobytes()
        except FileNotFoundError:
            continue
        for line in raw.decode("utf-8").split("\n"):
            line = line.strip()
            if not line:
                continue
            input_count += 1
            item = orjson.loads(line)
            item_id = item.get("id", "")
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            unique_count += 1
            batch.append(item)

            if len(batch) >= compact_rows:
                out_path = out_key_fn(part_idx)
                n = write_fn(batch, out_path)
                if n > 0:
                    out_keys.append(out_path)
                    total_rows += n
                part_idx += 1
                batch = []

    if batch:
        out_path = out_key_fn(part_idx)
        n = write_fn(batch, out_path)
        if n > 0:
            out_keys.append(out_path)
            total_rows += n

    return {
        "cell": cell,
        "year": year,
        "input_files": len(ndjson_keys),
        "input_items": input_count,
        "unique_items": unique_count,
        "output_files": out_keys,
        "output_rows": total_rows,
    }


def compact_cell_year(
    cell: str,
    year: str,
    staging_store: object,
    staging_prefix: str,
    warehouse_root: str,
    compact_rows: int = 100_000,
) -> dict[str, Any]:
    """Phase 3 worker (local filesystem): stream NDJSON → write GeoParquet."""
    prefix = f"{staging_prefix}/{cell}/{year}/"
    ndjson_keys = _scan_ndjson(staging_store, prefix)

    if not ndjson_keys:
        return _empty_compact_report(cell, year)

    def _write_local(batch: list[dict], rel_path: str) -> int:
        out_dir = Path(warehouse_root) / f"grid_partition={cell}" / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        full = str(out_dir / rel_path)
        n = _write_geoparquet(batch, full)
        return n if n <= 0 else n

    def _out_key(idx: int) -> str:
        out_dir = Path(warehouse_root) / f"grid_partition={cell}" / f"year={year}"
        return str(out_dir / f"part_{idx:06d}.parquet")

    return _stream_compact(
        cell, year, staging_store, ndjson_keys, compact_rows, _write_local, _out_key
    )


def compact_cell_year_s3(
    cell: str,
    year: str,
    staging_store: object,
    staging_prefix: str,
    warehouse_store: object,
    compact_rows: int = 100_000,
) -> dict[str, Any]:
    """Phase 3 worker (S3): stream NDJSON → write GeoParquet to S3."""
    prefix = f"{staging_prefix}/{cell}/{year}/"
    ndjson_keys = _scan_ndjson(staging_store, prefix)

    if not ndjson_keys:
        return _empty_compact_report(cell, year)

    def _write_s3(batch: list[dict], out_path: str) -> int:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            n = _write_geoparquet(batch, tmp_path)
            if n > 0:
                data = Path(tmp_path).read_bytes()
                obstore.put(warehouse_store, out_path, data)
            return n
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def _out_key(idx: int) -> str:
        return f"grid_partition={cell}/year={year}/part_{idx:06d}.parquet"

    return _stream_compact(
        cell, year, staging_store, ndjson_keys, compact_rows, _write_s3, _out_key
    )


def _empty_compact_report(cell: str, year: str) -> dict[str, Any]:
    return {
        "cell": cell,
        "year": year,
        "input_files": 0,
        "input_items": 0,
        "unique_items": 0,
        "output_files": [],
        "output_rows": 0,
    }


def _next_part_index_local(warehouse_root: str, cell: str, year: str) -> int:
    part_dir = Path(warehouse_root) / f"grid_partition={cell}" / f"year={year}"
    if not part_dir.exists():
        return 0
    import re

    pattern = re.compile(r"part_(\d+)\.parquet$")
    indices = []
    for f in part_dir.iterdir():
        m = pattern.match(f.name)
        if m:
            indices.append(int(m.group(1)))
    return (max(indices) + 1) if indices else 0


def _next_part_index_s3(warehouse_store: object, cell: str, year: str) -> int:
    import re

    prefix = f"grid_partition={cell}/year={year}/"
    pattern = re.compile(r"part_(\d+)\.parquet$")
    indices = []
    for batch in obstore.list(warehouse_store, prefix=prefix):
        for obj in batch:
            fname = obj["path"].rsplit("/", 1)[-1]
            m = pattern.match(fname)
            if m:
                indices.append(int(m.group(1)))
    return (max(indices) + 1) if indices else 0


def compact_cell_year_delta(
    cell: str,
    year: str,
    staging_store: object,
    staging_prefix: str,
    warehouse_root: str,
    compact_rows: int = 100_000,
) -> dict[str, Any]:
    """Phase 3 delta worker (local): compact NDJSON → new GeoParquet (no overwrite)."""
    prefix = f"{staging_prefix}/{cell}/{year}/"
    ndjson_keys = _scan_ndjson(staging_store, prefix)

    if not ndjson_keys:
        return _empty_compact_report(cell, year)

    start_idx = _next_part_index_local(warehouse_root, cell, year)

    def _write_local(batch: list[dict], rel_path: str) -> int:
        out_dir = Path(warehouse_root) / f"grid_partition={cell}" / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        full = str(out_dir / rel_path)
        n = _write_geoparquet(batch, full)
        return n if n <= 0 else n

    def _out_key(idx: int) -> str:
        out_dir = Path(warehouse_root) / f"grid_partition={cell}" / f"year={year}"
        return str(out_dir / f"part_{start_idx + idx:06d}.parquet")

    return _stream_compact(
        cell, year, staging_store, ndjson_keys, compact_rows, _write_local, _out_key
    )


def compact_cell_year_delta_s3(
    cell: str,
    year: str,
    staging_store: object,
    staging_prefix: str,
    warehouse_store: object,
    compact_rows: int = 100_000,
) -> dict[str, Any]:
    """Phase 3 delta worker (S3): compact NDJSON → new GeoParquet (no overwrite)."""
    prefix = f"{staging_prefix}/{cell}/{year}/"
    ndjson_keys = _scan_ndjson(staging_store, prefix)

    if not ndjson_keys:
        return _empty_compact_report(cell, year)

    start_idx = _next_part_index_s3(warehouse_store, cell, year)

    def _write_s3(batch: list[dict], out_path: str) -> int:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            n = _write_geoparquet(batch, tmp_path)
            if n > 0:
                data = Path(tmp_path).read_bytes()
                obstore.put(warehouse_store, out_path, data)
            return n
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def _out_key(idx: int) -> str:
        return f"grid_partition={cell}/year={year}/part_{start_idx + idx:06d}.parquet"

    return _stream_compact(
        cell, year, staging_store, ndjson_keys, compact_rows, _write_s3, _out_key
    )


# ---------------------------------------------------------------------------
# Phase 4 — Register: rebuild Iceberg catalog + cleanup
# ---------------------------------------------------------------------------


def register_and_cleanup(
    catalog_path: str,
    warehouse_root: str,
    staging_store: object,
    staging_prefix: str,
    upload: bool = True,
) -> None:
    """
    Phase 4: rebuild Iceberg catalog from warehouse files, upload, cleanup staging.

    1. Drop and recreate the Iceberg table.
    2. Scan warehouse for all Parquet files.
    3. Register via table.add_files().
    4. Upload catalog.
    5. Delete all staging files.
    """
    import re

    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

    from earthcatalog.core.catalog import FULL_NAME, ICEBERG_SCHEMA, NAMESPACE, PARTITION_SPEC

    catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_root)

    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(FULL_NAME)
        print("Dropped stale Iceberg table.")
    except NoSuchTableError:
        pass

    table = catalog.create_table(
        identifier=FULL_NAME,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )

    hive_re = re.compile(
        r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
    )

    all_paths: list[str] = []
    if warehouse_root.startswith("s3://"):
        no_scheme = warehouse_root.removeprefix("s3://")
        _bucket, prefix = no_scheme.split("/", 1)
        for batch in obstore.list(staging_store, prefix=prefix):
            for obj in batch:
                k: str = obj["path"]
                if k.endswith(".parquet") and hive_re.search(k):
                    all_paths.append(f"s3://{_bucket}/{k}")
    else:
        for f in Path(warehouse_root).glob("**/*.parquet"):
            rel = f.relative_to(warehouse_root)
            if hive_re.search(str(rel)):
                all_paths.append(str(f))

    if all_paths:
        batch_size = 2000
        for i in range(0, len(all_paths), batch_size):
            table.add_files(all_paths[i : i + batch_size])
        print(f"Registered {len(all_paths):,} files in Iceberg catalog.")

    if upload:
        upload_catalog(catalog_path)

    cleanup_staging(staging_store, staging_prefix)


def register_delta(
    catalog_path: str,
    warehouse_root: str,
    new_parquet_paths: list[str],
    staging_store: object,
    staging_prefix: str,
    upload: bool = True,
) -> None:
    """
    Phase 4 delta: add new parquet files to existing Iceberg table (no drop).

    Opens (or creates) the Iceberg table, calls ``table.add_files()``
    with only the newly written parquets, then uploads the catalog.
    Existing warehouse files are never touched.
    """
    from pyiceberg.exceptions import (
        NamespaceAlreadyExistsError,
        NoSuchNamespaceError,
        NoSuchTableError,
    )

    from earthcatalog.core.catalog import FULL_NAME, ICEBERG_SCHEMA, NAMESPACE, PARTITION_SPEC

    catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_root)

    try:
        catalog.create_namespace(NAMESPACE)
    except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
        pass

    try:
        table = catalog.load_table(FULL_NAME)
        print(f"Opened existing Iceberg table {FULL_NAME}")
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        print(f"Created new Iceberg table {FULL_NAME}")

    if new_parquet_paths:
        batch_size = 2000
        for i in range(0, len(new_parquet_paths), batch_size):
            table.add_files(new_parquet_paths[i : i + batch_size])
        print(f"Added {len(new_parquet_paths):,} new files to Iceberg catalog.")
    else:
        print("No new parquet files to register.")

    if upload:
        upload_catalog(catalog_path)

    cleanup_staging(staging_store, staging_prefix)


def cleanup_staging(staging_store: object, staging_prefix: str) -> int:
    """Delete completion markers and pending files. Keeps chunks and NDJSON."""
    deleted = 0
    for batch in obstore.list(staging_store, prefix=f"{staging_prefix}/_completed/"):
        for obj in batch:
            k: str = obj["path"]
            try:
                obstore.delete(staging_store, k)
                deleted += 1
            except Exception:
                pass
    for batch in obstore.list(staging_store, prefix=f"{staging_prefix}/pending_chunks/"):
        for obj in batch:
            k: str = obj["path"]
            try:
                obstore.delete(staging_store, k)
                deleted += 1
            except Exception:
                pass
    print(f"Cleanup: deleted {deleted:,} staging files")
    return deleted


def _get_client(
    create_client: Callable[[], object] | None = None,
) -> object | None:
    """Return a Dask client: use *create_client* if provided, else try get_client()."""
    if create_client is not None:
        return create_client()
    try:
        from dask.distributed import get_client

        return get_client()
    except (ImportError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Orchestrator — run_backfill
# ---------------------------------------------------------------------------


def run_backfill(
    inventory_path: str,
    catalog_path: str,
    staging_store: object,
    staging_prefix: str,
    warehouse_store: object,
    warehouse_root: str,
    partitioner: object | None = None,
    h3_resolution: int = 1,
    chunk_size: int = 100_000,
    compact_rows: int = 100_000,
    fetch_concurrency: int = 256,
    limit: int | None = None,
    since: datetime | None = None,
    use_lock: bool = True,
    skip_inventory: bool = False,
    skip_ingest: bool = False,
    retry_pending: bool = False,
    delta: bool = False,
    create_client: Callable[[], object] | None = None,
) -> None:
    """
    Four-phase staging-based backfill pipeline.

    Parameters
    ----------
    inventory_path:
        Local path or s3:// URI to inventory (CSV, Parquet, or manifest.json).
    catalog_path:
        Local path for SQLite catalog.
    staging_store:
        obstore-compatible store for staging (chunks + NDJSON).
    staging_prefix:
        Key prefix within staging_store (e.g. "ingest").
    warehouse_store:
        obstore-compatible store for warehouse GeoParquet output.
        Unused for local filesystem (warehouse_root is used directly).
    warehouse_root:
        Path or s3:// URI for the warehouse root (used by add_files).
    partitioner:
        H3Partitioner or similar. Defaults to H3Partitioner(h3_resolution).
    chunk_size:
        Items per chunk Parquet in Phase 1.
    compact_rows:
        Max rows per output GeoParquet file in Phase 3.
    skip_ingest:
        If True, skip Phase 2 entirely and go straight to Phase 3 (Compact).
        Phase 3 scans S3 for existing NDJSON files. Useful when Phase 2 already
        completed but Phase 3 needs to be re-run (e.g. with bigger instances).
    retry_pending:
        If True, Phase 2 retries chunks that had fetch failures (stored in
        pending_chunks/). If False (default), pending chunks are logged but
        skipped — Phase 3 proceeds with whatever succeeded.
    delta:
        If True, run in delta mode: Phase 3 writes new parquets without
        overwriting existing ones, and Phase 4 adds files to the existing
        Iceberg table instead of dropping and recreating it.
    create_client:
        Optional callable that returns a Dask Client. Called lazily
        right before Phase 2 (after Phase 1 completes). Used for
        Coiled to avoid idle cluster timeout during long Phase 1 runs.
    """
    if partitioner is None:
        partitioner = H3Partitioner(resolution=h3_resolution)

    def _run() -> None:
        # Phase 1 — Scheduler
        print("\n" + "=" * 64)
        print("Phase 1 — Scheduler: writing chunk files")
        print("=" * 64)

        if skip_inventory:
            existing_ids = _list_existing_chunks(staging_store, f"{staging_prefix}/chunks")
            chunk_keys = [
                f"{staging_prefix}/chunks/chunk_{cid:06d}.parquet" for cid in sorted(existing_ids)
            ]
            print(f"Phase 1: --skip-inventory, using {len(chunk_keys)} existing chunks")
        else:
            chunk_keys = write_chunks(
                inventory_path=inventory_path,
                staging_store=staging_store,
                staging_prefix=f"{staging_prefix}/chunks",
                chunk_size=chunk_size,
                limit=limit,
                since=since,
            )

        # Create Dask client once — shared by Phase 2 and Phase 3
        client = _get_client(create_client)

        # Phase 2 — Ingest
        all_ndjson_keys: list[str] = []

        if skip_ingest:
            print("\n" + "=" * 64)
            print("Phase 2 — Ingest: SKIPPED (--skip-ingest)")
            print("=" * 64)
        else:
            print("\n" + "=" * 64)
            print("Phase 2 — Ingest: fetching items + writing NDJSON")
            print("=" * 64)

            pending_prefix = f"{staging_prefix}/pending_chunks"
            ndjson_prefix = f"{staging_prefix}/staging"

            completed_ids = _list_completed_chunk_ids(staging_store, ndjson_prefix, pending_prefix)
            if completed_ids:
                print(f"Phase 2: {len(completed_ids)} chunks already completed, skipping")

            pending_ids = _list_existing_chunks(staging_store, pending_prefix)
            if pending_ids and not retry_pending:
                print(
                    f"Phase 2: {len(pending_ids)} pending chunks recorded (use --retry-pending to retry)"
                )

            if retry_pending and pending_ids:
                pending_keys = [
                    f"{pending_prefix}/chunk_{cid:06d}.parquet" for cid in sorted(pending_ids)
                ]
                print(f"Phase 2: retrying {len(pending_ids)} pending chunks")
                chunk_keys = pending_keys

            if completed_ids:

                def _chunk_id_from_key(key: str) -> int:
                    fname = key.rsplit("/", 1)[-1]
                    return int(fname.removeprefix("chunk_").removesuffix(".parquet"))

                chunk_keys = [
                    ck for ck in chunk_keys if _chunk_id_from_key(ck) not in completed_ids
                ]
                print(f"Phase 2: {len(chunk_keys)} chunks remaining after skip")

            n_tasks = len(chunk_keys)

            if n_tasks > 0:
                if client is not None:
                    from dask.distributed import as_completed as distributed_ac

                    print(f"Submitting {n_tasks} ingest tasks via client.map …")
                    futures = client.map(
                        ingest_chunk,
                        chunk_keys,
                        staging_store=staging_store,
                        staging_prefix=f"{staging_prefix}/staging",
                        pending_prefix=pending_prefix,
                        partitioner=partitioner,
                        fetch_concurrency=fetch_concurrency,
                    )
                    with tqdm.tqdm(total=n_tasks, desc="Phase 2", unit="chunk") as pbar:
                        total_items = total_fetched = total_failures = total_groups = 0
                        for future in distributed_ac(futures):
                            r = future.result()
                            total_items += r["source_items"]
                            total_fetched += r["fetched_items"]
                            total_failures += r["fetch_failures"]
                            total_groups += r["groups_written"]
                            all_ndjson_keys.extend(r.get("ndjson_keys", []))
                            pbar.set_postfix(
                                fetched=f"{total_fetched:,}",
                                failed=total_failures,
                                groups=total_groups,
                                refresh=False,
                            )
                            pbar.update(1)
                else:
                    print(f"Processing {n_tasks} ingest tasks sequentially …")
                    with tqdm.tqdm(total=n_tasks, desc="Phase 2", unit="chunk") as pbar:
                        for ck in chunk_keys:
                            r = ingest_chunk(
                                chunk_key=ck,
                                staging_store=staging_store,
                                staging_prefix=f"{staging_prefix}/staging",
                                pending_prefix=pending_prefix,
                                partitioner=partitioner,
                                fetch_concurrency=fetch_concurrency,
                            )
                            all_ndjson_keys.extend(r.get("ndjson_keys", []))
                            pbar.update(1)

        # Phase 3 — Compact
        print("\n" + "=" * 64)
        print("Phase 3 — Compact: NDJSON → GeoParquet")
        print("=" * 64)

        ndjson_base = f"{staging_prefix}/staging"
        buckets: dict[tuple[str, str], None] = {}

        for nk in all_ndjson_keys:
            parts = nk.split("/")
            if len(parts) >= 3:
                buckets[(parts[-3], parts[-2])] = None

        if not buckets:
            for batch in obstore.list(staging_store, prefix=f"{ndjson_base}/"):
                for obj in batch:
                    path: str = obj["path"]
                    parts = path.split("/")
                    if len(parts) >= 3 and parts[-1].endswith(".ndjson"):
                        buckets[(parts[-3], parts[-2])] = None

        compact_results: list[dict] = []

        if not buckets:
            print("No NDJSON groups to compact.")
        else:
            is_s3 = warehouse_root.startswith("s3://")
            if delta:
                compact_fn = compact_cell_year_delta_s3 if is_s3 else compact_cell_year_delta
            else:
                compact_fn = compact_cell_year_s3 if is_s3 else compact_cell_year
            bucket_list = list(buckets.keys())

            print(f"Submitting {len(bucket_list)} compact tasks …")

            if client is not None:
                from dask.distributed import as_completed as distributed_ac

                common_kwargs: dict[str, Any] = {
                    "staging_store": staging_store,
                    "staging_prefix": f"{staging_prefix}/staging",
                    "compact_rows": compact_rows,
                }
                if is_s3:
                    common_kwargs["warehouse_store"] = warehouse_store
                else:
                    common_kwargs["warehouse_root"] = warehouse_root

                def _compact_task(cell_year):
                    cell, year = cell_year
                    return compact_fn(cell=cell, year=year, **common_kwargs)

                futures = client.map(_compact_task, bucket_list)
                with tqdm.tqdm(total=len(bucket_list), desc="Phase 3", unit="partition") as pbar:
                    for future in distributed_ac(futures):
                        r = future.result()
                        compact_results.append(r)
                        pbar.set_postfix(
                            rows=f"{r['output_rows']:,}",
                            refresh=False,
                        )
                        pbar.update(1)
            else:
                for cell, year in tqdm.tqdm(bucket_list, desc="Phase 3", unit="partition"):
                    kwargs = {
                        "cell": cell,
                        "year": year,
                        "staging_store": staging_store,
                        "staging_prefix": f"{staging_prefix}/staging",
                        "compact_rows": compact_rows,
                    }
                    if is_s3:
                        kwargs["warehouse_store"] = warehouse_store
                    else:
                        kwargs["warehouse_root"] = warehouse_root
                    compact_results.append(compact_fn(**kwargs))

            total_input = sum(r["input_items"] for r in compact_results)
            total_unique = sum(r["unique_items"] for r in compact_results)
            total_output = sum(r["output_rows"] for r in compact_results)
            total_files = sum(len(r["output_files"]) for r in compact_results)
            print(
                f"Compact done: {total_input:,} items → {total_unique:,} unique "
                f"→ {total_output:,} rows in {total_files:,} files"
            )

        # Phase 4 — Register + cleanup
        print("\n" + "=" * 64)
        if delta:
            print("Phase 4 — Delta Register: add files to existing catalog")
        else:
            print("Phase 4 — Register: rebuild catalog + cleanup")
        print("=" * 64)

        if delta:
            new_paths = []
            for r in compact_results:
                new_paths.extend(r.get("output_files", []))
            register_delta(
                catalog_path=catalog_path,
                warehouse_root=warehouse_root,
                new_parquet_paths=new_paths,
                staging_store=staging_store,
                staging_prefix=staging_prefix,
            )
        else:
            register_and_cleanup(
                catalog_path=catalog_path,
                warehouse_root=warehouse_root,
                staging_store=staging_store,
                staging_prefix=staging_prefix,
            )

    if use_lock:
        from earthcatalog.core.lock import S3Lock

        with S3Lock(owner="backfill-v2"):
            _run()
    else:
        _run()
