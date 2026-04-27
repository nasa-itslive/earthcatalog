"""
Dask distributed backfill pipeline for earthcatalog.

Architecture (three phases)
--------------------------

Phase 1 — ``ingest_chunk`` (one Dask task per inventory chunk)
    Each worker fetches STAC JSON from S3, fans out through the H3 partitioner,
    groups by ``(grid_partition, year)``, and writes one GeoParquet file per
    group directly to the warehouse store via ``write_geoparquet_s3``.
    Returns a list of ``FileMetadata`` dicts (~200 B each) — no Parquet data
    crosses the network to the head node.

Phase 2 — ``compact_group`` (one Dask task per (cell, year) bucket)
    Each worker downloads all part files for its bucket from the warehouse store,
    merges them into one Arrow table, sorts by ``(platform, datetime)``, writes
    one consolidated GeoParquet file back to the store, and deletes the originals.
    Returns a single ``FileMetadata``.
    If a bucket already has only one file, the task is a no-op passthrough.

Phase 3 — catalog registration (head node only)
    ``table.add_files(final_paths)`` registers all compacted files in exactly
    one Iceberg snapshot.  The catalog is then uploaded to the configured store.

Key invariant
-------------
Parquet data never travels through the head node.  Workers own all I/O.
The head node handles only inventory streaming, metadata collection (~200 B/file),
and the final ``table.add_files()`` call.

CLI
---
::

    # Synchronous (single-process, readable tracebacks — good for debugging)
    python -m earthcatalog.pipelines.backfill \\
        --inventory /tmp/inventory.csv \\
        --catalog   /tmp/catalog.db \\
        --warehouse /tmp/warehouse \\
        --scheduler synchronous --limit 200

    # Local Dask cluster
    python -m earthcatalog.pipelines.backfill \\
        --inventory /tmp/inventory.csv \\
        --catalog   /tmp/catalog.db \\
        --warehouse /tmp/warehouse \\
        --scheduler local --workers 4

    # Coiled cloud cluster
    python -m earthcatalog.pipelines.backfill \\
        --inventory  s3://my-bucket/inventory.parquet \\
        --catalog    /tmp/catalog.db \\
        --warehouse  /tmp/warehouse \\
        --scheduler  coiled \\
        --coiled-n-workers   20 \\
        --coiled-software    my-software-env \\
        --coiled-region      us-west-2
"""

import concurrent.futures
import io
import json
import sys
import tempfile
import uuid
from collections import Counter, defaultdict
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path

import dask
import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from earthcatalog.core.catalog import (
    download_catalog,
    get_or_create_table,
    open_catalog,
    upload_catalog,
)
from earthcatalog.core.lock import S3Lock
from earthcatalog.core.transform import (
    FileMetadata,
    fan_out,
    group_by_partition,
    write_geoparquet_s3,
)
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.pipelines.incremental import (
    _iter_inventory,
    _iter_inventory_file_from_store,
    _list_manifest_files,
)

# ---------------------------------------------------------------------------
# Default S3 item fetcher (production) — async via rustac
# ---------------------------------------------------------------------------

_S3_FETCH_RETRIES = 3
_S3_FETCH_BACKOFF_BASE = 0.5
_FETCH_CONCURRENCY = 256


async def _fetch_item_async(bucket: str, key: str) -> dict | None:
    """Fetch one STAC JSON item from S3 via rustac with retry and backoff.

    Error handling
    --------------
    * ``404 Not Found`` / ``NoSuchKey`` → return ``None`` (graceful skip — the
      inventory may be stale).
    * ``403 Forbidden`` / ``AccessDenied`` → raise immediately (auth failure,
      will crash the task — fails loud).
    * All other errors (network, 5xx, etc.) → retry 3×, then **raise** (fails
      loud rather than silently producing empty results).
    """
    import asyncio

    import rustac
    from obstore.store import S3Store

    store = S3Store(bucket=bucket, region="us-west-2", skip_signature=True)
    href = f"s3://{bucket}/{key}"
    last_exc = None
    for attempt in range(_S3_FETCH_RETRIES + 1):
        try:
            return await rustac.read(href, store=store)
        except Exception as exc:
            last_exc = exc
            msg = str(exc)
            if "404 Not Found" in msg or "NoSuchKey" in msg:
                print(f"WARN: {href} not found in S3 — skipping", file=sys.stderr)
                return None
            if "403 Forbidden" in msg or "AccessDenied" in msg:
                raise
            if attempt < _S3_FETCH_RETRIES:
                delay = _S3_FETCH_BACKOFF_BASE * (2**attempt)
                print(
                    f"RETRY {attempt + 1}/{_S3_FETCH_RETRIES} {href}: {exc} — waiting {delay:.1f}s",
                    file=sys.stderr,
                )
                await asyncio.sleep(delay)

    print(
        f"FATAL: failed to fetch {href} after {_S3_FETCH_RETRIES} retries: {last_exc}",
        file=sys.stderr,
    )
    raise RuntimeError(
        f"Failed to fetch {href} after {_S3_FETCH_RETRIES} retries: {last_exc}"
    ) from last_exc


async def _fetch_items_async(
    pairs: list[tuple[str, str]],
    concurrency: int = _FETCH_CONCURRENCY,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """Fetch multiple STAC items concurrently via rustac.

    Returns (items, failed_pairs) where items are successfully fetched dicts
    and failed_pairs are the (bucket, key) pairs that failed after retries.
    """
    from asyncio import Semaphore, TaskGroup

    sem = Semaphore(concurrency)
    results: dict[int, dict | None] = {}

    async def _fetch(index: int, bucket: str, key: str) -> None:
        async with sem:
            try:
                results[index] = await _fetch_item_async(bucket, key)
            except Exception as exc:
                print(
                    f"ERROR: failed to fetch s3://{bucket}/{key}: {exc}",
                    file=sys.stderr,
                )
                results[index] = None

    async with TaskGroup() as tg:
        for i, (bucket, key) in enumerate(pairs):
            tg.create_task(_fetch(i, bucket, key))

    items: list[dict] = []
    failed: list[tuple[str, str]] = []
    for i, (bucket, key) in enumerate(pairs):
        if results[i] is not None:
            items.append(results[i])
        else:
            failed.append((bucket, key))
    return items, failed


def _s3_fetcher(bucket: str, key: str) -> dict | None:
    """Sync wrapper for tests — fetches a single item (no concurrency)."""
    import asyncio

    return asyncio.get_event_loop().run_until_complete(_fetch_item_async(bucket, key))


# ---------------------------------------------------------------------------
# Phase 1 — ingest_chunk
# ---------------------------------------------------------------------------


def _ingest_chunk_impl(
    bucket_key_pairs: list[tuple[str, str]],
    partitioner: object,
    warehouse_store: object,
    chunk_id: int,
    item_fetcher: object = None,
    max_workers: int = 16,
    fetch_concurrency: int = 256,
) -> tuple[list[FileMetadata], Counter, int]:
    """
    Phase 1 worker: fetch STAC items → fan_out → group_by_partition → write.

    Parameters
    ----------
    bucket_key_pairs:
        List of ``(bucket, key)`` pairs to fetch from S3.
    partitioner:
        An :class:`~earthcatalog.core.partitioner.AbstractPartitioner` instance
        (e.g. ``H3Partitioner``).  Must be serialisable by cloudpickle.
    warehouse_store:
        An ``obstore``-compatible store where GeoParquet files are written.
        Use ``LocalStore`` or ``MemoryStore`` for tests; ``S3Store`` for
        production.
    chunk_id:
        Integer chunk identifier used in the output filename.
    item_fetcher:
        Callable ``(bucket, key) -> dict | None``.  When ``None``, the async
        rustac-based fetcher is used (ignores *max_workers* — concurrency is
        controlled by ``_FETCH_CONCURRENCY``).  Pass a custom callable for tests.
    max_workers:
        Ignored when *item_fetcher* is ``None`` (async path).  When a custom
        *item_fetcher* is provided, this is the thread-pool size.

    Returns
    -------
    A tuple of:

    - ``list[FileMetadata]`` — one entry per ``(grid_partition, year)`` group
      written.  Empty groups produce no entry.  No Parquet data is returned;
      only tiny metadata records (~200 B each).
    - ``Counter[int]`` — fan-out distribution: maps number-of-cells (1, 2, 3,
      …) to count-of-source-items that landed in that many cells.  Used by the
      driver to compute overlap statistics without a post-hoc warehouse scan.
    - ``int`` — number of items that failed to fetch (returned ``None``).
    """
    import asyncio

    if item_fetcher is None:
        items, failed = asyncio.run(
            _fetch_items_async(bucket_key_pairs, concurrency=fetch_concurrency)
        )
        fetch_failures = len(failed)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(lambda bc: item_fetcher(*bc), bucket_key_pairs))
        items = [r for r in results if r is not None]
        fetch_failures = len(results) - len(items)

    if not items:
        return [], Counter(), len(bucket_key_pairs)

    fan_out_items = fan_out(items, partitioner)
    if not fan_out_items:
        return [], Counter(), fetch_failures

    # Compute fan-out distribution: how many distinct cells did each item land in?
    cells_per_id: dict[str, set[str]] = defaultdict(set)
    for item in fan_out_items:
        item_id = item["id"]
        cell = item["properties"].get("grid_partition", "__none__")
        cells_per_id[item_id].add(cell)
    fan_out_counter: Counter[int] = Counter(len(cells) for cells in cells_per_id.values())

    groups = group_by_partition(fan_out_items)
    file_metas: list[FileMetadata] = []

    for (cell, year), group_items in groups.items():
        year_str = str(year) if year is not None else "unknown"
        key = (
            f"grid_partition={cell}/year={year_str}/"
            f"part_{chunk_id:06d}_{uuid.uuid4().hex[:8]}.parquet"
        )
        n_rows, n_bytes = write_geoparquet_s3(group_items, warehouse_store, key)
        if n_rows > 0:
            file_metas.append(
                FileMetadata(
                    s3_key=key,
                    grid_partition=cell,
                    year=year,
                    row_count=n_rows,
                    file_size_bytes=n_bytes,
                )
            )

    return file_metas, fan_out_counter, fetch_failures


#: Dask-delayed version of :func:`_ingest_chunk_impl`.
ingest_chunk = dask.delayed(_ingest_chunk_impl)


# ---------------------------------------------------------------------------
# Phase 1 (file-level) — ingest_file
# ---------------------------------------------------------------------------


def _ingest_file_impl(
    inv_data_key: str,
    inv_store: object,
    partitioner: object,
    warehouse_store: object,
    chunk_size: int,
    max_workers: int,
    since: datetime | None,
    item_fetcher: object = None,
    fetch_concurrency: int = 256,
) -> tuple[list[FileMetadata], Counter, int, int]:
    """
    Phase 1 worker (file-level): fetch one inventory Parquet file, process all
    STAC items it references, and write GeoParquet part files to the warehouse.

    Unlike :func:`_ingest_chunk_impl`, this function receives only a tiny S3
    key reference — the inventory Parquet is downloaded by the worker itself.
    This keeps the Dask graph small regardless of inventory file size.

    Internally the items are processed in batches of *chunk_size* using a
    thread pool (same parallelism model as :func:`_ingest_chunk_impl`).

    Parameters
    ----------
    inv_data_key:
        Object key of the inventory Parquet file within *inv_store*.
    inv_store:
        Authenticated ``obstore``-compatible store for the inventory bucket.
        Credentials are embedded in the store object so no credential lookup
        is required on the remote worker.
    partitioner:
        An :class:`~earthcatalog.core.partitioner.AbstractPartitioner` instance.
    warehouse_store:
        An ``obstore``-compatible store for GeoParquet output.
    chunk_size:
        Number of STAC items per internal processing batch.
    max_workers:
        Thread-pool size for parallel STAC item fetching within each batch.
    since:
        When set, only inventory rows modified on or after this timestamp
        are processed.

    Returns
    -------
    A tuple of:

    - ``list[FileMetadata]`` — one entry per ``(grid_partition, year)`` group
      written across all batches.
    - ``Counter[int]`` — fan-out distribution across all batches.
    - ``int`` — total number of ``.stac.json`` source items processed.
    - ``int`` — total number of items that failed to fetch across all batches.
    """
    import io as _io

    # Import here so the function is self-contained on the remote worker.
    from earthcatalog.pipelines.incremental import _iter_inventory_parquet

    raw_bytes = bytes(obstore.get(inv_store, inv_data_key).bytes())
    pairs = [
        (b, k)
        for b, k in _iter_inventory_parquet(_io.BytesIO(raw_bytes), since=since)
        if k.endswith(".stac.json")
    ]

    n_pairs = len(pairs)
    n_chunks = (n_pairs + chunk_size - 1) // chunk_size if n_pairs else 0
    file_label = inv_data_key.rsplit("/", 1)[-1]
    print(f"[{file_label}] {n_pairs:,} items → {n_chunks} chunks")

    all_metas: list[FileMetadata] = []
    all_fan_out: Counter = Counter()
    source_items = 0
    total_fetch_failures = 0
    chunk_id = 0

    for i in range(0, len(pairs), chunk_size):
        batch = pairs[i : i + chunk_size]
        if not batch:
            continue
        metas, fan_out_ctr, fetch_failures = _ingest_chunk_impl(
            batch,
            partitioner,
            warehouse_store,
            chunk_id,
            item_fetcher,
            max_workers=max_workers,
            fetch_concurrency=fetch_concurrency,
        )
        all_metas.extend(metas)
        all_fan_out += fan_out_ctr
        source_items += len(batch)
        total_fetch_failures += fetch_failures
        chunk_id += 1
        print(
            f"[{file_label}] chunk {chunk_id}/{n_chunks} done"
            f" — {source_items:,}/{n_pairs:,} items"
            f", {len(all_metas)} part files so far"
            f", {total_fetch_failures} fetch failures"
        )

    return all_metas, all_fan_out, source_items, total_fetch_failures


#: Dask-delayed version of :func:`_ingest_file_impl`.
ingest_file = dask.delayed(_ingest_file_impl)


# ---------------------------------------------------------------------------
# Phase 2 — compact_group
# ---------------------------------------------------------------------------


def _compact_group_impl(
    file_metas: list[FileMetadata],
    out_key: str,
    store: object,
) -> FileMetadata:
    """
    Phase 2 worker: merge all part files for one ``(cell, year)`` bucket,
    deduplicate rows by ``id``, sort, and write one consolidated GeoParquet
    file to *store*.

    If the bucket has only one file the file is returned as-is (no I/O).

    Steps for multi-file buckets:

    1. **Download** all part files from *store*.
    2. **Concatenate** Arrow tables (schema metadata including ``geo`` and
       ``ARROW:extension:name: geoarrow.wkb`` is preserved from the first
       table).
    3. **Deduplicate** on ``id``: sort by ``(id ASC, updated DESC NULLS LAST)``
       and keep the first (most-recent) occurrence of each id.
    4. **Sort** by ``(platform, datetime)`` for Parquet locality.
    5. **Write** one consolidated GeoParquet file to *store* at *out_key*.
    6. **Delete** all input part files.

    .. note::
        The orphan sweep (deleting ``part_`` files not in this run) was
        intentionally removed from this function.  It is only safe when one
        writer owns a given ``(cell, year)`` prefix at a time, which is
        guaranteed only in ``final_compact.py``.  Running it here during
        concurrent ingest caused workers to delete each other's registered
        part files (race condition).

    Parameters
    ----------
    file_metas:
        Metadata for all part files belonging to one ``(grid_partition, year)``
        bucket — all must have the same ``grid_partition`` and ``year``.
    out_key:
        Destination key within *store* for the compacted file.
    store:
        An ``obstore``-compatible store.

    Returns
    -------
    A single :class:`~earthcatalog.core.transform.FileMetadata` for the
    compacted output file.
    """
    cell = file_metas[0].grid_partition
    year = file_metas[0].year

    # ------------------------------------------------------------------
    # Single-file passthrough.
    # ------------------------------------------------------------------
    if len(file_metas) == 1:
        return file_metas[0]

    # ------------------------------------------------------------------
    # Download and read all part files.
    # ------------------------------------------------------------------
    tables: list[pa.Table] = []
    for fm in file_metas:
        raw = bytes(obstore.get(store, fm.s3_key).bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        tables.append(tbl)

    # Merge: pa.concat_tables preserves schema metadata (geo, field metadata)
    # from the first table when all schemas are identical.
    merged = pa.concat_tables(tables, promote_options="none")

    # ------------------------------------------------------------------
    # Deduplicate on `id`: keep the row with the most-recent `updated`
    # timestamp.  Sort so the best candidate for each id comes first, then
    # take the first occurrence.
    # ------------------------------------------------------------------
    sort_indices = pc.sort_indices(
        merged,
        sort_keys=[("id", "ascending"), ("updated", "descending")],
        null_placement="at_end",
    )
    merged = merged.take(sort_indices)
    id_col = merged.column("id").to_pylist()
    seen: set = set()
    keep: list[int] = []
    for i, id_val in enumerate(id_col):
        if id_val not in seen:
            seen.add(id_val)
            keep.append(i)
    if len(keep) < merged.num_rows:
        print(f"INFO: dedup removed {merged.num_rows - len(keep)} duplicate rows in {out_key}")
    merged = merged.take(pa.array(keep, type=pa.int64()))

    # Sort by (platform, datetime) for optimal Parquet predicate pushdown.
    merged = merged.sort_by(
        [
            ("platform", "ascending"),
            ("datetime", "ascending"),
        ]
    )

    # ------------------------------------------------------------------
    # Write compacted file to temp, then upload.
    # ------------------------------------------------------------------
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(merged, tmp_path)
        data = Path(tmp_path).read_bytes()
        obstore.put(store, out_key, data)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    # Delete input part files now that the compacted file is safely written.
    for fm in file_metas:
        try:
            obstore.delete(store, fm.s3_key)
        except Exception as exc:
            print(f"WARN: could not delete {fm.s3_key}: {exc}")

    return FileMetadata(
        s3_key=out_key,
        grid_partition=cell,
        year=year,
        row_count=merged.num_rows,
        file_size_bytes=len(data),
    )


#: Dask-delayed version of :func:`_compact_group_impl`.
compact_group = dask.delayed(_compact_group_impl)


# ---------------------------------------------------------------------------
# Ingest stats helpers
# ---------------------------------------------------------------------------


def _load_processed_files(report_location: str) -> set[str]:
    """
    Read ``ingest_stats.json`` and return the set of ``inventory_file`` keys
    that have already been processed in prior runs.

    Used by the spot-resilient per-file mini-run loop to skip data files that
    were successfully registered in a previous (possibly interrupted) run.

    Returns an empty set if the file does not exist, is not readable, or no
    record contains an ``inventory_file`` field.

    Parameters
    ----------
    report_location:
        Local path or ``s3://`` URI to the ``ingest_stats.json`` file.
    """
    try:
        if report_location.startswith("s3://"):
            import obstore as _obs

            no_scheme = report_location.removeprefix("s3://")
            s3_bucket, s3_key = no_scheme.split("/", 1)
            import configparser as _cp
            import os as _os

            from obstore.store import S3Store as _S3S

            key_id = _os.environ.get("AWS_ACCESS_KEY_ID")
            secret = _os.environ.get("AWS_SECRET_ACCESS_KEY")
            token = _os.environ.get("AWS_SESSION_TOKEN")
            region = (
                _os.environ.get("AWS_DEFAULT_REGION")
                or _os.environ.get("AWS_REGION")
                or "us-west-2"
            )
            if not (key_id and secret):
                cfg = _cp.ConfigParser()
                cfg.read(_os.path.expanduser("~/.aws/credentials"))
                profile = _os.environ.get("AWS_PROFILE", "default")
                if profile in cfg:
                    key_id = cfg[profile].get("aws_access_key_id", key_id)
                    secret = cfg[profile].get("aws_secret_access_key", secret)
                    token = cfg[profile].get("aws_session_token", token) or token
            sk: dict = dict(bucket=s3_bucket, region=region)
            if key_id:
                sk["aws_access_key_id"] = key_id
            if secret:
                sk["aws_secret_access_key"] = secret
            if token:
                sk["aws_session_token"] = token
            raw = bytes(_obs.get(_S3S(**sk), s3_key).bytes())
            records: list[dict] = json.loads(raw)
        else:
            p = Path(report_location)
            if not p.exists():
                return set()
            records = json.loads(p.read_text())
        return {r["inventory_file"] for r in records if r.get("inventory_file")}
    except Exception as exc:
        print(f"WARN: could not load processed files from {report_location}: {exc}")
        return set()


def _write_ingest_stats(
    catalog_path: str,
    inventory_path: str,
    since: datetime | None,
    source_items: int,
    total_rows: int,
    final_metas: list[FileMetadata],
    fan_out_counter: Counter,
    h3_resolution: int,
    report_location: str | None = None,
    inventory_file: str | None = None,
    fetch_failures: int = 0,
) -> None:
    """
    Append one run record to the ingest stats JSON file and, if the target is
    on S3, upload it via obstore.

    Resolution order for the output path
    -------------------------------------
    1. ``report_location`` if supplied (local path **or** ``s3://`` URI).
    2. When ``report_location`` is ``None`` and ``catalog_path`` is a local
       path: ``<catalog_dir>/ingest_stats.json`` (original behaviour).
    3. When ``report_location`` is ``None`` and ``catalog_path`` is on S3
       (i.e. begins with ``s3://``): ``<catalog_s3_dir>/ingest_stats.json``
       — uploaded via the catalog's obstore after writing a temp local copy.

    The file is a JSON array; each element represents one pipeline run.
    If the file does not exist it is created.  If it already exists the new
    record is appended so the full run history is preserved.

    Fields written per run
    ----------------------
    run_id              ISO-8601 UTC timestamp of this run
    inventory           inventory path or URI passed to the pipeline
    inventory_file      individual data-file key within the manifest (or null)
    since               --since cutoff (ISO string) or null
    h3_resolution       H3 resolution used for spatial partitioning
    source_items        number of STAC items read from the inventory
    total_rows          total rows written (source_items × avg_fan_out)
    fetch_failures      number of items that failed to fetch after retries
    unique_cells        number of distinct H3 cells with data
    unique_years        number of distinct acquisition years with data
    files_written       number of Parquet files registered in Iceberg
    avg_fan_out         total_rows / source_items (rounded to 3 d.p.)
    overhead_pct        (total_rows - source_items) / source_items × 100
    fan_out_distribution  mapping of n_cells → item_count
    """
    record: dict = {
        "run_id": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "inventory": inventory_path,
        "inventory_file": inventory_file,
        "since": since.isoformat() if since else None,
        "h3_resolution": h3_resolution,
        "source_items": source_items,
        "total_rows": total_rows,
        "fetch_failures": fetch_failures,
        "unique_cells": len({fm.grid_partition for fm in final_metas}),
        "unique_years": len({fm.year for fm in final_metas}),
        "files_written": len(final_metas),
        "avg_fan_out": round(total_rows / source_items, 3) if source_items else 0,
        "overhead_pct": round((total_rows - source_items) * 100 / source_items, 1)
        if source_items
        else 0,
        "fan_out_distribution": {str(k): v for k, v in sorted(fan_out_counter.items())},
    }

    # ------------------------------------------------------------------
    # Determine effective target: explicit override > catalog sibling
    # ------------------------------------------------------------------
    effective: str = report_location or (Path(catalog_path).parent / "ingest_stats.json").as_posix()
    target_s3 = effective.startswith("s3://")

    # Always write to a local staging file first
    if target_s3:
        import configparser
        import os
        import tempfile

        import obstore as _obstore
        from obstore.store import S3Store

        no_scheme = effective.removeprefix("s3://")
        s3_bucket, s3_key = no_scheme.split("/", 1)

        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        token = os.environ.get("AWS_SESSION_TOKEN")
        region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
        if not (key_id and secret):
            cfg = configparser.ConfigParser()
            cfg.read(os.path.expanduser("~/.aws/credentials"))
            profile = os.environ.get("AWS_PROFILE", "default")
            if profile in cfg:
                key_id = cfg[profile].get("aws_access_key_id", key_id)
                secret = cfg[profile].get("aws_secret_access_key", secret)
                token = cfg[profile].get("aws_session_token", token) or token
        sk: dict = dict(bucket=s3_bucket, region=region)
        if key_id:
            sk["aws_access_key_id"] = key_id
        if secret:
            sk["aws_secret_access_key"] = secret
        if token:
            sk["aws_session_token"] = token
        rpt_store = S3Store(**sk)

        _tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        local_stats_path = Path(_tmp.name)
        _tmp.close()

        # Download existing stats from S3 before appending so we don't lose
        # records from previous mini-runs (Bug 1 fix).
        try:
            existing_bytes = bytes(_obstore.get(rpt_store, s3_key).bytes())
            local_stats_path.write_bytes(existing_bytes)
        except Exception:
            pass  # file doesn't exist yet — start fresh
    else:
        local_stats_path = Path(effective)
        local_stats_path.parent.mkdir(parents=True, exist_ok=True)

    records: list[dict] = []
    if local_stats_path.exists():
        try:
            records = json.loads(local_stats_path.read_text())
        except Exception:
            records = []  # corrupt file — start fresh

    records.append(record)
    local_stats_path.write_text(json.dumps(records, indent=2))

    if target_s3:
        _obstore.put(rpt_store, s3_key, local_stats_path.read_bytes())
        local_stats_path.unlink(missing_ok=True)
        print(f"Ingest stats: {effective}")
    else:
        print(f"Ingest stats: {local_stats_path}")


# ---------------------------------------------------------------------------
# Orchestrator — run_backfill
# ---------------------------------------------------------------------------


def run_backfill(
    inventory_path: str,
    catalog_path: str,
    warehouse_store: object,
    warehouse_root: str,
    item_fetcher: object = None,
    partitioner: object = None,
    h3_resolution: int = 1,
    chunk_size: int = 20_000,
    max_workers_per_task: int = 16,
    fetch_concurrency: int = 256,
    compact: bool = True,
    compact_threshold: int = 2,
    limit: int | None = None,
    use_lock: bool = True,
    since: datetime | None = None,
    report_location: str | None = None,
    per_file_mini_runs: bool = True,
    files_per_batch: int = 1,
    grid_config=None,
) -> None:
    """
    Three-phase Dask distributed backfill pipeline.

    Phase 1 — ``ingest_chunk`` tasks (one per *chunk_size* items):
        Each Dask worker fetches STAC items, fans them out, and writes
        GeoParquet files to *warehouse_store*.  Returns ``FileMetadata`` only.

    Phase 2 — ``compact_group`` tasks (one per ``(cell, year)`` bucket):
        Only dispatched when *compact* is ``True`` and a bucket has ≥
        *compact_threshold* part files.  Merges part files into one
        consolidated file.

    Phase 3 — catalog registration (head node):
        ``table.add_files()`` is called once with all final file paths →
        exactly one Iceberg snapshot regardless of chunk count.

    Spot resilience (per_file_mini_runs)
    -------------------------------------
    When *per_file_mini_runs* is ``True`` (default) **and** *inventory_path*
    is an S3 Inventory ``manifest.json``, the pipeline processes each manifest
    data file as an independent mini-run:

    1. Read ``ingest_stats.json`` (at *report_location*) to find already-
       processed data files → skip them.
    2. For each remaining data file:

       a. Acquire the S3 lock.
       b. Download the catalog → open the Iceberg table.
       c. Ingest: fetch items, fan-out, write GeoParquet.
       d. Compact: merge part files per (cell, year) bucket.
       e. Register: ``table.add_files()`` + ``upload_catalog()``.
       f. Append one record to ``ingest_stats.json`` (with
          ``inventory_file`` set to the data-file key).
       g. Release the lock.

    If the spot instance is interrupted between step (e) and (g) the
    catalog is already updated; on restart, step 1 detects the committed
    record and skips the file.  At worst one data file is re-processed
    (idempotent: dedup by ``id`` during compaction handles duplicate rows).

    When *inventory_path* is not a manifest (CSV, CSV.gz, single Parquet),
    *per_file_mini_runs* is ignored and the original monolithic behaviour
    applies (one lock acquisition, one ``add_files`` call at the end).

    Parameters
    ----------
    inventory_path:
        Local path or ``s3://`` URI to an S3 Inventory file
        (CSV, CSV.gz, or Parquet) **or** an S3 Inventory ``manifest.json``.
    catalog_path:
        Local path where the SQLite catalog is written.
    warehouse_store:
        ``obstore``-compatible store used by all workers for GeoParquet I/O.
    warehouse_root:
        Filesystem root (local path) or S3 prefix (``s3://bucket/prefix``)
        prepended to each ``FileMetadata.s3_key`` to form the absolute path
        registered with ``table.add_files()``.
    item_fetcher:
        Callable ``(bucket, key) -> dict | None`` for fetching STAC JSON.
        When ``None``, the default public-S3 fetcher is used.  Pass a custom
        callable for unit tests.
    partitioner:
        An :class:`~earthcatalog.core.partitioner.AbstractPartitioner`.
        Defaults to ``H3Partitioner(resolution=h3_resolution)``.
    h3_resolution:
        H3 grid resolution used when *partitioner* is ``None``.
    chunk_size:
        Number of inventory items per ingest task.
    max_workers_per_task:
        Thread-pool size used inside each ``ingest_chunk`` worker for
        parallel item fetching.
    compact:
        When ``True`` (default), dispatch compact tasks for any
        bucket with ≥ *compact_threshold* part files.
    compact_threshold:
        Minimum part-file count for a bucket to be compacted.
    limit:
        Stop after ingesting this many ``.stac.json`` items.  ``None``
        means process the entire inventory.
    use_lock:
        Wrap each mini-run (or the full run) in an ``S3Lock``.  Set to
        ``False`` for tests.
    since:
        When set (timezone-aware UTC), only inventory rows with
        ``last_modified_date >= since`` are ingested.
    report_location:
        Local path or ``s3://`` URI where ``ingest_stats.json`` is written.
        When ``None``, co-located with the catalog.
    per_file_mini_runs:
        When ``True`` (default) and *inventory_path* is a manifest, process
        each manifest data file as a separate mini-run with its own lock
        cycle and catalog upload.  Set to ``False`` to use the original
        monolithic behaviour regardless of inventory type.
    files_per_batch:
        Number of manifest data files to read before submitting tasks to
        Dask.  Increasing this saturates the worker pool when individual
        inventory files are small (few chunks each).  Checkpoint granularity
        stays per-file — one ``ingest_stats.json`` record is written for
        every inventory file in the batch after the batch completes.
        Default ``1`` preserves the original one-file-at-a-time behaviour.
        Recommended: set to ``ceil(n_workers / expected_chunks_per_file)``
        so the Dask graph always has at least ``n_workers`` tasks ready.
    """
    if partitioner is None:
        partitioner = H3Partitioner(resolution=h3_resolution)

    def _join_path(root: str, key: str) -> str:
        if root.startswith("s3://"):
            return root.rstrip("/") + "/" + key
        return str(Path(root) / key)

    def _build_ingest_tasks(
        item_pairs: list[tuple[str, str]],
        chunk_id_start: int,
    ) -> tuple[list, int, int]:
        """
        Build Dask delayed ingest tasks for *item_pairs*.

        Returns
        -------
        tasks:
            List of ``dask.delayed`` ingest_chunk calls.
        source_items:
            Number of ``.stac.json`` items found in *item_pairs*.
        next_chunk_id:
            Chunk-id counter after the last task — pass as *chunk_id_start*
            for the next call within the same batch to avoid name collisions.
        """
        tasks: list = []
        chunk: list[tuple[str, str]] = []
        chunk_id = chunk_id_start
        source_items = 0
        for bucket, key in item_pairs:
            if not key.endswith(".stac.json"):
                continue
            chunk.append((bucket, key))
            source_items += 1
            if len(chunk) >= chunk_size:
                tasks.append(
                    ingest_chunk(
                        list(chunk),
                        partitioner,
                        warehouse_store,
                        chunk_id,
                        item_fetcher,
                        max_workers_per_task,
                        fetch_concurrency,
                    )
                )
                chunk.clear()
                chunk_id += 1
            if limit and source_items >= limit:
                break
        if chunk:
            tasks.append(
                ingest_chunk(
                    list(chunk),
                    partitioner,
                    warehouse_store,
                    chunk_id,
                    item_fetcher,
                    max_workers_per_task,
                    fetch_concurrency,
                )
            )
            chunk_id += 1
        return tasks, source_items, chunk_id

    def _run_one(
        item_pairs: list[tuple[str, str]],
        inv_file_key: str | None,
        total_items_hint: int,
    ) -> None:
        """
        Run the full three-level pipeline for *item_pairs* and register the
        results into the Iceberg catalog.

        Parameters
        ----------
        item_pairs:
            List of ``(bucket, key)`` pairs to process.
        inv_file_key:
            The data-file key within the manifest (used in stats).  ``None``
            for monolithic (non-manifest) runs.
        total_items_hint:
            Pre-counted number of items (used only for logging).
        """
        download_catalog(catalog_path)
        catalog = open_catalog(
            db_path=catalog_path,
            warehouse_path=warehouse_root,
        )
        table = get_or_create_table(catalog, grid_config=grid_config)

        # Ingest ─ dispatch ingest_chunk tasks
        ingest_tasks, total_items, _ = _build_ingest_tasks(item_pairs, chunk_id_start=0)

        if not ingest_tasks:
            print(f"  no .stac.json items found in {inv_file_key or 'chunk'}")
            # Still write stats so the caller knows this file was processed.
            _write_ingest_stats(
                catalog_path=catalog_path,
                inventory_path=inventory_path,
                since=since,
                source_items=0,
                total_rows=0,
                final_metas=[],
                fan_out_counter=Counter(),
                h3_resolution=h3_resolution,
                report_location=report_location,
                inventory_file=inv_file_key,
            )
            return

        print(f"Ingest: {len(ingest_tasks)} chunks ({total_items} items)")
        all_nested: tuple[tuple[list[FileMetadata], Counter, int], ...] = dask.compute(
            *ingest_tasks
        )
        all_metas: list[FileMetadata] = [fm for file_metas, _, _ in all_nested for fm in file_metas]
        fan_out_counter: Counter[int] = sum((counter for _, counter, _ in all_nested), Counter())
        total_rows = sum(fm.row_count for fm in all_metas)
        print(f"Ingest done: {len(all_metas)} files, {total_rows} rows")

        # Compact ─ merge part files within each (cell, year) bucket
        final_metas: list[FileMetadata] = []

        if compact:
            buckets: dict[tuple[str, int | None], list[FileMetadata]] = defaultdict(list)
            for fm in all_metas:
                buckets[(fm.grid_partition, fm.year)].append(fm)

            compact_tasks = []
            compact_pass_through: list[FileMetadata] = []

            for (cell, year), fms in buckets.items():
                if len(fms) >= compact_threshold:
                    year_str = str(year) if year is not None else "unknown"
                    out_key = (
                        f"grid_partition={cell}/year={year_str}/"
                        f"compacted_{uuid.uuid4().hex[:8]}.parquet"
                    )
                    compact_tasks.append(compact_group(fms, out_key, warehouse_store))
                else:
                    compact_pass_through.extend(fms)

            print(
                f"Compact: {len(compact_tasks)} merge tasks, "
                f"{len(compact_pass_through)} files passed through"
            )

            if compact_tasks:
                compacted: tuple[FileMetadata, ...] = dask.compute(*compact_tasks)
                final_metas.extend(compacted)

            final_metas.extend(compact_pass_through)
        else:
            final_metas = all_metas

        # Register ─ add all files to the Iceberg catalog in one snapshot
        final_paths = [_join_path(warehouse_root, fm.s3_key) for fm in final_metas]

        if final_paths:
            print(f"Register: {len(final_paths)} files in one Iceberg snapshot")
            table.add_files(final_paths)

        upload_catalog(catalog_path)

        _write_ingest_stats(
            catalog_path=catalog_path,
            inventory_path=inventory_path,
            since=since,
            source_items=total_items,
            total_rows=total_rows,
            final_metas=final_metas,
            fan_out_counter=fan_out_counter,
            h3_resolution=h3_resolution,
            report_location=report_location,
            inventory_file=inv_file_key,
        )

        print(
            f"\nMini-run done. {total_items} items → {total_rows} rows "
            f"across {len(final_paths)} files"
        )
        print(f"Snapshots in catalog: {len(table.history())}")

    # ------------------------------------------------------------------
    # Decide: per-file mini-runs (manifest) vs monolithic
    # ------------------------------------------------------------------
    is_manifest = inventory_path.endswith("manifest.json")

    if per_file_mini_runs and is_manifest:
        # ── Spot-resilient path: one Dask task per manifest data file ─────
        #
        # Each worker receives only the S3 key of its inventory Parquet file
        # (~100 bytes) — the actual (bucket, key) pairs are never serialised
        # into the Dask graph, so graph size stays near zero regardless of
        # file size.
        #
        # All pending files are submitted at once.  When a dask.distributed
        # Client is active (Coiled / LocalCluster), as_completed() is used so
        # each file is checkpointed the moment it finishes, maximising both
        # worker utilisation and checkpoint granularity.  When no distributed
        # client is available (synchronous scheduler / tests), files are
        # processed one at a time via the original _run_one path.
        effective_report = report_location
        if effective_report is None:
            if catalog_path.startswith("s3://"):
                cat_dir = catalog_path.rsplit("/", 1)[0]
                effective_report = f"{cat_dir}/ingest_stats.json"
            else:
                effective_report = str(Path(catalog_path).parent / "ingest_stats.json")

        already_done = _load_processed_files(effective_report)
        if already_done:
            print(f"Checkpoint: {len(already_done)} data file(s) already processed — will skip")

        _source_bucket, _dest_store, data_keys = _list_manifest_files(inventory_path)
        pending = [k for k in data_keys if k not in already_done]
        skipped = len(data_keys) - len(pending)
        print(
            f"Manifest: {len(data_keys)} data files total, "
            f"{skipped} already processed, {len(pending)} to go"
        )

        def _finish_file(
            inv_key: str,
            file_metas: list[FileMetadata],
            fan_out_ctr: Counter,
            source_items: int,
            _table: object,
            fetch_failures: int = 0,
        ) -> list[FileMetadata]:
            """
            Register and checkpoint one completed inventory file.
            Compaction is intentionally omitted here — it is unsafe when multiple
            workers share the same warehouse prefix, because each worker's orphan
            sweep would delete part files written by other workers.
            Compaction must be done as a separate phase via final_compact.py, where
            exactly one task owns each (cell, year) prefix at a time.
            Returns the final FileMetadata list for this file.
            """
            total_rows = sum(fm.row_count for fm in file_metas)
            final_metas: list[FileMetadata] = file_metas

            final_paths = [_join_path(warehouse_root, fm.s3_key) for fm in final_metas]
            if final_paths:
                print(f"  Register: {len(final_paths)} files")
                _table.add_files(final_paths)

            upload_catalog(catalog_path)

            _write_ingest_stats(
                catalog_path=catalog_path,
                inventory_path=inventory_path,
                since=since,
                source_items=source_items,
                total_rows=total_rows,
                final_metas=final_metas,
                fan_out_counter=fan_out_ctr,
                h3_resolution=h3_resolution,
                report_location=report_location,
                inventory_file=inv_key,
                fetch_failures=fetch_failures,
            )
            print(
                f"  Done: {source_items:,} items → {total_rows:,} rows "
                f"across {len(final_paths)} files  [{inv_key.rsplit('/', 1)[-1]}]"
            )
            if fetch_failures:
                print(
                    f"  WARNING: {fetch_failures:,} items failed to fetch "
                    f"({100 * fetch_failures / source_items:.1f}% of chunk)"
                )
            return final_metas

        # Try to use dask.distributed as_completed for parallel execution
        try:
            from dask.distributed import as_completed as _as_completed
            from dask.distributed import get_client as _get_client

            _client = _get_client()
            _use_distributed = True
        except Exception:
            _use_distributed = False

        if _use_distributed:
            # Submit all pending files at once — workers pull tasks as they
            # become free, keeping all N workers busy throughout.
            print(f"Submitting {len(pending)} ingest tasks to distributed cluster …")
            future_to_key = {
                _client.submit(
                    _ingest_file_impl,
                    inv_key,
                    _dest_store,
                    partitioner,
                    warehouse_store,
                    chunk_size,
                    max_workers_per_task,
                    since,
                ): inv_key
                for inv_key in pending
            }

            completed = 0
            for future in _as_completed(future_to_key):
                inv_key = future_to_key[future]
                completed += 1
                print(f"\n── Completed {completed}/{len(pending)}: {inv_key.rsplit('/', 1)[-1]}")
                file_metas, fan_out_ctr, source_items, fetch_failures = future.result()

                def _finish(
                    _key: str = inv_key,
                    _metas: list = file_metas,
                    _ctr: Counter = fan_out_ctr,
                    _si: int = source_items,
                    _ff: int = fetch_failures,
                ) -> None:
                    download_catalog(catalog_path)
                    _cat = open_catalog(db_path=catalog_path, warehouse_path=warehouse_root)
                    _tbl = get_or_create_table(_cat, grid_config=grid_config)
                    _finish_file(_key, _metas, _ctr, _si, _tbl, _ff)
                    print(f"Snapshots in catalog: {len(_tbl.history())}")

                if use_lock:
                    with S3Lock(owner="backfill"):
                        _finish()
                else:
                    _finish()

        else:
            # ── Fallback: sequential (synchronous scheduler / tests) ───────
            for file_idx, inv_key in enumerate(pending):
                print(f"\n── File {file_idx + 1}/{len(pending)}: {inv_key}")
                item_pairs = list(
                    _iter_inventory_file_from_store(_dest_store, inv_key, since=since)
                )

                def _mini_run(
                    pairs: list[tuple[str, str]] = item_pairs,
                    key: str | None = inv_key,
                ) -> None:
                    _run_one(pairs, inv_file_key=key, total_items_hint=len(pairs))

                if use_lock:
                    with S3Lock(owner="backfill"):
                        _mini_run()
                else:
                    _mini_run()

    else:
        # ── Monolithic path: original behaviour ───────────────────────────
        def _monolithic_run() -> None:
            all_pairs: list[tuple[str, str]] = list(_iter_inventory(inventory_path, since=since))
            _run_one(all_pairs, inv_file_key=None, total_items_hint=len(all_pairs))

        if use_lock:
            with S3Lock(owner="backfill"):
                _monolithic_run()
        else:
            _monolithic_run()


# ---------------------------------------------------------------------------
# Scheduler context helpers
# ---------------------------------------------------------------------------


def _synchronous_context() -> object:
    """Return a context manager that activates the Dask synchronous scheduler."""
    return dask.config.set(scheduler="synchronous")


def _local_cluster_context(n_workers: int, threads_per_worker: int = 1) -> object:
    """
    Return a context manager that starts a ``LocalCluster`` and connects a
    ``Client`` to it.

    The cluster and client are shut down when the context exits.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx() -> Generator[object, None, None]:
        from dask.distributed import Client, LocalCluster

        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
        )
        client = Client(cluster)
        try:
            print(
                f"Dask dashboard: {client.dashboard_link}\n"
                f"Workers: {n_workers}  threads/worker: {threads_per_worker}"
            )
            yield client
        finally:
            client.close()
            cluster.close()

    return _ctx()


def _coiled_context(
    n_workers: int, worker_vm_types: list[str] | None = None, **extra_kwargs: object
) -> object:
    """
    Return a context manager that starts a Coiled cloud cluster.

    Requires ``coiled`` to be installed (``pip install coiled``).
    Coiled credentials must be configured (``coiled login`` or
    ``COILED_API_TOKEN`` environment variable).

    Parameters
    ----------
    n_workers:
        Number of Coiled cloud workers.
    worker_vm_types:
        Cloud VM instance types for workers (e.g. ``["m7i.xlarge"]``).
        Passed to ``coiled.Cluster(worker_vm_types=...)``.
    **extra_kwargs:
        Any additional keyword arguments forwarded verbatim to
        ``coiled.Cluster()``.  For example ``software="my-env"``,
        ``region="us-west-2"``, ``secret_env=["MY_SECRET"]``.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx() -> Generator[object, None, None]:
        try:
            import coiled
        except ImportError:
            raise ImportError("coiled is not installed.  Run: pip install coiled") from None

        kwargs: dict = {"n_workers": n_workers, **extra_kwargs}
        if worker_vm_types is not None:
            kwargs["worker_vm_types"] = worker_vm_types

        from dask.distributed import Client

        cluster = coiled.Cluster(**kwargs)
        client = Client(cluster)
        try:
            print(f"Coiled dashboard: {client.dashboard_link}\nWorkers requested: {n_workers}")
            yield client
        finally:
            client.close()
            cluster.close()

    return _ctx()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_coiled_extra(unknown_args: list[str]) -> dict:
    """
    Parse leftover ``--coiled-<name> <value>`` arguments into a kwargs dict.

    Rules
    -----
    * Only args that start with ``--coiled-`` are accepted; others are ignored.
    * The ``--coiled-`` prefix is stripped and hyphens are converted to
      underscores to form the kwarg name.
    * If the same key appears more than once the values are collected into a
      list (e.g. ``--coiled-secret-env VAR1 --coiled-secret-env VAR2``
      → ``{"secret_env": ["VAR1", "VAR2"]}``).

    Parameters
    ----------
    unknown_args:
        The leftover args list returned by ``parser.parse_known_args()``.

    Returns
    -------
    dict
        Kwargs to be forwarded to ``coiled.Cluster(**kwargs)``.
    """
    result: dict = {}
    i = 0
    while i < len(unknown_args):
        token = unknown_args[i]
        if token.startswith("--coiled-"):
            key = token[len("--coiled-") :].replace("-", "_")
            value = unknown_args[i + 1] if i + 1 < len(unknown_args) else None
            if value is not None and not value.startswith("--"):
                if key in result:
                    existing = result[key]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        result[key] = [existing, value]
                else:
                    result[key] = value
                i += 2
            else:
                # Boolean flag (no value)
                result[key] = True
                i += 1
        else:
            i += 1
    return result


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m earthcatalog.pipelines.backfill",
        description=(
            "EarthCatalog Dask distributed backfill pipeline.\n\n"
            "Ingests STAC items from an S3 Inventory file into a spatially-\n"
            "partitioned Apache Iceberg table using a three-level Dask pipeline."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
scheduler choices
-----------------
  synchronous   Single-process, sequential.  Readable tracebacks.
                Best for debugging with a small --limit.

  local         dask.distributed.LocalCluster on this machine.
                Use --workers to set the worker count (default: 4).

  coiled        Coiled cloud cluster.  Requires 'pip install coiled'
                and a configured API token (coiled login).
                Use --coiled-n-workers, --coiled-vm-type, and any
                additional --coiled-<kwarg> flags to configure the cluster.

examples
--------
  # Debug 50 items in a single process
  python -m earthcatalog.pipelines.backfill \\
      --inventory /tmp/inventory.csv \\
      --catalog   /tmp/catalog.db \\
      --warehouse /tmp/warehouse \\
      --scheduler synchronous --limit 50

  # Full local run with 4 workers
  python -m earthcatalog.pipelines.backfill \\
      --inventory /tmp/inventory.csv \\
      --catalog   /tmp/catalog.db \\
      --warehouse /tmp/warehouse \\
      --scheduler local --workers 4 --chunk-size 500

  # Coiled cloud run — explicit flags + pass-through kwargs
  python -m earthcatalog.pipelines.backfill \\
      --inventory  s3://my-bucket/inventory.parquet \\
      --catalog    /tmp/catalog.db \\
      --warehouse  /tmp/warehouse \\
      --scheduler  coiled \\
      --coiled-n-workers   20 \\
      --coiled-vm-type     m6i.xlarge \\
      --coiled-software    my-software-env \\
      --coiled-region      us-west-2 \\
      --coiled-secret-env  MY_SECRET_1 \\
      --coiled-secret-env  MY_SECRET_2
""",
    )

    # Required
    parser.add_argument(
        "--inventory",
        required=True,
        metavar="PATH",
        help="Local path or s3:// URI to S3 Inventory file (CSV, CSV.gz, or Parquet).",
    )

    # Catalog / warehouse
    parser.add_argument(
        "--catalog",
        default="/tmp/earthcatalog.db",
        metavar="PATH",
        help="Local path for the SQLite Iceberg catalog (default: /tmp/earthcatalog.db).",
    )
    parser.add_argument(
        "--warehouse",
        default="/tmp/earthcatalog_warehouse",
        metavar="PATH",
        help="Local directory for GeoParquet warehouse files (default: /tmp/earthcatalog_warehouse).",
    )

    # Ingest parameters
    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=1,
        metavar="N",
        help="H3 grid resolution for spatial partitioning (default: 1 = 842 global cells).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        metavar="N",
        help="Number of STAC items per Level-0 Dask task (default: 500).",
    )
    parser.add_argument(
        "--fetch-workers",
        type=int,
        default=16,
        metavar="N",
        help="Thread-pool size inside each ingest_chunk worker (default: 16).",
    )
    parser.add_argument(
        "--fetch-concurrency",
        type=int,
        default=256,
        metavar="N",
        help="Max concurrent async item fetches per worker (default: 256).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N items (useful for smoke-tests).",
    )
    parser.add_argument(
        "--since",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only process inventory items modified on or after this date (UTC). "
            "Format: YYYY-MM-DD or ISO-8601.  Example: --since 2026-04-21"
        ),
    )

    # Compaction
    parser.add_argument(
        "--no-compact",
        action="store_true",
        default=False,
        help="Skip Level-1 compaction; leave one part file per (cell, year) per chunk.",
    )
    parser.add_argument(
        "--compact-threshold",
        type=int,
        default=2,
        metavar="N",
        help="Compact buckets that have >= N part files (default: 2).",
    )

    # Lock
    parser.add_argument(
        "--no-lock",
        action="store_true",
        default=False,
        help="Disable the S3 distributed lock (for local/test runs).",
    )

    # Spot resilience
    parser.add_argument(
        "--no-per-file-mini-runs",
        action="store_true",
        default=False,
        help=(
            "Disable per-file mini-run mode for manifest inventories. "
            "Reverts to the original monolithic behaviour: one lock acquisition, "
            "one table.add_files() call at the very end.  Use this only if you "
            "are certain the run will not be interrupted (e.g. a short smoke test "
            "on a non-spot machine)."
        ),
    )

    # Report
    parser.add_argument(
        "--report-location",
        default=None,
        metavar="PATH_OR_URI",
        help=(
            "Local path or s3:// URI for ingest_stats.json. "
            "Defaults to <catalog_dir>/ingest_stats.json for local catalogs "
            "or s3://<cat-bucket>/<cat-prefix-dir>/ingest_stats.json for S3 catalogs."
        ),
    )

    # Scheduler
    parser.add_argument(
        "--scheduler",
        choices=["synchronous", "local", "coiled"],
        default="local",
        help="Dask scheduler to use (default: local).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of workers for --scheduler local (default: 4).",
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=1,
        metavar="N",
        help="Threads per worker for --scheduler local (default: 1).",
    )
    parser.add_argument(
        "--files-per-batch",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Number of manifest data files to read before submitting tasks to Dask. "
            "Increase to saturate the worker pool when inventory files are small "
            "(rule of thumb: ceil(n_workers / chunks_per_file), e.g. 4 with 20 workers "
            "and ~5-chunk files). Default 1 preserves original one-file-at-a-time behaviour."
        ),
    )

    # Explicit coiled options (take precedence over pass-through kwargs)
    coiled_group = parser.add_argument_group(
        "coiled options (--scheduler coiled)",
        description=(
            "Explicit flags for common Coiled settings.  Any other --coiled-<name> <value> "
            "argument is forwarded directly to coiled.Cluster(name=value) as a pass-through "
            "kwarg.  Repeated flags (e.g. --coiled-secret-env) are collected into a list."
        ),
    )
    coiled_group.add_argument(
        "--coiled-n-workers",
        type=int,
        default=10,
        metavar="N",
        help="Number of Coiled cloud workers (default: 10).",
    )
    coiled_group.add_argument(
        "--coiled-worker-vm-types",
        nargs="+",
        default=None,
        metavar="INSTANCE_TYPE",
        help="Cloud VM instance types for workers (e.g. m7i.xlarge).",
    )

    args, unknown = parser.parse_known_args()

    since: datetime | None = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)

    # ------------------------------------------------------------------
    # Build coiled kwargs: pass-through extras merged with explicit flags
    # ------------------------------------------------------------------
    coiled_extra = _parse_coiled_extra(unknown)
    # Explicit --coiled-n-workers / --coiled-worker-vm-types take precedence
    coiled_extra.pop("n_workers", None)
    coiled_extra.pop("worker_vm_types", None)

    # ------------------------------------------------------------------
    # Build stores from --catalog and --warehouse URIs
    # ------------------------------------------------------------------
    import configparser
    import os

    from obstore.store import LocalStore, S3Store

    def _make_s3_store(bucket: str, prefix: str = "") -> S3Store:
        """Authenticated S3Store using env vars or ~/.aws/credentials."""
        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        token = os.environ.get("AWS_SESSION_TOKEN")
        region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
        if not (key_id and secret):
            cfg = configparser.ConfigParser()
            cfg.read(os.path.expanduser("~/.aws/credentials"))
            profile = os.environ.get("AWS_PROFILE", "default")
            if profile in cfg:
                key_id = cfg[profile].get("aws_access_key_id", key_id)
                secret = cfg[profile].get("aws_secret_access_key", secret)
                token = cfg[profile].get("aws_session_token", token) or token
        kwargs: dict = dict(bucket=bucket, region=region)
        if prefix:
            kwargs["prefix"] = prefix
        if key_id:
            kwargs["aws_access_key_id"] = key_id
        if secret:
            kwargs["aws_secret_access_key"] = secret
        if token:
            kwargs["aws_session_token"] = token
        return S3Store(**kwargs)

    catalog_s3 = args.catalog.startswith("s3://")
    warehouse_s3 = args.warehouse.startswith("s3://")

    if warehouse_s3:
        # s3://bucket/prefix/to/warehouse  →  bucket + prefix
        wh_no_scheme = args.warehouse.removeprefix("s3://")
        wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
        warehouse_store = _make_s3_store(wh_bucket, prefix=wh_prefix)
        warehouse_root = args.warehouse  # full s3:// URI; used by add_files
    else:
        warehouse_path = Path(args.warehouse)
        warehouse_path.mkdir(parents=True, exist_ok=True)
        warehouse_store = LocalStore(str(warehouse_path))
        warehouse_root = str(warehouse_path)

    if catalog_s3:
        # s3://bucket/prefix/catalog.db  →  store on bucket, key = prefix/catalog.db
        cat_no_scheme = args.catalog.removeprefix("s3://")
        cat_bucket, cat_key = cat_no_scheme.split("/", 1)
        from earthcatalog.core import store_config

        store_config.set_store(_make_s3_store(cat_bucket))
        store_config.set_catalog_key(cat_key)
        store_config.set_lock_key(str(Path(cat_key).parent / ".lock"))
        # download_catalog needs a local temp file
        import tempfile

        _catalog_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        catalog_local = _catalog_tmp.name
        _catalog_tmp.close()
    else:
        Path(args.catalog).parent.mkdir(parents=True, exist_ok=True)
        catalog_local = args.catalog

    # ------------------------------------------------------------------
    # Resolve report_location — explicit flag > auto-derived S3 sibling
    # ------------------------------------------------------------------
    if args.report_location:
        report_location: str | None = args.report_location
    elif catalog_s3:
        # Auto-derive: co-locate with catalog.db on S3
        cat_dir = args.catalog.rsplit("/", 1)[0]  # strip filename
        report_location = f"{cat_dir}/ingest_stats.json"
    else:
        report_location = None  # _write_ingest_stats uses local catalog sibling

    # ------------------------------------------------------------------
    # Pick scheduler context
    # ------------------------------------------------------------------
    if args.scheduler == "synchronous":
        ctx = _synchronous_context()
    elif args.scheduler == "local":
        ctx = _local_cluster_context(
            n_workers=args.workers,
            threads_per_worker=args.threads_per_worker,
        )
    else:  # coiled
        ctx = _coiled_context(
            n_workers=args.coiled_n_workers,
            worker_vm_types=args.coiled_worker_vm_types,
            **coiled_extra,
        )

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    with ctx:
        run_backfill(
            inventory_path=args.inventory,
            catalog_path=catalog_local,
            warehouse_store=warehouse_store,
            warehouse_root=warehouse_root,
            h3_resolution=args.h3_resolution,
            chunk_size=args.chunk_size,
            max_workers_per_task=args.fetch_workers,
            fetch_concurrency=args.fetch_concurrency,
            compact=not args.no_compact,
            compact_threshold=args.compact_threshold,
            limit=args.limit,
            use_lock=not args.no_lock,
            since=since,
            report_location=report_location,
            per_file_mini_runs=not args.no_per_file_mini_runs,
            files_per_batch=args.files_per_batch,
        )


if __name__ == "__main__":
    main()
