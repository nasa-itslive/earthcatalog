"""
Dask distributed backfill pipeline for earthcatalog.

Architecture (three levels)
---------------------------

Level 0 — ``ingest_chunk`` (one Dask task per inventory chunk)
    Each worker fetches STAC JSON from S3, fans out through the H3 partitioner,
    groups by ``(grid_partition, year)``, and writes one GeoParquet file per
    group directly to the warehouse store via ``write_geoparquet_s3``.
    Returns a list of ``FileMetadata`` dicts (~200 B each) — no Parquet data
    crosses the network to the head node.

Level 1 — ``compact_group`` (one Dask task per (cell, year) bucket)
    Each worker downloads all part files for its bucket from the warehouse store,
    merges them into one Arrow table, sorts by ``(platform, datetime)``, writes
    one consolidated GeoParquet file back to the store, and deletes the originals.
    Returns a single ``FileMetadata``.
    If a bucket already has only one file, the task is a no-op passthrough.

Level 2 — catalog registration (head node only)
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
import tempfile
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
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
# Default S3 item fetcher (production)
# ---------------------------------------------------------------------------

_S3_STORES: dict[str, object] = {}


def _get_s3_store(bucket: str):
    from obstore.store import S3Store
    if bucket not in _S3_STORES:
        _S3_STORES[bucket] = S3Store(
            bucket=bucket,
            region="us-west-2",
            skip_signature=True,
        )
    return _S3_STORES[bucket]


def _s3_fetcher(bucket: str, key: str) -> dict | None:
    """Fetch one STAC JSON item from a public S3 bucket via obstore."""
    try:
        raw = obstore.get(_get_s3_store(bucket), key).bytes()
        return json.loads(bytes(raw))
    except Exception as exc:
        print(f"WARN: failed to fetch s3://{bucket}/{key}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Level 0 — ingest_chunk
# ---------------------------------------------------------------------------

def _ingest_chunk_impl(
    bucket_key_pairs: list[tuple[str, str]],
    partitioner,
    warehouse_store,
    chunk_id: int,
    item_fetcher=None,
    max_workers: int = 16,
) -> tuple[list[FileMetadata], Counter]:
    """
    Level 0 worker: fetch STAC items → fan_out → group_by_partition → write.

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
        Callable ``(bucket, key) -> dict | None``.  When ``None``, the default
        public S3 fetcher is used.  Pass a custom callable for tests.
    max_workers:
        Number of threads for parallel item fetching.

    Returns
    -------
    A tuple of:

    - ``list[FileMetadata]`` — one entry per ``(grid_partition, year)`` group
      written.  Empty groups produce no entry.  No Parquet data is returned;
      only tiny metadata records (~200 B each).
    - ``Counter[int]`` — fan-out distribution: maps number-of-cells (1, 2, 3,
      …) to count-of-source-items that landed in that many cells.  Used by the
      driver to compute overlap statistics without a post-hoc warehouse scan.
    """
    if item_fetcher is None:
        item_fetcher = _s3_fetcher

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        items = list(filter(
            None,
            pool.map(lambda bc: item_fetcher(*bc), bucket_key_pairs),
        ))

    if not items:
        return [], Counter()

    fan_out_items = fan_out(items, partitioner)
    if not fan_out_items:
        return [], Counter()

    # Compute fan-out distribution: how many distinct cells did each item land in?
    cells_per_id: dict[str, set[str]] = defaultdict(set)
    for item in fan_out_items:
        item_id = item["id"]
        cell    = item["properties"].get("grid_partition", "__none__")
        cells_per_id[item_id].add(cell)
    fan_out_counter: Counter[int] = Counter(
        len(cells) for cells in cells_per_id.values()
    )

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
            file_metas.append(FileMetadata(
                s3_key=key,
                grid_partition=cell,
                year=year,
                row_count=n_rows,
                file_size_bytes=n_bytes,
            ))

    return file_metas, fan_out_counter


#: Dask-delayed version of :func:`_ingest_chunk_impl`.
#: Call ``ingest_chunk(...).compute()`` or pass to ``dask.compute()``.
ingest_chunk = dask.delayed(_ingest_chunk_impl)


# ---------------------------------------------------------------------------
# Level 1 — compact_group
# ---------------------------------------------------------------------------

def _compact_group_impl(
    file_metas: list[FileMetadata],
    out_key: str,
    store,
) -> FileMetadata:
    """
    Level 1 worker: merge all part files for one ``(cell, year)`` bucket,
    deduplicate rows by ``id``, and remove orphan Parquet files from the
    bucket's prefix.

    If the bucket has only one file **and** there are no orphan files in the
    prefix, the file is returned as-is (no I/O).

    Steps for multi-file buckets (or buckets with orphans):

    1. **Orphan sweep** — list all ``.parquet`` files under
       ``grid_partition=<cell>/year=<year>/``; delete any that are neither in
       *file_metas* nor the *out_key*.
    2. **Download** all part files from *store*.
    3. **Concatenate** Arrow tables (schema metadata including ``geo`` and
       ``ARROW:extension:name: geoarrow.wkb`` is preserved from the first
       table).
    4. **Deduplicate** on ``id``: sort by ``(id ASC, updated DESC NULLS LAST)``
       and keep the first (most-recent) occurrence of each id.
    5. **Sort** by ``(platform, datetime)`` for Parquet locality.
    6. **Write** one consolidated GeoParquet file to *store* at *out_key*.
    7. **Delete** all input part files.

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
    year_str = str(year) if year is not None else "unknown"
    prefix = f"grid_partition={cell}/year={year_str}/"
    known_keys = {fm.s3_key for fm in file_metas} | {out_key}

    # ------------------------------------------------------------------
    # Orphan sweep — delete any .parquet files in the prefix that are not
    # among the known part files or the output key.
    # obstore.list() returns a list of batches; each batch is a list of dicts.
    # ------------------------------------------------------------------
    try:
        for batch in obstore.list(store, prefix=prefix):
            for obj in batch:
                key = obj["path"]
                if key.endswith(".parquet") and key not in known_keys:
                    try:
                        obstore.delete(store, key)
                        print(f"INFO: deleted orphan {key}")
                    except Exception as exc:
                        print(f"WARN: could not delete orphan {key}: {exc}")
    except Exception as exc:
        print(f"WARN: orphan sweep failed for {prefix}: {exc}")

    # ------------------------------------------------------------------
    # Single-file passthrough — only after orphan sweep.
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
        print(
            f"INFO: dedup removed {merged.num_rows - len(keep)} duplicate rows "
            f"in {out_key}"
        )
    merged = merged.take(pa.array(keep, type=pa.int64()))

    # Sort by (platform, datetime) for optimal Parquet predicate pushdown.
    merged = merged.sort_by([
        ("platform", "ascending"),
        ("datetime", "ascending"),
    ])

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
            from obstore.store import S3Store as _S3S
            import os as _os, configparser as _cp
            key_id = _os.environ.get("AWS_ACCESS_KEY_ID")
            secret  = _os.environ.get("AWS_SECRET_ACCESS_KEY")
            token   = _os.environ.get("AWS_SESSION_TOKEN")
            region  = (_os.environ.get("AWS_DEFAULT_REGION")
                       or _os.environ.get("AWS_REGION") or "us-west-2")
            if not (key_id and secret):
                cfg = _cp.ConfigParser()
                cfg.read(_os.path.expanduser("~/.aws/credentials"))
                profile = _os.environ.get("AWS_PROFILE", "default")
                if profile in cfg:
                    key_id = cfg[profile].get("aws_access_key_id", key_id)
                    secret  = cfg[profile].get("aws_secret_access_key", secret)
                    token   = cfg[profile].get("aws_session_token", token) or token
            sk: dict = dict(bucket=s3_bucket, region=region)
            if key_id: sk["aws_access_key_id"] = key_id
            if secret:  sk["aws_secret_access_key"] = secret
            if token:   sk["aws_session_token"] = token
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
    unique_cells        number of distinct H3 cells with data
    unique_years        number of distinct acquisition years with data
    files_written       number of Parquet files registered in Iceberg
    avg_fan_out         total_rows / source_items (rounded to 3 d.p.)
    overhead_pct        (total_rows - source_items) / source_items × 100
    fan_out_distribution  mapping of n_cells → item_count
    """
    record: dict = {
        "run_id":          datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        "inventory":       inventory_path,
        "inventory_file":  inventory_file,
        "since":           since.isoformat() if since else None,
        "h3_resolution":   h3_resolution,
        "source_items":    source_items,
        "total_rows":      total_rows,
        "unique_cells":    len({fm.grid_partition for fm in final_metas}),
        "unique_years":    len({fm.year for fm in final_metas}),
        "files_written":   len(final_metas),
        "avg_fan_out":     round(total_rows / source_items, 3) if source_items else 0,
        "overhead_pct":    round((total_rows - source_items) * 100 / source_items, 1)
                           if source_items else 0,
        "fan_out_distribution": {
            str(k): v for k, v in sorted(fan_out_counter.items())
        },
    }

    # ------------------------------------------------------------------
    # Determine effective target: explicit override > catalog sibling
    # ------------------------------------------------------------------
    effective: str = report_location or (Path(catalog_path).parent / "ingest_stats.json").as_posix()
    target_s3 = effective.startswith("s3://")

    # Always write to a local staging file first
    if target_s3:
        import tempfile
        _tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        local_stats_path = Path(_tmp.name)
        _tmp.close()
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
        # Upload to S3: derive bucket + key from the s3:// URI
        import obstore
        no_scheme = effective.removeprefix("s3://")
        s3_bucket, s3_key = no_scheme.split("/", 1)
        from obstore.store import S3Store
        import os, configparser
        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        secret  = os.environ.get("AWS_SECRET_ACCESS_KEY")
        token   = os.environ.get("AWS_SESSION_TOKEN")
        region  = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
        if not (key_id and secret):
            cfg = configparser.ConfigParser()
            cfg.read(os.path.expanduser("~/.aws/credentials"))
            profile = os.environ.get("AWS_PROFILE", "default")
            if profile in cfg:
                key_id = cfg[profile].get("aws_access_key_id", key_id)
                secret  = cfg[profile].get("aws_secret_access_key", secret)
                token   = cfg[profile].get("aws_session_token", token) or token
        sk: dict = dict(bucket=s3_bucket, region=region)
        if key_id: sk["aws_access_key_id"] = key_id
        if secret:  sk["aws_secret_access_key"] = secret
        if token:   sk["aws_session_token"] = token
        rpt_store = S3Store(**sk)
        obstore.put(rpt_store, s3_key, local_stats_path.read_bytes())
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
    warehouse_store,
    warehouse_root: str,
    item_fetcher=None,
    partitioner=None,
    h3_resolution: int = 1,
    chunk_size: int = 20_000,
    max_workers_per_task: int = 16,
    compact: bool = True,
    compact_threshold: int = 2,
    limit: int | None = None,
    use_lock: bool = True,
    since: datetime | None = None,
    report_location: str | None = None,
    per_file_mini_runs: bool = True,
) -> None:
    """
    Three-level Dask distributed backfill pipeline.

    Level 0 — ``ingest_chunk`` tasks (one per *chunk_size* items):
        Each Dask worker fetches STAC items, fans them out, and writes
        GeoParquet files to *warehouse_store*.  Returns ``FileMetadata`` only.

    Level 1 — ``compact_group`` tasks (one per ``(cell, year)`` bucket):
        Only dispatched when *compact* is ``True`` and a bucket has ≥
        *compact_threshold* part files.  Merges part files into one
        consolidated file.

    Level 2 — catalog registration (head node):
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
       c. Level 0: ingest the file's items.
       d. Level 1: compact the file's buckets.
       e. Level 2: ``table.add_files()`` + ``upload_catalog()``.
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
        Number of inventory items per Level 0 task.
    max_workers_per_task:
        Thread-pool size used inside each ``ingest_chunk`` worker for
        parallel item fetching.
    compact:
        When ``True`` (default), dispatch Level 1 compact tasks for any
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
    """
    if partitioner is None:
        partitioner = H3Partitioner(resolution=h3_resolution)

    def _join_path(root: str, key: str) -> str:
        if root.startswith("s3://"):
            return root.rstrip("/") + "/" + key
        return str(Path(root) / key)

    def _effective_report(catalog_p: str) -> str | None:
        return report_location  # None means _write_ingest_stats uses catalog sibling

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
        table = get_or_create_table(catalog)

        # Level 0 ─ dispatch ingest_chunk tasks
        level0_tasks = []
        chunk: list[tuple[str, str]] = []
        chunk_id = 0
        total_items = 0

        for bucket, key in item_pairs:
            if not key.endswith(".stac.json"):
                continue
            chunk.append((bucket, key))
            total_items += 1
            if len(chunk) >= chunk_size:
                level0_tasks.append(ingest_chunk(
                    list(chunk), partitioner, warehouse_store,
                    chunk_id, item_fetcher, max_workers_per_task,
                ))
                chunk.clear()
                chunk_id += 1
            if limit and total_items >= limit:
                break

        if chunk:
            level0_tasks.append(ingest_chunk(
                list(chunk), partitioner, warehouse_store,
                chunk_id, item_fetcher, max_workers_per_task,
            ))

        if not level0_tasks:
            print(f"  no .stac.json items found in {inv_file_key or 'chunk'}")
            # Still write stats so the caller knows this file was processed.
            _write_ingest_stats(
                catalog_path    = catalog_path,
                inventory_path  = inventory_path,
                since           = since,
                source_items    = 0,
                total_rows      = 0,
                final_metas     = [],
                fan_out_counter = Counter(),
                h3_resolution   = h3_resolution,
                report_location = report_location,
                inventory_file  = inv_file_key,
            )
            return

        print(f"Level 0: {len(level0_tasks)} ingest_chunk tasks ({total_items} items)")
        all_nested: tuple[tuple[list[FileMetadata], Counter], ...] = dask.compute(*level0_tasks)
        all_metas: list[FileMetadata] = [
            fm for file_metas, _ in all_nested for fm in file_metas
        ]
        fan_out_counter: Counter[int] = sum(
            (counter for _, counter in all_nested), Counter()
        )
        total_rows = sum(fm.row_count for fm in all_metas)
        print(f"Level 0 done: {len(all_metas)} files, {total_rows} rows")

        # Level 1 ─ compact_group tasks
        final_metas: list[FileMetadata] = []

        if compact:
            buckets: dict[tuple[str, int | None], list[FileMetadata]] = defaultdict(list)
            for fm in all_metas:
                buckets[(fm.grid_partition, fm.year)].append(fm)

            level1_tasks = []
            level1_pass_through: list[FileMetadata] = []

            for (cell, year), fms in buckets.items():
                if len(fms) >= compact_threshold:
                    year_str = str(year) if year is not None else "unknown"
                    out_key = (
                        f"grid_partition={cell}/year={year_str}/"
                        f"compacted_{uuid.uuid4().hex[:8]}.parquet"
                    )
                    level1_tasks.append(compact_group(fms, out_key, warehouse_store))
                else:
                    level1_pass_through.extend(fms)

            print(
                f"Level 1: {len(level1_tasks)} compact tasks, "
                f"{len(level1_pass_through)} files passed through"
            )

            if level1_tasks:
                compacted: tuple[FileMetadata, ...] = dask.compute(*level1_tasks)
                final_metas.extend(compacted)

            final_metas.extend(level1_pass_through)
        else:
            final_metas = all_metas

        # Level 2 ─ register all files in one Iceberg snapshot
        final_paths = [
            _join_path(warehouse_root, fm.s3_key)
            for fm in final_metas
        ]

        if final_paths:
            print(f"Level 2: registering {len(final_paths)} files in one snapshot")
            table.add_files(final_paths)

        upload_catalog(catalog_path)

        _write_ingest_stats(
            catalog_path    = catalog_path,
            inventory_path  = inventory_path,
            since           = since,
            source_items    = total_items,
            total_rows      = total_rows,
            final_metas     = final_metas,
            fan_out_counter = fan_out_counter,
            h3_resolution   = h3_resolution,
            report_location = report_location,
            inventory_file  = inv_file_key,
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
        # ── Spot-resilient path: one mini-run per manifest data file ──────
        effective_report = report_location
        if effective_report is None:
            # Auto-derive sibling of catalog (same logic as _write_ingest_stats)
            if catalog_path.startswith("s3://"):
                cat_dir = catalog_path.rsplit("/", 1)[0]
                effective_report = f"{cat_dir}/ingest_stats.json"
            else:
                effective_report = str(Path(catalog_path).parent / "ingest_stats.json")

        already_done = _load_processed_files(effective_report)
        if already_done:
            print(f"Checkpoint: {len(already_done)} data file(s) already processed — will skip")

        _source_bucket, dest_store, data_keys = _list_manifest_files(inventory_path)
        pending = [k for k in data_keys if k not in already_done]
        skipped = len(data_keys) - len(pending)
        print(
            f"Manifest: {len(data_keys)} data files total, "
            f"{skipped} already processed, {len(pending)} to go"
        )

        for file_idx, data_key in enumerate(pending):
            print(f"\n── File {file_idx + 1}/{len(pending)}: {data_key}")
            # Collect (bucket, key) pairs from this one inventory file
            item_pairs = [
                (bkt, k)
                for bkt, k in _iter_inventory_file_from_store(
                    dest_store, data_key, since=since
                )
            ]

            def _mini_run(pairs=item_pairs, key=data_key):
                _run_one(pairs, inv_file_key=key, total_items_hint=len(pairs))

            if use_lock:
                with S3Lock(owner="backfill"):
                    _mini_run()
            else:
                _mini_run()

    else:
        # ── Monolithic path: original behaviour ───────────────────────────
        def _monolithic_run() -> None:
            all_pairs: list[tuple[str, str]] = list(
                _iter_inventory(inventory_path, since=since)
            )
            _run_one(all_pairs, inv_file_key=None, total_items_hint=len(all_pairs))

        if use_lock:
            with S3Lock(owner="backfill"):
                _monolithic_run()
        else:
            _monolithic_run()


# ---------------------------------------------------------------------------
# Scheduler context helpers
# ---------------------------------------------------------------------------

def _synchronous_context():
    """Return a context manager that activates the Dask synchronous scheduler."""
    return dask.config.set(scheduler="synchronous")


def _local_cluster_context(n_workers: int, threads_per_worker: int = 1):
    """
    Return a context manager that starts a ``LocalCluster`` and connects a
    ``Client`` to it.

    The cluster and client are shut down when the context exits.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx():
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


def _coiled_context(n_workers: int, vm_type: str | None = None, **extra_kwargs):
    """
    Return a context manager that starts a Coiled cloud cluster.

    Requires ``coiled`` to be installed (``pip install coiled``).
    Coiled credentials must be configured (``coiled login`` or
    ``COILED_API_TOKEN`` environment variable).

    Parameters
    ----------
    n_workers:
        Number of Coiled cloud workers.
    vm_type:
        Cloud VM instance type (e.g. ``m6i.xlarge``).  Passed to
        ``coiled.Cluster(vm_type=...)``.
    **extra_kwargs:
        Any additional keyword arguments forwarded verbatim to
        ``coiled.Cluster()``.  For example ``software="my-env"``,
        ``region="us-west-2"``, ``secret_env=["MY_SECRET"]``.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        try:
            import coiled
        except ImportError:
            raise ImportError(
                "coiled is not installed.  Run: pip install coiled"
            ) from None

        kwargs: dict = {"n_workers": n_workers, **extra_kwargs}
        if vm_type is not None:
            kwargs["vm_type"] = vm_type

        from dask.distributed import Client
        cluster = coiled.Cluster(**kwargs)
        client = Client(cluster)
        try:
            print(
                f"Coiled dashboard: {client.dashboard_link}\n"
                f"Workers requested: {n_workers}"
            )
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
            key = token[len("--coiled-"):].replace("-", "_")
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
        "--inventory", required=True, metavar="PATH",
        help="Local path or s3:// URI to S3 Inventory file (CSV, CSV.gz, or Parquet).",
    )

    # Catalog / warehouse
    parser.add_argument("--catalog",   default="/tmp/earthcatalog.db", metavar="PATH",
                        help="Local path for the SQLite Iceberg catalog (default: /tmp/earthcatalog.db).")
    parser.add_argument("--warehouse", default="/tmp/earthcatalog_warehouse", metavar="PATH",
                        help="Local directory for GeoParquet warehouse files (default: /tmp/earthcatalog_warehouse).")

    # Ingest parameters
    parser.add_argument("--h3-resolution", type=int, default=1, metavar="N",
                        help="H3 grid resolution for spatial partitioning (default: 1 = 842 global cells).")
    parser.add_argument("--chunk-size", type=int, default=500, metavar="N",
                        help="Number of STAC items per Level-0 Dask task (default: 500).")
    parser.add_argument("--fetch-workers", type=int, default=16, metavar="N",
                        help="Thread-pool size inside each ingest_chunk worker (default: 16).")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Stop after N items (useful for smoke-tests).")
    parser.add_argument(
        "--since", default=None, metavar="YYYY-MM-DD",
        help=(
            "Only process inventory items modified on or after this date (UTC). "
            "Format: YYYY-MM-DD or ISO-8601.  Example: --since 2026-04-21"
        ),
    )

    # Compaction
    parser.add_argument("--no-compact", action="store_true", default=False,
                        help="Skip Level-1 compaction; leave one part file per (cell, year) per chunk.")
    parser.add_argument("--compact-threshold", type=int, default=2, metavar="N",
                        help="Compact buckets that have >= N part files (default: 2).")

    # Lock
    parser.add_argument("--no-lock", action="store_true", default=False,
                        help="Disable the S3 distributed lock (for local/test runs).")

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
        "--report-location", default=None, metavar="PATH_OR_URI",
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
    parser.add_argument("--workers", type=int, default=4, metavar="N",
                        help="Number of workers for --scheduler local (default: 4).")
    parser.add_argument("--threads-per-worker", type=int, default=1, metavar="N",
                        help="Threads per worker for --scheduler local (default: 1).")

    # Explicit coiled options (take precedence over pass-through kwargs)
    coiled_group = parser.add_argument_group(
        "coiled options (--scheduler coiled)",
        description=(
            "Explicit flags for common Coiled settings.  Any other --coiled-<name> <value> "
            "argument is forwarded directly to coiled.Cluster(name=value) as a pass-through "
            "kwarg.  Repeated flags (e.g. --coiled-secret-env) are collected into a list."
        ),
    )
    coiled_group.add_argument("--coiled-n-workers", type=int, default=10, metavar="N",
                              help="Number of Coiled cloud workers (default: 10).")
    coiled_group.add_argument("--coiled-vm-type", default=None, metavar="INSTANCE_TYPE",
                              help="Cloud VM instance type (e.g. m6i.xlarge).")

    args, unknown = parser.parse_known_args()

    since: datetime | None = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # Build coiled kwargs: pass-through extras merged with explicit flags
    # ------------------------------------------------------------------
    coiled_extra = _parse_coiled_extra(unknown)
    # Explicit --coiled-n-workers / --coiled-vm-type take precedence
    coiled_extra.pop("n_workers", None)
    coiled_extra.pop("vm_type", None)

    # ------------------------------------------------------------------
    # Build stores from --catalog and --warehouse URIs
    # ------------------------------------------------------------------
    import configparser, os
    from obstore.store import LocalStore, S3Store

    def _make_s3_store(bucket: str, prefix: str = "") -> S3Store:
        """Authenticated S3Store using env vars or ~/.aws/credentials."""
        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        secret  = os.environ.get("AWS_SECRET_ACCESS_KEY")
        token   = os.environ.get("AWS_SESSION_TOKEN")
        region  = (os.environ.get("AWS_DEFAULT_REGION")
                   or os.environ.get("AWS_REGION")
                   or "us-west-2")
        if not (key_id and secret):
            cfg = configparser.ConfigParser()
            cfg.read(os.path.expanduser("~/.aws/credentials"))
            profile = os.environ.get("AWS_PROFILE", "default")
            if profile in cfg:
                key_id = cfg[profile].get("aws_access_key_id", key_id)
                secret  = cfg[profile].get("aws_secret_access_key", secret)
                token   = cfg[profile].get("aws_session_token", token) or token
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

    catalog_s3   = args.catalog.startswith("s3://")
    warehouse_s3 = args.warehouse.startswith("s3://")

    if warehouse_s3:
        # s3://bucket/prefix/to/warehouse  →  bucket + prefix
        wh_no_scheme  = args.warehouse.removeprefix("s3://")
        wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
        warehouse_store = _make_s3_store(wh_bucket, prefix=wh_prefix)
        warehouse_root  = args.warehouse          # full s3:// URI; used by add_files
    else:
        warehouse_path = Path(args.warehouse)
        warehouse_path.mkdir(parents=True, exist_ok=True)
        warehouse_store = LocalStore(str(warehouse_path))
        warehouse_root  = str(warehouse_path)

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
            vm_type=args.coiled_vm_type,
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
            compact=not args.no_compact,
            compact_threshold=args.compact_threshold,
            limit=args.limit,
            use_lock=not args.no_lock,
            since=since,
            report_location=report_location,
            per_file_mini_runs=not args.no_per_file_mini_runs,
        )


if __name__ == "__main__":
    main()
