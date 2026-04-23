"""
Single-node incremental ingest from an AWS S3 Inventory file.

Supported inventory formats
---------------------------
- CSV           (plain or .gz)  — AWS S3 Inventory default
- Parquet                       — AWS S3 Inventory optional output; preferred
                                  at scale (typed, compressed, no quoting
                                  ambiguity).  Read in row-group batches so
                                  memory is bounded regardless of file size.
- manifest.json                 — AWS S3 Inventory manifest; references
                                  multiple Parquet data files in a private
                                  destination bucket.  Pass the manifest URI
                                  as ``inventory_path``; credentials for the
                                  destination bucket are read from
                                  ``~/.aws/credentials`` (default profile) or
                                  from the ``AWS_ACCESS_KEY_ID`` /
                                  ``AWS_SECRET_ACCESS_KEY`` environment
                                  variables.

The inventory must have at minimum two columns named ``bucket`` and ``key``
(case-insensitive for CSV; exact for Parquet).

Delta ingestion
---------------
Pass ``since`` (a timezone-aware UTC ``datetime``) to skip objects that
were last modified before that cutoff:

- **Parquet**: filters on the ``last_modified_date`` column; rows missing
  that column are passed through unchanged.  The column may be either a
  string (ISO-8601) or a native Parquet TIMESTAMP — both are handled.
- **CSV**: parses the ``last_modified_date`` column when a header row is
  present and the column is present; otherwise all rows are passed through
  (graceful degradation).

A typical 2-day delta run::

    from datetime import datetime, timezone, timedelta
    since = datetime.now(tz=timezone.utc) - timedelta(days=2)
    run(inventory_path=..., since=since)

Memory notes
------------
All S3 I/O goes through ``obstore``.  For S3 inventory files the full object
is downloaded once via ``obstore.get().bytes()``:

- **Parquet**: ``pq.ParquetFile.iter_batches()`` then reads one row-group at a
  time — peak RAM is bounded by ``batch_size`` rows.
- **CSV.gz**: ``gzip.open(BytesIO(compressed_bytes))`` — only the compressed
  bytes are held; decompression is line-by-line.
- **CSV (plain)**: ``TextIOWrapper(BytesIO(raw_bytes))`` — one copy of the raw
  bytes; no extra full-string decode.  For very large plain-text inventories
  prefer CSV.gz or Parquet to halve the peak RAM.

True zero-copy streaming from the obstore async byte stream is a planned
improvement (``obstore.GetResult.stream()``); it would eliminate the full-file
download for all formats.

Usage:
    python -m earthcatalog.pipelines.incremental \\
        --inventory /tmp/test_inventory.csv \\
        --catalog   /tmp/earthcatalog.db \\
        --warehouse /tmp/earthcatalog_warehouse \\
        --since     2026-04-21 \\
        --limit     500

    # Using a real S3 Inventory manifest (requires AWS credentials):
    python -m earthcatalog.pipelines.incremental \\
        --inventory s3://my-log-bucket/inventory/.../manifest.json \\
        --catalog   /tmp/earthcatalog.db \\
        --warehouse /tmp/earthcatalog_warehouse \\
        --since     2026-04-21
"""

import argparse
import configparser
import csv
import gzip
import io
import json
import os
import concurrent.futures
import uuid
from datetime import datetime, timezone
from pathlib import Path

import obstore
import pyarrow.parquet as pq
from obstore.store import S3Store

from earthcatalog.core.catalog import (
    open_catalog, get_or_create_table,
    download_catalog, upload_catalog,
)
from earthcatalog.core.lock import S3Lock
from earthcatalog.core.transform import fan_out, write_geoparquet, group_by_partition
from earthcatalog.grids import build_partitioner
from earthcatalog.grids.h3_partitioner import H3Partitioner

# One store per bucket — reused across all fetches
_STORES: dict[str, S3Store] = {}


def _get_store(bucket: str) -> S3Store:
    if bucket not in _STORES:
        _STORES[bucket] = S3Store(
            bucket=bucket,
            region="us-west-2",
            skip_signature=True,
        )
    return _STORES[bucket]


def _get_authenticated_store(bucket: str) -> S3Store:
    """
    Return an authenticated S3Store for a private bucket.

    Credential resolution order (first match wins):
    1. ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars
    2. ``~/.aws/credentials`` default profile

    Explicitly passing credentials avoids the EC2 Instance Metadata Service
    (IMDS) probe that obstore performs on non-EC2 machines, which causes a
    13-second timeout.
    """
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret  = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token   = os.environ.get("AWS_SESSION_TOKEN")

    if not (key_id and secret):
        creds_file = os.path.expanduser("~/.aws/credentials")
        cfg = configparser.ConfigParser()
        cfg.read(creds_file)
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret  = cfg[profile].get("aws_secret_access_key", secret)
            token   = cfg[profile].get("aws_session_token", token) or token

    kwargs: dict = dict(bucket=bucket, region="us-west-2")
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token

    return S3Store(**kwargs)


# ---------------------------------------------------------------------------
# Inventory reading — streaming, never loads the whole file into RAM
# ---------------------------------------------------------------------------

def _fetch_inventory_bytes(inventory_path: str) -> bytes:
    """Download an inventory file from S3 via obstore (one round-trip)."""
    bucket, key = inventory_path.removeprefix("s3://").split("/", 1)
    return bytes(obstore.get(_get_store(bucket), key).bytes())


def _parse_last_modified(value: str) -> datetime | None:
    """
    Parse an AWS S3 Inventory ``last_modified_date`` string to a UTC datetime.

    AWS formats seen in the wild:
    - ``2023-01-15T12:34:56.000Z``   (Parquet and CSV)
    - ``2023-01-15T12:34:56Z``
    Returns ``None`` on any parse failure so the row is always passed through
    on error (fail-open, not fail-closed).
    """
    try:
        # Strip trailing Z and parse; fromisoformat needs +00:00 style
        s = value.strip().rstrip("Z")
        dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _coerce_last_modified(lm_raw) -> datetime | None:
    """
    Coerce a ``last_modified_date`` value from any Parquet type to UTC datetime.

    - ``datetime`` (PyArrow TIMESTAMP → Python datetime): attach UTC tz if naive.
    - ``str`` (string column or stringified value): pass through ``_parse_last_modified``.
    - ``None``: return ``None`` (fail-open).
    """
    if lm_raw is None:
        return None
    if isinstance(lm_raw, datetime):
        return lm_raw if lm_raw.tzinfo else lm_raw.replace(tzinfo=timezone.utc)
    return _parse_last_modified(str(lm_raw))


def _iter_inventory_csv(inventory_path: str, since: datetime | None = None):
    """
    Yield ``(bucket, key)`` pairs from a CSV or CSV.gz inventory.

    Reads line-by-line: only one row lives in memory at a time.

    Parameters
    ----------
    inventory_path:
        Local path or ``s3://`` URI.
    since:
        When set (timezone-aware UTC), only rows whose ``last_modified_date``
        column is **≥ since** are yielded.  If the column is absent (no header
        or column not present), all rows are yielded (graceful degradation).

    For S3 paths the object is downloaded once via obstore:

    - CSV.gz: ``gzip.open(BytesIO(compressed_bytes))`` — only the compressed
      bytes are held; decompression is streaming.
    - CSV plain: ``TextIOWrapper(BytesIO(raw_bytes))`` — one copy of the raw
      bytes without an extra full-string decode.
    """
    is_gz = inventory_path.endswith(".gz")

    if inventory_path.startswith("s3://"):
        raw_bytes = _fetch_inventory_bytes(inventory_path)
        if is_gz:
            fh = gzip.open(io.BytesIO(raw_bytes), "rt", newline="")
        else:
            fh = io.TextIOWrapper(io.BytesIO(raw_bytes), encoding="utf-8",
                                  newline="")
    elif is_gz:
        fh = gzip.open(inventory_path, "rt", newline="")
    else:
        fh = open(inventory_path, newline="", encoding="utf-8")

    with fh:
        reader = csv.reader(fh)
        header_seen = False
        lm_col: int | None = None  # index of last_modified_date column

        for row in reader:
            if not row:
                continue
            if not header_seen:
                header_seen = True
                if row[0].strip('"').lower() == "bucket":
                    # Parse header to find last_modified_date column index.
                    lower_cols = [c.strip('"').lower() for c in row]
                    if "last_modified_date" in lower_cols:
                        lm_col = lower_cols.index("last_modified_date")
                    continue
                # No header — first row is data; fall through to yield.

            if since is not None and lm_col is not None and lm_col < len(row):
                lm = _parse_last_modified(row[lm_col].strip('"'))
                if lm is not None and lm < since:
                    continue

            yield row[0].strip('"'), row[1].strip('"')


def _iter_inventory_parquet(
    inventory_path: "str | io.BytesIO",
    batch_size: int = 65_536,
    since: datetime | None = None,
):
    """
    Yield ``(bucket, key)`` pairs from a Parquet inventory.

    Reads one row-group batch at a time so peak RAM is bounded by
    ``batch_size`` rows regardless of file size.  For S3 paths the file
    is downloaded once into a ``BytesIO`` buffer; for local paths it is
    opened directly by PyArrow (no extra copy).

    ``inventory_path`` may also be a ``BytesIO`` object (used internally by
    :func:`_iter_inventory_manifest` to pass pre-downloaded Parquet bytes).

    AWS S3 Inventory Parquet schema uses columns named ``bucket`` and ``key``.
    The optional ``last_modified_date`` column may be a string (ISO-8601) or a
    native Parquet TIMESTAMP (int64 MILLIS) — both are handled via
    :func:`_coerce_last_modified`.  If the column is absent all rows are
    yielded (graceful degradation).

    Parameters
    ----------
    since:
        When set (timezone-aware UTC), only rows whose ``last_modified_date``
        is **≥ since** are yielded.
    """
    if isinstance(inventory_path, io.BytesIO):
        source = inventory_path
    elif inventory_path.startswith("s3://"):
        source = io.BytesIO(_fetch_inventory_bytes(inventory_path))
    else:
        source = inventory_path  # pyarrow accepts a path string directly

    pf = pq.ParquetFile(source)
    schema_names = set(pf.schema_arrow.names)
    has_lm = "last_modified_date" in schema_names
    read_cols = ["bucket", "key"] + (["last_modified_date"] if has_lm else [])

    for batch in pf.iter_batches(batch_size=batch_size, columns=read_cols):
        buckets = batch.column("bucket").to_pylist()
        keys    = batch.column("key").to_pylist()
        if since is not None and has_lm:
            lm_values = batch.column("last_modified_date").to_pylist()
            for bucket, key, lm_raw in zip(buckets, keys, lm_values):
                if lm_raw is None:
                    yield bucket, key
                    continue
                lm = _coerce_last_modified(lm_raw)
                if lm is None or lm >= since:
                    yield bucket, key
        else:
            yield from zip(buckets, keys)


def _parse_manifest(manifest_s3_uri: str) -> tuple[str, object, list[str]]:
    """
    Download and parse an AWS S3 Inventory ``manifest.json``.

    Returns
    -------
    (source_bucket, dest_store, data_keys)
        ``source_bucket``  — the S3 bucket that was inventoried.
        ``dest_store``     — authenticated ``S3Store`` for the destination bucket.
        ``data_keys``      — list of object keys for the Parquet data files.

    The manifest lives in the **private** destination bucket.  Credentials are
    read from env vars or ``~/.aws/credentials`` via
    :func:`_get_authenticated_store`.
    """
    manifest_path = manifest_s3_uri.removeprefix("s3://")
    manifest_bucket, manifest_key = manifest_path.split("/", 1)

    dest_store_manifest = _get_authenticated_store(manifest_bucket)
    raw = bytes(obstore.get(dest_store_manifest, manifest_key).bytes())
    manifest = json.loads(raw)

    # "arn:aws:s3:::bucket-name" → "bucket-name"
    dest_arn = manifest.get("destinationBucket", "")
    dest_bucket = dest_arn.split(":::")[1] if ":::" in dest_arn else manifest_bucket

    data_keys = [f["key"] for f in manifest.get("files", [])]
    source_bucket = manifest.get("sourceBucket", "")
    dest_store = _get_authenticated_store(dest_bucket)
    print(f"Manifest: {len(data_keys)} data file(s) in {dest_bucket}")
    return source_bucket, dest_store, data_keys


def _list_manifest_files(
    manifest_s3_uri: str,
) -> tuple[str, object, list[str]]:
    """
    Parse an S3 Inventory ``manifest.json`` and return the file list without
    streaming any row data.

    This is the split-friendly entry point for the spot-resilient backfill
    pipeline.  Each data file can be processed as an independent mini-run,
    enabling per-file checkpointing.

    Parameters
    ----------
    manifest_s3_uri:
        ``s3://`` URI to the ``manifest.json``, e.g.
        ``s3://my-log-bucket/inventory/.../manifest.json``.

    Returns
    -------
    (source_bucket, dest_store, data_keys)
        ``source_bucket``  — the S3 bucket inventoried (e.g. ``its-live-data``).
        ``dest_store``     — authenticated ``S3Store`` for the destination bucket.
        ``data_keys``      — list of object keys for Parquet data files.
    """
    return _parse_manifest(manifest_s3_uri)


def _iter_inventory_file_from_store(
    store,
    data_key: str,
    batch_size: int = 65_536,
    since: datetime | None = None,
):
    """
    Yield ``(bucket, key)`` pairs from a single Parquet inventory data file
    already located in *store* at *data_key*.

    This is used by the spot-resilient per-file mini-run in
    :func:`~earthcatalog.pipelines.backfill.run_backfill` to stream one
    inventory data file at a time.

    Parameters
    ----------
    store:
        An ``obstore``-compatible authenticated store for the destination bucket.
    data_key:
        Object key of the Parquet data file within *store*.
    batch_size:
        Row-group batch size forwarded to :func:`_iter_inventory_parquet`.
    since:
        When set, only rows with ``last_modified_date >= since`` are yielded.
    """
    raw_bytes = bytes(obstore.get(store, data_key).bytes())
    yield from _iter_inventory_parquet(
        io.BytesIO(raw_bytes), batch_size=batch_size, since=since
    )


def _iter_inventory_manifest(
    manifest_s3_uri: str,
    batch_size: int = 65_536,
    since: datetime | None = None,
):
    """
    Yield ``(bucket, key)`` pairs from all Parquet data files listed in an
    AWS S3 Inventory ``manifest.json``.

    The manifest lives in the **private** destination bucket (different from
    the source bucket whose objects are inventoried).  An authenticated
    ``S3Store`` is built via :func:`_get_authenticated_store` using credentials
    from env vars or ``~/.aws/credentials``.

    Parameters
    ----------
    manifest_s3_uri:
        ``s3://`` URI to the ``manifest.json`` file, e.g.
        ``s3://my-log-bucket/inventory/.../manifest.json``.
    batch_size:
        Row-group batch size forwarded to :func:`_iter_inventory_parquet`.
    since:
        When set, only rows with ``last_modified_date >= since`` are yielded.

    Manifest format (AWS S3 Inventory)::

        {
          "sourceBucket": "its-live-data",
          "destinationBucket": "arn:aws:s3:::my-log-bucket",
          "fileFormat": "Parquet",
          "files": [
            {"key": "inventory/.../data/<uuid>.parquet", "MD5checksum": "..."},
            ...
          ]
        }

    The ``destinationBucket`` field is an ARN of the form
    ``arn:aws:s3:::bucket-name``; the bucket name is extracted by splitting on
    ``:::``.
    """
    _source_bucket, dest_store, data_keys = _parse_manifest(manifest_s3_uri)
    for data_key in data_keys:
        yield from _iter_inventory_file_from_store(
            dest_store, data_key, batch_size=batch_size, since=since
        )


def _iter_inventory(
    inventory_path: str,
    parquet_batch_size: int = 65_536,
    since: datetime | None = None,
):
    """
    Yield ``(bucket, key)`` pairs from an S3 Inventory file or manifest.

    Dispatches on file extension / suffix:
      - ``manifest.json``          → :func:`_iter_inventory_manifest`
      - ``.parquet``               → :func:`_iter_inventory_parquet`
      - ``.csv`` / ``.csv.gz``     → :func:`_iter_inventory_csv`

    Parameters
    ----------
    since:
        When set, only rows with ``last_modified_date >= since`` are yielded.
        See individual readers for per-format details and graceful-degradation
        rules.
    """
    if inventory_path.endswith("manifest.json"):
        yield from _iter_inventory_manifest(
            inventory_path,
            batch_size=parquet_batch_size,
            since=since,
        )
    elif inventory_path.endswith(".parquet"):
        yield from _iter_inventory_parquet(
            inventory_path,
            batch_size=parquet_batch_size,
            since=since,
        )
    else:
        yield from _iter_inventory_csv(inventory_path, since=since)


# ---------------------------------------------------------------------------
# STAC item fetching
# ---------------------------------------------------------------------------

def _fetch_item(bucket: str, key: str) -> dict | None:
    try:
        raw = obstore.get(_get_store(bucket), key).bytes()
        return json.loads(bytes(raw))
    except Exception as exc:
        print(f"WARN: failed to fetch s3://{bucket}/{key}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Main ingest loop
# ---------------------------------------------------------------------------

def run(
    inventory_path: str,
    catalog_path: str,
    warehouse_path: str,
    chunk_size: int = 500,
    max_workers: int = 16,
    limit: int | None = None,
    h3_resolution: int = 1,
    partitioner=None,
    use_lock: bool = True,
    batch_add_files: bool = False,
    since: datetime | None = None,
):
    """
    Read inventory → fetch STAC JSON from S3 in parallel →
    fan_out() → write_geoparquet() → add_files() to PyIceberg table.

    Each ``(grid_partition, year)`` group is written as a separate GeoParquet
    file in hive-style layout.  Files are registered in the Iceberg catalog via
    ``table.add_files()``.

    Parameters
    ----------
    since:
        When set (timezone-aware UTC), only inventory rows with
        ``last_modified_date >= since`` are processed.  Pass
        ``datetime.now(tz=timezone.utc) - timedelta(days=2)`` for a
        2-day delta run.  ``None`` processes the full inventory.
    batch_add_files:
        When ``False`` (default), ``table.add_files()`` is called after every
        chunk — one Iceberg snapshot per chunk.  Safe for incremental daily
        runs: a mid-run crash leaves the catalog in a consistent partial state.

        When ``True``, all GeoParquet paths are collected and registered in a
        **single** ``table.add_files()`` call at the very end — exactly one
        Iceberg snapshot regardless of how many chunks were processed.  Use
        this for initial backfills to prevent snapshot explosion.
        If the process crashes mid-run, no files are registered; re-run from
        scratch.

    The full run is wrapped in an S3Lock (set ``use_lock=False`` for tests).
    ``download_catalog`` / ``upload_catalog`` are called inside the lock so
    the SQLite catalog.db is safely synchronised with the configured store.
    """
    if partitioner is None:
        partitioner = H3Partitioner(resolution=h3_resolution)

    warehouse = Path(warehouse_path)
    warehouse.mkdir(parents=True, exist_ok=True)

    def _ingest() -> None:
        download_catalog(catalog_path)

        catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_path)
        table   = get_or_create_table(catalog)
        print(f"Catalog  : {catalog_path}")
        print(f"Table    : {table.name()}")

        chunk: list[tuple[str, str]] = []
        total_items = 0
        total_rows  = 0
        part_index  = 0
        # Populated only when batch_add_files=True; flushed once at the end.
        pending_paths: list[str] = []

        def flush(chunk: list[tuple[str, str]]) -> None:
            nonlocal total_rows, part_index

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                items = list(filter(None, pool.map(lambda bc: _fetch_item(*bc), chunk)))

            if not items:
                return

            fan_out_items = fan_out(items, partitioner)
            if not fan_out_items:
                return

            # Group by (grid_partition, year) — each group produces exactly one
            # Parquet file with a single partition value for both Iceberg transforms.
            groups  = group_by_partition(fan_out_items)
            paths   = []
            n_rows  = 0

            for (cell, year), group_items in groups.items():
                year_str = str(year) if year is not None else "unknown"
                # Hive-style layout: grid_partition=<cell>/year=<year>/part_N.parquet
                out_dir = warehouse / f"grid_partition={cell}" / f"year={year_str}"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = str(
                    out_dir / f"part_{part_index:06d}_{uuid.uuid4().hex[:8]}.parquet"
                )
                n = write_geoparquet(group_items, out_path)
                paths.append(out_path)
                n_rows += n

            if paths:
                if batch_add_files:
                    pending_paths.extend(paths)
                else:
                    table.add_files(paths)

            total_rows += n_rows
            part_index += 1
            print(
                f"  chunk {part_index}: {len(items)} items → "
                f"{n_rows} rows in {len(paths)} partition files"
            )

        print(f"Reading inventory: {inventory_path}")
        for bucket, key in _iter_inventory(inventory_path, since=since):
            if not key.endswith(".stac.json"):
                continue

            chunk.append((bucket, key))
            total_items += 1

            if len(chunk) >= chunk_size:
                flush(chunk)
                chunk.clear()

            if limit and total_items >= limit:
                break

        if chunk:
            flush(chunk)

        if batch_add_files and pending_paths:
            print(f"  registering {len(pending_paths)} files in a single snapshot …")
            table.add_files(pending_paths)

        upload_catalog(catalog_path)
        print(f"\nDone. {total_items} items → {total_rows} rows")
        print(f"Snapshots in catalog: {len(table.history())}")

    if use_lock:
        with S3Lock(owner="incremental"):
            _ingest()
    else:
        _ingest()


# ---------------------------------------------------------------------------
# Config-driven entry point
# ---------------------------------------------------------------------------

def run_from_config(inventory_path: str, config, limit: int | None = None):
    """
    Drive the incremental pipeline from an ``AppConfig`` instance.

    Parameters
    ----------
    inventory_path:
        Local path or ``s3://`` URI to the S3 Inventory CSV, CSV.gz, or
        Parquet file.
    config:
        An :class:`earthcatalog.config.AppConfig` instance.
    limit:
        Optional cap on the number of STAC items processed (for testing).
    """
    partitioner = build_partitioner(config.grid)
    run(
        inventory_path=inventory_path,
        catalog_path=config.catalog.db_path,
        warehouse_path=config.catalog.warehouse,
        chunk_size=config.ingest.chunk_size,
        max_workers=config.ingest.max_workers,
        batch_add_files=config.ingest.batch_add_files,
        limit=limit,
        partitioner=partitioner,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="EarthCatalog single-node ingest")
    parser.add_argument("--inventory",  required=True)
    parser.add_argument("--catalog",    default="/tmp/earthcatalog.db")
    parser.add_argument("--warehouse",  default="/tmp/earthcatalog_warehouse")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--workers",    type=int, default=16)
    parser.add_argument("--limit",      type=int, default=None)
    parser.add_argument("--h3-resolution", type=int, default=1)
    parser.add_argument(
        "--since", default=None, metavar="YYYY-MM-DD",
        help=(
            "Only process inventory items modified on or after this date "
            "(UTC).  Format: YYYY-MM-DD or ISO-8601.  "
            "Example: --since 2026-04-21"
        ),
    )
    parser.add_argument(
        "--batch-add-files",
        action="store_true",
        default=False,
        help=(
            "Collect all GeoParquet paths and register them in a single "
            "Iceberg snapshot at the end of the run.  Recommended for initial "
            "backfills; avoids snapshot explosion."
        ),
    )
    args = parser.parse_args()

    since: datetime | None = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)

    run(
        inventory_path=args.inventory,
        catalog_path=args.catalog,
        warehouse_path=args.warehouse,
        chunk_size=args.chunk_size,
        max_workers=args.workers,
        limit=args.limit,
        h3_resolution=args.h3_resolution,
        batch_add_files=args.batch_add_files,
        since=since,
    )


if __name__ == "__main__":
    main()
