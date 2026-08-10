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
import concurrent.futures
import uuid
from datetime import UTC, datetime
from pathlib import Path

import obstore  # noqa: F401  (test patches target this module)

from earthcatalog.catalog import (
    _open_sqlite,
    download_catalog,
    get_or_create,
    upload_catalog,
)
from earthcatalog.grids import build_partitioner
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.inventory import (
    _coerce_last_modified,  # noqa: F401  (test-compat re-export)
    _fetch_item,
    _get_authenticated_store,  # noqa: F401  (test-compat re-export)
    _iter_inventory,
    _iter_inventory_csv,  # noqa: F401  (test-compat re-export)
    _iter_inventory_manifest,  # noqa: F401  (test-compat re-export)
    _iter_inventory_parquet,  # noqa: F401  (test-compat re-export)
)
from earthcatalog.lock import S3Lock
from earthcatalog.transform import fan_out, group_by_partition, write_geoparquet

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
    partitioner: object = None,
    use_lock: bool = True,
    batch_add_files: bool = False,
    since: datetime | None = None,
    grid_config=None,
) -> None:
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

        catalog = _open_sqlite(db_path=catalog_path, warehouse_path=warehouse_path)
        table = get_or_create(catalog, grid_config=grid_config)
        print(f"Catalog  : {catalog_path}")
        print(f"Table    : {table.name()}")

        chunk: list[tuple[str, str]] = []
        total_items = 0
        total_rows = 0
        part_index = 0
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
            groups = group_by_partition(fan_out_items)
            paths = []
            n_rows = 0

            for (cell, year), group_items in groups.items():
                year_str = str(year) if year is not None else "unknown"
                # Hive-style layout: grid_partition=<cell>/year=<year>/part_N.parquet
                out_dir = warehouse / f"grid_partition={cell}" / f"year={year_str}"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = str(out_dir / f"part_{part_index:06d}_{uuid.uuid4().hex[:8]}.parquet")
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


def run_from_config(inventory_path: str, config: object, limit: int | None = None) -> None:
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
        grid_config=config.grid,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="EarthCatalog single-node ingest")
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--catalog", default="/tmp/earthcatalog.db")
    parser.add_argument("--warehouse", default="/tmp/earthcatalog_warehouse")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--h3-resolution", type=int, default=1)
    parser.add_argument(
        "--since",
        default=None,
        metavar="YYYY-MM-DD",
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
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)

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
