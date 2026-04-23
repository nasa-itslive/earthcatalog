"""
Standalone compaction for the earthcatalog warehouse.

Purpose
-------
The incremental pipeline (``earthcatalog.pipelines.incremental.run``) writes
**one new part file per (cell, year) bucket per run**.  After N incremental
runs a bucket can have N part files, each potentially containing duplicate
``id`` rows if the same STAC item appeared in more than one inventory delta.

This tool:

1. Scans the warehouse directory for all Parquet files, grouped by
   ``(grid_partition, year)`` bucket.
2. For every bucket with ≥ *threshold* part files: merges them into one
   consolidated, deduplicated, sorted file (reusing
   :func:`~earthcatalog.pipelines.backfill._compact_group_impl`).
3. Rebuilds the Iceberg catalog from all files currently in the warehouse
   (a "repair table" operation) so stale and new paths are resolved in one
   clean snapshot.

``since=`` / delta ingestion note
----------------------------------
For now the incremental pipeline scans the **full** S3 Inventory file and
filters rows client-side by ``last_modified_date >= since``.  This is
correct and memory-efficient (streaming by row-group batch) but the I/O
cost scales with the total inventory size, not the delta size.

When you set up an Athena cron job to emit a pre-filtered delta Parquet,
you simply pass that smaller file as ``--inventory`` and omit ``--since``;
no pipeline changes are needed.

Usage
-----
As a module::

    from earthcatalog.maintenance.compact import compact_warehouse
    compact_warehouse(
        warehouse_path="/tmp/earthcatalog_warehouse",
        catalog_path="/tmp/earthcatalog.db",
        threshold=2,
    )

As a CLI::

    python -m earthcatalog.maintenance.compact \\
        --warehouse /tmp/earthcatalog_warehouse \\
        --catalog   /tmp/earthcatalog.db \\
        [--threshold 2]

    # With S3 lock (recommended for production):
    python -m earthcatalog.maintenance.compact \\
        --warehouse s3://my-bucket/warehouse \\
        --catalog   /tmp/earthcatalog.db \\
        --use-lock
"""

from __future__ import annotations

import argparse
import re
import uuid
from collections import defaultdict
from pathlib import Path

import obstore
from obstore.store import LocalStore

from earthcatalog.core.catalog import (
    download_catalog,
    get_or_create_table,
    open_catalog,
    upload_catalog,
)
from earthcatalog.core.transform import FileMetadata
from earthcatalog.pipelines.backfill import _compact_group_impl

# Matches the hive-style path layout written by both run() and run_backfill():
# grid_partition=<cell>/year=<year>/part_NNNNNN_<uuid>.parquet
# or
# grid_partition=<cell>/year=<year>/compacted_<hex>.parquet
_HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)


def _scan_warehouse(store: object) -> dict[tuple[str, str], list[FileMetadata]]:
    """
    List every ``.parquet`` file in *store* and group by ``(cell, year)``.

    Returns
    -------
    Dict mapping ``(grid_partition, year_str)`` → list of
    :class:`~earthcatalog.core.transform.FileMetadata`.
    The ``row_count`` field is left at 0 (not needed for compaction decisions).
    """
    buckets: dict[tuple[str, str], list[FileMetadata]] = defaultdict(list)

    for batch in obstore.list(store):
        for obj in batch:
            path: str = obj["path"]
            m = _HIVE_RE.search(path)
            if m is None:
                continue
            cell = m.group("cell")
            year_str = m.group("year")
            year_int = int(year_str) if year_str.isdigit() else None
            buckets[(cell, year_str)].append(
                FileMetadata(
                    s3_key=path,
                    grid_partition=cell,
                    year=year_int,
                    row_count=0,
                    file_size_bytes=0,
                )
            )

    return dict(buckets)


def compact_warehouse(
    warehouse_path: str,
    catalog_path: str,
    threshold: int = 2,
    use_lock: bool = False,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Compact all over-threshold buckets in *warehouse_path* and rebuild the
    Iceberg catalog.

    Parameters
    ----------
    warehouse_path:
        Local path to the warehouse root (e.g. ``/tmp/earthcatalog_warehouse``).
        Must be a local directory — S3 warehouse support is planned.
    catalog_path:
        Local path to the SQLite catalog file (e.g. ``/tmp/earthcatalog.db``).
    threshold:
        Minimum number of part files in a bucket before it is compacted.
        Default: 2 (compact any bucket with more than one part file).
    use_lock:
        When ``True``, wrap the entire operation in an
        :class:`~earthcatalog.core.lock.S3Lock`.  Requires the lock store to
        be configured in :mod:`earthcatalog.core.store_config`.
    dry_run:
        When ``True``, report what *would* be compacted but make no changes.

    Returns
    -------
    A summary dict::

        {
            "buckets_scanned": int,
            "buckets_compacted": int,
            "files_before": int,
            "files_after": int,
        }
    """

    def _run() -> dict[str, int]:
        return _compact_warehouse_impl(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=threshold,
            dry_run=dry_run,
        )

    if use_lock:
        from earthcatalog.core.lock import S3Lock

        with S3Lock(owner="compact"):
            return _run()
    else:
        return _run()


def _compact_warehouse_impl(
    warehouse_path: str,
    catalog_path: str,
    threshold: int,
    dry_run: bool,
) -> dict[str, int]:
    # ------------------------------------------------------------------
    # 1.  Open the warehouse store (local only for now).
    # ------------------------------------------------------------------
    wh_path = Path(warehouse_path)
    if not wh_path.is_dir():
        raise FileNotFoundError(f"Warehouse directory not found: {warehouse_path}")

    store = LocalStore(str(wh_path))

    # ------------------------------------------------------------------
    # 2.  Scan warehouse → group by (cell, year).
    # ------------------------------------------------------------------
    buckets = _scan_warehouse(store)
    total_files_before = sum(len(v) for v in buckets.values())

    print(
        f"Warehouse : {warehouse_path}\n"
        f"Buckets   : {len(buckets)}\n"
        f"Part files: {total_files_before}"
    )

    # ------------------------------------------------------------------
    # 3.  Compact over-threshold buckets.
    # ------------------------------------------------------------------
    compacted = 0
    for (cell, year_str), file_metas in sorted(buckets.items()):
        n = len(file_metas)
        if n < threshold:
            continue

        out_key = f"grid_partition={cell}/year={year_str}/compacted_{uuid.uuid4().hex[:8]}.parquet"

        if dry_run:
            print(f"  [dry-run] would compact {n} files → {out_key}")
            compacted += 1
            continue

        print(f"  compacting {n} files in grid_partition={cell}/year={year_str} …")
        try:
            new_fm = _compact_group_impl(file_metas, out_key, store)
            # Replace the in-memory bucket entry with the single new file so
            # the catalog rebuild below sees the correct current state.
            buckets[(cell, year_str)] = [new_fm]
            compacted += 1
        except Exception as exc:
            print(f"  WARN: compaction failed for ({cell}, {year_str}): {exc}")

    if dry_run:
        return {
            "buckets_scanned": len(buckets),
            "buckets_compacted": compacted,
            "files_before": total_files_before,
            "files_after": total_files_before,  # unchanged in dry-run
        }

    # ------------------------------------------------------------------
    # 4.  Rebuild Iceberg catalog from all files currently in the warehouse.
    #
    #     Strategy: re-scan the store (physical ground truth), drop the
    #     existing table, recreate it, and register every surviving file in
    #     one snapshot.  This is equivalent to Hive's MSCK REPAIR TABLE —
    #     simple, correct, and takes < 1 s for typical warehouse sizes.
    # ------------------------------------------------------------------
    download_catalog(catalog_path)
    catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_path)

    # Drop and recreate the table to clear all stale manifest entries.
    from pyiceberg.exceptions import NoSuchTableError

    from earthcatalog.core.catalog import FULL_NAME

    try:
        catalog.drop_table(FULL_NAME)
        print("Dropped stale Iceberg table — will recreate.")
    except NoSuchTableError:
        pass  # first run or table was never created

    table = get_or_create_table(catalog)

    # Collect all surviving part files (after compaction).
    current_buckets = _scan_warehouse(store)
    all_paths = [str(wh_path / fm.s3_key) for fms in current_buckets.values() for fm in fms]
    total_files_after = len(all_paths)

    if all_paths:
        table.add_files(all_paths)
        print(f"Catalog rebuilt: {total_files_after} files registered in one snapshot.")

    upload_catalog(catalog_path)

    return {
        "buckets_scanned": len(buckets),
        "buckets_compacted": compacted,
        "files_before": total_files_before,
        "files_after": total_files_after,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compact earthcatalog warehouse: merge part files and rebuild catalog."
    )
    parser.add_argument(
        "--warehouse",
        required=True,
        help="Path to the warehouse root directory.",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Path to the SQLite catalog file.",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=2,
        help="Minimum part files in a bucket to trigger compaction (default: 2).",
    )
    parser.add_argument(
        "--use-lock",
        action="store_true",
        help="Acquire S3Lock before compacting (requires store_config to be set).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be compacted without making any changes.",
    )

    args = parser.parse_args()

    summary = compact_warehouse(
        warehouse_path=args.warehouse,
        catalog_path=args.catalog,
        threshold=args.threshold,
        use_lock=args.use_lock,
        dry_run=args.dry_run,
    )

    print(
        f"\nSummary\n"
        f"  Buckets scanned   : {summary['buckets_scanned']}\n"
        f"  Buckets compacted : {summary['buckets_compacted']}\n"
        f"  Part files before : {summary['files_before']}\n"
        f"  Part files after  : {summary['files_after']}"
    )


if __name__ == "__main__":
    main()
