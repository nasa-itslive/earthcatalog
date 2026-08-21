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
   :func:`~earthcatalog.maintenance.compact._compact_group_impl`).
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
import configparser
import io
import os
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from obstore.store import LocalStore, S3Store

from earthcatalog.catalog import (
    _HIVE_RE,
    FULL_NAME,
    _open_sqlite,
    download_catalog,
    get_or_create,
    upload_catalog,
)
from earthcatalog.transform import FileMetadata

# Matches the hive-style path layout written by both run() and run_backfill():
# grid_partition=<cell>/year=<year>/part_NNNNNN_<uuid>.parquet
# or
# grid_partition=<cell>/year=<year>/compacted_<hex>.parquet


def _s3_store(bucket: str, prefix: str = "") -> S3Store:
    """Build an authenticated S3Store, optionally scoped to a key prefix."""
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
    kwargs: dict = {"bucket": bucket, "region": region}
    if prefix:
        kwargs["prefix"] = prefix
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


def _compact_group_impl(
    file_metas: list[FileMetadata],
    out_key: str,
    store: object,
) -> FileMetadata:
    """
    Merge all part files for one ``(cell, year)`` bucket,
    deduplicate rows by ``id``, sort, and write one consolidated GeoParquet
    file to *store*.

    If the bucket has only one file the file is returned as-is (no I/O).
    """
    cell = file_metas[0].grid_partition
    year = file_metas[0].year

    if len(file_metas) == 1:
        return file_metas[0]

    tables: list[pa.Table] = []
    for fm in file_metas:
        raw = bytes(obstore.get(store, fm.s3_key).bytes())
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        tables.append(tbl)

    merged = pa.concat_tables(tables, promote_options="default")

    dedup_sort_keys = [("id", "ascending")]
    if "created" in merged.schema.names:
        dedup_sort_keys.append(("created", "descending"))
    sort_indices = pc.sort_indices(
        merged,
        sort_keys=dedup_sort_keys,
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

    merged = merged.sort_by(
        [
            ("platform", "ascending"),
            ("datetime", "ascending"),
        ]
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(merged, tmp_path, compression="zstd")
        data = Path(tmp_path).read_bytes()
        obstore.put(store, out_key, data)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

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


def _scan_warehouse(store: object) -> dict[tuple[str, str], list[FileMetadata]]:
    """
    List every ``.parquet`` file in *store* and group by ``(cell, year)``.

    Returns
    -------
    Dict mapping ``(grid_partition, year_str)`` → list of
    :class:`~earthcatalog.transform.FileMetadata`.
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
    store: object | None = None,
    catalog_key: str | None = None,
) -> dict[str, int]:
    """
    Compact all over-threshold buckets in *warehouse_path* and rebuild the
    Iceberg catalog.

    Parameters
    ----------
    warehouse_path:
        Local path or ``s3://`` URI of the warehouse root.
    catalog_path:
        Local path to the SQLite catalog file (e.g. ``/tmp/earthcatalog.db``).
    threshold:
        Minimum number of part files in a bucket before it is compacted.
        Default: 2 (compact any bucket with more than one part file).
    use_lock:
        When ``True``, wrap the entire operation in an
        :class:`~earthcatalog.lock.S3Lock`.  Requires the lock store to
        be configured in :mod:`earthcatalog.store_config`.
    dry_run:
        When ``True``, report what *would* be compacted but make no changes.
    store:
        Bucket-level obstore store used to download/upload ``catalog_path``
        (for ``s3://`` warehouses).  Defaults to ``store_config``.
    catalog_key:
        Object key within *store* for the catalog file (for ``s3://``
        warehouses).  Defaults to ``store_config``.

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
            store=store,
            catalog_key=catalog_key,
        )

    if use_lock:
        from earthcatalog.lock import S3Lock

        with S3Lock(owner="compact"):
            return _run()
    else:
        return _run()


def _compact_warehouse_impl(
    warehouse_path: str,
    catalog_path: str,
    threshold: int,
    dry_run: bool,
    store: object | None = None,
    catalog_key: str | None = None,
) -> dict[str, int]:
    # ------------------------------------------------------------------
    # 1.  Open the warehouse store (local dir or s3:// prefix).
    # ------------------------------------------------------------------
    if warehouse_path.startswith("s3://"):
        bucket, _, prefix = warehouse_path.removeprefix("s3://").partition("/")
        wh_store = _s3_store(bucket, prefix=prefix)
        wh_path = None
    else:
        wh_path = Path(warehouse_path)
        if not wh_path.is_dir():
            raise FileNotFoundError(f"Warehouse directory not found: {warehouse_path}")
        wh_store = LocalStore(str(wh_path))

    # ------------------------------------------------------------------
    # 2.  Scan warehouse → group by (cell, year).
    # ------------------------------------------------------------------
    buckets = _scan_warehouse(wh_store)
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
            new_fm = _compact_group_impl(file_metas, out_key, wh_store)
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
    download_catalog(catalog_path, store=store, catalog_key=catalog_key)
    catalog = _open_sqlite(db_path=catalog_path, warehouse_path=warehouse_path)

    # Drop and recreate the table to clear all stale manifest entries.
    from pyiceberg.exceptions import NoSuchTableError

    try:
        catalog.drop_table(FULL_NAME)
        print("Dropped stale Iceberg table — will recreate.")
    except NoSuchTableError:
        pass  # first run or table was never created

    table = get_or_create(catalog)

    # Collect all surviving part files (after compaction).
    current_buckets = _scan_warehouse(wh_store)
    if wh_path is not None:
        all_paths = [str(wh_path / fm.s3_key) for fms in current_buckets.values() for fm in fms]
    else:
        all_paths = [
            f"{warehouse_path.rstrip('/')}/{fm.s3_key}"
            for fms in current_buckets.values()
            for fm in fms
        ]
    total_files_after = len(all_paths)

    if all_paths:
        table.add_files(all_paths)
        print(f"Catalog rebuilt: {total_files_after} files registered in one snapshot.")

    upload_catalog(catalog_path, store=store, catalog_key=catalog_key)

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
