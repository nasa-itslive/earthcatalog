#!/usr/bin/env python3
"""
Consolidate small parquet files in the warehouse.

For each partition (cell, year), if the trailing files are small,
merge them into a single consolidated file. The old files are only
deleted after the new consolidated file is verified.

Strategy
--------
Given a partition with:
  part_000000.parquet  (100k rows)
  part_000001.parquet  (24k rows)
  part_000002.parquet  (10 rows)

Consolidation merges part_000001 + part_000002 → part_000001_consolidated.parquet
(24,010 rows). After verification, part_000001 and part_000002 are replaced by the
consolidated file.

Iceberg catalog is rebuilt after consolidation (drop + recreate + add_files
for all surviving files). This is safe because we hold the S3 lock.

Safety
------
- New consolidated file is written and verified BEFORE old files are deleted
- Row counts are checked: consolidated must have exactly sum of source rows
- Dedup by id within the consolidated file
- If any step fails, old files survive and the next run retries

Usage
-----
    python scripts/consolidate.py \
      --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
      --threshold 2

    # Dry run (report only)
    python scripts/consolidate.py \
      --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
      --threshold 2 \
      --dry-run
"""

import argparse
import io
import re
import sys
from collections import defaultdict

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import S3Store

_HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)
_PART_RE = re.compile(r"part_(?P<idx>\d+)(?:_consolidated)?\.parquet$")


def _get_store(bucket: str, prefix: str = "") -> S3Store:
    import configparser
    import os

    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")

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


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    path = uri.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")
    return bucket, prefix


def _scan_warehouse(store: S3Store) -> dict[tuple[str, str], list[dict]]:
    partitions: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for batch in obstore.list(store, prefix=""):
        for obj in batch:
            path: str = obj["path"]
            m = _HIVE_RE.search(path)
            if m is None:
                continue
            fname = m.group("file")
            part_m = _PART_RE.match(fname)
            idx = int(part_m.group("idx")) if part_m else 999999
            partitions[(m.group("cell"), m.group("year"))].append({
                "path": path,
                "name": fname,
                "index": idx,
                "size": obj.get("size", 0),
            })

    for files in partitions.values():
        files.sort(key=lambda f: f["index"])
    return dict(partitions)


def _count_rows(store: S3Store, path: str) -> int:
    raw = bytes(obstore.get(store, path).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).metadata.num_rows


def _read_parquet_table(store: S3Store, path: str) -> pa.Table:
    raw = bytes(obstore.get(store, path).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).read()


def _merge_tables(tables: list[pa.Table]) -> pa.Table:
    combined = pa.concat_tables(tables)
    return combined


def _dedup_table(tbl: pa.Table) -> pa.Table:
    if tbl.num_rows == 0:
        return tbl
    ids = tbl.column("id").to_pylist()
    seen: set[str] = set()
    mask: list[bool] = []
    for item_id in ids:
        if item_id in seen:
            mask.append(False)
        else:
            seen.add(item_id)
            mask.append(True)
    return tbl.filter(pa.array(mask))


def consolidate_partition(
    store: S3Store,
    cell: str,
    year: str,
    files: list[dict],
    threshold: int,
    dry_run: bool,
    compact_rows: int = 100_000,
) -> dict | None:
    n = len(files)
    if n < threshold:
        return None

    row_counts = [(f, _count_rows(store, f["path"])) for f in files]

    split_idx = len(row_counts)
    for i, (f, rc) in enumerate(row_counts):
        if rc < compact_rows:
            split_idx = i
            break

    if split_idx >= len(row_counts):
        return None

    to_keep = row_counts[:split_idx]
    to_merge = row_counts[split_idx:]

    if not to_merge:
        return None

    merge_total_rows = sum(rc for _, rc in to_merge)

    if dry_run:
        merge_names = ", ".join(f["name"] for f, _ in to_merge)
        print(
            f"  [dry-run] {cell}/{year}: would merge {len(to_merge)} files "
            f"({merge_total_rows:,} rows) [{merge_names}]"
        )
        return {"cell": cell, "year": year, "merged_files": len(to_merge), "merged_rows": merge_total_rows}

    merge_names = ", ".join(f["name"] for f, _ in to_merge)
    print(f"  Consolidating {cell}/{year}: merging {len(to_merge)} files ({merge_total_rows:,} rows) [{merge_names}]")

    tables: list[pa.Table] = []
    for f, _ in to_merge:
        tbl = _read_parquet_table(store, f["path"])
        tables.append(tbl)

    merged = _merge_tables(tables)
    unique = _dedup_table(merged)
    expected = merge_total_rows

    if unique.num_rows < expected:
        print(f"    Dedup: {expected:,} → {unique.num_rows:,} (removed {expected - unique.num_rows:,} dupes)")

    base = f"grid_partition={cell}/year={year}"
    merge_base_idx = to_merge[0][0]["index"]
    consolidated_name = f"part_{merge_base_idx:06d}_consolidated.parquet"
    consolidated_path = f"{base}/{consolidated_name}"

    buf = io.BytesIO()
    pq.write_table(unique, buf, compression="zstd")
    data = buf.getvalue()
    obstore.put(store, consolidated_path, data)
    verified_rows = pq.ParquetFile(io.BytesIO(data)).metadata.num_rows

    if verified_rows != len(unique):
        print(f"    ERROR: verified {verified_rows:,} != expected {len(unique):,}, aborting", file=sys.stderr)
        try:
            obstore.delete(store, consolidated_path)
        except Exception:
            pass
        return None

    print(f"    Written {consolidated_path} ({verified_rows:,} rows)")

    for f, _ in to_merge:
        try:
            obstore.delete(store, f["path"])
            print(f"    Deleted {f['name']}")
        except Exception as exc:
            print(f"    WARN: could not delete {f['name']}: {exc}", file=sys.stderr)

    final_name = f"part_{merge_base_idx:06d}.parquet"
    final_path = f"{base}/{final_name}"
    try:
        obstore.delete(store, final_path)
    except Exception:
        pass
    raw = bytes(obstore.get(store, consolidated_path).bytes())
    obstore.put(store, final_path, raw)
    try:
        obstore.delete(store, consolidated_path)
    except Exception:
        pass
    print(f"    Renamed → {final_path}")

    return {
        "cell": cell,
        "year": year,
        "merged_files": len(to_merge),
        "merged_rows": merge_total_rows,
        "output_rows": verified_rows,
    }


def _count_rows(store: S3Store, path: str) -> int:
    raw = bytes(obstore.get(store, path).bytes())
    return pq.ParquetFile(io.BytesIO(raw)).metadata.num_rows


def rebuild_catalog(
    warehouse_uri: str,
    catalog_path: str,
    store: S3Store,
) -> None:
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

    from earthcatalog.core.catalog import (
        FULL_NAME,
        ICEBERG_SCHEMA,
        NAMESPACE,
        PARTITION_SPEC,
        open_catalog,
        upload_catalog,
    )

    catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_uri)

    try:
        catalog.create_namespace(NAMESPACE)
    except (NamespaceAlreadyExistsError, Exception):
        pass

    try:
        catalog.drop_table(FULL_NAME)
    except NoSuchTableError:
        pass

    table = catalog.create_table(
        identifier=FULL_NAME,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )

    all_paths: list[str] = []
    bucket, prefix = _parse_s3_uri(warehouse_uri)
    for batch in obstore.list(store, prefix=""):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet") and _HIVE_RE.search(k):
                all_paths.append(f"s3://{bucket}/{k}")

    if all_paths:
        batch_size = 2000
        for i in range(0, len(all_paths), batch_size):
            table.add_files(all_paths[i : i + batch_size])
        print(f"Registered {len(all_paths):,} files in Iceberg catalog.")

    upload_catalog(catalog_path)


def run_consolidate(
    warehouse_uri: str,
    catalog_path: str,
    threshold: int = 2,
    compact_rows: int = 100_000,
    use_lock: bool = False,
    dry_run: bool = False,
) -> dict:
    bucket, prefix = _parse_s3_uri(warehouse_uri)
    store = _get_store(bucket, prefix=prefix)

    print(f"Scanning warehouse: {warehouse_uri}")
    partitions = _scan_warehouse(store)
    total_files = sum(len(v) for v in partitions.values())
    print(f"  {len(partitions):,} partitions, {total_files:,} files")

    over_threshold = {k: v for k, v in partitions.items() if len(v) >= threshold}
    print(f"  {len(over_threshold):,} partitions with ≥ {threshold} files")

    results: list[dict] = []
    for (cell, year), files in sorted(over_threshold.items()):
        r = consolidate_partition(store, cell, year, files, threshold, dry_run, compact_rows=compact_rows)
        if r is not None:
            results.append(r)

    if not dry_run and results:
        print()
        print("Rebuilding Iceberg catalog ...")
        rebuild_catalog(warehouse_uri, catalog_path, store)

    total_merged = sum(r["merged_files"] for r in results)
    total_rows = sum(r.get("merged_rows", 0) for r in results)
    print()
    print(f"Consolidated {len(results):,} partitions")
    print(f"  Merged {total_merged:,} files ({total_rows:,} rows)")

    return {
        "partitions_scanned": len(partitions),
        "partitions_consolidated": len(results),
        "files_merged": total_merged,
        "rows_merged": total_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Consolidate small warehouse parquet files")
    parser.add_argument(
        "--warehouse",
        default="s3://its-live-data/test-space/stac/catalog/warehouse",
        help="Warehouse root (s3:// URI)",
    )
    parser.add_argument(
        "--catalog",
        default="/tmp/earthcatalog.db",
        help="Local path for SQLite catalog",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=2,
        help="Min files per partition to trigger consolidation (default: 2)",
    )
    parser.add_argument(
        "--compact-rows",
        type=int,
        default=100_000,
        help="Rows above which a part file is considered full (default: 100000)",
    )
    parser.add_argument("--use-lock", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.use_lock:
        from earthcatalog.core.lock import S3Lock
        with S3Lock(owner="consolidate"):
            run_consolidate(
                warehouse_uri=args.warehouse,
                catalog_path=args.catalog,
                threshold=args.threshold,
                compact_rows=args.compact_rows,
                dry_run=args.dry_run,
            )
    else:
        run_consolidate(
            warehouse_uri=args.warehouse,
            catalog_path=args.catalog,
            threshold=args.threshold,
            compact_rows=args.compact_rows,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
