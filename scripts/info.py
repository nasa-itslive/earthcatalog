"""
Print catalog summary: grid metadata, file counts, row counts, year/cell distribution.

Reads Iceberg manifest files only — no Parquet data is scanned.
Supports local catalogs and auto-download from S3.

Usage
-----
    python scripts/info.py --catalog /tmp/earthcatalog.db

    python scripts/info.py --catalog-s3 s3://its-live-data/.../earthcatalog.db
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description="Print earthcatalog summary")
parser.add_argument("--catalog", help="Local SQLite catalog path")
parser.add_argument("--catalog-s3", help="S3 URI to auto-download catalog from")
parser.add_argument(
    "--warehouse",
    default="s3://its-live-data/test-space/stac/catalog/warehouse",
    help="Warehouse root path",
)
parser.add_argument(
    "--hash-index",
    default=None,
    help="Path to warehouse_id_hashes.parquet (local or s3://)",
)
args = parser.parse_args()

# ---------------------------------------------------------------------------
# Resolve catalog path
# ---------------------------------------------------------------------------

if args.catalog:
    catalog_path = args.catalog
elif args.catalog_s3:
    import obstore
    from obstore.store import S3Store

    no_scheme = args.catalog_s3.removeprefix("s3://")
    bucket, key = no_scheme.split("/", 1)
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    store_kwargs: dict = dict(bucket=bucket, region=region, skip_signature=True)
    cred_id = os.environ.get("AWS_ACCESS_KEY_ID")
    cred_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if cred_id and cred_secret:
        store_kwargs["aws_access_key_id"] = cred_id
        store_kwargs["aws_secret_access_key"] = cred_secret
        del store_kwargs["skip_signature"]
    store = S3Store(**store_kwargs)
    catalog_path = "/tmp/earthcatalog_info.db"
    data = bytes(obstore.get(store, key).bytes())
    Path(catalog_path).write_bytes(data)
    print(f"Downloaded catalog from s3://{bucket}/{key}")
else:
    print("ERROR: specify --catalog or --catalog-s3")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Open catalog
# ---------------------------------------------------------------------------

from earthcatalog.core.catalog import FULL_NAME, PROP_HASH_INDEX_PATH, open  # noqa: E402
from earthcatalog.core.catalog_info import catalog_info  # noqa: E402

# Force anonymous S3 access — public buckets don't need credentials,
# and existing creds may not have permission on every bucket.
os.environ.pop("AWS_ACCESS_KEY_ID", None)
os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
os.environ.pop("AWS_SESSION_TOKEN", None)

cat = open(db_path=catalog_path, warehouse_path=args.warehouse)

try:
    table = cat.load_table(FULL_NAME)
except Exception:
    print("ERROR: could not load table from catalog")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Grid info
# ---------------------------------------------------------------------------

info = catalog_info(table)
print(f"\n{'=' * 60}")
print("  Catalog Info")
print(f"{'=' * 60}")
print(f"  Grid type     : {info.grid_type}")
print(f"  Resolution    : {info.grid_resolution}")
print(f"  Warehouse     : {args.warehouse}")

hash_index_path = args.hash_index or table.properties.get(PROP_HASH_INDEX_PATH)
unique_items = None
if hash_index_path:
    print(f"  Hash index    : {hash_index_path}")
    try:
        import obstore
        import pyarrow.parquet as pq
        from obstore.store import S3Store

        if hash_index_path.startswith("s3://"):
            region = (
                os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
            )
            no_scheme = hash_index_path.removeprefix("s3://")
            h_bucket, h_key = no_scheme.split("/", 1)
            hstore = S3Store(bucket=h_bucket, region=region, skip_signature=True)
            cred_id = os.environ.get("AWS_ACCESS_KEY_ID")
            cred_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
            if cred_id and cred_secret:
                hstore = S3Store(
                    bucket=h_bucket,
                    region=region,
                    aws_access_key_id=cred_id,
                    aws_secret_access_key=cred_secret,
                )
            # Read only the footer (~64KB), not the full file
            meta = hstore.head(h_key)
            size = meta["size"]
            tail = bytes(obstore.get_range(hstore, h_key, start=size - 65536, length=65536))
            pf = pq.ParquetFile(io.BytesIO(tail))
            unique_items = pf.metadata.num_rows
        else:
            pf = pq.ParquetFile(hash_index_path)
            unique_items = pf.metadata.num_rows
        print(f"  Unique items  : {unique_items:,} (from hash index)")
    except Exception as e:
        print(f"  Unique items  : (could not read hash index: {e})")
else:
    print("  Hash index    : not registered")

# ---------------------------------------------------------------------------
# Iceberg metadata
# ---------------------------------------------------------------------------

snap = table.current_snapshot()
print(f"  Snapshot id   : {snap.snapshot_id if snap else 'none'}")
if snap:
    manifests = list(table.inspect.manifests())
    print(f"  Manifest files: {len(manifests)}")
    print(f"  Snapshot time : {snap.timestamp_ms}")

# ---------------------------------------------------------------------------
# Stats (manifest-only, no parquet reads)
# ---------------------------------------------------------------------------

stats = info.stats(table)
total_rows = sum(s["row_count"] for s in stats)
total_files = sum(s["file_count"] for s in stats)
total_bytes = sum(s["total_bytes"] for s in stats)
cells = {s["grid_partition"] for s in stats}
unique_years = sorted({s["year"] for s in stats})

print(f"\n{'=' * 60}")
print("  Summary")
print(f"{'=' * 60}")
print(f"  Total rows    : {total_rows:,} (fan-out: 1 item × N cells)")
if unique_items is not None:
    fan_out_ratio = total_rows / unique_items if unique_items else 0
    print(f"  Unique items  : {unique_items:,} (fan-out ratio: {fan_out_ratio:.1f}x)")
print(f"  Total files   : {total_files:,}")
print(f"  Total size    : {total_bytes / 1e9:.2f} GB")
print(f"  Unique cells  : {len(cells):,}")
print(f"  Years         : {unique_years[0]}–{unique_years[-1]} ({len(unique_years)} years)")

# ---------------------------------------------------------------------------
# Year distribution
# ---------------------------------------------------------------------------

from collections import Counter  # noqa: E402

year_rows = Counter()
year_files = Counter()
for s in stats:
    year_rows[s["year"]] += s["row_count"]
    year_files[s["year"]] += s["file_count"]

print(f"\n{'=' * 60}")
print("  Year distribution")
print(f"{'=' * 60}")
print(f"  {'Year':<6} {'Rows':>12} {'Files':>8} {'%':>6}")
print(f"  {'-' * 34}")
for y in sorted(year_rows):
    pct = year_rows[y] / total_rows * 100 if total_rows else 0
    bar = "█" * int(pct / 2)
    print(f"  {y:<6} {year_rows[y]:>12,} {year_files[y]:>8} {pct:>5.1f}% {bar}")

# ---------------------------------------------------------------------------
# Top cells
# ---------------------------------------------------------------------------

top = sorted(stats, key=lambda s: s["row_count"], reverse=True)[:15]
print(f"\n{'=' * 60}")
print("  Top 15 cells by row count")
print(f"{'=' * 60}")
print(f"  {'Cell':<25} {'Year':>6} {'Rows':>10} {'Files':>6} {'Size':>8}")
print(f"  {'-' * 57}")
for s in top:
    mb = s["total_bytes"] / 1e6
    print(
        f"  {s['grid_partition']:<25} {s['year']:>6} {s['row_count']:>10,} {s['file_count']:>6} {mb:>7.1f}MB"
    )

# ---------------------------------------------------------------------------
# Compaction candidates
# ---------------------------------------------------------------------------

multi = [s for s in stats if s["file_count"] > 1]
if multi:
    multi_rows = sum(s["row_count"] for s in multi)
    print(f"\n{'=' * 60}")
    print("  Compaction candidates")
    print(f"{'=' * 60}")
    print(f"  Cells with >1 file: {len(multi)}")
    print(f"  Rows affected     : {multi_rows:,} ({multi_rows / total_rows * 100:.1f}%)")
    print("  Run consolidate.py to merge them.")
print()
