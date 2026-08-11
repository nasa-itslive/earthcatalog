"""
Run garbage collection against the EarthCatalog warehouse.

Uses a Bloom-filter pass over the current S3 Inventory to identify STAC items
whose source objects are no longer present, rewrites the affected GeoParquet
partition files, and rebuilds the Iceberg catalog.

Usage
-----
    # Dry run — detect orphans, make no changes
    python scripts/run_gc.py \
        --catalog    /tmp/earthcatalog.db \
        --warehouse  s3://its-live-data/test-space/stac/catalog/warehouse \
        --inventory  s3://pds-buckets-its-live-logbucket-70tr3aw5f2op/inventory/.../manifest.json \
        --dry-run

    # Live run
    python scripts/run_gc.py \
        --catalog   /tmp/earthcatalog.db \
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
        --inventory s3://pds-buckets-its-live-logbucket.../manifest.json
"""

from __future__ import annotations

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Garbage-collect orphaned STAC items from the EarthCatalog warehouse."
)
parser.add_argument(
    "--catalog",
    required=True,
    help="Local path to SQLite Iceberg catalog (earthcatalog.db).",
)
parser.add_argument(
    "--warehouse",
    default="s3://its-live-data/test-space/stac/catalog/warehouse",
    help="Warehouse root path (s3:// URI).",
)
parser.add_argument(
    "--inventory",
    required=True,
    help="S3 URI to S3 Inventory manifest.json for the current snapshot.",
)
parser.add_argument(
    "--catalog-key",
    default="test-space/stac/catalog/earthcatalog.db",
    help="Object key within the bucket for uploading the rebuilt catalog.",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Detect and report orphans but make no changes to S3 or the catalog.",
)
args = parser.parse_args()

# ---------------------------------------------------------------------------
# Open EarthCatalog
# ---------------------------------------------------------------------------

from obstore.store import S3Store  # noqa: E402

from earthcatalog.catalog import EarthCatalog, _open_sqlite  # noqa: E402

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"

# Derive bucket from warehouse URI
warehouse = args.warehouse.rstrip("/")
if warehouse.startswith("s3://"):
    rest = warehouse[5:]
    bucket = rest.split("/", 1)[0]
else:
    print("ERROR: --warehouse must be an s3:// URI")
    sys.exit(1)

store = S3Store(bucket=bucket, region=region)

# Open the SQLite catalog directly (catalog was already downloaded by CI)
cat_obj = _open_sqlite(db_path=args.catalog, warehouse_path=warehouse)

ec = EarthCatalog.__new__(EarthCatalog)
ec._catalog = cat_obj
ec._table = cat_obj.load_table("earthcatalog.stac_items")
ec._store = store
ec._catalog_key = args.catalog_key
# Populate _info via the public helper so grid queries work
from earthcatalog.catalog import _catalog_info  # noqa: E402

ec._info = _catalog_info(ec._table)

# ---------------------------------------------------------------------------
# Run GC
# ---------------------------------------------------------------------------

print(f"{'=' * 60}")
print("  EarthCatalog — Garbage Collection")
print(f"{'=' * 60}")
print(f"  Warehouse : {warehouse}")
print(f"  Inventory : {args.inventory}")
print(f"  Dry run   : {args.dry_run}")
print()

result = ec.garbage_collect(args.inventory, dry_run=args.dry_run)

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

print()
print(f"{'=' * 60}")
print("  GC Summary")
print(f"{'=' * 60}")
for k, v in result.items():
    print(f"  {k:<25} {v:>12,}" if isinstance(v, int) else f"  {k:<25} {v}")
print()

if args.dry_run:
    print("Dry run complete — no changes written.")
else:
    confirmed = result.get("confirmed", 0)
    rewritten = result.get("files_rewritten", 0)
    removed = result.get("rows_removed", 0)
    if confirmed == 0:
        print("No orphans found — warehouse is clean.")
    else:
        print(
            f"GC complete: {removed:,} rows removed across "
            f"{rewritten:,} rewritten files."
        )
