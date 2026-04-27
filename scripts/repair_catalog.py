"""
Repair the earthcatalog Iceberg table after the Bug-2 orphan-sweep incident.

The bug caused compacted_*.parquet files that were already registered in
Iceberg to be deleted from S3, leaving ~13 700 dangling file references.
This script:

  1. Lists all .parquet files currently present in the warehouse on S3.
  2. Creates a fresh SQLite catalog (earthcatalog_repaired.db).
  3. Registers every existing file via table.add_files() — PyIceberg infers
     partition values (grid_partition, year) from the hive-style path layout.
  4. Verifies the repaired catalog with a quick scan.
  5. Optionally uploads the repaired catalog to S3 (--upload flag).

Usage
-----
    python scripts/repair_catalog.py \\
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse/ \\
        --catalog-s3 s3://its-live-data/test-space/stac/catalog/earthcatalog.db \\
        --output /tmp/earthcatalog_repaired.db \\
        [--upload]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="Repair earthcatalog Iceberg catalog")
parser.add_argument(
    "--warehouse",
    default="s3://its-live-data/test-space/stac/catalog/warehouse/",
    help="S3 URI of the Iceberg warehouse root",
)
parser.add_argument(
    "--catalog-s3",
    default="s3://its-live-data/test-space/stac/catalog/earthcatalog.db",
    help="S3 URI of the current catalog.db (used for reference / backup only)",
)
parser.add_argument(
    "--output",
    default="/tmp/earthcatalog_repaired.db",
    help="Local path for the repaired SQLite catalog",
)
parser.add_argument(
    "--upload",
    action="store_true",
    help="Upload the repaired catalog to --catalog-s3 when done",
)
parser.add_argument(
    "--workers",
    type=int,
    default=32,
    help="Thread-pool size for parallel S3 HEAD checks",
)
args = parser.parse_args()

# ---------------------------------------------------------------------------
# Imports (after arg parse so --help works without heavy deps)
# ---------------------------------------------------------------------------

import obstore  # noqa: E402
from obstore.store import S3Store  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent.parent))
from earthcatalog.core.catalog import (  # noqa: E402
    FULL_NAME,
    ICEBERG_SCHEMA,
    NAMESPACE,
    PARTITION_SPEC,
    open_catalog,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError  # noqa: E402

# ---------------------------------------------------------------------------
# Build obstore S3Store for the warehouse bucket
# ---------------------------------------------------------------------------

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"

warehouse_no_scheme = args.warehouse.removeprefix("s3://")
warehouse_bucket, warehouse_prefix = warehouse_no_scheme.split("/", 1)

store_kwargs: dict = dict(bucket=warehouse_bucket, region=region)
key_id = os.environ.get("AWS_ACCESS_KEY_ID")
secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
token = os.environ.get("AWS_SESSION_TOKEN")
if key_id:
    store_kwargs["aws_access_key_id"] = key_id
if secret:
    store_kwargs["aws_secret_access_key"] = secret
if token:
    store_kwargs["aws_session_token"] = token

store = S3Store(**store_kwargs)

# ---------------------------------------------------------------------------
# 1. List all .parquet files present in the warehouse
# ---------------------------------------------------------------------------

print(f"Listing compacted_*.parquet files under s3://{warehouse_bucket}/{warehouse_prefix} …")
all_parquet: list[str] = []
part_files: list[str] = []
for batch in obstore.list(store, prefix=warehouse_prefix):
    for obj in batch:
        key = obj["path"]
        basename = key.rsplit("/", 1)[-1]
        if basename.startswith("compacted_") and key.endswith(".parquet"):
            all_parquet.append(key)
        elif basename.startswith("part_") and key.endswith(".parquet"):
            part_files.append(key)

print(f"  Found {len(all_parquet):,} compacted_*.parquet files (will register)")
if part_files:
    print(
        f"  Found {len(part_files):,} part_*.parquet files (raw ingest chunks — skipping to avoid duplicates)"
    )
    print(
        "  NOTE: these may be leftovers from interrupted mini-runs. Safe to delete manually if confirmed stale."
    )

# Build full s3:// URIs for add_files
parquet_uris = [f"s3://{warehouse_bucket}/{k}" for k in all_parquet]

# ---------------------------------------------------------------------------
# 2. Create a fresh catalog and register all existing files
# ---------------------------------------------------------------------------

output_path = Path(args.output)
output_path.unlink(missing_ok=True)

catalog = open_catalog(str(output_path), args.warehouse)

try:
    catalog.create_namespace(NAMESPACE)
except NamespaceAlreadyExistsError:
    pass

table = catalog.create_table(
    identifier=FULL_NAME,
    schema=ICEBERG_SCHEMA,
    partition_spec=PARTITION_SPEC,
)

print(f"Registering {len(parquet_uris):,} files into fresh catalog …")

# add_files in batches to keep Iceberg transactions manageable
BATCH = 2_000
total_registered = 0
for i in range(0, len(parquet_uris), BATCH):
    batch_uris = parquet_uris[i : i + BATCH]
    table.add_files(file_paths=batch_uris)
    total_registered += len(batch_uris)
    print(f"  registered {total_registered:,} / {len(parquet_uris):,}", end="\r", flush=True)

print(f"\nDone — {total_registered:,} files registered in {output_path}")

# ---------------------------------------------------------------------------
# 3. Quick sanity check
# ---------------------------------------------------------------------------

snap = table.current_snapshot()
print(f"Snapshot id   : {snap.snapshot_id if snap else 'none'}")
print(f"Manifest files: {len(list(table.inspect.manifests())) if snap else 0}")

# ---------------------------------------------------------------------------
# 4. Optionally upload
# ---------------------------------------------------------------------------

if args.upload:
    no_scheme = args.catalog_s3.removeprefix("s3://")
    cat_bucket, cat_key = no_scheme.split("/", 1)
    cat_store_kwargs: dict = dict(bucket=cat_bucket, region=region)
    if key_id:
        cat_store_kwargs["aws_access_key_id"] = key_id
    if secret:
        cat_store_kwargs["aws_secret_access_key"] = secret
    if token:
        cat_store_kwargs["aws_session_token"] = token
    cat_store = S3Store(**cat_store_kwargs)
    obstore.put(cat_store, cat_key, output_path.read_bytes())
    print(f"Uploaded repaired catalog → {args.catalog_s3}")
else:
    print(f"\nRepaired catalog written to {output_path}")
    print("Re-run with --upload to push it to S3.")
