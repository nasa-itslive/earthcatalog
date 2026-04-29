"""
Rebuild the earthcatalog Iceberg catalog from warehouse Parquet files.

Scans the warehouse for all ``*.parquet`` files with hive-style paths
(``grid_partition=X/year=Y/*.parquet``), creates a fresh SQLite catalog,
and registers every file via ``table.add_files()``.

Supports local filesystem paths and S3 URIs.  For public S3 buckets,
no credentials are needed — anonymous access is used automatically.

Usage
-----
Local::

    python scripts/repair_catalog.py \\
        --warehouse /tmp/warehouse_test \\
        --output /tmp/earthcatalog.db \\
        --h3-resolution 1

S3::

    python scripts/repair_catalog.py \\
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse/ \\
        --output /tmp/earthcatalog_repaired.db \\
        --h3-resolution 1 \\
        --upload
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser(
    description="Rebuild earthcatalog Iceberg catalog from warehouse files"
)
parser.add_argument(
    "--warehouse",
    required=True,
    help="Warehouse root (local path or s3:// URI)",
)
parser.add_argument(
    "--output",
    default="/tmp/earthcatalog_repaired.db",
    help="Local path for the rebuilt SQLite catalog",
)
parser.add_argument(
    "--h3-resolution",
    type=int,
    default=None,
    help="H3 resolution to store as table property",
)
parser.add_argument(
    "--upload",
    action="store_true",
    help="Upload rebuilt catalog to S3 (requires --catalog-s3)",
)
parser.add_argument(
    "--catalog-s3",
    default="s3://its-live-data/test-space/stac/catalog/earthcatalog.db",
    help="S3 URI to upload the catalog to (used with --upload)",
)
args = parser.parse_args()

HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

# noqa: E402
from pyiceberg.exceptions import NamespaceAlreadyExistsError  # noqa: E402

from earthcatalog.core.catalog import (  # noqa: E402
    FULL_NAME,
    ICEBERG_SCHEMA,
    NAMESPACE,
    PARTITION_SPEC,
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
    open_catalog,
)

# ---------------------------------------------------------------------------
# 1. List all hive-style parquet files
# ---------------------------------------------------------------------------

parquet_uris: list[str] = []

if args.warehouse.startswith("s3://"):
    import obstore
    from obstore.store import S3Store

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    no_scheme = args.warehouse.removeprefix("s3://")
    bucket, prefix = no_scheme.split("/", 1)

    store_kwargs: dict = dict(bucket=bucket, region=region, skip_signature=True)
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")
    if key_id and secret:
        store_kwargs["aws_access_key_id"] = key_id
        store_kwargs["aws_secret_access_key"] = secret
        del store_kwargs["skip_signature"]
        if token:
            store_kwargs["aws_session_token"] = token

    store = S3Store(**store_kwargs)

    print(f"Listing *.parquet files under s3://{bucket}/{prefix} …")
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            key: str = obj["path"]
            if key.endswith(".parquet") and HIVE_RE.search(key):
                parquet_uris.append(f"s3://{bucket}/{key}")
else:
    warehouse_path = Path(args.warehouse)
    print(f"Listing *.parquet files under {warehouse_path} …")
    for f in warehouse_path.glob("**/*.parquet"):
        rel = str(f.relative_to(warehouse_path))
        if HIVE_RE.search(rel):
            parquet_uris.append(str(f))

print(f"  Found {len(parquet_uris):,} parquet files with hive-style paths")

# ---------------------------------------------------------------------------
# 2. Create a fresh catalog and register all files
# ---------------------------------------------------------------------------

output_path = Path(args.output)
output_path.unlink(missing_ok=True)

catalog = open_catalog(str(output_path), args.warehouse)

try:
    catalog.create_namespace(NAMESPACE)
except NamespaceAlreadyExistsError:
    pass

props: dict[str, str] = {PROP_GRID_TYPE: "h3"}
if args.h3_resolution is not None:
    props[PROP_GRID_RESOLUTION] = str(args.h3_resolution)

table = catalog.create_table(
    identifier=FULL_NAME,
    schema=ICEBERG_SCHEMA,
    partition_spec=PARTITION_SPEC,
    properties=props,
)

print(f"Registering {len(parquet_uris):,} files …")

# Group files by cell so we can commit one snapshot per cell batch.
# We pass files in batches of BATCH size regardless of cell to keep
# snapshot count low, but we annotate each DataFile with its partition value.

by_cell: dict[str, list[str]] = defaultdict(list)
for uri in parquet_uris:
    m = HIVE_RE.search(uri)
    if m:
        by_cell[m.group("cell")].append(uri)

BATCH = 500  # files per snapshot commit
total_registered = 0
batch_uris: list[str] = []


def _flush(uris: list[str]) -> None:
    table.add_files(file_paths=uris)


for cell, uris in by_cell.items():
    for uri in uris:
        batch_uris.append(uri)
        if len(batch_uris) >= BATCH:
            _flush(batch_uris)
            total_registered += len(batch_uris)
            print(
                f"  registered {total_registered:,} / {len(parquet_uris):,}", end="\r", flush=True
            )
            batch_uris = []

if batch_uris:
    _flush(batch_uris)
    total_registered += len(batch_uris)

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
    import obstore
    from obstore.store import S3Store

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    no_scheme = args.catalog_s3.removeprefix("s3://")
    cat_bucket, cat_key = no_scheme.split("/", 1)
    cat_store_kwargs: dict = dict(bucket=cat_bucket, region=region)
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if key_id:
        cat_store_kwargs["aws_access_key_id"] = key_id
    if secret:
        cat_store_kwargs["aws_secret_access_key"] = secret
    cat_store = S3Store(**cat_store_kwargs)
    obstore.put(cat_store, cat_key, output_path.read_bytes())
    print(f"Uploaded catalog → {args.catalog_s3}")
else:
    print(f"\nCatalog written to {output_path}")
    print("Re-run with --upload to push it to S3.")
