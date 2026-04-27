"""
Compare unique STAC item IDs between PGSTAC and the Iceberg catalog.

Usage
-----
    # Compare PGSTAC CSV export vs Iceberg catalog parquet files
    python scripts/compare_catalogs.py \
        --pgstac /tmp/pgstac_ids.csv.gz \
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse/

    # Plain CSV also works
    python scripts/compare_catalogs.py \
        --pgstac /tmp/pgstac_ids.csv \
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse/

    # With a local catalog (uses DuckDB to scan parquet directly)
    python scripts/compare_catalogs.py \
        --pgstac /tmp/pgstac_ids.csv.gz \
        --catalog /tmp/earthcatalog_final.db \
        --warehouse s3://its-live-data/test-space/stac/catalog/warehouse/

    # Limit output
    python scripts/compare_catalogs.py \
        --pgstac /tmp/pgstac_ids.csv.gz \
        --sample 50 \
        --missing-prefix missing_ids
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description="Compare PGSTAC and Iceberg catalog item IDs")
parser.add_argument(
    "--pgstac",
    required=True,
    help="Path to PGSTAC ID export (CSV or CSV.gz)",
)
parser.add_argument(
    "--catalog",
    default=None,
    help="Local path to Iceberg catalog.db (optional, for registered file counts)",
)
parser.add_argument(
    "--warehouse",
    default="s3://its-live-data/test-space/stac/catalog/warehouse/",
)
parser.add_argument(
    "--sample",
    type=int,
    default=20,
    help="Number of sample missing IDs to show (default: 20)",
)
parser.add_argument(
    "--missing-prefix",
    default=None,
    help="Write full missing ID lists to <prefix>_in_pgstac_not_iceberg.csv and vice versa",
)
args = parser.parse_args()

try:
    import duckdb
except ImportError:
    print("duckdb not installed — run: pip install duckdb")
    sys.exit(1)

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"

wh_no_scheme = args.warehouse.removeprefix("s3://")
wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
glob_uri = f"s3://{wh_bucket}/{wh_prefix}**/*.parquet"

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_region='{region}';")
key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
token = os.environ.get("AWS_SESSION_TOKEN", "")
if key_id:
    con.execute(f"SET s3_access_key_id='{key_id}';")
if secret:
    con.execute(f"SET s3_secret_access_key='{secret}';")
if token:
    con.execute(f"SET s3_session_token='{token}';")

print("=" * 64)
print("Loading sources …")
print("=" * 64)

pgstac_path = args.pgstac
if pgstac_path.endswith(".gz"):
    pgstac_sql = f"SELECT id FROM read_csv('{pgstac_path}', header=false, columns={{'id': 'varchar'}}, filename=true, compression='gzip')"
else:
    pgstac_sql = (
        f"SELECT id FROM read_csv('{pgstac_path}', header=false, columns={{'id': 'varchar'}})"
    )

iceberg_sql = f"SELECT DISTINCT id FROM read_parquet('{glob_uri}', hive_partitioning=true)"

print(f"  PGSTAC  : {pgstac_path}")
print(f"  Iceberg : {glob_uri}")
print()

print("Counting PGSTAC IDs …")
pgstac_count = con.execute(f"SELECT COUNT(*) FROM ({pgstac_sql})").fetchone()[0]
print(f"  PGSTAC total rows: {pgstac_count:,}")

print("Counting Iceberg catalog IDs …")
iceberg_count = con.execute(f"SELECT COUNT(*) FROM ({iceberg_sql})").fetchone()[0]
print(f"  Iceberg unique IDs: {iceberg_count:,}")

print()
print("=" * 64)
print("Source counts")
print("=" * 64)
print(f"  PGSTAC rows        : {pgstac_count:>14,}")
print(f"  Iceberg unique IDs : {iceberg_count:>14,}")
print(
    f"  Difference         : {pgstac_count - iceberg_count:>14,}  ({100 * (pgstac_count - iceberg_count) / pgstac_count:.2f}%)"
    if pgstac_count
    else ""
)
print()

print("=" * 64)
print("Set comparison (this may take a few minutes) …")
print("=" * 64)

con.execute(f"CREATE VIEW pgstac_ids AS {pgstac_sql}")
con.execute(f"CREATE VIEW iceberg_ids AS {iceberg_sql}")

print("  Counting IDs in PGSTAC but not in Iceberg …")
in_pg_not_ice = con.execute(
    "SELECT COUNT(*) FROM pgstac_ids WHERE id NOT IN (SELECT id FROM iceberg_ids)"
).fetchone()[0]
print(f"  In PGSTAC but not Iceberg: {in_pg_not_ice:,}")

print("  Counting IDs in Iceberg but not in PGSTAC …")
in_ice_not_pg = con.execute(
    "SELECT COUNT(*) FROM iceberg_ids WHERE id NOT IN (SELECT id FROM pgstac_ids)"
).fetchone()[0]
print(f"  In Iceberg but not PGSTAC: {in_ice_not_pg:,}")

shared = pgstac_count - in_pg_not_ice
print()
print(f"  Shared IDs               : {shared:,}")
print(
    f"  PGSTAC-only              : {in_pg_not_ice:,}  ({100 * in_pg_not_ice / pgstac_count:.2f}%)"
    if pgstac_count
    else ""
)
print(f"  Iceberg-only             : {in_ice_not_pg:,}")

if in_pg_not_ice > 0:
    print()
    print(f"  Sample IDs in PGSTAC but not in Iceberg (first {args.sample}):")
    samples = con.execute(
        f"SELECT id FROM pgstac_ids WHERE id NOT IN (SELECT id FROM iceberg_ids) LIMIT {args.sample}"
    ).fetchall()
    for (sid,) in samples:
        print(f"    {sid}")

if in_ice_not_pg > 0:
    print()
    print(f"  Sample IDs in Iceberg but not in PGSTAC (first {args.sample}):")
    samples = con.execute(
        f"SELECT id FROM iceberg_ids WHERE id NOT IN (SELECT id FROM pgstac_ids) LIMIT {args.sample}"
    ).fetchall()
    for (sid,) in samples:
        print(f"    {sid}")

if args.missing_prefix and in_pg_not_ice > 0:
    path = f"{args.missing_prefix}_in_pgstac_not_iceberg.csv"
    con.execute(
        f"COPY (SELECT id FROM pgstac_ids WHERE id NOT IN (SELECT id FROM iceberg_ids)) TO '{path}' (HEADER, DELIMITER ',')"
    )
    print(f"\n  Full list written → {path}")

if args.missing_prefix and in_ice_not_pg > 0:
    path = f"{args.missing_prefix}_in_iceberg_not_pgstac.csv"
    con.execute(
        f"COPY (SELECT id FROM iceberg_ids WHERE id NOT IN (SELECT id FROM pgstac_ids)) TO '{path}' (HEADER, DELIMITER ',')"
    )
    print(f"  Full list written → {path}")

if in_pg_not_ice > 0:
    print()
    print("=" * 64)
    print("Investigating PGSTAC-only items")
    print("=" * 64)

    print("  Checking for NULL/empty IDs …")
    null_count = con.execute(
        "SELECT COUNT(*) FROM pgstac_ids WHERE id IS NULL OR id = ''"
    ).fetchone()[0]
    print(f"    NULL/empty IDs: {null_count:,}")

    print("  Checking ID length distribution (sample of 1000) …")
    len_stats = con.execute(
        "SELECT MIN(LENGTH(id)), MAX(LENGTH(id)), AVG(LENGTH(id))::int "
        "FROM (SELECT id FROM pgstac_ids WHERE id NOT IN (SELECT id FROM iceberg_ids) LIMIT 1000)"
    ).fetchone()
    print(f"    Min length: {len_stats[0]}, Max length: {len_stats[1]}, Avg length: {len_stats[2]}")

    print("  Checking for common prefixes in PGSTAC-only IDs (sample of 10000) …")
    prefix_rows = con.execute(
        "SELECT SUBSTRING(id, 1, POSITION('_' IN id) - 1) AS prefix, COUNT(*) as cnt "
        "FROM (SELECT id FROM pgstac_ids WHERE id NOT IN (SELECT id FROM iceberg_ids) LIMIT 10000) "
        "GROUP BY prefix ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    print(f"    {'Prefix':>30}  {'Count':>8}")
    for prefix, cnt in prefix_rows:
        print(f"    {str(prefix):>30}  {cnt:>8,}")

if args.catalog:
    print()
    print("=" * 64)
    print("Iceberg catalog metadata")
    print("=" * 64)

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from earthcatalog.core.catalog import FULL_NAME, open_catalog

    catalog = open_catalog(args.catalog, args.warehouse)
    table = catalog.load_table(FULL_NAME)
    files = table.inspect.files()
    total_rows = sum(r for r in files["record_count"].to_pylist() if r is not None)
    compacted = sum(
        1 for p in files["file_path"].to_pylist() if Path(p).name.startswith("compacted_")
    )
    part = sum(1 for p in files["file_path"].to_pylist() if Path(p).name.startswith("part_"))
    print(f"  Registered files   : {len(files):,}  (compacted={compacted}, part={part})")
    print(f"  Rows (metadata)    : {total_rows:,}")
    print(f"  Snapshots          : {len(table.history())}")

print()
print("=" * 64)
print("Done")
print("=" * 64)
