# ruff: noqa: E402
"""
Catalog validation and statistics.

Works at any point during or after ingestion — mid-run, post-ingest,
or post-final-compaction.  The unique-item count is always exact.

Sections
--------
1. Registered file breakdown  (compacted_ vs part_, missing files)
2. Inventory coverage         (ingest_stats.json vs manifest)
3. Unique item count          (DuckDB COUNT DISTINCT — full scan)
4. Per-year breakdown         (DuckDB GROUP BY)
5. Per-cell breakdown         (DuckDB GROUP BY, top-N)

Usage
-----
    # Catalog already downloaded locally
    python scripts/catalog_stats.py --catalog /tmp/earthcatalog.db

    # Auto-download from S3
    python scripts/catalog_stats.py --catalog-s3 s3://its-live-data/test-space/stac/catalog/earthcatalog.db

    # Skip the full DuckDB scan (sections 3-5) for a quick coverage check
    python scripts/catalog_stats.py --no-scan
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="Catalog validation and statistics")
parser.add_argument("--catalog", default=None, help="Local path to catalog.db")
parser.add_argument(
    "--catalog-s3",
    default="s3://its-live-data/test-space/stac/catalog/earthcatalog.db",
    help="S3 URI of catalog.db — downloaded automatically if --catalog not given",
)
parser.add_argument(
    "--warehouse",
    default="s3://its-live-data/test-space/stac/catalog/warehouse/",
)
parser.add_argument(
    "--manifest",
    default=(
        "s3://pds-buckets-its-live-logbucket-70tr3aw5f2op/inventory/velocity_image_pair"
        "/its-live-data/VelocityGranuleInventory/2026-04-24T01-00Z/manifest.json"
    ),
)
parser.add_argument(
    "--ingest-stats",
    default="s3://its-live-data/test-space/stac/catalog/ingest_stats.json",
)
parser.add_argument("--top-n", type=int, default=20, help="Top-N cells to show in section 5")
parser.add_argument("--no-scan", action="store_true", help="Skip DuckDB full scan (sections 3-5)")
args = parser.parse_args()

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import obstore
from obstore.store import S3Store

sys.path.insert(0, str(Path(__file__).parent.parent))
from earthcatalog.core.catalog import FULL_NAME, open_catalog

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"


def _make_store(bucket: str) -> S3Store:
    sk: dict = dict(bucket=bucket, region=region)
    for env, key in [
        ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ("AWS_SESSION_TOKEN", "aws_session_token"),
    ]:
        v = os.environ.get(env)
        if v:
            sk[key] = v
    return S3Store(**sk)


def _s3_get_json(uri: str) -> object:
    no_scheme = uri.removeprefix("s3://")
    bucket, key = no_scheme.split("/", 1)
    return json.loads(bytes(obstore.get(_make_store(bucket), key).bytes()))


# ---------------------------------------------------------------------------
# Resolve local catalog path — download from S3 if needed
# ---------------------------------------------------------------------------

if args.catalog and Path(args.catalog).exists():
    local_catalog = args.catalog
else:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    local_catalog = tmp.name
    tmp.close()
    print(f"Downloading catalog from {args.catalog_s3} …")
    no_scheme = args.catalog_s3.removeprefix("s3://")
    cat_bucket, cat_key = no_scheme.split("/", 1)
    cat_bytes = bytes(obstore.get(_make_store(cat_bucket), cat_key).bytes())
    Path(local_catalog).write_bytes(cat_bytes)
    print(f"  Downloaded {len(cat_bytes) / 1e6:.1f} MB → {local_catalog}")

catalog = open_catalog(local_catalog, args.warehouse)
table = catalog.load_table(FULL_NAME)

snap = table.current_snapshot()
if snap is None:
    print("ERROR: catalog has no snapshot yet.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Section 1 — Registered file breakdown
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("1. Registered file breakdown")
print("=" * 64)

files_df = table.inspect.files().to_pydict()
all_file_paths: list[str] = files_df["file_path"]
record_counts: list[int] = files_df["record_count"]

compacted = [p for p in all_file_paths if Path(p).name.startswith("compacted_")]
part_files = [p for p in all_file_paths if Path(p).name.startswith("part_")]
other = [p for p in all_file_paths if not Path(p).name.startswith(("compacted_", "part_"))]

total_rows_meta = sum(record_counts)

print(f"  Snapshots in history       : {len(table.history()):>10,}")
print(f"  Total registered files     : {len(all_file_paths):>10,}")
print(f"    compacted_*.parquet      : {len(compacted):>10,}")
print(
    f"    part_*.parquet           : {len(part_files):>10,}  {'<-- duplicates possible' if part_files else 'OK'}"
)
if other:
    print(f"    other                    : {len(other):>10,}")
print(
    f"  Total rows (file metadata) : {total_rows_meta:>10,}  (fast estimate, may overcount if duplicates)"
)

# Check for missing files on S3
print()
print("  Checking S3 for missing files …")
wh_no_scheme = args.warehouse.removeprefix("s3://")
wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
wh_store = _make_store(wh_bucket)

on_s3: set[str] = set()
for batch in obstore.list(wh_store, prefix=wh_prefix):
    for obj in batch:
        k = obj["path"]
        if k.endswith(".parquet"):
            on_s3.add(f"s3://{wh_bucket}/{k}")

missing = [p for p in all_file_paths if p not in on_s3]
unregistered = [p for p in on_s3 if p not in set(all_file_paths)]

print(f"  Files registered & present : {len(all_file_paths) - len(missing):>10,}  OK")
print(
    f"  Files registered but MISSING: {len(missing):>9,}  {'OK' if not missing else '<-- dangling refs!'}"
)
print(
    f"  Files on S3 but unregistered: {len(unregistered):>9,}  {'OK' if not unregistered else '<-- not in catalog'}"
)
if missing:
    print(f"  First 3 missing: {missing[:3]}")

# ---------------------------------------------------------------------------
# Section 2 — Inventory coverage
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("2. Inventory coverage")
print("=" * 64)

try:
    manifest = _s3_get_json(args.manifest)
    manifest_files = manifest.get("files", [])
    manifest_keys = {f["key"] for f in manifest_files}
    total_manifest = len(manifest_keys)
    print(f"  Manifest data files total  : {total_manifest:>10,}")
except Exception as exc:
    print(f"  Could not fetch manifest: {exc}")
    manifest_keys = set()
    total_manifest = None

try:
    stats = _s3_get_json(args.ingest_stats)
    processed_keys = {r["inventory_file"] for r in stats if r.get("inventory_file")}
    source_items_total = sum(r.get("source_items", 0) for r in stats)
    total_rows_stats = sum(r.get("total_rows", 0) for r in stats)

    print(f"  Inventory files processed  : {len(processed_keys):>10,}  / {total_manifest or '?'}")
    if total_manifest:
        remaining = total_manifest - len(processed_keys)
        print(
            f"  Remaining                  : {remaining:>10,}  {'ALL DONE ✓' if remaining == 0 else '<-- still running or needs re-run'}"
        )
        if remaining > 0 and manifest_keys:
            unprocessed = sorted(manifest_keys - processed_keys)
            print(f"  Unprocessed files ({len(unprocessed)}):")
            for k in unprocessed[:10]:
                print(f"    {k}")
            if len(unprocessed) > 10:
                print(f"    … and {len(unprocessed) - 10} more")
    print(f"  Source items (sum)         : {source_items_total:>10,}")
    print(f"  Rows written (sum)         : {total_rows_stats:>10,}")
    avg_fan_out = total_rows_stats / source_items_total if source_items_total else 0
    print(f"  Overall avg fan-out        : {avg_fan_out:>13.4f}")
except Exception as exc:
    print(f"  Could not fetch ingest_stats.json: {exc}")

# ---------------------------------------------------------------------------
# Sections 3-5 — DuckDB full scan
# ---------------------------------------------------------------------------

if args.no_scan:
    print()
    print("(Skipping DuckDB scan — remove --no-scan for unique item count and breakdowns)")
    sys.exit(0)

print()
print("=" * 64)
print("3-5. Full scan via DuckDB (this may take several minutes)")
print("=" * 64)

try:
    import duckdb
except ImportError:
    print("duckdb not installed — run: pip install duckdb")
    sys.exit(1)

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

# Use glob — avoids passing thousands of individual paths
glob_uri = f"s3://{wh_bucket}/{wh_prefix}**/*.parquet"
print(f"  Scanning: {glob_uri}")

# ---------------------------------------------------------------------------
# Section 3 — Unique item count
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("3. Unique item count")
print("=" * 64)

row = con.execute(
    f"SELECT COUNT(*) as total, COUNT(DISTINCT id) as unique_ids "
    f"FROM read_parquet('{glob_uri}', hive_partitioning=true)"
).fetchone()
total_scanned, unique_ids = row
duplicates = total_scanned - unique_ids
dup_pct = 100 * duplicates / total_scanned if total_scanned else 0

print(f"  Total rows scanned         : {total_scanned:>12,}")
print(f"  Unique STAC item IDs       : {unique_ids:>12,}")
print(f"  Duplicate rows             : {duplicates:>12,}  ({dup_pct:.2f}%)")

# ---------------------------------------------------------------------------
# Section 4 — Per-year breakdown
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("4. Per-year breakdown")
print("=" * 64)

year_rows = con.execute(
    f"""
    SELECT
        year,
        COUNT(*)             AS total_rows,
        COUNT(DISTINCT id)   AS unique_items,
        COUNT(*) - COUNT(DISTINCT id) AS duplicates
    FROM read_parquet('{glob_uri}', hive_partitioning=true)
    GROUP BY year
    ORDER BY year
    """
).fetchall()

print(f"  {'Year':>6}  {'Total rows':>12}  {'Unique items':>12}  {'Duplicates':>12}  {'Dup %':>7}")
print("  " + "-" * 58)
for year, total, unique, dupes in year_rows:
    pct = 100 * dupes / total if total else 0
    print(f"  {str(year):>6}  {total:>12,}  {unique:>12,}  {dupes:>12,}  {pct:>6.2f}%")

# ---------------------------------------------------------------------------
# Section 5 — Per-cell breakdown (top-N)
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print(f"5. Top {args.top_n} cells by unique item count")
print("=" * 64)

cell_rows = con.execute(
    f"""
    SELECT
        grid_partition,
        COUNT(DISTINCT YEAR(datetime))  AS years_covered,
        COUNT(DISTINCT id)              AS unique_items,
        COUNT(*) - COUNT(DISTINCT id)   AS duplicates,
        COUNT(*)                        AS total_rows
    FROM read_parquet('{glob_uri}', hive_partitioning=true)
    GROUP BY grid_partition
    ORDER BY unique_items DESC
    LIMIT {args.top_n}
    """
).fetchall()

print(f"  {'Cell':>20}  {'Years':>6}  {'Unique items':>12}  {'Dup %':>7}")
print("  " + "-" * 52)
for cell, years, unique, dupes, total in cell_rows:
    pct = 100 * dupes / total if total else 0
    print(f"  {str(cell):>20}  {years:>6}  {unique:>12,}  {pct:>6.2f}%")

# ---------------------------------------------------------------------------
# Save JSON summary
# ---------------------------------------------------------------------------

summary_path = Path(local_catalog).with_suffix(".validation.json")
summary_path.write_text(
    json.dumps(
        {
            "registered_files": len(all_file_paths),
            "compacted_files": len(compacted),
            "part_files": len(part_files),
            "missing_from_s3": len(missing),
            "unregistered_on_s3": len(unregistered),
            "total_rows_metadata": total_rows_meta,
            "total_rows_scanned": total_scanned,
            "unique_items": unique_ids,
            "duplicate_rows": duplicates,
            "duplicate_pct": round(dup_pct, 4),
            "per_year": [
                {"year": str(y), "total_rows": t, "unique_items": u, "duplicates": d}
                for y, t, u, d in year_rows
            ],
            "top_cells": [
                {
                    "cell": str(c),
                    "years_covered": y,
                    "unique_items": u,
                    "duplicate_pct": round(100 * d / t, 4) if t else 0,
                }
                for c, y, u, d, t in cell_rows
            ],
        },
        indent=2,
    )
)
print()
print(f"  Summary saved → {summary_path}")
print("=" * 64)
