# ruff: noqa: E402
"""
Compact the small number of (cell, year) groups that failed during final_compact.py
(typically due to OOM on the heaviest groups) and register them into the local catalog.

Usage
-----
    # Local sequential run
    python scripts/compact_stragglers.py \
        --catalog /tmp/earthcatalog_final.db \
        --upload

    # On Coiled (recommended — avoids egress, runs groups in parallel)
    python scripts/compact_stragglers.py \
        --coiled \
        --catalog /tmp/earthcatalog_final.db \
        --upload

The script:
  1. Scans the warehouse for any groups that still have non-compacted files
     (part_*.parquet or tmp_*.parquet).  For those groups ALL .parquet files
     in the prefix are collected (including any orphaned compacted_* from a
     prior failed run), so nothing is missed and dedup removes any overlap.

  2. Compacts each group using a two-pass streaming merge (batch_size files
     per batch in pass 1 → temp files on S3; pass 2 merges temps → final).

     OPTIMISATION: if a group has NO part_* files its original inputs were
     already consumed by a prior (aborted) run; the existing tmp_* and
     compacted_* files are already pass-1 output.  We skip pass 1 entirely
     for those groups and jump straight to pass 2, saving significant S3
     egress and CPU.

  3. Registers the new compacted files into the local Iceberg catalog.
  4. Optionally uploads the updated catalog to S3.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import uuid
from pathlib import Path

parser = argparse.ArgumentParser(description="Compact straggler groups and register them")
parser.add_argument("--warehouse", default="s3://its-live-data/test-space/stac/catalog/warehouse/")
parser.add_argument("--catalog", default="/tmp/earthcatalog_final.db")
parser.add_argument(
    "--catalog-s3", default="s3://its-live-data/test-space/stac/catalog/earthcatalog.db"
)
parser.add_argument(
    "--batch-size",
    type=int,
    default=5,
    help="Files per batch per merge level (lower = less memory, more S3 round-trips)",
)
parser.add_argument(
    "--upload", action="store_true", help="Upload updated catalog to --catalog-s3 when done"
)
parser.add_argument("--dry-run", action="store_true", help="Discover stragglers only, no writes")
parser.add_argument(
    "--coiled", action="store_true", help="Run compaction on a Coiled cluster (recommended)"
)
parser.add_argument(
    "--coiled-workers",
    type=int,
    default=0,
    help="Number of Coiled workers (default: one per straggler group)",
)
parser.add_argument(
    "--coiled-vm-type",
    default="m6i.2xlarge",
    help="Coiled worker VM type (default: m6i.2xlarge)",
)
args = parser.parse_args()

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from obstore.store import S3Store

sys.path.insert(0, str(Path(__file__).parent.parent))
from earthcatalog.core.catalog import FULL_NAME, open_catalog

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
wh_no_scheme = args.warehouse.removeprefix("s3://")
wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)

store = S3Store(bucket=wh_bucket, region=region)

# ---------------------------------------------------------------------------
# Phase 1 — Discovery
#
# First pass: identify which (cell, year) groups are "stragglers" — i.e. still
# have part_* or tmp_* files present (meaning they were never cleanly compacted).
#
# Second pass: for each straggler group, collect ALL .parquet files in that
# prefix (part_*, tmp_*, and any orphaned compacted_* from a prior failed run).
# Dedup in the compact step will resolve any row overlap between them.
# ---------------------------------------------------------------------------

print("Scanning warehouse for straggler groups …")

# Identify straggler groups by presence of part_* or tmp_* files
straggler_prefixes: set[tuple[str, str]] = set()

for batch in obstore.list(store, prefix=wh_prefix):
    for obj in batch:
        key: str = obj["path"]
        if not key.endswith(".parquet"):
            continue
        fname = key.rsplit("/", 1)[-1]
        if not (fname.startswith("part_") or fname.startswith("tmp_")):
            continue
        rel = key.removeprefix(wh_prefix).strip("/")
        parts = rel.split("/")
        if len(parts) < 3:
            continue
        cell = parts[0].removeprefix("grid_partition=")
        year = parts[1].removeprefix("year=")
        straggler_prefixes.add((cell, year))

if not straggler_prefixes:
    print("No straggler groups found — catalog is complete.")
    sys.exit(0)

# For each straggler group, collect ALL .parquet files (part_*, tmp_*, compacted_*)
straggler_groups: dict[tuple[str, str], list[str]] = {}
for cell, year in straggler_prefixes:
    prefix = f"{wh_prefix}grid_partition={cell}/year={year}/"
    keys = []
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet"):
                keys.append(k)
    straggler_groups[(cell, year)] = keys

print(f"Found {len(straggler_groups)} straggler groups (all .parquet files per group):")
total_files = 0
for (cell, year), keys in sorted(straggler_groups.items(), key=lambda x: -len(x[1])):
    n_part = sum(1 for k in keys if k.rsplit("/", 1)[-1].startswith("part_"))
    n_tmp = sum(1 for k in keys if k.rsplit("/", 1)[-1].startswith("tmp_"))
    n_comp = sum(1 for k in keys if k.rsplit("/", 1)[-1].startswith("compacted_"))
    skip = " [SKIP pass-1: no part_* files]" if n_part == 0 else ""
    print(
        f"  {len(keys):>4} files  grid_partition={cell}/year={year}"
        f"  (part={n_part}, tmp={n_tmp}, compacted={n_comp}){skip}"
    )
    total_files += len(keys)
print(f"Total files to process: {total_files:,}")

if args.dry_run:
    print("\n--dry-run: stopping before compaction.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Core compaction function (top-level so it can be pickled for Coiled)
# ---------------------------------------------------------------------------


def compact_group(
    cell: str,
    year: str,
    keys: list[str],
    wh_bucket: str,
    wh_prefix: str,
    region: str,
    batch_size: int,
    aws_key_id: str | None = None,
    aws_secret: str | None = None,
    aws_token: str | None = None,
) -> str:
    """
    Compact all parquet files for one (cell, year) group into a single
    ``compacted_*.parquet`` on S3.  Returns the S3 URI of the output file.

    Pass-1 skip: if the group has no ``part_*`` files, the existing
    ``tmp_*`` + ``compacted_*`` files are already pass-1 output — we jump
    straight to pass 2 (merge-and-dedup), saving significant egress.
    """
    import os

    import obstore
    import pyarrow as pa
    from obstore.store import S3Store

    sk: dict = dict(bucket=wh_bucket, region=region)
    if aws_key_id:
        sk["aws_access_key_id"] = aws_key_id
    if aws_secret:
        sk["aws_secret_access_key"] = aws_secret
    if aws_token:
        sk["aws_session_token"] = aws_token
    store = S3Store(**sk)
    prefix = f"{wh_prefix}grid_partition={cell}/year={year}/"
    out_key = f"{prefix}compacted_{uuid.uuid4().hex[:8]}.parquet"

    # Re-scan the prefix right now so that task retries (after a worker crash)
    # always see the *current* S3 state rather than the stale list captured at
    # discovery time.  A crashed prior attempt may have deleted some source
    # files and written intermediate tmp_* files that aren't in the original
    # keys list — a fresh scan picks all of them up.
    live_keys: list[str] = []
    for _batch in obstore.list(store, prefix=prefix):
        for _obj in _batch:
            k: str = _obj["path"]
            if k.endswith(".parquet"):
                live_keys.append(k)

    if not live_keys:
        # All files are gone — a previous successful attempt already finished
        # and deleted everything, but the compacted_ file wasn't returned.
        # This shouldn't happen in normal flow; surface as an error.
        raise RuntimeError(f"No parquet files found in {prefix} — was the group already compacted?")

    # Single compacted_ file means a prior attempt finished successfully.
    if len(live_keys) == 1 and live_keys[0].rsplit("/", 1)[-1].startswith("compacted_"):
        print(f"  {cell}/{year}: already compacted → {live_keys[0].rsplit('/', 1)[-1]}")
        return f"s3://{wh_bucket}/{live_keys[0]}"

    # Use the live list for all subsequent processing.
    keys = live_keys

    def _read_and_cast(key: str) -> pa.Table | None:
        try:
            raw = bytes(obstore.get(store, key).bytes())
        except Exception as exc:
            if any(s in str(exc) for s in ("404", "Not Found", "NoSuchKey")):
                print(f"  WARN 404, skipping: {key.rsplit('/', 1)[-1]}")
                return None
            raise
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        new_cols: dict = {}
        new_fields: list = []
        for fi, field in enumerate(tbl.schema):
            col = tbl.column(fi)
            if pa.types.is_string(field.type):
                col = col.cast(pa.large_utf8())
                field = field.with_type(pa.large_utf8())
            new_cols[field.name] = col
            new_fields.append(field)
        return pa.table(new_cols, schema=pa.schema(new_fields, metadata=tbl.schema.metadata))

    def _dedup(tbl: pa.Table) -> pa.Table:
        idx = pc.sort_indices(
            tbl,
            sort_keys=[("id", "ascending"), ("updated", "descending")],
            null_placement="at_end",
        )
        tbl = tbl.take(idx)
        seen: set = set()
        keep: list[int] = []
        for i, v in enumerate(tbl.column("id").to_pylist()):
            if v not in seen:
                seen.add(v)
                keep.append(i)
        return tbl.take(pa.array(keep, type=pa.int64()))

    def _write_to_s3(tbl: pa.Table, key: str) -> int:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp_path = f.name
        try:
            pq.write_table(tbl, tmp_path, compression="snappy", row_group_size=64 * 1024)
            data = open(tmp_path, "rb").read()
            obstore.put(store, key, data)
            return len(data)
        finally:
            os.unlink(tmp_path)

    part_keys = [k for k in keys if k.rsplit("/", 1)[-1].startswith("part_")]
    has_part_files = len(part_keys) > 0

    new_tmp_keys: list[str] = []
    input_rows = 0

    if has_part_files:
        # When part_* files exist, old tmp_*/compacted_* are redundant (their
        # source data is still the part files from a prior failed run).  Only
        # process the part_* files to avoid re-merging already-merged data and
        # exploding the file count on repeated re-runs.
        pass1_input = part_keys
        n_batches = -(-len(pass1_input) // batch_size)
        print(
            f"  grid_partition={cell}/year={year}: {len(pass1_input)} part_* files "
            f"({len(keys) - len(pass1_input)} redundant tmp/compacted skipped), "
            f"{n_batches} batches (pass 1 needed)"
        )
        for bi, i in enumerate(range(0, len(pass1_input), batch_size), 1):
            batch = pass1_input[i : i + batch_size]
            tables = [t for k in batch if (t := _read_and_cast(k)) is not None]
            if not tables:
                continue
            btbl = pa.concat_tables(tables, promote_options="none")
            input_rows += btbl.num_rows
            btbl = _dedup(btbl)
            tmp_key = f"{prefix}tmp_{uuid.uuid4().hex[:12]}.parquet"
            _write_to_s3(btbl, tmp_key)
            new_tmp_keys.append(tmp_key)
            print(
                f"    batch {bi}/{n_batches}: {btbl.num_rows:,} rows → {tmp_key.rsplit('/', 1)[-1]}"
            )
    else:
        # Pass-1 SKIP: existing tmp_* + compacted_* ARE the pass-1 output
        print(
            f"  grid_partition={cell}/year={year}: {len(keys)} files "
            f"(pass 1 skipped — no part_* files)"
        )
        new_tmp_keys = keys  # treat existing files as pass-1 output

    if not new_tmp_keys:
        raise RuntimeError(f"All files were 404 for grid_partition={cell}/year={year}")

    # Multi-level merge: keep batching until ≤ batch_size files remain.
    # Each level writes new tmp files BEFORE deleting the previous level's
    # files, so a crash mid-level always leaves enough data for the next run
    # to resume correctly (pass-1 skip will pick up whatever tmp_* survive).
    level = 1
    while len(new_tmp_keys) > batch_size:
        n_batches = -(-len(new_tmp_keys) // batch_size)
        print(f"  Level-{level + 1} merge: {len(new_tmp_keys)} files → {n_batches} batches …")
        next_round: list[str] = []
        for bi, i in enumerate(range(0, len(new_tmp_keys), batch_size), 1):
            batch = new_tmp_keys[i : i + batch_size]
            tables = [t for k in batch if (t := _read_and_cast(k)) is not None]
            if not tables:
                continue
            btbl = pa.concat_tables(tables, promote_options="none")
            btbl = _dedup(btbl)
            rnd_key = f"{prefix}tmp_{uuid.uuid4().hex[:12]}.parquet"
            _write_to_s3(btbl, rnd_key)
            next_round.append(rnd_key)
            print(
                f"    batch {bi}/{n_batches}: {btbl.num_rows:,} rows → {rnd_key.rsplit('/', 1)[-1]}"
            )
        # Delete this level's inputs only after next level is fully written
        for k in new_tmp_keys:
            try:
                obstore.delete(store, k)
            except Exception:
                pass
        new_tmp_keys = next_round
        level += 1

    # Final merge: guaranteed ≤ batch_size files
    print(f"  Final merge: {len(new_tmp_keys)} files for {cell}/{year} …")
    final_tables = [t for k in new_tmp_keys if (t := _read_and_cast(k)) is not None]
    if not final_tables:
        raise RuntimeError(f"All final-merge files were 404 for grid_partition={cell}/year={year}")

    merged = pa.concat_tables(final_tables, promote_options="none")
    merged = _dedup(merged)
    merged = merged.sort_by([("platform", "ascending"), ("datetime", "ascending")])

    size = _write_to_s3(merged, out_key)
    out_rows = merged.num_rows
    print(
        f"  → {out_rows:,} unique rows  ({size / 1e6:.1f} MB)  "
        f"dupes removed: {input_rows - out_rows:,}"
    )

    # Delete original input keys + final-round tmp files.
    # (Intermediate-level tmp files were already deleted inside the loop above.)
    for k in set(keys) | set(new_tmp_keys):
        try:
            obstore.delete(store, k)
        except Exception:
            pass

    return f"s3://{wh_bucket}/{out_key}"


# ---------------------------------------------------------------------------
# Phase 2 — Compact each straggler group
# ---------------------------------------------------------------------------

groups_list = sorted(straggler_groups.items(), key=lambda x: -len(x[1]))

aws_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_token = os.environ.get("AWS_SESSION_TOKEN")

new_uris: list[str] = []

if args.coiled:
    import coiled
    from dask.distributed import as_completed

    n_workers = args.coiled_workers if args.coiled_workers > 0 else len(groups_list)
    cluster = coiled.Cluster(
        n_workers=n_workers,
        worker_vm_types=[args.coiled_vm_type],
        region=region,
        spot_policy="spot_with_fallback",
        name="earthcatalog-compact-stragglers",
    )
    client = cluster.get_client()
    print(f"Coiled cluster ready: {client.dashboard_link}")

    futures = {
        client.submit(
            compact_group,
            cell,
            year,
            keys,
            wh_bucket,
            wh_prefix,
            region,
            args.batch_size,
            aws_key_id,
            aws_secret,
            aws_token,
        ): (cell, year)
        for (cell, year), keys in groups_list
    }

    errors = 0
    for future in as_completed(list(futures)):
        cell, year = futures[future]
        try:
            uri = future.result()
            new_uris.append(uri)
            print(f"  OK  grid_partition={cell}/year={year}  → {uri}")
        except Exception as exc:
            errors += 1
            print(f"  ERROR  grid_partition={cell}/year={year}: {exc}")

    cluster.close()
    if errors:
        print(f"\n{errors} group(s) failed — check output above.")
else:
    for (cell, year), keys in groups_list:
        try:
            uri = compact_group(
                cell,
                year,
                keys,
                wh_bucket,
                wh_prefix,
                region,
                args.batch_size,
                aws_key_id,
                aws_secret,
                aws_token,
            )
            new_uris.append(uri)
        except Exception as exc:
            print(f"  ERROR  grid_partition={cell}/year={year}: {exc}")

# ---------------------------------------------------------------------------
# Phase 3 — Register new files into the Iceberg catalog
# ---------------------------------------------------------------------------

if not new_uris:
    print("\nNothing new to register.")
    sys.exit(0)

print(f"\nRegistering {len(new_uris)} new files into {args.catalog} …")
cat = open_catalog(args.catalog, args.warehouse)
iceberg_table = cat.load_table(FULL_NAME)
iceberg_table.add_files(file_paths=new_uris)

files = iceberg_table.inspect.files()
total_rows = sum(r for r in files["record_count"].to_pylist() if r is not None)
print(f"Catalog now: {len(files):,} files, {total_rows:,} rows")

if args.upload:
    no_scheme = args.catalog_s3.removeprefix("s3://")
    cat_bucket, cat_key = no_scheme.split("/", 1)
    cat_store = S3Store(bucket=cat_bucket, region=region)
    obstore.put(cat_store, cat_key, Path(args.catalog).read_bytes())
    print(f"Uploaded → {args.catalog_s3}")
