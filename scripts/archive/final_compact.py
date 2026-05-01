# ruff: noqa: E402
"""
Final global compaction pass for earthcatalog.

After all per-file mini-runs complete, the warehouse contains many
``(cell, year)`` prefixes with multiple files (one or more ``part_*.parquet``
from each mini-run, possibly alongside earlier ``compacted_*.parquet``).

This script:

  Phase 1 — Discovery
    List every Parquet file in the warehouse; group by ``(cell, year)``.
    Identify which groups need merging (>1 file).

  Phase 2 — Compact  [Dask, optionally on Coiled]
    One Dask delayed task per ``(cell, year)`` group:
      - Download all files for the group
      - Concat + dedup on ``id`` (keep most-recent ``updated``)
      - Write one ``compacted_*.parquet`` back to S3
      - Delete the input files
      - Return a ``CompactResult`` (cell, year, input_files, input_rows,
        output_rows, duplicates_removed, file_size_bytes)

  Phase 3 — Rebuild Iceberg catalog
    Create a fresh SQLite catalog.
    Register every post-compaction file via ``table.add_files()``.
    Upload to S3.

  Phase 4 — Stats report
    Unique items, per-year breakdown, per-cell breakdown (top-N),
    duplicate overlap %, distribution of items-per-cell.

Usage
-----
    # Dry run (discovery + stats only, no writes)
    python scripts/final_compact.py --dry-run

    # Full run, local Dask scheduler
    python scripts/final_compact.py --output /tmp/earthcatalog_final.db --upload

    # Full run on Coiled
    python scripts/final_compact.py \\
        --coiled \\
        --coiled-workers 50 \\
        --output /tmp/earthcatalog_final.db \\
        --upload
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="Final global compaction for earthcatalog")
parser.add_argument("--warehouse", default="s3://its-live-data/test-space/stac/catalog/warehouse/")
parser.add_argument(
    "--catalog-s3", default="s3://its-live-data/test-space/stac/catalog/earthcatalog.db"
)
parser.add_argument(
    "--output", default="/tmp/earthcatalog_final.db", help="Local path for repaired catalog"
)
parser.add_argument("--upload", action="store_true", help="Upload repaired catalog to --catalog-s3")
parser.add_argument("--dry-run", action="store_true", help="Discovery + stats only, no S3 writes")
parser.add_argument("--coiled", action="store_true", help="Run Phase 2 on Coiled cluster")
parser.add_argument("--coiled-workers", type=int, default=50)
parser.add_argument("--coiled-worker-vm-types", nargs="+", default=["m6i.xlarge"])
parser.add_argument(
    "--batch-size",
    type=int,
    default=50,
    help="Files per batch per merge level (default 50; use ~5 for very heavy groups)",
)
parser.add_argument("--stats-top-n", type=int, default=20, help="Top-N cells to show in stats")
args = parser.parse_args()

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import dask
import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from obstore.store import S3Store
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from earthcatalog.core.catalog import (
    FULL_NAME,
    ICEBERG_SCHEMA,
    NAMESPACE,
    PARTITION_SPEC,
    open_catalog,
)

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"


def _make_store(bucket: str) -> S3Store:
    sk: dict = dict(bucket=bucket, region=region)
    for env, key in [
        ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ("AWS_SESSION_TOKEN", "aws_session_token"),
    ]:
        val = os.environ.get(env)
        if val:
            sk[key] = val
    return S3Store(**sk)


warehouse_no_scheme = args.warehouse.removeprefix("s3://")
warehouse_bucket, warehouse_prefix = warehouse_no_scheme.split("/", 1)

# ---------------------------------------------------------------------------
# Phase 1 — Discovery
# ---------------------------------------------------------------------------

print("=" * 64)
print("Phase 1 — Discovery")
print("=" * 64)

print(f"Listing files under s3://{warehouse_bucket}/{warehouse_prefix} …")
store = _make_store(warehouse_bucket)

# group: { (cell, year_str) -> [s3_key, ...] }
groups: dict[tuple[str, str], list[str]] = defaultdict(list)
total_listed = 0

for batch in obstore.list(store, prefix=warehouse_prefix):
    for obj in batch:
        key: str = obj["path"]
        if not key.endswith(".parquet"):
            continue
        # path layout: <warehouse_prefix>/grid_partition=<cell>/year=<year>/<file>
        rel = key.removeprefix(warehouse_prefix)
        parts = rel.strip("/").split("/")
        if len(parts) < 3:
            print(f"  WARN: unexpected path layout: {key}")
            continue
        cell_part = parts[0]  # grid_partition=XXX
        year_part = parts[1]  # year=YYYY
        cell = cell_part.removeprefix("grid_partition=")
        year = year_part.removeprefix("year=")
        groups[(cell, year)].append(key)
        total_listed += 1

total_groups = len(groups)
needs_merge = {k: v for k, v in groups.items() if len(v) > 1}
already_single = {k: v for k, v in groups.items() if len(v) == 1}
# Groups with no part_* files are re-run resumption candidates: their
# tmp_*/compacted_* files are already pass-1 output so pass 1 will be skipped.
pass1_skip = {
    k: v
    for k, v in needs_merge.items()
    if not any(key.rsplit("/", 1)[-1].startswith("part_") for key in v)
}

print(f"  Total parquet files      : {total_listed:>10,}")
print(f"  Total (cell, year) groups: {total_groups:>10,}")
print(f"  Groups needing merge     : {len(needs_merge):>10,}")
print(f"  Already single-file      : {len(already_single):>10,}")
if pass1_skip:
    print(f"  Pass-1 skip (no part_*)  : {len(pass1_skip):>10,}  (resuming prior partial run)")

if args.dry_run:
    print("\n--dry-run: stopping after discovery.")
    # Quick stats from Parquet metadata only
    print("\nEstimated row counts from file metadata (no data read):")
    total_estimated = 0
    for batch in obstore.list(store, prefix=warehouse_prefix):
        for obj in batch:
            pass  # already done above
    # For a real dry-run stats preview, just show group size distribution
    from collections import Counter as _Counter

    size_dist = _Counter(len(v) for v in groups.values())
    print("  Files-per-group distribution:")
    for n_files in sorted(size_dist):
        print(f"    {n_files:>4} files : {size_dist[n_files]:>8,} groups")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Phase 2 — Compact
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("Phase 2 — Compact")
print("=" * 64)


@dataclass
class CompactResult:
    cell: str
    year: str
    input_files: int
    input_rows: int
    output_rows: int
    duplicates_removed: int
    file_size_bytes: int
    out_key: str
    skipped: bool = False  # True when group already had 1 file


def _compact_cell_year(
    cell: str,
    year: str,
    keys: list[str],
    wh_bucket: str,
    wh_prefix: str,
    aws_region: str,
    aws_key_id: str | None,
    aws_secret: str | None,
    aws_token: str | None,
    batch_size: int = 50,
) -> CompactResult:
    """
    Dask worker: merge all files for one (cell, year) into one compacted file.
    Runs entirely on the worker — no data crosses the network to the scheduler.

    Pass-1 skip: if none of the input keys are ``part_*`` files the group's
    original inputs were already consumed by a prior (possibly aborted) run
    and the existing ``tmp_*`` / ``compacted_*`` files are already pass-1
    output.  We jump straight to pass 2, saving substantial S3 egress and
    CPU — important for re-runs after partial failures on heavy groups.
    """

    import obstore as _obs
    import pyarrow as pa
    from obstore.store import S3Store as _S3

    sk: dict = dict(bucket=wh_bucket, region=aws_region)
    if aws_key_id:
        sk["aws_access_key_id"] = aws_key_id
    if aws_secret:
        sk["aws_secret_access_key"] = aws_secret
    if aws_token:
        sk["aws_session_token"] = aws_token
    _store = _S3(**sk)

    year_str = year
    prefix = f"{wh_prefix}grid_partition={cell}/year={year_str}/"
    out_key = f"{prefix}compacted_{uuid.uuid4().hex[:8]}.parquet"

    # Re-scan the prefix so retries (after a worker crash mid-task) always work
    # from the *current* S3 state rather than the stale list passed in.
    live_keys: list[str] = []
    for _batch in _obs.list(_store, prefix=prefix):
        for _obj in _batch:
            k: str = _obj["path"]
            if k.endswith(".parquet"):
                live_keys.append(k)
    keys = live_keys  # shadow the caller-supplied list with the live one

    # Single-file passthrough: rename part_ → compacted_ if needed, else no-op
    if len(keys) == 1 and keys[0].rsplit("/", 1)[-1].startswith("compacted_"):
        pf = pq.ParquetFile(io.BytesIO(bytes(_obs.get(_store, keys[0]).bytes())))
        meta = pf.metadata
        return CompactResult(
            cell=cell,
            year=year_str,
            input_files=1,
            input_rows=meta.num_rows,
            output_rows=meta.num_rows,
            duplicates_removed=0,
            file_size_bytes=0,
            out_key=keys[0],
            skipped=True,
        )
    if len(keys) == 1:
        # part_ single file — rename to compacted_
        raw = bytes(_obs.get(_store, keys[0]).bytes())
        _obs.put(_store, out_key, raw)
        _obs.delete(_store, keys[0])
        meta = pq.ParquetFile(io.BytesIO(raw)).metadata
        return CompactResult(
            cell=cell,
            year=year_str,
            input_files=1,
            input_rows=meta.num_rows,
            output_rows=meta.num_rows,
            duplicates_removed=0,
            file_size_bytes=len(raw),
            out_key=out_key,
            skipped=False,
        )
    if not keys:
        return CompactResult(
            cell=cell,
            year=year_str,
            input_files=0,
            input_rows=0,
            output_rows=0,
            duplicates_removed=0,
            file_size_bytes=0,
            out_key="",
            skipped=True,
        )

    # ---------------------------------------------------------------------------
    # Helpers (inner scope so they close over _store)
    # ---------------------------------------------------------------------------

    def _read_and_cast(key: str) -> pa.Table | None:
        """Download one parquet file and cast string→large_string.  Returns None on 404."""
        try:
            raw = bytes(_obs.get(_store, key).bytes())
        except Exception as exc:
            if "404" in str(exc) or "Not Found" in str(exc) or "NoSuchKey" in str(exc):
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
        """Keep most-recent updated per id."""
        sort_idx = pc.sort_indices(
            tbl,
            sort_keys=[("id", "ascending"), ("updated", "descending")],
            null_placement="at_end",
        )
        tbl = tbl.take(sort_idx)
        id_col = tbl.column("id").to_pylist()
        seen: set = set()
        keep: list[int] = []
        for i, id_val in enumerate(id_col):
            if id_val not in seen:
                seen.add(id_val)
                keep.append(i)
        return tbl.take(pa.array(keep, type=pa.int64()))

    def _write_tmp(tbl: pa.Table, key: str) -> None:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            pq.write_table(tbl, tmp_path, compression="snappy", row_group_size=64 * 1024)
            _obs.put(_store, key, open(tmp_path, "rb").read())
        finally:
            import os as _os

            _os.unlink(tmp_path)

    # ---------------------------------------------------------------------------
    # Multi-file: streaming merge in batches to bound peak memory
    # ---------------------------------------------------------------------------
    # Pass 1: compact input files in batches of BATCH_SIZE → temp files on S3.
    # Pass 2: merge all temp files into final compacted output.
    # This keeps peak memory ≤ BATCH_SIZE × avg_file_size regardless of N.
    # Dedup correctness: per-batch dedup + final dedup = global dedup.
    #
    # Pass-1 skip: if the group has no part_* files its originals were already
    # consumed by a prior (aborted) run.  The existing tmp_* / compacted_*
    # files ARE the pass-1 output — go straight to pass 2.

    BATCH_SIZE = batch_size
    not_found = 0
    input_rows = 0

    has_part_files = any(k.rsplit("/", 1)[-1].startswith("part_") for k in keys)

    if has_part_files:
        # When part_* files exist, old tmp_*/compacted_* are redundant — their
        # source data is still the part files from a prior failed run.  Only
        # process the part_* files to avoid re-merging already-merged data and
        # exploding the file count on repeated re-runs.
        pass1_input = [k for k in keys if k.rsplit("/", 1)[-1].startswith("part_")]
        tmp_keys: list[str] = []
        batches = [pass1_input[i : i + BATCH_SIZE] for i in range(0, len(pass1_input), BATCH_SIZE)]
        for batch in batches:
            tables: list[pa.Table] = []
            for key in batch:
                tbl = _read_and_cast(key)
                if tbl is None:
                    not_found += 1
                    continue
                tables.append(tbl)
            if not tables:
                continue
            batch_tbl = pa.concat_tables(tables, promote_options="none")
            input_rows += batch_tbl.num_rows
            batch_tbl = _dedup(batch_tbl)
            # Write as a temp file; cleaned up after final merge
            tmp_key = f"{prefix}tmp_{uuid.uuid4().hex[:12]}.parquet"
            _write_tmp(batch_tbl, tmp_key)
            tmp_keys.append(tmp_key)
    else:
        # Pass-1 skip: treat existing files as already-batched intermediates
        tmp_keys = list(keys)

    if not tmp_keys:
        # Every input file was a 404 — nothing to do
        return CompactResult(
            cell=cell,
            year=year_str,
            input_files=len(keys),
            input_rows=0,
            output_rows=0,
            duplicates_removed=0,
            file_size_bytes=0,
            out_key="",
            skipped=True,
        )

    # ---------------------------------------------------------------------------
    # Multi-level merge: keep batching until ≤ BATCH_SIZE files remain.
    #
    # Each level writes new tmp files BEFORE deleting the previous level's
    # files, so a crash mid-level always leaves enough data for the next run
    # to resume correctly (pass-1 skip will pick up whatever tmp_* survive).
    # ---------------------------------------------------------------------------
    while len(tmp_keys) > BATCH_SIZE:
        next_round: list[str] = []
        for i in range(0, len(tmp_keys), BATCH_SIZE):
            batch = tmp_keys[i : i + BATCH_SIZE]
            tables = [t for k in batch if (t := _read_and_cast(k)) is not None]
            if not tables:
                continue
            btbl = pa.concat_tables(tables, promote_options="none")
            btbl = _dedup(btbl)
            rnd_key = f"{prefix}tmp_{uuid.uuid4().hex[:12]}.parquet"
            _write_tmp(btbl, rnd_key)
            next_round.append(rnd_key)
        # Delete this level's inputs only after the next level is fully written
        for k in tmp_keys:
            try:
                _obs.delete(_store, k)
            except Exception:
                pass
        tmp_keys = next_round

    # Final merge: guaranteed ≤ BATCH_SIZE files
    final_tables: list[pa.Table] = [t for k in tmp_keys if (t := _read_and_cast(k)) is not None]
    if not final_tables:
        return CompactResult(
            cell=cell,
            year=year_str,
            input_files=len(keys),
            input_rows=0,
            output_rows=0,
            duplicates_removed=0,
            file_size_bytes=0,
            out_key="",
            skipped=True,
        )

    merged = pa.concat_tables(final_tables, promote_options="none")
    merged = _dedup(merged)
    merged = merged.sort_by([("platform", "ascending"), ("datetime", "ascending")])

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(merged, tmp_path, compression="snappy", row_group_size=64 * 1024)
        data = open(tmp_path, "rb").read()
        _obs.put(_store, out_key, data)
    finally:
        import os as _os

        _os.unlink(tmp_path)

    # Delete original input keys + the final-round tmp files.
    # (Intermediate-level tmp files were already deleted inside the loop above.)
    for key in set(keys) | set(tmp_keys):
        try:
            _obs.delete(_store, key)
        except Exception:
            pass

    # When pass 1 was skipped we don't have a true input_rows count (the
    # originals were consumed by a prior run).  Use output_rows for both so
    # stats stay consistent (duplicates_removed = 0 is correct — dedup across
    # already-batched intermediates removes only inter-intermediate overlap).
    effective_input_rows = input_rows if has_part_files else merged.num_rows

    return CompactResult(
        cell=cell,
        year=year_str,
        input_files=len(keys),
        input_rows=effective_input_rows,
        output_rows=merged.num_rows,
        duplicates_removed=effective_input_rows - merged.num_rows,
        file_size_bytes=len(data),
        out_key=out_key,
        skipped=False,
    )


# Build Dask delayed tasks — one per (cell, year) group
aws_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_token = os.environ.get("AWS_SESSION_TOKEN")

all_groups = list(groups.items())  # [((cell, year), [keys...]), ...]
tasks = [
    dask.delayed(_compact_cell_year)(
        cell,
        year,
        keys,
        warehouse_bucket,
        warehouse_prefix,
        region,
        aws_key_id,
        aws_secret,
        aws_token,
        args.batch_size,
    )
    for (cell, year), keys in all_groups
]

print(f"Submitting {len(tasks):,} Dask tasks …")

if args.coiled:
    import coiled
    from dask.distributed import as_completed

    cluster = coiled.Cluster(
        n_workers=args.coiled_workers,
        worker_vm_types=args.coiled_worker_vm_types,
        region=region,
        spot_policy="spot_with_fallback",
        name="earthcatalog-final-compact",
    )
    client = cluster.get_client()
    print(f"Coiled cluster ready: {client.dashboard_link}")

    futures = client.compute(tasks)
    results: list[CompactResult] = []
    errors = 0
    for i, future in enumerate(as_completed(futures), 1):
        try:
            results.append(future.result())
        except Exception as exc:
            errors += 1
            print(f"  ERROR ({errors}): {exc}")
        if i % 50 == 0 or i == len(tasks):
            done_rows = sum(r.output_rows for r in results)
            print(
                f"  [{i:>5}/{len(tasks):>5}]  completed  {done_rows:>12,} rows so far  errors={errors}"
            )
else:
    results: list[CompactResult] = dask.compute(
        *tasks, scheduler="synchronous" if len(tasks) < 20 else "threads"
    )

print(f"Compact phase done. {len(results):,} groups processed.")

# ---------------------------------------------------------------------------
# Phase 3 — Rebuild Iceberg catalog
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("Phase 3 — Rebuild Iceberg catalog")
print("=" * 64)

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

# Collect all post-compaction file URIs
final_uris = [
    f"s3://{warehouse_bucket}/{r.out_key}"
    for r in results
    if not r.skipped or r.out_key  # include skipped (already-compacted single files)
]

BATCH = 2_000
registered = 0
for i in tqdm(range(0, len(final_uris), BATCH), desc="Registering files"):
    table.add_files(file_paths=final_uris[i : i + BATCH])
    registered += len(final_uris[i : i + BATCH])

print(f"Registered {registered:,} files into {output_path}")

if args.upload:
    no_scheme = args.catalog_s3.removeprefix("s3://")
    cat_bucket, cat_key = no_scheme.split("/", 1)
    cat_store = _make_store(cat_bucket)
    obstore.put(cat_store, cat_key, output_path.read_bytes())
    print(f"Uploaded → {args.catalog_s3}")

# ---------------------------------------------------------------------------
# Phase 4 — Stats report
# ---------------------------------------------------------------------------

print()
print("=" * 64)
print("Phase 4 — Stats")
print("=" * 64)

total_input_rows = sum(r.input_rows for r in results)
total_output_rows = sum(r.output_rows for r in results)
total_duplicates = sum(r.duplicates_removed for r in results)
total_size_gb = sum(r.file_size_bytes for r in results if not r.skipped) / 1e9
pct_dupes = 100 * total_duplicates / total_input_rows if total_input_rows else 0

print(f"  Total input rows          : {total_input_rows:>14,}")
print(f"  Total unique rows (output): {total_output_rows:>14,}")
print(f"  Duplicate rows removed    : {total_duplicates:>14,}  ({pct_dupes:.2f}%)")
print(f"  Compacted data written    : {total_size_gb:>14.2f} GB")
print(f"  (cell, year) groups       : {len(results):>14,}")

# Per-year breakdown
print()
print("  Per-year breakdown:")
print(f"  {'Year':>6}  {'Groups':>8}  {'Unique items':>14}  {'Duplicates':>12}  {'Dup %':>7}")
print("  " + "-" * 56)
year_stats: dict[str, dict] = defaultdict(lambda: {"groups": 0, "output": 0, "dupes": 0})
for r in results:
    year_stats[r.year]["groups"] += 1
    year_stats[r.year]["output"] += r.output_rows
    year_stats[r.year]["dupes"] += r.duplicates_removed

for year in sorted(year_stats):
    ys = year_stats[year]
    input_y = ys["output"] + ys["dupes"]
    pct = 100 * ys["dupes"] / input_y if input_y else 0
    print(f"  {year:>6}  {ys['groups']:>8,}  {ys['output']:>14,}  {ys['dupes']:>12,}  {pct:>6.2f}%")

# Top-N cells by unique item count
print()
print(f"  Top {args.stats_top_n} cells by unique item count:")
print(f"  {'Cell':>20}  {'Years':>6}  {'Unique items':>14}  {'Dup %':>7}")
print("  " + "-" * 54)
cell_stats: dict[str, dict] = defaultdict(lambda: {"years": 0, "output": 0, "input": 0})
for r in results:
    cell_stats[r.cell]["years"] += 1
    cell_stats[r.cell]["output"] += r.output_rows
    cell_stats[r.cell]["input"] += r.input_rows

top_cells = sorted(cell_stats.items(), key=lambda x: x[1]["output"], reverse=True)[
    : args.stats_top_n
]
for cell, cs in top_cells:
    pct = 100 * (cs["input"] - cs["output"]) / cs["input"] if cs["input"] else 0
    print(f"  {cell:>20}  {cs['years']:>6,}  {cs['output']:>14,}  {pct:>6.2f}%")

# Distribution of unique items per (cell, year)
print()
print("  Items-per-(cell,year) distribution:")
import math

buckets_dist: dict[str, int] = defaultdict(int)
for r in results:
    if r.output_rows == 0:
        label = "0"
    else:
        mag = int(math.log10(r.output_rows))
        lo = 10**mag
        hi = 10 ** (mag + 1)
        label = f"{lo:,}–{hi:,}"
    buckets_dist[label] += 1

for label in sorted(
    buckets_dist, key=lambda x: int(x.split("–")[0].replace(",", "")) if "–" in x else 0
):
    bar = "█" * min(40, buckets_dist[label] // max(1, len(results) // 40))
    print(f"  {label:>14}  {buckets_dist[label]:>8,}  {bar}")

# Save stats as JSON for further analysis
import json

stats_path = Path(args.output).with_suffix(".stats.json")
stats_path.write_text(
    json.dumps(
        {
            "summary": {
                "total_input_rows": total_input_rows,
                "total_unique_rows": total_output_rows,
                "total_duplicates_removed": total_duplicates,
                "duplicate_pct": round(pct_dupes, 4),
                "total_groups": len(results),
                "compacted_size_gb": round(total_size_gb, 3),
            },
            "per_year": {
                y: {
                    "groups": s["groups"],
                    "unique_items": s["output"],
                    "duplicates": s["dupes"],
                }
                for y, s in sorted(year_stats.items())
            },
            "top_cells": [
                {
                    "cell": cell,
                    "years": cs["years"],
                    "unique_items": cs["output"],
                    "duplicate_pct": round(100 * (cs["input"] - cs["output"]) / cs["input"], 4)
                    if cs["input"]
                    else 0,
                }
                for cell, cs in top_cells
            ],
        },
        indent=2,
    )
)
print(f"\n  Full stats saved → {stats_path}")
print("=" * 64)
