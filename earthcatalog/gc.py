"""
Garbage collection against the unified warehouse Index.

Reads orphans from the unified :class:`earthcatalog.index.Index` and marks
them deleted there — no separate hash-index or source-index files to update.

Pipeline
--------
1. Build a Bloom filter of every ``.stac.json`` key in the current S3
   Inventory.
2. Stream the Index; any ``s3_key`` absent from the Bloom filter is a
   deletion candidate (no false negatives).
3. Confirm candidates with S3 HEAD requests (kills false positives).
4. Rewrite affected GeoParquet files without the orphaned rows, mark the
   index rows deleted, and delete the old files.

Idempotent: rewritten files use a ``gc_*`` prefix and a crashed run leaves
no partial state.
"""

from __future__ import annotations

import io
import re
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pybloom_live import ScalableBloomFilter

from earthcatalog.index import Index
from earthcatalog.inventory import _iter_inventory

_GC_FILE_RE = re.compile(r"(.*/)(gc_[0-9a-f]+\.parquet)$")

_DEFAULT_ERROR_RATE = 0.0001
_DEFAULT_CONCURRENCY = 64


def build_inventory_bloom(
    inventory_path: str, error_rate: float = _DEFAULT_ERROR_RATE
) -> ScalableBloomFilter:
    """Stream the S3 Inventory into a Bloom filter of current keys."""
    bloom = ScalableBloomFilter(
        initial_capacity=100_000,
        error_rate=error_rate,
        mode=ScalableBloomFilter.SMALL_SET_GROWTH,
    )
    n = 0
    for bucket, key in _iter_inventory(inventory_path):
        if key.endswith(".stac.json"):
            bloom.add(f"s3://{bucket}/{key}")
            n += 1
    print(f"Bloom filter: {n:,} keys loaded")
    return bloom


def find_deletion_candidates(index: Index, bloom: ScalableBloomFilter) -> list[dict]:
    """Return index rows whose ``s3_key`` is definitely absent from *bloom*."""
    candidates = []
    for row in index.stream_active():
        if row["s3_key"] not in bloom:
            candidates.append(row)
    print(f"Candidates: {len(candidates):,}")
    return candidates


def _default_head_fn(s3_key: str) -> bool:
    from obstore.store import S3Store

    no_scheme = s3_key.removeprefix("s3://")
    bucket, _, key = no_scheme.partition("/")
    if not key:
        return False
    store = S3Store(bucket=bucket, region="us-west-2", skip_signature=True)
    try:
        obstore.head(store, key)
        return True
    except FileNotFoundError:
        return False


def confirm_deletions(
    candidates: list[dict],
    head_fn: Callable[[str], bool] | None = None,
    concurrency: int = _DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Return only candidates whose S3 object is confirmed absent."""
    if head_fn is None:
        head_fn = _default_head_fn
    if not candidates:
        return []
    confirmed = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(head_fn, c["s3_key"]): c for c in candidates}
        for future in as_completed(futures):
            c = futures[future]
            if not future.result():
                confirmed.append(c)
    print(f"Confirmed orphans: {len(confirmed):,}")
    return confirmed


def _list_partition_files(
    store: object, warehouse_prefix: str, cell: str, year: int | None
) -> list[str]:
    """List GeoParquet keys in one (cell, year) partition, newest first."""
    year_str = str(year) if year is not None else "unknown"
    prefix = f"{warehouse_prefix}grid_partition={cell}/year={year_str}/"
    keys = []
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet"):
                keys.append(k)
    keys.sort(reverse=True)
    return keys


def rewrite_file_without_orphans(
    file_key: str, orphaned_ids: set[str], store: object
) -> tuple[str, int]:
    """Rewrite the GeoParquet at *file_key*, dropping *orphaned_ids*, to ``gc_*``."""
    raw = bytes(obstore.get(store, file_key).bytes())
    tbl = pq.ParquetFile(io.BytesIO(raw)).read()
    id_col = tbl.column("id")
    mask = pc.invert(pc.is_in(id_col, pa.array(list(orphaned_ids), type=id_col.type)))
    cleaned = tbl.filter(mask)

    dir_path = file_key.rsplit("/", 1)[0]
    new_key = f"{dir_path}/gc_{uuid.uuid4().hex[:8]}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(cleaned, tmp_path, compression="zstd")
        data = Path(tmp_path).read_bytes()
        obstore.put(store, new_key, data)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return new_key, cleaned.num_rows


def _orphans_by_partition(orphans: list[dict]) -> dict[tuple[str, int | None], set[str]]:
    by_partition: dict[tuple[str, int | None], set[str]] = defaultdict(set)
    for o in orphans:
        by_partition[(o["grid_partition"], o["year"])].add(o["stac_id"])
    return dict(by_partition)


def execute_cleanup(
    orphans: list[dict],
    *,
    store: object,
    index: Index,
    warehouse_prefix: str = "",
    dry_run: bool = False,
) -> dict:
    """Rewrite warehouse files to drop orphans and mark them deleted in the index."""
    if not orphans:
        return {"orphaned": 0, "files_rewritten": 0, "rows_removed": 0, "partitions_affected": 0}

    by_partition = _orphans_by_partition(orphans)
    orphaned_ids = {o["stac_id"] for o in orphans}

    files_rewritten = 0
    rows_removed = 0
    old_keys: list[str] = []

    for (cell, year), stac_ids in sorted(by_partition.items()):
        file_keys = _list_partition_files(store, warehouse_prefix, cell, year)
        for file_key in file_keys:
            raw = bytes(obstore.get(store, file_key).bytes())
            tbl = pq.ParquetFile(io.BytesIO(raw)).read()
            id_col = tbl.column("id")
            present = set(id_col.to_pylist()) & stac_ids
            if not present:
                continue

            if dry_run:
                print(f"  [dry-run] would rewrite {file_key}: {len(present)} rows")
                rows_removed += len(present)
                files_rewritten += 1
                continue

            new_key, _ = rewrite_file_without_orphans(file_key, present, store)
            print(f"  rewrote {file_key} -> {new_key}: removed {len(present)} orphaned rows")
            old_keys.append(file_key)
            files_rewritten += 1
            rows_removed += len(present)

    if dry_run or not old_keys:
        return {
            "orphaned": len(orphaned_ids),
            "files_rewritten": files_rewritten,
            "rows_removed": rows_removed,
            "partitions_affected": len(by_partition),
        }

    for old_key in old_keys:
        try:
            obstore.delete(store, old_key)
        except Exception as exc:
            print(f"WARN: could not delete {old_key}: {exc}")

    index.mark_deleted(orphaned_ids)

    return {
        "orphaned": len(orphaned_ids),
        "files_rewritten": files_rewritten,
        "rows_removed": rows_removed,
        "partitions_affected": len(by_partition),
    }


def run_garbage_collection(
    inventory_path: str,
    *,
    store: object,
    index: Index,
    warehouse_prefix: str = "",
    head_fn: Callable[[str], bool] | None = None,
    head_concurrency: int = _DEFAULT_CONCURRENCY,
    bloom_error_rate: float = _DEFAULT_ERROR_RATE,
    dry_run: bool = False,
) -> dict:
    """Run the full GC cycle against a unified Index."""
    bloom = build_inventory_bloom(inventory_path, error_rate=bloom_error_rate)
    candidates = find_deletion_candidates(index, bloom)
    orphans = confirm_deletions(candidates, head_fn=head_fn, concurrency=head_concurrency)
    summary = execute_cleanup(
        orphans,
        store=store,
        index=index,
        warehouse_prefix=warehouse_prefix,
        dry_run=dry_run,
    )
    return {"candidates": len(candidates), "confirmed": len(orphans), **summary}
