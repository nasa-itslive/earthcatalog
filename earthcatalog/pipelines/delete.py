"""
Garbage collection pipeline for the earthcatalog warehouse.

Removes STAC items whose source S3 objects no longer exist in the S3
Inventory.  Runs weekly on a GitHub runner, so memory and CPU are
bounded:

1. Build a Bloom filter of every ``.stac.json`` key in the current S3
   Inventory (~80 MB, independent of inventory size).
2. Stream the source index; any ``s3_key`` definitely absent from the
   Bloom filter is a deletion candidate.
3. Confirm candidates with S3 HEAD requests (eliminates Bloom false
   positives).
4. For each confirmed orphan, find the GeoParquet file(s) that contain it
   (via ``grid_partition`` + ``year``), rewrite them without the orphaned
   rows, update the hash index, and mark the source index rows deleted.

Every step is idempotent and safe to retry: rewritten files use a
``gc_*`` key prefix, so a crashed run leaves no partial state and the
next run overwrites leftovers.

Public API
----------
run_garbage_collection(inventory_path, *, store, source_index_key, ...) -> dict
    Full weekly GC pipeline.

build_inventory_bloom(inventory_path, ...) -> ScalableBloomFilter
    Stream the S3 Inventory into a Bloom filter of current keys.

find_deletion_candidates(store, source_index_key, bloom) -> list[dict]
    Source-index rows whose ``s3_key`` is absent from the Bloom filter.

confirm_deletions(candidates, head_fn, concurrency) -> list[dict]
    Keep only candidates confirmed absent via HEAD.

rewrite_file_without_orphans(file_key, orphaned_ids, store) -> tuple[str, int]
    Rewrite one GeoParquet file to a new key, dropping orphaned rows.
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

from earthcatalog.hash_index import hash_id, read_hashes, write_hashes
from earthcatalog.inventory import _iter_inventory
from earthcatalog.source_index import mark_deleted, stream_active

_GC_FILE_RE = re.compile(r"(.*/)(gc_[0-9a-f]+\.parquet)$")

_DEFAULT_ERROR_RATE = 0.0001
_DEFAULT_CONCURRENCY = 64


# ---------------------------------------------------------------------------
# Phase 1 — Bloom filter of current inventory keys
# ---------------------------------------------------------------------------


def build_inventory_bloom(
    inventory_path: str,
    error_rate: float = _DEFAULT_ERROR_RATE,
) -> ScalableBloomFilter:
    """
    Stream the S3 Inventory at *inventory_path* into a Bloom filter.

    Every ``.stac.json`` object key (canonical ``s3://bucket/key`` form) is
    inserted.  Uses :class:`ScalableBloomFilter` so capacity grows
    automatically and the false-positive rate stays bounded regardless of
    inventory size.
    """
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


# ---------------------------------------------------------------------------
# Phase 2 — Candidate detection
# ---------------------------------------------------------------------------


def find_deletion_candidates(
    store: object,
    source_index_key: str,
    bloom: ScalableBloomFilter,
) -> list[dict]:
    """
    Return source-index rows whose ``s3_key`` is definitely absent from *bloom*.

    The Bloom filter has no false negatives, so every returned row is a
    genuine deletion candidate; the (rare) false positives are filtered
    out in :func:`confirm_deletions`.
    """
    candidates: list[dict] = []
    for row in stream_active(store, source_index_key):
        if row["s3_key"] not in bloom:
            candidates.append(row)
    print(f"Candidates: {len(candidates):,}")
    return candidates


# ---------------------------------------------------------------------------
# Phase 3 — HEAD confirmation
# ---------------------------------------------------------------------------


def _default_head_fn(s3_key: str) -> bool:
    """Return True if the object at *s3_key* exists in S3."""
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
    """
    Return only candidates whose S3 object is confirmed absent.

    *head_fn* is injectable for testing; by default it issues anonymous
    S3 HEAD requests.
    """
    if head_fn is None:
        head_fn = _default_head_fn
    if not candidates:
        return []

    confirmed: list[dict] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(head_fn, c["s3_key"]): c for c in candidates}
        for future in as_completed(futures):
            c = futures[future]
            exists = future.result()
            if not exists:
                confirmed.append(c)

    print(f"Confirmed orphans: {len(confirmed):,}")
    return confirmed


# ---------------------------------------------------------------------------
# Phase 4 — Rewrite affected files + update indices
# ---------------------------------------------------------------------------


def _list_partition_files(
    store: object,
    warehouse_prefix: str,
    cell: str,
    year: int | None,
) -> list[str]:
    """List GeoParquet keys in one (cell, year) partition, newest first.

    All ``.parquet`` files are included regardless of prefix (``part_*`` or
    ``gc_*``).  After a successful GC run the canonical file in the directory
    is the ``gc_*`` file written by that run; excluding it would make
    subsequent GC passes blind to items in previously-collected partitions.
    Leftover ``gc_*`` files from *crashed* runs are handled separately by
    :func:`cleanup_stale_gc_files` and do not need special-casing here.
    """
    year_str = str(year) if year is not None else "unknown"
    prefix = f"{warehouse_prefix}grid_partition={cell}/year={year_str}/"
    keys: list[str] = []
    for batch in obstore.list(store, prefix=prefix):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet"):
                keys.append(k)
    keys.sort(reverse=True)
    return keys


def rewrite_file_without_orphans(
    file_key: str,
    orphaned_ids: set[str],
    store: object,
) -> tuple[str, int]:
    """
    Rewrite the GeoParquet file at *file_key*, dropping *orphaned_ids*.

    Writes to a new ``gc_<hex>.parquet`` key in the same directory and
    returns ``(new_key, rows_written)``.  The original file is left in
    place — the caller decides whether to delete it.
    """
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
    """Group orphaned stac_ids by (grid_partition, year)."""
    by_partition: dict[tuple[str, int | None], set[str]] = defaultdict(set)
    for o in orphans:
        by_partition[(o["grid_partition"], o["year"])].add(o["stac_id"])
    return dict(by_partition)


def execute_cleanup(
    orphans: list[dict],
    *,
    store: object,
    warehouse_prefix: str = "",
    hash_index_key: str,
    source_index_key: str,
    dry_run: bool = False,
) -> dict:
    """
    Rewrite warehouse files to remove orphaned rows and update both indices.

    Returns a summary dict with ``orphaned``, ``files_rewritten``,
    ``rows_removed`` and ``partitions_affected``.
    """
    if not orphans:
        return {
            "orphaned": 0,
            "files_rewritten": 0,
            "rows_removed": 0,
            "partitions_affected": 0,
        }

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

    # 2. Drop old warehouse files (after all rewrites succeeded).
    for old_key in old_keys:
        try:
            obstore.delete(store, old_key)
        except Exception as exc:
            print(f"WARN: could not delete {old_key}: {exc}")

    # 3. Update the hash index (remove orphaned hashes).
    existing = read_hashes(store, hash_index_key)
    orphan_hashes = {hash_id(sid) for sid in orphaned_ids}
    remaining = {h for h in existing if h not in orphan_hashes}
    if len(remaining) != len(existing):
        write_hashes(remaining, store, hash_index_key)
        print(f"  hash index: {len(existing):,} -> {len(remaining):,}")

    # 4. Mark source-index rows deleted.
    n_marked = mark_deleted(orphaned_ids, store, source_index_key)
    print(f"  source index: marked {n_marked} rows deleted")

    return {
        "orphaned": len(orphaned_ids),
        "files_rewritten": files_rewritten,
        "rows_removed": rows_removed,
        "partitions_affected": len(by_partition),
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_garbage_collection(
    inventory_path: str,
    *,
    store: object,
    source_index_key: str,
    hash_index_key: str,
    warehouse_prefix: str = "",
    head_fn: Callable[[str], bool] | None = None,
    head_concurrency: int = _DEFAULT_CONCURRENCY,
    bloom_error_rate: float = _DEFAULT_ERROR_RATE,
    dry_run: bool = False,
) -> dict:
    """
    Weekly garbage collection: detect and remove orphaned STAC items.

    Parameters
    ----------
    inventory_path:
        Path or ``s3://`` URI to the current S3 Inventory.
    store:
        obstore-compatible store holding the source index, hash index, and
        warehouse GeoParquet files.
    source_index_key:
        Key of the source index within *store*.
    hash_index_key:
        Key of the hash index within *store*.
    warehouse_prefix:
        Key prefix within *store* where warehouse files live (e.g. ``""``
        for a store rooted at the warehouse, or ``"catalog/warehouse/"``).
    head_fn:
        Injectable ``s3_key -> bool`` existence check (tests only).
    head_concurrency:
        Max concurrent HEAD requests in Phase 3.
    bloom_error_rate:
        Target false-positive rate for the inventory Bloom filter.
    dry_run:
        When True, detect and report orphans but make no changes.

    Returns
    -------
    dict with keys ``candidates``, ``confirmed``, and the cleanup summary
    (``orphaned``, ``files_rewritten``, ``rows_removed``,
    ``partitions_affected``).
    """
    bloom = build_inventory_bloom(inventory_path, error_rate=bloom_error_rate)
    candidates = find_deletion_candidates(store, source_index_key, bloom)
    orphans = confirm_deletions(candidates, head_fn=head_fn, concurrency=head_concurrency)

    summary = execute_cleanup(
        orphans,
        store=store,
        warehouse_prefix=warehouse_prefix,
        hash_index_key=hash_index_key,
        source_index_key=source_index_key,
        dry_run=dry_run,
    )
    return {
        "candidates": len(candidates),
        "confirmed": len(orphans),
        **summary,
    }


def cleanup_stale_gc_files(store: object, warehouse_prefix: str = "") -> int:
    """
    Delete leftover ``gc_*.parquet`` files from crashed previous runs.

    These files are never registered in Iceberg and are safe to remove once
    the original (canonical) file they replaced has been deleted.
    """
    deleted = 0
    for batch in obstore.list(store, prefix=warehouse_prefix):
        for obj in batch:
            k: str = obj["path"]
            if _GC_FILE_RE.search(k):
                try:
                    obstore.delete(store, k)
                    deleted += 1
                except Exception:
                    pass
    return deleted
