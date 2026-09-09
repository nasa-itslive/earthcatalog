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
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import ObjectStore
from pyarrow.compute import (  # type: ignore[attr-defined]
    invert as pc_invert,
)
from pyarrow.compute import (  # type: ignore[attr-defined]
    is_in as pc_is_in,
)
from pybloom_live import ScalableBloomFilter

from earthcatalog.index import Index
from earthcatalog.inventory import _iter_inventory
from earthcatalog.schema import partition_prefix

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
    store: ObjectStore,
    warehouse_prefix: str,
    cell: str,
    year: int | None,
    layout: tuple[str, str, str] | None = None,
) -> list[str]:
    """List GeoParquet keys in one (cell, year) partition, newest first.

    When *layout* (``(grid, level, time_bin)`` from the table properties) is
    given, the schema-driven ``grid=/level=/tile=/{bin}=`` prefix is listed
    *in addition to* the legacy ``grid_partition=.../year=...`` one — a
    warehouse migrated mid-life can hold both layouts for the same partition.
    """
    year_str = str(year) if year is not None else "unknown"
    prefixes = [f"{warehouse_prefix}grid_partition={cell}/year={year_str}/"]
    if layout is not None:
        grid, level, time_bin = layout
        prefixes.append(partition_prefix(warehouse_prefix, grid, level, cell, time_bin, year_str))
    keys = []
    for prefix in prefixes:
        for batch in obstore.list(store, prefix=prefix):
            for obj in batch:
                k: str = obj["path"]
                if k.endswith(".parquet"):
                    keys.append(k)
    keys.sort(reverse=True)
    return keys


def rewrite_file_without_orphans(
    file_key: str, orphaned_ids: set[str], store: ObjectStore
) -> tuple[str, int]:
    """Rewrite the GeoParquet at *file_key*, dropping *orphaned_ids*, to ``gc_*``."""
    raw = bytes(obstore.get(store, file_key).bytes())
    tbl = pq.ParquetFile(io.BytesIO(raw)).read()
    id_col = tbl.column("id")
    mask = pc_invert(pc_is_in(id_col, pa.array(list(orphaned_ids), type=id_col.type)))
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


def _store_key_from_uri(uri: str) -> str:
    """Convert ``s3://bucket/key`` to the bucket-relative *key* (others pass through)."""
    if uri.startswith("s3://"):
        _, _, rest = uri.removeprefix("s3://").partition("/")
        return rest
    return uri


def iceberg_orphan_file_scan(table, batch_size: int = 5_000) -> Callable[[set[str]], set[str]]:
    """Build a discovery callable from an Iceberg table.

    The callable maps orphan ids to every data file that Iceberg metadata
    says *may* contain them (``plan_files`` with an ``In("id", ...)`` row
    filter — manifests and column stats only, no data reads).  This makes
    cleanup independent of index pointer coverage: Iceberg is the source of
    truth for where physical copies live.
    """
    from pyiceberg.expressions import In

    def discover(orphan_ids: set[str]) -> set[str]:
        found: set[str] = set()
        ordered = sorted(orphan_ids)
        for i in range(0, len(ordered), batch_size):
            chunk = ordered[i : i + batch_size]
            try:
                plan = table.scan(row_filter=In("id", chunk)).plan_files()  # type: ignore[misc,arg-type,call-arg]
            except Exception as exc:
                print(
                    f"WARN: Iceberg orphan discovery failed ({exc}); using index-derived files only"
                )
                return found
            for task in plan:
                found.add(_store_key_from_uri(task.file.file_path))
        return found

    return discover


def _file_ids(store: ObjectStore, file_key: str) -> set[str]:
    """Read the ``id`` column of one warehouse file, or ``set()`` if missing."""
    try:
        raw = bytes(obstore.get(store, file_key).bytes())
    except Exception:
        return set()
    return set(pq.ParquetFile(io.BytesIO(raw)).read().column("id").to_pylist())


def _rewrite_pass(
    store: ObjectStore,
    file_keys: set[str],
    remaining_ids: set[str],
    outside_keys: set[str],
    *,
    dry_run: bool,
) -> tuple[int, int, int, list[str], set[str]]:
    """Rewrite every file that still holds orphans; return progress counters.

    Returns ``(files_rewritten, rows_removed, copies_outside_index, old_keys,
    found_ids)`` — *found_ids* are the orphan ids actually located in these
    files, whether or not anything was written.  In dry-run mode
    *files_rewritten* counts files that *would* be rewritten.
    """
    files_rewritten = 0
    rows_removed = 0
    copies_outside = 0
    old_keys: list[str] = []
    found_ids: set[str] = set()
    for file_key in sorted(file_keys):
        present = _file_ids(store, file_key) & remaining_ids
        if not present:
            continue
        found_ids |= present
        rows_removed += len(present)
        if file_key in outside_keys:
            copies_outside += len(present)
        files_rewritten += 1
        if dry_run:
            print(f"  [dry-run] would rewrite {file_key}: {len(present)} rows")
            continue
        new_key, _ = rewrite_file_without_orphans(file_key, present, store)
        print(f"  rewrote {file_key} -> {new_key}: removed {len(present)} orphaned rows")
        old_keys.append(file_key)
    return files_rewritten, rows_removed, copies_outside, old_keys, found_ids


_MAX_GC_PASSES = 3


def execute_cleanup(
    orphans: list[dict],
    *,
    store: ObjectStore,
    index: Index,
    warehouse_prefix: str = "",
    dry_run: bool = False,
    layout: tuple[str, str, str] | None = None,
    discover_fn: Callable[[set[str]], set[str]] | None = None,
) -> dict:
    """Rewrite warehouse files to drop orphans and mark them deleted in the index.

    Cleanup targets are the index-derived partition files *unioned* with the
    files found by *discover_fn* (Iceberg metadata) — copies in cells the
    index never pointed at are cleaned too.  When *discover_fn* is given the
    run iterates to a fixpoint and raises if orphan copies survive
    ``_MAX_GC_PASSES`` passes; ``residual_copies`` in the summary is the
    hard guarantee (0).
    """
    if not orphans:
        return {
            "orphaned": 0,
            "files_rewritten": 0,
            "rows_removed": 0,
            "partitions_affected": 0,
            "copies_outside_index": 0,
            "residual_copies": 0,
        }

    by_partition = _orphans_by_partition(orphans)
    orphaned_ids = {o["stac_id"] for o in orphans}

    index_files: set[str] = set()
    for cell, year in by_partition:
        index_files.update(
            _list_partition_files(store, warehouse_prefix, cell, year, layout=layout)
        )
    discovered: set[str] = set()
    if discover_fn is not None:
        discovered = discover_fn(orphaned_ids)
    outside_keys = discovered - index_files

    remaining_ids = set(orphaned_ids)
    files_rewritten = 0
    rows_removed = 0
    copies_outside_index = 0
    old_keys: list[str] = []

    for _pass in range(_MAX_GC_PASSES):
        dirty = (index_files | discovered) if _pass == 0 else discovered
        f_rewritten, f_removed, f_outside, f_old, found = _rewrite_pass(
            store, dirty, remaining_ids, outside_keys, dry_run=dry_run
        )
        files_rewritten += f_rewritten
        rows_removed += f_removed
        copies_outside_index += f_outside
        old_keys.extend(f_old)
        for file_key in f_old:
            try:
                obstore.delete(store, file_key)
            except Exception as exc:
                print(f"WARN: could not delete {file_key}: {exc}")
        remaining_ids -= found
        if dry_run or discover_fn is None or not remaining_ids:
            break
        discovered = discover_fn(remaining_ids)

    residual_copies = len(remaining_ids)
    if not dry_run and discover_fn is not None and residual_copies:
        raise RuntimeError(
            f"GC did not converge: {residual_copies} orphan copies survive "
            f"{_MAX_GC_PASSES} passes — refusing to mark them deleted in the index"
        )

    if not dry_run:
        index.mark_deleted(orphaned_ids)

    return {
        "orphaned": len(orphaned_ids),
        "files_rewritten": files_rewritten,
        "rows_removed": rows_removed,
        "partitions_affected": len(by_partition),
        "copies_outside_index": copies_outside_index,
        "residual_copies": residual_copies,
    }


def run_garbage_collection(
    inventory_path: str,
    *,
    store: ObjectStore,
    index: Index,
    warehouse_prefix: str = "",
    head_fn: Callable[[str], bool] | None = None,
    head_concurrency: int = _DEFAULT_CONCURRENCY,
    bloom_error_rate: float = _DEFAULT_ERROR_RATE,
    dry_run: bool = False,
    layout: tuple[str, str, str] | None = None,
    discover_fn: Callable[[set[str]], set[str]] | None = None,
) -> dict:
    """Run the full GC cycle against a unified Index.

    *discover_fn* (see :func:`iceberg_orphan_file_scan`) widens cleanup to
    every file Iceberg metadata associates with the orphans and enables the
    iterate-to-fixpoint guarantee.
    """
    bloom = build_inventory_bloom(inventory_path, error_rate=bloom_error_rate)
    candidates = find_deletion_candidates(index, bloom)
    orphans = confirm_deletions(candidates, head_fn=head_fn, concurrency=head_concurrency)
    summary = execute_cleanup(
        orphans,
        store=store,
        index=index,
        warehouse_prefix=warehouse_prefix,
        dry_run=dry_run,
        layout=layout,
        discover_fn=discover_fn,
    )
    return {"candidates": len(candidates), "confirmed": len(orphans), **summary}
