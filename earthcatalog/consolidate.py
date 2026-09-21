"""Consolidate small GeoParquet parts into fewer files per partition.

The daily ingest appends one small part per batch, so hot (tile, bin)
partitions accumulate many small files — slow prunes, lots of S3 requests.
Consolidation rewrites a partition into one file and replaces the old
entries with an atomic Iceberg transaction (append the new file, delete the
old entries), so search never sees the partition doubled or missing.  Old
objects are deleted only after the commit succeeds **and** the catalog
holding that commit has been published: :func:`run` flushes the (local)
catalog through ``on_flush`` before it unlinks anything, so a cancelled or
timed-out job can never leave the remote metadata pointing at deleted
objects.

Performance notes (why this is CI-friendly at the ~9k-file scale):

* Planning is metadata-only (``plan``): the audit report and the dry-run
  never read or write Parquet.
* Rewrites are **batched into one Iceberg transaction per flush** instead
  of one transaction per partition.  pyiceberg's per-partition
  ``add_files`` re-reads every manifest to check for duplicates and each
  partition commits two snapshots; batching turns O(partitions) manifest
  scans and snapshot deep-copies into O(flushes).  This is the dominant
  win at production scale.
* ``add_files(..., check_duplicate_files=False)`` — the new part name is
  freshly allocated, so the whole-table duplicate scan is pure overhead.
* The "which old objects are still referenced?" check is **one** manifest
  scan per flush, not one per partition.
* Source files are fetched with a bounded thread pool (obstore releases the
  GIL); parsing/dedupe/writing stays sequential so memory peaks at one
  partition's compressed bytes plus one decoded file.
* The next ``part_NNNNNN`` sequence number is derived from the directory
  listing already taken for the crash-window check — no extra LIST.
* Old objects are unlinked with a bounded thread pool after the commit is
  published, with per-key retries; failures are non-fatal orphans.
"""

from __future__ import annotations

import io
import re
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import ObjectStore

from .schema import partition_bin_value

_PART_SEQ = re.compile(r"part_(\d+)\.parquet$")
_SNAPSHOT_PROPS = {"earthcatalog.consolidated": "true"}

# Bounded pools: GitHub-hosted runners have 4 vCPUs and modest bandwidth.
# Fetching is network-bound (obstore releases the GIL); delete is too.
DEFAULT_FETCH_WORKERS = 8
DEFAULT_DELETE_WORKERS = 16
_DELETE_RETRIES = 3


@dataclass(frozen=True)
class PartitionPlan:
    """One (tile, bin value) partition worth of files, from metadata only."""

    tile: str
    bin_value: str
    files: tuple[str, ...]  # full URIs, as the table metadata stores them
    total_rows: int
    total_bytes: int

    @property
    def key(self) -> tuple[str, str]:
        return (self.tile, self.bin_value)


@dataclass
class _Rewrite:
    """One partition rewritten but not yet committed to Iceberg."""

    report: dict
    predicate: object
    new_uri: str | None
    old_uris: tuple[str, ...]


def plan(
    table,
    *,
    min_files: int = 4,
    limit_tiles: int | None = None,
    max_bytes: int = 512_000_000,
    max_rows: int = 5_000_000,
) -> list[PartitionPlan]:
    """Rank partitions by file count (metadata-only; never touches data).

    *min_files* is the consolidation trigger; *limit_tiles* caps how many
    partitions are returned (the biggest offenders first).  Partitions
    bigger than *max_bytes* or *max_rows* are skipped — a consolidation
    holds one partition's rows in memory, and the caps keep that bounded
    on a CI runner.  The ``unknown`` temporal bin is skipped too (its
    files carry no datetime to predicate on).
    """
    time_bin = table.properties.get("earthcatalog.time_bin", "year")
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for task in table.scan().plan_files():
        f = task.file
        groups[(str(f.partition[0]), partition_bin_value(time_bin, f.partition[1]))].append(f)

    plans: list[PartitionPlan] = []
    for (tile, bv), dfs in groups.items():
        if len(dfs) < min_files or bv == "unknown":
            continue
        total_bytes = sum(df.file_size_in_bytes for df in dfs)
        total_rows = sum(df.record_count for df in dfs)
        if total_bytes > max_bytes or total_rows > max_rows:
            continue
        paths = tuple(df.file_path for df in dfs)
        plans.append(
            PartitionPlan(
                tile=tile,
                bin_value=bv,
                files=paths,
                total_rows=total_rows,
                total_bytes=total_bytes,
            )
        )
    plans.sort(key=lambda p: (-len(p.files), p.total_bytes))
    return plans[:limit_tiles] if limit_tiles is not None else plans


def _next_seq(listed: Iterable[str]) -> int:
    """Next ``part_NNNNNN`` sequence from an already-taken directory listing."""
    seqs = [0]
    for path in listed:
        m = _PART_SEQ.search(path.rsplit("/", 1)[-1])
        if m:
            seqs.append(int(m.group(1)))
    return max(seqs) + 1


def _key(uri: str, warehouse_prefix: str) -> str:
    """Store-relative object key for a metadata URI.

    Metadata stores full URIs (``s3://bucket/<prefix>/warehouse/…`` or
    absolute local paths); obstore keys are bucket- or root-relative.  The
    warehouse prefix component anchors the two: everything from it onward
    is the key.
    """
    marker = warehouse_prefix.strip("/")
    return (marker + "/" + uri.rsplit(f"/{marker}/", 1)[1]).lstrip("/")


def _registered_paths(table) -> set[str]:
    """Every file path currently registered in the table (one manifest scan)."""
    try:
        inspect = table.inspect.files()
        col = inspect.column("file_path") if hasattr(inspect, "column") else inspect["file_path"]
        return {str(v).removeprefix("file:") for v in col.to_pylist()}
    except Exception:
        return {t.file.file_path for t in table.scan().plan_files()}


def _temporal_predicate(tile: str, bin_value: str):
    """BooleanExpression matching exactly the rows of one (tile, bin).

    Files of the partition hold only datetimes inside the bin (the bin is
    derived from the datetime), so the strict metrics evaluator can prove
    whole files match and drop them without a rewrite.
    """
    import datetime as dt

    from pyiceberg.expressions import (  # type: ignore[attr-defined]
        And,
        EqualTo,
        GreaterThanOrEqual,
        LessThan,
    )

    if bin_value == "unknown":
        raise ValueError("the unknown bin has no datetime range")
    if len(bin_value) == 4:  # year
        start = dt.datetime(int(bin_value), 1, 1, tzinfo=dt.UTC)
        end = start + dt.timedelta(days=366 if start.year % 4 == 0 else 365)
    elif len(bin_value) == 7:  # month
        y, m = int(bin_value[:4]), int(bin_value[5:7])
        start = dt.datetime(y, m, 1, tzinfo=dt.UTC)
        end = dt.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=dt.UTC)
    else:  # day
        y, m, d = (int(x) for x in bin_value.split("-"))
        start = dt.datetime(y, m, d, tzinfo=dt.UTC)
        end = start + dt.timedelta(days=1)
    # NB: pyiceberg's inline stubs describe the *bound* predicate
    # constructors; the runtime accepts plain strings and values (same as
    # catalog.file_paths) — hence the narrow ignores.
    return And(
        EqualTo("grid_partition", tile),  # type: ignore[misc,arg-type,call-arg]
        And(
            GreaterThanOrEqual("datetime", start),  # type: ignore[misc,arg-type,call-arg]
            LessThan("datetime", end),  # type: ignore[misc,arg-type,call-arg]
        ),
    )


def _fetch_bytes(store: ObjectStore, key: str) -> bytes:
    return bytes(obstore.get(store, key).bytes())


def _fetch_many(store: ObjectStore, keys: list[str], workers: int) -> list[bytes | None]:
    """Fetch *keys* concurrently (order-preserving); missing keys → ``None``."""

    def _get(key: str) -> bytes | None:
        try:
            return _fetch_bytes(store, key)
        except FileNotFoundError:
            return None

    if workers <= 1 or len(keys) <= 1:
        return [_get(k) for k in keys]
    with ThreadPoolExecutor(max_workers=min(workers, len(keys))) as pool:
        return list(pool.map(_get, keys))


def _delete_old_objects(
    store: ObjectStore,
    warehouse_prefix: str,
    entries: list[tuple[str, dict]],
    registered: set[str],
    *,
    workers: int = DEFAULT_DELETE_WORKERS,
) -> None:
    """Unlink old objects that the commit removed from the metadata.

    *entries* pairs each candidate URI with the report it belongs to.  A
    URI still present in *registered* is left alone (the predicate did not
    prove every row and the file survived).  Every other delete is retried;
    a persistent failure is counted as ``old_files_left`` — non-fatal, the
    object is merely an unreferenced orphan.
    """
    pending = [(u, rep) for u, rep in entries if u not in registered]
    if not pending:
        return

    def _delete(uri: str) -> str:
        key = _key(uri, warehouse_prefix)
        for attempt in range(_DELETE_RETRIES):
            try:
                obstore.delete(store, key)
                return "removed"
            except FileNotFoundError:
                return "missing"
            except Exception:
                if attempt == _DELETE_RETRIES - 1:
                    return "left"
                time.sleep(5 * (attempt + 1))
        return "left"  # pragma: no cover — loop always returns

    uris = [u for u, _ in pending]
    if workers <= 1 or len(uris) <= 1:
        outcomes = [_delete(u) for u in uris]
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(uris))) as pool:
            outcomes = list(pool.map(_delete, uris))

    for (_, rep), outcome in zip(pending, outcomes):
        if outcome == "left":
            rep["old_files_left"] += 1
        else:
            rep["old_files_deleted"] += 1


def _rewrite_partition(
    store: ObjectStore,
    table,
    plan: PartitionPlan,
    warehouse_prefix: str,
    *,
    dedupe: bool = True,
    fetch_workers: int = DEFAULT_FETCH_WORKERS,
) -> _Rewrite:
    """Rewrite *plan*'s files into one new part; do **not** commit yet.

    Reads every surviving file in the partition, (optionally) dedupes by
    item id, and writes ``part_{next:06d}.parquet`` beside them.  The
    Iceberg transaction that drops the old files and registers the new one
    is staged by :func:`_commit_batch`, so many partitions share one
    manifest rewrite.
    """
    first_key = _key(plan.files[0], warehouse_prefix)
    dir_key = first_key.rsplit("/", 1)[0]

    # One listing replaces N failed GETs: compare what physically exists
    # against what the metadata lists.  Missing files are the signature of
    # a crash window — the previous run's merged output may sit in the
    # directory, unregistered.  If so, adopt it; never merge from fewer
    # rows when the fuller file is right there.
    meta_keys = {_key(u, warehouse_prefix) for u in plan.files}
    listed = {
        obj["path"]
        for listing in obstore.list(store, prefix=dir_key)
        for obj in listing
        if obj["path"].endswith(".parquet")
    }
    already_missing = len(meta_keys - listed)
    orphan_key: str | None = None
    if already_missing:
        candidates = sorted(listed - meta_keys)
        orphan_key = candidates[0] if candidates else None

    new_uri: str | None = None
    new_key: str | None = None
    rows = 0
    removed_dupes = 0
    if orphan_key:
        new_key = orphan_key
        new_uri = plan.files[0].rsplit("/", 1)[0] + "/" + orphan_key.rsplit("/", 1)[1]
        rows = pq.ParquetFile(io.BytesIO(_fetch_bytes(store, new_key))).metadata.num_rows
    elif meta_keys & listed:
        # Fetch every surviving source blob concurrently, then stream one at
        # a time into the output writer: memory peaks at the partition's
        # compressed bytes plus a single decoded file, never at the decoded
        # whole partition (plan() caps partition sizes upstream).
        source_keys = [k for k in (_key(u, warehouse_prefix) for u in plan.files) if k in listed]
        blobs = _fetch_many(store, source_keys, fetch_workers)
        sink = io.BytesIO()
        writer: pq.ParquetWriter | None = None
        seen: set[str] = set()
        rows_in = 0
        for blob in blobs:
            if blob is None:  # vanished between LIST and GET — crash window
                continue
            tbl = pq.ParquetFile(io.BytesIO(blob)).read()
            rows_in += tbl.num_rows
            if dedupe:
                keep: list[bool] = []
                for item_id in tbl.column("id").to_pylist():
                    if item_id not in seen:
                        seen.add(item_id)
                        keep.append(True)
                    else:
                        keep.append(False)
                if not keep:
                    continue
                tbl = tbl.filter(pa.array(keep, type=pa.bool_()))
            if tbl.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(sink, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            rows += tbl.num_rows
        if writer is not None:
            writer.close()
            # Footer check: the output must hold exactly the streamed rows.
            written = pq.ParquetFile(io.BytesIO(sink.getvalue())).metadata.num_rows
            if written != rows:
                raise RuntimeError(f"consolidation verification failed for {dir_key}")
            removed_dupes = rows_in - rows
            seq = _next_seq(listed)
            new_key = f"{dir_key}/part_{seq:06d}.parquet"
            obstore.put(store, new_key, sink.getvalue())
            new_uri = plan.files[0].rsplit("/", 1)[0] + f"/part_{seq:06d}.parquet"
    # else: every listed object is gone and no orphan exists — a pure
    # phantom cleanup (predicate-delete only, nothing appended).

    report = {
        "tile": plan.tile,
        "bin_value": plan.bin_value,
        "files_before": len(plan.files),
        "files_after": 1 if new_uri else 0,
        "rows": rows,
        "rows_removed_dupes": removed_dupes,
        "old_files_deleted": 0,
        "old_files_left": 0,
        "already_missing": already_missing,
        "new_file": new_key,
    }
    return _Rewrite(
        report=report,
        predicate=_temporal_predicate(plan.tile, plan.bin_value),
        new_uri=new_uri,
        old_uris=plan.files,
    )


def _commit_batch(table, rewrites: list[_Rewrite]) -> None:
    """Commit many rewritten partitions in one Iceberg transaction.

    All predicates are OR-ed into a single delete producer (one manifest
    rewrite for the whole batch); all new parts are appended in one
    ``add_files`` with the duplicate scan disabled.  The delete producer is
    staged before the append — as the per-partition version required — so
    it computes against the old manifests.
    """
    if not rewrites:
        return
    new_uris = [r.new_uri for r in rewrites if r.new_uri]
    with table.transaction() as tx:
        deleter = tx.update_snapshot(_SNAPSHOT_PROPS).delete()
        for r in rewrites:
            deleter.delete_by_predicate(r.predicate)
        deleter.commit()
        if new_uris:
            tx.add_files(new_uris, check_duplicate_files=False)


def consolidate_partition(
    store: ObjectStore,
    table,
    plan: PartitionPlan,
    warehouse_prefix: str,
    *,
    dedupe: bool = True,
    delete_old: bool = True,
    fetch_workers: int = DEFAULT_FETCH_WORKERS,
) -> dict:
    """Rewrite and atomically replace a single partition (commit + delete).

    Convenience wrapper around :func:`_rewrite_partition` /
    :func:`_commit_batch` for callers outside :func:`run`.  ``run`` uses the
    batched path so that many partitions share one transaction.
    """
    rewrite = _rewrite_partition(
        store, table, plan, warehouse_prefix, dedupe=dedupe, fetch_workers=fetch_workers
    )
    _commit_batch(table, [rewrite])
    report = rewrite.report
    if delete_old:
        _delete_old_objects(
            store,
            warehouse_prefix,
            [(u, report) for u in rewrite.old_uris],
            _registered_paths(table),
        )
    else:
        report["_old_files"] = rewrite.old_uris
    return report


def run(
    store: ObjectStore,
    table,
    warehouse_prefix: str,
    *,
    min_files: int = 4,
    limit_tiles: int | None = None,
    max_bytes: int = 512_000_000,
    max_rows: int = 5_000_000,
    dry_run: bool = False,
    dedupe: bool = True,
    flush_every: int | None = None,
    on_flush: Callable[[], None] | None = None,
    on_report: Callable[[dict], None] | None = None,
    fetch_workers: int = DEFAULT_FETCH_WORKERS,
    delete_workers: int = DEFAULT_DELETE_WORKERS,
) -> list[dict]:
    """Plan (always) and consolidate (unless *dry_run*). Returns the reports.

    Partitions are rewritten and committed in batches of *flush_every*
    (one Iceberg transaction per batch).  When *on_flush* is given (the CLI
    passes a catalog upload) it is called after each batch's commit — and
    once more at the end — **before** that batch's old objects are
    unlinked.  A timeout therefore discards at most the last batch, and the
    remote catalog never references an object that has been deleted.
    """
    plans = plan(
        table,
        min_files=min_files,
        limit_tiles=limit_tiles,
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    if dry_run:
        return [
            {
                "tile": p.tile,
                "bin_value": p.bin_value,
                "files": len(p.files),
                "rows": p.total_rows,
                "bytes": p.total_bytes,
                "dry_run": True,
            }
            for p in plans
        ]

    reports: list[dict] = []
    batch: list[_Rewrite] = []
    pending: list[tuple[str, dict]] = []

    def flush() -> None:
        if not batch and not pending:
            return
        if batch:
            _commit_batch(table, batch)
            for rewrite in batch:
                pending.extend((uri, rewrite.report) for uri in rewrite.old_uris)
            batch.clear()
        if on_flush is not None:
            on_flush()
        if pending:
            _delete_old_objects(
                store,
                warehouse_prefix,
                pending,
                _registered_paths(table),
                workers=delete_workers,
            )
            pending.clear()

    for p in plans:
        rewrite = _rewrite_partition(
            store,
            table,
            p,
            warehouse_prefix,
            dedupe=dedupe,
            fetch_workers=fetch_workers,
        )
        batch.append(rewrite)
        reports.append(rewrite.report)
        if on_report is not None:
            on_report(rewrite.report)
        if flush_every and len(batch) >= flush_every:
            flush()

    flush()
    return reports
