"""Consolidate small GeoParquet parts into fewer files per partition.

The daily ingest appends one small part per batch, so hot (tile, bin)
partitions accumulate many small files — slow prunes, lots of S3 requests.
Consolidation rewrites a partition into one file and replaces the old
entries with a single atomic Iceberg commit (append the new file, delete
the old entries in the same transaction), so search never sees the
partition doubled or missing.  Old objects are deleted only after the
commit succeeds; a crash leaves the new file orphaned and harmless.

All planning is metadata-only (``plan``): the audit report and the dry-run
never read or write Parquet.
"""

from __future__ import annotations

import io
import re
from collections import defaultdict
from dataclasses import dataclass

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import ObjectStore

from .schema import partition_bin_value

_PART_SEQ = re.compile(r"part_(\d+)\.parquet$")


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


def plan(table, *, min_files: int = 4, limit_tiles: int | None = None) -> list[PartitionPlan]:
    """Rank partitions by file count (metadata-only; never touches data).

    *min_files* is the consolidation trigger; *limit_tiles* caps how many
    partitions are returned (the biggest offenders first).  The ``unknown``
    temporal bin is skipped — its files carry no datetime to predicate on.
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
        paths = tuple(df.file_path for df in dfs)
        plans.append(
            PartitionPlan(
                tile=tile,
                bin_value=bv,
                files=paths,
                total_rows=sum(df.record_count for df in dfs),
                total_bytes=sum(df.file_size_in_bytes for df in dfs),
            )
        )
    plans.sort(key=lambda p: (-len(p.files), p.total_bytes))
    return plans[:limit_tiles] if limit_tiles is not None else plans


def _next_seq(dir_key: str, store: ObjectStore) -> int:
    seqs = [0]
    for listing in obstore.list(store, prefix=dir_key):
        for obj in listing:
            m = _PART_SEQ.search(obj["path"].rsplit("/", 1)[-1])
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


def consolidate_partition(
    store: ObjectStore,
    table,
    plan: PartitionPlan,
    warehouse_prefix: str,
    *,
    dedupe: bool = True,
) -> dict:
    """Rewrite *plan*'s files into one and atomically replace them.

    Reads every file in the partition, (optionally) dedupes by item id,
    writes ``part_{next:06d}.parquet`` beside them, and commits ONE
    transaction that drops the old files (by partition predicate — whole
    files leave only when every row provably matches, so nothing is ever
    partially deleted) and appends the new file.  Old S3 objects are
    removed only after the commit confirms they left the metadata.

    Returns a stats dict; raises on any verification failure (the new file
    is removed and the table untouched).
    """
    first_key = _key(plan.files[0], warehouse_prefix)
    dir_key = first_key.rsplit("/", 1)[0]

    tables: list[pa.Table] = []
    already_missing = 0
    for uri in plan.files:
        try:
            raw = bytes(obstore.get(store, _key(uri, warehouse_prefix)).bytes())
        except FileNotFoundError:
            # A previous run may have gotten partway through this partition
            # before dying; what matters is whether its merged output exists.
            already_missing += 1
            continue
        tables.append(pq.ParquetFile(io.BytesIO(raw)).read())

    # Missing files are the signature of a crash window: the previous run's
    # merged output — which holds MORE than the surviving files — may sit in
    # the directory, unregistered.  If so, adopt it; never merge from fewer
    # rows when the fuller file is right there.
    orphan_key: str | None = None
    if already_missing:
        listed = {
            obj["path"]
            for listing in obstore.list(store, prefix=dir_key)
            for obj in listing
            if obj["path"].endswith(".parquet")
        }
        candidates = sorted(listed - {_key(u, warehouse_prefix) for u in plan.files})
        orphan_key = candidates[0] if candidates else None

    new_uri: str | None = None
    new_key: str | None = None
    rows = 0
    removed_dupes = 0
    if orphan_key:
        new_key = orphan_key
        new_uri = plan.files[0].rsplit("/", 1)[0] + "/" + orphan_key.rsplit("/", 1)[1]
        rows = pq.ParquetFile(
            io.BytesIO(bytes(obstore.get(store, new_key).bytes()))
        ).metadata.num_rows
    elif tables:
        merged = pa.concat_tables(tables)
        if dedupe and merged.num_rows:
            seen: set[str] = set()
            keep: list[bool] = []
            for item_id in merged.column("id").to_pylist():
                keep.append(item_id not in seen)
                seen.add(item_id)
            merged = merged.filter(pa.array(keep))
        rows = merged.num_rows

        seq = _next_seq(dir_key, store)
        new_key = f"{dir_key}/part_{seq:06d}.parquet"
        buf = io.BytesIO()
        pq.write_table(merged, buf, compression="zstd")
        data = buf.getvalue()
        obstore.put(store, new_key, data)
        if pq.ParquetFile(io.BytesIO(data)).metadata.num_rows != rows:
            obstore.delete(store, new_key)
            raise RuntimeError(f"consolidation verification failed for {dir_key}")
        new_uri = plan.files[0].rsplit("/", 1)[0] + f"/part_{seq:06d}.parquet"
        removed_dupes = sum(t.num_rows for t in tables) - rows
    # else: every listed object is gone and no orphan exists — a pure
    # phantom cleanup (predicate-delete only, nothing appended).

    # The delete producer must be created (and its parent snapshot pinned)
    # before the append is staged, so it computes against the old manifests.
    with table.transaction() as tx:
        deleter = tx.update_snapshot({"earthcatalog.consolidated": "true"}).delete()
        deleter.delete_by_predicate(_temporal_predicate(plan.tile, plan.bin_value))
        deleter.commit()
        if new_uri:
            tx.add_files([new_uri])

    # Drop only objects the commit actually removed from the metadata.  A
    # failed delete is never fatal: the object is orphaned but harmless
    # (unreferenced), so retry briefly and leave anything that still fails.
    remaining = {
        t.file.file_path for t in table.scan().plan_files() if t.file.partition[0] == plan.tile
    }
    removed = 0
    left = 0
    import time

    for uri in plan.files:
        if uri in remaining:
            continue
        for attempt in range(3):
            try:
                obstore.delete(store, _key(uri, warehouse_prefix))
                removed += 1
                break
            except FileNotFoundError:
                removed += 1
                break
            except Exception:
                if attempt == 2:
                    left += 1
                else:
                    time.sleep(5 * (attempt + 1))

    return {
        "tile": plan.tile,
        "bin_value": plan.bin_value,
        "files_before": len(plan.files),
        "files_after": 1 if new_uri else 0,
        "rows": rows,
        "rows_removed_dupes": removed_dupes,
        "old_files_deleted": removed,
        "old_files_left": left,
        "already_missing": already_missing,
        "new_file": new_key,
    }


def run(
    store: ObjectStore,
    table,
    warehouse_prefix: str,
    *,
    min_files: int = 4,
    limit_tiles: int | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Plan (always) and consolidate (unless *dry_run*). Returns the reports."""
    plans = plan(table, min_files=min_files, limit_tiles=limit_tiles)
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
    return [consolidate_partition(store, table, p, warehouse_prefix) for p in plans]
