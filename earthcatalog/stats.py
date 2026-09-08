"""Catalog stats snapshot — one small JSON at the top of the catalog.

``stats.json`` (a sibling of ``earthcatalog.db``) holds everything
``earthcatalog info`` prints, so reading stats never downloads data.
Every mutating task (ingest, GC, consolidation) refreshes it via
:func:`refresh_after` at the same moment the catalog db is uploaded.
Two kinds of numbers live in it:

* **maintained counters** — ``unique_items``, ``index_rows``,
  ``deleted_rows``, ``items_per_day`` — facts that only exist in the
  index column data, advanced incrementally by each task's delta;
* **recomputed fields** — warehouse rows/files/bytes/cells/years/hot
  locations — always rebuilt from Iceberg metadata on refresh, so they
  can never go stale.

``earthcatalog info --verify`` forces the full recomputation
(:func:`compute_full`) and reports drift against the stored snapshot.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import obstore
from obstore.store import ObjectStore

from .schema import PROP_TIME_BIN, partition_year

STATS_VERSION = 1


def stats_key_for(warehouse: str) -> str:
    """Store key for stats.json — the top of the catalog, beside
    ``earthcatalog.db``: ``s3://bucket/prefix/warehouse`` → ``prefix/stats.json``.
    """
    if warehouse.startswith("s3://"):
        bucket, _, path = warehouse.removeprefix("s3://").partition("/")
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        key = f"{parent}/stats.json" if parent else "stats.json"
        return f"s3://{bucket}/{key}"
    from pathlib import Path

    return str(Path(warehouse).parent / "stats.json")


def _store_key(key: str) -> str:
    """Store-relative object key: callers pass ``stats_key_for`` output
    (an ``s3://`` URI for remote warehouses); bucket-level stores need the
    scheme and bucket stripped."""
    if key.startswith("s3://"):
        return key.removeprefix("s3://").partition("/")[2]
    return key


def load(store: ObjectStore, key: str) -> dict[str, Any] | None:
    """Read the stored snapshot; ``None`` when absent or unreadable."""
    try:
        doc = json.loads(bytes(obstore.get(store, _store_key(key)).bytes()))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return doc if doc.get("stats_version") == STATS_VERSION else None


def save(store: ObjectStore, key: str, stats: dict[str, Any]) -> None:
    doc = {"stats_version": STATS_VERSION, **stats}
    obstore.put(store, _store_key(key), json.dumps(doc, indent=1).encode())


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def index_locations(table, store: ObjectStore, warehouse: str) -> list[str]:
    """Full URIs (or absolute paths) of every index part for *warehouse* —
    what the metadata-based and full-recompute passes read."""
    from pathlib import Path

    from .index import Index, resolve_index_path

    key = resolve_index_path(table, f"{warehouse.rstrip('/')}_index.parquet")
    if key.startswith("s3://"):
        bucket, _, rel = key.removeprefix("s3://").partition("/")
        return [f"s3://{bucket}/{loc}" for loc in Index(store, rel).locations()]
    if os.path.isabs(key):
        base = Path(key).parent
        return [str(base / loc) for loc in Index(store, os.path.basename(key)).locations()]
    return [key]


# ---------------------------------------------------------------------------
# maintainable counters
# ---------------------------------------------------------------------------


def apply_ingest(stats: dict[str, Any], *, new_items: int, rows: int) -> dict[str, Any]:
    """Ingest delta: new distinct source keys and their warehouse rows."""
    stats["unique_items"] = stats.get("unique_items", 0) + new_items
    stats["index_rows"] = stats.get("index_rows", 0) + rows
    stats["warehouse_rows"] = stats.get("warehouse_rows", 0) + rows
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    per_day = stats.setdefault("items_per_day", {})
    per_day[today] = per_day.get(today, 0) + new_items
    return stats


def apply_gc(stats: dict[str, Any], *, confirmed: int, rows_removed: int) -> dict[str, Any]:
    """GC delta: confirmed orphans leave the catalog."""
    stats["unique_items"] = max(0, stats.get("unique_items", 0) - confirmed)
    stats["deleted_rows"] = stats.get("deleted_rows", 0) + rows_removed
    stats["warehouse_rows"] = max(0, stats.get("warehouse_rows", 0) - rows_removed)
    return stats


def apply_consolidation(
    stats: dict[str, Any], *, rows_removed_dupes: int, files_saved: int
) -> dict[str, Any]:
    """Consolidation delta: dedupe drops rows, file count shrinks."""
    stats["warehouse_rows"] = max(0, stats.get("warehouse_rows", 0) - rows_removed_dupes)
    stats["warehouse_files"] = max(0, stats.get("warehouse_files", 0) - files_saved)
    return stats


# ---------------------------------------------------------------------------
# warehouse side — Iceberg metadata only, no data reads
# ---------------------------------------------------------------------------


def recompute_warehouse(stats: dict[str, Any], table) -> dict[str, Any]:
    """Refresh rows/files/bytes/cells/years/hot from manifest statistics."""
    time_bin = table.properties.get(PROP_TIME_BIN, "year")
    rows = files = size = 0
    cells: dict[str, int] = defaultdict(int)
    years: dict[int, int] = defaultdict(int)
    for task in table.scan().plan_files():
        f = task.file
        rows += f.record_count
        files += 1
        size += f.file_size_in_bytes
        cells[str(f.partition[0])] += f.record_count
        years[partition_year(time_bin, f.partition[1])] += f.record_count
    hot = sorted(cells.items(), key=lambda kv: -kv[1])[:5]
    stats.update(
        {
            "warehouse_rows": rows,
            "warehouse_files": files,
            "warehouse_bytes": size,
            "cells": dict(sorted(cells.items())),
            "years": {str(y): n for y, n in sorted(years.items())},
            "hot_locations": [{"grid_partition": c, "row_count": n} for c, n in hot],
        }
    )
    return stats


# ---------------------------------------------------------------------------
# full recomputation (bootstrap + --verify)
# ---------------------------------------------------------------------------


def compute_full(table, index, locations: list[str]) -> dict[str, Any]:
    """Expensive, exact: distinct keys / per-day from the index column data,
    the warehouse side from metadata.  *locations* are full URIs of every
    index part (the caller knows the bucket)."""
    import duckdb

    stats: dict[str, Any] = {
        "unique_items": 0,
        "index_rows": 0,
        "deleted_rows": 0,
        "items_per_day": {},
    }
    if locations:
        con = duckdb.connect()
        con.execute("INSTALL aws; LOAD aws; CALL load_aws_credentials();")
        con.execute("SET s3_region='us-west-2';")
        loc_list = ", ".join(f"'{loc}'" for loc in locations)

        def scalar(sql: str) -> int:
            row = con.execute(sql).fetchone()
            return int(row[0]) if row else 0

        stats["unique_items"] = scalar(
            f"SELECT count(DISTINCT s3_key) FROM read_parquet([{loc_list}])"
        )
        stats["index_rows"] = scalar(f"SELECT count(*) FROM read_parquet([{loc_list}])")
        stats["deleted_rows"] = scalar(
            f"SELECT count(*) FROM read_parquet([{loc_list}]) WHERE deleted"
        )
        per_day = con.execute(
            f"SELECT CAST(ingested_at AS DATE) AS day, count(*) "
            f"FROM read_parquet([{loc_list}]) GROUP BY day ORDER BY day"
        ).fetchall()
        stats["items_per_day"] = {str(day): int(n) for day, n in per_day}
    recompute_warehouse(stats, table)
    stats["computed_at"] = now_iso()
    return stats


def refresh_after(
    store: ObjectStore,
    key: str,
    table,
    index,
    locations: list[str],
    *,
    apply: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Load (or bootstrap via full recomputation) the snapshot, apply the
    task's delta, refresh the warehouse side from metadata, and save.
    Called at each task's durable commit moment."""
    stats = load(store, key)
    if stats is None:
        stats = compute_full(table, index, locations)
    elif apply is not None:
        stats = apply(stats)
    recompute_warehouse(stats, table)
    stats["computed_at"] = now_iso()
    save(store, key, stats)
    return stats
