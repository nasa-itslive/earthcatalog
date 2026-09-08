"""Stats snapshot: maintained counters, deltas, persistence."""

from __future__ import annotations

import json

from obstore.store import MemoryStore

from earthcatalog import stats as stats_mod
from earthcatalog.stats import (
    apply_consolidation,
    apply_gc,
    apply_ingest,
    load,
    stats_key_for,
)


class _FakeTable:
    properties = {"earthcatalog.time_bin": "year"}


def test_stats_key_for_s3():
    assert (
        stats_key_for("s3://its-live-data/test-space/stac/catalog/warehouse")
        == "s3://its-live-data/test-space/stac/catalog/stats.json"
    )


def test_ingest_delta_then_gc_delta():
    s: dict = {}
    s = apply_ingest(s, new_items=100, rows=130)
    assert s["unique_items"] == 100
    assert s["index_rows"] == 130
    assert sum(s["items_per_day"].values()) == 100

    s = apply_ingest(s, new_items=50, rows=60)
    assert s["unique_items"] == 150

    s = apply_gc(s, confirmed=10, rows_removed=12)
    assert s["unique_items"] == 140
    assert s["deleted_rows"] == 12
    assert s["warehouse_rows"] == 130 + 60 - 12


def test_consolidation_delta_shrinks_files_and_dupe_rows():
    s = apply_consolidation(
        {"warehouse_rows": 500, "warehouse_files": 20}, rows_removed_dupes=30, files_saved=8
    )
    assert s["warehouse_rows"] == 470
    assert s["warehouse_files"] == 12


def test_gc_never_goes_negative():
    s = apply_gc({"unique_items": 3, "warehouse_rows": 1}, confirmed=10, rows_removed=50)
    assert s["unique_items"] == 0
    assert s["warehouse_rows"] == 0


def test_load_save_roundtrip_and_corrupt_fallback():
    store = MemoryStore()
    key = "catalog/stats.json"
    stats = {"stats_version": stats_mod.STATS_VERSION, "unique_items": 5}
    store.put(key, json.dumps(stats).encode())
    assert load(store, key)["unique_items"] == 5

    store.put(key, b"{corrupt")
    assert load(store, key) is None

    store.put(key, b'{"unique_items": 5}')  # missing version → treated as absent
    assert load(store, key) is None


def test_unknown_time_bin_partition_labels():
    from earthcatalog.schema import partition_bin_value

    assert partition_bin_value("year", None) == "unknown"
