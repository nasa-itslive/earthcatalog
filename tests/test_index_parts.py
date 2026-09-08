"""Tests for the part-based unified index.

The index is a set of immutable Parquet parts under ``{base}/`` plus, for
warehouses predating parts, the legacy single-file ``{base}.parquet``.
Appending never reads existing parts (O(delta)); reads stream every
location; soft deletes rewrite only the parts they touch.
"""

from __future__ import annotations

from obstore.store import LocalStore, MemoryStore

from earthcatalog.index import Index


def _row(key: str, stac_id: str | None = None) -> dict:
    return {
        "s3_key": f"s3://b/{key}",
        "stac_id": stac_id or key,
        "grid_partition": "cellA",
        "year": 2020,
    }


def test_append_writes_parts_and_never_reads_them():
    store = MemoryStore()
    idx = Index(store, "warehouse/index")

    calls = {"n": 0}
    real_get = store.get

    def counting_get(*a, **kw):
        calls["n"] += 1
        return real_get(*a, **kw)

    store.get = counting_get  # type: ignore[method-assign]
    idx.append([_row("a.stac.json")], part="run1/0000")
    idx.append([_row("b.stac.json")], part="run1/0001")
    assert calls["n"] == 0, "append must not read existing parts"

    assert idx.locations() == [
        "warehouse/index/run1/0000.parquet",
        "warehouse/index/run1/0001.parquet",
    ]


def test_locations_includes_legacy_single_file():
    store = MemoryStore()
    idx = Index(store, "warehouse/index.parquet")
    idx.append([_row("a.stac.json")])
    idx.compact()  # merges into the legacy single file
    idx.append([_row("b.stac.json")], part="run1/0000")  # a part after compaction

    assert "warehouse/index.parquet" in idx.locations()
    assert "warehouse/index/run1/0000.parquet" in idx.locations()


def test_reads_span_parts_and_legacy(tmp_path):
    store = MemoryStore()
    idx = Index(store, "warehouse/index.parquet")
    idx.append([_row("a.stac.json")])  # legacy single file (first appends kept writing it)
    idx.append([_row("b.stac.json")], part="run1/0000")

    assert idx.count_active() == 2
    assert idx.known_source_keys() == {"s3://b/a.stac.json", "s3://b/b.stac.json"}
    assert {r["s3_key"] for r in idx.stream_active()} == {
        "s3://b/a.stac.json",
        "s3://b/b.stac.json",
    }


def test_exists_false_for_absent_index():
    assert Index(MemoryStore(), "warehouse/index").exists() is False


def test_mark_deleted_rewrites_only_affected_parts():
    store = MemoryStore()
    idx = Index(store, "warehouse/index")
    idx.append([_row("a.stac.json", "a")], part="p1")
    idx.append([_row("b.stac.json", "b")], part="p2")

    n = idx.mark_deleted({"a"})
    assert n == 1
    assert idx.count_active() == 1
    assert {r["stac_id"] for r in idx.stream_active()} == {"b"}


def test_compact_merges_parts_into_legacy_file():
    store = MemoryStore()
    idx = Index(store, "warehouse/index")
    idx.append([_row("a.stac.json")], part="p1")
    idx.append([_row("b.stac.json")], part="p2")

    n = idx.compact()
    assert n == 2
    assert idx.locations() == ["warehouse/index.parquet"]
    assert idx.count_active() == 2


def test_append_auto_part_id_when_omitted():
    store = MemoryStore()
    idx = Index(store, "warehouse/index")
    idx.append([_row("a.stac.json")])
    assert len(idx.locations()) == 1


def test_items_per_day(tmp_path):
    """Index rows group by ingested_at date — the per-day ingest report."""
    from earthcatalog.index import Index

    store = LocalStore(str(tmp_path))
    idx = Index(store, "warehouse_index.parquet")
    idx.append(
        [
            {
                "stac_id": f"a{i}",
                "s3_key": f"s3://b/a{i}.stac.json",
                "grid_partition": "c",
                "year": 2020,
                "ingested_at": "2026-09-05T01:00:00+00:00",
            }
            for i in range(3)
        ]
    )
    idx.append(
        [
            {
                "stac_id": "b",
                "s3_key": "s3://b/b.stac.json",
                "grid_partition": "c",
                "year": 2020,
                "ingested_at": "2026-09-06T01:00:00+00:00",
            }
        ],
        part="run2/0000",
    )

    full = [str(tmp_path / loc) for loc in idx.locations()]
    per_day = idx.items_per_day(locations=full)
    assert dict(per_day) == {"2026-09-06": 1, "2026-09-05": 3}
