"""Tests for the index-path resolver and migrate_indices (A1/A8 fixes)."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import LocalStore

from earthcatalog.catalog import _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.index import Index, resolve_index_path
from earthcatalog.migrate import migrate_indices
from earthcatalog.schema import PROP_HASH_INDEX_PATH, PROP_INDEX_PATH
from tests.test_ingest import _make_item


class _Props:
    def __init__(self, props: dict | None = None):
        self.properties = props or {}


def test_resolver_property_wins():
    t = _Props({PROP_INDEX_PATH: "s3://b/custom_index.parquet"})
    assert resolve_index_path(t, "s3://b/warehouse") == "s3://b/custom_index.parquet"


def test_resolver_conventional_fallback():
    default = "s3://b/warehouse_index.parquet"
    assert resolve_index_path(_Props(), default) == default
    assert resolve_index_path(None, "wh_index.parquet") == "wh_index.parquet"
    assert resolve_index_path(_Props(), "") == ""


def test_resolver_ignores_legacy_property():
    """hash_index_path names the retired id_hashes file — never follow it."""
    t = _Props({PROP_HASH_INDEX_PATH: "s3://b/warehouse_id_hashes.parquet"})
    assert (
        resolve_index_path(t, "s3://b/warehouse_index.parquet") == "s3://b/warehouse_index.parquet"
    )


def _write_parquet(path: Path, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    path.write_bytes(buf.getvalue())


def _make_warehouse(tmp_path: Path):
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True, exist_ok=True)
    store = LocalStore(str(wh))
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    return wh, store, cat, table


def test_get_or_create_stamps_index_property(tmp_path):
    _, _, _, table = _make_warehouse(tmp_path)
    assert table.properties.get(PROP_INDEX_PATH) == f"{tmp_path / 'warehouse'}_index.parquet"


def test_get_or_create_leaves_legacy_warehouse_alone(tmp_path):
    _, _, cat, table = _make_warehouse(tmp_path)
    with table.transaction() as tx:
        tx.set_properties(**{PROP_HASH_INDEX_PATH: "x/warehouse_id_hashes.parquet"})
        tx.remove_properties(PROP_INDEX_PATH)
    table2 = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    assert table2.properties.get(PROP_INDEX_PATH) is None
    assert table2.properties.get(PROP_HASH_INDEX_PATH) == "x/warehouse_id_hashes.parquet"


def test_migrate_merges_legacy_indices(tmp_path):
    wh, store, cat, table = _make_warehouse(tmp_path)
    with table.transaction() as tx:
        tx.remove_properties(PROP_INDEX_PATH)

    # Legacy source index: full provenance rows (old schema: no id_hash).
    now = "2026-01-01T00:00:00+00:00"
    source = pa.table(
        {
            "s3_key": ["s3://b/a.stac.json", "s3://b/b.stac.json"],
            "stac_id": ["a", "b"],
            "grid_partition": ["cellA", "cellA"],
            "year": pa.array([2020, 2020], type=pa.int32()),
            "ingested_at": [now, now],
            "deleted": pa.array([False, False], type=pa.bool_()),
        }
    )
    _write_parquet(wh / "warehouse_source_index.parquet", source)

    # Legacy hash index: one hash-only item + one duplicate of "a".
    import xxhash

    def hid(s):
        return xxhash.xxh3_128(s.encode(), seed=42).digest()

    hashes = pa.table({"id_hash": pa.array([hid("a"), hid("c")], type=pa.binary(16))})
    _write_parquet(wh / "warehouse_id_hashes.parquet", hashes)

    report = migrate_indices(cat, store, str(wh))
    assert report["status"] == "migrated"
    assert report["source_rows"] == 2
    assert report["hash_only_rows"] == 1
    assert report["total_rows"] == 3

    # The unified index is live at the conventional key.
    index = Index(store, "warehouse_index.parquet")
    assert index.count_active() == 3
    assert "s3://b/a.stac.json" in index.known_source_keys()
    table = cat.load_table("earthcatalog.stac_items")  # re-load: properties moved
    assert table.properties.get(PROP_INDEX_PATH).endswith("_index.parquet")

    # Idempotent.
    assert migrate_indices(cat, store, str(wh))["status"] == "already-migrated"

    # Dry-run writes nothing (the fresh table is stamped, so drop the
    # property to simulate a not-yet-migrated warehouse).
    _, store2, cat2, table2 = _make_warehouse(tmp_path / "dry")
    with table2.transaction() as tx:
        tx.remove_properties(PROP_INDEX_PATH)
    report2 = migrate_indices(cat2, store2, str(tmp_path / "dry" / "warehouse"), dry_run=True)
    assert report2["status"] == "dry-run"


def test_migrate_skipped_when_no_legacy_files(tmp_path):
    _, store, cat, table = _make_warehouse(tmp_path)
    with table.transaction() as tx:
        tx.remove_properties(PROP_INDEX_PATH)  # simulate a not-yet-migrated table
    wh = str(tmp_path / "warehouse")
    report = migrate_indices(cat, store, wh, dry_run=True)
    assert report["source_rows"] == 0
    assert report["hash_only_rows"] == 0
    assert report["expected_rows"] == 0


def test_full_mode_resets_index_and_staging(tmp_path, monkeypatch):
    """A --mode full run resets the index object and sweeps _staging/ —
    otherwise the resume checkpoint would suppress every key and a 'full'
    rebuild would ingest nothing."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    wh, store, cat, table = _make_warehouse(tmp_path)

    from earthcatalog.catalog import EarthCatalog
    from earthcatalog.ingest import Ingester
    from earthcatalog.pipeline import IngestPipeline

    index = Index(store, "warehouse_index.parquet")

    class _FakeTable:
        def __init__(self):
            self.files: list[str] = []
            self.properties: dict[str, str] = {}

        def add_files(self, paths):
            self.files.extend(paths)

    ing = Ingester(
        store=store,
        index=index,
        table=_FakeTable(),
        fetch_fn=lambda b, k: _make_item(k),
        warehouse_prefix="warehouse",
        warehouse_root=str(wh),
        batch_size=10,
    )
    keys = ["a.stac.json", "b.stac.json"]
    ing.run([("data-bucket", k) for k in keys])
    assert index.known_source_keys() == {f"s3://data-bucket/{k}" for k in keys}

    # Leave crash debris; full mode must sweep it.
    store.put("warehouse/_staging/journal/dead/0000.json", b"{}")
    store.put("warehouse/_staging/ndjson/grid_partition=cellA/year=2020/x.jsonl", b"{}\n")

    # Patch the Ingester: the contract under test is the reset, not the
    # re-ingest (which the membership tests already pin down).
    import earthcatalog.ingest as ing_mod

    captured: dict = {}

    class _RecordingIngester:
        def __init__(self, store, index, table, **kwargs):
            captured["index"] = index
            captured["kwargs"] = kwargs

        def run(self, inventory):
            captured["considered"] = sum(1 for _ in inventory)
            return {"items": captured["considered"], "rows": captured["considered"]}

    monkeypatch.setattr(ing_mod, "Ingester", _RecordingIngester)

    ec = EarthCatalog(
        catalog=cat,
        table=table,
        info=_catalog_info(table),
        store=store,
        catalog_key=None,  # no catalog download/upload round-trip locally
    )
    # A full run still ingests from an inventory — a complete day snapshot.
    inv = pa.table({"bucket": ["data-bucket"] * len(keys), "key": keys})
    inv_path = tmp_path / "full_inventory.parquet"
    buf = io.BytesIO()
    pq.write_table(inv, buf, compression="zstd")
    inv_path.write_bytes(buf.getvalue())

    from earthcatalog.ingest_config import IngestConfig

    summary = IngestPipeline(ec, config=IngestConfig()).run(str(inv_path), mode="full")

    # The new index object was empty → nothing suppressed the re-run.
    fresh = Index(store, "warehouse_index.parquet")
    assert len(fresh.known_source_keys()) == 0
    assert captured["considered"] == len(keys)
    assert summary["items"] == len(keys)
    leftovers = [obj["path"] for batch in store.list(prefix="warehouse/_staging") for obj in batch]
    assert leftovers == [], leftovers


def test_full_mode_requires_credentials(monkeypatch, tmp_path):
    """Sanity: pipeline.run still refuses to run without AWS creds."""
    from earthcatalog.catalog import EarthCatalog

    wh, store, cat, table = _make_warehouse(tmp_path)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    ec = EarthCatalog(
        catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None
    )
    from earthcatalog.pipeline import IngestPipeline

    try:
        IngestPipeline(ec).run(str(wh), mode="full")
        raised = False
    except RuntimeError as e:
        raised = "AWS credentials" in str(e)
    assert raised
