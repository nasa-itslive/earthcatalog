"""Consolidation: plan from metadata, atomic replace, dedupe, idempotence."""

from __future__ import annotations

import pytest
from obstore.store import LocalStore

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.consolidate import plan, run
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester
from tests.test_ingest import _make_item


@pytest.fixture()
def warehouse(tmp_path):
    """A real table holding 5 items in ONE partition, written as 3 parts."""
    store = LocalStore(str(tmp_path))
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))

    Ingester(
        store=store,
        index=Index(store, "seed_index.parquet"),
        table=table,
        fetch_fn=lambda b, k: _make_item(k),
        warehouse_prefix="warehouse",
        warehouse_root=str(wh),
        batch_size=2,  # 5 items → 3 parts in the same (tile, year)
    ).run([("data-bucket", f"{i}.stac.json") for i in range(5)])
    return store, table, str(wh)


def _part_file_count(store, prefix="warehouse/"):
    return sum(
        1
        for batch in store.list(prefix=prefix)
        for obj in batch
        if obj["path"].endswith(".parquet")
    )


def _table_ids(table):
    return sorted(table.scan().to_arrow().column("id").to_pylist())


def test_plan_finds_the_hot_partition_only(warehouse):
    store, table, _ = warehouse
    plans = plan(table, min_files=3)
    assert len(plans) == 1
    assert len(plans[0].files) == 3
    assert plans[0].total_rows == 5
    assert plans[0].bin_value == "2020"
    # Planning is read-only.
    assert _part_file_count(store) == 3


def test_plan_respects_threshold_and_limit(warehouse):
    _, table, _ = warehouse
    assert plan(table, min_files=4) == []  # 3 files < 4 → nothing to do
    assert len(plan(table, min_files=1, limit_tiles=1)) == 1


def test_consolidate_replaces_files_atomically(warehouse):
    store, table, _ = warehouse
    before_ids = _table_ids(table)

    reports = run(store, table, "warehouse", min_files=3)

    assert len(reports) == 1
    r = reports[0]
    assert r["files_before"] == 3 and r["files_after"] == 1
    assert r["rows"] == 5
    assert r["old_files_deleted"] == 3

    # Exactly one file remains on S3 and the metadata agrees.
    assert _part_file_count(store) == 1
    assert sum(1 for _ in table.scan().plan_files()) == 1
    assert table.scan().count() == 5
    assert _table_ids(table) == before_ids


def test_consolidate_is_idempotent(warehouse):
    store, table, _ = warehouse
    run(store, table, "warehouse", min_files=3)
    # One file left → nothing meets the threshold; a re-run is a no-op.
    reports = run(store, table, "warehouse", min_files=3)
    assert reports == []
    assert _part_file_count(store) == 1
    assert table.scan().count() == 5


def test_consolidate_dedupes(warehouse):
    store, table, wh = warehouse
    # Re-ingest the same items through a FRESH index: the partition now
    # holds every row twice — exactly what consolidation must clean up.
    Ingester(
        store=store,
        index=Index(store, "fresh_index.parquet"),
        table=table,
        fetch_fn=lambda b, k: _make_item(k),
        warehouse_prefix="warehouse",
        warehouse_root=wh,
        batch_size=5,
    ).run([("data-bucket", f"{i}.stac.json") for i in range(5)])
    assert table.scan().count() == 10

    reports = run(store, table, "warehouse", min_files=2)
    assert reports[0]["rows"] == 5
    assert reports[0]["rows_removed_dupes"] == 5
    assert table.scan().count() == 5
    assert _table_ids(table) == [f"item-{i}.stac.json" for i in range(5)]


def test_consolidate_tolerates_already_deleted_files(warehouse):
    """Crash window from a previous run: its commit landed but the db
    upload didn't, so metadata lists objects already deleted on S3.
    Consolidation must heal the partition from what physically exists."""
    store, table, wh = warehouse
    plans = plan(table, min_files=3)
    victim = plans[0].files[0]  # whichever part sorts first in the metadata
    from earthcatalog.consolidate import _key

    victim_key = _key(victim, "warehouse")
    import io

    import pyarrow.parquet as pq

    victim_rows = pq.ParquetFile(io.BytesIO(bytes(store.get(victim_key).bytes()))).metadata.num_rows
    store.delete(victim_key)

    reports = run(store, table, "warehouse", min_files=3)

    assert reports[0]["already_missing"] == 1
    assert reports[0]["rows"] == 5 - victim_rows
    assert _part_file_count(store) == 1
    assert sum(1 for _ in table.scan().plan_files()) == 1
    assert table.scan().count() == 5 - victim_rows


def test_consolidate_adopts_orphaned_output(warehouse):
    """Worst crash window: the previous run merged, committed AND deleted
    the old objects, but never uploaded its db — S3 holds one unregistered
    merged file while the metadata lists only phantoms.  Consolidation
    must adopt the orphan and drop the phantoms."""
    store, table, _ = warehouse
    from earthcatalog.consolidate import _key

    plans = plan(table, min_files=3)
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    def read(uri):
        return pq.ParquetFile(io.BytesIO(bytes(store.get(_key(uri, "warehouse")).bytes()))).read()

    merged = pa.concat_tables([read(u) for u in plans[0].files])
    buf = io.BytesIO()
    pq.write_table(merged, buf, compression="zstd")
    orphan_key = "warehouse/grid=h3/level=2/tile=cellA/year=2020/part_000009.parquet"
    for u in plans[0].files:  # the crashed run deleted every listed object
        store.delete(_key(u, "warehouse"))
    store.put(orphan_key, buf.getvalue())

    reports = run(store, table, "warehouse", min_files=3)

    assert reports[0]["already_missing"] == len(plans[0].files)
    assert reports[0]["new_file"] == orphan_key
    assert reports[0]["rows"] == 5
    assert table.scan().count() == 5
    assert _part_file_count(store) == 1
    assert _table_ids(table) == [f"item-{i}.stac.json" for i in range(5)]


def test_dry_run_writes_nothing(warehouse):
    store, table, _ = warehouse
    reports = run(store, table, "warehouse", min_files=3, dry_run=True)
    assert reports and reports[0]["dry_run"] is True
    assert _part_file_count(store) == 3
    assert sum(1 for _ in table.scan().plan_files()) == 3
