"""Daily-workflow reports: diff report before the run, index/Iceberg
reconciliation after it — recorded in the summary and ``_last_run.json``.
"""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import LocalStore

from earthcatalog.catalog import EarthCatalog, _catalog_info, _open_sqlite, get_or_create
from earthcatalog.config import GridConfig
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester
from earthcatalog.ingest_config import IngestConfig
from earthcatalog.pipeline import IngestPipeline
from tests.test_ingest import _make_item


def _write_parquet(path: Path, table: pa.Table) -> str:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    path.write_bytes(buf.getvalue())
    return str(path)


def _write_diff(path: Path, keys: list[str]) -> str:
    """A diff Parquet as `earthcatalog diff` writes it: full inventory schema."""
    n = len(keys)
    return _write_parquet(
        path,
        pa.table(
            {
                "bucket": pa.array(["data-bucket"] * n, type=pa.string()),
                "key": pa.array(keys, type=pa.string()),
                "size": pa.array([100] * n, type=pa.int64()),
                "last_modified_date": pa.array(["2026-09-07T00:00:00"] * n, type=pa.string()),
            }
        ),
    )


def test_diff_report_then_reconciliation(tmp_path, capsys, monkeypatch):
    """The daily flow prints "X new items from the current inventory"
    BEFORE ingesting, and reports index + Iceberg contents after."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    store = LocalStore(str(tmp_path))
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    index = Index(store, "warehouse_index.parquet")

    # Phase 1: two items already ingested (yesterday's run).
    known_keys = ["dir/item-1.stac.json", "dir/item-2.stac.json"]
    Ingester(
        store=store,
        index=index,
        table=table,
        fetch_fn=lambda b, k: _make_item(k),
        warehouse_prefix="warehouse",
        warehouse_root=str(wh),
        batch_size=10,
    ).run([("data-bucket", k) for k in known_keys])

    ec = EarthCatalog(
        catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None
    )

    # Hermetic run: the production fetcher would hit real S3 for item-3.
    import earthcatalog.inventory as inv_mod

    monkeypatch.setattr(
        inv_mod, "fetch_items_async", lambda pairs, concurrency: [_make_item(k) for _, k in pairs]
    )

    # Phase 2: today's diff — the two known keys plus one genuinely new.
    all_keys = [*known_keys, "dir/item-3.stac.json"]
    diff_path = _write_diff(tmp_path / "new-20260906-20260907.parquet", all_keys)

    summary = IngestPipeline(ec, config=IngestConfig(chunk_size=10)).run(diff_path, mode="delta")
    out = capsys.readouterr().out

    # Pre-ingest: the diff report precedes any ingest work.
    assert "1 new item" in out, out
    assert "3 considered" in out, out
    assert "2 already indexed" in out, out
    diff_pos = out.index("Diff report")
    ingest_pos = out.index("Run summary")
    assert diff_pos < ingest_pos

    # Post-ingest: reconciliation against the real index and table.
    assert f"index holds {3:,} source keys" in out, out
    assert "Iceberg holds" in out, out
    assert summary["index_keys"] == 3
    assert summary["iceberg_rows"] >= 1
    assert summary["items"] == 1  # only the genuinely new key was fetched


def test_fresh_warehouse_reconciliation(tmp_path, capsys, monkeypatch):
    """First-ever run: no index exists yet so there is no diff report —
    everything is new — but the reconciliation still reports real counts
    against the index the run itself created."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    store = LocalStore(str(tmp_path))
    wh = tmp_path / "warehouse"
    wh.mkdir(parents=True)
    cat = _open_sqlite(db_path=str(tmp_path / "catalog.db"), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))

    ec = EarthCatalog(
        catalog=cat, table=table, info=_catalog_info(table), store=store, catalog_key=None
    )

    keys = ["only.stac.json"]
    diff_path = _write_diff(tmp_path / "diff.parquet", keys)

    # Hermetic run: the production fetcher would hit real S3.
    import earthcatalog.inventory as inv_mod

    monkeypatch.setattr(
        inv_mod, "fetch_items_async", lambda pairs, concurrency: [_make_item(k) for _, k in pairs]
    )

    summary = IngestPipeline(ec, config=IngestConfig(chunk_size=10)).run(diff_path, mode="delta")
    out = capsys.readouterr().out

    assert "Diff report" not in out, out  # no index to diff against yet
    assert summary["items"] == 1
    assert summary["index_keys"] == 1
    assert summary["iceberg_rows"] >= 1
    assert "Post-ingest: index holds 1 source key" in out, out
