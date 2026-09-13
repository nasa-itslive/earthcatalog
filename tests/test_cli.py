"""Tests for the earthcatalog CLI (earthcatalog/cli.py)."""

from __future__ import annotations

import pytest
from obstore.store import LocalStore
from typer.testing import CliRunner

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.cli import _local_catalog_has_table, app
from earthcatalog.config import GridConfig
from earthcatalog.index import Index
from earthcatalog.ingest import Ingester
from tests.test_ingest import _make_item

runner = CliRunner()


def test_info_prints_catalog_summary(tmp_path):
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = _open_sqlite(db_path=db, warehouse_path=wh)
    get_or_create(catalog, grid_config=GridConfig(type="h3", resolution=2))

    result = runner.invoke(app, ["info", "--catalog", db, "--warehouse", wh])
    assert result.exit_code == 0, result.output
    assert "Grid type" in result.output
    assert "h3" in result.output
    assert "Resolution" in result.output
    assert "Summary" in result.output


def test_info_never_writes_stats_json_without_update(tmp_path):
    """`info` is entirely read-only unless --update is explicitly passed —
    this must hold even on a first run where no stats.json snapshot exists
    yet remotely (bootstrap), and even with --verify (which only reports
    drift)."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")
    catalog = _open_sqlite(db_path=db, warehouse_path=wh)
    get_or_create(catalog, grid_config=GridConfig(type="h3", resolution=2))
    stats_path = tmp_path / "stats.json"

    # Plain `info`, no snapshot exists yet: must not write anything, and
    # must not silently fall back to a full manifest/index scan either
    # ("Total rows" only ever gets printed by that expensive path).
    result = runner.invoke(app, ["info", "--catalog", db, "--warehouse", wh])
    assert result.exit_code == 0, result.output
    assert "none stored yet" in result.output
    assert "Total rows" not in result.output
    assert not stats_path.exists()
    assert list(tmp_path.rglob("stats.json")) == []

    # --verify, still no snapshot stored: still must not write anything.
    result = runner.invoke(app, ["info", "--catalog", db, "--warehouse", wh, "--verify"])
    assert result.exit_code == 0, result.output
    assert "updated" not in result.output
    assert not stats_path.exists()

    # --update explicitly bootstraps it, written exactly beside the
    # warehouse dir — not nested under some duplicated path inside it
    # (see stats._resolve).
    result = runner.invoke(app, ["info", "--catalog", db, "--warehouse", wh, "--update"])
    assert result.exit_code == 0, result.output
    assert "stats.json bootstrapped" in result.output
    assert stats_path.exists()
    assert list(tmp_path.rglob("stats.json")) == [stats_path]
    before = stats_path.read_text()

    # --verify alone (snapshot now exists) must not touch stats.json.
    result = runner.invoke(app, ["info", "--catalog", db, "--warehouse", wh, "--verify"])
    assert result.exit_code == 0, result.output
    assert "[verify]" in result.output
    assert "updated" not in result.output
    assert stats_path.read_text() == before

    # --verify --update explicitly persists the recomputed snapshot.
    result = runner.invoke(
        app, ["info", "--catalog", db, "--warehouse", wh, "--verify", "--update"]
    )
    assert result.exit_code == 0, result.output
    assert "stats.json updated" in result.output


def test_info_defaults_to_production_catalog():
    """No flags: info reads the default production catalog and its
    stats snapshot (the old "specify --catalog" error is gone — the
    default location is the point)."""
    result = runner.invoke(app, ["info"])
    output = result.output
    assert result.exit_code == 0 or "Catalog Info" in output or "Stats" in output


def test_ingest_delegates_to_run(monkeypatch):
    """earthcatalog ingest forwards the essential knobs to earthcatalog.run.run."""
    captured = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return {"items": 0, "rows": 0}

    import earthcatalog.run as _mod

    monkeypatch.setattr(_mod, "run", fake_run)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--inventory",
            "s3://b/inv/manifest.json",
            "--warehouse",
            "s3://b/wh",
            "--mode",
            "full",
            "--limit",
            "100",
            "--skip-compact",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.get("inventory") == "s3://b/inv/manifest.json"
    assert captured.get("diff") is None
    assert captured.get("warehouse") == "s3://b/wh"
    assert captured.get("mode") == "full"
    assert captured.get("limit") == 100
    assert captured.get("skip_compact") is True


def test_ingest_diff_path_delegates(monkeypatch):
    """--diff reaches run() as the diff source (daily path)."""
    captured = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return {"items": 0, "rows": 0}

    import earthcatalog.run as _mod

    monkeypatch.setattr(_mod, "run", fake_run)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--diff",
            "s3://b/diffs/new-20260905-20260906.parquet",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.get("inventory") is None
    assert captured.get("diff") == "s3://b/diffs/new-20260905-20260906.parquet"
    assert captured.get("dry_run") is True


def test_ingest_rejects_inventory_and_diff():
    result = runner.invoke(
        app,
        ["ingest", "--inventory", "s3://b/manifest.json", "--diff", "s3://b/d.parquet"],
    )
    assert result.exit_code == 1
    assert "exactly one" in result.output


# ---------------------------------------------------------------------------
# `consolidate` — stale local catalog file handling
#
# Regression coverage for the Consolidate workflow failure in
# https://github.com/nasa-itslive/earthcatalog/actions/runs/34758257136 —
# NoSuchTableError: Table does not exist: earthcatalog.stac_items.
#
# Root cause: the "Catalog info (before)" step ran
# `earthcatalog info --catalog /tmp/earthcatalog.db ...` (no --catalog-s3),
# which never downloads the real catalog — but *opening* a SqlCatalog at
# that path still creates a valid, empty SQLite file (PyIceberg's own
# bookkeeping tables, no `stac_items` row). The "Consolidate" step then
# skipped its S3 download because `os.path.exists(catalog)` was already
# True for that empty file, and failed loading a table that was never
# registered in it. The Iceberg catalog in S3 was fine the whole time.
# ---------------------------------------------------------------------------


def test_local_catalog_has_table_detects_stale_empty_file(tmp_path):
    """`_local_catalog_has_table` must distinguish "file exists" from "file
    actually has our table registered" — a bare `os.path.exists` check
    cannot tell these apart, which is exactly what caused the CI failure."""
    db = str(tmp_path / "catalog.db")
    wh = str(tmp_path / "warehouse")

    # No file at all.
    assert _local_catalog_has_table(db, wh) is False

    # File exists — opening a SqlCatalog creates its bookkeeping tables —
    # but nothing ever registered `earthcatalog.stac_items` in it. This is
    # precisely what the `info` step left behind in the failed run.
    _open_sqlite(db_path=db, warehouse_path=wh)
    assert _local_catalog_has_table(db, wh) is False

    # A real catalog: the table is registered.
    cat = _open_sqlite(db_path=db, warehouse_path=wh)
    get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    assert _local_catalog_has_table(db, wh) is True


def test_consolidate_recovers_from_stale_local_catalog(tmp_path, monkeypatch):
    """End-to-end reproduction of the CI failure, with the fix applied.

    A real catalog + warehouse ("the bucket") is prepared, and a stale,
    empty catalog file is placed at the CI scratch path ("the bug"). The
    `consolidate` CLI command must detect the stale file has no registered
    table, download the real catalog, and complete successfully instead of
    raising NoSuchTableError.
    """
    # --- "the bucket": a real catalog + warehouse with 5 items, 3 parts,
    # in one partition (same shape as tests/test_consolidate.py's fixture).
    bucket_root = tmp_path / "bucket"
    wh = bucket_root / "warehouse"
    wh.mkdir(parents=True)
    real_store = LocalStore(str(bucket_root))

    real_catalog_path = tmp_path / "real_catalog.db"
    cat = _open_sqlite(db_path=str(real_catalog_path), warehouse_path=str(wh))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=2))
    Ingester(
        store=real_store,
        index=Index(real_store, "seed_index.parquet"),
        table=table,
        fetch_fn=lambda b, k: _make_item(k),
        warehouse_prefix="warehouse",
        warehouse_root=str(wh),
        batch_size=2,  # 5 items -> 3 parts in the same (tile, year)
    ).run([("data-bucket", f"{i}.stac.json") for i in range(5)])

    # Publish catalog.db into "the bucket" too, as upload_catalog() would.
    (bucket_root / "catalog.db").write_bytes(real_catalog_path.read_bytes())

    # --- "the bug": a stale, empty local catalog file at the CI scratch
    # path, exactly like the one `info --catalog <path>` (no --catalog-s3)
    # leaves behind.
    scratch_catalog = tmp_path / "scratch" / "earthcatalog.db"
    scratch_catalog.parent.mkdir(parents=True)
    _open_sqlite(db_path=str(scratch_catalog), warehouse_path="s3://test-bucket/warehouse")
    assert scratch_catalog.exists()
    assert _local_catalog_has_table(str(scratch_catalog), "s3://test-bucket/warehouse") is False

    # Stand in for real S3 with the local "bucket" store built above.
    import earthcatalog.run as run_mod

    monkeypatch.setattr(run_mod, "_make_s3_store", lambda bucket: real_store)
    monkeypatch.setenv("EARTHCATALOG_CATALOG_KEY", "catalog.db")

    result = runner.invoke(
        app,
        [
            "consolidate",
            "--catalog",
            str(scratch_catalog),
            "--warehouse",
            "s3://test-bucket/warehouse",
            "--min-files",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "consolidated" in result.output

    # The stale file was overwritten with the real, downloaded catalog.
    assert _local_catalog_has_table(str(scratch_catalog), "s3://test-bucket/warehouse") is True
