"""Tests for the earthcatalog CLI (earthcatalog/cli.py)."""

from __future__ import annotations

from typer.testing import CliRunner

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.cli import app
from earthcatalog.config import GridConfig

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
