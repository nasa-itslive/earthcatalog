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


def test_info_requires_catalog_or_s3():
    result = runner.invoke(app, ["info"])
    assert result.exit_code == 1
    assert "specify --catalog or --catalog-s3" in result.output


def test_backfill_delegates_to_run(monkeypatch):
    """earthcatalog backfill forwards the essential knobs to run_backfill.run."""
    captured = {}

    def fake_run(**kwargs):
        captured.update(kwargs)

    import scripts.run_backfill as _mod

    monkeypatch.setattr(_mod, "run", fake_run)

    result = runner.invoke(
        app,
        [
            "backfill",
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
    assert captured.get("warehouse") == "s3://b/wh"
    assert captured.get("mode") == "full"
    assert captured.get("limit") == 100
    assert captured.get("skip_compact") is True
