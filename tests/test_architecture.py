"""Architectural guard tests.

These tests enforce invariants that prevent regressions during the
simplification refactor.  They are *meta* tests: they check the structure
of the codebase, not its behaviour.

The guards start LOOSE (matching current state) and are tightened in
each phase as modules are slimmed.  A tightening commit should be the
LAST commit in a phase so it is easy to revert if something goes wrong.
"""

from __future__ import annotations

import pathlib

LIB_ROOT = pathlib.Path(__file__).resolve().parent.parent / "earthcatalog"

# catalog.py: 1058 after stamping earthcatalog.index_path in get_or_create
# (includes the temporary bulk_ingest deprecation alias — lower this again
# once the alias is removed).  Target: <600.
CATALOG_PY_MAX_LINES = 1095

# Per-module budget.  Tighten as modules are split.
MODULE_LINE_BUDGETS: dict[str, int] = {
    # Phase 4/6 target (slimmed, SQL deduped)
    "catalog.py": CATALOG_PY_MAX_LINES,
    "search.py": 580,
    # Phase 3 target (+ journal hooks, bounded fetch pool, dedupe DI).
    # Serial ndjson branch deleted in RPI-C (bulk-only now).
    "ingest.py": 780,
    # Extracted from catalog.py — ingest orchestration + daily dry-run +
    # _last_run.json writer + full-mode reset + anti-join wiring
    # (+34: pre-ingest diff report and post-ingest index/Iceberg
    # reconciliation, per the daily-workflow reporting requirement)
    "pipeline.py": 440,
    # Everything else
    "*": 1600,
}

# Modules still allowed to use store_config globals during migration.
STORE_CONFIG_ALLOWLIST = {
    "catalog.py",
    "cli.py",
    "lock.py",
    "run.py",
    "store_config.py",
}


def test_catalog_py_line_guard():
    lines = _line_count("catalog.py")
    assert lines <= CATALOG_PY_MAX_LINES, (
        f"catalog.py grew from {CATALOG_PY_MAX_LINES} to {lines} lines. "
        "Extract responsibilities before adding code."
    )


def test_module_line_budgets():
    for py in sorted(LIB_ROOT.glob("*.py")):
        if py.name.startswith("_") or py.name == "__init__.py":
            continue
        budget = MODULE_LINE_BUDGETS.get(py.name, MODULE_LINE_BUDGETS["*"])
        lines = _line_count(py.name)
        assert lines <= budget, f"{py.name} is {lines} lines (budget {budget})"


def test_no_new_store_config_globals():
    """Non-allowlisted modules must not import from store_config."""
    for py in sorted(LIB_ROOT.glob("*.py")):
        if py.name in STORE_CONFIG_ALLOWLIST or py.name.startswith("_"):
            continue
        src = py.read_text()
        for pattern in (
            "from . import store_config",
            "from earthcatalog import store_config",
            "import earthcatalog.store_config",
            "store_config.get_store()",
            "store_config.set_store(",
            "store_config.set_catalog_key(",
            "store_config.set_lock_key(",
        ):
            assert pattern not in src, (
                f"{py.name} contains '{pattern}' — use explicit store injection"
            )


def _line_count(name: str) -> int:
    path = LIB_ROOT / name
    return len(path.read_text().splitlines()) if path.exists() else 0
