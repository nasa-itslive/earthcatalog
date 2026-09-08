# Dead Code Cleanup Plan

Status: **Draft — not yet implemented**

Analysis of branch `feature/garbage-collection` vs `main` (as of 2026-08-10).
This document lists confirmed dead code and recommended simplifications. No
code has been changed yet.

---

## Branch Overview

`feature/garbage-collection` is **11 commits ahead of `main`, 0 behind**
(main is a strict ancestor, so a fast-forward merge is possible with no
conflicts). The branch adds the garbage-collection pipeline:

- `earthcatalog/pipelines/delete.py` — bloom-filter orphan detection
- `earthcatalog/source_index.py` — source provenance index
- `scripts/run_gc.py`, `.github/workflows/garbage_collect.yml`
- `tests/test_delete.py`, `tests/test_gc_bugs.py`, `tests/test_source_index.py`
- Changes to `catalog.py`, `backfill.py`, `incremental.py`, `run_backfill.py`,
  `daily_delta.yml`, `ingest.yml`, `pyproject.toml`, `uv.lock`
- New deps: `bokeh`, `duckdb` (moved to main deps), `pybloom-live`, `coiled`
- Planning docs: `docs/delete_assessment.md`, `docs/delete_plan.md`,
  `docs/delete_plan_v2.md`, `notebooks/build_catalog_from_scratch.ipynb`

---

## Confirmed Dead Code

### 1. `scripts/update_hash_index.py` — dead standalone script

- **Referenced by no workflow and no other script.** Only a docstring in
  `tests/test_daily_delta.py:2` and the `update_from_delta` tests at
  `tests/test_daily_delta.py:505-557` reference it.
- Its job is fully covered by `run_backfill --update-hash-index`, which calls
  `_update_hash_index_from_parquets` in `earthcatalog/pipelines/backfill.py:911`.

**Action:** remove `scripts/update_hash_index.py` and the
`update_from_delta` tests in `tests/test_daily_delta.py:505-557` (after
confirming they don't cover anything else).

### 2. `scripts/archive/` — 10 dead scripts

- `audit_inventory.py`, `catalog_stats.py`, `compact_stragglers.py`,
  `compare_catalogs.py`, `diff_manifests.py`, `dump_inventory_ids.py`,
  `extract_manifest_ids.py`, `final_compact.py`, `migrate_schema.py`,
  `repair_catalog.py`
- None referenced by workflows, other scripts, or tests. Git history
  preserves them, so removal is safe.

**Action:** `git rm -r scripts/archive/`.

### 3. Duplicate `_count_rows` in `scripts/consolidate.py`

- Defined **twice** with identical bodies: line 121 and line 268. The second
  definition shadows the first, so the first (line 121) is dead.

**Action:** delete the first definition at `scripts/consolidate.py:121-123`.

### 4. Unused variable in `earthcatalog/pipelines/backfill.py:881`

- `n = rebuild_iceberg_from_warehouse(...)` — `n` is never used
  (ruff F841).

**Action:** drop the assignment.

### 5. Stale planning docs

- `docs/delete_assessment.md` and `docs/delete_plan.md` are superseded by
  `docs/delete_plan_v2.md`. They are not in `docs/site/` (zensical
  `docs_dir = "docs/site"`), so they are never published.

**Action:** remove `docs/delete_assessment.md` and `docs/delete_plan.md`;
keep `docs/delete_plan_v2.md`.

### 6. `bokeh>=3.1.0` dependency — possibly unused

- Pinned in `pyproject.toml` by commit `e4356e2` ("remove manual wheel install
  from Coiled path"), but **no Python code imports `bokeh`**.

**Action:** confirm with whoever runs the Coiled path whether Coiled/Dask
workers need it; default recommendation is to drop it from
`[project.dependencies]`.

### 7. `compact_source_index()` in `earthcatalog/source_index.py:144`

- Used only by `tests/test_source_index.py`, never by the pipeline.

**Action:** keep (small, tested utility) unless a leaner API is desired.

---

## Structural Simplification (not strictly dead code)

### CLI / script fragmentation — the biggest win

The `earthcatalog` CLI (`earthcatalog/cli.py`) exposes **only one
subcommand**: `incremental`. Everything else ships as standalone argparse
scripts in `scripts/`, each reimplementing the same plumbing:

| Capability        | Where it lives            | In `earthcatalog` CLI? |
|-------------------|---------------------------|------------------------|
| Incremental ingest| `incremental.py:main`     | Yes (`incremental`)    |
| Backfill          | `scripts/run_backfill.py` | No                     |
| Garbage collect   | `scripts/run_gc.py`       | No                     |
| Catalog info      | `scripts/info.py`         | **No**                 |
| Consolidate       | `scripts/consolidate.py`  | No                     |
| Daily delta       | `scripts/daily_delta.py`  | No                     |
| Compaction        | `python -m …maintenance.compact` | No             |

Only `incremental.yml` invokes the real CLI
(`uv run earthcatalog incremental …`); every other workflow shells out to
`python scripts/*.py`.

**Concretely duplicated across scripts** (≥5 copies each):
- `_get_store(bucket, prefix)` — S3Store construction (in `consolidate.py`,
  `daily_delta.py`, `run_backfill.py`, `info.py`, `update_hash_index.py`)
- `_parse_s3_uri(uri)` — `s3://` parsing
- S3 auth boilerplate (region resolution, `AWS_*` env handling,
  `skip_signature` toggling)
- `--warehouse` / `--catalog` defaults and resolution logic

**Proposed**: fold the scripts into typer subcommands so users can run
`earthcatalog info --catalog-s3 s3://…`, `earthcatalog gc …`,
`earthcatalog backfill …`, etc. Extract the shared S3/auth helpers into a
single module (e.g. `earthcatalog/store_config.py`, which already exists
and is only partially used). This removes ~5 copies of the same boilerplate
and makes `scripts/` shims (or eliminates them entirely).

### Catalog `info` is not in the CLI

Directly answering "can we do `earthcatalog --info URL`?": **no**.
`scripts/info.py` already implements this (grid metadata, row/file counts,
year/cell distribution, hash-index unique-item count, compaction candidates)
and already accepts `--catalog-s3 s3://…` for auto-download. None of it is
wired into the `earthcatalog` CLI.

**Proposed**: add an `info` subcommand that wraps the logic currently in
`scripts/info.py` (ideally lifting the printing into a reusable function in
`earthcatalog/` so both the CLI and the script call it).

### Duplicate entry point

`pyproject.toml:56-57` registers two console scripts that overlap:

```
earthcatalog-ingest = "earthcatalog.pipelines.incremental:main"
earthcatalog        = "earthcatalog.cli:main"          # has an `incremental` subcommand
```

`earthcatalog-ingest` and `earthcatalog incremental` run the same pipeline
through different code paths. **Proposed**: drop `earthcatalog-ingest` (or
keep it as a thin alias) once the CLI is the single front door.

### CLI is stale relative to active development

The `earthcatalog` CLI drives the legacy `incremental` pipeline, while the
GC branch's work and day-to-day operations run through
`backfill.py` / `run_backfill.py`. When consolidating, make `backfill` a
first-class subcommand too.

---

## Deferred (not strictly dead)

### `consolidate.yml` vs `daily_delta.yml` overlap

Both workflows do inventory scan → delta → ingest. Not dead code, but worth
reconciling in a separate discussion.

---

## Verification After Changes

```bash
uv run ruff check earthcatalog/ scripts/
uv run pytest --tb=short -q -m "not integration and not performance and not e2e"
rg -n "update_hash_index" .github/workflows/ scripts/  # only run_backfill/backfill refs remain
```

---

## Suggested PR Order

1. Merge/deal with `feature/garbage-collection` independently.
2. Apply this cleanup as a separate commit or PR so GC logic and cleanup are
   reviewable independently.
