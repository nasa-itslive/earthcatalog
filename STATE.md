# Project State — daily-ingest rework

Last updated: **2026-09-06 evening** · Branch: `feature/garbage-collection` ·
Suite: **320 passed** (`pytest -m "not integration and not performance and not e2e"`)
· 9 commits landed today (see `git log --oneline d6a75ca..HEAD`, note `origin/main` is ~4 months stale).

---

## Process (adopted as of today): RPI — Research → Plan → Implement

Every unit of work from now on goes through three explicitly-verified stages:

1. **Research** — gather evidence from the real system first (read code, run
   read-only queries/benchmarks against real data, reproduce bugs). Output:
   findings with numbers/file:line evidence recorded in this file or a
   benchmark artifact. No code changes.
2. **Plan** — a written, reviewable plan (ask the user when scope/semantics
   are ambiguous). Output: approved plan text; todos updated.
3. **Implement** — TDD where practical; every stage ends with the suite
   green, a commit, and real-S3 verification confined to the scratch path.

Standing constraints:
- **S3 writes only under** `s3://its-live-data/test-space/stac/refactoring/`
  (current layout: `diffs/`, `benchmarks/`, `warehouse/`).
- The real catalog at `s3://its-live-data/test-space/stac/catalog/` is
  **read-only for us** (its index is ~395k keys behind — see below).
- Local env: mamba env `earthcatalog` (aws cli + duckdb 1.5.2 + deps);
  credentials live in `~/.aws/credentials` — **pyiceberg only reads env
  creds**, so export them: `export AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)"` etc.

---

## What landed (this cycle)

The daily pipeline is now **two exact steps**, both verified against the real
inventory mirror at `s3://its-live-data/test-space/stac/inventory-test/`:

```
earthcatalog diff  --current <day-N manifest|filelist> --previous <day-N-1> \
    --out s3://…refactoring/diffs/new-<d1>-<d2>.parquet [--out-old …]
earthcatalog ingest --diff <new.parquet> --mode delta \
    --scheduler synchronous --fetch-workers 16
```

- **`earthcatalog/diff.py`** — DuckDB EXCEPT engine: out-of-core, string-exact,
  tuple-keyed `(key, size, last_modified)` so re-uploads surface; required dep.
- **`earthcatalog/keydiff.py` + `Index.known_key_hashes()`** — in-process
  streaming anti-join vs the index: sorted 16-byte xxh3_128(s3_key) array,
  built once per run (one GET, column-streamed, 16 B/row), batched
  `searchsorted`. Replaced the per-item full-index-re-read resume (A2).
- **`earthcatalog/journal.py`** — write-ahead batch journal
  (`_staging/journal/{run_id}/{seq}.json`) + run-start recovery closing every
  commit window; fault-injection matrix in `tests/test_fault_matrix.py`
  (6 cells green). Scope: **serial direct stage** (see Open item).
- **`--mode full` really rebuilds** — index object deleted + `_staging/`
  swept with the table drop (A1).
- **`earthcatalog/migrate.py` + `earthcatalog migrate-indices`** — legacy
  `*_id_hashes.parquet` / `*_source_index.parquet` → unified index;
  validated sidecar, atomic swap, idempotent (A8). Single resolver
  `resolve_index_path()` (`earthcatalog.index_path` property → conventional
  path; legacy property deliberately not followed).
- **Bulk profile** — scatter/map-reduce landed (step 0); DaskIngester head
  pre-filters shards against the index before `client.map` (A5).
- **CLI/packaging (A7)** — entry point lives in `earthcatalog/run.py`;
  `scripts/ingest.py` is a shim; wheel smoke test added to CI.
- **`--dry-run`** (counts, no writes) and **`_last_run.json`** written to the
  warehouse every run.
- **A11/A12** — year=NULL sentinel (GC partition lookup matches
  `year=unknown/`); dead `_GC_FILE_RE` removed; docs + plan_simplification
  reconciled; `scripts/daily_delta.py` and the two-job workflow deleted —
  `daily_delta.yml` is now one job (diff → ingest, concurrency group,
  timeout 120 min), still dispatch-only.

## Real-data verification (T0 + E2E, all numbers real)

| check | result |
|---|---|
| inventory day | 43.2M `.stac.json` rows/day (134 files, ~5 GB) |
| day-over-day diff (fresh scans + EXCEPT + S3 write) | **432 s**, 27,458 new/changed |
| EXCEPT compute alone (temp tables) | ~15 s |
| index vs Sep-6 inventory | catalog index is **394,964 keys behind** |
| dry-run on real diff (`--limit 200`) | considered 200 · new 200 · known 0 |
| real ingest (`--limit 200`, fetch-workers 16) | fetched 200, 308 GeoParquet rows, 34 files registered |
| idempotent re-run | fetched 0 · considered 200 |
| `earthcatalog info` | 308 rows · 25 cells · years 2025–2026 |
| spatial search (`bbox` from an item) | 38 items returned |
| `_last_run.json` | written every run, correct counters |

Benchmark report: `s3://its-live-data/test-space/stac/refactoring/benchmarks/baseline-2026-09-06.md`.
Full baseline is also in `/tmp/t0/results.json` (re-create via `/tmp/t0/bench2.py` if needed).

---

## OPEN (found during live verification — first RPI item)

### RPI-1 · Journal survives ndjson runs (stage-scope mismatch)

**Evidence:** after a successful real run, `_staging/journal/{run}/0000.json`
remained; its content is `keys: 20, files: 0`; no WARN from `finish_batch`.
**Cause (confirmed):** `IngestConfig.stage` defaults to `"ndjson"` and the CLI
does not expose `--stage`, so real runs go through ndjson — the pooled path
calls `journal.start_batch()` before fetching in *every* stage, but only the
direct stage's `_flush_direct` ever records files / finishes the journal.
Next run's recovery cleans it (keys-only branch), so behavior is *safe* but
noisy and semantically wrong.
**Fix sketch (implement):** create/start the journal only when
`self._stage == "direct"` in `Ingester.run` (guard `journal = BatchJournal(…) if direct else None`),
add `"stage"` to the run summary, extend the fault matrix with one ndjson-run
assertion (no journal created), re-verify live.
**Also decide:** should the daily CLI default `--stage direct` explicitly
(it is the documented daily profile; ndjson remains the PGSTAC/bulk option)?

## Backlog (RPI candidates, in priority order)

- **RPI-2 · Single-writer enforcement** — workflow `concurrency` group is in
  `daily_delta.yml`; the `S3Lock` wrap for ingest/GC entry points is **not**
  wired yet (`EarthCatalog.lock()` still has zero callers). Plan §2.7 half-done.
- **RPI-3 · Changed-keys report** — the diff parquet carries
  `(size, last_modified)`; count/flag re-uploaded-but-known keys explicitly
  in `_last_run.json` (user decision: report, don't re-ingest).
- **RPI-4 · Consolidation `--audit`** — per-partition row counts vs
  `Index.count_active()` into `_last_run.json` (crash-orphan visibility,
  plan §5).
- **RPI-5 · Production backlog** — the real index at `catalog/` is 394,964
  keys behind; decide: run `migrate-indices` + a catch-up diff ingest there,
  or rebuild the test warehouse under refactoring first. Needs a user call.
- **RPI-6 · Enable workflows** — only after AWS secrets exist in the repo and
  RPI-1/2 land; first run in `--dry-run`.
- **RPI-7 · Optional** — ndjson-stage fault cells (de-scoped by design),
  `earthcatalog delta` naming bikeshed, reverse diff (index↛inventory) for GC.

## Verification commands (resume here)

```bash
source ~/.pyenv/versions/miniforge3-latest/etc/profile.d/conda.sh && mamba activate earthcatalog
cd ~/github/nasa-itslive/earthcatalog
pytest -q -m "not integration and not performance and not e2e"   # expect: 320 passed

# live dry-run / ingest (scratch only!)
export AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)"
export AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)"
export AWS_SESSION_TOKEN="$(aws configure get aws_session_token)"
export EARTHCATALOG_CATALOG_KEY="test-space/stac/refactoring/warehouse/earthcatalog.db"
export EARTHCATALOG_LOCK_KEY="test-space/stac/refactoring/warehouse/.lock"
python -m earthcatalog.cli ingest \
  --diff s3://its-live-data/test-space/stac/refactoring/diffs/new-20260905-20260906.parquet \
  --warehouse s3://its-live-data/test-space/stac/refactoring/warehouse \
  --catalog /tmp/ec_refactoring.db --mode delta --limit 200 --dry-run
```
