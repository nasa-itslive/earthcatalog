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

## End-to-end status (2026-09-07, live on real S3)

The daily ingest works end to end on the current tree, verified against the
real mirrored inventory:

    earthcatalog ingest --diff s3://…refactoring/diffs/new-20260905-20260906.parquet \
        --warehouse s3://…refactoring/warehouse --mode delta

* dry-run projection: considered 27,458 · new 0 · already indexed 27,458 (full
  diff now ingested; anti-join recognizes everything)
* idempotent re-run: fetched 0 · considered 0
* catalog: 39,219 rows · 213 files · 27,458-item day fully ingested
* index layout: legacy file + per-run parts, DuckDB anti-join across both
* journals: none left after successful runs; recovery self-heals leftovers

**First real runner dispatch — DONE (2026-09-07, RPI-6 closed).** With AWS
secrets in the repo, `daily_delta.yml` (dispatch-only, defaults = scratch pad,
every write path a workflow input) was validated on `ubuntu-latest` in four
escalating runs against the known 2026-09-05→06 manifest pair:

| run | config | result |
|---|---|---|
| A | `dry_run` | ✅ 2m16s — `considered 27,458 · new 0 · already indexed 27,458`, matches local exactly |
| B | `limit=1000`, fresh `warehouse-ci` | ✅ 3m17s — 1,000 keys fetched; reconciliation `index 1,000 · Iceberg 1,342 rows / 30 files` |
| C | full delta | ✅ **15m15s total** — 26,458 fetched (resume after B), final `index 27,458 · Iceberg 39,219 rows / 386 files` |
| C2 | identical re-run | ✅ 4m33s — `new 0`, state byte-identical (idempotent) |

Run C v1 **caught a real production bug**: `run()` opened the local sqlite and
ran `get_or_create` *before* downloading the remote catalog db (the old
download was gated on the legacy `--delta` flag; the CLI passes `--mode
delta`), so a fresh process built a competing table and the first Iceberg
commit died with `branch main was created concurrently`.  Fixed in 91ea567:
download before open, unconditionally; the pipeline only fetches when no
local db exists.  The same bug would have forked the production catalog db
during the catch-up — fix first, then catch up.

**Catch-up — DONE and verified (2026-09-08).** All 394,964 keys ingested into
the real catalog in 8 durable 50k chunks (4h11m, per-batch catalog-db
uploads).  Final dry-run: `considered 394,964 · new 0`.  Catalog: 43,245,133
unique items (exact), Iceberg 68,067,891 rows / 11,822 files.  Pre-catch-up
db backed up at `refactoring/backups/earthcatalog-pre-catchup-20260907.db`.

**Production flip — applied (2026-09-08, user GO).** `daily_delta.yml`
defaults now target `catalog/` (warehouse, catalog db, lock, diffs) and run
on a daily 14:00 UTC schedule (manifests are stamped T01-00Z; 13h buffer).
The schedule takes effect once this branch merges to main.

**Stale-metadata repair — done (2026-09-08).** The PR's integration tests
exposed that the live `earthcatalog.db` still referenced the pre-migration
legacy layout: 4,473 `grid_partition=…` files (67.5M rows) that 404, while
the real data — migrated earlier into `grid=h3/level=1/tile=…` — sat
unreferenced.  Verified first: 11,822 v2 files on S3 hold exactly
68,067,891 rows (matching the metadata total to the row).  Then rebuilt the
table over the real files (fixing a doubled-prefix URI bug in
`_list_warehouse_keys` en route, b0299de) and uploaded.  Validation:
68,067,891 rows / 11,822 files, 300/300 spot-checked files exist, the exact
CI-failing Greenland query returns 28,713 items, per-decade searches
(1985→2026) all return results.  Backups:
`refactoring/backups/earthcatalog-pre-rebuild-20260908.db` and
`…pre-catchup-20260907.db`.  Remaining non-blockers: consolidate.py
v2-layout refresh + GC real-data deletion validation (both dry-run first).

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

## Decisions

- **RPI-2 (S3Lock wiring) CANCELLED — 2026-09-07.** There are no intra-run
  races to lock: each worker fans out its own shard, each worker compacts its
  own (cell, year) partition, and only the head commits to Iceberg/index.
  Across runs, the daily workflow's `concurrency` group is the guard. The
  residual hole — two *manual* simultaneous dispatches — is documented here
  rather than locked. The `S3Lock` module stays for consolidation, which has
  always used it. The lock wiring that was briefly added to ingest/GC was
  reverted.

## Realignment vs. the Rust/DuckDB/Iceberg architecture plan (2026-09-07)

Assessed `plan` (hash-bucketed normalized inventories → ItemIndex +
MaterializationIndex → one Iceberg transaction per run). Verdict:

**The daily ingest is NOT off the rails — it already implements the plan's
core.** Out-of-core diff (our DuckDB EXCEPT ≈ their bucketed ANTI JOIN;
benchmarked 432 s for a 2×43.2M-row day pair under a 10 GB cap), append-only
Iceberg path (small delta part files added by `add_files`, no rewrite of
400k-row bases), derived index separate from Iceberg, spatial×temporal
fan-out with multi-materialization, sorted per-partition GeoParquet,
runner-sized. Real-data verified end to end (see table above).

**Deliberate deltas (keep as-is):**
- One unified Index instead of ItemIndex + MaterializationIndex split — at
  50M rows one Parquet with `s3_key/stac_id/grid_partition/year` serves
  resume (ItemIndex role) *and* GC (Materialization role); splitting is an
  optimization without a current failure mode.
- Per-batch commit + journal instead of ONE commit per run — bounds crash
  re-work to one batch on the runner; the plan's whole-run commit trades that
  away for simplicity we don't need.
- `size + last_modified` identity instead of `etag` — the ITS_Live inventory
  schema has no etag column (manifest fileSchema: bucket, key, size,
  last_modified_date, storage_class, IT tier).
- Changed keys are reported, not re-ingested (user decision); full
  modification handling (swap old+new materializations) deferred until
  changes actually occur in practice.

**Adopt later, only on evidence:**
- Persisted hash-bucketed normalized inventories (`dt=/bucket=000..1023`):
  buys resumable/multi-day bucket diffs and sub-day increments; costs a daily
  ~5 GB normalized rewrite. Our one-query EXCEPT is 7 min/day — adopt only if
  the runner budget breaks or multi-day catch-ups become routine.
- Iceberg equality deletes for GC (current weekly file-rewrite GC is fine
  while deletes are rare).

**Where the refactor DID go too far (the distributed side):**
- The ndjson stage gives the Ingester a 2×2 matrix (direct/ndjson ×
  serial/dask); the stage-scope journal bug (RPI-1) was a symptom. ndjson
  exists for PGSTAC interchange — if nothing consumes it in production,
  retire it or confine it to the bulk profile behind `--stage ndjson`.
- The bulk/scatter profile should be scoped to what the plan says Dask is
  for: high-volume fan-out, large compactions, historical rebuilds — not the
  daily path (which it no longer touches).

## Backlog (RPI candidates, in priority order)

- **RPI-2 · Simplification pass on the ingest matrix** — one first-class
  serial path (direct); decide ndjson's fate (retire vs bulk-only); delete
  whatever has no production consumer.
- **RPI-3 · Changed-keys report** — count/flag re-uploaded-but-known keys in
  `_last_run.json` (diff rows whose key is already indexed).
- **RPI-4 · Consolidation `--audit`** — per-partition row counts vs
  `Index.count_active()` into `_last_run.json` (crash-orphan visibility,
  plan §5).
- **RPI-5 · Production backlog decision** — the real index at `catalog/` is
  394,964 keys behind; decide: run `migrate-indices` + a catch-up diff ingest
  there, or rebuild the test warehouse under refactoring first. Needs a user
  call.
- **RPI-6 · Enable workflows** — only after AWS secrets exist in the repo and
  RPI-1 lands; first run in `--dry-run`.
- **RPI-7 · Optional** — ndjson-stage fault cells (de-scoped by design),
  reverse diff (index↛inventory) for GC, hash-bucketed normalized inventories
  (see Adopt-later).

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
