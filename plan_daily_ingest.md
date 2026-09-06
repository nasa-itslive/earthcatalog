# Daily Ingest — Bulletproofing Plan

Status: **Proposed** (2026-09-05)
Branch: `feature/garbage-collection`
Supersedes the daily-ingest portions of `plan_simplification.md`. The
structural refactor (one ingest module, unified index, slim facade) is
treated as landed; this plan closes its durability gaps and collapses the
daily pipeline to one command.

---

## 0. Why: audit findings this plan fixes

Each defect below is mapped to the phase that fixes it. File refs are
current `main`-of-branch state.

| # | Defect | Evidence | Severity | Phase |
|---|---|---|---|---|
| A1 | `--mode full` drops the table but never resets the index → a "rebuild from scratch" of an existing warehouse ingests **nothing** | `catalog.py:839-861` | critical | 3 |
| A2 | Resume check re-downloads and re-parses the **entire index Parquet per inventory item** (`contains_source_key` → `known_source_keys` → full read, uncached); `append` rewrites the whole file per batch | `ingest.py:106`, `index.py:136-138`, `index.py:101-124` | critical at scale | 1 |
| A3 | Crash windows inside the commit (after `add_files`, before `index.append`; and Stage B before NDJSON delete) produce **permanent duplicate rows** — the compensating "compaction dedups on id_hash" does not exist in the ingest path | `ingest.py:153-159`, `ingest.py:206-234` | critical | 2 |
| A4 | Unregistered crash-orphan files get **resurrected** as duplicate rows by any later `rebuild_iceberg_from_warehouse` (registers every Parquet found) | `rebuild.py:119-124` | high | 2 |
| A5 | `DaskIngester` workers fetch every shard key with **no index check** — distributed re-runs re-fetch and re-write everything | `ingest.py:348-352` | high | 5 |
| A6 | No locking on ingest/GC; only `scripts/consolidate.py` takes the `S3Lock`. Concurrent runs lose index rows (last-write-wins on whole-file rewrite) | `run_backfill.py`, `catalog.py:903-992` | high (latent: workflows disabled) | 4 |
| A7 | Installed CLI is broken: `cli.py` imports `scripts.run_backfill`, but wheels package only `earthcatalog*` | `cli.py:131`, `pyproject.toml:60-62` | high | 0 |
| A8 | `migrate_indices()` is marked ✅ done in `plan_simplification.md` but **does not exist**; index path is re-derived by convention in 4 places; `unique_item_count` prefers the legacy table property while ingest/GC use the conventional path | grep (no hits); `catalog.py:206`, `catalog.py:858`, `catalog.py:954`, `cli.py:221` | medium | 3 |
| A9 | NDJSON intra-run resume (`if ndjson_exists_for(key): continue`) was planned but not implemented — a Stage-A crash re-fetches the whole run; only `--skip-fetch` (all-or-nothing) exists | `ingest.py:100-118` | medium | 2 |
| A10 | `test_rerun_is_noop` is vacuous (second `_run()` builds a fresh store; asserts on the first run's objects); the DoD fault-injection test ("kill after every item") does not exist | `tests/test_ingest.py:95-104` | medium | 0, 2 |
| A11 | Items without `datetime` get `year=0` in the index but live in `year=unknown/` directories → GC looks in the wrong partition and never physically removes those rows | `ingest.py:434`, `gc.py:109-120` | low | 6 |
| A12 | Doc drift: status header contradicts the status table; `compact.py` / `store.py` from the target architecture never landed (compaction = `scripts/consolidate.py`; `store_config` globals still set) | `plan_simplification.md` | low | 0 |

## 1. Goal: the daily path after this plan

One command, one process, one lock:

```
earthcatalog ingest \
  --inventory s3://…/inventory/…/manifest.json \
  --warehouse s3://its-live-data/test-space/stac/catalog/warehouse \
  --mode delta
```

The command diffs the inventory against the index **in-process** (the
index is the resume checkpoint — no intermediate delta file is needed for
correctness), fetches/fans-out/writes only unknown keys, commits per batch
journal-first, and exits with a machine-readable summary also written to
`{warehouse}/_last_run.json`.

**Consistency model (explicit, testable):**

1. **Single writer.** `S3Lock` around every ingest and GC run (the lock
   already exists at `catalog.py:994` — it is simply never acquired), plus
   `concurrency:` groups in the three workflows. The lock is advisory;
   the concurrency groups are the real guard.
2. **Every commit window is recoverable.** Ingest commit order is
   journal → files → `add_files` → `index.append` → journal-delete, and a
   run start performs recovery for any unfinished journal (§2.2).
3. **Best-effort single copy at ingest; enforced single copy weekly.**
   Transient duplicates are possible only inside crash windows *before*
   recovery; the weekly consolidation pass (dedup on `id` across merged
   files) is the enforcement. Between a crash and the next consolidation a
   duplicate may be **visible in search** — accepted, bounded, and
   reported by the audit (§5).
4. **Re-run is idempotent at every granularity** — item, batch, run.
   Re-running after any crash converges to the same final state as a
   clean run (post-consolidation).
5. **Bounded cost.** The index file is read once per run (not per item);
   memory for the resume set is documented and configurable (§2.1).

### Goals
1. `earthcatalog ingest --mode delta` is the *only* daily path (single-node
   default; `--scheduler coiled|local` for distributed) — and it works from
   a pip-installed wheel.
2. Crash at any point → re-run the same command → no loss, no permanent
   duplicates.
3. `--mode full` really rebuilds (table **and** index reset).
4. One source of truth for the index path; legacy warehouses migratable.
5. Suite proves it: the fault-injection matrix (§3 Phase 2) runs in CI.

### Non-Goals
- Iceberg schema, partition spec, and `grids/` are unchanged.
- NDJSON staging stays available (`--stage ndjson`) for PGSTAC interchange;
  it is no longer the daily default.
- No cross-region/HA work; the SQLite catalog single-writer model stays.
- `store_config` global removal stays parked (the allowlist guard already
  prevents spread; the facade takes explicit stores).

## 2. Design changes

### 2.1 Run-scoped membership + streaming anti-join (fixes A2)

The resume structure is a **sorted array of 16-byte key hashes**, not a
Python set. `Index` gains one accessor:

```python
def key_hash_array(self) -> pa.Array:   # fixed_size_binary[16], sorted
    """xxh3_128(s3_key) for every ACTIVE row, sorted ascending."""
```

built by streaming the `s3_key` column in batches (the Parquet is
columnar, so this never materializes the other columns). The `Ingester`
loads it once per run and tests membership in vectorized batches:

```python
known = self._index.key_hash_array()                 # one full read per run
for batch in chunked(inventory, 10_000):
    hashes = [xxh3_128(f"s3://{b}/{k}") for b, k in batch]
    known_mask = np.searchsorted(known_view, hashes) hit-mask
    for (b, k), is_known in zip(batch, known_mask):
        if is_known: continue
        ...
```

Why this shape and not the alternatives:
- `set[str]` of s3_keys ≈ 110 B/row → ~5 GB at 45M rows (uncomfortable on
  a 16 GB runner). The sorted array is 16 B/row → **0.72 GB at 45M rows,
  3.2 GB at 200M**.
- `pc.is_in(batch, value_set=…)` rebuilds the value-set hash table on
  every call — quadratic across batches at 45M. Batched
  `np.searchsorted` over a `void(16)` view of the Arrow buffer is
  O(log N) per key with no per-call setup.

Soft-deleted rows are excluded when building the array, so an item GC
removed and upstream re-added is diffed as new (matches the legacy
`daily_delta.py` behavior via `hash_set()`).

New keys discovered during the run are appended to a small in-memory
`known_new` sorted list (or simply re-checked against the journal —
within one run a key appears once, so the within-run cache is a
correctness nicety, not a scale concern).

`index.append` is unchanged (whole-file rewrite per batch; daily write
volume is bounded by delta size, not catalog size). The one-time cost of
hashing 45M `s3_key` strings during array construction (~30–60 s of
Python-loop `xxh3_128`) is accepted; *storing* an `s3_key_hash` column to
skip it is parked unless Phase 6 measurements say otherwise.

### 2.1.1 Daily run budget on a GitHub-hosted runner

`ubuntu-latest` = 4 vCPU / 16 GB RAM / 14 GB disk. At a 45M-row index and
a 50k-item daily delta:

| Step | Cost |
|---|---|
| GET index parquet (once) | ~2–3 GB from us-west-2, 1–4 min |
| Build membership array (stream + hash + sort) | ~1 min CPU, 0.7 GB RSS |
| Stream inventory + anti-join | 5–15 min (parquet-decode bound), O(batch) memory |
| Fetch + ingest 50k items | serial ≈ 25–40 min; with a bounded 16-thread fetch pool ≈ 3–6 min |
| Commit (journals → add_files → append) | minutes — bounded by delta size |
| **Total** | **~15–30 min** peak RSS ~1–2 GB; set `timeout-minutes: 120` against the 6 h job ceiling |

The streaming rule is absolute: nothing in the diff path may materialize
the inventory. (The legacy `scripts/daily_delta.py` violates this —
`_stream_inventory_hashes` returns the *entire* inventory as Python lists
before the anti-join, ~10 GB at 45M keys; it is retired in §2.3, not
ported.)

### 2.2 Batch journal — closing the commit windows (fixes A3, A4, A9)

A write-ahead journal per fetch batch, under
`{warehouse}/_staging/journal/{run_id}/{batch_id}.json`:

```json
{
  "source_keys": ["s3://…"],      // written BEFORE fetching the batch
  "files":    ["warehouse/grid_partition=…/part_<uuid>.parquet"],  // appended after EACH file write
  "index_rows": [ … ]             // appended together with `files`
}
```

**Lifecycle:** write journal (keys only) → fetch + fan-out + write files,
updating the journal after each file → `table.add_files(...)` → filter
`index_rows` against `index.hash_set()` (defensive no-op on the happy
path) → `index.append(...)` → delete journal → (ndjson stage: delete
NDJSON, then journal).

**Recovery at every run start** (idempotent, cheap — lists the journal
prefix; the registered-file set is computed from
`table.inspect.files()` only when a journal lists files):

| Journal state | Registered? | Meaning | Action |
|---|---|---|---|
| all rows in index | — | commit completed, crash before journal delete | delete journal |
| rows not in index | files **are** registered | crash inside the window (`add_files` done, append not) | append rows → delete journal |
| rows not in index | files not registered | crash before commit | delete the listed files → re-process batch |
| journal has keys, no files | — | crash mid-fetch/write | re-process batch (bounded re-work: `batch_size` items) |

This never relies on `add_files` idempotency and never registers a file
twice. Stage A of ndjson mode uses the same journal purely as an
intra-run resume marker (keys-only → re-fetch at most one batch), which
implements the planned-but-missing `ndjson_exists_for(key)` skip (A9).

Residual risk, stated honestly: a crash in the microseconds between a
file write and its journal update leaks one unregistered file whose name
is unknowable. It is invisible to search until a rebuild resurrects it.
Mitigations: weekly consolidation audit reports rows-vs-index per
partition (visibility), and `_staging/` is swept after recovery.

### 2.3 Daily simplification: collapse the two-job split (fixes —)

`scripts/daily_delta.py` exists because resume used to be impossible
without a materialized delta file. Once the index is the checkpoint
(§2.1), the "produce-delta" job is a **report**, not a dependency:

- Daily becomes the single command in §1 over the raw manifest: the
  streaming anti-join (§2.1) *is* the diff — it yields unknown keys
  directly into the ingest loop. No intermediate delta file exists on the
  correctness path.
- The pending-delta accumulation, inventory caches, and
  `delta_YYYY-MM-DD.parquet` machinery are retired. `earthcatalog delta
  --dry-run` keeps a diff *report* (N inventory items, M new, per-day
  counts) by running the same generator with counters and no writes.
- This also removes the daily path's dependence on filename-derived
  `id_hash` (`daily_delta.py:126-129` assumes `id == filename stem`); the
  `s3_key`-based membership in the `Ingester` is the robust key.

### 2.4 Full mode reset (fixes A1)

`mode="full"` drops the table **and** deletes the index object and
`_staging/` before recreating. Test: full mode against a populated
warehouse re-ingests everything.

### 2.5 Index path: one source of truth (fixes A8)

- `get_or_create` and `bulk_ingest` write table property
  `earthcatalog.index_path`.
- One resolver, used by `bulk_ingest`, `garbage_collect`,
  `unique_item_count`, and `cli info`:
  `index_path property → legacy earthcatalog.hash_index_path property →
  conventional {warehouse}_index.parquet`.
- Implement `migrate_indices()` for real (sidecar write, validate row
  counts against the sum of inputs, atomic swap, old files kept until a
  green GC run — the validation scheme from `plan_simplification.md` §8),
  then schedule the one-shot production run.

### 2.6 DaskIngester pre-filter (fixes A5)

The head loads the run-scoped set once (§2.1) and filters shards before
`client.map`:

```python
shards = [[(b, k) for b, k in shard if f"s3://{b}/{k}" not in known] for shard in shards]
```

Workers stay write-only; the head remains the sole committer. A fully
filtered shard short-circuits (no tasks scheduled).

### 2.7 Locking and workflow hygiene (fixes A6)

- `bulk_ingest` and `garbage_collect` wrap their bodies in
  `self.lock(owner="ingest"|"gc")` (exists at `catalog.py:994`);
  consolidation keeps its own lock.
- Add `concurrency: { group: earthcatalog-${{ github.event_name }}, cancel-in-progress: false }`
  to `daily_delta.yml`, `garbage_collect.yml`, `consolidate.yml`, and
  document schedule separation (delta daily; GC + consolidate weekly,
  offset).

### 2.8 Packaging and module direction (fixes A7)

- Move `scripts/run_backfill.py::run` (store building, client
  resolution, Coiled env plumbing) into `earthcatalog/ingest_pipeline.py`;
  `cli.py` imports it from the package; `scripts/run_backfill.py` becomes
  a thin shim (argv → package call). This restores the originally planned
  dependency direction (CLI → package) and fixes the wheel.
- CI gains a wheel smoke test: build sdist/wheel, install into a fresh
  venv, assert `earthcatalog --help` and `earthcatalog ingest --help`.

### 2.9 Small fixes (A10, A11, A12)

- Rewrite `test_rerun_is_noop` to re-run against the *same* store/index.
- `year` sentinel: index rows store `year` as stored-in-partition
  (`"unknown"` → nullable int or string column) so GC's partition lookup
  matches the physical layout.
- Delete dead `_GC_FILE_RE` (`gc.py:41`).
- Reconcile `plan_simplification.md` (status header vs table; point
  deferred items at this plan).

### 2.10 Bounded fetch parallelism

Fetch is the daily run's long pole: 50k serial `_fetch_fn` calls at
~30 ms each is 25–40 minutes; the anti-join and commit are minutes. Wrap
the fetch in a bounded `ThreadPoolExecutor(max_workers=fetch_workers)`
(16 default) *inside* the batch loop — fan-out, file writes, `add_files`,
and `index.append` stay on the main thread, so the journal and commit
model is untouched. Item order within a batch is irrelevant (fan-out
regroups by cell/year). Dask mode keeps its existing shard parallelism.

## 3. Phases (TDD — red → green → commit, suite green throughout)

**Phase 0 — Truth & packaging** *(no behavior change)*
A7, A10 (first half), A12: move the pipeline into the package; scripts
become shims; wheel smoke test in CI; make `test_rerun_is_noop` real
(same-store re-run); reconcile the old plan doc.

**Phase 1 — Streaming anti-join + membership array** *(A2)*
`Index.key_hash_array()` + batched `searchsorted` membership in
`Ingester` (and the DaskIngester head). Tests: (a) array equals brute-force
`sorted(hash(k) for k in known_source_keys())` on a small fixture;
(b) batched membership agrees with set membership on mixed known/unknown
batches; (c) guard: wrap `obstore.get` with a counter over a synthetic
1M-row index and assert the index file is fetched **O(1) times per run**,
not per item; (d) the inventory iterator is consumed lazily (a generator
that raises after N items proves nothing materializes it up front).

**Phase 2 — Batch journal + recovery + the DoD matrix** *(A3, A4, A9, A10)*
Implement §2.2 in `Ingester` (both stages; DaskIngester inherits via the
shared commit path). Then the plan_simplification DoD, made real —
fault-injection matrix on `MemoryStore`, each cell asserts: same item-id
multiset as a clean run, same index rows, no leftover journals/NDJSON,
and no unregistered files under the warehouse prefix:

| Inject failure after | direct | ndjson | dask |
|---|---|---|---|
| Nth fetch | ✅ exists | new | new |
| file write, pre-journal-update | new | new | new |
| journal update, pre-`add_files` | new | new | new |
| `add_files`, pre-`append` | new | new | new |
| `append`, pre-journal-delete | new | new | new |
| commit, pre-NDJSON-delete | — | new | new |
| crash, then **recovery-only** re-run (`--skip-fetch`-equivalent) | new | new | new |

**Phase 3 — Full mode + index path + migration** *(A1, A8)*
§2.4 and §2.5 with tests: full-mode re-ingest; resolver fallback order;
`migrate_indices()` against fixtures of the legacy
`*_id_hashes.parquet` / `*_source_index.parquet` formats, idempotency
test (re-run detects already-migrated), row-count validation.

**Phase 4 — One-command daily path + locking** *(A6, §2.3, §2.10)*
`earthcatalog ingest` consumes the manifest directly (delta producer
demoted to `earthcatalog delta --dry-run` report); daily default
`stage=direct`; bounded fetch pool (`--fetch-workers`); lock acquisition
in `bulk_ingest`/`garbage_collect`; `concurrency:` groups in the three
workflows. Tests: second concurrent ingester raises `CatalogLocked` (the
lock module's existing error); fetch pool preserves batch semantics
(all items fetched before flush; failures propagate and leave the
journal for recovery). Ship the daily workflow recipe:
`scheduler=synchronous --fetch-workers 16`, `timeout-minutes: 120`,
`concurrency` group, and upload `_last_run.json` as a workflow artifact.

**Phase 5 — Distributed resume** *(A5)*
Head-side shard pre-filter; distributed fault tests with the fake client
(re-filter + re-run converges); assert zero fetch calls for known keys.

**Phase 6 — Sweep**
A11 year sentinel; dead `_GC_FILE_RE`; run the daily command once
against the real warehouse in dry-run and record actual GET time,
membership-array build time, and peak RSS into §2.1.1; final docs pass;
only then consider enabling the workflows.

## 4. Definition of done

- [ ] Fault-injection matrix (Phase 2) green in the default CI suite (MemoryStore, seconds).
- [ ] `--mode full` on a populated test warehouse re-ingests everything.
- [ ] Wheel-installed `earthcatalog ingest --help` works in a clean venv.
- [ ] Two concurrent ingests: exactly one proceeds, the other exits with `CatalogLocked`.
- [ ] Every ingest run writes `{warehouse}/_last_run.json` (keys considered, skipped, fetched, files, rows, duration).
- [ ] `plan_simplification.md` status reflects reality; this plan's boxes checked as landed.
- [ ] Pre-commit suite (`pytest -m "not integration and not performance and not e2e"`) green; integration suite still passing against the public bucket.

## 5. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Membership memory / runner headroom | Sorted hash array is 16 B/row: 0.72 GB at 45M rows, 3.2 GB at 200M — fine on a 16 GB runner. Past ~200M rows, shard the anti-join by hash prefix across runs or move the diff to Coiled (parked). Peak RSS measured in Phase 6 before enabling workflows. |
| Journal orphans from interrupted *old* runs | Recovery scans the whole `_staging/journal/` prefix (any run_id), not just the current run; sweep only after recovery succeeds. |
| `S3Lock` is advisory (TTL expiry mid-run) | Long TTL (12 h default), owner + heartbeat not required for v1; workflow `concurrency` groups are the primary guard; document that manual simultaneous dispatches are the remaining hole. |
| Consolidation only dedups what it merges (small-file tail) → duplicates inside large files persist | Add `--audit` to consolidation: per-partition GeoParquet row count vs `Index.count_active()`, reported to `_last_run.json`; discrepancies are the crash-orphan signal. |
| Transient duplicates visible in search between crash and weekly consolidation | Accepted explicitly (§1.3); recovery on the next run closes the window; audit makes it observable. Never silent. |
| `migrate_indices()` runtime on 45M rows | One-shot, sidecar + validated swap; dry-run mode prints timings; schedule in a maintenance window. |
| `add_files` of a partially-written file | Files are written via single `obstore.put` (atomic object creation) before journaling; `add_files` only ever sees complete objects. |

## 6. Out of scope (parked)

- Iceberg schema / partition spec / grid partitioners.
- `store_config` global removal (guard test already prevents spread).
- PGSTAC export format details (ndjson stage preserved as-is).
- Zero-copy inventory streaming, `bokeh` removal, consolidation/daily
  workflow merge (beyond concurrency groups).
- Compaction of *large* files (dedup inside files above the consolidation
  threshold) — the audit makes the need, if any, measurable first.
