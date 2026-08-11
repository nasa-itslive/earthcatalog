# Architecture Simplification Plan

Status: **In progress — Phases 0–4 done, Phase 6 partially done**
Branch: `feature/garbage-collection`

## Implementation status

| Phase | Status |
|---|---|
| 0 — Guard tests (`tests/test_architecture.py`) | ✅ done |
| 1 — Extract `schema.py` + `inventory.py` | ✅ done |
| 2 — Unified `index.py` (merges hash + source index) | ✅ done |
| 3 — Resumable `ingest.py` (`Ingester`, direct + ndjson) | ✅ done |
| 4 — Rewire `EarthCatalog.ingest_resumable()` → `Ingester` | ✅ done |
| 5 — Unified-Index GC (`earthcatalog.gc`) | ✅ done |
| 6a — `earthcatalog info` CLI subcommand | ✅ done |
| 6b — Search SQL dedup (`build_query`) | ✅ done |
| 7 — `migrate_indices()` + dead-code cleanup | ✅ done |
| 8 — `run_backfill` kwargs simplification | ✅ done — **callers migrated to Ingester** |

Also done while implementing: added `pyrightconfig.json` (fixes "could not be
resolved" type errors), installed dev extra `coiled`, fixed pre-existing ruff
errors in `tests/test_gc_bugs.py`, simplified `Ingester` kwargs.

**Deferred (explicitly, to avoid breaking things):**
- `run_backfill` is **deprecated** (docstring) and no longer used by any
  production caller: `EarthCatalog.bulk_ingest` and
  `scripts/run_backfill.py` now route through the resumable
  `Ingester`/`DaskIngester`.  The function itself remains for
  backward compatibility and its tests.
- CLI `incremental` options — deferred; `info` subcommand shipped as the
  pattern to follow.
- `migrate_indices()` shipped; the one-shot run in production (GC workflow)
  to fold old `*_id_hashes.parquet` / `*_source_index.parquet` files into
  the unified index still needs to be scheduled.

Goal: simplify an over-architected library into something rock-solid and
resumable, where ingest never loses progress, GC/consolidation/search keep
working, `info` is a first-class client command, and the two provenance
indices collapse into one.

---

## 1. Goals & Non-Goals

### Goals (must be true when done)
1. **Ingest is rock-solid and resumable.** A crash at any point can be
   recovered by re-running; no item is lost, no item is duplicated after
   compaction.
2. **One ingest code path** (not three).
3. **One provenance index** (merge `hash_index` + `source_index`).
4. **GC, consolidation, and search all work** against the simplified core.
5. **`info` is a CLI subcommand** (`earthcatalog info --catalog-s3 …`),
   not a standalone script.
6. **SOLID + TDD.** Each change lands with tests written first.

### Non-Goals (explicitly out of scope)
- Changing the on-disk GeoParquet schema written by `rustac`.
- Changing the Iceberg partition spec (`grid_partition` + `year`).
- Rewriting the grid partitioners (`grids/`).
- Distributed-execution provider work (Coiled stays supported, just via a
  thinner adapter).

---

## 2. Current-State Assessment (what's over-architected)

| Symptom | Evidence |
|---|---|
| **God object** | `catalog.py` is 1484 lines mixing Iceberg schema, grid metadata, catalog lifecycle, three search engines, ingest, GC, compaction, locking, and HTML rendering. |
| **Three ingest paths** | `incremental.run()` (715-line single-node), `backfill.run_backfill()` (1533-line 4-phase distributed), and `EarthCatalog.ingest()` (a *third* inline reimplementation in the facade). All duplicate fan-out/group/write logic. |
| **Inconsistent dedup** | `backfill` dedups via a Bloom filter in `_stream_compact`; `incremental.run` does **no** dedup (re-running creates duplicate rows); `EarthCatalog.ingest` also does no dedup. |
| **Resume is partial** | Only `backfill` resumes (4-phase staging with `skip_inventory`/`skip_ingest`/`retry_pending`). `incremental` and the facade's `ingest()` have no resume — a mid-run crash wastes all fetch work. |
| **Two indices that overlap** | `hash_index` stores `xxh3_128(stac_id)` (dedup). `source_index` stores the raw `stac_id` **plus** provenance. The hash is derivable from `stac_id`, so `hash_index` carries zero extra information. Two files, two update paths, two read paths. |
| **Duplicated search SQL** | `duck_search` and `search_uris` build nearly identical DuckDB SQL (geometry, datetime, CQL2→SQL) side by side. |
| **Global mutable state** | `store_config.py` is a module of globals; `bulk_ingest` save/restores them in a `try/finally`. Hard to test, unsafe under concurrency. |
| **Scripts reimplement CLI** | 6 scripts each ship their own `argparse` + `_get_store`/`_parse_s3_uri`/S3-auth boilerplate (≥5 copies). |
| **Duplicate entry point** | `earthcatalog-ingest` and `earthcatalog incremental` run the same pipeline via two code paths. |

---

## 3. Target Architecture

Keep the package **flatter**, not deeper. The split is by responsibility,
not by ceremony.

```
earthcatalog/
  __init__.py          # public API: open, EarthCatalog, ingest, search types
  config.py            # keep: dataclasses + YAML loader
  schema.py            # NEW (extracted): ICEBERG_SCHEMA, PARTITION_SPEC, constants
  store.py             # NEW: Store protocol + make_store(uri) helper (replaces store_config globals)
  lock.py              # keep, but drop store_config dep; take explicit store
  catalog.py           # SLIMMED: open(), CatalogInfo, get_or_create, download/upload lifecycle
  earthcatalog.py      # EarthCatalog facade: read-only query + orchestration (no inline logic)
  partitioner.py + grids/   # keep
  transform.py         # keep (fan_out, group_by_partition, write_geoparquet[_s3])
  index.py             # NEW: unified Index (merges hash_index + source_index)
  inventory.py         # extracted from incremental.py: _iter_inventory* readers
  ingest.py            # NEW: ONE resumable ingest pipeline (single + distributed adapter)
  search.py            # keep engine; split out a query builder to kill SQL dup
  gc.py                # pipelines/delete.py renamed + simplified to use unified index
  compact.py           # maintenance/compact.py promoted to top level
  cli.py               # typer: ingest | gc | compact | info | search  (subcommands)
```

`pipelines/` and `maintenance/` subpackages go away; their contents are
promoted to first-class modules. Fewer folders = less ceremony.

### Responsibility assignment (SRP)
- `schema.py` — *knows* the Iceberg schema/partition spec/property keys.
- `catalog.py` — *opens* a catalog (PyIceberg + table + grid metadata).
- `index.py` — *owns* provenance + dedup state.
- `inventory.py` — *streams* `(bucket, key)` pairs from S3 inventory.
- `ingest.py` — *orchestrates* fetch → fan-out → write → index, resumably.
- `search.py` — *queries*; one SQL builder, pluggable backend (rustac/duckdb).
- `gc.py` / `compact.py` — *mutate* the warehouse (delete orphans, merge files).
- `earthcatalog.py` (facade) — *composes* the above; no inline business logic.

---

## 4. Key Design Decisions

### 4.1 Unified Index (`index.py`) — merges `hash_index` + `source_index`

**One Parquet file** (`warehouse_index.parquet`) with schema:

| column | type | purpose |
|---|---|---|
| `id_hash` | `fixed_size_binary[16]` | `xxh3_128(stac_id)` — compact in-memory dedup set |
| `stac_id` | `string` | raw id — human-readable, used by GC cleanup |
| `s3_key` | `string` | provenance — **the cheap resume checkpoint + GC orphan key** |
| `grid_partition` | `string` | locates the GeoParquet file for GC |
| `year` | `int32` | locates the GeoParquet file for GC |
| `ingested_at` | `string` | bookkeeping |
| `deleted` | `bool` | soft-delete flag set by GC |

**Why `s3_key` is the primary resume key:** it is available from the
inventory *before* fetching the STAC JSON, so we can skip already-ingested
objects with zero extra I/O. `stac_id`/`id_hash` require fetching the JSON,
which is exactly the work we want to avoid re-doing.

**API (single responsibility):**
```python
class Index:
    def known_source_keys() -> set[str]      # cheap resume checkpoint
    def contains_source_key(s3_key) -> bool
    def append(rows: list[IndexRow]) -> int  # fan-out rows from one batch
    def stream_active() -> Iterator[IndexRow]# for GC
    def mark_deleted(stac_ids: set[str]) -> int
    def compact() -> int                     # drop deleted rows
    def hash_set() -> set[bytes]             # legacy dedup-set compat
```

**Migration:** a one-shot `migrate_indices()` reads the existing
`warehouse_id_hashes.parquet` + `warehouse_source_index.parquet` and writes
the unified file. Old files can be deleted after verification. The Iceberg
table property `earthcatalog.hash_index_path` becomes
`earthcatalog.index_path`.

### 4.2 Resumable Ingest (`ingest.py`) — one pipeline, two stages, NDJSON optional

Replace all three ingest paths with one. Two checkpoint mechanisms, each
serving a distinct purpose — they compose rather than conflict:

| Mechanism | Granularity | Purpose | Lifetime |
|---|---|---|---|
| **Unified Index** (`index.py`) | per **source key** (`s3://bucket/key`) | **cross-run** resume + dedup truth | permanent (in the warehouse) |
| **NDJSON staging** (optional) | per **(cell, year)** fan-out bucket | **intra-run** cache + **PGSTAC interchange** | ephemeral (deleted after compact) |

The index answers *"is this source object durably in the catalog?"* (avoids
re-fetching across days/runs). NDJSON answers *"have I already fanned these
items out, and can I hand them to PGSTAC?"* (avoids re-fetching within a run,
and exports an interchange format). NDJSON is **opt-in**; direct mode skips it.

#### Mode A — direct (`stage="direct"`, default for small/single-node)
```
for bucket, key in iter_inventory(...):
    if index.contains_source_key(f"s3://{bucket}/{key}"): continue   # cross-run resume
    item = fetch(key)
    fan_out(item) → group_by_partition → write_geoparquet_s3(...)    # one file per (cell,year)
    table.add_files([new_paths])
    index.append(rows)                                               # advance checkpoint
```
Fewest files, simplest. No PGSTAC artifact.

#### Mode B — staged NDJSON (`stage="ndjson"`, default for distributed/PGSTAC)
```
# Stage A — fetch + fan_out → NDJSON (the flow you like, kept)
for bucket, key in iter_inventory(...):
    if index.contains_source_key(f"s3://{bucket}/{key}"): continue   # cross-run resume
    if ndjson_exists_for(key): continue                              # intra-run resume
    item = fetch(key)
    append_to_ndjson(fan_out(item), per cell/year)                   # checkpoint + PGSTAC export

# Stage B — compact NDJSON → GeoParquet (dedup vs index.hash_set)
for cell, year in ndjson_buckets:
    if compact_done(cell, year): continue                            # intra-run resume
    write_geoparquet_s3(dedup(read_ndjson(cell, year), index))
    table.add_files([new_paths])
    index.append(rows for this bucket)                               # advance checkpoint
    delete_ndjson(cell, year)   # optional — keep for PGSTAC if desired
```
This **is** the current `backfill` flow (Phase 2 + Phase 3), but with the
unified index as the durable cross-run checkpoint and the chunk-parquet
Phase 1 replaced by streaming + `contains_source_key`.

#### Rock-solid guarantees (both modes)
1. **Crash before `index.append`** → re-run re-fetches; duplicates are
   removed by the next `compact` (dedups on `id_hash`). Eventually consistent.
   In Mode B, NDJSON artifacts survive the crash and skip re-fetching.
2. **Crash after `index.append`, before/after `add_files`** → Iceberg
   snapshots are atomic; `rebuild_iceberg_from_warehouse` repairs the catalog
   on the next run if a file is unreferenced.
3. **Invariant:** an entry in the index ⇒ the row is durable in the warehouse.
   The checkpoint advances only after `add_files` succeeds.

#### Distributed path (Coiled)
A thin `DaskIngester` adapter shards the inventory by source-key ranges; each
worker runs the same loop (Mode B) against the shared index + shared NDJSON
staging prefix. The 4-phase ceremony of the current `backfill` collapses to
"Stage A (map) → Stage B (map)".

#### Idempotency test (must pass before "done")
Run ingest twice against the same inventory in **both** modes; assert zero
new rows on the second run and zero duplicates after compaction.

#### How this differs from today
| | Today | Proposed |
|---|---|---|
| Ingest code paths | 3 (`incremental.run`, `backfill.run_backfill`, `EarthCatalog.ingest`) | **1**, with a `stage` flag |
| NDJSON staging | mandatory in `backfill`, absent elsewhere | **optional** (`direct` \| `ndjson`); on by default for distributed/PGSTAC |
| Cross-run resume | none — re-running re-fetches everything (only `backfill` skips within the same staging prefix) | **unified index** skips already-ingested source keys across any run, any day |
| Dedup | inconsistent: `backfill` dedups via Bloom in compact; `incremental`/facade do **no** dedup | one path, dedups via `index.hash_set()` in both modes |
| Provenance files | 2 (`_id_hashes.parquet` + `_source_index.parquet`) | **1** (`warehouse_index.parquet`) |
| PGSTAC interchange | implicit (the NDJSON from `backfill`) | explicit Mode B artifact, kept if you want it |

The fan-out→NDJSON→compact flow you like is **preserved** — it becomes Mode B.
What changes is: (a) the NDJSON stage is opt-in rather than mandatory,
(b) a single unified index replaces both the hash file and the source file and
adds cross-run resume that doesn't exist today, and (c) the three duplicated
ingest entry points become one.

### 4.3 Search — kill the duplicated SQL

Extract a `build_query(paths, geom, datetime_range, cql2_filter)` helper
that returns `(sql, params)`. `duck_search`, `search_uris`, and any future
search method call it. `search()` (rustac) and `search_to_arrow()` keep
using `_FileSearchEngine` unchanged (they don't share SQL with DuckDB).

### 4.4 Store — explicit, not global

Replace `store_config.py` globals with a `make_store(uri, *, anonymous=None)`
factory plus explicit `store` parameters everywhere. `EarthCatalog` already
holds a `self._store`; we stop reaching for module globals. `S3Lock` takes
an explicit `store` (its docstring already says the global path is
"deprecated").

### 4.5 CLI — one front door

`earthcatalog/cli.py` gains subcommands; each scripts/*.py becomes a thin
shim (or is deleted once the workflow calls the subcommand):

```
earthcatalog info     --catalog-s3 s3://…/earthcatalog.db
earthcatalog ingest   --inventory … --catalog … --warehouse … [--resume] [--since …]
earthcatalog gc       --inventory … --catalog …
earthcatalog compact  --catalog … --warehouse …
earthcatalog search   --catalog-s3 … --bbox … --datetime …
```

`scripts/info.py` logic moves into `catalog.info_report()` (a reusable
function) so the CLI and any notebook can call it. Drop the
`earthcatalog-ingest` entry point (duplicate of `earthcatalog ingest`).

---

## 5. Phased Implementation (TDD — tests first, small commits)

Each phase: **red** (write failing test) → **green** (minimum code) →
**refactor**. Commit per phase. Keep the suite green at every commit.

### Phase 0 — Scaffolding & guard tests
- Add architectural guard tests (import-time): assert module sizes stay
  bounded, assert no new `store_config` global usage in non-legacy modules.
- Pin the *current* behavior of ingest/GC/search with characterization tests
  (golden outputs on `MemoryStore`) so refactors are safe.

### Phase 1 — Extract `schema.py` and `inventory.py`
- Move Iceberg constants/schema/partition spec out of `catalog.py` into
  `schema.py` (pure data, trivial to test).
- Move the `_iter_inventory*` family out of `incremental.py` into
  `inventory.py`. Pure functions over `MemoryStore` → easy unit tests.
- `catalog.py` and `incremental.py` import from the new modules. No behavior
  change.

### Phase 2 — Unified `index.py`
- TDD the `Index` class against `MemoryStore`: append, contains, stream,
  mark_deleted, compact, hash_set, known_source_keys.
- Add `migrate_indices(old_hash_path, old_source_path) -> Index` with tests
  using fixtures of the current file formats.
- **Do not** wire it in yet — keep it parallel and fully tested.

### Phase 3 — Resumable `ingest.py` (both modes)
- TDD an `Ingester` class with a `stage` parameter (`"direct"` | `"ndjson"`).
- Direct mode first: fake inventory + `MemoryStore` + `Index`; assert items
  are written, indexed, and re-running is a no-op.
- NDJSON mode second: same asserts + assert per-(cell,year) NDJSON is
  produced in Stage A and consumed/deduped in Stage B; assert the NDJSON can
  be read back as PGSTAC-ready lines.
- Inject failures mid-run (mock `fetch_item` to raise after N items) in
  **both** modes; assert resume skips completed work and produces no
  duplicates after compaction. In NDJSON mode, assert a crash after Stage A
  resumes without re-fetching.
- Implement single-node execution; add the `DaskIngester` adapter for
  distributed (delegating to Dask `client.map`: Stage A map → Stage B map).

### Phase 4 — Rewire `EarthCatalog`
- `EarthCatalog.ingest()` delegates to `ingest.py` (delete the inline third
  copy). `bulk_ingest()` delegates to the distributed adapter.
- `EarthCatalog.garbage_collect()` and `.compact()` call `gc.py`/`compact.py`
  unchanged in behavior but reading the unified index.
- Slim the facade: move `_repr_html_` helpers and search-SQL building out.

### Phase 5 — GC + compact against the unified index
- Port `pipelines/delete.py` → `gc.py`; replace `hash_index` + `source_index`
  calls with `Index` methods.
- Port `maintenance/compact.py` → `compact.py` (top level).
- Keep the existing `test_delete.py` / `test_gc_bugs.py` passing; they
  already encode the GC invariants.

### Phase 6 — Search dedup + CLI
- Extract `build_query()`; rewrite `duck_search`/`search_uris` to use it.
- Add `earthcatalog info|ingest|gc|compact|search` subcommands; lift
  `scripts/info.py` into `catalog.info_report()`.
- Update workflows to call `earthcatalog <subcommand>` instead of
  `python scripts/*.py`; keep scripts as shims for one release.

### Phase 7 — Cleanup & migration
- Run `migrate_indices()` in the GC workflow (once) to fold the two old
  index files into one.
- Remove deprecated modules: `store_config` globals, `update_hash_index.py`,
  `scripts/archive/`, duplicate `_count_rows`, stale planning docs (per
  `cleanup_plan.md`).
- Drop the `earthcatalog-ingest` entry point.

---

## 6. Migration & Compatibility

- **Index format:** `migrate_indices()` is idempotent and detects already-
  migrated warehouses (unified file present). Old code paths keep working
  until the migration runs.
- **Iceberg table:** property rename `hash_index_path` → `index_path`, read
  with a fallback to the old key for one release.
- **Workflows:** updated in the same PR that ships the CLI subcommands.
  Scripts remain as thin shims so a rollback doesn't break CI.
- **No breaking public-API changes** to `earthcatalog.open` /
  `EarthCatalog.search*` / `.ingest()` signatures — internals move, callers
  don't.

---

## 7. Test Strategy

| Layer | Tooling | What we assert |
|---|---|---|
| Unit | `pytest`, `MemoryStore`/`LocalStore` | Index ops, inventory readers, query builder, resume state machine |
| Characterization | golden fixtures | Pre-refactor ingest/GC/search outputs unchanged post-refactor |
| Property | hypothesis (optional) | Index append/contains consistency; compact idempotence |
| Architecture | import-time guards | No regressions: God-object size, no new global-store usage |
| Integration (existing) | `pytest -m integration` | read-only S3 queries still pass |
| Resume | fault-injection unit tests | crash-at-each-step → re-run → no loss, no duplicates after compact |

Pre-commit keeps running `pytest -m "not integration and not performance and not e2e"`.

Definition of done for "ingest is rock-solid": a test that kills the ingester
after every single item, re-runs, and ends with the same final catalog as a
no-crash run (post-compaction).

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Unified index grows large (45M+ rows) | Keep `id_hash` (16 B) for the in-memory set; stream the file in row-group batches (already the pattern). `s3_key`-keyed checkpoint avoids loading the whole index. |
| Dropping NDJSON staging breaks Coiled throughput | NDJSON is **kept** (Mode B, default for distributed); not dropped. Direct mode (Mode A) is only the new default for small/single-node runs. Benchmark both on a real run before flipping workflow defaults. |
| Migration writes a bad unified file | `migrate_indices()` writes to a sidecar first, validates row counts against the sum of the two inputs, then swaps. Old files kept until a green GC run. |
| Behavior drift during refactor | Characterization tests (Phase 0) lock current outputs; every phase keeps the suite green. |
| Search SQL extraction changes results | `test_search.py` already covers `search`/`duck_search`/`search_uris`; add equality asserts on the new builder's SQL vs. the old inline SQL. |

---

## 9. Out-of-scope follow-ups (parked)
- `consolidate.yml` vs `daily_delta.yml` workflow overlap.
- True zero-copy streaming inventory reads (`obstore.GetResult.stream()`).
- Dropping the `bokeh` dependency (see `cleanup_plan.md`).
