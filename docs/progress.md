# EarthCatalog — Progress Log

## Goal

Build the `earthcatalog` Python package: a single-node (and eventually Dask-distributed) pipeline that reads an AWS S3 Inventory file (CSV, CSV.gz, or Parquet), fetches STAC JSON files directly from a public S3 bucket via obstore, transforms them into a spatially-partitioned PyArrow table (H3 grid fan-out), and appends them to a PyIceberg table backed by a local SQLite catalog. The catalog.db will eventually be shuttled to/from S3 with an atomic S3 lockfile. The design is: **first make it work correctly and memory-safely on a single node, then scale to Dask**.

---

## Environment & Key Constraints

- Use **mamba** (not conda) for virtual environment commands. Environment name: `itslive-ingest`
- The S3 bucket `its-live-data` is **public** — use `obstore` with `skip_signature=True`, no AWS credentials needed
- **Never use boto3** — all S3 I/O goes through `obstore`. boto3 is in the environment only to satisfy PyIceberg's internal extras
- STAC items live at prefix `test-space/velocity_image_pair/nisar/v02`, files end in `.stac.json`
- Float fields `date_dt` and `percent_valid_pixels` from the catalog must be **rounded to int32** (catalog sends `5.0`, we store `5`)
- H3 partitioning must use **boundary-inclusive** cell assignment: interior cells via `h3.geo_to_cells` UNION boundary cells via densified ring walk with `h3.latlng_to_cell`; **Points** use `h3.latlng_to_cell` directly (`h3.geo_to_cells` crashes on Point geometry)
- Default H3 resolution is **3** for production; tests use **resolution 2**
- Colon characters in STAC property names are sanitized: `proj:code` → `proj_code`, `sat:orbit_state` → `sat_orbit_state`
- `pyiceberg-core` extra must be installed: `pip install "pyiceberg[pyiceberg-core]"` — required for `YearTransform`
- For the S3 lock, use **S3 Conditional Writes** (`If-None-Match: *` via `obstore.put(..., mode="create")`) — atomically safe, no DynamoDB needed
- All I/O goes through the injected store from `core/store_config.py` — default is `LocalStore`, swap to `S3Store` for production or `MemoryStore` for tests
- `obstore` raises `FileNotFoundError` (builtin) for missing keys, **not** `obstore.exceptions.NotFoundError`
- DuckDB `iceberg_scan()` on a local path requires `SET unsafe_enable_version_guessing = true` before the query
- **Do not use async / asyncio** for fetching — `ThreadPoolExecutor` + synchronous `obstore.get()` is sufficient

---

## Key Discoveries

- `obstore.get(store, key)` returns a `GetResult`; to get bytes: `bytes(result.bytes())`
- `obstore.put(..., mode="create")` implements `If-None-Match: *`; raises `AlreadyExistsError` on conflict. Works natively on `MemoryStore` and `LocalStore` for testing
- `obstore` raises `FileNotFoundError` (builtin), not `obstore.exceptions.NotFoundError`
- `LocalStore` requires the root directory to exist before instantiation
- `h3.geo_to_cells` only accepts Polygon/MultiPolygon — crashes with `ValueError` on Point geometry. Fixed with `geom.geom_type == "Point"` branch
- Empty geometries (e.g. `Point([])`) survive `shapely.geometry.shape()` but crash inside the partitioner — try/except wraps both `shape()` and `get_intersecting_keys()` together
- PyIceberg reads `geometry` (BinaryType) back from Iceberg as `pa.binary()`, not `pa.large_binary()` — test asserts `in (pa.binary(), pa.large_binary())`
- `pyiceberg-core` (Rust extension) is required for `YearTransform`
- `setuptools.build_meta` must be used in `pyproject.toml` (not `setuptools.backends.legacy:build`)
- 2000 STAC items from real S3 → 29,199 rows (fan-out ~14.6x at H3 resolution 3)
- The old `list[dict]` + `pa.Table.from_pylist` pattern in `process_chunk` had ~3× peak RAM vs output size. Replaced with 22 per-column flat Python lists + `pa.array(col, type=T)` per column — ~1× peak RAM
- AWS S3 Inventory Parquet files contain extra columns (`size`, `storage_class`, etc.) that must be ignored; only `bucket` and `key` are needed — use `iter_batches(columns=["bucket", "key"])`
- Async fetching inside the single-node pipeline was analysed and **rejected**: obstore already uses Tokio internally; ThreadPoolExecutor is simpler and works cleanly inside Dask workers without event loop management issues
- Producer/consumer pipeline overlap was also **rejected**: Dask provides this at task-graph level for free; implementing it single-node creates complexity that must be unwound when Dask is added

---

## Accomplished

### Completed and working — 63 tests, all passing

**Core package:**
- `earthcatalog/core/schema.py` — PyArrow schema (22 fields, correct types)
- `earthcatalog/core/partitioner.py` — `AbstractPartitioner` ABC with `get_intersecting_keys()` + `key_to_wkt()`
- `earthcatalog/core/transform.py` — `process_chunk()` with **column-oriented Arrow build** (per-column lists, not `list[dict]`); `_parse_ts()`, `_to_int()`; per-item values pre-computed once, appended N times across fan-out
- `earthcatalog/core/catalog.py` — `open_catalog()`, `get_or_create_table()`, `download_catalog()`, `upload_catalog()` (all via obstore)
- `earthcatalog/core/lock.py` — `S3Lock` context manager using `obstore.put(..., mode="create")`
- `earthcatalog/core/store_config.py` — global store config, defaults to `LocalStore("/tmp/earthcatalog_store")`

**Grids:**
- `earthcatalog/grids/h3_partitioner.py` — boundary-inclusive H3, handles Point geometry, `key_to_wkt()` via `h3.cell_to_boundary`, default res=3
- `earthcatalog/grids/geojson_partitioner.py` — STRtree R-tree, loads boundaries via obstore (local or `s3://`), `key_to_wkt()`
- `earthcatalog/grids/__init__.py` — `build_partitioner(GridConfig)` factory (h3, geojson; s2 raises NotImplementedError)

**Config & CLI:**
- `earthcatalog/config.py` — `AppConfig` / `GridConfig` / `IngestConfig` dataclasses + `load_config(path)` YAML loader
- `earthcatalog/cli.py` — Typer `earthcatalog incremental` command; accepts `--config` + per-flag overrides on top of config

**Pipeline:**
- `earthcatalog/pipelines/incremental.py` — streaming inventory reader (`_iter_inventory_csv`, `_iter_inventory_parquet`, `_iter_inventory` dispatcher); `run()` + `run_from_config()`; **never loads full inventory file into RAM**
  - Local CSV: `open()` + `csv.reader` line-by-line
  - Local CSV.gz: `gzip.open()` line-by-line
  - S3 CSV/CSV.gz: downloaded once via obstore, stream-decompressed via `gzip.open(BytesIO(...))`
  - Parquet (local or S3): `pq.ParquetFile.iter_batches(batch_size=65536, columns=["bucket","key"])`

**Tools:**
- `earthcatalog/tools/make_test_inventory.py` — lists public S3 prefix, writes inventory CSV

**Package config:**
- `pyproject.toml` — `pyyaml`, `typer` deps; both `earthcatalog` (Typer) and `earthcatalog-ingest` (argparse) CLI entry points
- `environment.yml` — includes `pytest`, `pyiceberg[pyiceberg-core]`

**Tests (63 total, all passing):**
- `tests/test_transform.py` (22 tests) — `_parse_ts`, `_to_int`, `process_chunk` including column-oriented build regression, fan-out correctness, 50-item batch
- `tests/test_h3_partitioner.py` (8 tests) — boundary-inclusive, Point, resolution, no duplicates, `key_to_wkt`
- `tests/test_inventory.py` (14 tests) — CSV with/without header, CSV.gz, quoted values, empty lines, Parquet (basic, order, batch boundary crossing, extra columns ignored, empty file), dispatch for all three formats
- `tests/test_lock.py` (8 tests) — acquire/release, context manager, stale override, race — all via `MemoryStore`
- `tests/test_catalog_lifecycle.py` (3 tests) — download/upload roundtrip via `MemoryStore`
- `tests/test_roundtrip.py` (7 tests) — full `process_chunk → table.append → scan` cycle

**Documentation:**
- `docs/DESIGN_v3.md` — complete rewrite reflecting actual implementation; all boto3/requests/s3fs references removed; covers streaming inventory, column-oriented transform, Parquet inventory support, deferred Dask backfill

---

## Repository Layout

```
earthcatalog/                              # repo root
├── pyproject.toml
├── environment.yml
├── docs/
│   ├── DESIGN_v2.md                       # old design doc (partially stale, superseded)
│   ├── DESIGN_v3.md                       # current reference architecture
│   └── progress.md                        # this file
├── tests/
│   ├── test_transform.py                  # 22 tests
│   ├── test_h3_partitioner.py             # 8 tests
│   ├── test_inventory.py                  # 14 tests
│   ├── test_lock.py                       # 8 tests
│   ├── test_catalog_lifecycle.py          # 3 tests
│   └── test_roundtrip.py                  # 7 tests
└── earthcatalog/
    ├── cli.py
    ├── config.py
    ├── core/
    │   ├── schema.py
    │   ├── partitioner.py
    │   ├── transform.py
    │   ├── catalog.py
    │   ├── lock.py
    │   └── store_config.py
    ├── grids/
    │   ├── __init__.py
    │   ├── h3_partitioner.py
    │   └── geojson_partitioner.py
    ├── pipelines/
    │   └── incremental.py
    └── tools/
        └── make_test_inventory.py
```

---

## Not Yet Implemented (deferred)

- `pipelines/backfill.py` — Dask distributed version (design settled in DESIGN_v3.md §9)
- `maintenance/compact.py` — Iceberg sort/compact
- `grids/s2_partitioner.py` — S2 grid backend
- `tests/test_pipeline_e2e.py` — end-to-end test: calls `run()` with a local inventory CSV pointing to local STAC JSON files (no real S3), verifies rows appear in an Iceberg scan. **This is the immediate next task.**
