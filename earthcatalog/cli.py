"""
EarthCatalog CLI.

Usage
-----
    # Full (bulk) ingest from an S3 inventory
    earthcatalog ingest --inventory s3://.../manifest.json --mode full

    # Incremental (delta) ingest
    earthcatalog ingest --inventory s3://.../delta/pending/delta_2026-04-28.parquet --mode delta

    # Catalog summary
    earthcatalog info --catalog /tmp/earthcatalog.db --warehouse s3://.../warehouse
"""

from __future__ import annotations

import typer

app = typer.Typer(
    name="earthcatalog",
    help="EarthCatalog STAC → Iceberg ingest tool.",
    add_completion=False,
)


# ---------------------------------------------------------------------------
# `ingest` sub-command — full (bulk) / delta ingest from an S3 inventory
# ---------------------------------------------------------------------------


@app.command()
def ingest(
    inventory: str | None = typer.Option(
        None,
        "--inventory",
        "-i",
        help="Path or s3:// URI to the S3 Inventory (CSV, Parquet, or manifest.json). "
        "Mutually exclusive with --diff.",
    ),
    diff: str | None = typer.Option(
        None,
        "--diff",
        help="Diff Parquet from `earthcatalog diff` (new/changed keys vs the previous "
        "inventory day) — the daily path.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Count the keys the run would fetch (diff vs index) and exit — no writes.",
    ),
    catalog: str = typer.Option(
        "/tmp/earthcatalog.db",
        "--catalog",
        help="Local SQLite Iceberg catalog path.",
    ),
    warehouse: str = typer.Option(
        "s3://its-live-data/test-space/stac/catalog/warehouse",
        "--warehouse",
        help="Warehouse root (s3:// URI or local path).",
    ),
    mode: str = typer.Option(
        "auto",
        "--mode",
        help="'full' (rebuild from scratch), 'delta' (incremental append), or 'auto'.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Stop after processing this many items (for testing).",
    ),
    chunk_size: int = typer.Option(
        100_000,
        "--chunk-size",
        help="Items per fetch chunk.",
    ),
    scheduler: str = typer.Option(
        "synchronous",
        "--scheduler",
        help="'synchronous' | 'local' | 'coiled'.",
    ),
    workers: int = typer.Option(
        4,
        "--workers",
        help="Dask local workers (when --scheduler local).",
    ),
    memory_limit: str = typer.Option(
        "auto",
        "--memory-limit",
        help="Worker memory limit (e.g. '14GiB', or 0 to disable). "
        "Default 'auto' = 60% of worker RAM.",
    ),
    skip_fetch: bool = typer.Option(
        False,
        "--skip-fetch",
        help="Resume: skip fetch + staging, only compact staged data.",
    ),
    skip_compact: bool = typer.Option(
        False,
        "--skip-compact",
        help="Only fetch + stage; leave compaction for a later run.",
    ),
    scatter_only: bool = typer.Option(
        False,
        "--scatter-only",
        help="Only scatter the inventory into fixed-row shard files (no cluster "
        "needed); print the scatter.json path. Re-run with --inventory <that path> "
        "to ingest without re-reading the inventory.",
    ),
    fetch_concurrency: int = typer.Option(
        256,
        "--fetch-concurrency",
        help="Concurrent in-flight S3 GETs per Dask worker during the STAC fetch.",
    ),
    fetch_workers: int = typer.Option(
        16,
        "--fetch-workers",
        help="Bounded fetch pool for the serial (daily) path.",
    ),
    grid: str = typer.Option(
        "h3",
        "--grid",
        help="Grid system: 'h3' | 's2' | 'utm' | 'geojson' (for fresh full builds).",
    ),
    resolution: int | None = typer.Option(
        None,
        "--resolution",
        help="Grid resolution (h3/s2). Default: h3=1, s2=2.",
    ),
    boundaries: str | None = typer.Option(
        None,
        "--boundaries",
        help="GeoJSON boundaries path for --grid geojson.",
    ),
    id_field: str | None = typer.Option(
        None,
        "--id-field",
        help="GeoJSON feature property used as the partition key (--grid geojson).",
    ),
    catalog_key: str | None = typer.Option(
        None,
        "--catalog-key",
        help="Object key within the bucket for the uploaded catalog.db. "
        "Defaults to EARTHCATALOG_CATALOG_KEY.",
    ),
    lock_key: str | None = typer.Option(
        None,
        "--lock-key",
        help="Object key for the distributed lock file. Defaults to EARTHCATALOG_LOCK_KEY.",
    ),
) -> None:
    """Run a full or delta ingest from an S3 inventory into the warehouse.

    Routes through the resumable ``ingest_inventory`` / ``Ingester`` pipeline
    and the unified index.  A failed run can be re-invoked safely
    (already-ingested source keys are skipped).  Use ``--mode full`` for a
    fresh build and ``--mode delta`` for incremental updates (e.g. a daily
    delta parquet).

    Grid: use ``--grid h3|s2|utm|geojson`` for fresh full builds.  ``geojson``
    requires ``--boundaries`` (path or s3:// URI) and ``--id-field``.
    """

    import os

    from earthcatalog.config import GridConfig
    from earthcatalog.run import run as run_ingest

    grid_cfg = GridConfig(
        type=grid,
        resolution=resolution,
        boundaries_path=boundaries,
        id_field=id_field,
    )

    if bool(inventory) == bool(diff):
        typer.echo("ERROR: give exactly one of --inventory or --diff")
        raise typer.Exit(1)

    result = run_ingest(
        inventory=inventory,
        diff=diff,
        catalog=catalog,
        warehouse=warehouse,
        catalog_key=catalog_key
        or os.environ.get("EARTHCATALOG_CATALOG_KEY", "test-space/stac/catalog/earthcatalog.db"),
        lock_key=lock_key
        or os.environ.get("EARTHCATALOG_LOCK_KEY", "test-space/stac/catalog/.lock"),
        chunk_size=chunk_size,
        limit=limit,
        mode=mode,
        dry_run=dry_run,
        scheduler=scheduler,
        workers=workers,
        memory_limit=memory_limit,
        skip_fetch=skip_fetch,
        skip_compact=skip_compact,
        scatter_only=scatter_only,
        fetch_concurrency=fetch_concurrency,
        fetch_workers=fetch_workers,
        grid=grid_cfg,
    )
    if result and result.get("dry_run"):
        typer.echo(
            f"dry run: considered {result['considered']:,}, "
            f"new {result['new']:,}, already indexed {result['known']:,}"
        )
    else:
        typer.echo(
            f"ingest done: fetched {result.get('items', 0):,}, "
            f"index rows {result.get('rows', 0):,}, "
            f"considered {result.get('considered', 0):,}"
        )


# ---------------------------------------------------------------------------
# `diff` sub-command — inventory-vs-inventory (or -vs-index) diff via DuckDB
# ---------------------------------------------------------------------------


@app.command()
def diff(
    current: str = typer.Option(
        ...,
        "--current",
        help="Current inventory day: manifest.json, a URI-per-line .txt/.files "
        "list, or a Parquet path/glob.",
    ),
    previous: str | None = typer.Option(
        None,
        "--previous",
        help="Previous inventory day (same forms). Day-over-day EXCEPT mode.",
    ),
    against_index: str | None = typer.Option(
        None,
        "--against-index",
        help="Unified index Parquet URI — keys not yet ingested (first-run mode).",
    ),
    out: str = typer.Option(
        ...,
        "--out",
        help="Where to write the new-keys Parquet (s3:// URI or local path).",
    ),
    out_old: str | None = typer.Option(
        None,
        "--out-old",
        help="Optionally also write the disappeared-keys Parquet (with --previous).",
    ),
    suffix: str = typer.Option(
        ".stac.json",
        "--suffix",
        help="Only diff keys with this suffix.",
    ),
    max_memory: str = typer.Option(
        "10GB",
        "--max-memory",
        help="DuckDB memory cap; excess spills to the temp directory.",
    ),
) -> None:
    """Diff inventories exactly (DuckDB, out-of-core, string comparison).

    The daily narrowing step: `--current` EXCEPT `--previous` gives new and
    re-uploaded (changed) keys; the reverse EXCEPT (`--out-old`) gives keys
    that disappeared (future GC input).  Feed the new-keys file to
    `earthcatalog ingest --diff`.
    """
    from earthcatalog.diff import run_diff

    if bool(previous) == bool(against_index):
        typer.echo("ERROR: give exactly one of --previous or --against-index")
        raise typer.Exit(1)

    result = run_diff(
        current=current,
        out=out,
        previous=previous,
        against_index=against_index,
        out_old=out_old,
        suffix=suffix,
        max_memory=max_memory,
    )
    typer.echo(f"new keys: {result.new_rows:,}")
    if result.old_rows is not None:
        typer.echo(f"disappeared keys: {result.old_rows:,}")
    typer.echo(f"done in {result.seconds:.1f}s")


# ---------------------------------------------------------------------------
# `migrate-indices` sub-command — legacy index files → unified index
# ---------------------------------------------------------------------------


@app.command("migrate-indices")
def migrate_indices_command(
    catalog: str = typer.Option(
        "/tmp/earthcatalog.db",
        "--catalog",
        help="Local SQLite Iceberg catalog path.",
    ),
    warehouse: str = typer.Option(
        ...,
        "--warehouse",
        help="Warehouse root (s3:// URI or local path).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report what would be migrated without writing.",
    ),
) -> None:
    """Merge legacy *_id_hashes / *_source_index files into the unified index.

    One-shot, validated (row counts against the sum of inputs), atomic
    (sidecar write then single PUT).  Legacy files are kept until a green
    GC run.  Idempotent: an already-migrated warehouse is reported and
    left alone.
    """
    from obstore.store import LocalStore, S3Store

    from earthcatalog.catalog import _open_sqlite
    from earthcatalog.migrate import migrate_indices
    from earthcatalog.run import _make_s3_store

    cat = _open_sqlite(db_path=catalog, warehouse_path=warehouse)
    if warehouse.startswith("s3://"):
        bucket = warehouse.removeprefix("s3://").split("/", 1)[0]
        store: S3Store | LocalStore = _make_s3_store(bucket)
    else:
        store = LocalStore(warehouse)

    report = migrate_indices(cat, store, warehouse, dry_run=dry_run)
    for k, v in report.items():
        typer.echo(f"{k}: {v}")


# ---------------------------------------------------------------------------
# `consolidate` sub-command — merge small parts per partition
# ---------------------------------------------------------------------------


@app.command()
def consolidate(
    catalog: str = typer.Option(
        "/tmp/earthcatalog.db",
        "--catalog",
        help="Local SQLite Iceberg catalog path (downloaded from the warehouse when remote).",
    ),
    warehouse: str = typer.Option(
        ...,
        "--warehouse",
        help="Warehouse root (s3:// URI or local path).",
    ),
    min_files: int = typer.Option(
        4,
        "--min-files",
        help="Consolidate partitions holding at least this many files.",
    ),
    limit_tiles: int | None = typer.Option(
        None,
        "--limit-tiles",
        help="Cap how many partitions to consolidate (biggest offenders first).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report consolidation targets without writing anything.",
    ),
) -> None:
    """Merge a partition's small parts into one file, atomically in Iceberg.

    Metadata-only planning; the replacement is a single Iceberg transaction
    (drop old files, append the merged one), and old objects are deleted
    only after that commit succeeds.
    """
    import os
    from pathlib import Path as _Path

    from obstore.store import LocalStore, S3Store

    from earthcatalog.catalog import _open_sqlite, download_catalog, upload_catalog
    from earthcatalog.consolidate import run as run_consolidation
    from earthcatalog.run import _make_s3_store

    catalog_key = None
    if warehouse.startswith("s3://"):
        bucket, key_path = warehouse.removeprefix("s3://").split("/", 1)
        store: S3Store | LocalStore = _make_s3_store(bucket)
        warehouse_prefix = key_path.rstrip("/")
        catalog_key = os.environ.get(
            "EARTHCATALOG_CATALOG_KEY", f"{warehouse_prefix}/earthcatalog.db"
        )
        if not os.path.exists(catalog):
            download_catalog(catalog, store=store, catalog_key=catalog_key)
    else:
        store = LocalStore(str(_Path(warehouse).parent))
        warehouse_prefix = _Path(warehouse).name

    cat = _open_sqlite(db_path=catalog, warehouse_path=warehouse)
    table = cat.load_table("earthcatalog.stac_items")

    reports = run_consolidation(
        store,
        table,
        warehouse_prefix,
        min_files=min_files,
        limit_tiles=limit_tiles,
        dry_run=dry_run,
    )
    for r in reports:
        typer.echo(
            f"{r['tile']}/{r['bin_value']}: "
            + (
                f"[dry-run] {r['files']} files, {r['rows']:,} rows, {r['bytes']:,} bytes"
                if r.get("dry_run")
                else f"{r['files_before']} → {r['files_after']} files, "
                f"{r['rows']:,} rows, {r['rows_removed_dupes']:,} dupes removed"
            )
        )
    typer.echo(f"{len(reports)} partition(s) {'targeted' if dry_run else 'consolidated'}")

    if not dry_run and catalog_key:
        upload_catalog(catalog, store=store, catalog_key=catalog_key)

        # Refresh the catalog-stats snapshot at the durable commit moment.
        from earthcatalog import stats as stats_mod
        from earthcatalog.index import Index

        def _apply(s):
            return stats_mod.apply_consolidation(
                s,
                rows_removed_dupes=sum(r["rows_removed_dupes"] for r in reports),
                files_saved=sum(r["files_before"] - r["files_after"] for r in reports),
            )

        stats_mod.refresh_after(
            store,
            stats_mod.stats_key_for(warehouse),
            table,
            Index(store, os.path.basename(catalog_key)),
            stats_mod.index_locations(table, store, warehouse),
            apply=_apply,
        )


# ---------------------------------------------------------------------------
# `index-backfill` sub-command — complete pointer coverage, locally first
# ---------------------------------------------------------------------------


@app.command("index-backfill")
def index_backfill_command(
    warehouse: str = typer.Option(
        "s3://its-live-data/test-space/stac/catalog/warehouse",
        "--warehouse",
        help="Warehouse root (s3:// URI). Scanned read-only; never modified.",
    ),
    work_dir: str = typer.Option(
        "./index_backfill_work",
        "--work-dir",
        help="Local work dir for staged index parts, the warehouse scan cache, and the manifest.",
    ),
    index_key: str = typer.Option(
        None,
        "--index-key",
        help="Index base key within the warehouse store (default: warehouse_index).",
    ),
    chunk_rows: int = typer.Option(1_000_000, "--chunk-rows", help="Rows per emitted index part."),
    run_id: str = typer.Option(
        None,
        "--run-id",
        help="Part name prefix (default: backfill-YYYYMMDD). Deterministic, so re-runs resume.",
    ),
    stage: bool = typer.Option(
        False, "--stage", help="Download the current index parts locally first."
    ),
    rescan: bool = typer.Option(
        False, "--rescan", help="Force a fresh warehouse scan (cache is reused otherwise)."
    ),
    build: bool = typer.Option(
        False, "--build", help="Emit the missing pointer parts into the local work dir."
    ),
    verify: bool = typer.Option(
        False,
        "--verify",
        help="Full cards-vs-copies verification over the local index (gate before upload).",
    ),
    upload: bool = typer.Option(
        False, "--upload", help="GATED: copy the built parts to the live index."
    ),
    rollback: bool = typer.Option(
        False, "--rollback", help="Delete exactly the manifest's parts from the live index."
    ),
) -> None:
    """Backfill missing (granule x cell) index pointers from warehouse metadata.

    Local-first: stage the index, cache one read-only warehouse scan, build
    and verify locally, then upload in a separate explicit step.
    """
    from pathlib import Path as _Path

    import duckdb
    from obstore.store import LocalStore

    from earthcatalog import index_backfill as ib
    from earthcatalog.run import _make_s3_store

    wd = _Path(work_dir)
    bucket = warehouse.removeprefix("s3://").split("/", 1)[0]
    remote_store = _make_s3_store(bucket)
    # On a bucket-level store the base key is the full path: the index is a
    # sibling of the warehouse dir ({warehouse}_index/).
    key = index_key
    if key is None:
        if warehouse.startswith("s3://"):
            key = f"{warehouse.removeprefix('s3://').split('/', 1)[1].rstrip('/')}_index"
        else:
            key = f"{warehouse.rstrip('/')}_index"
    manifest: ib.BackfillManifest | None = None
    if (wd / "manifest.json").exists():
        manifest = ib.BackfillManifest.load(wd)

    if rollback:
        if manifest is None:
            typer.echo(f"ERROR: no manifest in {wd} — nothing to roll back")
            raise typer.Exit(1)
        n = ib.rollback(manifest, remote_store, key)
        typer.echo(f"Rolled back {n} part(s) from the live index.")
        return

    if stage:
        staged = ib.stage_index_parts(remote_store, key, wd)
        typer.echo(f"Staged {len(staged)} index part(s) into {wd / 'index'}")

    con = duckdb.connect()
    con.execute("SET memory_limit='24GB';")
    # Let the big anti-join spill to temp instead of OOMing on the
    # insertion-order buffer; ORDER BY still makes chunk boundaries exact.
    con.execute("SET preserve_insertion_order=false;")
    con.execute(f"SET temp_directory='{wd / 'tmp'}';")
    if warehouse.startswith("s3://"):
        con.execute("INSTALL aws; LOAD aws; CALL load_aws_credentials();")
        con.execute("SET s3_region='us-west-2';")

    cache = wd / "warehouse_triples.parquet"
    if rescan and cache.exists():
        cache.unlink()
    scan = ib.scan_warehouse(remote_store, warehouse, wd, con)
    typer.echo(
        f"Warehouse scan: {scan['rows']:,} record copies"
        + (" (cached)" if scan["cached"] else f" from {scan.get('files', '?')} files")
    )

    locs = ib.staged_locations(wd)
    if not locs:
        typer.echo(f"ERROR: no staged index parts in {wd} — run with --stage first")
        raise typer.Exit(1)

    rep = ib.report(cache, locs, con)
    typer.echo(
        f"Index today : {rep['index_rows']:,} rows, {rep['distinct_keys']:,} distinct keys\n"
        f"Missing     : {rep['missing_pairs']:,} (granule x cell) pointers\n"
        f"No s3_key   : {rep['granules_without_key']:,} granules\n"
        + "".join(
            f"  top cell  : {t['grid_partition']} ({t['missing']:,} missing)\n"
            for t in rep["top_cells"]
        )
    )

    if build:
        rid = run_id or f"backfill-{_now_stamp()}"
        stage_root = wd / "index"
        stage_root.mkdir(parents=True, exist_ok=True)
        local_store = LocalStore(str(stage_root))
        manifest = ib.build(
            cache,
            locs,
            local_store,
            key,
            wd,
            con,
            run_id=rid,
            chunk_rows=chunk_rows,
        )
        typer.echo(
            f"Built {len(manifest.parts)} part(s), {manifest.rows_written:,} rows — manifest at {wd / 'manifest.json'}"
        )

    if verify:
        v = ib.verify(cache, ib.staged_locations(wd), con)
        typer.echo(
            f"Index rows          : {v['index_rows']:,}\n"
            f"Warehouse copies    : {v['warehouse_rows']:,}\n"
            f"Distinct keys       : {v['distinct_keys']:,}\n"
            f"Duplicate pairs     : {v['duplicate_pairs']:,}\n"
            f"Cards w/o copy      : {v['cards_without_copy']:,}\n"
            f"Copies w/o card     : {v['copies_without_card']:,}"
        )

    if upload:
        if manifest is None:
            typer.echo("ERROR: nothing built yet — run with --build first")
            raise typer.Exit(1)
        local_store = LocalStore(str(wd / "index"))
        n = ib.upload(manifest, local_store, key, remote_store)
        typer.echo(f"Uploaded {n:,} rows ({len(manifest.parts)} part(s)) to the live index.")


def _now_stamp() -> str:
    from datetime import datetime as _dt

    return _dt.now().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# `info` sub-command — catalog summary
# ---------------------------------------------------------------------------


@app.command()
def info(
    catalog: str | None = typer.Option(None, "--catalog", help="Local SQLite catalog path."),
    catalog_s3: str | None = typer.Option(
        None,
        "--catalog-s3",
        help="s3:// URI to auto-download the catalog from (defaults to the "
        "earthcatalog.db beside the warehouse).",
    ),
    warehouse: str = typer.Option(
        "s3://its-live-data/test-space/stac/catalog/warehouse",
        "--warehouse",
        help="Warehouse root path (s3:// URI).",
    ),
    verify: bool = typer.Option(
        False,
        "--verify",
        help="Recompute the stats snapshot from the data and report drift "
        "against the stored one. Read-only — pass --update to also persist "
        "the recomputed snapshot.",
    ),
    update: bool = typer.Option(
        False,
        "--update",
        help="Write the (re)computed snapshot back to stats.json — bootstraps "
        "it if absent, or persists the --verify recomputation. Never written "
        "without this flag.",
    ),
) -> None:
    """Print a catalog summary: grid metadata, file/row counts, year distribution.

    Entirely read-only and instant by default: with no arguments this
    reads the default production catalog and prints its maintained
    ``stats.json`` snapshot — no manifest or index scans at all. If no
    snapshot exists yet, it says so and stops; it does *not* silently
    fall back to a full recompute. ``--verify`` recomputes the snapshot
    the expensive way and reports drift against the stored one (still
    read-only). Neither mode ever writes to storage unless ``--update``
    is also given.
    """
    import os
    from pathlib import Path

    if not catalog and not catalog_s3:
        catalog_s3 = f"{warehouse.rsplit('/', 1)[0]}/earthcatalog.db"

    catalog_path = catalog or "/tmp/earthcatalog_info.db"
    if catalog_s3 and catalog_s3.startswith("s3://"):
        import obstore
        from obstore.store import S3Store

        no_scheme = catalog_s3.removeprefix("s3://")
        bucket, key = no_scheme.split("/", 1)
        region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
        store = S3Store(bucket=bucket, region=region, skip_signature=True)
        catalog_path = f"/tmp/earthcatalog_info_{key.rsplit('/', 1)[-1]}.db"
        data = bytes(obstore.get(store, key).bytes())
        Path(catalog_path).write_bytes(data)
        typer.echo(f"Downloaded catalog from s3://{bucket}/{key}")
    elif not catalog and catalog_s3:
        catalog_path = catalog_s3  # local warehouse: the db sits beside it

    if not update:
        # info is a public, anonymous-read command by design — strip any
        # local credentials so behavior doesn't depend on what happens to
        # be configured. --update needs real write credentials, so it
        # keeps them.
        os.environ.pop("AWS_ACCESS_KEY_ID", None)
        os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
        os.environ.pop("AWS_SESSION_TOKEN", None)

    from earthcatalog.catalog import FULL_NAME, _catalog_info, _open_sqlite
    from earthcatalog.index import resolve_index_path

    cat = _open_sqlite(db_path=catalog_path, warehouse_path=warehouse)
    try:
        table = cat.load_table(FULL_NAME)
    except Exception:
        typer.echo("ERROR: could not load table from catalog")
        raise typer.Exit(1)

    info = _catalog_info(table)
    typer.echo(f"\n{'=' * 60}")
    typer.echo("  Catalog Info")
    typer.echo(f"{'=' * 60}")
    typer.echo(f"  Grid type     : {info.grid_type}")
    typer.echo(f"  Resolution    : {info.grid_resolution}")
    typer.echo(f"  Warehouse     : {warehouse}")

    index_path = resolve_index_path(table, f"{warehouse.rstrip('/')}_index.parquet")
    typer.echo(f"  Unique index  : {index_path}")

    from obstore.store import LocalStore, ObjectStore, S3Store

    from earthcatalog import stats as stats_mod
    from earthcatalog.index import Index as _Index
    from earthcatalog.run import _make_s3_store

    idx_store: ObjectStore
    if warehouse.startswith("s3://"):
        bucket = warehouse.removeprefix("s3://").split("/", 1)[0]
        if update:
            # Writing needs real, authenticated credentials.
            idx_store = _make_s3_store(bucket)
        else:
            # Read-only (default / --verify): anonymous, unsigned access —
            # matching how the catalog db itself was downloaded above.
            # Relying on _make_s3_store here would silently build an
            # unauthenticated *signed* client whenever no local AWS
            # credentials happen to be configured, whose requests S3
            # rejects outright — Index.locations() swallows that failure
            # and reports zero unique items / index rows instead of the
            # real counts.
            region = (
                os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
            )
            idx_store = S3Store(bucket=bucket, region=region, skip_signature=True)
    else:
        idx_store = LocalStore(str(Path(warehouse).parent))
    skey = stats_mod.stats_key_for(warehouse)
    index_rel = (
        index_path.removeprefix("s3://").split("/", 1)[1]
        if index_path.startswith("s3://")
        else os.path.basename(index_path)
    )
    idx = _Index(idx_store, index_rel)
    stored = stats_mod.load(idx_store, skey)

    if stored is not None and not verify:
        # Fast path: render entirely from the snapshot — no manifest or
        # data scans at all.
        typer.echo(f"\n{'=' * 60}")
        typer.echo("  Summary")
        typer.echo(f"{'=' * 60}")
        typer.echo(f"  Warehouse rows: {stored['warehouse_rows']:,}")
        typer.echo(f"  Warehouse files: {stored['warehouse_files']:,}")
        typer.echo(f"  Warehouse size : {stored['warehouse_bytes'] / 1e9:.2f} GB")
        typer.echo(f"  Unique cells  : {len(stored.get('cells', {})):,}")
        years = sorted(stored.get("years", {}))
        if years:
            typer.echo(f"  Years         : {years[0]}-{years[-1]} ({len(years)} years)")
        hot = sorted(stored.get("hot_locations", []), key=lambda h: -h["row_count"])[:5]
        if hot:
            typer.echo("  Hot locations :")
            for s in hot:
                typer.echo(f"    {s['grid_partition']}: {s['row_count']:,} rows")
        typer.echo(f"  Unique items  : {stored['unique_items']:,}")
        typer.echo(f"  Index rows    : {stored['index_rows']:,}")
        if stored.get("deleted_rows"):
            typer.echo(f"  Deleted rows  : {stored['deleted_rows']:,}")
        per_day = sorted(stored.get("items_per_day", {}).items())[-14:]
        if per_day:
            typer.echo("  Items per day :")
            for d, n in per_day:
                typer.echo(f"    {d}: {n:,}")
        typer.echo(f"  Stats computed: {stored.get('computed_at', 'n/a')}")
        return

    if not verify and not update:
        # No cached snapshot, and the caller didn't ask for a recompute:
        # stay instant and read-only rather than silently falling back to
        # a full manifest + index scan. Only --verify / --update trigger
        # the expensive path below.
        typer.echo(f"\n{'=' * 60}")
        typer.echo("  Summary")
        typer.echo(f"{'=' * 60}")
        typer.echo("  Stats snapshot: none stored yet")
        typer.echo("  Pass --verify (recompute + compare) or --update (bootstrap) to compute it.")
        return

    # Slow path — only reached with --verify or --update: metadata
    # summary, then (re)compute and optionally store the snapshot.
    stats = info.stats(table)
    total_rows = sum(s["row_count"] for s in stats)
    total_files = sum(s["file_count"] for s in stats)
    total_bytes = sum(s["total_bytes"] for s in stats)
    cells = {s["grid_partition"] for s in stats}
    years = sorted({s["year"] for s in stats})

    typer.echo(f"\n{'=' * 60}")
    typer.echo("  Summary")
    typer.echo(f"{'=' * 60}")
    typer.echo(f"  Total rows    : {total_rows:,}")
    typer.echo(f"  Total files   : {total_files:,}")
    typer.echo(f"  Total size    : {total_bytes / 1e9:.2f} GB")
    typer.echo(f"  Unique cells  : {len(cells):,}")
    if years:
        typer.echo(f"  Years         : {years[0]}-{years[-1]} ({len(years)} years)")

    # Hot locations — top partitions by row count.
    top = info.top_cells(table, limit=5)
    if top:
        typer.echo("  Hot locations :")
        for s in top:
            typer.echo(f"    {s['grid_partition']}: {s['row_count']:,} rows")

    # (Re)compute the snapshot the expensive way: bootstrap or --verify.
    # Always read-only — nothing is written back to stats.json unless the
    # caller explicitly passes --update (with or without --verify).
    locations = stats_mod.index_locations(table, idx_store, warehouse)
    computed = stats_mod.compute_full(table, idx, locations)
    if stored is not None:
        drift = {
            k: (stored.get(k), computed.get(k))
            for k in computed
            if k != "computed_at" and stored.get(k) != computed.get(k)
        }
        if drift:
            typer.echo("  [verify] drift detected:")
            for k, (old, new) in drift.items():
                typer.echo(f"    {k}: {old} -> {new}")
        else:
            typer.echo("  [verify] stored stats matched the recomputation")
    else:
        typer.echo("  Stats snapshot: none stored yet (pass --update to bootstrap it)")

    if update:
        stats_mod.save(idx_store, skey, computed)
        typer.echo("  stats.json " + ("updated" if stored is not None else "bootstrapped"))

    typer.echo(f"  Unique items  : {computed['unique_items']:,}")
    typer.echo(f"  Index rows    : {computed['index_rows']:,}")
    if computed.get("deleted_rows"):
        typer.echo(f"  Deleted rows  : {computed['deleted_rows']:,}")
    per_day = computed.get("items_per_day", {})
    if per_day:
        typer.echo("  Items per day :")
        for d, n in sorted(per_day.items())[-14:]:
            typer.echo(f"    {d}: {n:,}")
    typer.echo(f"  Stats computed: {computed.get('computed_at', 'n/a')}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
