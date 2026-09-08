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


# ---------------------------------------------------------------------------
# `info` sub-command — catalog summary
# ---------------------------------------------------------------------------


@app.command()
def info(
    catalog: str | None = typer.Option(None, "--catalog", help="Local SQLite catalog path."),
    catalog_s3: str | None = typer.Option(
        None,
        "--catalog-s3",
        help="s3:// URI to auto-download the catalog from.",
    ),
    warehouse: str = typer.Option(
        "s3://its-live-data/test-space/stac/catalog/warehouse",
        "--warehouse",
        help="Warehouse root path (s3:// URI).",
    ),
) -> None:
    """Print a catalog summary: grid metadata, file/row counts, year distribution."""
    import os
    from pathlib import Path

    if not catalog and not catalog_s3:
        typer.echo("ERROR: specify --catalog or --catalog-s3")
        raise typer.Exit(1)

    assert catalog is not None
    catalog_path = catalog
    if catalog_s3:
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

    # Unified-index counts: unique active items + ingest rate per day.
    try:
        from obstore.store import LocalStore

        from earthcatalog.index import Index as _Index
        from earthcatalog.run import _make_s3_store

        if index_path.startswith("s3://"):
            bucket = index_path.removeprefix("s3://").split("/", 1)[0]
            store = _make_s3_store(bucket)
            idx = _Index(store, index_path.removeprefix("s3://").split("/", 1)[1])
            full = [f"s3://{bucket}/{loc}" for loc in idx.locations()]
        else:
            idx = _Index(LocalStore(str(Path(index_path).parent)), Path(index_path).name)
            full = [str(Path(index_path).parent / loc) for loc in idx.locations()]
        unique = idx.count_active()
        typer.echo(f"  Unique items  : {unique:,}")
        per_day = idx.items_per_day(days=14, locations=full)
        if per_day:
            typer.echo("  Items per day :")
            for d, n in per_day:
                typer.echo(f"    {d}: {n:,}")
    except Exception as exc:
        typer.echo(f"  Unique items  : unavailable ({exc})")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
