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
    inventory: str = typer.Option(
        ...,
        "--inventory",
        "-i",
        help="Path or s3:// URI to the S3 Inventory (CSV, Parquet, or manifest.json).",
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
        help="Object key for the distributed lock file. "
        "Defaults to EARTHCATALOG_LOCK_KEY.",
    ),
) -> None:
    """Run a full or delta ingest from an S3 inventory into the warehouse.

    Routes through the resumable ``bulk_ingest`` / ``Ingester`` pipeline and
    the unified index.  A failed run can be re-invoked safely (already-ingested
    source keys are skipped).  Use ``--mode full`` for a fresh bulk build and
    ``--mode delta`` for incremental updates (e.g. a daily delta parquet).

    Grid: use ``--grid h3|s2|utm|geojson`` for fresh full builds.  ``geojson``
    requires ``--boundaries`` (path or s3:// URI) and ``--id-field``.
    """
    import os

    from earthcatalog.config import GridConfig
    from scripts.run_backfill import run as run_ingest

    grid_cfg = GridConfig(
        type=grid,
        resolution=resolution,
        boundaries_path=boundaries,
        id_field=id_field,
    )

    run_ingest(
        inventory=inventory,
        catalog=catalog,
        warehouse=warehouse,
        catalog_key=catalog_key
        or os.environ.get("EARTHCATALOG_CATALOG_KEY", "test-space/stac/catalog/earthcatalog.db"),
        lock_key=lock_key
        or os.environ.get("EARTHCATALOG_LOCK_KEY", "test-space/stac/catalog/.lock"),
        chunk_size=chunk_size,
        limit=limit,
        mode=mode,
        scheduler=scheduler,
        workers=workers,
        skip_fetch=skip_fetch,
        skip_compact=skip_compact,
        grid=grid_cfg,
    )


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

    from earthcatalog.catalog import FULL_NAME, PROP_HASH_INDEX_PATH, _catalog_info, _open_sqlite

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

    index_path = table.properties.get(PROP_HASH_INDEX_PATH) or f"{warehouse.rstrip('/')}_index.parquet"
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
