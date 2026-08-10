"""
EarthCatalog CLI — wires YAML config to ingest pipelines.

Usage
-----
    # Single-node incremental ingest (GitHub Actions / laptop)
    earthcatalog incremental --config config/h3_r3.yaml --inventory /tmp/delta.csv

    # Or pass individual flags without a config file
    earthcatalog incremental --inventory /tmp/delta.csv \\
        --catalog /tmp/earthcatalog.db --warehouse /tmp/wh --limit 100
"""

from __future__ import annotations

import typer

app = typer.Typer(
    name="earthcatalog",
    help="EarthCatalog STAC → Iceberg ingest tool.",
    add_completion=False,
)


# ---------------------------------------------------------------------------
# `incremental` sub-command
# ---------------------------------------------------------------------------


@app.command()
def incremental(
    inventory: str = typer.Option(
        ...,
        "--inventory",
        "-i",
        help="Path or s3:// URI to the S3 Inventory CSV / CSV.gz.",
    ),
    config: str | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to a YAML config file.  When provided, all other options default "
        "to the values in the file.",
    ),
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        help="Path to the SQLite catalog file (overrides config).",
    ),
    warehouse: str | None = typer.Option(
        None,
        "--warehouse",
        help="Path to the Iceberg warehouse directory (overrides config).",
    ),
    chunk_size: int | None = typer.Option(
        None,
        "--chunk-size",
        help="STAC items per fetch chunk (overrides config).",
    ),
    workers: int | None = typer.Option(
        None,
        "--workers",
        help="Thread-pool size for parallel S3 fetches (overrides config).",
    ),
    h3_resolution: int | None = typer.Option(
        None,
        "--h3-resolution",
        help="H3 resolution (overrides config grid.resolution).",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Stop after processing this many STAC items (for testing).",
    ),
) -> None:
    """Run single-node incremental ingest (for GitHub Actions / laptops)."""
    from earthcatalog.config import AppConfig, load_config
    from earthcatalog.pipelines.incremental import run, run_from_config

    if config:
        cfg = load_config(config)
        # Apply any CLI overrides on top of the config
        if catalog:
            cfg.catalog.db_path = catalog
        if warehouse:
            cfg.catalog.warehouse = warehouse
        if chunk_size is not None:
            cfg.ingest.chunk_size = chunk_size
        if workers is not None:
            cfg.ingest.max_workers = workers
        if h3_resolution is not None:
            cfg.grid.resolution = h3_resolution

        run_from_config(inventory, cfg, limit=limit)

    else:
        # No config file: build defaults and apply CLI flags directly
        cfg = AppConfig()
        run(
            inventory_path=inventory,
            catalog_path=catalog or cfg.catalog.db_path,
            warehouse_path=warehouse or cfg.catalog.warehouse,
            chunk_size=chunk_size or cfg.ingest.chunk_size,
            max_workers=workers or cfg.ingest.max_workers,
            limit=limit,
            h3_resolution=h3_resolution or (cfg.grid.resolution or 3),
        )


# ---------------------------------------------------------------------------
# `info` sub-command — catalog summary (grid, stats, hash index)
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

    hash_index_path = table.properties.get(PROP_HASH_INDEX_PATH)
    if hash_index_path:
        typer.echo(f"  Hash index    : {hash_index_path}")

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
