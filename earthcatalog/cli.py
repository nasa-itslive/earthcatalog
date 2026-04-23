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
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
