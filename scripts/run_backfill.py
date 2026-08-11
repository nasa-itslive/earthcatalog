#!/usr/bin/env python3
"""CLI entry point for backfill staging pipeline.

Can also be imported and called directly from Python::

    from scripts.run_backfill import run

    run(
        inventory="s3://.../manifest.json",
        warehouse="s3://its-live-data/my-build/warehouse",
        staging="s3://its-live-data/my-build/ingest",
        catalog_key="my-build/earthcatalog.db",
        create_client=lambda: my_dask_client,
        update_hash_index=True,
        update_source_index=True,
    )
"""

import argparse
import configparser
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from obstore.store import LocalStore, S3Store


def _make_s3_store(bucket: str, prefix: str = "") -> S3Store:
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token
    kwargs: dict = dict(bucket=bucket, region=region)
    if prefix:
        kwargs["prefix"] = prefix
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


def run(
    *,
    inventory: str,
    catalog: str = "/tmp/earthcatalog_v2.db",
    warehouse: str = "s3://its-live-data/test-space/stac/catalog/warehouse",
    staging: str = "s3://its-live-data/test-space/stac/catalog/ingest",
    # Where to upload earthcatalog.db inside the bucket (key only, no s3://bucket/)
    catalog_key: str = "test-space/stac/catalog/earthcatalog.db",
    lock_key: str = "test-space/stac/catalog/.lock",
    chunk_size: int = 100_000,
    compact_rows: int = 100_000,
    fetch_concurrency: int = 256,
    h3_resolution: int | None = None,
    limit: int | None = None,
    since: "datetime | None" = None,
    use_lock: bool = True,
    skip_upload: bool = False,
    skip_inventory: bool = False,
    skip_ingest: bool = False,
    retry_pending: bool = False,
    delta: bool = False,
    mode: str | None = None,  # "full" | "delta" | "auto" — overrides delta
    skip_fetch: bool = False,
    skip_compact: bool = False,
    grid=None,  # Optional GridConfig for fresh (full) builds
    # Scheduler — mutually exclusive with create_client
    scheduler: str = "synchronous",   # "synchronous" | "local" | "coiled"
    workers: int = 4,
    threads_per_worker: int = 2,
    coiled_n_workers: int = 10,
    coiled_vm_type: str = "c6i.xlarge",
    coiled_scheduler_address: str | None = None,
    # Pass a pre-built Dask client directly (takes precedence over scheduler/coiled_*)
    create_client: Callable[[], object] | None = None,
    hash_index: str | None = None,
    update_hash_index: bool = False,
    source_index: str | None = None,
    update_source_index: bool = False,
) -> None:
    """Run the backfill pipeline from Python without shelling out.

    All path arguments accept ``s3://`` URIs or local filesystem paths.
    Stores are built internally from credentials found in the environment or
    ``~/.aws/credentials``, mirroring what the CLI does.

    Parameters
    ----------
    inventory:
        S3 Inventory manifest.json URI or local path.
    catalog:
        Local SQLite path for the Iceberg catalog (created if absent).
    warehouse:
        Warehouse root (``s3://`` URI or local path).
    staging:
        Staging root (``s3://`` URI or local path).
    catalog_key:
        Object key *within the warehouse bucket* used to upload the rebuilt
        ``earthcatalog.db`` at the end of the run.
    lock_key:
        Object key for the distributed lock file.
    create_client:
        Optional callable that returns a Dask ``Client``.  When provided,
        ``scheduler`` / ``coiled_*`` parameters are ignored.  Use this from a
        notebook where you've already provisioned a cluster::

            client = Client(cluster)
            run(..., create_client=lambda: client)
    """
    from collections.abc import Callable  # noqa: F401 (used in type hint above)

    # ------------------------------------------------------------------
    # Build stores
    # ------------------------------------------------------------------
    if warehouse.startswith("s3://"):
        wh_no_scheme = warehouse.removeprefix("s3://")
        wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
        warehouse_store = _make_s3_store(wh_bucket, prefix=wh_prefix)
    else:
        Path(warehouse).mkdir(parents=True, exist_ok=True)
        warehouse_store = LocalStore(str(warehouse))
        wh_bucket = "its-live-data"  # fallback for store_config

    # ------------------------------------------------------------------
    # Configure store_config (controls catalog upload destination)
    # ------------------------------------------------------------------
    from earthcatalog import store_config

    store_config.set_store(_make_s3_store(wh_bucket))
    store_config.set_catalog_key(catalog_key)
    store_config.set_lock_key(lock_key)

    if delta and warehouse.startswith("s3://"):
        from earthcatalog.catalog import download_catalog
        download_catalog(catalog)

    # ------------------------------------------------------------------
    # Resolve the distributed client (None = single-node Ingester)
    # ------------------------------------------------------------------
    resolved_client = None

    if create_client is not None:
        resolved_client = create_client
    elif coiled_scheduler_address:
        from dask.distributed import Client

        print(f"Connecting to existing scheduler: {coiled_scheduler_address} …")
        client = Client(coiled_scheduler_address)
        print(f"Connected. Dashboard: {client.dashboard_link}")
        resolved_client = lambda: client  # noqa: E731

    elif scheduler == "coiled":
        import coiled
        from dask.distributed import Client

        def _create_cluster():
            print("Starting Coiled cluster …")
            cluster = coiled.Cluster(
                n_workers=coiled_n_workers,
                worker_vm_types=[coiled_vm_type],
                region="us-west-2",
                name="backfill-v3",
                worker_options={"nthreads": threads_per_worker},
                spot_policy="spot_with_fallback",
            )
            client = Client(cluster)
            print(f"Coiled dashboard: {client.dashboard_link}")

            aws_envs = {
                k: os.environ[k]
                for k in (
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "AWS_DEFAULT_REGION",
                )
                if k in os.environ
            }
            if aws_envs:
                cluster.send_private_envs(aws_envs)
                print(f"AWS credentials forwarded to workers ({', '.join(aws_envs)}).")
            else:
                print("WARN: no AWS credentials found in environment — workers may lack S3 access.")

            return client

        resolved_client = _create_cluster

    elif scheduler == "local":
        from dask.distributed import Client, LocalCluster

        def _make_local():
            cluster = LocalCluster(n_workers=workers, threads_per_worker=threads_per_worker)
            return Client(cluster)

        resolved_client = _make_local

    # ------------------------------------------------------------------
    # Open the catalog and run through EarthCatalog.bulk_ingest
    # ------------------------------------------------------------------
    from earthcatalog.backfill_config import BackfillConfig
    from earthcatalog.catalog import (
        EarthCatalog,
        _catalog_info,
        _open_sqlite,
        get_or_create,
    )

    cat = _open_sqlite(db_path=catalog, warehouse_path=warehouse)
    table = get_or_create(cat, grid_config=grid)
    ec = EarthCatalog(
        catalog=cat,
        table=table,
        info=_catalog_info(table),
        store=warehouse_store,
        catalog_key=catalog_key,
    )

    cfg = BackfillConfig(
        chunk_size=chunk_size,
        compact_rows=compact_rows,
        fetch_concurrency=fetch_concurrency,
        limit=limit,
        since=since,
        create_client=resolved_client,
        update_hash_index=update_hash_index,
        hash_index_path=hash_index,
        delta=(delta or None),
        skip_fetch=skip_fetch,
        skip_compact=skip_compact,
    )

    ec.bulk_ingest(
        inventory_path=inventory,
        mode=mode or ("delta" if delta else "auto"),
        config=cfg,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="EarthCatalog backfill v2 (staging pipeline)")
    parser.add_argument(
        "--inventory", required=True, help="S3 inventory path (CSV, Parquet, or manifest.json)"
    )
    parser.add_argument(
        "--catalog", default="/tmp/earthcatalog_v2.db", help="Local SQLite catalog path"
    )
    parser.add_argument(
        "--warehouse",
        default="s3://its-live-data/test-space/stac/catalog/warehouse",
        help="Warehouse root (s3:// URI or local path)",
    )
    parser.add_argument(
        "--staging",
        default="s3://its-live-data/test-space/stac/catalog/ingest",
        help="Staging root (s3:// URI, chunks + NDJSON go here)",
    )
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Items per chunk (Phase 1)")
    parser.add_argument(
        "--compact-rows", type=int, default=100_000, help="Max rows per GeoParquet (Phase 3)"
    )
    parser.add_argument(
        "--fetch-concurrency", type=int, default=256, help="Async fetch concurrency per worker"
    )
    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=None,
        help="H3 resolution (auto-detected from catalog for delta runs)",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--since", default=None, help="Only items modified >= this date (YYYY-MM-DD)"
    )
    parser.add_argument("--no-lock", action="store_true")
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip uploading catalog.db to S3 (for local testing)",
    )
    parser.add_argument(
        "--skip-inventory",
        action="store_true",
        help="Skip Phase 1 inventory scan, use existing chunks in staging",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip Phase 2 ingest, go straight to Phase 3 (Compact)",
    )
    parser.add_argument(
        "--retry-pending",
        action="store_true",
        help="Retry chunks that had fetch failures (pending_chunks/)",
    )
    parser.add_argument(
        "--delta",
        action="store_true",
        help="Delta mode: append new parquets without overwriting existing warehouse files",
    )
    parser.add_argument(
        "--scheduler",
        choices=["synchronous", "local", "coiled"],
        default="synchronous",
        help="Dask scheduler",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--threads-per-worker", type=int, default=2)
    parser.add_argument("--coiled-n-workers", type=int, default=10)
    parser.add_argument("--coiled-vm-type", default="c6i.xlarge")
    parser.add_argument(
        "--coiled-scheduler-address",
        default=None,
        metavar="ADDRESS",
        help=(
            "Connect to an already-running Coiled (or Dask distributed) scheduler "
            "instead of provisioning a new cluster.  Accepts any address accepted "
            "by ``dask.distributed.Client``, e.g. "
            "``tls://scheduler-abc123.us-west-2.aws.dask.host:8786``.  "
            "When set, ``--scheduler coiled`` is implied and the wheel-install / "
            "cluster-creation steps are skipped — the cluster is assumed to already "
            "have the correct earthcatalog version installed."
        ),
    )
    parser.add_argument(
        "--hash-index",
        default=None,
        help="S3 URI for hash index (default: {warehouse}_id_hashes.parquet)",
    )
    parser.add_argument(
        "--update-hash-index",
        action="store_true",
        help=(
            "After delta ingest, update the hash index by reading item IDs from "
            "the newly written warehouse parquet files (Plan B: exact, no warehouse scan). "
            "Only effective with --delta."
        ),
    )
    parser.add_argument(
        "--source-index",
        default=None,
        help="S3 URI for source index (default: {warehouse}_source_index.parquet)",
    )
    parser.add_argument(
        "--update-source-index",
        action="store_true",
        help=(
            "Track source provenance (s3_key, stac_id, grid_partition, year) in the "
            "source index during ingest, so weekly garbage collection can detect "
            "deleted S3 objects without re-fetching STAC JSONs."
        ),
    )
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)

    run(
        inventory=args.inventory,
        catalog=args.catalog,
        warehouse=args.warehouse,
        staging=args.staging,
        catalog_key=os.environ.get(
            "EARTHCATALOG_CATALOG_KEY", "test-space/stac/catalog/earthcatalog.db"
        ),
        lock_key=os.environ.get("EARTHCATALOG_LOCK_KEY", "test-space/stac/catalog/.lock"),
        chunk_size=args.chunk_size,
        compact_rows=args.compact_rows,
        fetch_concurrency=args.fetch_concurrency,
        h3_resolution=args.h3_resolution,
        limit=args.limit,
        since=since,
        use_lock=not args.no_lock,
        skip_upload=args.skip_upload,
        skip_inventory=args.skip_inventory,
        skip_ingest=args.skip_ingest,
        retry_pending=args.retry_pending,
        delta=args.delta,
        scheduler=args.scheduler,
        workers=args.workers,
        threads_per_worker=args.threads_per_worker,
        coiled_n_workers=args.coiled_n_workers,
        coiled_vm_type=args.coiled_vm_type,
        coiled_scheduler_address=args.coiled_scheduler_address,
        hash_index=args.hash_index,
        update_hash_index=args.update_hash_index,
        source_index=args.source_index,
        update_source_index=args.update_source_index,
    )


if __name__ == "__main__":
    main()
