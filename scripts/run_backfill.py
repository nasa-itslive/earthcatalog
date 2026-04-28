#!/usr/bin/env python3
"""CLI entry point for backfill staging pipeline."""

import argparse
import configparser
import os
from datetime import UTC, datetime
from pathlib import Path

import dask
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
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)

    # Parse warehouse
    if args.warehouse.startswith("s3://"):
        wh_no_scheme = args.warehouse.removeprefix("s3://")
        wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
        warehouse_store = _make_s3_store(wh_bucket, prefix=wh_prefix)
        warehouse_root = args.warehouse
    else:
        Path(args.warehouse).mkdir(parents=True, exist_ok=True)
        warehouse_store = LocalStore(str(args.warehouse))
        warehouse_root = args.warehouse

    # Parse staging
    if args.staging.startswith("s3://"):
        st_no_scheme = args.staging.removeprefix("s3://")
        st_bucket, st_prefix = st_no_scheme.split("/", 1)
        staging_store = _make_s3_store(st_bucket, prefix=st_prefix)
        staging_prefix = ""
    else:
        Path(args.staging).mkdir(parents=True, exist_ok=True)
        staging_store = LocalStore(str(args.staging))
        staging_prefix = ""

    from earthcatalog.core import store_config

    store_config.set_store(
        _make_s3_store(wh_bucket if args.warehouse.startswith("s3://") else "its-live-data")
    )
    store_config.set_catalog_key("test-space/stac/catalog/earthcatalog.db")
    store_config.set_lock_key("test-space/stac/catalog/.lock")

    if args.delta and args.warehouse.startswith("s3://"):
        from earthcatalog.core.catalog import download_catalog

        download_catalog(args.catalog)

    if args.scheduler == "coiled":
        import glob
        import subprocess
        import sys
        import tempfile

        import coiled
        from dask.distributed import Client

        wheel_dir = tempfile.mkdtemp(prefix="earthcatalog-wheel-")
        print("Building local wheel …")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "-w", wheel_dir, "--quiet"]
        )
        wheels = glob.glob(f"{wheel_dir}/*.whl")
        if not wheels:
            print("ERROR: no wheel built")
            sys.exit(1)
        wheel_path = wheels[0]
        print(f"Built wheel: {os.path.basename(wheel_path)}")

        with open(wheel_path, "rb") as f:
            whl_bytes = f.read()
        whl_name = os.path.basename(wheel_path)

        def _install(whl_bytes, whl_name):
            import os
            import subprocess
            import sys
            import tempfile

            subprocess.check_call(
                [sys.executable, "-m", "pip", "uninstall", "-y", "earthcatalog", "--quiet"]
            )
            td = tempfile.mkdtemp()
            path = os.path.join(td, whl_name)
            with open(path, "wb") as f:
                f.write(whl_bytes)
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--no-deps", path, "--quiet"]
            )

            for mod in list(sys.modules):
                if "earthcatalog" in mod:
                    del sys.modules[mod]

        def _verify():
            import inspect

            from earthcatalog.pipelines import backfill

            src = inspect.getsource(backfill._fetch_item_async)
            if "memoryview" not in src:
                raise RuntimeError(f"WRONG CODE: {backfill.__file__}")
            print(f"VERIFIED: {backfill.__file__}")

        def _create_cluster():
            print("Starting Coiled cluster (spot with fallback) …")
            cluster = coiled.Cluster(
                n_workers=args.coiled_n_workers,
                worker_vm_types=[args.coiled_vm_type],
                region="us-west-2",
                name="backfill-v3",
                worker_options={"nthreads": args.threads_per_worker},
                spot_policy="spot_with_fallback",
            )
            client = Client(cluster)
            print(f"Coiled dashboard: {client.dashboard_link}")

            print(f"Installing local wheel on workers ({len(whl_bytes):,} bytes) …")
            client.run(_install, whl_bytes=whl_bytes, whl_name=whl_name)
            print("Verifying worker code …")
            client.run(_verify)
            return client

        from earthcatalog.pipelines.backfill import run_backfill

        run_backfill(
            inventory_path=args.inventory,
            catalog_path=args.catalog,
            staging_store=staging_store,
            staging_prefix=staging_prefix,
            warehouse_store=warehouse_store,
            warehouse_root=warehouse_root,
            h3_resolution=args.h3_resolution,
            chunk_size=args.chunk_size,
            compact_rows=args.compact_rows,
            fetch_concurrency=args.fetch_concurrency,
            limit=args.limit,
            since=since,
            use_lock=not args.no_lock,
            skip_inventory=args.skip_inventory,
            skip_ingest=args.skip_ingest,
            retry_pending=args.retry_pending,
            delta=args.delta,
            create_client=_create_cluster,
            upload=not args.skip_upload,
        )
    elif args.scheduler == "local":
        from dask.distributed import Client, LocalCluster

        cluster = LocalCluster(n_workers=args.workers, threads_per_worker=args.threads_per_worker)
        client = Client(cluster)

        from earthcatalog.pipelines.backfill import run_backfill

        with client:
            run_backfill(
                inventory_path=args.inventory,
                catalog_path=args.catalog,
                staging_store=staging_store,
                staging_prefix=staging_prefix,
                warehouse_store=warehouse_store,
                warehouse_root=warehouse_root,
                h3_resolution=args.h3_resolution,
                chunk_size=args.chunk_size,
                compact_rows=args.compact_rows,
                fetch_concurrency=args.fetch_concurrency,
                limit=args.limit,
                since=since,
                use_lock=not args.no_lock,
                skip_inventory=args.skip_inventory,
                skip_ingest=args.skip_ingest,
                retry_pending=args.retry_pending,
                delta=args.delta,
                upload=not args.skip_upload,
            )
    else:
        from earthcatalog.pipelines.backfill import run_backfill

        with dask.config.set(scheduler="synchronous"):
            run_backfill(
                inventory_path=args.inventory,
                catalog_path=args.catalog,
                staging_store=staging_store,
                staging_prefix=staging_prefix,
                warehouse_store=warehouse_store,
                warehouse_root=warehouse_root,
                h3_resolution=args.h3_resolution,
                chunk_size=args.chunk_size,
                compact_rows=args.compact_rows,
                fetch_concurrency=args.fetch_concurrency,
                limit=args.limit,
                since=since,
                use_lock=not args.no_lock,
                skip_inventory=args.skip_inventory,
                skip_ingest=args.skip_ingest,
                retry_pending=args.retry_pending,
                delta=args.delta,
                upload=not args.skip_upload,
            )


if __name__ == "__main__":
    main()
