#!/usr/bin/env python3
"""End-to-end ingest entry point — store building, client resolution, run.

Lives in the package (wheels only package ``earthcatalog*``) behind the
``earthcatalog ingest`` CLI command.

Can also be imported and called directly from Python::

    from earthcatalog.run import run

    run(
        inventory="s3://.../manifest.json",
        warehouse="s3://its-live-data/my-build/warehouse",
        catalog_key="my-build/earthcatalog.db",
        create_client=lambda: my_dask_client,
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
    inventory: str | None = None,
    diff: str | None = None,
    catalog: str = "/tmp/earthcatalog_v2.db",
    warehouse: str = "s3://its-live-data/test-space/stac/catalog/warehouse",
    # Where to upload earthcatalog.db inside the bucket (key only, no s3://bucket/)
    catalog_key: str = "test-space/stac/catalog/earthcatalog.db",
    lock_key: str = "test-space/stac/catalog/.lock",
    chunk_size: int = 100_000,
    limit: int | None = None,
    since: "datetime | None" = None,
    delta: bool = False,
    mode: str | None = None,  # "full" | "delta" | "auto" — overrides delta
    dry_run: bool = False,
    skip_fetch: bool = False,
    skip_compact: bool = False,
    scatter_only: bool = False,
    fetch_concurrency: int = 256,
    fetch_workers: int = 16,
    grid=None,  # Optional GridConfig for fresh (full) builds
    # Scheduler — mutually exclusive with create_client.  The distributed
    # path is the BULK profile; the daily path runs synchronous.
    scheduler: str = "synchronous",  # "synchronous" | "local" | "coiled"
    workers: int = 4,
    threads_per_worker: int = 2,
    memory_limit: str = "auto",
    coiled_n_workers: int = 10,
    coiled_vm_type: str = "c6i.xlarge",
    coiled_scheduler_address: str | None = None,
    # Pass a pre-built Dask client directly (takes precedence over scheduler/coiled_*)
    create_client: Callable[[], object] | None = None,
) -> dict:
    """Run the ingest pipeline from Python without shelling out.

    All path arguments accept ``s3://`` URIs or local filesystem paths.
    Stores are built internally from credentials found in the environment or
    ``~/.aws/credentials``, mirroring what the CLI does.

    Parameters
    ----------
    inventory:
        S3 Inventory manifest.json URI or local path.  May also point at a
        ``scatter.json`` manifest written by a previous ``scatter_only=True``
        run — the pre-scattered shard files are consumed as-is and the
        inventory is not re-read.  Mutually exclusive with *diff*.
    diff:
        A diff Parquet written by ``earthcatalog diff`` (new/changed keys
        against a previous inventory day).  Mutually exclusive with
        *inventory*; this is the daily path.
    dry_run:
        Count the keys the run would fetch (diff vs index) and exit —
        no writes.
    catalog:
        Local SQLite path for the Iceberg catalog (created if absent).
    warehouse:
        Warehouse root (``s3://`` URI or local path).
    catalog_key:
        Object key *within the warehouse bucket* used to upload the rebuilt
        ``earthcatalog.db`` at the end of the run.
    lock_key:
        Object key for the distributed lock file.
    fetch_workers:
        Bounded fetch pool for the serial (daily) path.
    scatter_only:
        Distributed only: stop after writing the fixed-row shard files (no
        cluster needed) and print the scatter manifest path.  Re-run with
        ``inventory=<that path>`` to execute the map/reduce — workers start
        immediately instead of idling behind the head's inventory read.
    create_client:
        Optional callable that returns a Dask ``Client``.  When provided,
        ``scheduler`` / ``coiled_*`` parameters are ignored.  Use this from a
        notebook where you've already provisioned a cluster::

            client = Client(cluster)
            run(..., create_client=lambda: client)
    """
    if (inventory is None) == (diff is None):
        raise ValueError("give exactly one of `inventory` or `diff`")

    # ------------------------------------------------------------------
    # Build stores
    # ------------------------------------------------------------------
    if warehouse.startswith("s3://"):
        wh_no_scheme = warehouse.removeprefix("s3://")
        wh_bucket, wh_prefix = wh_no_scheme.split("/", 1)
        # Bucket-level store: the pipeline uses full bucket keys
        # (warehouse_prefix / index_key), so a prefix here would double-prefix.
        warehouse_store: S3Store | LocalStore = _make_s3_store(wh_bucket)
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

    if warehouse.startswith("s3://"):
        # Pull the remote catalog db BEFORE opening the local sqlite:
        # get_or_create below must load the last uploaded table.  Without
        # this, a fresh process creates a competing empty table and the
        # first Iceberg commit fails the snapshot-ref requirement
        # ("branch main was created concurrently" — observed on CI).
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
                name="earthcatalog-ingest",
                worker_options={
                    "nthreads": threads_per_worker,
                    "memory_limit": memory_limit,
                },
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
            cluster = LocalCluster(
                n_workers=workers,
                threads_per_worker=threads_per_worker,
                memory_limit=memory_limit,
            )
            return Client(cluster)

        resolved_client = _make_local

    # ------------------------------------------------------------------
    # Open the catalog and run through EarthCatalog.ingest_inventory
    # ------------------------------------------------------------------
    from earthcatalog.catalog import (
        EarthCatalog,
        _catalog_info,
        _open_sqlite,
        get_or_create,
    )
    from earthcatalog.ingest_config import IngestConfig

    cat = _open_sqlite(db_path=catalog, warehouse_path=warehouse)
    table = get_or_create(cat, grid_config=grid)
    ec = EarthCatalog(
        catalog=cat,
        table=table,
        info=_catalog_info(table),
        store=warehouse_store,
        catalog_key=catalog_key,
    )

    cfg = IngestConfig(
        chunk_size=chunk_size,
        limit=limit,
        since=since,
        create_client=resolved_client,
        delta=(delta or None),
        dry_run=dry_run,
        diff=diff,
        skip_fetch=skip_fetch,
        skip_compact=skip_compact,
        scatter_only=scatter_only,
        fetch_concurrency=fetch_concurrency,
        fetch_workers=fetch_workers,
    )

    source = inventory if inventory is not None else diff
    assert source is not None
    return ec.ingest_inventory(
        inventory_path=source,
        mode=mode or ("delta" if delta else "auto"),
        config=cfg,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="EarthCatalog ingest pipeline")
    parser.add_argument(
        "--inventory",
        default=None,
        help=(
            "S3 inventory path (CSV, Parquet, or manifest.json), or a scatter.json "
            "manifest from a previous --scatter-only run. Mutually exclusive with --diff."
        ),
    )
    parser.add_argument(
        "--diff",
        default=None,
        help=(
            "Diff Parquet from `earthcatalog diff` (new/changed keys vs the previous "
            "inventory day). The daily path: ingest consumes it directly."
        ),
    )
    parser.add_argument(
        "--catalog", default="/tmp/earthcatalog_v2.db", help="Local SQLite catalog path"
    )
    parser.add_argument(
        "--warehouse",
        default="s3://its-live-data/test-space/stac/catalog/warehouse",
        help="Warehouse root (s3:// URI or local path)",
    )
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Items per fetch chunk")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--since", default=None, help="Only items modified >= this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--delta",
        action="store_true",
        help="Delta mode: append new parquets without overwriting existing warehouse files",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "delta", "auto"],
        default=None,
        help="Overrides --delta: 'full' (rebuild), 'delta' (append), or 'auto'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count the keys the run would fetch (diff vs index) and exit — no writes.",
    )
    parser.add_argument(
        "--scheduler",
        choices=["synchronous", "local", "coiled"],
        default="synchronous",
        help="Dask scheduler (the daily path stays synchronous; local/coiled are bulk)",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--threads-per-worker", type=int, default=2)
    parser.add_argument(
        "--memory-limit",
        default="auto",
        help="Worker memory limit (Dask format, e.g. '14GiB', or 0 to disable). "
        "Default 'auto' = 60%% of worker RAM.",
    )
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
        "--skip-fetch",
        action="store_true",
        help="Resume: skip fetch + NDJSON staging, only compact staged NDJSON.",
    )
    parser.add_argument(
        "--skip-compact",
        action="store_true",
        help="Only fetch + stage NDJSON; leave compaction for a later run.",
    )
    parser.add_argument(
        "--scatter-only",
        action="store_true",
        help=(
            "Only scatter the inventory into fixed-row shard files (no cluster needed); "
            "print the scatter.json path. Re-run with --inventory <that path> to "
            "ingest without re-reading the inventory."
        ),
    )
    parser.add_argument(
        "--fetch-concurrency",
        type=int,
        default=256,
        help="Concurrent in-flight S3 GETs per Dask worker during the STAC fetch.",
    )
    parser.add_argument(
        "--fetch-workers",
        type=int,
        default=16,
        help="Bounded fetch pool for the serial (daily) path.",
    )
    parser.add_argument(
        "--grid",
        default="h3",
        choices=["h3", "s2", "utm", "geojson", "lat_lon"],
        help="Grid system for fresh full builds. lat_lon takes --resolution in "
        "degrees per tile.",
    )
    parser.add_argument(
        "--resolution",
        type=float,
        default=None,
        help="Grid resolution (h3/s2 level; lat_lon degrees per tile). Default: h3=1, s2=2, lat_lon=2.",
    )
    parser.add_argument(
        "--boundaries",
        default=None,
        help="GeoJSON boundaries path for --grid geojson.",
    )
    parser.add_argument(
        "--id-field",
        default=None,
        help="GeoJSON feature property used as the partition key (--grid geojson).",
    )
    parser.add_argument(
        "--time-bin",
        default="year",
        choices=["year", "month", "day"],
        help="Temporal binning for the warehouse layout (default: year).",
    )
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)

    from earthcatalog.config import GridConfig

    grid_cfg = GridConfig(
        type=args.grid,
        resolution=args.resolution,
        boundaries_path=args.boundaries,
        id_field=args.id_field,
        time_bin=args.time_bin,
    )

    run(
        inventory=args.inventory,
        diff=args.diff,
        catalog=args.catalog,
        warehouse=args.warehouse,
        catalog_key=os.environ.get(
            "EARTHCATALOG_CATALOG_KEY", "test-space/stac/catalog/earthcatalog.db"
        ),
        lock_key=os.environ.get("EARTHCATALOG_LOCK_KEY", "test-space/stac/catalog/.lock"),
        chunk_size=args.chunk_size,
        limit=args.limit,
        since=since,
        delta=args.delta,
        mode=args.mode,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
        skip_compact=args.skip_compact,
        scatter_only=args.scatter_only,
        fetch_concurrency=args.fetch_concurrency,
        fetch_workers=args.fetch_workers,
        grid=grid_cfg,
        scheduler=args.scheduler,
        workers=args.workers,
        threads_per_worker=args.threads_per_worker,
        memory_limit=args.memory_limit,
        coiled_n_workers=args.coiled_n_workers,
        coiled_vm_type=args.coiled_vm_type,
        coiled_scheduler_address=args.coiled_scheduler_address,
    )


if __name__ == "__main__":
    main()
