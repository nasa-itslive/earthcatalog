"""Ingest orchestration — the single pipeline behind ``EarthCatalog.ingest_inventory``.

There is one ingest operation, not several.  "Full" vs. "delta" differ only
in *input scope* (which inventory you pass: a complete manifest vs. a
newer snapshot / precomputed new-keys parquet) and whether the Iceberg
table is rebuilt first (``mode``).  The engine — fetch, fan-out, stage,
compact, commit — is identical, and the unified index makes every run
resumable and idempotent regardless of scope.

Distributed execution (scatter → map → reduce)
----------------------------------------------
*Scatter*: the head streams the inventory once and writes fixed-row shard
parquets to the warehouse (``write_inventory_shards``); head memory stays
bounded and every shard holds exactly ``chunk_size`` items, so worker load
is uniform regardless of how unbalanced the source inventory part files
are.  *Map*: each worker reads its own shard URL
(:class:`earthcatalog.inventory.InventoryShard`), fetches the STAC items,
and fans them out to per-(cell, year) NDJSON (or writes GeoParquet
directly in ``direct`` stage).  *Reduce*: only the head node compacts,
commits to Iceberg and appends to the unified index — exactly once — so
neither is ever written concurrently.  Shard files are deleted after the
run; a crashed run leaves only orphaned files under a dead run prefix.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime as _dt
from functools import partial
from itertools import islice
from typing import TYPE_CHECKING

from obstore.store import ObjectStore

from .ingest_config import IngestConfig

if TYPE_CHECKING:
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.table import Table

    from .catalog import EarthCatalog


class IngestPipeline:
    """Orchestrates a (re)ingest of STAC items from an S3 Inventory."""

    def __init__(self, catalog: EarthCatalog, config: IngestConfig | None = None) -> None:
        self._cat = catalog
        self._cfg = config or IngestConfig()
        self._catalog: SqlCatalog = catalog._catalog
        self._table: Table = catalog._table

    @staticmethod
    def _store_relative(key: str) -> str:
        """Normalize an index path/URI to a store-relative object key."""
        if key.startswith("s3://"):
            return key.removeprefix("s3://").split("/", 1)[1]
        if os.path.isabs(key):
            # Local stores are rooted at the warehouse dir.
            return os.path.basename(key)
        return key

    def run(self, inventory_path: str, *, mode: str = "auto") -> dict:
        """Ingest *inventory_path* into the catalog's Iceberg table.

        *mode* is ``"auto"`` (delta iff the table has rows), ``"full"``
        (drop and rebuild the table), or ``"delta"`` (append).  Returns the
        run summary dict (``{"items": …, "rows": …}``).
        """
        from .catalog import get_or_create
        from .config import GridConfig
        from .diff import anti_join, count_rows, resolve_files
        from .grids import build_partitioner
        from .index import Index, resolve_index_path
        from .ingest import DaskIngester, Ingester
        from .inventory import (
            delete_scatter,
            is_scatter_manifest,
            iter_inventory,
            load_inventory_shards,
            scatter_manifest_exists,
            scatter_manifest_path,
            scatter_staging_prefix,
            write_inventory_shards,
        )
        from .schema import FULL_NAME, NAMESPACE

        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            raise RuntimeError(
                "No AWS credentials found in environment. "
                "ingest_inventory() requires write access to S3. "
                "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use an IAM role."
            )

        cat = self._cat
        cfg = self._cfg
        # The daily path consumes a diff Parquet (from `earthcatalog diff`)
        # in place of a raw inventory manifest.
        source = cfg.diff or inventory_path

        warehouse_root = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///")

        grid_cfg = GridConfig(
            type=cat._info.grid_type,
            resolution=cat._info.grid_resolution,
            boundaries_path=cat._info.boundaries_path,
            id_field=cat._info.id_field,
        )
        partitioner = build_partitioner(grid_cfg)

        if cfg.staging_prefix is None:
            date_str = _dt.now(UTC).strftime("%Y%m%d")
            cfg.staging_prefix = f"ingest/{date_str}"

        delta = True
        if mode == "full":
            delta = False
        elif mode == "auto":
            try:
                n = sum(s["row_count"] for s in cat._info.stats(cat._table))
                delta = n > 0
            except Exception:
                delta = False

        if cat._store and cat._catalog_key and not os.path.exists(local_db):
            # run.py downloads before opening the catalog; re-downloading
            # here would swap the sqlite under the open connection.
            cat.download_catalog(local_db)

        if not delta:
            from pyiceberg.exceptions import NoSuchTableError

            try:
                self._catalog.drop_table(FULL_NAME)
            except NoSuchTableError:
                pass
            try:
                self._catalog.create_namespace(NAMESPACE)
            except Exception:
                pass
            cat._table = self._table = get_or_create(self._catalog, grid_config=grid_cfg)

        warehouse_prefix = warehouse_root.rstrip("/") + "/"
        if warehouse_prefix.startswith("s3://"):
            # Store-relative key prefix (obstore keys are relative to the
            # bucket); the full s3:// URI is passed as warehouse_root so
            # Iceberg add_files resolves real paths.
            warehouse_prefix = warehouse_prefix.removeprefix("s3://").split("/", 1)[1]
        elif os.path.isabs(warehouse_prefix):
            # Local stores are rooted at the warehouse dir — same
            # store-relative rule as the index key above.
            warehouse_prefix = os.path.basename(warehouse_root.rstrip("/")) + "/"
        index_prop = resolve_index_path(cat._table, f"{warehouse_root.rstrip('/')}_index.parquet")
        index_key = self._store_relative(index_prop)
        if cat._store is None:
            raise RuntimeError("ingest requires a warehouse store")
        store: ObjectStore = cat._store
        index = Index(store, index_key)

        if not delta and store:
            # Full mode really rebuilds: the index object and staging area
            # die with the table, or the resume checkpoint would skip every
            # item and a "full" ingest would ingest nothing.
            import obstore

            try:
                obstore.delete(store, index_key)
            except FileNotFoundError:
                pass
            for prefix in (
                f"{index_key.removesuffix('.parquet')}/",  # index parts
                f"{warehouse_prefix.rstrip('/')}/_staging",  # journals + NDJSON
            ):
                try:
                    for listing in obstore.list(store, prefix=prefix):
                        for obj in listing:
                            obstore.delete(store, obj["path"])
                except FileNotFoundError:
                    pass

        # Resume filter: DuckDB anti-join against the unified index (every
        # location: legacy single file + all parts) when it exists; absent
        # index (first run) → everything is new.
        if warehouse_root.startswith("s3://"):
            bucket = warehouse_root.removeprefix("s3://").split("/", 1)[0]
            index_uris = [f"s3://{bucket}/{loc}" for loc in index.locations()]
        else:
            # Local: anchor to the absolute index property path — the
            # LocalStore root is not derivable from the warehouse dir
            # (the store may be rooted at the warehouse's parent).
            index_uris = [
                os.path.join(os.path.dirname(index_prop), loc) for loc in index.locations()
            ]
        dedupe = partial(anti_join, index_uri=index_uris) if index_uris else None
        direct_left = (
            source if dedupe is not None and source.endswith((".parquet", ".json")) else None
        )

        def dedupe_pairs(pairs):
            assert dedupe is not None
            return dedupe(pairs)

        def dedupe_source(limit: int | None = None):
            assert dedupe is not None
            return dedupe(direct_left, suffix=".stac.json", since=cfg.since, limit=limit)

        pairs_iter: Iterator[tuple[str, str]] | islice
        if cfg.dry_run:
            if direct_left:
                from .diff import count_rows

                considered = count_rows(resolve_files(source))
                new = sum(1 for _ in dedupe_source())
            else:
                base: Iterator[tuple[str, str]] = (
                    (b, k)
                    for b, k in iter_inventory(source, since=cfg.since)
                    if k.endswith(".stac.json")
                )
                if cfg.limit is not None:
                    base = islice(base, cfg.limit)
                pairs_iter = dedupe_pairs(base) if dedupe is not None else base
                considered = 0

                def _counted():
                    nonlocal considered
                    for pair in pairs_iter:
                        considered += 1
                        yield pair

                new = sum(1 for _ in _counted())
            return {
                "dry_run": True,
                "source": source,
                "considered": considered,
                "new": new,
                "known": considered - new,
            }

        kwargs = dict(
            store=store,
            index=index,
            table=cat._table,
            partitioner=partitioner,
            warehouse_prefix=warehouse_prefix,
            warehouse_root=warehouse_root,
            batch_size=cfg.chunk_size,
            skip_fetch=cfg.skip_fetch,
            skip_compact=cfg.skip_compact,
            fetch_concurrency=cfg.fetch_concurrency,
            fetch_workers=cfg.fetch_workers,
            delta=delta,
        )
        if dedupe is not None:
            kwargs["dedupe"] = dedupe_pairs
        # Bulk profile stages NDJSON internally (fan-out byproduct); the
        # serial path is direct-only.

        if cfg.scatter_only or cfg.create_client is not None:
            # Scatter step (head-only, no cluster needed).  The head streams
            # the inventory, filters to .stac.json items, and writes fixed-row
            # shard parquets + a scatter.json manifest.  scatter_only stops
            # here; create_client proceeds to the distributed map/reduce.
            # skip_fetch (Stage B only) consolidates already-staged NDJSON and
            # needs no shards.
            staging_prefix = ""
            if cfg.skip_fetch and not cfg.scatter_only:
                shards: list = []
                print("skip_fetch set — consolidating staged NDJSON (no scatter).")
            elif is_scatter_manifest(source):
                # Step 2: consume pre-scattered shards, skip the head read.
                shards = load_inventory_shards(inventory_path, store)
                staging_prefix = inventory_path.rstrip("/").rsplit("/", 1)[0]
                print(
                    f"Map/reduce: {len(shards)} pre-scattered shard file(s) from {inventory_path}"
                )
            else:
                # Step 1: scatter the inventory into fixed-row shard files.  The
                # prefix is deterministic in the inventory + params, so a re-run
                # of this step detects an existing scatter and reuses it.
                staging_prefix = scatter_staging_prefix(
                    warehouse_prefix,
                    source,
                    chunk_size=cfg.chunk_size,
                    since=cfg.since,
                    suffix=".stac.json",
                    limit=cfg.limit,
                )
                if scatter_manifest_exists(store, staging_prefix):
                    shards = load_inventory_shards(scatter_manifest_path(staging_prefix), store)
                    print(
                        f"Scatter already exists ({len(shards)} shard file(s)) — reusing "
                        f"{scatter_manifest_path(staging_prefix)}"
                    )
                else:
                    shards = write_inventory_shards(
                        inventory_path,
                        store,
                        staging_prefix=staging_prefix,
                        chunk_size=cfg.chunk_size,
                        since=cfg.since,
                        suffix=".stac.json",
                        limit=cfg.limit,
                    )
                    print(
                        f"Scatter: {len(shards)} shard file(s) of ≤{cfg.chunk_size:,} items "
                        f"({scatter_manifest_path(staging_prefix)})"
                    )

            if cfg.scatter_only:
                print("scatter_only set — skipping map/reduce.")
                if store and cat._catalog_key:
                    cat.upload_catalog(local_db)
                return {
                    "items": 0,
                    "rows": 0,
                    "scatter": scatter_manifest_path(staging_prefix),
                }

            assert cfg.create_client is not None
            client = cfg.create_client()
            try:
                summary = DaskIngester(**kwargs).run(shards, client=client)
            except Exception:
                if staging_prefix:
                    print(
                        "Ingest failed — shard files kept for resume; re-run with "
                        f"inventory_path={scatter_manifest_path(staging_prefix)}"
                    )
                else:
                    print("Ingest failed — staged NDJSON kept; re-run with skip_fetch=True.")
                raise
            if staging_prefix:
                deleted = delete_scatter(store, staging_prefix, shards)
                print(f"Scatter cleanup: removed {deleted} object(s) under {staging_prefix}")
        else:
            # Serial (the daily path): the anti-join IS the resume check.
            # dedupe is None only on a warehouse's very first run.
            if direct_left:
                # Pre-ingest diff report — the join is re-executed for the run.
                considered = count_rows(resolve_files(source))
                if dedupe is not None:
                    from .diff import count_new_keys

                    new = count_new_keys(
                        direct_left,
                        index_uris,
                        suffix=".stac.json",
                        since=cfg.since,
                        limit=cfg.limit,
                    )
                else:
                    new = considered
                print(
                    f"Diff report: {new:,} new items from the current inventory "
                    f"({considered:,} considered, {considered - new:,} already indexed)"
                )
                pairs_iter = dedupe_source(limit=cfg.limit)
            elif dedupe is not None:
                base = (
                    (b, k)
                    for b, k in iter_inventory(source, since=cfg.since)
                    if k.endswith(".stac.json")
                )
                if cfg.limit is not None:
                    base = islice(base, cfg.limit)
                pairs_iter = dedupe_pairs(base)
            else:
                pairs_iter = (
                    (b, k)
                    for b, k in iter_inventory(source, since=cfg.since)
                    if k.endswith(".stac.json")
                )
                if cfg.limit is not None:
                    pairs_iter = islice(pairs_iter, cfg.limit)
            serial_kwargs = dict(kwargs)
            # Bulk-only knobs (the Dask workers own them).
            for bulk_only in ("skip_fetch", "skip_compact", "fetch_concurrency"):
                serial_kwargs.pop(bulk_only, None)
            serial_kwargs.pop("dedupe", None)  # pairs already filtered
            summary = Ingester(**serial_kwargs).run(pairs_iter)

        summary["source"] = source
        summary["mode"] = mode
        if not cfg.scatter_only:
            self._reconcile(summary)
        self._write_last_run(summary)

        if store and cat._catalog_key:
            cat.upload_catalog(local_db)

        return summary

    def _reconcile(self, summary: dict) -> None:
        """Post-ingest report: what the index and Iceberg hold *right now*.

        Metadata-only counts — DuckDB over the index parts, manifest
        statistics for the table.  No full scans, no item fetches.
        """
        from .index import Index, resolve_index_path

        cat = self._cat
        root = self._catalog.properties.get("warehouse", "")
        key = resolve_index_path(cat._table, f"{root.rstrip('/')}_index.parquet")
        try:
            key = self._store_relative(key)
            if key and cat._store is not None:
                summary["index_keys"] = Index(cat._store, key).count_active()
            summary["iceberg_rows"] = cat._table.scan().count()
            summary["iceberg_files"] = sum(1 for _ in cat._table.scan().plan_files())
        except Exception as exc:
            print(f"(reconciliation counts unavailable: {exc})")
            return
        print(
            f"Post-ingest: index holds {summary['index_keys']:,} source keys; "
            f"Iceberg holds {summary['iceberg_rows']:,} rows in "
            f"{summary['iceberg_files']} data files (rows > keys with multi-cell fan-out)"
        )

    def _write_last_run(self, summary: dict) -> None:
        """Persist the run summary to ``{warehouse}/_last_run.json``."""
        import json

        import obstore

        cat = self._cat
        store = cat._store
        if not store:
            return
        root = cat._catalog.properties.get("warehouse", "")
        rel = root.removeprefix("s3://").split("/", 1)
        key = f"{rel[1].rstrip('/')}/_last_run.json" if len(rel) == 2 else "_last_run.json"
        payload = dict(summary)
        payload["finished_at"] = _dt.now(UTC).isoformat()
        obstore.put(store, key, json.dumps(payload, default=str).encode())
        print(f"Run summary: {root}/_last_run.json")
