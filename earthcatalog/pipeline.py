"""Ingest orchestration — the single pipeline behind ``EarthCatalog.ingest_inventory``.

There is one ingest operation, not several.  "Full" vs. "delta" differ only
in *input scope* (which inventory you pass: a complete manifest vs. a
newer snapshot / precomputed new-keys parquet) and whether the Iceberg
table is rebuilt first (``mode``).  The engine — fetch, fan-out, stage,
compact, commit — is identical, and the unified index makes every run
resumable and idempotent regardless of scope.

Sharding
--------
Distributed runs split the inventory with
:func:`earthcatalog.inventory.iter_inventory_shards`: a ``manifest.json``
inventory becomes one shard per part file (workers stream their own files,
so the head never materialises the full pair list), while single-file
inventories or an exact ``limit`` fall back to pair-list shards.  Workers
only write data files; the head node commits to Iceberg and the unified
index exactly once, so neither is ever written concurrently.
"""

from __future__ import annotations

import os
from datetime import UTC
from datetime import datetime as _dt
from typing import TYPE_CHECKING

from .ingest_config import IngestConfig

if TYPE_CHECKING:
    from .catalog import EarthCatalog


class IngestPipeline:
    """Orchestrates a (re)ingest of STAC items from an S3 Inventory."""

    def __init__(self, catalog: EarthCatalog, config: IngestConfig | None = None) -> None:
        self._cat = catalog
        self._cfg = config or IngestConfig()

    def run(self, inventory_path: str, *, mode: str = "auto") -> dict:
        """Ingest *inventory_path* into the catalog's Iceberg table.

        *mode* is ``"auto"`` (delta iff the table has rows), ``"full"``
        (drop and rebuild the table), or ``"delta"`` (append).  Returns the
        run summary dict (``{"items": …, "rows": …}``).
        """
        from .catalog import get_or_create
        from .config import GridConfig
        from .grids import build_partitioner
        from .index import Index
        from .ingest import DaskIngester, Ingester
        from .inventory import iter_inventory, iter_inventory_shards
        from .schema import FULL_NAME, NAMESPACE

        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            raise RuntimeError(
                "No AWS credentials found in environment. "
                "ingest_inventory() requires write access to S3. "
                "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use an IAM role."
            )

        cat = self._cat
        cfg = self._cfg

        warehouse_root = cat._catalog.properties.get("warehouse", "")
        uri = cat._catalog.properties.get("uri", "")
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

        if cat._store and cat._catalog_key:
            cat.download_catalog(local_db)

        if not delta:
            from pyiceberg.exceptions import NoSuchTableError

            try:
                cat._catalog.drop_table(FULL_NAME)
            except NoSuchTableError:
                pass
            try:
                cat._catalog.create_namespace(NAMESPACE)
            except Exception:
                pass
            cat._table = get_or_create(cat._catalog, grid_config=grid_cfg)  # type: ignore[assignment]

        warehouse_prefix = warehouse_root.rstrip("/") + "/"
        if warehouse_prefix.startswith("s3://"):
            # Store-relative key prefix (obstore keys are relative to the
            # bucket); the full s3:// URI is passed as warehouse_root so
            # Iceberg add_files resolves real paths.
            warehouse_prefix = warehouse_prefix.removeprefix("s3://").split("/", 1)[1]
        index_key = f"{warehouse_root.rstrip('/')}_index.parquet"
        if index_key.startswith("s3://"):
            index_key = index_key.removeprefix("s3://").split("/", 1)[1]
        index = Index(cat._store, index_key)

        kwargs = dict(
            store=cat._store,
            index=index,
            table=cat._table,
            partitioner=partitioner,
            warehouse_prefix=warehouse_prefix,
            warehouse_root=warehouse_root,
            batch_size=cfg.chunk_size,
            compact_rows=cfg.compact_rows,
            skip_fetch=cfg.skip_fetch,
            skip_compact=cfg.skip_compact,
            stage=cfg.stage,
        )

        if cfg.create_client is not None:
            # Distributed: one shard per inventory part file when possible;
            # workers stream their own pairs, the head never materialises them.
            shards = iter_inventory_shards(
                inventory_path,
                chunk_size=cfg.chunk_size,
                since=cfg.since,
                suffix=".stac.json",
                limit=cfg.limit,
            )
            client = cfg.create_client()
            summary = DaskIngester(**kwargs).run(shards, client=client)
        else:
            summary = Ingester(**kwargs).run(iter_inventory(inventory_path, since=cfg.since))

        if cat._store and cat._catalog_key:
            cat.upload_catalog(local_db)

        return summary
