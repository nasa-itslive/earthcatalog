"""
EarthCatalog — simplified facade for querying spatially-partitioned STAC catalogs.

Provides a clean API that encapsulates PyIceberg catalog, table, and grid metadata
discovery into a single object.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.table import Table

    from .lock import S3Lock

from .catalog_info import CatalogInfo


class EarthCatalog:
    """Simplified facade for querying an EarthCatalog.

    Combines PyIceberg catalog, table, and CatalogInfo into a single interface
    for spatial/temporal queries with automatic file pruning.

    Example::

        from earthcatalog.core import catalog
        from obstore.store import S3Store
        from shapely.geometry import Point

        store = S3Store(bucket='my-bucket', region='us-west-2')
        ec = catalog.open(store=store, base='s3://my-bucket/catalog')

        point = Point(-133.99, 58.74)
        paths = ec.search_files(point, start_datetime='2020-01-01')
    """

    def __init__(
        self,
        catalog: object,
        table: Table,
        info: CatalogInfo,
        store: object | None = None,
        *,
        catalog_key: str | None = None,
    ):
        """Initialize an EarthCatalog facade.

        Args:
            catalog: PyIceberg SqlCatalog instance
            table: PyIceberg Table instance
            info: CatalogInfo with grid metadata
            store: obstore Store instance (for reading hash index from S3)
            catalog_key: Key within *store* where catalog.db is persisted.
                         Required for ingest() which needs to upload changes.
        """
        self._catalog = catalog
        self._table = table
        self._info = info
        self._store = store
        self._catalog_key = catalog_key

    def search_files(
        self,
        geom,
        start_datetime: str | datetime | None = None,
        end_datetime: str | datetime | None = None,
    ) -> list[str]:
        """Return Parquet file paths for partitions intersecting *geom*.

        Uses Iceberg partition pruning (zero I/O on irrelevant files) and
        returns paths suitable for DuckDB's ``read_parquet()``.

        Args:
            geom: Shapely geometry (Point, Polygon, Box, etc.)
            start_datetime: Filter items after this datetime (inclusive)
            end_datetime: Filter items before this datetime (inclusive)

        Returns:
            List of S3 or local Parquet file paths

        Example::
            from shapely.geometry import box

            greenland = box(-60, 60, -20, 85)
            paths = ec.search_files(greenland, start_datetime='2020-01-01')
        """
        return self._info.file_paths(
            self._table,
            geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def stats(self) -> list[dict]:
        """Return per-partition row counts and file sizes from Iceberg metadata.

        Reads manifest files only — no Parquet data is opened. Each dict
        contains ``grid_partition``, ``year``, ``row_count``, ``file_count``,
        and ``total_bytes`` aggregated across all files in that partition.

        Returns:
            List of partition statistics dicts

        Example::
            for s in ec.stats():
                print(f"{s['grid_partition']}/{s['year']}: {s['row_count']} rows")
        """
        return self._info.stats(self._table)

    def unique_item_count(self) -> int:
        """Return the count of unique STAC items from the hash index.

        Queries the warehouse_id_hashes.parquet file (if available) to get
        the actual count of unique STAC item IDs, excluding duplicates from
        spatial fan-out.

        Returns:
            Number of unique STAC items, or 0 if hash index is not available.
        """
        # Derive default hash index path from warehouse location
        default_hash_index_path = None
        if self._catalog is not None:
            warehouse = self._catalog.properties.get("warehouse", "")
            if warehouse:
                # warehouse/ -> warehouse_id_hashes.parquet
                # e.g., s3://bucket/catalog/warehouse -> s3://bucket/catalog/warehouse_id_hashes.parquet
                default_hash_index_path = warehouse.rstrip("/") + "_id_hashes.parquet"

        return self._info.unique_item_count(self._table, self._store, default_hash_index_path)

    def ingest(
        self,
        inventory_path: str,
        *,
        mode: str = "auto",
        chunk_size: int = 10000,
        limit: int | None = None,
        since: datetime | None = None,
        update_hash_index: bool = False,
    ) -> dict:
        """Ingest STAC items from an S3 Inventory into the catalog.

        Unified entry point replacing both ``backfill.run_backfill`` and
        ``incremental.run``.  Handles full backfill (drop+recreate table)
        and delta append (add files to existing table).

        The caller is responsible for holding an S3Lock around this call
        when running against a shared store (use ``self.lock()``).

        Parameters
        ----------
        inventory_path:
            Path or ``s3://`` URI to an S3 Inventory file (CSV, Parquet,
            or ``manifest.json``).
        mode:
            ``"auto"``  — delta if the table already has data, else full.
            ``"full"``  — drop+recreate Iceberg table, start fresh.
            ``"delta"`` — append new files to the existing table.
        chunk_size:
            Items per fetch batch.  Each batch is fetched from S3 in
            parallel, fanned out through the grid partitioner, written
            as GeoParquet files, and registered with Iceberg.
        limit:
            Stop after processing this many STAC items (for testing).
        since:
            Only process inventory rows whose ``last_modified_date`` is
            >= this datetime (timezone-aware UTC).
        update_hash_index:
            When True, read the ``id`` column from newly registered
            warehouse parquet files, hash each ID, and merge into the
            warehouse hash index (``*_id_hashes.parquet``).

        Returns
        -------
        dict with keys ``items_processed``, ``rows_written``, and
        ``files_registered``.

        Example
        -------
        ::

            store = S3Store(bucket="its-live-data", region="us-west-2")
            ec = catalog.open(store=store, base="s3://my-bucket/catalog")
            ec.ingest("s3://bucket/inventory/delta.parquet", mode="delta")
        """
        import uuid
        from concurrent.futures import ThreadPoolExecutor

        from earthcatalog.core.hash_index import (
            merge_hashes_from_parquets,
            read_hashes,
            write_hashes,
        )
        from earthcatalog.core.transform import (
            fan_out,
            group_by_partition,
            write_geoparquet_s3,
        )
        from earthcatalog.grids import build_partitioner
        from earthcatalog.pipelines.incremental import _fetch_item, _iter_inventory

        # --- resolve mode ---
        if mode == "auto":
            try:
                n = sum(s["row_count"] for s in self._info.stats(self._table))
                mode = "delta" if n > 0 else "full"
            except Exception:
                mode = "full"

        is_delta = mode == "delta"

        # --- build partitioner from table properties ---
        from earthcatalog.config import GridConfig

        grid_cfg = GridConfig(
            type=self._info.grid_type,
            resolution=self._info.grid_resolution,
            boundaries_path=self._info.boundaries_path,
            id_field=self._info.id_field,
        )
        partitioner = build_partitioner(grid_cfg)

        # --- paths ---
        warehouse_root = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///") if uri else "/tmp/earthcatalog.db"

        if self._store and self._catalog_key:
            self.download_catalog(local_db)

        # ----------------------------------------------------------------
        # Full mode: drop + recreate table
        # ----------------------------------------------------------------
        if not is_delta:
            from pyiceberg.exceptions import NoSuchTableError

            from earthcatalog.core.catalog import (
                FULL_NAME,
                NAMESPACE,
                get_or_create,
            )

            try:
                self._catalog.drop_table(FULL_NAME)
            except NoSuchTableError:
                pass
            try:
                self._catalog.create_namespace(NAMESPACE)
            except Exception:
                pass
            self._table = get_or_create(self._catalog, grid_config=grid_cfg)

        # ----------------------------------------------------------------
        # Ingest loop
        # ----------------------------------------------------------------
        total_items = 0
        total_rows = 0
        written_keys: list[str] = []
        batch: list[tuple[str, str]] = []

        def _flush(chunk: list[tuple[str, str]]) -> None:
            nonlocal total_rows

            with ThreadPoolExecutor(max_workers=16) as pool:
                items = list(filter(None, pool.map(lambda bc: _fetch_item(*bc), chunk)))

            if not items:
                return

            fo = fan_out(items, partitioner)
            if not fo:
                return

            for (cell, year), group_items in group_by_partition(fo).items():
                year_str = str(year) if year is not None else "unknown"
                part_tag = uuid.uuid4().hex[:8]
                s3_key = f"grid_partition={cell}/year={year_str}/part_{part_tag}.parquet"
                n, _ = write_geoparquet_s3(group_items, self._store, s3_key)
                if n > 0:
                    written_keys.append(s3_key)
                    total_rows += n

        print(f"Ingesting from: {inventory_path}")
        for bucket, key in _iter_inventory(inventory_path, since=since):
            if not key.endswith(".stac.json"):
                continue
            batch.append((bucket, key))
            total_items += 1
            if len(batch) >= chunk_size:
                _flush(batch)
                batch.clear()
            if limit and total_items >= limit:
                break

        if batch:
            _flush(batch)

        # ----------------------------------------------------------------
        # Register with Iceberg
        # ----------------------------------------------------------------
        if written_keys:
            full_paths = [f"{warehouse_root.rstrip('/')}/{k}" for k in written_keys]
            batch_sz = 2000
            for i in range(0, len(full_paths), batch_sz):
                self._table.add_files(full_paths[i : i + batch_sz])
            print(f"Registered {len(full_paths)} files in Iceberg catalog.")

        # ----------------------------------------------------------------
        # Hash index update
        # ----------------------------------------------------------------
        if update_hash_index and written_keys:
            hash_index_path = self._table.properties.get("earthcatalog.hash_index_path")
            if not hash_index_path:
                hash_index_path = f"{warehouse_root.rstrip('/')}_id_hashes.parquet"
                with self._table.transaction() as tx:
                    tx.set_properties(**{"earthcatalog.hash_index_path": hash_index_path})

            if hash_index_path.startswith("s3://"):
                import re as _re

                m = _re.match(r"s3://([^/]+)/(.+)", hash_index_path)
                if m:
                    hash_key = m.group(2)
                    existing = read_hashes(self._store, hash_key)
                    print(f"  Existing hashes: {len(existing):,}")
                    updated, n_new = merge_hashes_from_parquets(
                        full_paths, existing, store=self._store
                    )
                    print(f"  New hashes: {n_new:,} from {len(full_paths)} files")
                    write_hashes(updated, self._store, hash_key)
            else:
                print("WARN: hash index update skipped — only s3:// paths supported")

        # ----------------------------------------------------------------
        # Upload catalog
        # ----------------------------------------------------------------
        if self._store and self._catalog_key:
            self.upload_catalog(local_db)

        result = {
            "items_processed": total_items,
            "rows_written": total_rows,
            "files_registered": len(written_keys),
        }
        print(f"Done. {total_items} items → {total_rows} rows in {len(written_keys)} files")
        return result

    def bulk_ingest(
        self,
        inventory_path: str,
        *,
        mode: str = "auto",
        chunk_size: int = 100_000,
        compact_rows: int = 100_000,
        limit: int | None = None,
        since: datetime | None = None,
        update_hash_index: bool = False,
        staging_prefix: str | None = None,
        create_client: Callable[[], object] | None = None,
        skip_inventory: bool = False,
        skip_ingest: bool = False,
        retry_pending: bool = False,
    ) -> None:
        """Ingest large inventories using a distributed Dask cluster.

        Thin wrapper around the 4-phase staging pipeline
        (:func:`~earthcatalog.pipelines.backfill.run_backfill`) that derives
        catalog path, warehouse root, and partitioner from this EarthCatalog.

        Unlike :meth:`ingest` (single-node, ``ThreadPoolExecutor``), this method
        is designed for large backfills on Dask/Coiled.  It writes chunk files
        to a staging prefix, fetches items via async S3 I/O in parallel, writes
        NDJSON intermediates, compacts to GeoParquet, and registers files with
        Iceberg — all with spot-instance resilience (completion markers,
        pending-chunk retry, skip-inventory recovery).

        Parameters
        ----------
        inventory_path:
            Path or ``s3://`` URI to an S3 Inventory file (CSV, Parquet,
            or ``manifest.json``).
        mode:
            ``"auto"``  — delta if the table already has data, else full.
            ``"full"``  — drop+recreate Iceberg table, then re-register all
                          warehouse files (start fresh).
            ``"delta"`` — append new files to the existing Iceberg table.
        chunk_size:
            Items per chunk parquet in Phase 1.
        compact_rows:
            Max rows per GeoParquet output file in Phase 3.
        limit:
            Stop after processing this many STAC items (for testing).
        since:
            Only process inventory rows whose ``last_modified_date`` is >=
            this datetime (timezone-aware UTC).
        update_hash_index:
            When True, merge new item IDs into the warehouse hash index.
        staging_prefix:
            S3 key prefix for chunks + NDJSON + completion markers.
            Auto-generated from the current date when not provided.
        create_client:
            Optional callable that returns a Dask Client.  Called lazily
            after Phase 1 completes (avoids idle cluster timeout during
            the inventory scan).  Pass ``lambda: coiled.Client(...)`` for
            Coiled spot clusters.
        skip_inventory:
            If True, skip Phase 1 and reuse existing chunks in staging.
            Useful when re-running after a crash.
        skip_ingest:
            If True, skip Phase 2 and go straight to Phase 3 (compact).
            Useful when NDJSON from a prior run is ready but compact
            needs bigger instances.
        retry_pending:
            If True, re-process chunks that had fetch failures in a
            prior run (stored in ``pending_chunks/``).

        Example
        -------
        ::

            from earthcatalog.core import catalog
            from obstore.store import S3Store

            store = S3Store(bucket="its-live-data", region="us-west-2")
            ec = catalog.open(store=store, base="s3://my-bucket/catalog")
            ec.bulk_ingest(
                "s3://bucket/inventory/full.parquet",
                mode="full",
                create_client=lambda: coiled.Client(n_workers=100),
            )
        """
        from datetime import UTC
        from datetime import datetime as _dt

        from earthcatalog.config import GridConfig
        from earthcatalog.grids import build_partitioner
        from earthcatalog.pipelines.backfill import run_backfill

        # --- derive paths from EarthCatalog state ---
        warehouse_root = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///")

        # --- build partitioner from table properties ---
        grid_cfg = GridConfig(
            type=self._info.grid_type,
            resolution=self._info.grid_resolution,
            boundaries_path=self._info.boundaries_path,
            id_field=self._info.id_field,
        )
        partitioner = build_partitioner(grid_cfg)

        # --- auto-generate staging prefix ---
        if staging_prefix is None:
            date_str = _dt.now(UTC).strftime("%Y%m%d")
            staging_prefix = f"bulk_ingest/{date_str}"

        # --- resolve mode ---
        delta = True
        if mode == "full":
            delta = False
        elif mode == "auto":
            try:
                n = sum(s["row_count"] for s in self._info.stats(self._table))
                delta = n > 0
            except Exception:
                delta = False

        # --- download catalog ---
        if self._store and self._catalog_key:
            self.download_catalog(local_db)

        # --- bridge: set store_config so run_backfill internals can find them ---
        from earthcatalog.core import store_config

        old_store = store_config.get_store()
        old_key = store_config.get_catalog_key()
        try:
            store_config.set_store(self._store)
            if self._catalog_key:
                store_config.set_catalog_key(self._catalog_key)

            run_backfill(
                inventory_path=inventory_path,
                catalog_path=local_db,
                staging_store=self._store,
                staging_prefix=staging_prefix,
                warehouse_store=self._store,
                warehouse_root=warehouse_root,
                partitioner=partitioner,
                chunk_size=chunk_size,
                compact_rows=compact_rows,
                limit=limit,
                since=since,
                use_lock=False,
                upload=True,
                skip_inventory=skip_inventory,
                skip_ingest=skip_ingest,
                retry_pending=retry_pending,
                delta=delta,
                create_client=create_client,
                update_hash_index=update_hash_index,
                hash_index_path=self._table.properties.get("earthcatalog.hash_index_path"),
            )
        finally:
            store_config.set_store(old_store)
            store_config.set_catalog_key(old_key)

    # ------------------------------------------------------------------
    # Catalog lifecycle helpers (use EarthCatalog's store, not globals)
    # ------------------------------------------------------------------

    def download_catalog(self, local_path: str) -> None:
        """Download catalog.db from the backing store to *local_path*."""
        from .catalog import download_catalog as _download

        _download(local_path, store=self._store)

    def upload_catalog(self, local_path: str) -> None:
        """Upload catalog.db from *local_path* to the backing store."""
        from .catalog import upload_catalog as _upload

        _upload(local_path, store=self._store)

    def lock(self, owner: str, ttl_hours: int = 12) -> S3Lock:
        """Return an S3Lock that uses this EarthCatalog's store and key.

        The lock key is derived from the catalog key (if available) or
        falls back to ``".lock"``.
        """
        from .lock import S3Lock

        lock_key = getattr(self._catalog, "_lock_key", None) or ".lock"
        return S3Lock(owner=owner, ttl_hours=ttl_hours, store=self._store, key=lock_key)

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys (H3 cells or GeoJSON IDs) that intersect *geom*.

        Useful for debugging spatial queries or building custom SQL filters.

        Args:
            geom: Shapely geometry

        Returns:
            List of partition keys (H3 cell IDs or GeoJSON region IDs)
        """
        return self._info.cells_for_geometry(geom)

    def cell_list_sql(self, geom) -> str:
        """Return a SQL fragment suitable for ``WHERE grid_partition IN (...)``.

        Example::
            info = ec.cell_list_sql(box(-60, 60, -30, 80))
            sql = f"SELECT * FROM t WHERE {info} AND datetime > '2020-01-01'"
        """
        return self._info.cell_list_sql(geom)

    @property
    def grid_type(self) -> str:
        """Return the grid partitioning system type."""
        return self._info.grid_type

    @property
    def grid_resolution(self) -> int | None:
        """Return the H3/S2 resolution (None for GeoJSON grids)."""
        return self._info.grid_resolution

    @property
    def table(self) -> Table:
        """Return the underlying PyIceberg Table (for advanced use)."""
        return self._table

    def _repr_html_(self) -> str:
        """Return an HTML representation for Jupyter notebooks (similar to xarray).

        Only uses metadata from table properties - no manifest scans or expensive queries.
        Statistics like total files and top cells are available via .stats() and .info methods.
        """
        # Build rows for the main table - only metadata from table properties
        rows = [
            ("Grid type", self._info.grid_type),
        ]

        if self._info.grid_type == "h3":
            rows.append(("H3 resolution", str(self._info.grid_resolution)))
        else:
            rows.append(("Boundaries", self._info.boundaries_path or "N/A"))

        # Add warehouse location if catalog is available
        if self._catalog is not None:
            warehouse_path = self._catalog.properties.get("warehouse", "")
            rows.append(("Warehouse", warehouse_path))

        # Check if hash index is available
        hash_index_path = self._table.properties.get("earthcatalog.hash_index_path")
        if hash_index_path:
            rows.append(("Hash index", "Available"))
        else:
            rows.append(("Hash index", "Not available"))

        # Build the main table HTML
        main_table_html = "<table style='border-collapse: collapse; width: 100%;'>"
        for label, value in rows:
            main_table_html += f"""
                <tr style='border-bottom: 1px solid currentColor;'>
                    <td style='padding: 6px 10px; text-align: left; border: none;'>{label}</td>
                    <td style='padding: 6px 10px; text-align: left; border: none;'><strong>{value}</strong></td>
                </tr>"""
        main_table_html += "</table>"

        # Build statistics section (right column) - all methods share cached manifest scan
        stats = self._info.stats(self._table)
        if stats:
            total_files = self._info.total_files(self._table)  # uses cached stats
            total_rows = sum(s["row_count"] for s in stats)

            # Derive default hash index path from warehouse location
            default_hash_index_path = None
            if self._catalog is not None:
                warehouse = self._catalog.properties.get("warehouse", "")
                if warehouse:
                    default_hash_index_path = warehouse.rstrip("/") + "_id_hashes.parquet"

            unique_items = self._info.unique_item_count(
                self._table, self._store, default_hash_index_path
            )

            stats_html = f"""
            <div style='font-weight: 600; margin-bottom: 8px;'>Statistics</div>
            <table style='border-collapse: collapse; width: 100%; font-size: 13px;'>
                <tr style='border-bottom: 1px solid currentColor;'>
                    <td style='padding: 4px 6px; text-align: left; border: none;'>Total files</td>
                    <td style='padding: 4px 6px; text-align: right; border: none;'><strong>{total_files:,}</strong></td>
                </tr>
                <tr style='border-bottom: 1px solid currentColor;'>
                    <td style='padding: 4px 6px; text-align: left; border: none;'>Total rows</td>
                    <td style='padding: 4px 6px; text-align: right; border: none;'><strong>{total_rows:,}</strong></td>
                </tr>
                <tr style='border-bottom: 1px solid currentColor;'>
                    <td style='padding: 4px 6px; text-align: left; border: none;'>Unique items</td>
                    <td style='padding: 4px 6px; text-align: right; border: none;'><strong>{unique_items:,}</strong></td>
                </tr>
                <tr>
                    <td style='padding: 4px 6px; text-align: left; border: none;'>Partitions</td>
                    <td style='padding: 4px 6px; text-align: right; border: none;'><strong>{len(stats):,}</strong></td>
                </tr>
            </table>
            """

            # Add top partitions
            top_cells = self._info.top_cells(self._table, limit=3)
            if top_cells:
                stats_html += """
                <div style='font-weight: 600; margin-top: 16px; margin-bottom: 8px;'>Top partitions</div>
                <table style='border-collapse: collapse; width: 100%; font-size: 13px;'>
                """
                for cell in top_cells:
                    stats_html += f"""
                    <tr style='border-bottom: 1px solid currentColor;'>
                        <td style='padding: 4px 6px; text-align: left; border: none; font-family: monospace;'>{cell["grid_partition"][:12]}...</td>
                        <td style='padding: 4px 6px; text-align: right; border: none;'>{cell["row_count"]:,} rows</td>
                    </tr>"""
                stats_html += "</table>"
        else:
            stats_html = "<div style='opacity: 0.7;'>No data</div>"

        # Build final HTML with 2-column layout
        html = f"""
        <div style='border: 1px solid currentColor; padding: 15px; border-radius: 5px; font-family: var(--jp-code-font-family, monospace); opacity: 0.9;'>
            <div style='font-size: 16px; font-weight: 600; margin-bottom: 12px; display: flex; align-items: center; gap: 8px;'>
                <span>🌍</span><span>EarthCatalog</span>
            </div>
            <div style='display: flex; gap: 30px;'>
                <div style='flex: 1;'>
                    {main_table_html}
                </div>
                <div style='flex: 1;'>
                    {stats_html}
                </div>
            </div>
        </div>
        """

        return html

    def __repr__(self) -> str:
        if self._info.grid_type == "h3":
            return f"EarthCatalog(grid_type='h3', resolution={self._info.grid_resolution})"
        return (
            f"EarthCatalog(grid_type='geojson', "
            f"boundaries_path={self._info.boundaries_path!r}, "
            f"id_field={self._info.id_field!r})"
        )
