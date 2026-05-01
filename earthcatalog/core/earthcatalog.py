"""
EarthCatalog — simplified facade for querying spatially-partitioned STAC catalogs.

Provides a clean API that encapsulates PyIceberg catalog, table, and grid metadata
discovery into a single object.
"""

from __future__ import annotations

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
        self, catalog: object, table: Table, info: CatalogInfo, store: object | None = None
    ):
        """Initialize an EarthCatalog facade.

        Args:
            catalog: PyIceberg SqlCatalog instance
            table: PyIceberg Table instance
            info: CatalogInfo with grid metadata
            store: obstore Store instance (for reading hash index from S3)
        """
        self._catalog = catalog
        self._table = table
        self._info = info
        self._store = store

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
