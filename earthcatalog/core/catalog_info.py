"""
catalog_info — discover grid system metadata from an open Iceberg table.

Grid type, resolution, and related parameters are stored as Iceberg table
properties at ingest time (see :func:`~earthcatalog.core.catalog.get_or_create_table`).

Example::

    from earthcatalog.core import catalog_info, open_catalog, get_or_create_table
    from shapely.geometry import box

    catalog = open_catalog(db_path="catalog.db", warehouse_path="warehouse/")
    table   = get_or_create_table(catalog)
    info    = catalog_info(table)

    bbox  = box(-60, 60, -30, 80)          # Greenland
    paths = info.file_paths(table, bbox)    # Iceberg-pruned file list
"""

from __future__ import annotations

import io
import struct
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from earthcatalog.core.catalog import (
    PROP_GRID_BOUNDARIES_PATH,
    PROP_GRID_ID_FIELD,
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
)


def _parse_dt(value: str | datetime) -> datetime:
    """Parse a datetime string or datetime object into a timezone-aware datetime.

    Supports flexible input formats:
    - Full ISO: "2020-01-15T10:30:00Z", "2020-01-15"
    - Year-month: "2020-01" → "2020-01-01T00:00:00Z"
    - Year only: "2020" → "2020-01-01T00:00:00Z"
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    # Try standard ISO format first
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        pass

    # Try partial date formats
    value = value.strip()

    # Year-month format: "2020-01"
    if len(value) == 7 and value.count("-") == 1:
        try:
            year, month = value.split("-")
            return datetime(int(year), int(month), 1, tzinfo=UTC)
        except ValueError:
            pass  # Fall through to error message

    # Year only format: "2020"
    if len(value) == 4 and value.isdigit():
        try:
            return datetime(int(value), 1, 1, tzinfo=UTC)
        except ValueError:
            pass  # Fall through to error message

    # If all else fails, raise
    raise ValueError(
        f"Unable to parse datetime: {value!r}. "
        "Supported formats: ISO 8601 (e.g., '2020-01-15', '2020-01-15T10:30:00Z'), "
        "year-month (e.g., '2020-01'), or year only (e.g., '2020')."
    )


def _parquet_row_count_from_store(store, obstore_key: str) -> int:
    """Read row count from a remote Parquet file's footer only — no full download."""
    import obstore
    import pyarrow.parquet as pq

    head_result = obstore.head(store, obstore_key)
    file_size = head_result["size"] if isinstance(head_result, dict) else head_result.size

    # Parquet footer suffix: 4-byte metadata length + 4-byte magic "PAR1"
    suffix = obstore.get_range(store, obstore_key, start=file_size - 8, end=file_size)
    if hasattr(suffix, "to_bytes"):
        suffix = suffix.to_bytes()
    elif not isinstance(suffix, bytes):
        suffix = bytes(suffix)
    metadata_len = struct.unpack("<i", suffix[:4])[0]

    footer = obstore.get_range(
        store,
        obstore_key,
        start=file_size - 8 - metadata_len,
        end=file_size,
    )
    if hasattr(footer, "to_bytes"):
        footer = footer.to_bytes()
    elif not isinstance(footer, bytes):
        footer = bytes(footer)

    return pq.ParquetFile(io.BytesIO(footer)).metadata.num_rows


def _build_stats_cache(table) -> list[dict]:
    """Aggregate per-(partition, year) stats from Iceberg manifests. No Parquet I/O."""
    agg: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0, 0])
    for task in table.scan().plan_files():
        f = task.file
        key = (f.partition[0], f.partition[1] + 1970)
        agg[key][0] += f.record_count
        agg[key][1] += 1
        agg[key][2] += f.file_size_in_bytes

    return [
        {
            "grid_partition": cell,
            "year": year,
            "row_count": rows,
            "file_count": files,
            "total_bytes": total_bytes,
        }
        for (cell, year), (rows, files, total_bytes) in sorted(agg.items())
    ]


@dataclass
class CatalogInfo:
    """Grid metadata read from Iceberg table properties.

    Construct via :func:`catalog_info(table)` — not directly.
    """

    grid_type: str
    grid_resolution: int | None
    boundaries_path: str | None
    id_field: str | None
    _cached_stats: list[dict] | None = field(default=None, repr=False)
    _cached_top_cells: list[dict] | None = field(default=None, repr=False)

    # ------------------------------------------------------------------
    # Geometry helpers
    # ------------------------------------------------------------------

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys that intersect *geom*."""
        if self.grid_type == "h3":
            return self._h3_cells(geom)
        if self.grid_type == "geojson":
            return self._geojson_keys(geom)
        raise ValueError(f"Unknown grid type: {self.grid_type!r}")

    def cell_list_sql(self, geom) -> str:
        """Return a SQL fragment suitable for ``WHERE grid_partition IN (...)``."""
        cells = self.cells_for_geometry(geom)
        if not cells:
            return "grid_partition IN (NULL)"
        quoted = ", ".join(f"'{c}'" for c in cells)
        return f"grid_partition IN ({quoted})"

    # ------------------------------------------------------------------
    # File paths
    # ------------------------------------------------------------------

    def file_paths(
        self,
        table,
        geom,
        start_datetime: str | datetime | None = None,
        end_datetime: str | datetime | None = None,
    ) -> list[str]:
        """Return Parquet file paths for partitions intersecting *geom*.

        Uses Iceberg partition pruning (zero I/O on irrelevant files).
        Datetime filters are pushed down to the ``year`` partition.
        """
        from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual

        cells = self.cells_for_geometry(geom)
        if not cells:
            return []

        expr = In("grid_partition", cells)
        if start_datetime is not None:
            expr = And(expr, GreaterThanOrEqual("datetime", _parse_dt(start_datetime)))
        if end_datetime is not None:
            expr = And(expr, LessThanOrEqual("datetime", _parse_dt(end_datetime)))

        return [task.file.file_path for task in table.scan(row_filter=expr).plan_files()]

    # ------------------------------------------------------------------
    # Stats / counts  (all manifest-only, no Parquet data reads)
    # ------------------------------------------------------------------

    def _ensure_stats(self, table) -> list[dict]:
        """Populate and return the stats cache (single manifest scan)."""
        if self._cached_stats is None:
            self._cached_stats = _build_stats_cache(table)
            # Derive top-cells cache for free while we have the data
            if self._cached_top_cells is None:
                cell_agg: dict[str, list[int]] = defaultdict(lambda: [0, 0])
                for s in self._cached_stats:
                    cell_agg[s["grid_partition"]][0] += s["row_count"]
                    cell_agg[s["grid_partition"]][1] += s["file_count"]
                self._cached_top_cells = sorted(
                    [
                        {"grid_partition": cell, "row_count": rows, "file_count": files}
                        for cell, (rows, files) in cell_agg.items()
                    ],
                    key=lambda d: d["row_count"],
                    reverse=True,
                )
        return self._cached_stats

    def stats(self, table) -> list[dict]:
        """Per-partition row counts and file sizes from Iceberg manifests.

        Each dict contains ``grid_partition``, ``year``, ``row_count``,
        ``file_count``, and ``total_bytes``. Results are cached.
        """
        return self._ensure_stats(table)

    def top_cells(self, table, limit: int = 5) -> list[dict]:
        """Top partitions by row count (cached alongside :meth:`stats`).

        Returns dicts with ``grid_partition``, ``row_count``, and ``file_count``.
        """
        self._ensure_stats(table)  # populates both caches in one pass
        return self._cached_top_cells[:limit]  # type: ignore[index]

    def total_files(self, table) -> int:
        """Total Parquet file count from Iceberg snapshot manifests."""
        return sum(s["file_count"] for s in self._ensure_stats(table))

    def unique_item_count(self, table, store, default_hash_index_path: str | None = None) -> int:
        """Row count of the hash-index Parquet file (footer read only).

        Reads only the remote footer — not the full file. Returns 0 if the
        hash index is not found.

        Args:
            table: PyIceberg Table instance
            store: obstore Store instance (for S3 paths)
            default_hash_index_path: Optional default path to try if table property is not set.
                                   Typically derived from warehouse path (e.g., ``s3://.../catalog/warehouse_id_hashes.parquet``).
        """
        import pyarrow.parquet as pq

        hash_index_path = table.properties.get("earthcatalog.hash_index_path")
        if hash_index_path is None:
            hash_index_path = default_hash_index_path
        if not hash_index_path:
            return 0

        try:
            if hash_index_path.startswith("s3://"):
                if not store:
                    return 0
                _, _, rest = hash_index_path.partition("s3://")
                obstore_key = rest.split("/", 1)[1] if "/" in rest else ""
                if not obstore_key:
                    return 0
                return _parquet_row_count_from_store(store, obstore_key)

            # Local path
            from pathlib import Path

            if not Path(hash_index_path).exists():
                return 0
            return pq.ParquetFile(hash_index_path).metadata.num_rows

        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Grid internals
    # ------------------------------------------------------------------

    def _h3_cells(self, geom) -> list[str]:
        from shapely import wkb

        from earthcatalog.grids.h3_partitioner import H3Partitioner

        res = self.grid_resolution if self.grid_resolution is not None else 1
        return H3Partitioner(resolution=res).get_intersecting_keys(wkb.dumps(geom))

    def _geojson_keys(self, geom) -> list[str]:
        if not self.boundaries_path:
            raise ValueError(
                "boundaries_path is required for geojson grid type. "
                "Re-ingest with a GridConfig that specifies boundaries_path."
            )
        from shapely import wkb

        from earthcatalog.grids.geojson_partitioner import GeoJSONPartitioner

        return GeoJSONPartitioner(
            boundaries_path=self.boundaries_path,
            id_field=self.id_field or "id",
        ).get_intersecting_keys(wkb.dumps(geom))

    def __repr__(self) -> str:  # pragma: no cover
        if self.grid_type == "h3":
            return f"CatalogInfo(grid_type='h3', resolution={self.grid_resolution})"
        return (
            f"CatalogInfo(grid_type='geojson', "
            f"boundaries_path={self.boundaries_path!r}, id_field={self.id_field!r})"
        )


def catalog_info(table) -> CatalogInfo:
    """Build a CatalogInfo from an open PyIceberg Table.

    Falls back to sensible defaults for pre-property catalogs
    (grid_type="h3", grid_resolution=1).
    """
    props = table.properties
    grid_type = props.get(PROP_GRID_TYPE, "h3")
    raw_res = props.get(PROP_GRID_RESOLUTION)
    grid_resolution = int(raw_res) if raw_res is not None else (1 if grid_type == "h3" else None)
    return CatalogInfo(
        grid_type=grid_type,
        grid_resolution=grid_resolution,
        boundaries_path=props.get(PROP_GRID_BOUNDARIES_PATH),
        id_field=props.get(PROP_GRID_ID_FIELD),
    )
