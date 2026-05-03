"""
EarthCatalog — simplified facade for querying spatially-partitioned STAC catalogs.

Provides a clean API that encapsulates PyIceberg catalog, table, and grid metadata
discovery into a single object.
"""

from __future__ import annotations

import io
import re
import struct
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import obstore
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

if TYPE_CHECKING:
    from .lock import S3Lock

from . import store_config

# Regex for matching hive-style warehouse partition paths.
_HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)

# ---------------------------------------------------------------------------
# Iceberg catalog constants
# ---------------------------------------------------------------------------

NAMESPACE = "earthcatalog"
TABLE_NAME = "stac_items"
FULL_NAME = f"{NAMESPACE}.{TABLE_NAME}"

# Iceberg table property keys for grid metadata.
# Written at table-creation time so downstream readers don't need a priori
# knowledge of the grid system or resolution used during ingest.
PROP_GRID_TYPE = "earthcatalog.grid.type"
PROP_GRID_RESOLUTION = "earthcatalog.grid.resolution"
PROP_GRID_BOUNDARIES_PATH = "earthcatalog.grid.boundaries_path"
PROP_GRID_ID_FIELD = "earthcatalog.grid.id_field"
PROP_HASH_INDEX_PATH = "earthcatalog.hash_index_path"

# PyIceberg schema — matches normalized rustac stac-geoparquet output.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", StringType(), required=False),
    NestedField(2, "grid_partition", StringType(), required=False),
    NestedField(3, "geometry", BinaryType(), required=False),
    NestedField(4, "datetime", TimestamptzType(), required=False),
    NestedField(5, "platform", StringType(), required=False),
    NestedField(6, "percent_valid_pixels", LongType(), required=False),
    NestedField(7, "date_dt", LongType(), required=False),
    NestedField(8, "proj:code", StringType(), required=False),
    NestedField(9, "assets", StringType(), required=False),
    NestedField(10, "links", StringType(), required=False),
    NestedField(11, "stac_version", StringType(), required=False),
    NestedField(12, "type", StringType(), required=False),
    NestedField(13, "start_datetime", TimestamptzType(), required=False),
    NestedField(14, "version", StringType(), required=False),
    NestedField(15, "sat:orbit_state", StringType(), required=False),
    NestedField(16, "scene_1_id", StringType(), required=False),
    NestedField(17, "scene_2_id", StringType(), required=False),
    NestedField(18, "scene_1_frame", StringType(), required=False),
    NestedField(19, "scene_2_frame", StringType(), required=False),
    NestedField(20, "mid_datetime", StringType(), required=False),
    NestedField(21, "created", TimestamptzType(), required=False),
    NestedField(22, "updated", TimestamptzType(), required=False),
    NestedField(23, "end_datetime", TimestamptzType(), required=False),
    NestedField(24, "stac_extensions", StringType(), required=False),
    NestedField(25, "collection", StringType(), required=False),
    NestedField(26, "latitude", DoubleType(), required=False),
    NestedField(27, "longitude", DoubleType(), required=False),
    NestedField(28, "bbox", StringType(), required=False),
)

# Partition spec: grid cell (identity) + year of acquisition.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="grid_partition"),
    PartitionField(source_id=4, field_id=101, transform=YearTransform(), name="year"),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_dt(value: str | datetime) -> datetime:
    """Parse a datetime string or datetime object into a timezone-aware datetime."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        pass

    value = value.strip()

    if len(value) == 7 and value.count("-") == 1:
        try:
            year, month = value.split("-")
            return datetime(int(year), int(month), 1, tzinfo=UTC)
        except ValueError:
            pass

    if len(value) == 4 and value.isdigit():
        try:
            return datetime(int(value), 1, 1, tzinfo=UTC)
        except ValueError:
            pass

    raise ValueError(
        f"Unable to parse datetime: {value!r}. "
        "Supported formats: ISO 8601 (e.g., '2020-01-15', '2020-01-15T10:30:00Z'), "
        "year-month (e.g., '2020-01'), or year only (e.g., '2020')."
    )


def _parquet_row_count_from_store(store, obstore_key: str) -> int:
    """Read row count from a remote Parquet file's footer only — no full download."""
    import pyarrow.parquet as pq

    head_result = obstore.head(store, obstore_key)
    file_size = head_result["size"] if isinstance(head_result, dict) else head_result.size

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


# ---------------------------------------------------------------------------
# CatalogInfo — grid metadata from Iceberg table properties
# ---------------------------------------------------------------------------


@dataclass
class CatalogInfo:
    """Grid metadata read from Iceberg table properties.
    """

    grid_type: str
    grid_resolution: int | None
    boundaries_path: str | None
    id_field: str | None
    _cached_stats: list[dict] | None = field(default=None, repr=False)
    _cached_top_cells: list[dict] | None = field(default=None, repr=False)

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

    def file_paths(
        self,
        table,
        geom,
        start_datetime: str | datetime | None = None,
        end_datetime: str | datetime | None = None,
    ) -> list[str]:
        """Return Parquet file paths for partitions intersecting *geom*."""
        from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual

        cells = self.cells_for_geometry(geom)
        if not cells:
            return []

        expr = In("grid_partition", cells)
        if start_datetime is not None:
            expr = And(expr, GreaterThanOrEqual("datetime", _parse_dt(start_datetime)))
        if end_datetime is not None:
            expr = And(expr, LessThanOrEqual("datetime", _parse_dt(end_datetime)))

        start_year = _parse_dt(start_datetime).year if start_datetime is not None else None
        end_year = _parse_dt(end_datetime).year if end_datetime is not None else None

        paths = []
        for task in table.scan(row_filter=expr).plan_files():
            year = task.file.partition[1] + 1970
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            paths.append(task.file.file_path)

        return paths

    def _ensure_stats(self, table) -> list[dict]:
        if self._cached_stats is None:
            self._cached_stats = _build_stats_cache(table)
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
        """Per-partition row counts and file sizes from Iceberg manifests."""
        return self._ensure_stats(table)

    def top_cells(self, table, limit: int = 5) -> list[dict]:
        """Top partitions by row count (cached alongside :meth:`stats`)."""
        self._ensure_stats(table)
        return self._cached_top_cells[:limit]  # type: ignore[index]

    def total_files(self, table) -> int:
        """Total Parquet file count from Iceberg snapshot manifests."""
        return sum(s["file_count"] for s in self._ensure_stats(table))

    def unique_item_count(self, table, store, default_hash_index_path: str | None = None) -> int:
        """Row count of the hash-index Parquet file (footer read only)."""
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

            if not Path(hash_index_path).exists():
                return 0
            return pq.ParquetFile(hash_index_path).metadata.num_rows

        except Exception:
            return 0

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

    def __repr__(self) -> str:
        if self.grid_type == "h3":
            return f"CatalogInfo(grid_type='h3', resolution={self.grid_resolution})"
        return (
            f"CatalogInfo(grid_type='geojson', "
            f"boundaries_path={self.boundaries_path!r}, id_field={self.id_field!r})"
        )


def _catalog_info(table) -> CatalogInfo:
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


# ---------------------------------------------------------------------------
# SQLite-in-S3 catalog lifecycle
# ---------------------------------------------------------------------------


def _open_sqlite(db_path: str, warehouse_path: str) -> SqlCatalog:
    """Open a PyIceberg SqlCatalog from local paths (internal use)."""
    import os

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    props: dict = {"uri": f"sqlite:///{db_path}", "warehouse": warehouse_path}

    if warehouse_path.startswith("s3://"):
        props["s3.region"] = region
        key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        token = os.environ.get("AWS_SESSION_TOKEN", "")
        if key_id and secret:
            props["s3.access-key-id"] = key_id
            props["s3.secret-access-key"] = secret
            if token:
                props["s3.session-token"] = token
        else:
            props["s3.anonymous"] = "true"
            props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"

    return SqlCatalog(NAMESPACE, **props)


def download_catalog(
    local_path: str,
    store: object | None = None,
    catalog_key: str | None = None,
) -> None:
    """Pull catalog.db from *store* to *local_path* before a job starts."""
    if store is None or catalog_key is None:
        store = store_config.get_store()
        catalog_key = store_config.get_catalog_key()
    try:
        result = obstore.get(store, catalog_key)
        Path(local_path).write_bytes(bytes(result.bytes()))
        print(f"Catalog downloaded: {catalog_key} -> {local_path}")
    except FileNotFoundError:
        print(f"No existing catalog at '{catalog_key}' — will create fresh.")


def upload_catalog(
    local_path: str,
    store: object | None = None,
    catalog_key: str | None = None,
) -> None:
    """Push the updated catalog.db to *store* after all writes."""
    if store is None or catalog_key is None:
        store = store_config.get_store()
        catalog_key = store_config.get_catalog_key()
    obstore.put(store, catalog_key, Path(local_path).read_bytes())
    print(f"Catalog uploaded: {local_path} -> {catalog_key}")


def get_or_create(catalog: SqlCatalog, grid_config=None) -> object:
    """Return the stac_items table, creating it (and the namespace) if needed.

    Parameters
    ----------
    catalog:
        Open SqlCatalog instance.
    grid_config:
        Optional :class:`earthcatalog.config.GridConfig`.  When provided, grid
        metadata (type, resolution, boundaries_path, id_field) is stored as
        Iceberg table properties so that :class:`CatalogInfo`
        can reconstruct the grid system without any external configuration.
    """
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    props: dict[str, str] = {}
    if grid_config is not None:
        props[PROP_GRID_TYPE] = str(grid_config.type)
        if grid_config.resolution is not None:
            props[PROP_GRID_RESOLUTION] = str(grid_config.resolution)
        if grid_config.boundaries_path is not None:
            props[PROP_GRID_BOUNDARIES_PATH] = str(grid_config.boundaries_path)
        if grid_config.id_field is not None:
            props[PROP_GRID_ID_FIELD] = str(grid_config.id_field)

    try:
        table = catalog.load_table(FULL_NAME)
        missing = {k: v for k, v in props.items() if k not in table.properties}
        if missing:
            with table.transaction() as tx:
                tx.set_properties(**missing)
        return table
    except NoSuchTableError:
        return catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
            properties=props,
        )


# ---------------------------------------------------------------------------
# Catalog open / ingest
# ---------------------------------------------------------------------------


def open(
    store: object,
    base: str,
    *,
    anonymous: bool | None = None,
) -> EarthCatalog:
    """Open an EarthCatalog backed by *store* at *base*.

    Parameters
    ----------
    store:
        An obstore-compatible store (``S3Store``, ``LocalStore``, etc.).
        All catalog I/O (download, upload) and warehouse file operations
        flow through this store.
    base:
        Base path containing:
        - ``earthcatalog.db``   (SQLite Iceberg catalog)
        - ``warehouse/``        (GeoParquet files)
        Optionally:
        - ``warehouse_id_hashes.parquet`` (hash index)
    anonymous:
        Force anonymous S3 access when the warehouse path is ``s3://``.
        Auto-detected for stores with ``skip_signature=True``.

    Returns
    -------
    EarthCatalog
        Facade combining PyIceberg catalog, table, and grid metadata.
    """
    import os
    import tempfile
    import uuid

    _warehouse_path = f"{base}/warehouse"

    if base.startswith("s3://"):
        rest = base[5:]
        parts = rest.split("/", 1)
        catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"
    else:
        catalog_key = str(Path(base) / "earthcatalog.db")

    _db_path = str(Path(tempfile.gettempdir()) / f"earthcatalog_{uuid.uuid4().hex[:8]}.db")
    try:
        result = obstore.get(store, catalog_key)
        Path(_db_path).write_bytes(bytes(result.bytes()))
    except FileNotFoundError:
        pass

    if anonymous is None and hasattr(store, "config"):
        skip_sig = store.config.get("skip_signature")
        if skip_sig in (True, "true"):
            anonymous = True

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    props: dict = {"uri": f"sqlite:///{_db_path}", "warehouse": _warehouse_path}

    if _warehouse_path.startswith("s3://"):
        props["s3.region"] = region
        if anonymous:
            props["s3.anonymous"] = "true"
            props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"
        else:
            props["s3.anonymous"] = "true"
            props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"

    sql_catalog = SqlCatalog(NAMESPACE, **props)
    table = get_or_create(sql_catalog)
    return EarthCatalog(
        catalog=sql_catalog,
        table=table,
        info=_catalog_info(table),
        store=store,
        catalog_key=catalog_key,
    )


def ingest(
    inventory_path: str,
    *,
    store: object | None = None,
    base: str | None = None,
    mode: str = "auto",
    chunk_size: int = 10000,
    limit: int | None = None,
    since: datetime | None = None,
    update_hash_index: bool = False,
) -> dict:
    """Open an EarthCatalog and ingest STAC items from an inventory.

    Convenience wrapper around ``EarthCatalog.ingest()`` for callers that
    only have a store and base path.

    Parameters
    ----------
    inventory_path:
        Path or ``s3://`` URI to an S3 Inventory file.
    store:
        An obstore-compatible store (``S3Store``, ``LocalStore``, etc.).
    base:
        Base path containing ``earthcatalog.db`` and ``warehouse/``.
    mode:
        ``"auto"``, ``"full"``, or ``"delta"``.  See ``EarthCatalog.ingest``.
    chunk_size:
        Items per fetch batch.
    limit:
        Max items to process.
    since:
        Only process items modified after this datetime.
    update_hash_index:
        Update the warehouse hash index after ingest.

    Returns
    -------
    dict with keys ``items_processed``, ``rows_written``, ``files_registered``.
    """
    ec = open(store=store, base=base)
    return ec.ingest(
        inventory_path=inventory_path,
        mode=mode,
        chunk_size=chunk_size,
        limit=limit,
        since=since,
        update_hash_index=update_hash_index,
    )


# ---------------------------------------------------------------------------
# EarthCatalog — main facade
# ---------------------------------------------------------------------------


class EarthCatalog:
    """Simplified facade for querying an EarthCatalog.

    Combines PyIceberg catalog, table, and CatalogInfo into a single interface
    for spatial/temporal queries with automatic file pruning.

    Example::

        from earthcatalog import open as ec_open
        from obstore.store import S3Store
        from shapely.geometry import Point

        store = S3Store(bucket='my-bucket', region='us-west-2')
        ec = ec_open(store=store, base='s3://my-bucket/catalog')

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
        """Return Parquet file paths for partitions intersecting *geom*."""
        return self._info.file_paths(
            self._table,
            geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def search(self, **kwargs):
        """Search across the catalog, returning a deferred ``EarthCatalogItemSearch``.

        Accepts the same kwargs as :func:`rustac.search`:
        ``intersects``, ``bbox``, ``datetime``, ``filter`` (CQL2 JSON),
        ``ids``, ``collections``, ``max_items``, ``limit``, ``sortby``,
        ``include``, ``exclude``, ``query``, etc.

        Use the top-level ``datetime`` kwarg for temporal filtering.  Do
        **not** reference ``datetime`` inside the CQL2 ``filter`` —
        rustac generates broken SQL when ``datetime`` appears in a CQL2
        expression.

        Returns
        -------
        EarthCatalogItemSearch
            A lazy, pystac_client-compatible search result.  No I/O until
            ``items()``, ``item_collection()``, or ``pages()`` is called.
        """
        from .search import EarthCatalogItemSearch, _FileSearchEngine

        engine = _FileSearchEngine(prune_fn=self._search_prune)
        return EarthCatalogItemSearch(
            params=kwargs,
            engine=engine,
            table=self._table,
            anonymous_ctx=self._cleared_env_s3,
        )

    def search_to_arrow(self, **kwargs):
        """Search across the catalog, returning a PyArrow table."""
        from .search import _FileSearchEngine

        engine = _FileSearchEngine(prune_fn=self._search_prune)
        with self._cleared_env_s3():
            return engine.search_to_arrow(**kwargs)

    def duck_search(self, format: str = "pystac", **kwargs):
        """Search using DuckDB, returning results in the requested *format*.

        Accepts the same kwargs as :meth:`search` (``intersects``, ``bbox``,
        ``datetime``, ``filter``, ``max_items``, etc.).

        DuckDB reads Parquet files in parallel internally, making this
        significantly faster than :meth:`search` for queries spanning many
        files (e.g. wide temporal ranges with sparse data).

        Parameters
        ----------
        format:
            ``"pystac"`` (default) — return ``list[pystac.Item]``.
            ``"native"`` — return a ``pandas.DataFrame`` with flat columns
            (no pystac conversion overhead).

        Use ``cql2.parse_text()`` for the ``filter`` kwarg, or pass raw
        CQL2 JSON dicts directly.

        Examples::

            import cql2
            items = catalog.duck_search(
                intersects={"type": "Point", "coordinates": [-45, 70]},
                datetime="1980-01-01/2015-12-31",
                filter=cql2.parse_text("percent_valid_pixels >= 80").to_json(),
                max_items=100,
            )
        """
        import os
        import duckdb
        from shapely.geometry import shape

        from .search import _extract_datetime_range, _cql2_to_sql, _rehydrate

        # --- geometry ---
        geom = None
        if "intersects" in kwargs:
            geom = shape(kwargs["intersects"])
        elif "bbox" in kwargs:
            from shapely.geometry import box
            b = kwargs["bbox"]
            geom = box(b[0], b[1], b[2], b[3])

        # --- Iceberg pruning ---
        start_dt, end_dt = _extract_datetime_range(**kwargs)
        paths = self._info.file_paths(
            self._table, geom,
            start_datetime=start_dt, end_datetime=end_dt,
        )
        if not paths:
            return []

        # --- build SQL ---
        path_list = ", ".join(repr(p) for p in paths)
        conditions: list[str] = []

        # spatial
        if geom is not None:
            conditions.append(f"ST_Intersects(geometry, ST_GeomFromText('{geom.wkt}'))")

        # temporal
        if start_dt is not None:
            conditions.append(f"datetime >= '{start_dt}'")
        if end_dt is not None:
            conditions.append(f"datetime <= '{end_dt}'")

        # CQL2 filter
        raw_filter = kwargs.get("filter")
        if raw_filter is not None:
            conditions.append(_cql2_to_sql(raw_filter))

        where = " AND ".join(conditions) if conditions else "TRUE"
        max_items = kwargs.get("max_items")
        limit = f" LIMIT {max_items}" if max_items is not None else ""

        sql = f"SELECT * FROM read_parquet([{path_list}]) WHERE {where}{limit}"

        # --- execute ---
        saved = {
            "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
            "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
            "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
        }
        os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            df = con.execute(sql).fetchdf()
        finally:
            os.environ.pop("AWS_NO_SIGN_REQUEST", None)
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

        # --- convert results ---
        if format == "native":
            return df

        _TOP_LEVEL = {"id", "type", "stac_version", "stac_extensions", "geometry", "bbox", "assets", "links", "collection"}
        items = []
        import pandas as pd
        import pystac
        from shapely import wkb

        for _, row in df.iterrows():
            d = row.to_dict()
            # DuckDB returns flat columns; nest non-top-level fields into "properties"
            props = {}
            top = {}
            for k, v in d.items():
                if k in _TOP_LEVEL:
                    top[k] = v
                else:
                    props[k] = v
            top["properties"] = props
            d = top

            # Convert pandas Timestamps → ISO strings
            for k, v in list(d.get("properties", {}).items()):
                if isinstance(v, pd.Timestamp):
                    d["properties"][k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")

            # geometry: WKB bytes → GeoJSON dict
            geo = d.get("geometry")
            if isinstance(geo, (bytes, bytearray)):
                d["geometry"] = wkb.loads(bytes(geo)).__geo_interface__

            # JSON-string fields → native types
            d = _rehydrate(d)
            items.append(pystac.Item.from_dict(d))

        return items

    def _search_prune(self, geom, start_datetime=None, end_datetime=None):
        """Prune warehouse files via Iceberg partition metadata (zero I/O)."""
        return self._info.file_paths(
            self._table, geom, start_datetime=start_datetime, end_datetime=end_datetime
        )

    def _cleared_env_s3(self):
        """Context manager: clear AWS cred env vars so rustac/DuckDB use unsigned requests.

        rustac and DuckDB read ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` from the
        environment rather than using the obstore store's auth.  When the store was created
        as anonymous (``skip_signature``) or the environment has no credentials, this
        context manager temporarily removes them and sets ``AWS_NO_SIGN_REQUEST=yes``.
        """
        import os
        from contextlib import contextmanager

        anonymous = not os.environ.get("AWS_ACCESS_KEY_ID")
        if not anonymous and self._store is not None and hasattr(self._store, "config"):
            anonymous = self._store.config.get("skip_signature") in (True, "true")

        @contextmanager
        def _ctx():
            if not anonymous:
                yield
                return
            saved = {
                "AWS_ACCESS_KEY_ID": os.environ.pop("AWS_ACCESS_KEY_ID", None),
                "AWS_SECRET_ACCESS_KEY": os.environ.pop("AWS_SECRET_ACCESS_KEY", None),
                "AWS_SESSION_TOKEN": os.environ.pop("AWS_SESSION_TOKEN", None),
            }
            os.environ["AWS_NO_SIGN_REQUEST"] = "yes"
            try:
                yield
            finally:
                os.environ.pop("AWS_NO_SIGN_REQUEST", None)
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v

        return _ctx()

    def stats(self) -> list[dict]:
        """Return per-partition row counts and file sizes from Iceberg metadata."""
        return self._info.stats(self._table)

    def unique_item_count(self) -> int:
        """Return the count of unique STAC items from the hash index."""
        default_hash_index_path = None
        if self._catalog is not None:
            warehouse = self._catalog.properties.get("warehouse", "")
            if warehouse:
                default_hash_index_path = warehouse.rstrip("/") + "_id_hashes.parquet"

        return self._info.unique_item_count(self._table, self._store, default_hash_index_path)

    def info(self) -> CatalogInfo:
        """Return the grid metadata and catalog statistics object."""
        return self._info

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
        """
        import uuid
        from concurrent.futures import ThreadPoolExecutor

        import os

        from .hash_index import (
            merge_hashes_from_parquets,
            read_hashes,
            write_hashes,
        )
        from .transform import (
            fan_out,
            group_by_partition,
            write_geoparquet_s3,
        )
        from earthcatalog.grids import build_partitioner
        from earthcatalog.pipelines.incremental import _fetch_item, _iter_inventory

        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            raise RuntimeError(
                "No AWS credentials found in environment. "
                "ingest() requires write access to S3. "
                "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use an IAM role."
            )

        if mode == "auto":
            try:
                n = sum(s["row_count"] for s in self._info.stats(self._table))
                mode = "delta" if n > 0 else "full"
            except Exception:
                mode = "full"

        is_delta = mode == "delta"

        from earthcatalog.config import GridConfig

        grid_cfg = GridConfig(
            type=self._info.grid_type,
            resolution=self._info.grid_resolution,
            boundaries_path=self._info.boundaries_path,
            id_field=self._info.id_field,
        )
        partitioner = build_partitioner(grid_cfg)

        warehouse_root = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///") if uri else "/tmp/earthcatalog.db"

        if self._store and self._catalog_key:
            self.download_catalog(local_db)

        if not is_delta:
            from pyiceberg.exceptions import NoSuchTableError

            try:
                self._catalog.drop_table(FULL_NAME)
            except NoSuchTableError:
                pass
            try:
                self._catalog.create_namespace(NAMESPACE)
            except Exception:
                pass
            self._table = get_or_create(self._catalog, grid_config=grid_cfg)

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

        if written_keys:
            full_paths = [f"{warehouse_root.rstrip('/')}/{k}" for k in written_keys]
            batch_sz = 2000
            for i in range(0, len(full_paths), batch_sz):
                self._table.add_files(full_paths[i : i + batch_sz])
            print(f"Registered {len(full_paths)} files in Iceberg catalog.")

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

        if self._store and self._catalog_key:
            self.upload_catalog(local_db)

        result = {
            "items_processed": total_items,
            "rows_written": total_rows,
            "files_registered": len(written_keys),
        }
        print(f"Done. {total_items} items -> {total_rows} rows in {len(written_keys)} files")
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
        """Ingest large inventories using a distributed Dask cluster."""
        from datetime import UTC
        from datetime import datetime as _dt

        from earthcatalog.config import GridConfig
        from earthcatalog.grids import build_partitioner
        from earthcatalog.pipelines.backfill import run_backfill

        import os
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            raise RuntimeError(
                "No AWS credentials found in environment. "
                "bulk_ingest() requires write access to S3. "
                "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use an IAM role."
            )

        warehouse_root = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///")

        grid_cfg = GridConfig(
            type=self._info.grid_type,
            resolution=self._info.grid_resolution,
            boundaries_path=self._info.boundaries_path,
            id_field=self._info.id_field,
        )
        partitioner = build_partitioner(grid_cfg)

        if staging_prefix is None:
            date_str = _dt.now(UTC).strftime("%Y%m%d")
            staging_prefix = f"bulk_ingest/{date_str}"

        delta = True
        if mode == "full":
            delta = False
        elif mode == "auto":
            try:
                n = sum(s["row_count"] for s in self._info.stats(self._table))
                delta = n > 0
            except Exception:
                delta = False

        if self._store and self._catalog_key:
            self.download_catalog(local_db)

        from . import store_config

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

    def download_catalog(self, local_path: str) -> None:
        """Download catalog.db from the backing store to *local_path*."""
        download_catalog(local_path, store=self._store)

    def upload_catalog(self, local_path: str) -> None:
        """Upload catalog.db from *local_path* to the backing store."""
        upload_catalog(local_path, store=self._store)

    def compact(
        self,
        threshold: int = 2,
        dry_run: bool = False,
    ) -> dict[str, int]:
        """Compact over-threshold partition buckets and rebuild the Iceberg catalog.

        Wraps :func:`earthcatalog.maintenance.compact.compact_warehouse` using this
        catalog's warehouse path and local catalog database.

        Parameters
        ----------
        threshold:
            Minimum number of part files in a bucket before it is compacted.
            Default: 2 (compact any bucket with more than one part file).
        dry_run:
            When ``True``, report what *would* be compacted but make no changes.

        Returns
        -------
        Summary dict with keys ``buckets_scanned``, ``buckets_compacted``,
        ``files_before``, ``files_after``.
        """
        from earthcatalog.maintenance.compact import compact_warehouse

        warehouse_path = self._catalog.properties.get("warehouse", "")
        uri = self._catalog.properties.get("uri", "")
        local_db = uri.removeprefix("sqlite:///")
        return compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=local_db,
            threshold=threshold,
            dry_run=dry_run,
        )

    def lock(self, owner: str, ttl_hours: int = 12):
        """Return an S3Lock that uses this EarthCatalog's store and key."""
        from .lock import S3Lock

        lock_key = getattr(self._catalog, "_lock_key", None) or ".lock"
        return S3Lock(owner=owner, ttl_hours=ttl_hours, store=self._store, key=lock_key)

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys that intersect *geom*."""
        return self._info.cells_for_geometry(geom)

    def cell_list_sql(self, geom) -> str:
        """Return a SQL fragment suitable for ``WHERE grid_partition IN (...)``."""
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
    def table(self):
        """Return the underlying PyIceberg Table (for advanced use)."""
        return self._table

    def _repr_html_(self) -> str:
        """Return an HTML representation for Jupyter notebooks.

        Single-column layout with metadata table and collapsible top partitions.
        Reads only Iceberg manifests — no Parquet data is scanned.
        """
        rows = [("Grid type", self._info.grid_type)]

        if self._info.grid_type == "h3":
            rows.append(("H3 resolution", str(self._info.grid_resolution)))
        else:
            rows.append(("Boundaries", self._info.boundaries_path or "N/A"))

        warehouse_path = self._catalog.properties.get("warehouse", "") if self._catalog else ""
        if warehouse_path:
            rows.append(("Warehouse", warehouse_path))

        hash_idx = self._table.properties.get("earthcatalog.hash_index_path")
        rows.append(("Hash index", "Available" if hash_idx else "Not available"))

        table_html = "<table style='border-collapse: collapse; width: 100%; margin: 0;'>"
        for label, value in rows:
            table_html += f"""
                <tr style='border-bottom: 1px solid currentColor;'>
                    <td style='padding: 6px 10px; border: none; width: 180px;'>{label}</td>
                    <td style='padding: 6px 10px; border: none;'><strong>{value}</strong></td>
                </tr>"""
        table_html += "</table>"

        stats = self._info.stats(self._table)
        bottom_html = ""
        if stats:
            total_files = self._info.total_files(self._table)
            total_rows = sum(s["row_count"] for s in stats)
            warehouse = self._catalog.properties.get("warehouse", "") if self._catalog else ""
            default_hi = warehouse.rstrip("/") + "_id_hashes.parquet" if warehouse else None
            unique = self._info.unique_item_count(self._table, self._store, default_hi)

            stat_rows = [
                ("Total files", f"{total_files:,}"),
                ("Total rows", f"{total_rows:,}"),
                ("Unique items", f"{unique:,}"),
                ("Partitions", f"{len(stats):,}"),
            ]
            stats_table = "<table style='border-collapse: collapse; width: 100%; font-size: 13px; margin: 0;'>"
            for label, value in stat_rows:
                stats_table += f"""
                    <tr style='border-bottom: 1px solid currentColor;'>
                        <td style='padding: 4px 6px; border: none; width: 180px;'>{label}</td>
                        <td style='padding: 4px 6px; border: none;'><strong>{value}</strong></td>
                    </tr>"""
            stats_table += "</table>"

            top_cells = self._info.top_cells(self._table, limit=3)
            top_html = ""
            if top_cells:
                top_rows = ""
                for cell in top_cells:
                    top_rows += f"""
                        <tr style='border-bottom: 1px solid currentColor;'>
                            <td style='padding: 4px 6px; border: none; width: 180px; font-family: monospace;'>{cell["grid_partition"][:12]}...</td>
                            <td style='padding: 4px 6px; border: none;'>{cell["row_count"]:,} rows</td>
                        </tr>"""
                top_html = f"""
                <details style='margin-top: 12px;'>
                    <summary style='font-weight: 600; cursor: pointer;'>Top partitions</summary>
                    <table style='border-collapse: collapse; width: 100%; font-size: 13px; margin: 8px 0 0 0;'>{top_rows}</table>
                </details>"""

            bottom_html = f"""
            <div style='font-weight: 600; margin-top: 12px;'>Statistics</div>
            {stats_table}
            {top_html}"""

        return f"""
        <div         style='border: 1px solid currentColor; padding: 15px; border-radius: 5px; font-family: var(--jp-code-font-family, monospace); opacity: 0.9; text-align: left; max-width: 800px;'>
            <div style='font-size: 16px; font-weight: 600; margin-bottom: 12px; display: flex; align-items: center; gap: 8px;'>
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10"/>
                    <path d="M2 12h20M12 2c-3.3 2-5.5 5.5-5.5 10s2.2 8 5.5 10c3.3-2 5.5-5.5 5.5-10S15.3 4 12 2z"/>
                </svg>
                <span>EarthCatalog</span>
            </div>
            {table_html}
            {bottom_html}
        </div>"""

    def __repr__(self) -> str:
        if self._info.grid_type == "h3":
            return f"EarthCatalog(grid_type='h3', resolution={self._info.grid_resolution})"
        return (
            f"EarthCatalog(grid_type='geojson', "
            f"boundaries_path={self._info.boundaries_path!r}, "
            f"id_field={self._info.id_field!r})"
        )
