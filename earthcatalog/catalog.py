"""
Catalog lifecycle and grid metadata.

* :class:`CatalogInfo` — grid metadata read from Iceberg table properties,
  owning the read-side partitioner (spatial prune + temporal bin).
* ``open()`` / ``get_or_create()`` / ``download_catalog`` / ``upload_catalog``
  — the catalog db lifecycle (open, create, persist).
* :class:`EarthCatalog` — re-exported from :mod:`earthcatalog.facade`, where
  the user-facing facade lives.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import obstore
from obstore.store import ObjectStore
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

if TYPE_CHECKING:
    from pyiceberg.table import Table

from . import store_config
from .facade import EarthCatalog  # noqa: F401  (re-export: facade lives here)
from .schema import (
    FULL_NAME,
    ICEBERG_SCHEMA,
    NAMESPACE,
    PARTITION_SPEC,  # noqa: F401  (re-export: default year spec)
    PROP_GRID_BOUNDARIES_PATH,
    PROP_GRID_ID_FIELD,
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
    PROP_HASH_INDEX_PATH,  # noqa: F401  (re-export: consumers import from catalog)
    PROP_INDEX_PATH,
    PROP_TIME_BIN,
    TABLE_NAME,  # noqa: F401  (re-export: consumers import from catalog)
    build_partition_spec,
    partition_year,
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


# ---------------------------------------------------------------------------
# CatalogInfo — grid metadata from Iceberg table properties
# ---------------------------------------------------------------------------


@dataclass
class CatalogInfo:
    """Grid metadata read from Iceberg table properties.

    The query-side partitioner is built lazily from these properties (same
    factory the ingest side uses) and cached — one partitioner per catalog,
    so every search prunes with exactly the grid the data was written with.
    """

    grid_type: str
    grid_resolution: float | None
    boundaries_path: str | None
    id_field: str | None
    time_bin: str = "year"
    _cached_stats: list[dict] | None = field(default=None, repr=False)
    _cached_top_cells: list[dict] | None = field(default=None, repr=False)
    _partitioner: object | None = field(default=None, repr=False, compare=False)

    def partitioner(self):
        """The cached read-side partitioner (spatial keys + temporal bin)."""
        if self._partitioner is None:
            from .config import GridConfig
            from .grids import build_partitioner

            self._partitioner = build_partitioner(
                GridConfig(
                    type=self.grid_type,
                    resolution=self.grid_resolution,
                    boundaries_path=self.boundaries_path,
                    id_field=self.id_field,
                    time_bin=self.time_bin,
                )
            )
        return self._partitioner

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys that intersect *geom*."""
        from shapely import wkb

        return self.partitioner().get_intersecting_keys(wkb.dumps(geom))

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
        year_lookback: int = 2,
    ) -> list[str]:
        """Return Parquet file paths for partitions overlapping *geom* and the
        temporal range.

        Overlap semantics: items carry a temporal extent
        (``start_datetime``..``end_datetime`` — velocity pairs span ~500
        days and routinely cross year boundaries), so a partition is
        relevant when the item's *start* is not after the query end and its
        *end* is not before the query start.  The partition-year window is
        widened by *year_lookback* on the start side to reach midpoints that
        fall before the query interval.
        """
        from pyiceberg.expressions import (
            And,
            GreaterThanOrEqual,
            In,
            LessThanOrEqual,
        )

        cells = self.cells_for_geometry(geom)
        if not cells:
            return []

        # NB: pyiceberg 0.11's inline stubs describe the *bound* predicate
        # constructors, not these unbound ones (runtime accepts a plain
        # string term + python values) — hence the narrow ignores here.
        expr = In("grid_partition", cells)  # type: ignore[misc,arg-type,call-arg]
        q_end = _parse_dt(end_datetime) if end_datetime is not None else None
        q_start = _parse_dt(start_datetime) if start_datetime is not None else None
        if q_end is not None:
            # Item starts before the query ends.
            expr = And(expr, LessThanOrEqual("start_datetime", q_end))  # type: ignore[misc,arg-type,call-arg,assignment]
        if q_start is not None:
            # Item ends after the query starts.
            expr = And(expr, GreaterThanOrEqual("end_datetime", q_start))  # type: ignore[misc,arg-type,call-arg,assignment]

        start_year = q_start.year - year_lookback if q_start is not None else None
        end_year = q_end.year + 1 if q_end is not None else None
        time_bin = self.time_bin

        paths = []
        for task in table.scan(row_filter=expr).plan_files():
            year = partition_year(time_bin, task.file.partition[1])
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            paths.append(task.file.file_path)

        return paths

    def _ensure_stats(self, table) -> list[dict]:
        if self._cached_stats is None:
            from .stats import aggregate_top_cells, build_stats_cache

            self._cached_stats = build_stats_cache(table)
            self._cached_top_cells = aggregate_top_cells(self._cached_stats)
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

    def unique_item_count(self, table, store, default_index_path: str | None = None) -> int:
        """Number of active (non-deleted) items in the unified index."""
        from .index import count_active_items, resolve_index_path

        index_path = resolve_index_path(table, default_index_path or "")
        if not index_path:
            return 0
        try:
            return count_active_items(index_path, store)
        except Exception:
            return 0

    def __repr__(self) -> str:
        parts = [f"grid_type={self.grid_type!r}"]
        if self.grid_resolution is not None:
            parts.append(f"resolution={self.grid_resolution}")
        if self.time_bin != "year":
            parts.append(f"time_bin={self.time_bin!r}")
        if self.boundaries_path is not None:
            parts.append(f"boundaries_path={self.boundaries_path!r}")
        return f"CatalogInfo({', '.join(parts)})"


def _catalog_info(table) -> CatalogInfo:
    props = table.properties
    grid_type = props.get(PROP_GRID_TYPE, "h3")
    raw_res = props.get(PROP_GRID_RESOLUTION)
    # Integral resolutions stay int (h3/s2 levels); fractional allowed for
    # degree-based grids (lat_lon).
    if raw_res is None:
        grid_resolution: float | None = None
    else:
        parsed = float(raw_res)
        grid_resolution = int(parsed) if parsed.is_integer() else parsed
    return CatalogInfo(
        grid_type=grid_type,
        grid_resolution=grid_resolution,
        boundaries_path=props.get(PROP_GRID_BOUNDARIES_PATH),
        id_field=props.get(PROP_GRID_ID_FIELD),
        time_bin=props.get(PROP_TIME_BIN, "year"),
    )


# ---------------------------------------------------------------------------
# SQLite-in-S3 catalog lifecycle
# ---------------------------------------------------------------------------


def open_sqlite(db_path: str, warehouse_path: str) -> SqlCatalog:
    """Open a PyIceberg SqlCatalog over a local SQLite db for *warehouse_path*.

    Part of the catalog lifecycle surface (used by the CLI, run.py, rebuild
    and the GC entry script).  Credentials come from
    :func:`earthcatalog.inventory.sql_catalog_props` — this is the
    *writer* configuration; the read-mostly catalog built by :func:`open`
    configures anonymous/region properties itself.
    """
    from .inventory import sql_catalog_props

    return SqlCatalog(NAMESPACE, **sql_catalog_props(db_path, warehouse_path))


# Historical private name — importers across cli/run/tests still use it.
_open_sqlite = open_sqlite


def download_catalog(
    local_path: str,
    store: ObjectStore | None = None,
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
    store: ObjectStore | None = None,
    catalog_key: str | None = None,
) -> None:
    """Push the updated catalog.db to *store* after all writes."""
    if store is None or catalog_key is None:
        store = store_config.get_store()
        catalog_key = store_config.get_catalog_key()
    obstore.put(store, catalog_key, Path(local_path).read_bytes())
    print(f"Catalog uploaded: {local_path} -> {catalog_key}")


def get_or_create(catalog: SqlCatalog, grid_config=None) -> Table:
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
        props[PROP_TIME_BIN] = grid_config.time_bin

    warehouse = catalog.properties.get("warehouse", "")
    if warehouse:
        props[PROP_INDEX_PATH] = f"{warehouse.rstrip('/')}_index.parquet"

    time_bin = grid_config.time_bin if grid_config is not None else "year"

    try:
        table = catalog.load_table(FULL_NAME)
        missing = {k: v for k, v in props.items() if k not in table.properties}
        # A legacy warehouse carries earthcatalog.hash_index_path; leave the
        # index property alone — it was stamped when the warehouse was
        # migrated to the unified index.
        if table.properties.get(PROP_HASH_INDEX_PATH):
            missing.pop(PROP_INDEX_PATH, None)
        if missing:
            with table.transaction() as tx:
                tx.set_properties(**missing)  # type: ignore[arg-type]
        return table
    except NoSuchTableError:
        return catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=build_partition_spec(time_bin),
            properties=props,
        )


# ---------------------------------------------------------------------------
# Catalog open / ingest
# ---------------------------------------------------------------------------


def open(
    store: ObjectStore,
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
        - ``warehouse_index.parquet`` (unified index)
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

    sql_catalog = SqlCatalog(NAMESPACE, **props)
    table = get_or_create(sql_catalog)
    return EarthCatalog(
        catalog=sql_catalog,
        table=table,
        info=_catalog_info(table),
        store=store,
        catalog_key=catalog_key,
    )
