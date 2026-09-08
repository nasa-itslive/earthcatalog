"""
EarthCatalog — simplified facade for querying spatially-partitioned STAC catalogs.

Provides a clean API that encapsulates PyIceberg catalog, table, and grid metadata
discovery into a single object.
"""

from __future__ import annotations

from collections import defaultdict
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

    from .ingest_config import IngestConfig

from . import store_config
from .schema import (
    _HIVE_RE,
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
    layout_of,
    partition_year,
)

HIVE_RE = _HIVE_RE


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


def _build_stats_cache(table) -> list[dict]:
    """Aggregate per-(partition, year) stats from Iceberg manifests. No Parquet I/O."""
    time_bin = table.properties.get(PROP_TIME_BIN, "year")
    agg: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0, 0])
    for task in table.scan().plan_files():
        f = task.file
        key = (f.partition[0], partition_year(time_bin, f.partition[1]))
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
    """Grid metadata read from Iceberg table properties."""

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
        time_bin = table.properties.get(PROP_TIME_BIN, "year")

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

    def unique_item_count(self, table, store, default_index_path: str | None = None) -> int:
        """Number of active (non-deleted) items in the unified index."""
        from obstore.store import LocalStore

        from earthcatalog.index import Index, resolve_index_path

        index_path = resolve_index_path(table, default_index_path or "")
        if not index_path:
            return 0

        try:
            if index_path.startswith("s3://"):
                if not store:
                    return 0
                _, _, rest = index_path.partition("s3://")
                obstore_key = rest.split("/", 1)[1] if "/" in rest else ""
                if not obstore_key:
                    return 0
                return Index(store, obstore_key).count_active()

            p = Path(index_path)
            # Parts layout: data under ``{base}/``; legacy: ``{base}.parquet``.
            if not p.exists() and not p.with_suffix("").is_dir():
                return 0
            return Index(LocalStore(str(p.parent)), p.name).count_active()
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
        # index property alone until migrate_indices() stamps it.
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
        catalog: SqlCatalog,
        table: Table,
        info: CatalogInfo,
        store: ObjectStore | None = None,
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

        Performance
        -----------
        For fastest results use :meth:`duck_search` with ``format="native"``
        (DuckDB parallel I/O, ~2× faster across all query types).
        ``search()`` and ``search_to_arrow()`` use rustac (sequential per-file)
        and have comparable speed.  See :doc:`/operations/search_performance`
        for detailed benchmarks.

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

    def search_uris(self, **kwargs):
        """Return asset URIs as a DataFrame with ``(id, uri)`` columns.

        Accepts the same kwargs as :meth:`search` (``intersects``, ``bbox``,
        ``datetime``, ``filter``, ``max_items``, etc.).

        Uses ``search_files()`` + DuckDB internally, reading **only** the
        ``id`` and ``assets`` columns from S3 — fastest way to get download
        URLs for thousands of items.  Returns a ``pandas.DataFrame``.

        Examples::

            import cql2
            df = catalog.search_uris(
                intersects={"type": "Point", "coordinates": [-45, 70]},
                datetime="2020-01-01/2020-12-31",
                filter=cql2.parse_text("percent_valid_pixels >= 80").to_json(),
                max_items=100,
            )
            # df has columns: id, uri
            for _, row in df.iterrows():
                print(row.id, row.uri)
        """
        import json

        import duckdb
        from shapely.geometry import shape

        from .search import _extract_datetime_range, build_query

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
            self._table,
            geom,
            start_datetime=start_dt,
            end_datetime=end_dt,
        )
        if not paths:
            import pandas as pd

            return pd.DataFrame({"id": [], "uri": []})

        # --- build SQL (read only id + assets) ---
        max_items = kwargs.get("max_items")
        sql = build_query(paths, geom, start_dt, end_dt, kwargs.get("filter"), select="id, assets")

        # --- execute (Arrow → list is faster than pandas iterrows) ---
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("SET s3_access_key_id='';")
        con.execute("SET s3_secret_access_key='';")
        con.execute("SET s3_session_token='';")
        table = con.execute(sql).to_arrow_table()
        if max_items is not None and table.num_rows > max_items:
            table = table.slice(0, max_items)

        # --- extract data URIs from JSON assets ---

        ids = table.column("id").to_pylist()
        assets_list = table.column("assets").to_pylist()
        uris = []
        for a in assets_list:
            href = None
            if a:
                try:
                    href = json.loads(a).get("data", {}).get("href")
                except (json.JSONDecodeError, AttributeError):
                    pass
            uris.append(href)

        import pandas as pd

        return pd.DataFrame({"id": ids, "uri": uris})

    def duck_search(self, **kwargs):
        """Search using DuckDB, returning results as a ``pandas.DataFrame``.

        Accepts the same kwargs as :meth:`search` (``intersects``, ``bbox``,
        ``datetime``, ``filter``, ``max_items``, etc.).

        DuckDB reads Parquet files in parallel internally, making this
        **~2× faster** than :meth:`search` across all query types.
        Returns a DataFrame with flat columns — no pystac conversion
        overhead.  For pystac Items use :meth:`search` (lazy iteration).

        Examples::

            df = catalog.duck_search(
                intersects={"type": "Point", "coordinates": [-45, 70]},
                datetime="1980-01-01/2015-12-31",
                max_items=100,
            )
            # df is a pandas.DataFrame
            print(df.columns.tolist())
        """
        import duckdb
        from shapely.geometry import shape

        from .search import _extract_datetime_range, build_query

        geom = None
        if "intersects" in kwargs:
            geom = shape(kwargs["intersects"])
        elif "bbox" in kwargs:
            from shapely.geometry import box

            b = kwargs["bbox"]
            geom = box(b[0], b[1], b[2], b[3])

        start_dt, end_dt = _extract_datetime_range(**kwargs)
        paths = self._info.file_paths(
            self._table, geom, start_datetime=start_dt, end_datetime=end_dt
        )
        if not paths:
            import pandas as pd

            return pd.DataFrame()

        max_items = kwargs.get("max_items")
        # LIMIT omitted — triggers 7× slower plan for multi-file reads
        sql = build_query(paths, geom, start_dt, end_dt, kwargs.get("filter"))

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("SET s3_access_key_id='';")
        con.execute("SET s3_secret_access_key='';")
        con.execute("SET s3_session_token='';")
        df = con.execute(sql).fetchdf()
        if max_items is not None and len(df) > max_items:
            df = df.head(max_items)
        return df

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
        """Return the count of active (non-deleted) items from the unified index."""
        default_index_path = None
        if self._catalog is not None:
            warehouse = self._catalog.properties.get("warehouse", "")
            if warehouse:
                default_index_path = warehouse.rstrip("/") + "_index.parquet"

        return self._info.unique_item_count(self._table, self._store, default_index_path)

    def info(self) -> CatalogInfo:
        """Return the grid metadata and catalog statistics object."""
        return self._info

    def ingest_inventory(
        self,
        inventory_path: str,
        *,
        mode: str = "auto",
        config: IngestConfig | None = None,
    ) -> dict:
        """Ingest an inventory using a (optionally distributed) Dask cluster.

        Delegates to :class:`earthcatalog.pipeline.IngestPipeline`.  *config*
        (a :class:`earthcatalog.ingest_config.IngestConfig`) holds the tuning
        knobs (chunk size, compact rows, stage, resume flags, create_client).

        There is one ingest operation: *mode* only controls table handling —
        ``"full"`` drops and rebuilds the Iceberg table, ``"delta"`` appends,
        ``"auto"`` appends iff the table has rows.  Input scope (complete
        inventory vs. newer snapshot vs. precomputed delta parquet) is simply
        which *inventory_path* you pass; the unified index dedups source
        keys, so every run is resumable and idempotent.

        With ``stage="ndjson"`` (default) items are staged to per-(cell,
        year) NDJSON before a memory-bounded compaction to GeoParquet;
        ``skip_fetch`` resumes from the staged NDJSON.  Distributed runs
        shard the inventory by part file where possible — workers stream
        their own files and only the head node commits.

        Returns the run summary dict (``{"items": …, "rows": …}``).
        """
        from .pipeline import IngestPipeline

        return IngestPipeline(self, config).run(inventory_path, mode=mode)

    def bulk_ingest(
        self,
        inventory_path: str,
        *,
        mode: str = "auto",
        config: IngestConfig | None = None,
    ) -> dict:
        """Deprecated alias for :meth:`ingest_inventory`."""
        import warnings

        warnings.warn(
            "EarthCatalog.bulk_ingest() is deprecated; use ingest_inventory()",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.ingest_inventory(inventory_path, mode=mode, config=config)

    def download_catalog(self, local_path: str) -> None:
        """Download catalog.db from the backing store to *local_path*."""
        download_catalog(local_path, store=self._store)

    def upload_catalog(self, local_path: str) -> None:
        """Upload catalog.db from *local_path* to the backing store."""
        upload_catalog(local_path, store=self._store)

    def garbage_collect(
        self,
        inventory_path: str,
        *,
        dry_run: bool = False,
    ) -> dict:
        """Remove orphaned STAC items whose source objects left the S3 Inventory.

        Wraps :func:`earthcatalog.gc.run_garbage_collection`
        using this catalog's store, unified index, and warehouse path.

        Detects deletions via a Bloom filter of the current inventory keys,
        then rewrites only the affected GeoParquet files.  See the v2 GC
        plan (``docs/delete_plan_v2.md``) for details.

        After any files are rewritten the Iceberg catalog is rebuilt from the
        current warehouse state so that subsequent searches reflect the changes.

        Parameters
        ----------
        inventory_path:
            Path or ``s3://`` URI to the current S3 Inventory.
        dry_run:
            When ``True``, detect and report orphans but make no changes.

        Returns
        -------
        Summary dict: ``candidates``, ``confirmed``, ``orphaned``,
        ``files_rewritten``, ``rows_removed``, ``partitions_affected``.
        """
        import os

        from earthcatalog.gc import run_garbage_collection
        from earthcatalog.index import Index

        warehouse_root = self._catalog.properties.get("warehouse", "")

        # Derive the key prefix *within the store* for _list_partition_files.
        # self._store is a bucket-level S3Store, so keys inside it look like
        # "test-space/stac/catalog/warehouse/grid_partition=.../year=.../...".
        # Stripping "s3://bucket/" from warehouse_root gives us that prefix.
        if warehouse_root.startswith("s3://"):
            _, _, key_path = warehouse_root.removeprefix("s3://").partition("/")
            warehouse_prefix = key_path.rstrip("/") + "/"
        else:
            warehouse_prefix = warehouse_root.rstrip("/") + "/"

        def _strip(uri: str) -> str:
            return uri.removeprefix("s3://").split("/", 1)[1] if uri.startswith("s3://") else uri

        # Unified index — same path the ingest pipeline writes to.
        from earthcatalog.index import resolve_index_path

        index_key = _strip(
            resolve_index_path(self._table, f"{warehouse_root.rstrip('/')}_index.parquet")
        )
        assert self._store is not None

        result = run_garbage_collection(
            inventory_path=inventory_path,
            store=self._store,
            index=Index(self._store, index_key),
            warehouse_prefix=warehouse_prefix,
            dry_run=dry_run,
            layout=layout_of(self._table.properties),
        )

        # After files have been physically rewritten the Iceberg table still
        # points to the now-deleted part_*.parquet paths and has no knowledge
        # of the new gc_*.parquet files.  Rebuild the table so searches work.
        if not dry_run and result.get("files_rewritten", 0) > 0:
            from earthcatalog.rebuild import rebuild_iceberg_from_warehouse

            uri = self._catalog.properties.get("uri", "")
            local_db = uri.removeprefix("sqlite:///") if uri else None

            if local_db and os.path.exists(local_db) and self._store and self._catalog_key:
                n = rebuild_iceberg_from_warehouse(
                    catalog_path=local_db,
                    warehouse_root=warehouse_root,
                    warehouse_store=self._store,
                    upload=False,  # we upload manually below with our store + key
                )
                obstore.put(
                    self._store,
                    self._catalog_key,
                    Path(local_db).read_bytes(),
                )
                print(f"Iceberg catalog rebuilt and uploaded ({n:,} files).")
            else:
                print(
                    "WARN: could not rebuild Iceberg catalog after GC — "
                    "local_db or catalog_key unavailable."
                )

        if not dry_run and self._store:
            # Refresh the catalog-stats snapshot at the durable commit
            # moment (GC removed confirmed items from warehouse and index).
            from . import stats as stats_mod

            def _apply(s):
                return stats_mod.apply_gc(
                    s,
                    confirmed=result.get("confirmed", 0),
                    rows_removed=result.get("rows_removed", 0),
                )

            stats_mod.refresh_after(
                self._store,
                stats_mod.stats_key_for(warehouse_root),
                self._table,
                Index(self._store, index_key),
                stats_mod.index_locations(self._table, self._store, warehouse_root),
                apply=_apply,
            )

        return result

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

        from earthcatalog.index import resolve_index_path

        index_path = (
            resolve_index_path(self._table, f"{warehouse_path.rstrip('/')}_index.parquet")
            if warehouse_path
            else ""
        )
        rows.append(("Unique index", "Available" if index_path else "Not available"))

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
            default_hi = warehouse.rstrip("/") + "_index.parquet" if warehouse else None
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
