"""EarthCatalog — the user-facing facade.

Thin composition layer: search delegation, ingest/GC orchestration,
lifecycle wrappers.  No inline business logic — the implementations live
in ``search.py`` (queries), ``pipeline.py``/``ingest.py`` (ingest),
``gc.py`` (garbage collection) and ``stats.py`` (rendering/statistics).
"""

from __future__ import annotations

from datetime import datetime  # noqa: F401  (annotation-only)
from typing import TYPE_CHECKING

from obstore.store import ObjectStore
from pyiceberg.catalog.sql import SqlCatalog

if TYPE_CHECKING:
    from pyiceberg.table import Table

    from .catalog import CatalogInfo
    from .ingest_config import IngestConfig


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

    # --- read-only component access (the facade composes, modules cooperate) ---

    @property
    def catalog(self) -> SqlCatalog:
        """The underlying PyIceberg catalog."""
        return self._catalog

    @property
    def table(self) -> Table:
        """The underlying Iceberg ``stac_items`` table."""
        return self._table

    @table.setter
    def table(self, value: Table) -> None:
        # Full-mode ingest drops and recreates the table; the pipeline
        # re-points the facade at the replacement.
        self._table = value

    @property
    def info(self) -> CatalogInfo:
        """Grid metadata + pruning for this catalog."""
        return self._info

    @property
    def store(self) -> ObjectStore | None:
        """The backing object store (bucket-level for S3 warehouses)."""
        return self._store

    @property
    def catalog_key(self) -> str | None:
        """Key of earthcatalog.db within :attr:`store`."""
        return self._catalog_key

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
        For fastest results use :func:`earthcatalog.search.duck_search`
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
        from .search import EarthCatalogItemSearch, _FileSearchEngine, cleared_env_s3

        engine = _FileSearchEngine(prune_fn=self._search_prune)
        store = self._store
        return EarthCatalogItemSearch(
            params=kwargs,
            engine=engine,
            table=self._table,
            # The search layer expects a zero-arg callable returning a
            # context manager (entered lazily, per result page).
            anonymous_ctx=lambda: cleared_env_s3(store),
        )

    def search_to_arrow(self, **kwargs):
        """Search across the catalog, returning a PyArrow table."""
        from .search import _FileSearchEngine, cleared_env_s3

        engine = _FileSearchEngine(prune_fn=self._search_prune)
        with cleared_env_s3(self._store):
            return engine.search_to_arrow(**kwargs)

    def _search_prune(self, geom, start_datetime=None, end_datetime=None):
        """Prune warehouse files via Iceberg partition metadata (zero I/O)."""
        return self._info.file_paths(
            self._table, geom, start_datetime=start_datetime, end_datetime=end_datetime
        )

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

    def download_catalog(self, local_path: str) -> None:
        """Download catalog.db from the backing store to *local_path*."""
        from .catalog import download_catalog as _download_catalog

        _download_catalog(local_path, store=self._store)

    def upload_catalog(self, local_path: str) -> None:
        """Upload catalog.db from *local_path* to the backing store."""
        from .catalog import upload_catalog as _upload_catalog

        _upload_catalog(local_path, store=self._store)

    def garbage_collect(
        self,
        inventory_path: str,
        *,
        dry_run: bool = False,
    ) -> dict:
        """Remove orphaned STAC items whose source objects left the S3 Inventory.

        Thin wrapper over :func:`earthcatalog.gc.garbage_collect_for_catalog`
        using this catalog's store, unified index, and warehouse path: Bloom
        detection, targeted GeoParquet rewrites, an Iceberg rebuild so
        searches reflect the changes, and a stats-snapshot refresh.

        Parameters
        ----------
        inventory_path:
            Path or ``s3://`` URI to the current S3 Inventory.
        dry_run:
            When ``True``, detect and report orphans but make no changes.

        Returns
        -------
        Summary dict: ``candidates``, ``confirmed``, ``orphaned``,
        ``files_rewritten``, ``rows_removed``, ``partitions_affected``,
        ``copies_outside_index``, ``residual_copies``.
        """
        from .gc import garbage_collect_for_catalog

        return garbage_collect_for_catalog(
            catalog=self._catalog,
            table=self._table,
            store=self._store,
            catalog_key=self._catalog_key,
            inventory_path=inventory_path,
            dry_run=dry_run,
        )

    @property
    def grid_type(self) -> str:
        """Return the grid partitioning system type."""
        return self._info.grid_type

    @property
    def grid_resolution(self) -> float | None:
        """Return the grid resolution (None for grids without one)."""
        return self._info.grid_resolution

    def _repr_html_(self) -> str:
        """Jupyter HTML representation — rendering lives in
        :func:`earthcatalog.stats.render_catalog_html`."""
        from .stats import render_catalog_html

        return render_catalog_html(
            self._info,
            self._table,
            self._store,
            self._catalog.properties if self._catalog is not None else {},
        )

    def __repr__(self) -> str:
        parts = [f"grid_type={self._info.grid_type!r}"]
        if self._info.grid_resolution is not None:
            parts.append(f"resolution={self._info.grid_resolution}")
        if self._info.time_bin != "year":
            parts.append(f"time_bin={self._info.time_bin!r}")
        return f"EarthCatalog({', '.join(parts)})"
