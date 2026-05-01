"""
PyIceberg catalog management using a local SQLite backend.

The catalog.db file lives on disk (local path or downloaded from S3 before
a job starts).  All table writes go through PyIceberg so every Parquet file
is properly registered, schema-validated, and partition-tracked.
"""

from pathlib import Path

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

from . import store_config

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
#
# rustac emits the full STAC item as stac-geoparquet.  write_geoparquet()
# post-processes the file: assets/links structs → JSON strings, null-typed
# columns (collection) → dropped.  This schema must match that normalized form.
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
#
# Each registered Parquet file must contain exactly one value for
# grid_partition (IdentityTransform) and span at most one calendar year
# (YearTransform).  This is enforced by writing one file per
# (grid_partition, year) group via group_by_partition() in transform.py.
#
# With this spec Iceberg maintains a file manifest keyed by (cell, year).
# A spatial query that first resolves the query geometry to candidate cells
# and then filters  WHERE grid_partition IN (<cells>)  will have only the
# files for those cells opened — O(query_cells / total_cells) of the data.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="grid_partition"),
    PartitionField(source_id=4, field_id=101, transform=YearTransform(), name="year"),
)


def open(
    db_path: str | None = None,
    warehouse_path: str | None = None,
    store: object | None = None,
    base: str | None = None,
    anonymous: bool | None = None,
) -> SqlCatalog | object:
    """
    Open (or create) an EarthCatalog.

    **New simplified API (returns EarthCatalog):**

        from earthcatalog.core import catalog
        from obstore.store import S3Store

        store = S3Store(bucket='its-live-data', region='us-west-2')
        ec = catalog.open(store=store, base='s3://its-live-data/test-space/stac/catalog')

    The *base* path should contain:
        - earthcatalog.db           (SQLite catalog)
        - warehouse/                (GeoParquet files)
        - warehouse_id_hashes.parquet (optional hash index)

    **Legacy API (returns SqlCatalog for backward compatibility):**

        c = catalog.open(db_path='/tmp/catalog.db', warehouse_path='/tmp/warehouse')

    When ``warehouse_path`` or ``base`` is an ``s3://`` URI, PyIceberg uses
    PyArrow's S3 filesystem to write Iceberg metadata files.  PyArrow auto-resolves
    the bucket region via a HEAD request unless the region is supplied explicitly.
    On non-EC2 machines (or with restricted IAM policies) that HEAD request
    fails with ``Not a valid bucket name: ''``.

    Region resolution order:
    1. ``AWS_DEFAULT_REGION`` environment variable
    2. ``AWS_REGION`` environment variable
    3. Hard-coded fallback ``us-west-2`` (ITS_LIVE bucket location)

    Args:
        db_path: Local path for SQLite catalog (legacy API)
        warehouse_path: S3 or local path for Parquet warehouse (legacy API)
        store: obstore Store instance (new API)
        base: S3 or local base path containing earthcatalog.db and warehouse/ (new API)
        anonymous: Force anonymous S3 access (use True for public buckets).
                  Auto-detected for S3Store with skip_signature=True when None.

    Returns:
        EarthCatalog for new API, SqlCatalog for legacy API
    """
    import os
    import tempfile
    import uuid
    from pathlib import Path

    from .earthcatalog import EarthCatalog

    # Detect which API is being used
    using_new_api = store is not None and base is not None
    using_legacy_api = db_path is not None and warehouse_path is not None

    if using_new_api and using_legacy_api:
        raise ValueError(
            "Cannot mix new API (store + base) with legacy API (db_path + warehouse_path). "
            "Use either: catalog.open(store=S3Store(...), base='s3://.../catalog') "
            "OR: catalog.open(db_path='...', warehouse_path='...')"
        )

    if not using_new_api and not using_legacy_api:
        raise ValueError(
            "Must provide either (store + base) OR (db_path + warehouse_path). "
            "For new API: catalog.open(store=S3Store(...), base='s3://.../catalog') "
            "For legacy API: catalog.open(db_path='...', warehouse_path='...')"
        )

    # Auto-detect anonymous mode for S3Store with skip_signature=True
    if anonymous is None and using_new_api:
        # Check if store config has skip_signature (indicates public bucket intent)
        if hasattr(store, "config"):
            skip_sig = store.config.get("skip_signature")
            if skip_sig in (True, "true"):
                anonymous = True

    if using_legacy_api:
        # Legacy API - return SqlCatalog directly
        _db_path = db_path
        _warehouse_path = warehouse_path
    else:
        # New API - derive paths from base
        # base = s3://its-live-data/test-space/stac/catalog
        # catalog_db = base/earthcatalog.db
        # warehouse = base/warehouse
        _warehouse_path = f"{base}/warehouse"

        # Catalog key relative to store root
        # If base is s3://bucket/prefix/catalog, catalog key is prefix/catalog/earthcatalog.db
        if base.startswith("s3://"):
            # Remove s3:// prefix and split to get bucket + key
            rest = base[5:]
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"
        else:
            # Local path
            catalog_key = str(Path(base) / "earthcatalog.db")

        # Download catalog.db to temp location
        _db_path = str(Path(tempfile.gettempdir()) / f"earthcatalog_{uuid.uuid4().hex[:8]}.db")
        try:
            import obstore

            result = obstore.get(store, catalog_key)
            Path(_db_path).write_bytes(bytes(result.bytes()))
        except FileNotFoundError:
            # No existing catalog - will create fresh
            pass

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"

    props: dict = {
        "uri": f"sqlite:///{_db_path}",
        "warehouse": _warehouse_path,
    }

    # Only inject S3 properties when the warehouse is on S3.
    if _warehouse_path.startswith("s3://"):
        props["s3.region"] = region

        # Check if we should use anonymous access
        if anonymous:
            # Public bucket — force anonymous access
            # PyArrow requires endpoint URL for anonymous S3 access
            props["s3.anonymous"] = "true"
            props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"
        else:
            # Use credentials if available
            key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
            secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
            session_token = os.environ.get("AWS_SESSION_TOKEN", "")
            if key_id and secret:
                props["s3.access-key-id"] = key_id
                props["s3.secret-access-key"] = secret
                if session_token:
                    props["s3.session-token"] = session_token
            else:
                # No credentials — anonymous access
                props["s3.anonymous"] = "true"
                props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"

    sql_catalog = SqlCatalog(NAMESPACE, **props)

    # For new API, return EarthCatalog facade
    if using_new_api:
        table = get_or_create(sql_catalog)
        info_obj = info(table)
        return EarthCatalog(catalog=sql_catalog, table=table, info=info_obj, store=store)

    return sql_catalog


# ---------------------------------------------------------------------------
# SQLite-in-S3 catalog lifecycle
# ---------------------------------------------------------------------------


def download_catalog(
    local_path: str,
    store: object | None = None,
    catalog_key: str | None = None,
) -> None:
    """
    Pull catalog.db from *store* to *local_path* before a job starts.

    When *store* and *catalog_key* are ``None`` (default), falls back to the
    global :mod:`earthcatalog.core.store_config` — this path is deprecated.

    If the key does not exist (first run), does nothing — a fresh catalog
    will be created by open_catalog / get_or_create_table.
    """
    if store is None or catalog_key is None:
        store = store_config.get_store()
        catalog_key = store_config.get_catalog_key()
    try:
        result = obstore.get(store, catalog_key)
        Path(local_path).write_bytes(bytes(result.bytes()))
        print(f"Catalog downloaded: {catalog_key} → {local_path}")
    except FileNotFoundError:
        print(f"No existing catalog at '{catalog_key}' — will create fresh.")


def upload_catalog(
    local_path: str,
    store: object | None = None,
    catalog_key: str | None = None,
) -> None:
    """
    Push the updated catalog.db to *store* after all writes.

    When *store* and *catalog_key* are ``None`` (default), falls back to the
    global :mod:`earthcatalog.core.store_config` — this path is deprecated.

    The caller must hold the S3Lock before calling this.
    """
    if store is None or catalog_key is None:
        store = store_config.get_store()
        catalog_key = store_config.get_catalog_key()
    obstore.put(store, catalog_key, Path(local_path).read_bytes())
    print(f"Catalog uploaded: {local_path} → {catalog_key}")


def get_or_create(catalog: SqlCatalog, grid_config=None) -> object:
    """Return the stac_items table, creating it (and the namespace) if needed.

    Parameters
    ----------
    catalog:
        Open SqlCatalog instance.
    grid_config:
        Optional :class:`earthcatalog.config.GridConfig`.  When provided, grid
        metadata (type, resolution, boundaries_path, id_field) is stored as
        Iceberg table properties so that :class:`~earthcatalog.core.catalog_info.CatalogInfo`
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


def info(table) -> object:
    """Return a CatalogInfo for *table*.

    Shortcut for :func:`earthcatalog.core.catalog_info.catalog_info`.
    """
    from .catalog_info import catalog_info

    return catalog_info(table)


# Backward-compatible aliases.
open_catalog = open
get_or_create_table = get_or_create
