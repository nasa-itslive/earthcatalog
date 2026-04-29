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


def open(db_path: str, warehouse_path: str) -> SqlCatalog:
    """
    Open (or create) a SQLite-backed Iceberg catalog.

    When ``warehouse_path`` is an ``s3://`` URI, PyIceberg uses PyArrow's S3
    filesystem to write Iceberg metadata files.  PyArrow auto-resolves the
    bucket region via a HEAD request unless the region is supplied explicitly.
    On non-EC2 machines (or with restricted IAM policies) that HEAD request
    fails with ``Not a valid bucket name: ''``.

    Region resolution order:
    1. ``AWS_DEFAULT_REGION`` environment variable
    2. ``AWS_REGION`` environment variable
    3. Hard-coded fallback ``us-west-2`` (ITS_LIVE bucket location)
    """
    import os

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"

    props: dict = {
        "uri": f"sqlite:///{db_path}",
        "warehouse": warehouse_path,
    }

    # Only inject S3 properties when the warehouse is on S3.
    if warehouse_path.startswith("s3://"):
        props["s3.region"] = region
        key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        session_token = os.environ.get("AWS_SESSION_TOKEN", "")
        if key_id and secret:
            props["s3.access-key-id"] = key_id
            props["s3.secret-access-key"] = secret
            if session_token:
                props["s3.session-token"] = session_token
        else:
            # Public bucket — anonymous access
            props["s3.anonymous"] = "true"

    return SqlCatalog(NAMESPACE, **props)


# ---------------------------------------------------------------------------
# SQLite-in-S3 catalog lifecycle
# ---------------------------------------------------------------------------


def download_catalog(local_path: str) -> None:
    """
    Pull catalog.db from the configured store to local_path before a job starts.
    If the key does not exist (first run), does nothing — a fresh catalog
    will be created by open_catalog / get_or_create_table.
    """
    store = store_config.get_store()
    key = store_config.get_catalog_key()
    try:
        result = obstore.get(store, key)
        Path(local_path).write_bytes(bytes(result.bytes()))
        print(f"Catalog downloaded: {key} → {local_path}")
    except FileNotFoundError:
        print(f"No existing catalog at '{key}' — will create fresh.")


def upload_catalog(local_path: str) -> None:
    """
    Push the updated catalog.db back to the configured store after all writes.
    The caller must hold the S3Lock before calling this.
    """
    store = store_config.get_store()
    key = store_config.get_catalog_key()
    obstore.put(store, key, Path(local_path).read_bytes())
    print(f"Catalog uploaded: {local_path} → {key}")


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
