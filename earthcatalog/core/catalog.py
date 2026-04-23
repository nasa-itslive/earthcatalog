"""
PyIceberg catalog management using a local SQLite backend.

The catalog.db file lives on disk (local path or downloaded from S3 before
a job starts).  All table writes go through PyIceberg so every Parquet file
is properly registered, schema-validated, and partition-tracked.
"""

from pathlib import Path

import obstore
from pyiceberg.catalog.sql import SqlCatalog

from . import store_config
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

NAMESPACE = "earthcatalog"
TABLE_NAME = "stac_items"
FULL_NAME = f"{NAMESPACE}.{TABLE_NAME}"

# PyIceberg schema — mirrors earthcatalog/schema.py (PyArrow)
# Field IDs must be stable; never reorder or reuse them.
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "id",                   StringType(),      required=True),
    NestedField(2,  "grid_partition",        StringType(),      required=True),
    NestedField(3,  "geometry",              BinaryType(),      required=False),
    NestedField(4,  "datetime",              TimestamptzType(), required=False),
    NestedField(5,  "start_datetime",        TimestamptzType(), required=False),
    NestedField(6,  "mid_datetime",          TimestamptzType(), required=False),
    NestedField(7,  "end_datetime",          TimestamptzType(), required=False),
    NestedField(8,  "created",               TimestamptzType(), required=False),
    NestedField(9,  "updated",               TimestamptzType(), required=False),
    NestedField(10, "percent_valid_pixels",  IntegerType(),     required=False),
    NestedField(11, "date_dt",               IntegerType(),     required=False),
    NestedField(12, "latitude",              DoubleType(),      required=False),
    NestedField(13, "longitude",             DoubleType(),      required=False),
    NestedField(14, "platform",              StringType(),      required=False),
    NestedField(15, "version",               StringType(),      required=False),
    NestedField(16, "proj_code",             StringType(),      required=False),
    NestedField(17, "sat_orbit_state",       StringType(),      required=False),
    NestedField(18, "scene_1_id",            StringType(),      required=False),
    NestedField(19, "scene_2_id",            StringType(),      required=False),
    NestedField(20, "scene_1_frame",         StringType(),      required=False),
    NestedField(21, "scene_2_frame",         StringType(),      required=False),
    NestedField(22, "raw_stac",              StringType(),      required=False),
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
    PartitionField(source_id=4, field_id=101, transform=YearTransform(),     name="year"),
)


def open_catalog(db_path: str, warehouse_path: str) -> SqlCatalog:
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
    region = (
        os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-west-2"
    )

    props: dict = {
        "uri":       f"sqlite:///{db_path}",
        "warehouse": warehouse_path,
    }

    # Only inject S3 properties when the warehouse is on S3.
    if warehouse_path.startswith("s3://"):
        props.update({
            "s3.region":             region,
            "s3.access-key-id":      os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "s3.secret-access-key":  os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        })
        session_token = os.environ.get("AWS_SESSION_TOKEN", "")
        if session_token:
            props["s3.session-token"] = session_token

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


def get_or_create_table(catalog: SqlCatalog):
    """Return the stac_items table, creating it (and the namespace) if needed."""
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        return catalog.load_table(FULL_NAME)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=FULL_NAME,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
