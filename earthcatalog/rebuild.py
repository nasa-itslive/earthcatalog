"""Iceberg catalog rebuild from the physical warehouse.

Drop-recreate the Iceberg table and re-register every current warehouse file.
This is the "repair table" operation used by garbage collection and
consolidation after Parquet files are rewritten on S3.
"""

from __future__ import annotations

import obstore
from obstore.store import ObjectStore


def _list_warehouse_keys(
    warehouse_store: ObjectStore,
    warehouse_root: str,
) -> list[str]:
    """List all hive-style GeoParquet keys in *warehouse_store*.

    Returns full paths suitable for ``table.add_files()`` by prepending
    *warehouse_root* to each relative store key.
    """
    import re

    # v2 (schema-driven) and v1 (legacy) hive layouts are both registered.
    hive_re_v2 = re.compile(
        r"grid=[^/]+/level=[^/]+/tile=[^/]+/(?:year|month|day)=[^/]+/(?P<file>[^/]+\.parquet)$"
    )
    hive_re_v1 = re.compile(r"grid_partition=[^/]+/year=[^/]+/(?P<file>[^/]+\.parquet)$")
    root = warehouse_root.rstrip("/")

    # For S3 stores the list prefix is the warehouse path within the bucket.
    # For LocalStore the store is already scoped to the warehouse root.
    prefix = ""
    if warehouse_root.startswith("s3://"):
        _bucket, _, path = warehouse_root.removeprefix("s3://").partition("/")
        prefix = path.rstrip("/") + "/"

    paths: list[str] = []
    for batch in obstore.list(warehouse_store, prefix=prefix):
        for obj in batch:
            k: str = obj["path"]
            if k.endswith(".parquet") and (hive_re_v2.search(k) or hive_re_v1.search(k)):
                # obstore keys are bucket-relative on S3 (they already include
                # the warehouse path); joining the full root would double it.
                if warehouse_root.startswith("s3://"):
                    bucket = warehouse_root.removeprefix("s3://").split("/", 1)[0]
                    paths.append(f"s3://{bucket}/{k}")
                else:
                    paths.append(f"{root}/{k}")
    return paths


def rebuild_iceberg_from_warehouse(
    catalog_path: str,
    warehouse_root: str,
    warehouse_store: ObjectStore,
    *,
    upload: bool = True,
) -> int:
    """Drop-recreate the Iceberg table and re-register every current warehouse file.

    Reads existing table properties (grid type, resolution, etc.) from the
    SQLite catalog *before* dropping the table so they are preserved in the
    recreated table.  All ``.parquet`` files found by
    :func:`_list_warehouse_keys` — including ``gc_*`` files written by a
    previous garbage-collection run — are registered via ``table.add_files()``.

    Parameters
    ----------
    catalog_path:
        Local path to the SQLite catalog file.
    warehouse_root:
        ``s3://`` URI or local path for the warehouse root, used both by
        PyIceberg for file-path storage and by :func:`_list_warehouse_keys`
        to enumerate current files.
    warehouse_store:
        obstore-compatible store used to list the warehouse.  For S3 this
        should be a bucket-level ``S3Store``; :func:`_list_warehouse_keys`
        derives the correct key prefix from *warehouse_root*.
    upload:
        When ``True`` (default) call :func:`upload_catalog` after rebuilding
        so the updated SQLite file is pushed back to S3.  Pass ``False`` when
        the caller will handle the upload itself.

    Returns
    -------
    int
        Number of Parquet files registered in the rebuilt Iceberg table.
    """
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

    from earthcatalog.catalog import (
        FULL_NAME,
        ICEBERG_SCHEMA,
        NAMESPACE,
        PARTITION_SPEC,
        _open_sqlite,
        upload_catalog,
    )

    catalog = _open_sqlite(db_path=catalog_path, warehouse_path=warehouse_root)

    try:
        existing = catalog.load_table(FULL_NAME)
        preserved_props: dict[str, str] = dict(existing.properties)
    except NoSuchTableError:
        preserved_props = {}

    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(FULL_NAME)
        print("Iceberg rebuild: dropped stale table.")
    except NoSuchTableError:
        pass

    table = catalog.create_table(
        identifier=FULL_NAME,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
        properties=preserved_props,
    )

    all_paths = _list_warehouse_keys(warehouse_store, warehouse_root)
    if all_paths:
        batch_size = 2000
        for i in range(0, len(all_paths), batch_size):
            table.add_files(all_paths[i : i + batch_size])
    print(f"Iceberg rebuild: registered {len(all_paths):,} files.")

    if upload:
        upload_catalog(catalog_path)

    return len(all_paths)
