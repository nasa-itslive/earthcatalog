"""
STAC item transformation: H3 fan-out + stac-geoparquet writing via rustac.

Public functions
----------------
``fan_out(items, partitioner)``
    Produce one synthetic STAC item per (source_item × grid_cell) pair.
    Injects ``grid_partition`` into each item's properties.

``group_by_partition(fan_out_items)``
    Group the output of ``fan_out()`` by ``(grid_partition, year)`` so that
    each group can be written to exactly one Parquet file.  This is required
    for Iceberg ``IdentityTransform`` + ``YearTransform`` partition pruning.

``write_geoparquet(fan_out_items, path)``
    Write a **single-partition** list of synthetic items to a GeoParquet file
    using rustac.write(). rustac writes proper stac-geoparquet with:
    - assets as struct column
    - links as list column
    - properties promoted to top-level columns
    - geoarrow.wkb extension on geometry column

Spatial predicate pushdown
--------------------------
The correct usage pattern for spatial queries:

1. Convert the query geometry to grid cell IDs (e.g. H3 cells at resolution 1):
       candidate_cells = h3.geo_to_cells(mapping(query_geom), resolution=1)
2. Filter the Iceberg table via:
       WHERE grid_partition IN (<candidate_cells>)
   Iceberg's IdentityTransform partition pruning will skip all files whose
   ``grid_partition`` value is not in the candidate set.
"""

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path

import obstore
import rustac

from .partitioner import AbstractPartitioner


@dataclass
class FileMetadata:
    """
    Lightweight record describing one GeoParquet file written to a store.

    Designed to be trivially serialisable by Dask (no PyArrow, no Iceberg
    imports) so it crosses the network from worker to head node at ~200 B/file.

    Attributes
    ----------
    s3_key:
        Key relative to the warehouse store root — e.g.
        ``"grid_partition=81003ffffffffff/year=2025/part_000000_abc1.parquet"``.
        The caller that knows the store root appends this to construct the full
        URI used in ``table.add_files()``.
    grid_partition:
        H3 cell string (or ``"__none__"`` for unlocated items).
    year:
        4-digit calendar year from the ``datetime`` property, or ``None`` for
        items without a parseable datetime.
    row_count:
        Number of rows in the file.
    file_size_bytes:
        Byte size of the written Parquet file.
    """

    s3_key: str
    grid_partition: str
    year: int | None
    row_count: int
    file_size_bytes: int


def _year_from_item(item: dict) -> int | None:
    """Extract the 4-digit year from a STAC item's datetime property, or None."""
    dt_str = item.get("properties", {}).get("datetime")
    if dt_str:
        try:
            return int(dt_str[:4])
        except (ValueError, TypeError):
            pass
    return None


def _sort_key(item: dict) -> tuple[str, str]:
    """Sort key for within-partition ordering: (platform, datetime)."""
    props = item.get("properties", {})
    return (props.get("platform") or "", props.get("datetime") or "")


def fan_out(
    stac_items: list[dict],
    partitioner: AbstractPartitioner,
) -> list[dict]:
    """
    Produce one synthetic STAC item per (source_item × grid_cell) pair.

    Each synthetic item is the original STAC item with ``grid_partition``
    injected into its ``properties``.  All original fields (assets, links,
    collection, …) are preserved as-is so that rustac.write() can emit a
    complete stac-geoparquet file with the native rustac schema.

    Items with unparseable or empty geometry are silently skipped.
    """
    from shapely.geometry import shape  # deferred — heavy library

    result: list[dict] = []
    for item in stac_items:
        props = item.get("properties", {})
        try:
            geom = shape(item["geometry"])
            keys = partitioner.get_intersecting_keys(geom.wkb) or ["__none__"]
        except Exception:
            continue

        for key in keys:
            synthetic = {**item, "properties": {**props, "grid_partition": key}}
            result.append(synthetic)

    return result


def group_by_partition(
    fan_out_items: list[dict],
) -> dict[tuple[str, int | None], list[dict]]:
    """
    Group fan-out items by ``(grid_partition, year)`` and sort each group by
    ``(platform, datetime)``.

    Each resulting group satisfies both Iceberg partition constraints:

    * ``IdentityTransform`` on ``grid_partition`` — every item in the group
      has the same ``grid_partition`` value, so Parquet column statistics give
      a single min == max that ``add_files()`` can use unambiguously.
    * ``YearTransform`` on ``datetime`` — every item in the group has a
      ``datetime`` in the same calendar year, so the year-level Parquet
      statistics are also unambiguous.

    The within-group sort by ``(platform, datetime)`` maximises Parquet
    row-group min/max statistics for predicate pushdown on those columns.

    Parameters
    ----------
    fan_out_items:
        Output of :func:`fan_out` — each item has exactly one
        ``grid_partition`` value in its ``properties``.

    Returns
    -------
    dict mapping ``(cell_id, year)`` → sorted list of synthetic STAC items.
    ``year`` is ``None`` for items that carry no ``datetime`` property.
    """
    groups: dict[tuple[str, int | None], list[dict]] = {}
    for item in fan_out_items:
        cell = item["properties"].get("grid_partition", "__none__")
        year = _year_from_item(item)
        key = (cell, year)
        groups.setdefault(key, []).append(item)

    # Sort within each group for optimal Parquet column statistics
    for key in groups:
        groups[key].sort(key=_sort_key)

    return groups


def _normalize_for_iceberg(table) -> object:
    """
    Post-process a rustac-written PyArrow table for PyIceberg V2 compatibility.

    rustac emits:
    - ``assets``: struct (keys = asset names) → cast to JSON string
    - ``links``: list<struct<href, rel, …>> → cast to JSON string
    - ``bbox``: struct<xmin, ymin, xmax, ymax> → cast to [xmin, ymin, xmax, ymax] JSON array
    - ``collection``: null type (PyIceberg V2 rejects pa.null()) → drop column
    - ``type``/``stac_version``: dictionary<string> → PyIceberg warns but accepts

    The geo metadata (ARROW:extension:name on geometry) is preserved because
    we only replace specific columns, not the file itself.
    """
    import json

    import pyarrow as pa

    def _to_json(col):
        return pa.array(
            [json.dumps(v.as_py()) if v.is_valid else None for v in col],
            type=pa.string(),
        )

    def _bbox_struct_to_array(col):
        """Convert struct<xmin,ymin,xmax,ymax> → JSON array string [xmin,ymin,xmax,ymax]."""
        field_names = {col.type.field(i).name for i in range(col.type.num_fields)}
        if field_names >= {"xmin", "ymin", "xmax", "ymax"}:
            return pa.array(
                [
                    json.dumps(
                        [v.as_py()["xmin"], v.as_py()["ymin"], v.as_py()["xmax"], v.as_py()["ymax"]]
                    )
                    if v.is_valid
                    else None
                    for v in col
                ],
                type=pa.string(),
            )
        return _to_json(col)

    def _try_cast_float_to_int(col):
        """Cast a float64/double column to int32 if all non-null values are whole numbers."""
        import pyarrow.compute as pc

        floored = pc.floor(col)
        if pc.all(pc.or_(pc.is_null(col), pc.equal(col, floored))).as_py():
            return col.cast(pa.int32())
        return col

    cols = {}
    fields = []
    for i, name in enumerate(table.schema.names):
        col = table.column(name)
        orig_field = table.schema.field(name)
        if pa.types.is_dictionary(col.type):
            # Cast dictionary-encoded columns to plain string so Iceberg
            # can read partition values (e.g. grid_partition) directly.
            decoded = col.cast(pa.string())
            cols[name] = decoded
            fields.append(pa.field(name, pa.string(), metadata=orig_field.metadata))
        elif pa.types.is_struct(col.type):
            converted = _bbox_struct_to_array(col) if name == "bbox" else _to_json(col)
            cols[name] = converted
            fields.append(pa.field(name, pa.string(), metadata=orig_field.metadata))
        elif pa.types.is_list(col.type):
            cols[name] = _to_json(col)
            fields.append(pa.field(name, pa.string(), metadata=orig_field.metadata))
        elif col.type == pa.null():
            pass  # drop — PyIceberg V2 cannot represent null type
        elif col.type in (pa.float64(), pa.float32()):
            # Never downcast coordinate columns — whole-number lat/lon values
            # (e.g. longitude=0.0) would be cast to int32, breaking the schema.
            casted = col if name in ("latitude", "longitude") else _try_cast_float_to_int(col)
            cols[name] = casted
            fields.append(pa.field(name, casted.type, metadata=orig_field.metadata))
        else:
            cols[name] = col
            fields.append(orig_field)

    schema = pa.schema(fields, metadata=table.schema.metadata)
    return pa.table(cols, schema=schema)


def write_geoparquet(fan_out_items: list[dict], path: str) -> int:
    """
    Write fan-out STAC items to a GeoParquet file using rustac.

    Caller's responsibility
    -----------------------
    Pass items for **a single (grid_partition, year) group** — i.e. the output
    of one iteration over :func:`group_by_partition`.  If items span multiple
    partitions the resulting file will violate the Iceberg ``IdentityTransform``
    constraint and ``table.add_files()`` will raise a ``ValueError``.

    rustac writes the full stac-geoparquet schema.  A post-processing step
    casts struct/list columns (assets, links) to JSON strings and drops null-
    typed columns (collection) so the file is compatible with PyIceberg V2
    ``add_files()``.

    Returns the number of rows written (0 if the input list is empty).
    """
    if not fan_out_items:
        return 0

    import pyarrow.parquet as pq

    async def _write():
        await rustac.write(path, fan_out_items)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_write())
    finally:
        loop.close()

    # Post-process: cast assets/links → JSON strings, drop null columns,
    # cast whole-number floats → int32.  Field metadata (e.g. geoarrow.wkb
    # extension on geometry) is preserved by _normalize_for_iceberg.
    # File-level keys written by rustac (geo, stac-geoparquet) live in the
    # Parquet file metadata, not the Arrow schema metadata.  We carry them
    # into the Arrow schema so pq.write_table re-encodes them.
    # Use ParquetFile to read the single file exactly — avoids PyArrow's
    # Hive-partition directory discovery which breaks inside partitioned layouts.
    pf = pq.ParquetFile(path)
    file_meta = pf.metadata.metadata
    table = pf.read()
    table = _normalize_for_iceberg(table)
    preserve_keys = (b"geo", b"stac-geoparquet")
    extra = {k: v for k, v in file_meta.items() if k in preserve_keys}
    if extra:
        merged = {**(table.schema.metadata or {}), **extra}
        table = table.replace_schema_metadata(merged)
    # Write via a file object rather than a path string to prevent PyArrow's
    # Parquet reader/writer from treating the parent directory as a Hive-
    # partitioned dataset (which causes schema-merge errors inside layouts
    # like grid_partition=X/year=Y/).  S3 paths never reach this code path —
    # write_geoparquet_s3 writes to a local temp file then uploads via obstore.
    with open(path, "wb") as _fh:
        pq.write_table(table, _fh, compression="zstd")

    return len(fan_out_items)


def write_geoparquet_s3(
    fan_out_items: list[dict],
    store: object,
    s3_key: str,
) -> tuple[int, int]:
    """
    Write a **single-partition** list of fan-out items as GeoParquet to a store.

    Writes to a local temporary file (via :func:`write_geoparquet`) then
    uploads the bytes via ``obstore.put``.  The temporary file is always
    deleted, even on error.

    This is the S3-capable counterpart to :func:`write_geoparquet`.  Workers
    on a Dask cluster call this function directly; the store is injected so
    the function is testable with any ``obstore``-compatible backend
    (``MemoryStore``, ``LocalStore``, ``S3Store``).

    Parameters
    ----------
    fan_out_items:
        Output of one ``group_by_partition()`` iteration — all items must
        belong to the same ``(grid_partition, year)`` group.
    store:
        An ``obstore``-compatible store (``S3Store``, ``LocalStore``, or
        ``MemoryStore``).
    s3_key:
        Key within the store, e.g.
        ``"grid_partition=81003ffffffffff/year=2025/part_000000_abc1.parquet"``.

    Returns
    -------
    ``(row_count, byte_count)`` — both zero if *fan_out_items* is empty (no
    file is uploaded in that case).
    """
    if not fan_out_items:
        return 0, 0

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        n = write_geoparquet(fan_out_items, tmp_path)
        if n == 0:
            return 0, 0
        data = Path(tmp_path).read_bytes()
        obstore.put(store, s3_key, data)
        return n, len(data)
    finally:
        Path(tmp_path).unlink(missing_ok=True)
