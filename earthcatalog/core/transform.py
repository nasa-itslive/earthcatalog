"""
STAC item transformation: H3 fan-out + GeoParquet writing via rustac.

Public functions
----------------
``fan_out(items, partitioner)``
    Produce one synthetic STAC item per (source_item × grid_cell) pair.
    Sanitises property names (``proj:code`` → ``proj_code``), rounds float
    fields to int, and injects ``grid_partition`` into each item's properties.

``group_by_partition(fan_out_items)``
    Group the output of ``fan_out()`` by ``(grid_partition, year)`` so that
    each group can be written to exactly one Parquet file.  This is required
    for Iceberg ``IdentityTransform`` + ``YearTransform`` partition pruning:
    ``table.add_files()`` requires every registered file to contain exactly one
    value for each partitioned column.

``write_geoparquet(fan_out_items, path)``
    Write a **single-partition** list of synthetic items to a GeoParquet file
    using ``rustac.GeoparquetWriter``.  rustac handles all geo metadata — the
    ``geo`` Parquet key and the ``geoarrow.wkb`` extension on the geometry
    column.  Rows are sorted by ``(platform, datetime)`` before writing so that
    Parquet row-group statistics are maximally useful for predicate pushdown.

Spatial predicate pushdown
--------------------------
The correct usage pattern for spatial queries:

1. Convert the query geometry to grid cell IDs (e.g. H3 cells at resolution 1):
       candidate_cells = h3.geo_to_cells(mapping(query_geom), resolution=1)
2. Filter the Iceberg table via:
       WHERE grid_partition IN (<candidate_cells>)
   Iceberg's IdentityTransform partition pruning will skip all files whose
   ``grid_partition`` value is not in the candidate set — zero row reads from
   irrelevant cells.

Schema alignment
----------------
rustac infers column types from the item dicts:
  - Python ``int``  → Arrow ``int64``  (cast to ``int32`` to match Iceberg)
  - STAC ``datetime``, ``start_datetime`` → ``timestamp[ms, tz=UTC]``
    (cast to ``timestamp[us, tz=UTC]`` to match Iceberg TimestamptzType)
  - Non-standard timestamps (``mid_datetime``) → ``string``
    (cast via Arrow string→timestamp)
  - Columns absent from a batch → filled with nulls of the target type
"""

import asyncio
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import obstore
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import rustac

from .partitioner import AbstractPartitioner
from .schema import GEOMETRY_FIELDS
from .schema import schema as ARROW_SCHEMA

# ---------------------------------------------------------------------------
# FileMetadata — tiny serialisable record returned by ingest / compact workers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Target schema — derived from the canonical schema.py so there is one source
# of truth.  Each entry is (name, Arrow type, nullable, is_geometry).
# ---------------------------------------------------------------------------

_COLUMNS: list[tuple[str, pa.DataType, bool, bool]] = [
    (
        f.name,
        f.type,
        f.nullable,
        f.name in GEOMETRY_FIELDS,
    )
    for f in ARROW_SCHEMA
]

# Keep the old explicit list as a sanity reference — not used at runtime.
# fmt: off
_EXPECTED_NAMES = [
    "id", "grid_partition", "geometry",
    "datetime", "start_datetime", "mid_datetime", "end_datetime", "created", "updated",
    "percent_valid_pixels", "date_dt",
    "latitude", "longitude",
    "platform", "version", "proj_code", "sat_orbit_state",
    "scene_1_id", "scene_2_id", "scene_1_frame", "scene_2_frame",
    "raw_stac",
]
# fmt: on
assert [c[0] for c in _COLUMNS] == _EXPECTED_NAMES, "schema.py field order mismatch"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _to_int(value: object) -> int | None:
    """Round a float/int value to Python int, or None if missing."""
    if value is None:
        return None
    return int(round(value))


def _cast_col(col: pa.ChunkedArray | pa.Array, target: pa.DataType) -> pa.Array:
    """
    Cast *col* to *target*, handling null-typed columns and string→timestamp.
    """
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    if col.type == target:
        return col
    if pa.types.is_null(col.type):
        return pa.nulls(len(col), type=target)
    return col.cast(target, safe=False)


def _align_schema(raw: pa.Table, geo_meta: bytes) -> pa.Table:
    """
    Select / cast columns from a rustac-written table to our target layout.

    ``geo_meta`` comes directly from the rustac Parquet file and is forwarded
    to the output schema as-is — we never construct it ourselves.
    """
    cols: dict[str, pa.Array] = {}
    fields: list[pa.Field] = []

    for name, typ, nullable, is_geom in _COLUMNS:
        if name in raw.schema.names:
            col = _cast_col(raw.column(name), typ)
        else:
            col = pa.nulls(len(raw), type=typ)

        cols[name] = col
        fmeta = {b"ARROW:extension:name": b"geoarrow.wkb"} if is_geom else None
        fields.append(pa.field(name, typ, nullable=nullable, metadata=fmeta))

    schema_meta = {b"geo": geo_meta} if geo_meta else None
    return pa.table(cols, schema=pa.schema(fields, metadata=schema_meta))


async def _async_write(items: list[dict], path: str) -> None:
    writer = await rustac.GeoparquetWriter.open(items, path)
    await writer.finish()


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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fan_out(
    stac_items: list[dict],
    partitioner: AbstractPartitioner,
) -> list[dict]:
    """
    Produce one synthetic STAC item per (source_item × grid_cell) pair.

    Each synthetic item carries sanitised, flattened properties (including
    ``grid_partition``) so that ``rustac.GeoparquetWriter`` promotes them
    directly to top-level GeoParquet columns.

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

        base_props = {
            "datetime": props.get("datetime"),
            "start_datetime": props.get("start_datetime"),
            "mid_datetime": props.get("mid_datetime"),
            "end_datetime": props.get("end_datetime"),
            "created": props.get("created"),
            "updated": props.get("updated"),
            "percent_valid_pixels": _to_int(props.get("percent_valid_pixels")),
            "date_dt": _to_int(props.get("date_dt")),
            "latitude": props.get("latitude"),
            "longitude": props.get("longitude"),
            "platform": props.get("platform"),
            "version": props.get("version"),
            "proj_code": props.get("proj:code"),
            "sat_orbit_state": props.get("sat:orbit_state"),
            "scene_1_id": props.get("scene_1_id"),
            "scene_2_id": props.get("scene_2_id"),
            "scene_1_frame": props.get("scene_1_frame"),
            "scene_2_frame": props.get("scene_2_frame"),
            "raw_stac": orjson.dumps(item).decode(),
        }

        for key in keys:
            result.append(
                {
                    "type": "Feature",
                    "stac_version": item.get("stac_version", "1.0.0"),
                    "id": item["id"],
                    "geometry": item["geometry"],
                    "properties": {**base_props, "grid_partition": key},
                    "links": [],
                    "assets": {},
                }
            )

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


def write_geoparquet(fan_out_items: list[dict], path: str) -> int:
    """
    Write fan-out STAC items to a GeoParquet file using rustac.

    Caller's responsibility
    -----------------------
    Pass items for **a single (grid_partition, year) group** — i.e. the output
    of one iteration over :func:`group_by_partition`.  If items span multiple
    partitions the resulting file will violate the Iceberg ``IdentityTransform``
    constraint and ``table.add_files()`` will raise a ``ValueError``.

    rustac.GeoparquetWriter produces:
    - a ``geo`` key in the Parquet file metadata
    - ``geoarrow.wkb`` extension type on the geometry column

    After writing, the output is aligned to our 22-column target schema (type
    casts, missing-column fill with nulls) and the ``geo`` metadata is
    forwarded unchanged from the rustac temp file to the final file.

    Returns the number of rows written (0 if the input list is empty).
    """
    if not fan_out_items:
        return 0

    tmp = path + ".rustac_tmp"
    try:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_async_write(fan_out_items, tmp))
        finally:
            loop.close()

        raw = pq.ParquetFile(tmp).read()
        geo_meta = pq.ParquetFile(tmp).metadata.metadata.get(b"geo", b"")
        aligned = _align_schema(raw, geo_meta)
        pq.write_table(aligned, path)
        return len(aligned)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


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
