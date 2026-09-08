"""Iceberg catalog constants, schema, partition spec, and helpers.

Pure data module — no behaviour, no side effects.  Keeps catalog.py focused
on lifecycle and the EarthCatalog facade.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

NAMESPACE = "earthcatalog"
TABLE_NAME = "stac_items"
FULL_NAME = f"{NAMESPACE}.{TABLE_NAME}"

PROP_GRID_TYPE = "earthcatalog.grid.type"
PROP_GRID_RESOLUTION = "earthcatalog.grid.resolution"
PROP_GRID_BOUNDARIES_PATH = "earthcatalog.grid.boundaries_path"
PROP_GRID_ID_FIELD = "earthcatalog.grid.id_field"
PROP_INDEX_PATH = "earthcatalog.index_path"
PROP_HASH_INDEX_PATH = "earthcatalog.hash_index_path"
PROP_TIME_BIN = "earthcatalog.time_bin"

# The warehouse layout is schema-driven: grid type, grid level, tile id and
# the temporal bin all come from the catalog configuration.
#
#   current (v2):  grid=h3/level=1/tile=810fbffffffffff/year=2025/part_000000.parquet
#                  grid=lat_lon/level=2/tile=…/month=2026-03/…
#                  grid=s2/level=4/tile=…/day=2026-03-05/…
#   legacy (v1):   grid_partition=810fbffffffffff/year=2025/part_xxx.parquet
#
# Both layouts remain readable everywhere (gc / rebuild / discovery parse
# either); new data is always written in the v2 layout.
_HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)
_HIVE_RE_V2 = re.compile(
    r"grid=(?P<grid>[^/]+)/level=(?P<level>[^/]+)/tile=(?P<tile>[^/]+)/"
    r"year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)

TIME_BINS = ("year", "month", "day")

_PROP_BY_BIN = {"year": "year", "month": "month", "day": "day"}
_TRANSFORM_BY_BIN: dict[str, Any] = {
    "year": lambda: YearTransform(),
    "month": lambda: MonthTransform(),
    "day": lambda: DayTransform(),
}


def partition_prefix(
    warehouse_prefix: str,
    grid: str,
    level: str,
    tile: str,
    time_bin: str,
    bin_value: str,
) -> str:
    """Hive prefix for one tile/temporal-bin partition, schema-driven:

    ``{warehouse}/grid={grid}/level={level}/tile={tile}/{time_bin}={value}/``

    *time_bin* is ``"year"``, ``"month"`` or ``"day"``; *bin_value* is the
    formatted value (``2025``, ``2026-03``, ``2026-03-05``).
    """
    if time_bin not in TIME_BINS:
        raise ValueError(f"unknown time bin: {time_bin!r}")
    return (
        f"{warehouse_prefix.rstrip('/')}/grid={grid}/level={level}/"
        f"tile={tile}/{time_bin}={bin_value}/"
    )


def bin_value(value: str | datetime | None, time_bin: str = "year") -> str:
    """Format a temporal value for the hive path: ``2025`` / ``2025-12`` /
    ``2025-12-20``.

    Accepts the ISO strings STAC items carry or a datetime.  Missing or
    unparseable values map to ``"unknown"`` — files without a datetime live
    in the ``unknown`` partition and index rows must agree.
    """
    if time_bin not in TIME_BINS:
        raise ValueError(f"unknown time bin: {time_bin!r}")
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        value = value.astimezone(UTC).isoformat()
    s = str(value)
    y, m, d = s[:4], s[5:7], s[8:10]
    if not (y.isdigit() and len(y) == 4):
        return "unknown"
    if time_bin == "year":
        return y
    if m.isdigit() and len(m) == 2:
        if time_bin == "month":
            return f"{y}-{m}"
        if d.isdigit() and len(d) == 2:
            return f"{y}-{m}-{d}"
    return "unknown"


def layout_of(props: Mapping[str, str]) -> tuple[str, str, str]:
    """``(grid, level, time_bin)`` from table properties; defaults h3/1/year.

    The single source of truth for how new warehouse keys are built —
    writers pass this plain tuple around (worker-safe) instead of the table.
    """
    grid = props.get(PROP_GRID_TYPE, "h3")
    level = props.get(PROP_GRID_RESOLUTION, "1")
    time_bin = props.get(PROP_TIME_BIN, "year")
    return grid, level, time_bin


def partition_year(bin_name: str, ordinal: int) -> int:
    """Calendar year from a temporal-transform partition ordinal.

    Iceberg stores YearTransform as years-since-1970, MonthTransform as
    months-since-1970 and DayTransform as days-since-1970; stats caches and
    the search prune need the calendar year back.
    """
    if bin_name == "year":
        return ordinal + 1970
    if bin_name == "month":
        return 1970 + ordinal // 12
    if bin_name == "day":
        return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=ordinal)).year
    raise ValueError(f"unknown time bin: {bin_name!r}")


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


def build_partition_spec(time_bin: str = "year") -> PartitionSpec:
    """Partition spec for a temporal binning: identity on ``grid_partition``
    plus a temporal transform (Year/Month/Day) on ``datetime``.  The
    transform field is named after the bin (``year`` / ``month`` / ``day``).
    """
    if time_bin not in TIME_BINS:
        raise ValueError(f"unknown time bin: {time_bin!r}")
    return PartitionSpec(
        PartitionField(
            source_id=2, field_id=100, transform=IdentityTransform(), name="grid_partition"
        ),
        PartitionField(
            source_id=4,
            field_id=101,
            transform=_TRANSFORM_BY_BIN[time_bin](),
            name=time_bin,
        ),
    )


PARTITION_SPEC = build_partition_spec("year")
# Legacy alias — new code passes the bin explicitly (see build_partition_spec).
