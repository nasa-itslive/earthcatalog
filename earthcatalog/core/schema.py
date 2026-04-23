"""
Canonical PyArrow schema for earthcatalog STAC items.

Single source of truth — imported by both ``transform.py`` (GeoParquet writing)
and ``catalog.py`` (PyIceberg schema derivation).

``GEOMETRY_FIELDS``
    Set of field names that carry WKB geometry and need the
    ``geoarrow.wkb`` Arrow extension annotation.
"""

import pyarrow as pa

# Fields whose Arrow type is WKB binary and must be annotated with the
# ``geoarrow.wkb`` extension type when writing GeoParquet.
GEOMETRY_FIELDS: frozenset[str] = frozenset({"geometry"})

schema = pa.schema([
    # Identity
    pa.field("id",                   pa.string(),              nullable=False),
    pa.field("grid_partition",       pa.string(),              nullable=False),
    pa.field("geometry",             pa.binary(),              nullable=True),   # WKB; optional (rustac writes as nullable)

    # Timestamps
    pa.field("datetime",             pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("start_datetime",       pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("mid_datetime",         pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("end_datetime",         pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("created",              pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("updated",              pa.timestamp("us", tz="UTC"), nullable=True),

    # Metrics — always integers; source floats are rounded in fan_out()
    pa.field("percent_valid_pixels", pa.int32(),  nullable=True),
    pa.field("date_dt",              pa.int32(),  nullable=True),

    # Coordinates
    pa.field("latitude",             pa.float64(), nullable=True),
    pa.field("longitude",            pa.float64(), nullable=True),

    # Categoricals
    pa.field("platform",             pa.string(), nullable=True),
    pa.field("version",              pa.string(), nullable=True),
    pa.field("proj_code",            pa.string(), nullable=True),   # "proj:code"      → safe column name
    pa.field("sat_orbit_state",      pa.string(), nullable=True),   # "sat:orbit_state" → safe column name
    pa.field("scene_1_id",           pa.string(), nullable=True),
    pa.field("scene_2_id",           pa.string(), nullable=True),
    pa.field("scene_1_frame",        pa.string(), nullable=True),
    pa.field("scene_2_frame",        pa.string(), nullable=True),

    # Fallback: full STAC item JSON for schema evolution / debugging
    pa.field("raw_stac",             pa.string(), nullable=True),
])
