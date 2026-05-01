"""
Shared STAC item fixtures for tests.

Provides factory functions and pre-built sample lists so every test module
uses the same canonical STAC item shape.  Import what you need:

    from tests.fixtures.stac_items import make_item, SAMPLE_ITEMS
"""

from __future__ import annotations


def make_item(
    item_id: str = "item-0001",
    *,
    lat: float = 65.0,
    lon: float = -10.0,
    width: float = 20.0,
    height: float = 10.0,
    datetime: str = "2021-06-15T00:00:00Z",
    platform: str = "sentinel-1",
    percent_valid_pixels: float = 80.0,
    date_dt: float = 5.0,
    proj_code: str = "EPSG:32632",
    sat_orbit_state: str = "ascending",
    version: str | None = None,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    mid_datetime: str | None = None,
    updated: str | None = None,
    scene_1_id: str | None = None,
    scene_2_id: str | None = None,
    scene_1_frame: str | None = None,
    scene_2_frame: str | None = None,
    grid_partition: str | None = None,
    stac_version: str = "1.0.0",
    geometry_type: str = "Polygon",
    links: list | None = None,
    assets: dict | None = None,
) -> dict:
    """Build a single STAC item dict with sensible defaults.

    The geometry is a rectangular polygon centred on (lat, lon) with the
    given width and height (in degrees).  Override *geometry_type* to
    ``"Point"`` for a point geometry (lat/lon only, width/height ignored).
    """
    if geometry_type == "Point":
        geometry = {"type": "Point", "coordinates": [lon, lat]}
    else:
        half_w, half_h = width / 2, height / 2
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [lon - half_w, lat - half_h],
                    [lon + half_w, lat - half_h],
                    [lon + half_w, lat + half_h],
                    [lon - half_w, lat + half_h],
                    [lon - half_w, lat - half_h],
                ]
            ],
        }

    props: dict = {
        "datetime": datetime,
        "platform": platform,
        "percent_valid_pixels": percent_valid_pixels,
        "date_dt": date_dt,
        "proj:code": proj_code,
        "sat:orbit_state": sat_orbit_state,
    }

    if version is not None:
        props["version"] = version
    if start_datetime is not None:
        props["start_datetime"] = start_datetime
    if end_datetime is not None:
        props["end_datetime"] = end_datetime
    if mid_datetime is not None:
        props["mid_datetime"] = mid_datetime
    if updated is not None:
        props["updated"] = updated
    if scene_1_id is not None:
        props["scene_1_id"] = scene_1_id
    if scene_2_id is not None:
        props["scene_2_id"] = scene_2_id
    if scene_1_frame is not None:
        props["scene_1_frame"] = scene_1_frame
    if scene_2_frame is not None:
        props["scene_2_frame"] = scene_2_frame
    if grid_partition is not None:
        props["grid_partition"] = grid_partition

    return {
        "id": item_id,
        "type": "Feature",
        "stac_version": stac_version,
        "geometry": geometry,
        "properties": props,
        "links": links if links is not None else [],
        "assets": assets if assets is not None else {},
    }


def make_items(
    n: int,
    *,
    id_prefix: str = "item",
    start_lat: float = 60.0,
    lat_step: float = 0.0,
    start_lon: float = -10.0,
    lon_step: float = 1.0,
    width: float = 20.0,
    height: float = 10.0,
    base_year: int = 2021,
    year_cycle: int = 4,
    base_month: int = 1,
    month_cycle: int = 9,
    platform: str = "sentinel-1",
    percent_valid_pixels_base: float = 80.0,
    date_dt_base: float = 10.0,
    proj_code: str = "EPSG:32632",
    sat_orbit_state: str = "ascending",
    stac_version: str = "1.0.0",
    links: list | None = None,
    assets: dict | None = None,
    full_props: bool = False,
) -> list[dict]:
    """Build *n* STAC items with varying geometries and dates.

    When *full_props* is ``True``, all optional scene/version properties
    are populated (matching the production ITS_LIVE schema).
    """
    items = []
    for i in range(n):
        lat = start_lat + i * lat_step
        lon = start_lon + i * lon_step
        yr = base_year + (i % year_cycle)
        mo = base_month + (i % month_cycle)
        dt = f"{yr}-{mo:02d}-15T00:00:00Z"

        kw: dict = dict(
            platform=platform,
            percent_valid_pixels=percent_valid_pixels_base + i,
            date_dt=date_dt_base + i,
            proj_code=proj_code,
            sat_orbit_state=sat_orbit_state,
            stac_version=stac_version,
        )

        if full_props:
            kw.update(
                version="002",
                start_datetime=f"{yr}-{mo:02d}-10T00:00:00Z",
                end_datetime=f"{yr}-{mo:02d}-20T00:00:00Z",
                mid_datetime=f"{yr}-{mo:02d}-15T12:00:00Z",
                updated=f"{yr}-06-01T00:00:00Z",
                scene_1_id=f"scene1_{i}",
                scene_2_id=f"scene2_{i}",
                scene_1_frame=f"frame1_{i}",
                scene_2_frame=f"frame2_{i}",
            )

        items.append(
            make_item(
                f"{id_prefix}-{i:04d}",
                lat=lat,
                lon=lon,
                width=width,
                height=height,
                datetime=dt,
                links=links,
                assets=assets,
                **kw,
            )
        )
    return items


SAMPLE_ITEMS = make_items(8, id_prefix="v2-item", full_props=True)

SAMPLE_ITEM_MAP: dict[tuple[str, str], dict] = {
    ("mock-bucket", f"stac/item-{i:04d}.stac.json"): item for i, item in enumerate(SAMPLE_ITEMS)
}
