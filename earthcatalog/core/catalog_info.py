"""
CatalogInfo — discover grid system metadata from an open Iceberg table.

Grid type, resolution, and related parameters are stored as Iceberg table
properties at ingest time (see :func:`~earthcatalog.core.catalog.get_or_create_table`).
CatalogInfo reads them back so downstream users don't need prior knowledge
of how the catalog was built.

Example::

    from earthcatalog.core.catalog import open_catalog, get_or_create_table
    from earthcatalog.core.catalog_info import CatalogInfo
    from shapely.geometry import box

    catalog = open_catalog(db_path="catalog.db", warehouse_path="warehouse/")
    table   = get_or_create_table(catalog)
    info    = CatalogInfo.from_table(table)

    bbox  = box(-60, 60, -30, 80)          # Greenland
    cells = info.cells_for_geometry(bbox)   # H3 cells at the catalog's resolution
    sql   = info.cell_list_sql(bbox)        # ready for WHERE grid_partition IN (...)
"""

from __future__ import annotations

from dataclasses import dataclass

from shapely.geometry import mapping

from earthcatalog.core.catalog import (
    PROP_GRID_BOUNDARIES_PATH,
    PROP_GRID_ID_FIELD,
    PROP_GRID_RESOLUTION,
    PROP_GRID_TYPE,
)


@dataclass
class CatalogInfo:
    """Grid metadata read from Iceberg table properties.

    Attributes
    ----------
    grid_type:
        ``"h3"`` or ``"geojson"``.  Defaults to ``"h3"`` for catalogs created
        before table properties were introduced.
    grid_resolution:
        H3 resolution level (0–15).  ``None`` for GeoJSON catalogs.
    boundaries_path:
        Path or S3 URI to the GeoJSON boundaries file.  ``None`` for H3 catalogs.
    id_field:
        GeoJSON feature property used as the partition key.  ``None`` for H3 catalogs.
    """

    grid_type: str
    grid_resolution: int | None
    boundaries_path: str | None
    id_field: str | None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_table(cls, table) -> CatalogInfo:
        """Build a CatalogInfo from an open PyIceberg Table.

        Falls back to sensible defaults for catalogs that predate table-property
        support (grid_type="h3", grid_resolution=1).
        """
        props = table.properties
        grid_type = props.get(PROP_GRID_TYPE, "h3")
        raw_res = props.get(PROP_GRID_RESOLUTION)
        grid_resolution = (
            int(raw_res) if raw_res is not None else (1 if grid_type == "h3" else None)
        )
        return cls(
            grid_type=grid_type,
            grid_resolution=grid_resolution,
            boundaries_path=props.get(PROP_GRID_BOUNDARIES_PATH),
            id_field=props.get(PROP_GRID_ID_FIELD),
        )

    # ------------------------------------------------------------------
    # Spatial helpers
    # ------------------------------------------------------------------

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys that intersect *geom*.

        Parameters
        ----------
        geom:
            Any Shapely geometry (Point, Polygon, MultiPolygon, …).

        Returns
        -------
        list[str]
            H3 cell indices (for H3 catalogs) or GeoJSON region IDs (for
            GeoJSON catalogs) that intersect *geom*.
        """
        if self.grid_type == "h3":
            return self._h3_cells(geom)
        if self.grid_type == "geojson":
            return self._geojson_keys(geom)
        raise ValueError(f"Unknown grid type: {self.grid_type!r}")

    def cell_list_sql(self, geom) -> str:
        """Return a SQL fragment suitable for ``WHERE grid_partition IN (...)``.

        Example::

            sql_filter = info.cell_list_sql(bbox)
            query = f"SELECT * FROM iceberg_scan(...) WHERE {sql_filter}"
        """
        cells = self.cells_for_geometry(geom)
        if not cells:
            # No intersecting cells — guarantee the query returns nothing.
            return "grid_partition IN (NULL)"
        quoted = ", ".join(f"'{c}'" for c in cells)
        return f"grid_partition IN ({quoted})"

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _h3_cells(self, geom) -> list[str]:
        import h3
        from shapely.geometry import Point

        res = self.grid_resolution if self.grid_resolution is not None else 1
        if isinstance(geom, Point):
            return [h3.latlng_to_cell(geom.y, geom.x, res)]
        # geo_to_cells: centroid-in test; add boundary cells for edge coverage.
        interior = set(h3.geo_to_cells(mapping(geom), res))
        boundary = self._h3_boundary_cells(geom, res)
        return list(interior | boundary)

    @staticmethod
    def _h3_boundary_cells(geom, resolution: int) -> set[str]:
        """Walk the exterior ring at ~10 km intervals, collecting H3 cells."""
        import h3
        from shapely.geometry import MultiPolygon, Polygon

        cells: set[str] = set()
        polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
        for poly in polys:
            if not isinstance(poly, Polygon):
                continue
            coords = list(poly.exterior.coords)
            for (lon0, lat0), (lon1, lat1) in zip(coords, coords[1:]):
                dist = ((lon1 - lon0) ** 2 + (lat1 - lat0) ** 2) ** 0.5
                n = max(2, int(dist / 0.1))
                for i in range(n):
                    t = i / n
                    lat = lat0 + t * (lat1 - lat0)
                    lon = lon0 + t * (lon1 - lon0)
                    cells.add(h3.latlng_to_cell(lat, lon, resolution))
        return cells

    def _geojson_keys(self, geom) -> list[str]:
        """Return GeoJSON region IDs that intersect *geom*."""
        if not self.boundaries_path:
            raise ValueError(
                "CatalogInfo.boundaries_path is required for geojson grid type. "
                "Re-ingest with a GridConfig that specifies boundaries_path."
            )
        from earthcatalog.grids.geojson_partitioner import GeoJSONPartitioner

        partitioner = GeoJSONPartitioner(
            boundaries_path=self.boundaries_path,
            id_field=self.id_field or "id",
        )
        from shapely import wkb

        return partitioner.get_intersecting_keys(wkb.dumps(geom))

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        if self.grid_type == "h3":
            return f"CatalogInfo(grid_type='h3', resolution={self.grid_resolution})"
        return (
            f"CatalogInfo(grid_type='geojson', "
            f"boundaries_path={self.boundaries_path!r}, id_field={self.id_field!r})"
        )
