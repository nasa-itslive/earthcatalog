"""
catalog_info — discover grid system metadata from an open Iceberg table.

Grid type, resolution, and related parameters are stored as Iceberg table
properties at ingest time (see :func:`~earthcatalog.core.catalog.get_or_create_table`).

Example::

    from earthcatalog.core import catalog_info, open_catalog, get_or_create_table
    from shapely.geometry import box

    catalog = open_catalog(db_path="catalog.db", warehouse_path="warehouse/")
    table   = get_or_create_table(catalog)
    info    = catalog_info(table)

    bbox  = box(-60, 60, -30, 80)          # Greenland
    paths = info.file_paths(table, bbox)    # Iceberg-pruned file list
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

    Construct via :func:`catalog_info(table)` — not directly.
    """

    grid_type: str
    grid_resolution: int | None
    boundaries_path: str | None
    id_field: str | None

    def cells_for_geometry(self, geom) -> list[str]:
        """Return the partition keys that intersect *geom*."""
        if self.grid_type == "h3":
            return self._h3_cells(geom)
        if self.grid_type == "geojson":
            return self._geojson_keys(geom)
        raise ValueError(f"Unknown grid type: {self.grid_type!r}")

    def file_paths(self, table, geom) -> list[str]:
        """Return Parquet file paths for partitions intersecting *geom*.

        Uses Iceberg partition pruning (zero I/O on irrelevant files) and
        returns paths suitable for DuckDB's ``read_parquet()``.
        """
        from pyiceberg.expressions import In

        cells = self.cells_for_geometry(geom)
        if not cells:
            return []
        scan = table.scan(row_filter=In("grid_partition", cells))
        return [task.file.file_path for task in scan.plan_files()]

    def stats(self, table) -> list[dict]:
        """Return per-partition row counts and file sizes from Iceberg metadata.

        Reads manifest files only — no Parquet data is opened.  Each dict
        contains ``grid_partition``, ``year``, ``row_count``, ``file_count``,
        and ``total_bytes`` aggregated across all files in that partition.

        Example::

            info = catalog_info(table)
            for s in info.stats(table):
                print(s)
            # {'grid_partition': '8206d7fffffffff', 'year': 2022,
            #  'row_count': 1500, 'file_count': 1, 'total_bytes': 32000}
        """
        from collections import defaultdict

        agg: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0, 0])
        for task in table.scan().plan_files():
            f = task.file
            cell = f.partition[0]
            year = f.partition[1] + 1970
            recs = f.record_count
            size = f.file_size_in_bytes
            key = (cell, year)
            agg[key][0] += recs
            agg[key][1] += 1
            agg[key][2] += size

        return [
            {
                "grid_partition": cell,
                "year": year,
                "row_count": rows,
                "file_count": files,
                "total_bytes": bytes_,
            }
            for (cell, year), (rows, files, bytes_) in sorted(agg.items())
        ]

    def cell_list_sql(self, geom) -> str:
        """Return a SQL fragment suitable for ``WHERE grid_partition IN (...)``."""
        cells = self.cells_for_geometry(geom)
        if not cells:
            return "grid_partition IN (NULL)"
        quoted = ", ".join(f"'{c}'" for c in cells)
        return f"grid_partition IN ({quoted})"

    def _h3_cells(self, geom) -> list[str]:
        import h3
        from shapely.geometry import Point

        res = self.grid_resolution if self.grid_resolution is not None else 1
        if isinstance(geom, Point):
            return [h3.latlng_to_cell(geom.y, geom.x, res)]
        interior = set(h3.geo_to_cells(mapping(geom), res))
        boundary = self._h3_boundary_cells(geom, res)
        return list(interior | boundary)

    @staticmethod
    def _h3_boundary_cells(geom, resolution: int) -> set[str]:
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
        if not self.boundaries_path:
            raise ValueError(
                "boundaries_path is required for geojson grid type. "
                "Re-ingest with a GridConfig that specifies boundaries_path."
            )
        from shapely import wkb

        from earthcatalog.grids.geojson_partitioner import GeoJSONPartitioner

        partitioner = GeoJSONPartitioner(
            boundaries_path=self.boundaries_path,
            id_field=self.id_field or "id",
        )
        return partitioner.get_intersecting_keys(wkb.dumps(geom))

    def __repr__(self) -> str:  # pragma: no cover
        if self.grid_type == "h3":
            return f"CatalogInfo(grid_type='h3', resolution={self.grid_resolution})"
        return (
            f"CatalogInfo(grid_type='geojson', "
            f"boundaries_path={self.boundaries_path!r}, id_field={self.id_field!r})"
        )


def catalog_info(table) -> CatalogInfo:
    """Build a CatalogInfo from an open PyIceberg Table.

    Falls back to sensible defaults for catalogs that predate table-property
    support (grid_type="h3", grid_resolution=1).
    """
    props = table.properties
    grid_type = props.get(PROP_GRID_TYPE, "h3")
    raw_res = props.get(PROP_GRID_RESOLUTION)
    grid_resolution = int(raw_res) if raw_res is not None else (1 if grid_type == "h3" else None)
    return CatalogInfo(
        grid_type=grid_type,
        grid_resolution=grid_resolution,
        boundaries_path=props.get(PROP_GRID_BOUNDARIES_PATH),
        id_field=props.get(PROP_GRID_ID_FIELD),
    )
