"""
GeoJSON partitioner using an R-tree (STRtree) for fast intersection lookups.

Supports arbitrary polygon boundaries (drainage basins, polar regions, etc.).
The boundaries file can be a local path or an s3:// URI; both are read via
obstore so there is no dependency on requests/s3fs/boto3.
"""

from __future__ import annotations

import json
from pathlib import Path

import obstore
from obstore.store import LocalStore, S3Store
from shapely import wkb
from shapely.geometry import shape
from shapely.strtree import STRtree

from earthcatalog.core.partitioner import AbstractPartitioner


def _load_bytes(path: str) -> bytes:
    """
    Read a file as bytes using obstore.

    Supports:
      - Local paths  : "/path/to/file.geojson"
      - S3 URIs      : "s3://bucket/path/to/file.geojson"
    """
    if path.startswith("s3://"):
        no_prefix = path.removeprefix("s3://")
        bucket, key = no_prefix.split("/", 1)
        store = S3Store(bucket=bucket, skip_signature=True)
        return bytes(obstore.get(store, key).bytes())
    else:
        # Local file — use LocalStore so all I/O stays within obstore
        resolved = Path(path).resolve()
        store = LocalStore(str(resolved.parent))
        return bytes(obstore.get(store, resolved.name).bytes())


class GeoJSONPartitioner(AbstractPartitioner):
    """
    Partitions geometries against a set of named polygon boundaries loaded
    from a GeoJSON FeatureCollection.  Uses an STRtree for O(log n) lookups.

    Parameters
    ----------
    boundaries_path:
        Local path or ``s3://`` URI to a GeoJSON FeatureCollection.
    id_field:
        The GeoJSON feature property to use as the partition key string.
    """

    def __init__(self, boundaries_path: str, id_field: str = "id") -> None:
        raw = _load_bytes(boundaries_path)
        features = json.loads(raw)["features"]

        self._geometries = [shape(feat["geometry"]) for feat in features]
        self._keys = [str(feat["properties"][id_field]) for feat in features]
        self._tree = STRtree(self._geometries)

    def get_intersecting_keys(self, geom_wkb: bytes) -> list[str]:
        geom = wkb.loads(geom_wkb)
        candidate_idxs = self._tree.query(geom, predicate="intersects")
        return [self._keys[i] for i in candidate_idxs]

    def key_to_wkt(self, key: str) -> str:
        idx = self._keys.index(key)
        return self._geometries[idx].wkt
