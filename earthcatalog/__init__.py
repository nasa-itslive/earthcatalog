"""EarthCatalog — spatially-partitioned STAC ingest pipeline backed by Apache Iceberg.

Top-level API
-------------

::

    import earthcatalog as ea

    ec = ea.open(store=store, base="s3://bucket/catalog")
    ec.ingest("delta.parquet", mode="delta")
    ec.bulk_ingest("full.parquet", create_client=coiled.Client)
    paths = ec.search_files(geometry)
    stats = ec.stats()
"""

from earthcatalog.core import EarthCatalog, ingest, open

__all__ = [
    "EarthCatalog",
    "ingest",
    "open",
]
