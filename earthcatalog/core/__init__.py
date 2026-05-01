from .catalog import get_or_create, info, ingest, open
from .catalog_info import CatalogInfo
from .earthcatalog import EarthCatalog

open_catalog = open
get_or_create_table = get_or_create

__all__ = [
    "CatalogInfo",
    "EarthCatalog",
    "get_or_create",
    "get_or_create_table",
    "info",
    "ingest",
    "open",
    "open_catalog",
]
