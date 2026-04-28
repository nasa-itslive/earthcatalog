from .catalog import get_or_create, info, open
from .catalog_info import CatalogInfo

open_catalog = open
get_or_create_table = get_or_create

__all__ = ["CatalogInfo", "get_or_create", "get_or_create_table", "info", "open", "open_catalog"]
