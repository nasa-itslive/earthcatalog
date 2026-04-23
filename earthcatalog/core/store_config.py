"""
Global store configuration for earthcatalog.

Defaults to a LocalStore rooted at /tmp/earthcatalog_store for zero-config
local development and testing. Override before running any job:

    from earthcatalog import store_config
    from obstore.store import S3Store

    store_config.set_store(S3Store(bucket="my-bucket", region="us-west-2"))
    store_config.set_catalog_key("catalog/catalog.db")
    store_config.set_lock_key("catalog/.lock")
"""

from pathlib import Path

from obstore.store import LocalStore

# Default paths used with the LocalStore
_DEFAULT_ROOT = "/tmp/earthcatalog_store"

Path(_DEFAULT_ROOT).mkdir(parents=True, exist_ok=True)
_store = LocalStore(_DEFAULT_ROOT)
_catalog_key: str = "catalog.db"
_lock_key: str = ".lock"


def get_store() -> object:
    """Return the active obstore-compatible store."""
    return _store


def get_catalog_key() -> str:
    return _catalog_key


def get_lock_key() -> str:
    return _lock_key


def set_store(store: object) -> None:
    """Override the store backend (e.g. S3Store for production)."""
    global _store
    _store = store


def set_catalog_key(key: str) -> None:
    global _catalog_key
    _catalog_key = key


def set_lock_key(key: str) -> None:
    global _lock_key
    _lock_key = key
