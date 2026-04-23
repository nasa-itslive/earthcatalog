"""
Tests for earthcatalog.core.catalog — download/upload using MemoryStore.
"""

import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.catalog import download_catalog, upload_catalog


@pytest.fixture(autouse=True)
def memory_store(tmp_path):
    store = MemoryStore()
    store_config.set_store(store)
    store_config.set_catalog_key("catalog.db")
    yield store, tmp_path


class TestCatalogLifecycle:
    def test_download_missing_key_is_silent(self, memory_store):
        """First run: no catalog.db on store yet — should not raise."""
        _, tmp = memory_store
        download_catalog(str(tmp / "catalog.db"))  # must not raise

    def test_upload_then_download_roundtrip(self, memory_store):
        store, tmp = memory_store
        local = tmp / "catalog.db"
        local.write_bytes(b"fake-sqlite-content")

        upload_catalog(str(local))

        dest = tmp / "catalog_restored.db"
        download_catalog(str(dest))

        assert dest.read_bytes() == b"fake-sqlite-content"

    def test_upload_overwrites(self, memory_store):
        store, tmp = memory_store
        local = tmp / "catalog.db"

        local.write_bytes(b"version-1")
        upload_catalog(str(local))

        local.write_bytes(b"version-2")
        upload_catalog(str(local))

        dest = tmp / "restored.db"
        download_catalog(str(dest))
        assert dest.read_bytes() == b"version-2"
