"""
Tests for earthcatalog.core.lock — S3Lock using MemoryStore.

All tests run fully offline with no S3 credentials needed.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.core.lock import CatalogLocked, S3Lock


@pytest.fixture(autouse=True)
def memory_store():
    """Point store_config at a fresh MemoryStore for every test."""
    store = MemoryStore()
    store_config.set_store(store)
    store_config.set_lock_key(".lock")
    yield store
    # Reset to default after each test
    store_config.set_lock_key(".lock")


class TestS3LockAcquireRelease:
    def test_acquire_and_release(self, memory_store):
        lock = S3Lock(owner="test")
        lock.acquire()
        import obstore

        # Lock file must exist after acquire
        result = obstore.get(memory_store, ".lock")
        data = json.loads(bytes(result.bytes()))
        assert data["owner"] == "test"

        lock.release()
        # Lock file must be gone after release
        with pytest.raises(FileNotFoundError):
            obstore.get(memory_store, ".lock")

    def test_context_manager_releases_on_exit(self, memory_store):
        import obstore

        with S3Lock(owner="ctx"):
            pass
        with pytest.raises(FileNotFoundError):
            obstore.get(memory_store, ".lock")

    def test_context_manager_releases_on_exception(self, memory_store):
        import obstore

        with pytest.raises(ValueError):
            with S3Lock(owner="ctx"):
                raise ValueError("boom")
        with pytest.raises(FileNotFoundError):
            obstore.get(memory_store, ".lock")

    def test_double_acquire_raises_catalog_locked(self, memory_store):
        lock_a = S3Lock(owner="job-a")
        lock_b = S3Lock(owner="job-b")
        lock_a.acquire()
        with pytest.raises(CatalogLocked, match="job-a"):
            lock_b.acquire()

    def test_release_when_not_held_is_silent(self, memory_store):
        lock = S3Lock(owner="test")
        lock.release()  # should not raise

    def test_payload_contains_expected_fields(self, memory_store):
        import obstore

        lock = S3Lock(owner="payload-test", ttl_hours=6)
        lock.acquire()
        data = json.loads(bytes(obstore.get(memory_store, ".lock").bytes()))
        assert data["owner"] == "payload-test"
        assert data["ttl_hours"] == 6
        assert "pid" in data
        assert "hostname" in data
        assert "acquired" in data


class TestS3LockStaleLockOverride:
    def test_stale_lock_is_overridden(self, memory_store):
        """A lock older than ttl_hours must be replaced by the next acquirer."""
        import obstore

        # Plant a stale lock manually
        stale_time = (datetime.now(UTC) - timedelta(hours=13)).isoformat()
        stale_payload = json.dumps(
            {
                "owner": "old-job",
                "pid": 9999,
                "hostname": "old-host",
                "acquired": stale_time,
                "ttl_hours": 12,
            }
        ).encode()
        obstore.put(memory_store, ".lock", stale_payload)

        # New acquirer should override without raising
        lock = S3Lock(owner="new-job", ttl_hours=12)
        lock.acquire()  # must not raise

        data = json.loads(bytes(obstore.get(memory_store, ".lock").bytes()))
        assert data["owner"] == "new-job"

    def test_fresh_lock_is_not_overridden(self, memory_store):
        """A lock within TTL must block the second acquirer."""
        import obstore

        fresh_payload = json.dumps(
            {
                "owner": "active-job",
                "pid": 1,
                "hostname": "host",
                "acquired": datetime.now(UTC).isoformat(),
                "ttl_hours": 12,
            }
        ).encode()
        obstore.put(memory_store, ".lock", fresh_payload)

        with pytest.raises(CatalogLocked):
            S3Lock(owner="interloper", ttl_hours=12).acquire()
