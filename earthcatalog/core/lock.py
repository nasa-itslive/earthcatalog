"""
S3 atomic lockfile using conditional writes (If-None-Match: *).

Prevents concurrent writes to the SQLite catalog.db.

Uses the store configured in earthcatalog.store_config (defaults to
LocalStore for zero-config local development and testing). Override
the store before running a job:

    from earthcatalog import store_config
    from obstore.store import S3Store

    store_config.set_store(S3Store(bucket="my-bucket", region="us-west-2"))
    store_config.set_lock_key("catalog/.lock")

Usage:
    from earthcatalog.lock import S3Lock

    with S3Lock(owner="incremental"):
        download_catalog(...)
        ... do work ...
        upload_catalog(...)
"""

import json
import os
import socket
from datetime import UTC, datetime, timedelta

import obstore
from obstore.exceptions import AlreadyExistsError

from . import store_config


class CatalogLocked(RuntimeError):
    """Raised when the lock is held by another process."""


class S3Lock:
    """
    Atomic lockfile using obstore conditional writes (If-None-Match: *).

    Reads the store and lock key from store_config at acquire time, so
    changing the store before entering the context is all that's needed
    to switch backends (S3, local, memory).

    Stale locks (older than ttl_hours) are automatically overridden.
    """

    def __init__(self, owner: str, ttl_hours: int = 12) -> None:
        """
        Args:
            owner:     Human-readable name for the lock holder (e.g. "backfill").
            ttl_hours: Age after which a lock is considered stale and overridable.
        """
        self._owner = owner
        self._ttl = ttl_hours

    def __enter__(self) -> "S3Lock":
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()

    @property
    def _store(self) -> object:
        return store_config.get_store()

    @property
    def _key(self) -> str:
        return store_config.get_lock_key()

    def acquire(self) -> None:
        """
        Atomically acquire the lock via mode='create' (If-None-Match: *).

        Succeeds only if the key does not exist. On conflict, reads the
        existing lock; if stale, deletes and retries. Raises CatalogLocked
        if a fresh lock is held by another process.
        """
        payload = self._make_payload()

        try:
            obstore.put(self._store, self._key, payload, mode="create")
            print(f"Lock acquired by '{self._owner}'.")
            return
        except AlreadyExistsError:
            pass

        # Key exists — read it to decide what to do
        existing = self._read_lock()
        if existing is None:
            # Disappeared between our failed PUT and this GET — retry once
            obstore.put(self._store, self._key, payload, mode="create")
            print(f"Lock acquired by '{self._owner}' (second attempt).")
            return

        acquired_at = datetime.fromisoformat(existing["acquired"])
        age = datetime.now(UTC) - acquired_at
        ttl = timedelta(hours=existing.get("ttl_hours", self._ttl))

        if age >= ttl:
            print(
                f"WARNING: Overriding stale lock from '{existing['owner']}' "
                f"on {existing['hostname']} (age: {age}, TTL: {ttl})."
            )
            obstore.delete(self._store, self._key)
            obstore.put(self._store, self._key, payload, mode="create")
            print(f"Lock acquired by '{self._owner}' (after stale override).")
            return

        raise CatalogLocked(
            f"Catalog is locked by '{existing['owner']}' on "
            f"{existing['hostname']} since {existing['acquired']}. "
            f"Lock expires in {ttl - age}."
        )

    def release(self) -> None:
        try:
            obstore.delete(self._store, self._key)
            print(f"Lock released by '{self._owner}'.")
        except Exception:
            pass  # already gone — fine

    def _make_payload(self) -> bytes:
        return json.dumps(
            {
                "owner": self._owner,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "acquired": datetime.now(UTC).isoformat(),
                "ttl_hours": self._ttl,
            }
        ).encode()

    def _read_lock(self) -> dict | None:
        try:
            result = obstore.get(self._store, self._key)
            return json.loads(bytes(result.bytes()))
        except FileNotFoundError:
            return None
