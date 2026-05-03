"""
Hash index utilities for the earthcatalog warehouse.

The hash index is a single sorted Parquet file
(``warehouse_id_hashes.parquet``) that contains one ``id_hash``
column (``fixed_size_binary[16]``).  Each row is an xxh3_128 hash
of a STAC item ID, used for fast O(log n) membership tests to
avoid re-ingesting already-catalogued items.

Public API
----------
hash_id(item_id)
    Hash a single item ID → 16-byte digest.

read_hashes(store, key) → set[bytes]
    Read all hashes from the index into memory.

merge_hashes_from_parquets(new_parquet_paths, existing, store) → (set[bytes], int)
    Read ``id`` column from new warehouse parquet files (local or S3),
    hash each ID, merge into *existing* set.  Returns updated set and
    count of genuinely new hashes added.

write_hashes(hashes, store, key) → int
    Write sorted hash index back to the store.
"""

from __future__ import annotations

import io
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import xxhash

_HASH_SEED = 42
_BATCH_SIZE = 100_000


def hash_id(item_id: str) -> bytes:
    """Return the 16-byte xxh3_128 digest for *item_id*."""
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


def read_hashes(store: object, key: str) -> set[bytes]:
    """
    Load the hash index from *store* at *key* into a set.

    Returns an empty set if the file does not exist yet.
    """
    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return set()
    pf = pq.ParquetFile(io.BytesIO(raw))
    hashes: set[bytes] = set()
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash"]):
        for h in batch.column("id_hash").to_pylist():
            hashes.add(bytes(h))
    return hashes


def merge_hashes_from_parquets(
    new_parquet_paths: list[str],
    existing: set[bytes],
    store: object | None = None,
) -> tuple[set[bytes], int]:
    """
    Read the ``id`` column from each new warehouse parquet file,
    hash every ID, and union into *existing*.

    Handles both local paths and ``s3://`` URIs.  For S3 paths the
    caller must supply *store* (an obstore-compatible store pointing
    to the warehouse bucket — without key prefix).

    Returns ``(updated_set, n_new)`` where ``n_new`` is the count of
    hashes that were not already in *existing*.
    """
    n_new = 0
    for path in new_parquet_paths:
        if path.startswith("s3://"):
            if store is None:
                raise ValueError("store is required for s3:// paths")
            no_scheme = path.removeprefix("s3://")
            _bucket, _, key = no_scheme.partition("/")
            raw = bytes(obstore.get(store, key).bytes())
            pf = pq.ParquetFile(io.BytesIO(raw))
        else:
            pf = pq.ParquetFile(Path(path))

        for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id"]):
            for item_id in batch.column("id").to_pylist():
                if item_id is None:
                    continue
                h = hash_id(str(item_id))
                if h not in existing:
                    existing.add(h)
                    n_new += 1
    return existing, n_new


def write_hashes(hashes: set[bytes], store: object, key: str) -> int:
    """
    Write *hashes* as a sorted Parquet file to *store* at *key*.

    Uses zstd compression.  Returns the total number of hashes written.
    """
    sorted_hashes = sorted(hashes)
    arr = pa.array(sorted_hashes, type=pa.binary(16))
    tbl = pa.table({"id_hash": arr})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return len(sorted_hashes)
