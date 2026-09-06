"""
Memory-bounded key diffing — the exact anti-join between an inventory and
the unified index.

Scale contract
--------------
* Index side: loaded **once per run** as a sorted array of 16-byte key
  hashes — 16 B/row, ~0.7 GB at 45M rows, ~3.2 GB at 200M (the documented
  in-RAM ceiling; past that, swap the backend, not the callers).
* Inventory side: consumed as a lazy ``(bucket, key)`` iterator in bounded
  batches — nothing ever materialises either side.

Membership is ``xxh3_128(s3_key, seed=42)``; the only way to get a false
"already known" is a 128-bit collision (~1e-22 odds at a billion keys).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

import numpy as np
import xxhash

_HASH_SEED = 42

# Two big-endian uint64 fields sort lexicographically, which for a
# big-endian 128-bit digest is exactly numeric order — so plain
# sort/searchsorted over this dtype is a correct 128-bit ordering.
KEY_HASH_DTYPE = np.dtype([("hi", ">u8"), ("lo", ">u8")])


def key_hash(s3_key: str) -> bytes:
    """Return the 16-byte xxh3_128 digest for *s3_key* (seed 42)."""
    return xxhash.xxh3_128(s3_key.encode("utf-8"), seed=_HASH_SEED).digest()


def pair_key(bucket: str, key: str) -> str:
    """The ``s3_key`` identity of an inventory pair (matches Index rows)."""
    return f"s3://{bucket}/{key}"


def hash_array(digests: Iterable[bytes]) -> np.ndarray:
    """Pack digests into a sorted, searchable :data:`KEY_HASH_DTYPE` array."""
    buf = bytearray()
    for d in digests:
        buf.extend(d)
    arr = np.frombuffer(bytes(buf), dtype=">u8").reshape(-1, 2).view(KEY_HASH_DTYPE)
    return np.sort(arr.ravel())


def contains(arr: np.ndarray, digest: bytes) -> bool:
    """Membership test for one digest in a sorted :data:`KEY_HASH_DTYPE` array."""
    if len(arr) == 0:
        return False
    q = np.frombuffer(digest, dtype=">u8").reshape(1, 2).view(KEY_HASH_DTYPE).ravel()
    pos = int(np.searchsorted(arr, q[0]))
    if pos >= len(arr):
        return False
    row = arr[pos]
    return row["hi"] == q[0]["hi"] and row["lo"] == q[0]["lo"]


def iter_new_keys(
    pairs: Iterable[tuple[str, str]],
    known: np.ndarray,
    batch_size: int = 10_000,
) -> Iterator[tuple[str, str]]:
    """Yield the ``(bucket, key)`` pairs from *pairs* not present in *known*.

    *pairs* is consumed lazily — one ``batch_size`` slice at a time — so a
    billion-record inventory streams through in O(batch) memory.  *known*
    is the sorted hash array from :meth:`earthcatalog.index.Index.known_key_hashes`.
    """
    if len(known) == 0:
        yield from pairs
        return

    batch: list[tuple[str, str]] = []
    for pair in pairs:
        batch.append(pair)
        if len(batch) >= batch_size:
            yield from _emit(batch, known)
            batch = []
    if batch:
        yield from _emit(batch, known)


def _emit(batch: list[tuple[str, str]], known: np.ndarray) -> Iterator[tuple[str, str]]:
    buf = bytearray()
    for bucket, key in batch:
        buf.extend(key_hash(pair_key(bucket, key)))
    q = np.frombuffer(bytes(buf), dtype=">u8").reshape(-1, 2).view(KEY_HASH_DTYPE).ravel()

    pos = np.searchsorted(known, q)
    np.clip(pos, 0, len(known) - 1, out=pos)
    hit = (known["hi"][pos] == q["hi"]) & (known["lo"][pos] == q["lo"])
    for (bucket, key), is_known in zip(batch, hit.tolist()):
        if not is_known:
            yield bucket, key
