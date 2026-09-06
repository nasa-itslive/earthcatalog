"""Tests for the keydiff anti-join (streaming membership against the index)."""

from __future__ import annotations

import numpy as np
import pytest
from obstore.store import MemoryStore

from earthcatalog.index import Index
from earthcatalog.keydiff import (
    KEY_HASH_DTYPE,
    contains,
    hash_array,
    iter_new_keys,
    key_hash,
    pair_key,
)


def _digests(s3_keys: list[str]) -> list[bytes]:
    return [key_hash(k) for k in s3_keys]


def test_key_hash_is_deterministic_xxh3_128():
    import xxhash

    h = key_hash("s3://bucket/a.stac.json")
    assert len(h) == 16
    assert h == xxhash.xxh3_128(b"s3://bucket/a.stac.json", seed=42).digest()
    assert h == key_hash("s3://bucket/a.stac.json")


def test_pair_key_matches_index_identity():
    assert pair_key("data-bucket", "x/y.stac.json") == "s3://data-bucket/x/y.stac.json"


def test_hash_array_sorts_numerically():
    keys = [f"s3://b/{i}.stac.json" for i in range(100)]
    arr = hash_array(_digests(keys))
    assert arr.dtype == KEY_HASH_DTYPE
    order = sorted(range(100), key=lambda i: int.from_bytes(key_hash(keys[i]), "big"))
    expected_hi = [int.from_bytes(key_hash(keys[i]), "big") >> 64 for i in order]
    assert arr["hi"].tolist() == expected_hi


def test_contains_roundtrip():
    keys = [f"s3://b/{i}.stac.json" for i in range(50)]
    arr = hash_array(_digests(keys))
    assert contains(arr, key_hash("s3://b/7.stac.json"))
    assert not contains(arr, key_hash("s3://b/missing.stac.json"))
    assert not contains(hash_array([]), key_hash("s3://b/0.stac.json"))


def test_iter_new_keys_matches_brute_force():
    known_keys = [f"s3://b/{i}.stac.json" for i in range(0, 100, 2)]
    pairs = [("b", f"{i}.stac.json") for i in range(100)]
    # batch_size coprime with the data size forces many partial batches.
    got = list(iter_new_keys(pairs, hash_array(_digests(known_keys)), batch_size=7))
    expected = [("b", f"{i}.stac.json") for i in range(1, 100, 2)]
    assert got == expected


def test_iter_new_keys_empty_known_yields_all():
    pairs = [("b", f"{i}.stac.json") for i in range(5)]
    assert list(iter_new_keys(pairs, np.empty(0, dtype=KEY_HASH_DTYPE))) == pairs


def test_iter_new_keys_is_lazy():
    """The inventory iterator is consumed one batch at a time — a generator
    that raises proves nothing materialises it up front."""
    produced = {"n": 0}

    def boom_after_four():
        for i in range(4):
            produced["n"] += 1
            yield ("b", f"{i}.stac.json")
        raise RuntimeError("inventory blew up")

    known = hash_array(_digests(["s3://b/unrelated.stac.json"]))
    it = iter_new_keys(boom_after_four(), known, batch_size=2)
    assert len(list(zip(range(2), it))) == 2
    assert produced["n"] == 2  # pulled exactly as much input as was consumed
    # Completing batch 2 pulls inputs 3 and 4.
    assert next(it) == ("b", "2.stac.json")
    assert next(it) == ("b", "3.stac.json")
    assert produced["n"] == 4
    with pytest.raises(RuntimeError):
        next(it)  # the pull past the last input raises — nothing was pre-buffered


def test_known_key_hashes_roundtrip_and_deleted_exclusion():
    store = MemoryStore()
    index = Index(store, "warehouse/index.parquet")
    index.append(
        [
            {"stac_id": "a", "s3_key": "s3://b/a.stac.json", "grid_partition": "cellA", "year": 2020},
            {"stac_id": "b", "s3_key": "s3://b/b.stac.json", "grid_partition": "cellA", "year": 2020},
            {"stac_id": "c", "s3_key": "s3://b/c.stac.json", "grid_partition": "cellA", "year": 2020},
        ]
    )
    index.mark_deleted({"b"})

    arr = index.known_key_hashes()
    assert contains(arr, key_hash("s3://b/a.stac.json"))
    assert contains(arr, key_hash("s3://b/c.stac.json"))
    # Soft-deleted rows are excluded so a GC'd-then-re-added item diffs as new.
    assert not contains(arr, key_hash("s3://b/b.stac.json"))


def test_known_key_hashes_empty_when_absent():
    index = Index(MemoryStore(), "warehouse/index.parquet")
    arr = index.known_key_hashes()
    assert len(arr) == 0
    assert arr.dtype == KEY_HASH_DTYPE


def test_index_fetched_once_per_array_build(monkeypatch):
    """The membership array is built with ONE index GET, streaming the
    parquet column — not one fetch per row."""
    store = MemoryStore()
    index = Index(store, "warehouse/index.parquet")
    index.append(
        [{"stac_id": str(i), "s3_key": f"s3://b/{i}.stac.json", "grid_partition": "c", "year": 2020} for i in range(300)]
    )

    import earthcatalog.index as index_mod

    calls = {"n": 0}
    real_get = index_mod.obstore.get

    def counting_get(*a, **kw):
        calls["n"] += 1
        return real_get(*a, **kw)

    monkeypatch.setattr(index_mod.obstore, "get", counting_get)
    arr = index.known_key_hashes()
    assert calls["n"] == 1
    assert len(arr) == 300


def test_ingester_reads_index_once_per_run(monkeypatch):
    """The serial run loads the membership array once — never per item."""
    from tests.test_ingest import _inventory, _make_item

    store = MemoryStore()
    index = Index(store, "warehouse/index.parquet")

    from earthcatalog.ingest import Ingester

    class _FakeTable:
        files: list[str] = []

        def add_files(self, paths):
            self.files.extend(paths)

    ing = Ingester(
        store=store,
        index=index,
        table=_FakeTable(),
        fetch_fn=lambda b, k: _make_item(k),
        stage="direct",
        warehouse_prefix="warehouse/",
        batch_size=10,
    )

    calls = {"n": 0}
    real = Index.known_key_hashes

    def counting(self):
        calls["n"] += 1
        return real(self)

    monkeypatch.setattr(Index, "known_key_hashes", counting)
    summary = ing.run(_inventory([f"{i}.stac.json" for i in range(25)]))
    assert summary["items"] == 25
    assert calls["n"] == 1
