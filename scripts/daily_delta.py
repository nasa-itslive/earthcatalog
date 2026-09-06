#!/usr/bin/env python3
"""
Daily delta producer for earthcatalog ingest.

Reads today's AWS S3 Inventory manifest, hashes all .stac.json item IDs,
compares against the warehouse unified index, and writes a delta parquet
containing (bucket, key) pairs for items not yet in the warehouse.

Accumulates unconsumed previous deltas so no items are lost if the ingest
pipeline doesn't run.

The inventory is streamed part-file by part-file and anti-joined against the
unified index with Arrow ``is_in`` — memory stays bounded by the batch size,
not the inventory size, so this runs comfortably on a CI runner even for a
multi-million-item inventory.

Supports both S3 (s3://) and local filesystem paths for --warehouse-hash
and --delta-prefix, enabling local testing without S3 credentials.

Usage
-----
    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json

    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json \
      --warehouse-hash /tmp/warehouse_index.parquet \
      --delta-prefix /tmp/delta

    # If delta already exists, skips inventory fetch entirely:
    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json \
      --warehouse-hash /tmp/warehouse_index.parquet \
      --delta-prefix /tmp/delta \
      --date 2026-04-27
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import xxhash
from obstore.store import S3Store
from tqdm import tqdm

_HASH_SEED = 42
_BATCH_SIZE = 100_000
_STAC_JSON_SUFFIX = ".stac.json"

_SCHEMA = pa.schema(
    [
        pa.field("bucket", pa.string()),
        pa.field("key", pa.string()),
        pa.field("id_hash", pa.binary(16)),
    ]
)


def _hash_id(item_id: str) -> bytes:
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


def _is_local(uri: str) -> bool:
    return not uri.startswith("s3://")


def _get_store(bucket: str, prefix: str = "") -> S3Store:
    import configparser

    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")

    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token

    kwargs: dict = {"bucket": bucket, "region": region}
    if prefix:
        kwargs["prefix"] = prefix
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    path = uri.removeprefix("s3://")
    bucket, _, key = path.partition("/")
    return bucket, key


def _fetch_manifest(store: S3Store, key: str) -> dict:
    raw = bytes(obstore.get(store, key).bytes())
    return json.loads(raw)


def _iter_inventory_batches(
    manifest: dict, store: S3Store
) -> Iterator[tuple[pa.Array, pa.Array, pa.Array]]:
    """Yield ``(buckets, keys, id_hashes)`` Arrow arrays for .stac.json rows.

    Streams each inventory part file in batches, filtering to .stac.json keys
    and hashing the item ID (last path segment without the suffix) with
    xxh3_128.  Never materialises more than one batch at a time.
    """
    for f in manifest.get("files", []):
        dk = f["key"]
        try:
            raw = bytes(obstore.get(store, dk).bytes())
        except Exception:
            print(f"WARN: could not read {dk}, skipping", file=sys.stderr)
            continue

        pf = pq.ParquetFile(io.BytesIO(raw))
        for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["bucket", "key"]):
            keys = batch.column("key")
            mask = pc.ends_with(keys, pattern=_STAC_JSON_SUFFIX)
            if not pc.any(mask).as_py():
                continue
            buckets = batch.column("bucket").filter(mask)
            keys = keys.filter(mask)
            keys_py = keys.to_pylist()
            hashes = pa.array(
                [_hash_id(k.rsplit("/", 1)[-1].removesuffix(_STAC_JSON_SUFFIX)) for k in keys_py],
                type=pa.binary(16),
            )
            yield buckets, keys, hashes


def _load_index_hash_array(uri: str) -> pa.Array:
    """Load the active (non-deleted) ``id_hash`` values from the unified Index.

    Returns a unique Arrow array of 16-byte hashes — used directly as the
    ``value_set`` for the streaming anti-join, so the index is never expanded
    into a Python ``set``.
    """
    from obstore.store import LocalStore

    if uri.startswith("s3://"):
        bucket, key = _parse_s3_uri(uri)
        store = _get_store(bucket)
    else:
        p = Path(uri)
        store = LocalStore(str(p.parent))
        key = p.name

    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return pa.array([], type=pa.binary(16))
    pf = pq.ParquetFile(io.BytesIO(raw))
    chunks = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash", "deleted"]):
        h = batch.column("id_hash")
        d = batch.column("deleted")
        chunks.append(h.filter(pc.invert(d)))

    if not chunks:
        return pa.array([], type=pa.binary(16))
    return pc.unique(pa.chunked_array(chunks))


# ---------------------------------------------------------------------------
# S3 I/O
# ---------------------------------------------------------------------------


def _list_pending_deltas(store: S3Store, prefix: str) -> list[str]:
    list_prefix = f"{prefix}/pending/" if prefix else "pending/"
    keys: list[str] = []
    for batch in obstore.list(store, prefix=list_prefix):
        for obj in batch:
            k: str = obj["path"]
            base = k.rsplit("/", 1)[-1] if "/" in k else k
            if base.startswith("delta_") and k.endswith(".parquet"):
                keys.append(k)
    return sorted(keys)


def _read_pending_delta(store: S3Store, key: str) -> list[tuple[str, str, bytes]]:
    raw = bytes(obstore.get(store, key).bytes())
    pf = pq.ParquetFile(io.BytesIO(raw))
    rows: list[tuple[str, str, bytes]] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["bucket", "key", "id_hash"]):
        buckets = batch.column("bucket").to_pylist()
        keys = batch.column("key").to_pylist()
        id_hashes = batch.column("id_hash").to_pylist()
        rows.extend(zip(buckets, keys, id_hashes))
    return rows


def _write_delta_parquet(rows: list[tuple[str, str, bytes]], store: S3Store, key: str) -> int:
    if not rows:
        return 0
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return len(rows)


# ---------------------------------------------------------------------------
# Local I/O
# ---------------------------------------------------------------------------


def _list_pending_deltas_local(prefix: str) -> list[str]:
    pending_dir = Path(prefix) / "pending"
    if not pending_dir.exists():
        return []
    return sorted(str(p) for p in pending_dir.glob("delta_*.parquet"))


def _read_pending_delta_local(path: str) -> list[tuple[str, str, bytes]]:
    pf = pq.ParquetFile(path)
    rows: list[tuple[str, str, bytes]] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["bucket", "key", "id_hash"]):
        buckets = batch.column("bucket").to_pylist()
        keys = batch.column("key").to_pylist()
        id_hashes = batch.column("id_hash").to_pylist()
        rows.extend(zip(buckets, keys, id_hashes))
    return rows


def _write_delta_parquet_local(rows: list[tuple[str, str, bytes]], path: str) -> int:
    if not rows:
        return 0
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    pq.write_table(tbl, path, compression="zstd")
    return len(rows)


def _write_inventory_cache_local(rows: list[tuple[str, str, bytes]], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    pq.write_table(tbl, path, compression="zstd")


def _read_inventory_cache_local(path: str) -> tuple[list[tuple[str, str]], list[bytes]]:
    pf = pq.ParquetFile(path)
    pairs: list[tuple[str, str]] = []
    hashes: list[bytes] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["bucket", "key", "id_hash"]):
        buckets = batch.column("bucket").to_pylist()
        keys = batch.column("key").to_pylist()
        id_hashes = batch.column("id_hash").to_pylist()
        pairs.extend(zip(buckets, keys))
        hashes.extend(id_hashes)
    return pairs, hashes


def run_daily_delta(
    manifest_uri: str,
    warehouse_hash_uri: str,
    delta_prefix: str,
    date_str: str | None = None,
) -> dict[str, Any]:
    if date_str is None:
        date_str = datetime.now(UTC).strftime("%Y-%m-%d")

    local_delta = _is_local(delta_prefix)
    delta_output = f"{delta_prefix}/pending/delta_{date_str}.parquet"
    inventory_cache = f"{delta_prefix}/pending/inventory_{date_str}.parquet"

    # Tier 1: delta already exists — skip everything
    if local_delta and Path(delta_output).exists():
        print(f"Delta already exists: {delta_output}")
        print("Skipping inventory fetch, hash download, and anti-join.")
        rows = _read_pending_delta_local(delta_output)
        print(f"Existing delta: {len(rows):,} items")
        return {
            "date": date_str,
            "skipped": True,
            "inventory_cached": False,
            "inventory_items": 0,
            "warehouse_hashes": 0,
            "new_items": 0,
            "previous_deltas_merged": 0,
            "accumulated_total": len(rows),
            "delta_key": delta_output,
        }

    # Load the warehouse's active id_hash set once (Arrow array, not a set).
    wh_hashes = _load_index_hash_array(warehouse_hash_uri)
    print(f"Warehouse index: {len(wh_hashes):,} active hashes")

    inventory_cached = False
    inventory_items = 0
    new_items = 0
    new_rows: dict[bytes, tuple[str, str, bytes]] = {}

    def _absorb(buckets: pa.Array, keys: pa.Array, hashes: pa.Array) -> None:
        """Anti-join one batch against the warehouse; keep new rows."""
        nonlocal new_items
        new_mask = pc.invert(pc.is_in(hashes, value_set=wh_hashes))
        if not pc.any(new_mask).as_py():
            return
        for b, k, h in zip(
            buckets.filter(new_mask).to_pylist(),
            keys.filter(new_mask).to_pylist(),
            hashes.filter(new_mask).to_pylist(),
        ):
            new_items += 1
            new_rows.setdefault(h, (b, k, h))

    if local_delta:
        cache_path = Path(inventory_cache)
        if cache_path.exists():
            # Tier 2: reuse the inventory cache (streamed anti-join).
            inventory_cached = True
            print(f"Inventory cache found: {inventory_cache}")
            pf = pq.ParquetFile(str(cache_path))
            with tqdm(desc="Anti-join", unit=" rows") as pbar:
                for batch in pf.iter_batches(batch_size=_BATCH_SIZE):
                    inventory_items += batch.num_rows
                    _absorb(batch.column("bucket"), batch.column("key"), batch.column("id_hash"))
                    pbar.update(batch.num_rows)
        else:
            manifest_bucket, manifest_key = _parse_s3_uri(manifest_uri)
            store = _get_store(manifest_bucket)
            print(f"Fetching manifest: {manifest_uri}")
            manifest = _fetch_manifest(store, manifest_key)
            print(f"Manifest: {len(manifest.get('files', []))} data file(s)")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with pq.ParquetWriter(str(cache_path), _SCHEMA, compression="zstd") as w:
                with tqdm(desc="Scatter", unit=" rows") as pbar:
                    for buckets, keys, hashes in _iter_inventory_batches(manifest, store):
                        inventory_items += len(buckets)
                        w.write_table(pa.Table.from_arrays([buckets, keys, hashes], schema=_SCHEMA))
                        _absorb(buckets, keys, hashes)
                        pbar.update(len(buckets))
            print(f"Wrote inventory cache: {inventory_cache}")
    else:
        delta_bucket, delta_path = _parse_s3_uri(delta_prefix)
        delta_store = _get_store(delta_bucket, prefix=delta_path)
        manifest_bucket, manifest_key = _parse_s3_uri(manifest_uri)
        store = _get_store(manifest_bucket)
        print(f"Fetching manifest: {manifest_uri}")
        manifest = _fetch_manifest(store, manifest_key)
        print(f"Manifest: {len(manifest.get('files', []))} data file(s)")

        # Stream the inventory: write the cache to a local temp file (so the
        # inventory is never held in RAM), anti-join, then upload the cache.
        fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        try:
            with pq.ParquetWriter(tmp_path, _SCHEMA, compression="zstd") as w:
                with tqdm(desc="Scatter", unit=" rows") as pbar:
                    for buckets, keys, hashes in _iter_inventory_batches(manifest, store):
                        inventory_items += len(buckets)
                        w.write_table(pa.Table.from_arrays([buckets, keys, hashes], schema=_SCHEMA))
                        _absorb(buckets, keys, hashes)
                        pbar.update(len(buckets))
            inv_key = f"pending/inventory_{date_str}.parquet"
            obstore.put(delta_store, inv_key, tmp_path)
            print(f"Wrote inventory cache: {delta_prefix.rstrip('/')}/{inv_key}")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    print(f"Inventory: {inventory_items:,} .stac.json items")
    print(f"New items (not in warehouse): {new_items:,}")

    # Merge unconsumed previous deltas so nothing is lost.
    print("Checking for pending deltas ...")
    if local_delta:
        pending_keys = _list_pending_deltas_local(delta_prefix)
    else:
        pending_keys = _list_pending_deltas(delta_store, "")

    for pk in pending_keys:
        print(f"  Merging previous delta: {pk}")
        rows = (
            _read_pending_delta_local(pk) if local_delta else _read_pending_delta(delta_store, pk)
        )
        for b, k, h in rows:
            if h not in new_rows:
                new_rows[h] = (b, k, h)

    final_rows = list(new_rows.values())
    print(f"Accumulated delta: {len(final_rows):,} items")

    if local_delta:
        n = _write_delta_parquet_local(final_rows, delta_output)
        print(f"Written: {delta_output} ({n:,} rows)")
    else:
        delta_key = f"pending/delta_{date_str}.parquet"
        n = _write_delta_parquet(final_rows, delta_store, delta_key)
        print(f"Written: {delta_prefix.rstrip('/')}/{delta_key} ({n:,} rows)")

    for pk in pending_keys:
        try:
            if local_delta:
                Path(pk).unlink()
            else:
                obstore.delete(delta_store, pk)
            print(f"  Deleted old delta: {pk}")
        except Exception:
            pass

    return {
        "date": date_str,
        "skipped": False,
        "inventory_cached": inventory_cached,
        "inventory_items": inventory_items,
        "warehouse_hashes": len(wh_hashes),
        "new_items": new_items,
        "previous_deltas_merged": len(pending_keys),
        "accumulated_total": len(final_rows),
        "delta_key": delta_output
        if local_delta
        else f"{delta_prefix.rstrip('/')}/pending/delta_{date_str}.parquet",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce daily delta for earthcatalog")
    parser.add_argument("manifest", help="s3:// URI to today's manifest.json")
    parser.add_argument(
        "--warehouse-hash",
        default="s3://its-live-data/test-space/stac/catalog/warehouse_index.parquet",
        help="s3:// URI or local path to the unified warehouse index parquet",
    )
    parser.add_argument(
        "--delta-prefix",
        default="s3://its-live-data/test-space/stac/catalog/delta",
        help="s3:// URI prefix or local directory for delta files",
    )
    parser.add_argument("--date", help="Date string (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    result = run_daily_delta(
        manifest_uri=args.manifest,
        warehouse_hash_uri=args.warehouse_hash,
        delta_prefix=args.delta_prefix,
        date_str=args.date,
    )
    print()
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
