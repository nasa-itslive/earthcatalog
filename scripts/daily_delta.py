#!/usr/bin/env python3
"""
Daily delta producer for earthcatalog backfill.

Reads today's AWS S3 Inventory manifest, hashes all .stac.json item IDs,
compares against the warehouse hash index, and writes a delta parquet
containing (bucket, key) pairs for items not yet in the warehouse.

Accumulates unconsumed previous deltas so no items are lost if the ingest
pipeline doesn't run.

Supports both S3 (s3://) and local filesystem paths for --warehouse-hash
and --delta-prefix, enabling local testing without S3 credentials.

Usage
-----
    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json

    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json \
      --warehouse-hash /tmp/warehouse_id_hashes.parquet \
      --delta-prefix /tmp/delta

    # If delta already exists, skips inventory fetch entirely:
    python scripts/daily_delta.py \
      s3://log-bucket/inventory/.../2026-04-28T01-00Z/manifest.json \
      --warehouse-hash /tmp/warehouse_id_hashes.parquet \
      --delta-prefix /tmp/delta \
      --date 2026-04-27
"""

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import xxhash
from obstore.store import S3Store

_HASH_SEED = 42
_BATCH_SIZE = 100_000
_STAC_JSON_SUFFIX = ".stac.json"


def _hash_id(item_id: str) -> bytes:
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


def _is_local(uri: str) -> bool:
    return not uri.startswith("s3://")


def _get_store(bucket: str, prefix: str = "") -> S3Store:
    import configparser
    import os

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


def _stream_inventory_hashes(
    manifest: dict, store: S3Store
) -> tuple[list[tuple[str, str]], list[bytes]]:
    """
    Stream all .stac.json rows from manifest parquets, returning
    (bucket, key) pairs and their xxh3_128 hashes.
    """
    data_keys = [f["key"] for f in manifest.get("files", [])]
    pairs: list[tuple[str, str]] = []
    hashes: list[bytes] = []

    for dk in data_keys:
        try:
            raw = bytes(obstore.get(store, dk).bytes())
        except Exception:
            print(f"WARN: could not read {dk}, skipping", file=sys.stderr)
            continue

        pf = pq.ParquetFile(io.BytesIO(raw))
        for batch in pf.iter_batches(
            batch_size=_BATCH_SIZE, columns=["bucket", "key"]
        ):
            buckets = batch.column("bucket").to_pylist()
            keys = batch.column("key").to_pylist()
            for b, k in zip(buckets, keys):
                if not k.endswith(_STAC_JSON_SUFFIX):
                    continue
                fname = k.rsplit("/", 1)[-1]
                item_id = fname.removesuffix(_STAC_JSON_SUFFIX)
                pairs.append((b, k))
                hashes.append(_hash_id(item_id))

    return pairs, hashes


# ---------------------------------------------------------------------------
# S3 I/O
# ---------------------------------------------------------------------------


def _download_hash_index(store: S3Store, key: str) -> list[bytes]:
    raw = bytes(obstore.get(store, key).bytes())
    pf = pq.ParquetFile(io.BytesIO(raw))
    hashes: list[bytes] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash"]):
        hashes.extend(batch.column("id_hash").to_pylist())
    return hashes


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


def _write_delta_parquet(
    rows: list[tuple[str, str, bytes]], store: S3Store, key: str
) -> int:
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


def _download_hash_index_local(path: str) -> list[bytes]:
    pf = pq.ParquetFile(path)
    hashes: list[bytes] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash"]):
        hashes.extend(batch.column("id_hash").to_pylist())
    return hashes


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


def _write_delta_parquet_local(
    rows: list[tuple[str, str, bytes]], path: str
) -> int:
    if not rows:
        return 0
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    pq.write_table(tbl, path, compression="zstd")
    return len(rows)


def _build_hash_set(hashes: list[bytes]) -> set[bytes]:
    return set(hashes)


def _write_inventory_cache(
    rows: list[tuple[str, str, bytes]], store: S3Store, key: str
) -> None:
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())


def _write_inventory_cache_local(
    rows: list[tuple[str, str, bytes]], path: str
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    buckets = pa.array([r[0] for r in rows], type=pa.string())
    keys = pa.array([r[1] for r in rows], type=pa.string())
    id_hashes = pa.array([r[2] for r in rows], type=pa.binary(16))
    tbl = pa.table({"bucket": buckets, "key": keys, "id_hash": id_hashes})
    pq.write_table(tbl, path, compression="zstd")


def _read_inventory_cache(store: S3Store, key: str) -> tuple[list[tuple[str, str]], list[bytes]]:
    raw = bytes(obstore.get(store, key).bytes())
    pf = pq.ParquetFile(io.BytesIO(raw))
    pairs: list[tuple[str, str]] = []
    hashes: list[bytes] = []
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["bucket", "key", "id_hash"]):
        buckets = batch.column("bucket").to_pylist()
        keys = batch.column("key").to_pylist()
        id_hashes = batch.column("id_hash").to_pylist()
        pairs.extend(zip(buckets, keys))
        hashes.extend(id_hashes)
    return pairs, hashes


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
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    local_delta = _is_local(delta_prefix)
    local_wh = _is_local(warehouse_hash_uri)
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

    # Tier 2: inventory cache exists — skip manifest fetch + inventory stream
    inv_pairs: list[tuple[str, str]] = []
    inv_hashes: list[bytes] = []
    inventory_cached = False

    if local_delta and Path(inventory_cache).exists():
        print(f"Inventory cache found: {inventory_cache}")
        inv_pairs, inv_hashes = _read_inventory_cache_local(inventory_cache)
        print(f"Loaded cached inventory: {len(inv_pairs):,} .stac.json items")
        inventory_cached = True
    else:
        manifest_bucket, manifest_key = _parse_s3_uri(manifest_uri)
        manifest_store = _get_store(manifest_bucket)

        print(f"Fetching manifest: {manifest_uri}")
        manifest = _fetch_manifest(manifest_store, manifest_key)
        n_data_files = len(manifest.get("files", []))
        print(f"Manifest: {n_data_files} data file(s)")

        print("Streaming inventory and hashing IDs ...")
        inv_pairs, inv_hashes = _stream_inventory_hashes(manifest, manifest_store)
        print(f"Inventory: {len(inv_pairs):,} .stac.json items")

        inv_rows = [(b, k, h) for (b, k), h in zip(inv_pairs, inv_hashes)]
        if local_delta:
            _write_inventory_cache_local(inv_rows, inventory_cache)
            print(f"Wrote inventory cache: {inventory_cache}")
        else:
            delta_bucket, delta_path = _parse_s3_uri(delta_prefix)
            delta_store = _get_store(delta_bucket, prefix=delta_path)
            inv_key = f"pending/inventory_{date_str}.parquet"
            _write_inventory_cache(inv_rows, delta_store, inv_key)
            print(f"Wrote inventory cache: {delta_prefix.rstrip('/')}/{inv_key}")

    # Anti-join against warehouse hash index
    if local_wh:
        print(f"Reading local warehouse hash index: {warehouse_hash_uri}")
        wh_hashes = _download_hash_index_local(warehouse_hash_uri)
    else:
        wh_bucket, wh_key = _parse_s3_uri(warehouse_hash_uri)
        print(f"Downloading warehouse hash index: {warehouse_hash_uri}")
        wh_hashes = _download_hash_index(_get_store(wh_bucket), wh_key)
    wh_set = _build_hash_set(wh_hashes)
    print(f"Warehouse hash index: {len(wh_set):,} unique hashes")

    print("Computing anti-join ...")
    new_pairs: list[tuple[str, str, bytes]] = []
    for (b, k), h in zip(inv_pairs, inv_hashes):
        if h not in wh_set:
            new_pairs.append((b, k, h))
    print(f"New items (not in warehouse): {len(new_pairs):,}")

    print("Checking for pending deltas ...")
    accumulated: dict[bytes, tuple[str, str, bytes]] = {}
    for (b, k, h) in new_pairs:
        accumulated[h] = (b, k, h)

    if local_delta:
        pending_keys = _list_pending_deltas_local(delta_prefix)
    else:
        delta_bucket, delta_path = _parse_s3_uri(delta_prefix)
        delta_store = _get_store(delta_bucket, prefix=delta_path)
        pending_keys = _list_pending_deltas(delta_store, "")

    for pk in pending_keys:
        print(f"  Merging previous delta: {pk}")
        if local_delta:
            rows = _read_pending_delta_local(pk)
        else:
            rows = _read_pending_delta(delta_store, pk)
        for b, k, h in rows:
            if h not in accumulated:
                accumulated[h] = (b, k, h)

    final_rows = list(accumulated.values())
    print(f"Accumulated delta: {len(final_rows):,} items")

    if local_delta:
        n = _write_delta_parquet_local(final_rows, delta_output)
        print(f"Written: {delta_output} ({n:,} rows)")
    else:
        delta_key = f"pending/delta_{date_str}.parquet"
        n = _write_delta_parquet(final_rows, delta_store, delta_key)
        print(f"Written: {delta_prefix.rstrip('/')}/{delta_key} ({n:,} rows)")

    for pk in pending_keys:
        if pk != delta_output and pk != f"pending/delta_{date_str}.parquet":
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
        "inventory_items": len(inv_pairs),
        "warehouse_hashes": len(wh_set),
        "new_items": len(new_pairs),
        "previous_deltas_merged": len(pending_keys),
        "accumulated_total": len(final_rows),
        "delta_key": delta_output if local_delta else f"{delta_prefix.rstrip('/')}/pending/delta_{date_str}.parquet",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce daily delta for earthcatalog")
    parser.add_argument("manifest", help="s3:// URI to today's manifest.json")
    parser.add_argument(
        "--warehouse-hash",
        default="s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet",
        help="s3:// URI or local path to warehouse hash index parquet",
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
