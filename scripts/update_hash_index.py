#!/usr/bin/env python3
"""
Update the warehouse hash index after a delta ingest.

Reads a delta parquet's (bucket, key) pairs, hashes the item IDs,
appends them to the existing warehouse hash index, and uploads.

Usage
-----
    python scripts/update_hash_index.py \
      s3://its-live-data/test-space/stac/catalog/delta/ingested/delta_2026-04-28.done \
      --warehouse-hash s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet

Also used for initial bootstrap from the warehouse GeoParquet files:

    python scripts/update_hash_index.py \
      --bootstrap s3://its-live-data/test-space/stac/catalog/warehouse \
      --warehouse-hash s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet
"""

import argparse
import io
import re

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import xxhash
from obstore.store import S3Store

_HASH_SEED = 42
_BATCH_SIZE = 100_000
_HIVE_RE = re.compile(
    r"grid_partition=(?P<cell>[^/]+)/year=(?P<year>[^/]+)/(?P<file>[^/]+\.parquet)$"
)


def _hash_id(item_id: str) -> bytes:
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


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


def _read_existing_hashes(store: S3Store, key: str) -> set[bytes]:
    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return set()
    pf = pq.ParquetFile(io.BytesIO(raw))
    hashes: set[bytes] = set()
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash"]):
        for h in batch.column("id_hash").to_pylist():
            hashes.add(h)
    return hashes


def _write_sorted_hash_index(hashes: set[bytes], store: S3Store, key: str) -> int:
    sorted_hashes = sorted(hashes)
    arr = pa.array(sorted_hashes, type=pa.binary(16))
    tbl = pa.table({"id_hash": arr})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return len(sorted_hashes)


def update_from_delta(
    delta_uri: str, warehouse_hash_uri: str
) -> dict:
    bucket, key = _parse_s3_uri(delta_uri)
    store = _get_store(bucket)

    wh_bucket, wh_key = _parse_s3_uri(warehouse_hash_uri)
    wh_store = _get_store(wh_bucket)

    print(f"Reading existing hash index: {warehouse_hash_uri}")
    existing = _read_existing_hashes(wh_store, wh_key)
    print(f"  Existing hashes: {len(existing):,}")

    print(f"Reading delta: {delta_uri}")
    raw = bytes(obstore.get(store, key).bytes())
    pf = pq.ParquetFile(io.BytesIO(raw))

    new_count = 0
    for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["key"]):
        for k in batch.column("key").to_pylist():
            fname = k.rsplit("/", 1)[-1]
            item_id = fname.removesuffix(".stac.json")
            h = _hash_id(item_id)
            if h not in existing:
                existing.add(h)
                new_count += 1

    print(f"  New hashes to add: {new_count:,}")

    total = _write_sorted_hash_index(existing, wh_store, wh_key)
    print(f"  Written: {warehouse_hash_uri} ({total:,} total hashes)")

    return {"new": new_count, "total": total}


def bootstrap_from_warehouse(
    warehouse_uri: str, warehouse_hash_uri: str
) -> dict:
    wh_bucket, wh_prefix = _parse_s3_uri(warehouse_uri)
    wh_store = _get_store(wh_bucket, prefix=wh_prefix)

    hash_bucket, hash_key = _parse_s3_uri(warehouse_hash_uri)
    hash_store = _get_store(hash_bucket)

    print(f"Bootstrapping hash index from: {warehouse_uri}")
    all_hashes: set[bytes] = set()
    file_count = 0

    for batch in obstore.list(wh_store, prefix=""):
        for obj in batch:
            k: str = obj["path"]
            if not k.endswith(".parquet") or not _HIVE_RE.search(k):
                continue
            file_count += 1
            raw = bytes(obstore.get(wh_store, k).bytes())
            pf = pq.ParquetFile(io.BytesIO(raw))
            for data_batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id"]):
                for item_id in data_batch.column("id").to_pylist():
                    all_hashes.add(_hash_id(item_id))
            if file_count % 100 == 0:
                print(f"  Processed {file_count} files, {len(all_hashes):,} hashes")

    print(f"  Total: {file_count} files, {len(all_hashes):,} unique hashes")

    total = _write_sorted_hash_index(all_hashes, hash_store, hash_key)
    print(f"  Written: {warehouse_hash_uri} ({total:,} hashes)")

    return {"files": file_count, "total": total}


def main() -> None:
    parser = argparse.ArgumentParser(description="Update warehouse hash index")
    parser.add_argument("delta", nargs="?", help="s3:// URI to ingested delta parquet")
    parser.add_argument(
        "--bootstrap", help="s3:// URI to warehouse root for initial hash index build",
    )
    parser.add_argument(
        "--warehouse-hash",
        default="s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet",
        help="s3:// URI to warehouse hash index parquet",
    )
    args = parser.parse_args()

    if args.bootstrap:
        result = bootstrap_from_warehouse(args.bootstrap, args.warehouse_hash)
    elif args.delta:
        result = update_from_delta(args.delta, args.warehouse_hash)
    else:
        parser.error("Provide either a delta URI or --bootstrap")

    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
