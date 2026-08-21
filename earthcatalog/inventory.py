"""S3 Inventory readers and STAC item fetching.

Pure I/O functions for streaming ``(bucket, key)`` pairs from AWS S3
Inventory files (CSV, Parquet, manifest.json) and fetching individual
STAC JSON objects.  No Iceberg, no fan-out, no write — just read.

Shared by the ingest pipeline, garbage-collection pipeline, and the
daily-delta script.
"""

from __future__ import annotations

import configparser
import csv
import gzip
import io
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime

import obstore
import pyarrow.parquet as pq
from obstore.store import S3Store

_STORES: dict[str, S3Store] = {}


def get_store(bucket: str) -> S3Store:
    if bucket not in _STORES:
        _STORES[bucket] = S3Store(
            bucket=bucket,
            region="us-west-2",
            skip_signature=True,
        )
    return _STORES[bucket]


def get_authenticated_store(bucket: str) -> S3Store:
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")

    if not (key_id and secret):
        creds_file = os.path.expanduser("~/.aws/credentials")
        cfg = configparser.ConfigParser()
        cfg.read(creds_file)
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token

    kwargs: dict = dict(bucket=bucket, region="us-west-2")
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token

    return S3Store(**kwargs)


# ---------------------------------------------------------------------------
# Inventory reading
# ---------------------------------------------------------------------------


def _fetch_inventory_bytes(inventory_path: str) -> bytes:
    bucket, key = inventory_path.removeprefix("s3://").split("/", 1)
    return bytes(obstore.get(get_store(bucket), key).bytes())


def _parse_last_modified(value: str) -> datetime | None:
    try:
        s = value.strip().rstrip("Z")
        dt = datetime.fromisoformat(s).replace(tzinfo=UTC)
        return dt
    except Exception:
        return None


def _coerce_last_modified(lm_raw: object) -> datetime | None:
    if lm_raw is None:
        return None
    if isinstance(lm_raw, datetime):
        return lm_raw if lm_raw.tzinfo else lm_raw.replace(tzinfo=UTC)
    return _parse_last_modified(str(lm_raw))


def iter_inventory_csv(
    inventory_path: str, since: datetime | None = None
) -> Iterator[tuple[str, str]]:
    is_gz = inventory_path.endswith(".gz")

    if inventory_path.startswith("s3://"):
        raw_bytes = _fetch_inventory_bytes(inventory_path)
        if is_gz:
            fh = gzip.open(io.BytesIO(raw_bytes), "rt", newline="")
        else:
            fh = io.TextIOWrapper(io.BytesIO(raw_bytes), encoding="utf-8", newline="")
    elif is_gz:
        fh = gzip.open(inventory_path, "rt", newline="")
    else:
        fh = open(inventory_path, newline="", encoding="utf-8")

    with fh:
        reader = csv.reader(fh)
        header_seen = False
        lm_col: int | None = None

        for row in reader:
            if not row:
                continue
            if not header_seen:
                header_seen = True
                if row[0].strip('"').lower() == "bucket":
                    lower_cols = [c.strip('"').lower() for c in row]
                    if "last_modified_date" in lower_cols:
                        lm_col = lower_cols.index("last_modified_date")
                    continue

            if since is not None and lm_col is not None and lm_col < len(row):
                lm = _parse_last_modified(row[lm_col].strip('"'))
                if lm is not None and lm < since:
                    continue

            yield row[0].strip('"'), row[1].strip('"')


def iter_inventory_parquet(
    inventory_path: str | io.BytesIO,
    batch_size: int = 65_536,
    since: datetime | None = None,
) -> Iterator[tuple[str, str]]:
    if isinstance(inventory_path, io.BytesIO):
        source = inventory_path
    elif inventory_path.startswith("s3://"):
        source = io.BytesIO(_fetch_inventory_bytes(inventory_path))
    else:
        source = inventory_path

    pf = pq.ParquetFile(source)
    schema_names = set(pf.schema_arrow.names)
    has_lm = "last_modified_date" in schema_names
    read_cols = ["bucket", "key"] + (["last_modified_date"] if has_lm else [])

    for batch in pf.iter_batches(batch_size=batch_size, columns=read_cols):
        buckets = batch.column("bucket").to_pylist()
        keys = batch.column("key").to_pylist()
        if since is not None and has_lm:
            lm_values = batch.column("last_modified_date").to_pylist()
            for bucket, key, lm_raw in zip(buckets, keys, lm_values):
                if lm_raw is None:
                    yield bucket, key
                    continue
                lm = _coerce_last_modified(lm_raw)
                if lm is None or lm >= since:
                    yield bucket, key
        else:
            yield from zip(buckets, keys)


def _parse_manifest(manifest_s3_uri: str) -> tuple[str, object, list[str]]:
    manifest_path = manifest_s3_uri.removeprefix("s3://")
    manifest_bucket, manifest_key = manifest_path.split("/", 1)

    dest_store_manifest = get_authenticated_store(manifest_bucket)
    raw = bytes(obstore.get(dest_store_manifest, manifest_key).bytes())
    manifest = json.loads(raw)

    dest_arn = manifest.get("destinationBucket", "")
    dest_bucket = dest_arn.split(":::")[1] if ":::" in dest_arn else manifest_bucket

    data_keys = [f["key"] for f in manifest.get("files", [])]
    source_bucket = manifest.get("sourceBucket", "")
    dest_store = get_authenticated_store(dest_bucket)
    print(f"Manifest: {len(data_keys)} data file(s) in {dest_bucket}")
    return source_bucket, dest_store, data_keys


def iter_inventory_file_from_store(
    store: object,
    data_key: str,
    batch_size: int = 65_536,
    since: datetime | None = None,
) -> Iterator[tuple[str, str]]:
    raw_bytes = bytes(obstore.get(store, data_key).bytes())
    yield from iter_inventory_parquet(io.BytesIO(raw_bytes), batch_size=batch_size, since=since)


def iter_inventory_manifest(
    manifest_s3_uri: str,
    batch_size: int = 65_536,
    since: datetime | None = None,
) -> Iterator[tuple[str, str]]:
    _source_bucket, dest_store, data_keys = _parse_manifest(manifest_s3_uri)
    for data_key in data_keys:
        yield from iter_inventory_file_from_store(
            dest_store, data_key, batch_size=batch_size, since=since
        )


def iter_inventory(
    inventory_path: str,
    parquet_batch_size: int = 65_536,
    since: datetime | None = None,
) -> Iterator[tuple[str, str]]:
    if inventory_path.endswith("manifest.json"):
        yield from iter_inventory_manifest(
            inventory_path,
            batch_size=parquet_batch_size,
            since=since,
        )
    elif inventory_path.endswith(".parquet"):
        yield from iter_inventory_parquet(
            inventory_path,
            batch_size=parquet_batch_size,
            since=since,
        )
    else:
        yield from iter_inventory_csv(inventory_path, since=since)


# ---------------------------------------------------------------------------
# STAC item fetching
# ---------------------------------------------------------------------------


def fetch_item(bucket: str, key: str) -> dict | None:
    try:
        raw = obstore.get(get_store(bucket), key).bytes()
        item = json.loads(bytes(raw))
        item["_source_bucket"] = bucket
        item["_source_key"] = key
        return item
    except Exception as exc:
        print(f"WARN: failed to fetch s3://{bucket}/{key}: {exc}")
        return None


# Backward-compatible aliases (modules that imported the private names
# from the deleted pipelines.incremental module can import from here).
_iter_inventory_csv = iter_inventory_csv
_iter_inventory_parquet = iter_inventory_parquet
_iter_inventory_file_from_store = iter_inventory_file_from_store
_iter_inventory_manifest = iter_inventory_manifest
_iter_inventory = iter_inventory
_fetch_item = fetch_item
_get_store = get_store
_get_authenticated_store = get_authenticated_store
_fetch_inventory_bytes = _fetch_inventory_bytes
_parse_last_modified = _parse_last_modified
_coerce_last_modified = _coerce_last_modified
_parse_manifest = _parse_manifest
