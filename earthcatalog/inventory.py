"""S3 Inventory readers and STAC item fetching.

Pure I/O functions for streaming ``(bucket, key)`` pairs from AWS S3
Inventory files (CSV, Parquet, manifest.json) and fetching individual
STAC JSON objects.  No Iceberg, no fan-out, no write — just read.

Shared by the ingest pipeline, garbage-collection pipeline, and the
daily-delta script.
"""

from __future__ import annotations

import asyncio
import configparser
import csv
import gzip
import hashlib
import io
import json
import os
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import chain, islice
from pathlib import Path

import obstore
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import ObjectStore, S3Store
from tqdm import tqdm

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
    key_id, secret, token = _aws_keys()

    kwargs: dict = dict(bucket=bucket, region="us-west-2")
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token

    return S3Store(**kwargs)


def _aws_keys() -> tuple[str, str, str]:
    """AWS credentials from the environment, else ~/.aws/credentials."""

    key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    token = os.environ.get("AWS_SESSION_TOKEN", "")
    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token
    return key_id, secret, token


def sql_catalog_props(db_path: str, warehouse_path: str) -> dict:
    """PyIceberg SqlCatalog properties for *warehouse_path*.

    Authenticated from the environment or the shared credentials file —
    pyiceberg's pyarrow IO does not read ~/.aws/credentials on its own, so
    without this a local (non-env) run would write metadata anonymously and
    be denied.
    """
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-west-2"
    props: dict = {"uri": f"sqlite:///{db_path}", "warehouse": warehouse_path}

    if warehouse_path.startswith("s3://"):
        props["s3.region"] = region
        key_id, secret, token = _aws_keys()
        if key_id and secret:
            props["s3.access-key-id"] = key_id
            props["s3.secret-access-key"] = secret
            if token:
                props["s3.session-token"] = token
        else:
            props["s3.anonymous"] = "true"
            props["s3.endpoint"] = f"https://s3.{region}.amazonaws.com"

    return props


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
        source: str | io.BytesIO = inventory_path
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


def _parse_manifest(manifest_s3_uri: str) -> tuple[str, ObjectStore, list[str]]:
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
    store: ObjectStore,
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
    assert dest_store is not None
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
# Distributed sharding
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InventoryShard:
    """One unit of distributed ingest work.

    *files* are parquet shard files (with *store* pointing at the bucket
    that holds them) that each worker reads itself, so only the file key —
    never the (bucket, key) pairs — crosses the wire to workers.  Shards
    are written by :func:`write_inventory_shards` with a fixed number of
    rows each, so every worker gets the same amount of work regardless of
    how skewed the source inventory part files are.  The *pairs* form
    carries materialised pairs for tests and serial use.  *since* /
    *suffix* / *limit* are applied by :meth:`iter_pairs` at iteration
    time, on whichever node consumes the shard.
    """

    files: tuple[str, ...] = ()
    pairs: tuple[tuple[str, str], ...] = ()
    store: ObjectStore | None = None
    since: datetime | None = None
    suffix: str | None = None
    limit: int | None = None

    def iter_pairs(self) -> Iterator[tuple[str, str]]:
        """Yield this shard's (bucket, key) pairs, applying since/suffix/limit."""
        it = self._iter_raw()
        if self.suffix:
            it = (pair for pair in it if pair[1].endswith(self.suffix))
        if self.limit is not None:
            it = islice(it, self.limit)
        return it

    def _iter_raw(self) -> Iterator[tuple[str, str]]:
        if self.pairs:
            return iter(self.pairs)
        assert self.store is not None
        return chain.from_iterable(
            iter_inventory_file_from_store(self.store, key, since=self.since) for key in self.files
        )


SCATTER_MANIFEST_NAME = "scatter.json"


def scatter_manifest_path(staging_prefix: str) -> str:
    """Object key of the scatter manifest written under *staging_prefix*."""
    return f"{staging_prefix.rstrip('/')}/{SCATTER_MANIFEST_NAME}"


def is_scatter_manifest(path: str) -> bool:
    """True when *path* points at a scatter manifest."""
    return Path(path.rstrip("/")).name == SCATTER_MANIFEST_NAME


def scatter_staging_prefix(
    warehouse_prefix: str,
    inventory_path: str,
    *,
    chunk_size: int,
    since: datetime | None = None,
    suffix: str | None = None,
    limit: int | None = None,
) -> str:
    """Deterministic scatter staging prefix for an inventory + parameters.

    Re-running the scatter step with the same inventory and parameters
    resolves to the same prefix, so an already-scattered inventory is
    detected and reused instead of re-read (the manifest path — and hence
    the snapshot date — is part of the key, so a *new* snapshot still
    scatters fresh).
    """
    key = json.dumps(
        {
            "inventory": inventory_path,
            "chunk_size": chunk_size,
            "since": since.isoformat() if since else None,
            "suffix": suffix,
            "limit": limit,
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"{warehouse_prefix.rstrip('/')}/staging/shards/{digest}"


def scatter_manifest_exists(store: ObjectStore, staging_prefix: str) -> bool:
    """True if a scatter manifest already exists under *staging_prefix*."""
    try:
        obstore.get(store, scatter_manifest_path(staging_prefix)).bytes()
        return True
    except FileNotFoundError:
        return False


def write_inventory_shards(
    inventory_path: str,
    store: ObjectStore,
    *,
    staging_prefix: str,
    chunk_size: int = 100_000,
    since: datetime | None = None,
    suffix: str | None = None,
    limit: int | None = None,
) -> list[InventoryShard]:
    """Scatter: stream *inventory_path* into fixed-row shard files in *store*.

    Reads the inventory (any supported format) sequentially on the head
    node, applies *since* / *suffix* / *limit*, and writes one parquet
    shard file per *chunk_size* matching pairs under *staging_prefix*
    (``<prefix>/shard_<NNNN>.parquet``), plus a ``scatter.json`` manifest
    recording the shard keys and scatter parameters.  Head memory stays
    bounded at ~*chunk_size* pairs.  Every shard holds exactly *chunk_size*
    rows (the last one may be partial), so worker load is uniform even when
    the source inventory part files are unbalanced.

    The scatter is a standalone step: run it once (no cluster needed), then
    ingest with ``inventory_path`` pointing at the scatter manifest —
    workers start immediately instead of idling behind the head's read.
    Resume a failed run the same way; the unified index dedups.

    Returns one file-backed :class:`InventoryShard` per written file;
    each worker then reads its own shard URL via :meth:`InventoryShard.iter_pairs`.
    """
    prefix = staging_prefix.rstrip("/")
    buf: list[tuple[str, str]] = []
    shards: list[InventoryShard] = []

    def _flush() -> None:
        if not buf:
            return
        key = f"{prefix}/shard_{len(shards):05d}.parquet"
        table = pa.table(
            {
                "bucket": pa.array([b for b, _ in buf], type=pa.string()),
                "key": pa.array([k for _, k in buf], type=pa.string()),
            }
        )
        out = io.BytesIO()
        pq.write_table(table, out)
        obstore.put(store, key, out.getvalue())
        shards.append(InventoryShard(files=(key,), store=store))
        buf.clear()

    n = 0
    pbar = tqdm(desc="Scatter", unit=" rows")
    for bucket, key in iter_inventory(inventory_path, since=since):
        pbar.update(1)
        if suffix is not None and not key.endswith(suffix):
            continue
        buf.append((bucket, key))
        n += 1
        if len(buf) >= chunk_size:
            _flush()
            pbar.set_postfix(matched=n, shards=len(shards))
        if limit is not None and n >= limit:
            break
    _flush()
    pbar.set_postfix(matched=n, shards=len(shards))
    pbar.close()

    manifest = {
        "version": 1,
        "created": datetime.now(UTC).isoformat(),
        "inventory": inventory_path,
        "chunk_size": chunk_size,
        "since": since.isoformat() if since else None,
        "suffix": suffix,
        "items": n,
        "shards": [s.files[0] for s in shards],
    }
    obstore.put(store, scatter_manifest_path(prefix), json.dumps(manifest).encode())
    return shards


def load_inventory_shards(scatter_path: str, store: ObjectStore) -> list[InventoryShard]:
    """Load shards scattered by :func:`write_inventory_shards`.

    *scatter_path* may be the ``scatter.json`` manifest key or the staging
    prefix that contains it.  The returned shards already carry the
    scattered pairs — do not re-apply since/suffix/limit.
    """
    key = scatter_path.rstrip("/")
    if not key.endswith(SCATTER_MANIFEST_NAME):
        key = scatter_manifest_path(key)
    manifest = json.loads(bytes(obstore.get(store, key).bytes()))
    return [InventoryShard(files=(k,), store=store) for k in manifest["shards"]]


def delete_shard_files(store: ObjectStore, shards: list[InventoryShard]) -> int:
    """Best-effort delete of shard files written by :func:`write_inventory_shards`."""
    deleted = 0
    for shard in shards:
        for key in shard.files:
            try:
                obstore.delete(store, key)
                deleted += 1
            except Exception:
                pass
    return deleted


def delete_scatter(store: ObjectStore, staging_prefix: str, shards: list[InventoryShard]) -> int:
    """Best-effort delete of shard files *and* their scatter manifest."""
    deleted = delete_shard_files(store, shards)
    try:
        obstore.delete(store, scatter_manifest_path(staging_prefix))
        deleted += 1
    except Exception:
        pass
    return deleted


# ---------------------------------------------------------------------------
# STAC item fetching
# ---------------------------------------------------------------------------

_FETCH_RETRIES = 3
_FETCH_BACKOFF_BASE = 0.5
_FETCH_CONCURRENCY = 256


def fetch_item(bucket: str, key: str) -> dict | None:
    try:
        raw = obstore.get(get_store(bucket), key).bytes()
        item = orjson.loads(bytes(raw))
        item["_source_bucket"] = bucket
        item["_source_key"] = key
        return item
    except Exception as exc:
        print(f"WARN: failed to fetch s3://{bucket}/{key}: {exc}")
        return None


async def _fetch_item_async(store: ObjectStore, bucket: str, key: str) -> dict | None:
    """Fetch one STAC JSON via ``obstore.get_async`` with retry/backoff.

    404 → None (skip); S3 error XML (SlowDown/Error) → retry; unexpected
    non-JSON content → None with a warning.
    """
    last_exc: Exception | None = None
    for attempt in range(_FETCH_RETRIES + 1):
        try:
            result = await obstore.get_async(store, key)
            raw = bytes(await result.bytes_async())
            if not raw or raw[0:1] != b"{":
                preview = raw[:200].decode("utf-8", errors="replace")
                if b"SlowDown" in raw or b"<Error>" in raw:
                    raise OSError(f"S3 error response: {preview}")
                print(
                    f"WARN: unexpected content for s3://{bucket}/{key}: {preview}", file=sys.stderr
                )
                return None
            item = orjson.loads(raw)
            item["_source_bucket"] = bucket
            item["_source_key"] = key
            return item
        except FileNotFoundError:
            return None
        except Exception as exc:
            last_exc = exc
            if attempt < _FETCH_RETRIES:
                await asyncio.sleep(_FETCH_BACKOFF_BASE * (2**attempt))
    print(f"WARN: failed to fetch s3://{bucket}/{key} after retries: {last_exc}", file=sys.stderr)
    return None


async def _fetch_all_async(
    pairs: list[tuple[str, str]],
    concurrency: int,
) -> list[dict]:
    """Fetch many STAC items concurrently via ``obstore.get_async``.

    Returns the successfully-fetched items (in no guaranteed order); failures
    are dropped (logged) — matching :func:`fetch_item`'s None-on-error contract.
    """
    from asyncio import Semaphore, TaskGroup

    sem = Semaphore(concurrency)
    stores: dict[str, S3Store] = {}
    results: dict[int, dict | None] = {}

    async def _one(i: int, bucket: str, key: str) -> None:
        async with sem:
            store = stores.get(bucket)
            if store is None:
                store = get_store(bucket)
                stores[bucket] = store
            results[i] = await _fetch_item_async(store, bucket, key)

    async with TaskGroup() as tg:
        for i, (bucket, key) in enumerate(pairs):
            tg.create_task(_one(i, bucket, key))

    items: list[dict] = []
    for i in range(len(pairs)):
        item = results.get(i)
        if item is not None:
            items.append(item)
    return items


def fetch_items_async(
    pairs: list[tuple[str, str]], *, concurrency: int = _FETCH_CONCURRENCY
) -> list[dict]:
    """Synchronous wrapper: fetch *pairs* concurrently via the async path."""
    return asyncio.run(_fetch_all_async(pairs, concurrency))


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
