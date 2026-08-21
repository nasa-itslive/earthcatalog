"""
Unified warehouse index — merges the legacy hash index and source index.

A single Parquet file (``warehouse_index.parquet``) tracking every STAC item's
provenance, its hash for fast dedup, and a soft-delete flag used by garbage
collection.

Schema
------
id_hash         fixed_size_binary[16]  xxh3_128(stac_id, seed=42)
stac_id         string
s3_key          string                 provenance; the resume checkpoint
grid_partition  string                 locates the GeoParquet file for GC
year            int32
ingested_at     string
deleted         bool                   soft-delete flag set by GC

Public API
----------
Index(store, key)
    Open (or create-on-write) the index file.

append(rows) -> int
    Append provenance rows; create the file on first use.

known_source_keys() -> set[str]
    All ``s3_key`` values — cheap cross-run resume checkpoint.

contains_source_key(s3_key) -> bool
    Membership test for resume.

stream_active() -> Iterator[dict]
    Yield non-deleted rows as dicts (for GC).

mark_deleted(stac_ids) -> int
    Set ``deleted=True`` for matching ``stac_id`` rows.

compact() -> int
    Physically drop deleted rows and rewrite the file.

hash_set() -> set[bytes]
    The set of ``id_hash`` values (legacy dedup-set compat).
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import UTC, datetime

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import xxhash

_HASH_SEED = 42
_BATCH_SIZE = 100_000

_SCHEMA = pa.schema(
    [
        pa.field("id_hash", pa.binary(16)),
        pa.field("s3_key", pa.string()),
        pa.field("stac_id", pa.string()),
        pa.field("grid_partition", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("ingested_at", pa.string()),
        pa.field("deleted", pa.bool_()),
    ]
)


def hash_id(item_id: str) -> bytes:
    """Return the 16-byte xxh3_128 digest for *item_id* (matches legacy hash index)."""
    return xxhash.xxh3_128(item_id.encode("utf-8"), seed=_HASH_SEED).digest()


class Index:
    """Provenance + dedup index backed by a single Parquet file in *store*."""

    def __init__(self, store: object, key: str) -> None:
        self._store = store
        self._key = key

    # -- reads ---------------------------------------------------------------

    def _read(self) -> pa.Table | None:
        try:
            raw = bytes(obstore.get(self._store, self._key).bytes())
        except FileNotFoundError:
            return None
        return pq.ParquetFile(io.BytesIO(raw)).read()

    def _write(self, tbl: pa.Table) -> None:
        buf = io.BytesIO()
        pq.write_table(tbl, buf, compression="zstd")
        obstore.put(self._store, self._key, buf.getvalue())

    # -- public API ----------------------------------------------------------

    def append(self, rows: list[dict]) -> int:
        """Append provenance rows; return total row count (0 if empty no-op)."""
        if not rows:
            tbl = self._read()
            return tbl.num_rows if tbl is not None else 0

        now = datetime.now(UTC).isoformat()
        new = pa.table(
            {
                "id_hash": pa.array([hash_id(r["stac_id"]) for r in rows], type=pa.binary(16)),
                "s3_key": [r["s3_key"] for r in rows],
                "stac_id": [r["stac_id"] for r in rows],
                "grid_partition": [r["grid_partition"] for r in rows],
                "year": pa.array([int(r.get("year") or 0) for r in rows], type=pa.int32()),
                "ingested_at": pa.array([now] * len(rows)),
                "deleted": pa.array([False] * len(rows)),
            },
            schema=_SCHEMA,
        )

        existing = self._read()
        merged = pa.concat_tables([existing, new]) if existing is not None else new
        self._write(merged)
        return merged.num_rows

    def known_source_keys(self) -> set[str]:
        """All ``s3_key`` values in the index (used as a resume checkpoint)."""
        tbl = self._read()
        if tbl is None:
            return set()
        keys: set[str] = set()
        for batch in tbl.column("s3_key").chunks:
            keys.update(str(k) for k in batch.to_pylist() if k)
        return keys

    def contains_source_key(self, s3_key: str) -> bool:
        """True if *s3_key* is already indexed."""
        return s3_key in self.known_source_keys()

    def stream_active(self) -> Iterator[dict]:
        """Yield non-deleted rows as dicts: ``{s3_key, stac_id, grid_partition, year}``."""
        tbl = self._read()
        if tbl is None:
            return
        keep = tbl.filter(pc.invert(tbl.column("deleted")))
        for batch in keep.to_batches():
            s3_keys = batch.column("s3_key").to_pylist()
            stac_ids = batch.column("stac_id").to_pylist()
            cells = batch.column("grid_partition").to_pylist()
            years = batch.column("year").to_pylist()
            for s3_key, stac_id, cell, year in zip(s3_keys, stac_ids, cells, years):
                if not s3_key:
                    continue
                yield {
                    "s3_key": s3_key,
                    "stac_id": stac_id,
                    "grid_partition": cell,
                    "year": year,
                }

    def mark_deleted(self, stac_ids: set[str]) -> int:
        """Set ``deleted=True`` for every row whose ``stac_id`` is in *stac_ids*."""
        if not stac_ids:
            return 0
        existing = self._read()
        if existing is None:
            return 0

        marked = pc.is_in(existing.column("stac_id"), pa.array(list(stac_ids)))
        if not pc.any(marked).as_py():
            return 0

        deleted_col = pc.or_(existing.column("deleted"), marked)
        existing = existing.set_column(
            existing.schema.get_field_index("deleted"),
            "deleted",
            deleted_col,
        )
        self._write(existing)
        return int(pc.sum(marked).as_py())

    def compact(self) -> int:
        """Physically remove ``deleted=True`` rows; return active row count."""
        existing = self._read()
        if existing is None:
            return 0
        active = existing.filter(pc.invert(existing.column("deleted")))
        self._write(active)
        return active.num_rows

    def hash_set(self) -> set[bytes]:
        """``id_hash`` values for active (non-deleted) rows — for dedup.

        Deleted rows are excluded so that a re-added item can be re-ingested
        after GC marks it deleted.
        """
        tbl = self._read()
        if tbl is None:
            return set()
        active = tbl.filter(pc.invert(tbl.column("deleted")))
        hashes: set[bytes] = set()
        for batch in active.column("id_hash").chunks:
            for h in batch.to_pylist():
                if h is not None:
                    hashes.add(bytes(h))
        return hashes

    def count_active(self) -> int:
        """Number of non-deleted rows (unique ingested items), streamed.

        Streams the ``deleted`` column in batches so it stays memory-bounded
        regardless of index size, and excludes soft-deleted rows (unlike a raw
        Parquet footer row count).
        """
        try:
            raw = bytes(obstore.get(self._store, self._key).bytes())
        except FileNotFoundError:
            return 0
        pf = pq.ParquetFile(io.BytesIO(raw))
        active = 0
        for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["deleted"]):
            deleted = batch.column("deleted")
            active += int(pc.sum(pc.invert(deleted).cast(pa.int32())).as_py())
        return active


def migrate_indices(
    store: object,
    *,
    hash_key: str,
    source_key: str,
    out_key: str | None = None,
) -> Index | None:
    """Fold the legacy hash + source index files into the unified Index.

    Reads ``*_id_hashes.parquet`` (an ``id_hash`` column) and
    ``*_source_index.parquet`` (``s3_key, stac_id, grid_partition, year,
    ingested_at, deleted``) and writes the unified schema.  Hash-only
    entries (from the hash index) are preserved as rows with an empty
    ``s3_key``/``stac_id``.  Returns ``None`` if neither file exists.

    The new file is written to *out_key* (defaults to ``source_key`` with
    the ``_index`` suffix) and the old files are **not** deleted here —
    the caller deletes them after verifying a green run.
    """
    source_tbl = _read_parquet(store, source_key)
    hash_tbl = _read_parquet(store, hash_key)

    if source_tbl is None and hash_tbl is None:
        return None

    if out_key is None:
        base = source_key if source_key else hash_key
        out_key = base.rsplit(".parquet", 1)[0] + "_index.parquet"

    rows: list[dict] = []
    seen: set[bytes] = set()

    if source_tbl is not None:
        cols = source_tbl.to_pydict()
        for i in range(source_tbl.num_rows):
            stac_id = cols["stac_id"][i]
            h = hash_id(stac_id) if stac_id else None
            if h is not None:
                seen.add(h)
            rows.append(
                {
                    "id_hash": h,
                    "s3_key": cols["s3_key"][i] or "",
                    "stac_id": stac_id or "",
                    "grid_partition": cols.get("grid_partition", ["__none__"] * source_tbl.num_rows)[i] or "__none__",
                    "year": int(cols.get("year", [0] * source_tbl.num_rows)[i] or 0),
                    "ingested_at": cols.get("ingested_at", [""] * source_tbl.num_rows)[i] or "",
                    "deleted": bool(cols.get("deleted", [False] * source_tbl.num_rows)[i]),
                }
            )

    if hash_tbl is not None:
        for h in hash_tbl.column("id_hash").to_pylist():
            h = bytes(h)
            if h in seen:
                continue
            seen.add(h)
            rows.append(
                {
                    "id_hash": h,
                    "s3_key": "",
                    "stac_id": "",
                    "grid_partition": "__none__",
                    "year": 0,
                    "ingested_at": "",
                    "deleted": False,
                }
            )

    idx = Index(store, out_key)
    now = datetime.now(UTC).isoformat()
    tbl = pa.table(
        {
            "id_hash": pa.array([r["id_hash"] for r in rows], type=pa.binary(16)),
            "s3_key": [r["s3_key"] for r in rows],
            "stac_id": [r["stac_id"] for r in rows],
            "grid_partition": [r["grid_partition"] for r in rows],
            "year": pa.array([int(r["year"]) for r in rows], type=pa.int32()),
            "ingested_at": [r.get("ingested_at") or now for r in rows],
            "deleted": [bool(r.get("deleted", False)) for r in rows],
        },
        schema=_SCHEMA,
    )
    idx._write(tbl)
    return idx


def _read_parquet(store: object, key: str) -> pa.Table | None:
    """Read a Parquet table from *store* at *key*, or ``None`` if absent."""
    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return None
    return pq.ParquetFile(io.BytesIO(raw)).read()
