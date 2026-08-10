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
        """All ``id_hash`` values — for in-memory dedup against new items."""
        tbl = self._read()
        if tbl is None:
            return set()
        hashes: set[bytes] = set()
        for batch in tbl.column("id_hash").chunks:
            for h in batch.to_pylist():
                if h is not None:
                    hashes.add(bytes(h))
        return hashes
