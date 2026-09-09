"""
Unified warehouse index — provenance, dedup, and soft-delete state.

Layout: a set of *immutable Parquet parts* under ``{base}/`` (one written
per ingest batch, after the Iceberg commit succeeds) plus, for warehouses
that predate parts, the legacy single file ``{base}.parquet``.  Appends
never read existing parts (O(delta) — the daily cost is the delta, not the
catalog); reads stream every location; soft deletes rewrite only the parts
they touch.  The whole index is derived state: it can be rebuilt from the
catalog's GeoParquet item ids plus the source inventory, and the weekly
consolidation audit reconciles it.

Schema
------
id_hash         fixed_size_binary[16]  xxh3_128(stac_id, seed=42)
stac_id         string
s3_key          string                 provenance; the resume checkpoint
grid_partition  string                 locates the GeoParquet file for GC
year            int32 (nullable)       as stored in the partition path
ingested_at     string
deleted         bool                   soft-delete flag set by garbage collection

Public API
----------
Index(store, key)
    Open the index.  *key* is the conventional ``{warehouse}_index.parquet``
    path; the ``.parquet`` suffix is stripped to derive the parts prefix.

append(rows, part=None) -> int
    Write one new part (never reads existing parts).  *part* is a
    deterministic flat name (``{run_id}--{seq:04d}`` in production) so crash
    recovery can rewrite the exact part it owes.

locations() -> list[str], exists() -> bool
known_source_keys() -> set[str], contains_source_key(s3_key) -> bool
stream_active() -> Iterator[dict], count_active() -> int
mark_deleted(stac_ids) -> int, compact() -> int
hash_set() -> set[bytes]
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import xxhash
from obstore.store import ObjectStore
from pyiceberg.table import Table

from ._pcc import pc_any, pc_filter, pc_invert, pc_is_in, pc_or, pc_sum

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


def resolve_index_path(table: Table | None, default_index_path: str) -> str:
    """The single resolver for the unified index location.

    ``earthcatalog.index_path`` table property first, then
    *default_index_path* (the caller's conventional
    ``{warehouse_root}_index.parquet``), else ``""``.  The legacy
    ``earthcatalog.hash_index_path`` property is deliberately NOT followed:
    it names the retired ``*_id_hashes.parquet`` file whose schema this
    Index cannot read (those warehouses were migrated to the unified
    index; the one-shot migration tool has since been removed).
    """
    from .schema import PROP_INDEX_PATH

    if table is not None:
        p = table.properties.get(PROP_INDEX_PATH)
        if p:
            return p
    return default_index_path or ""


class Index:
    """Provenance + dedup index, stored as immutable Parquet parts."""

    def __init__(self, store: ObjectStore, key: str) -> None:
        self._store = store
        self._base = key.removesuffix(".parquet")
        # Legacy single-file location — where pre-parts warehouses kept the
        # index and where compaction merges to.  Always ``{base}.parquet``.
        self._legacy_key = f"{self._base}.parquet"

    # -- locations -----------------------------------------------------------

    def _part_prefix(self) -> str:
        return f"{self._base}/"

    def locations(self) -> list[str]:
        """Every Parquet location: the legacy file (if present) + all parts."""
        locs: list[str] = []
        if self._legacy_exists():
            locs.append(self._legacy_key)
        parts: list[str] = []
        try:
            for listing in obstore.list(self._store, prefix=self._part_prefix()):
                for obj in listing:
                    k: str = obj["path"]
                    if k.endswith(".parquet"):
                        parts.append(k)
        except Exception:
            pass
        return locs + sorted(parts)

    def _legacy_exists(self) -> bool:
        try:
            obstore.head(self._store, self._legacy_key)
            return True
        except Exception:
            return False

    def exists(self) -> bool:
        """True if the index (legacy file or any part) exists in the store."""
        if self._legacy_exists():
            return True
        try:
            for listing in obstore.list(self._store, prefix=self._part_prefix()):
                for obj in listing:
                    if obj["path"].endswith(".parquet"):
                        return True
        except Exception:
            pass
        return False

    # -- writes ---------------------------------------------------------------

    def append(self, rows: list[dict], part: str | None = None) -> int:
        """Write one new part; return the number of rows written.

        Never reads existing parts — the daily cost is the delta, not the
        catalog.  *part* is a deterministic sub-path (production:
        ``{run_id}--{seq:04d}``, so crash recovery can rewrite exactly the
        part it owes); omitted, an opaque id is generated.
        """
        if not rows:
            return 0

        now = datetime.now(UTC).isoformat()
        tbl = pa.table(
            {
                "id_hash": pa.array([hash_id(r["stac_id"]) for r in rows], type=pa.binary(16)),
                "s3_key": [r["s3_key"] for r in rows],
                "stac_id": [r["stac_id"] for r in rows],
                "grid_partition": [r["grid_partition"] for r in rows],
                "year": pa.array(
                    [int(r["year"]) if r.get("year") is not None else None for r in rows],
                    type=pa.int32(),
                ),
                "ingested_at": pa.array([r.get("ingested_at") or now for r in rows]),
                "deleted": pa.array([False] * len(rows)),
            },
            schema=_SCHEMA,
        )

        part_id = part or f"_auto--{uuid.uuid4().hex}"
        buf = io.BytesIO()
        pq.write_table(tbl, buf, compression="zstd")
        obstore.put(self._store, f"{self._base}/{part_id}.parquet", buf.getvalue())
        return len(rows)

    def mark_deleted(self, stac_ids: set[str]) -> int:
        """Set ``deleted=True`` for rows whose ``stac_id`` is in *stac_ids*.

        Rewrites only the parts that actually contain matches.
        """
        total = 0
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            tbl = pq.ParquetFile(io.BytesIO(raw)).read()
            marked = pc_is_in(tbl.column("stac_id"), pa.array(list(stac_ids)))
            if not pc_any(marked).as_py():
                continue
            deleted_col = pc_or(tbl.column("deleted"), marked)
            tbl = tbl.set_column(tbl.schema.get_field_index("deleted"), "deleted", deleted_col)
            buf = io.BytesIO()
            pq.write_table(tbl, buf, compression="zstd")
            obstore.put(self._store, loc, buf.getvalue())
            total += int(pc_sum(marked).as_py())
        return total

    def compact(self) -> int:
        """Merge every location into the legacy single file; drop the parts.

        Physically removes soft-deleted rows.
        """
        tbl = self._read_all()
        active = tbl.filter(pc_invert(tbl.column("deleted")))
        buf = io.BytesIO()
        pq.write_table(active, buf, compression="zstd")
        obstore.put(self._store, self._legacy_key, buf.getvalue())
        for loc in self.locations():
            if loc != self._legacy_key:
                try:
                    obstore.delete(self._store, loc)
                except Exception:
                    pass
        return active.num_rows

    # -- reads ----------------------------------------------------------------

    def _read_all(self) -> pa.Table:
        tables = []
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            tables.append(pq.ParquetFile(io.BytesIO(raw)).read())
        return pa.concat_tables(tables) if tables else _SCHEMA.empty_table()

    def num_rows(self) -> int:
        """Total row count (including soft-deleted) from Parquet metadata."""
        total = 0
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            total += pq.ParquetFile(io.BytesIO(raw)).metadata.num_rows
        return total

    def known_source_keys(self) -> set[str]:
        """All ``s3_key`` values in the index (used as a resume checkpoint)."""
        keys: set[str] = set()
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            pf = pq.ParquetFile(io.BytesIO(raw))
            for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["s3_key"]):
                keys.update(str(k) for k in batch.column("s3_key").to_pylist() if k)
        return keys

    def contains_source_key(self, s3_key: str) -> bool:
        """True if *s3_key* is already indexed."""
        return s3_key in self.known_source_keys()

    def stream_active(self) -> Iterator[dict]:
        """Yield non-deleted rows as dicts: ``{s3_key, stac_id, grid_partition, year}``."""
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            pf = pq.ParquetFile(io.BytesIO(raw))
            for batch in pf.iter_batches(batch_size=_BATCH_SIZE):
                keep = pc_invert(batch.column("deleted"))
                active = batch.filter(keep)
                for row in active.to_pylist():
                    if not row["s3_key"]:
                        continue
                    yield {
                        "s3_key": row["s3_key"],
                        "stac_id": row["stac_id"],
                        "grid_partition": row["grid_partition"],
                        "year": row["year"],
                    }

    def count_active(self) -> int:
        """Number of non-deleted rows, streamed per location."""
        total = 0
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            pf = pq.ParquetFile(io.BytesIO(raw))
            for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["deleted"]):
                total += int(pc_sum(pc_invert(batch.column("deleted")).cast(pa.int32())).as_py())
        return total

    def items_per_day(
        self, days: int = 14, *, locations: list[str] | None = None
    ) -> list[tuple[str, int]]:
        """Items ingested per day, newest first — from ``ingested_at``.

        DuckDB groups the ``ingested_at`` column across all index locations
        out-of-core; distinct ``s3_key`` counts, so multi-cell items count
        once per day.  *locations* overrides ``self.locations()`` with
        filesystem paths / URIs readable by DuckDB (the caller maps
        store-relative keys to full paths/URIs).
        """
        import tempfile

        from .diff import DEFAULT_MAX_MEMORY, DEFAULT_REGION, _connect

        locs = locations if locations is not None else self.locations()
        if not locs:
            return []
        con = _connect(
            DEFAULT_REGION,
            DEFAULT_MAX_MEMORY,
            tempfile.gettempdir(),
            s3=any(f.startswith("s3://") for f in locs),
        )
        sql = (
            f"SELECT CAST(ingested_at AS DATE) AS d, count(DISTINCT s3_key) AS items "
            f"FROM read_parquet({locs!r}) "
            f"GROUP BY 1 ORDER BY 1 DESC LIMIT {int(days)}"
        )
        return [(str(d), int(n)) for d, n in con.execute(sql).fetchall()]

    def hash_set(self) -> set[bytes]:
        """``id_hash`` values for active (non-deleted) rows — for dedup."""
        hashes: set[bytes] = set()
        for loc in self.locations():
            try:
                raw = bytes(obstore.get(self._store, loc).bytes())
            except FileNotFoundError:
                continue
            pf = pq.ParquetFile(io.BytesIO(raw))
            for batch in pf.iter_batches(batch_size=_BATCH_SIZE, columns=["id_hash", "deleted"]):
                active = pc_filter(batch.column("id_hash"), pc_invert(batch.column("deleted")))
                for h in active.to_pylist():
                    if h is not None:
                        hashes.add(bytes(h))
        return hashes


def count_active_items(index_path: str, store=None) -> int:
    """Number of active (non-deleted) rows in the index at *index_path*.

    *index_path* may be an ``s3://`` URI (read through the bucket-level
    *store*) or a local filesystem path (parts layout ``{base}/`` or legacy
    ``{base}.parquet``).  Returns 0 when the path is unusable — best-effort
    by design (info display must never crash on a missing index).
    """
    from obstore.store import LocalStore

    if index_path.startswith("s3://"):
        if store is None:
            return 0
        from .stats import parse_s3_uri

        parsed = parse_s3_uri(index_path)
        if not parsed or not parsed[1]:
            return 0
        return Index(store, parsed[1]).count_active()

    p = Path(index_path)
    if not p.exists() and not p.with_suffix("").is_dir():
        return 0
    return Index(LocalStore(str(p.parent)), p.name).count_active()
