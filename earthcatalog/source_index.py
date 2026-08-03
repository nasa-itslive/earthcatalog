"""
Source index for the earthcatalog warehouse.

The source index (``{warehouse_root}_source_index.parquet``) tracks the
provenance of every STAC item in the catalog: which S3 object (``s3_key``)
each item was ingested from, its STAC ``id``, and the ``(grid_partition,
year)`` partition it was written into.

It is the bridge between two keyspaces:

- The S3 Inventory lists ``(bucket, key)`` object paths — the S3 truth.
- The hash index stores xxh3_128 digests of STAC item ``id`` values — the
  catalog truth.

During weekly garbage collection the source index lets us find orphaned
items without fetching any STAC JSON: stream the current S3 Inventory into
a Bloom filter, then stream the source index and flag every ``s3_key`` that
is definitely absent from S3.  ``grid_partition`` + ``year`` then point
directly at the GeoParquet files that need to be rewritten.

Public API
----------
append_source_index(rows, store, key) -> int
    Append provenance rows (create the file on first use).

mark_deleted(stac_ids, store, key) -> int
    Flag rows for the given STAC IDs as deleted (soft delete).

compact_source_index(store, key) -> int
    Physically drop ``deleted=True`` rows and rewrite the file.

stream_active(store, key) -> Iterator[dict]
    Yield non-deleted rows as dicts without loading the whole file.
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import obstore
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_BATCH_SIZE = 100_000

_SCHEMA = pa.schema(
    [
        pa.field("s3_key", pa.string()),
        pa.field("stac_id", pa.string()),
        pa.field("grid_partition", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("ingested_at", pa.string()),
        pa.field("deleted", pa.bool_()),
    ]
)


def _read_table(store: object, key: str) -> pa.Table | None:
    """Return the current source index as a Table, or ``None`` if absent."""
    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return None
    return pq.ParquetFile(io.BytesIO(raw)).read()


def append_source_index(
    rows: list[tuple[str, str, str, int]],
    store: object,
    key: str,
) -> int:
    """
    Append provenance rows to the source index at *store*/*key*.

    *rows* is a list of ``(s3_key, stac_id, grid_partition, year)`` tuples.
    The file is created on first use.  Returns the total number of rows now
    in the index.

    The single-file read+concat+write keeps the layout simple; at ~140k
    rows/week appended it stays small for years.
    """
    if not rows:
        return _total_rows(_read_table(store, key))

    now = datetime.now(UTC).isoformat()
    tbl = pa.table(
        {
            "s3_key": [r[0] for r in rows],
            "stac_id": [r[1] for r in rows],
            "grid_partition": [r[2] for r in rows],
            "year": pa.array([r[3] for r in rows], type=pa.int32()),
            "ingested_at": pa.array([now] * len(rows)),
            "deleted": pa.array([False] * len(rows)),
        },
        schema=_SCHEMA,
    )

    existing = _read_table(store, key)
    if existing is not None:
        merged = pa.concat_tables([existing, tbl])
    else:
        merged = tbl

    buf = io.BytesIO()
    pq.write_table(merged, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return merged.num_rows


def mark_deleted(stac_ids: set[str], store: object, key: str) -> int:
    """
    Set ``deleted=True`` for every row whose ``stac_id`` is in *stac_ids*.

    Returns the number of rows marked.  Already-deleted rows are a no-op.
    """
    if not stac_ids:
        return 0
    existing = _read_table(store, key)
    if existing is None:
        return 0

    col = existing.column("stac_id")
    marked = pc.is_in(col, pa.array(list(stac_ids)))
    if not pc.any(marked).as_py():
        return 0

    deleted_col = pc.or_(existing.column("deleted"), marked)
    existing = existing.set_column(
        existing.schema.get_field_index("deleted"),
        "deleted",
        deleted_col,
    )

    buf = io.BytesIO()
    pq.write_table(existing, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return int(pc.sum(marked).as_py())


def compact_source_index(store: object, key: str) -> int:
    """
    Physically remove ``deleted=True`` rows and rewrite the file.

    Returns the number of active (non-deleted) rows retained.
    """
    existing = _read_table(store, key)
    if existing is None:
        return 0

    active = existing.filter(pc.invert(existing.column("deleted")))
    buf = io.BytesIO()
    pq.write_table(active, buf, compression="zstd")
    obstore.put(store, key, buf.getvalue())
    return active.num_rows


def stream_active(store: object, key: str) -> Iterator[dict]:
    """
    Yield non-deleted rows as dicts: ``{s3_key, stac_id, grid_partition, year}``.

    Streams in row-group batches so peak memory is bounded regardless of
    file size.  Rows with an empty ``s3_key`` (legacy items ingested before
    provenance tracking was enabled) are skipped.
    """
    existing = _read_table(store, key)
    if existing is None:
        return

    keep = existing.filter(pc.invert(existing.column("deleted")))
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


def _total_rows(tbl: pa.Table | None) -> int:
    return tbl.num_rows if tbl is not None else 0


def partition_key(cell: str, year: int | None) -> str:
    """Return the hive-style warehouse directory for a (cell, year) pair."""
    year_str = str(year) if year is not None else "unknown"
    return f"grid_partition={cell}/year={year_str}"


def new_part_key(cell: str, year: int | None) -> str:
    """Return a fresh GeoParquet key in the (cell, year) partition."""
    year_str = str(year) if year is not None else "unknown"
    return f"grid_partition={cell}/year={year_str}/gc_{uuid.uuid4().hex[:8]}.parquet"
