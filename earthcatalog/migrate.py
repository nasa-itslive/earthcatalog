"""
One-shot migration of legacy index files to the unified index.

Legacy warehouses carry up to two sidecar files beside the warehouse:

* ``{warehouse}_id_hashes.parquet`` — a single ``id_hash`` column
  (xxh3_128 of the STAC id).  Dedup data only: no s3_key, so resume and
  GC cannot be served from it.
* ``{warehouse}_source_index.parquet`` — full provenance rows
  (``s3_key, stac_id, grid_partition, year, ingested_at, deleted``);
  the unified index absorbed this schema verbatim, minus ``id_hash``,
  which is re-derived from ``stac_id``.

:meth:`migrate_indices` merges both into ``{warehouse}_index.parquet``,
writing a validated sidecar first and swapping it in with a single
atomic PUT.  Legacy files are kept until a green GC run confirms the
new index.  Idempotent: a table already carrying
``earthcatalog.index_path`` is reported, not re-migrated.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import ObjectStore

from .index import _SCHEMA, hash_id
from .schema import FULL_NAME, PROP_INDEX_PATH

if TYPE_CHECKING:
    pass


def _strip(uri: str) -> str:
    return uri.removeprefix("s3://").split("/", 1)[1] if uri.startswith("s3://") else uri


def _store_key(warehouse_root: str, suffix: str) -> str:
    """Store-relative key for ``{warehouse_root}{suffix}``.

    S3: the bucket-level store needs the path *within* the bucket.  Local:
    the LocalStore root *is* the warehouse directory, so only the final
    segment survives.
    """
    if warehouse_root.startswith("s3://"):
        return warehouse_root.removeprefix("s3://").split("/", 1)[1].rstrip("/") + suffix
    return warehouse_root.rstrip("/").rsplit("/", 1)[-1] + suffix


def _read_optional_parquet(store: ObjectStore, key: str) -> pa.Table | None:
    try:
        raw = bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return None
    return pq.ParquetFile(io.BytesIO(raw)).read()


def migrate_indices(
    catalog,
    store: ObjectStore,
    warehouse_root: str,
    *,
    dry_run: bool = False,
) -> dict:
    """Merge legacy ``*_id_hashes`` / ``*_source_index`` files into the
    unified index; stamp ``earthcatalog.index_path`` on the table.

    Returns a report: status, rows contributed per source, validation
    counts.
    """
    warehouse = warehouse_root.rstrip("/")
    index_key = _store_key(warehouse, "_index.parquet")
    hashes_key = _store_key(warehouse, "_id_hashes.parquet")
    source_key = _store_key(warehouse, "_source_index.parquet")

    table = catalog.load_table(FULL_NAME)
    if table.properties.get(PROP_INDEX_PATH):
        return {"status": "already-migrated", "index_key": index_key}

    source_tbl = _read_optional_parquet(store, source_key)
    hashes_tbl = _read_optional_parquet(store, hashes_key)

    rows: list[dict] = []
    source_rows = 0
    if source_tbl is not None:
        source_rows = source_tbl.num_rows
        ids = source_tbl.column("stac_id").to_pylist()
        s3_keys = source_tbl.column("s3_key").to_pylist()
        cells = source_tbl.column("grid_partition").to_pylist()
        years = source_tbl.column("year").to_pylist()
        stamps = (
            source_tbl.column("ingested_at").to_pylist()
            if "ingested_at" in source_tbl.column_names
            else [""] * source_rows
        )
        deleted = (
            source_tbl.column("deleted").to_pylist()
            if "deleted" in source_tbl.column_names
            else [False] * source_rows
        )
        for stac_id, s3_key, cell, year, stamp, dele in zip(
            ids, s3_keys, cells, years, stamps, deleted
        ):
            rows.append(
                {
                    "id_hash": hash_id(stac_id or ""),
                    "s3_key": s3_key or "",
                    "stac_id": stac_id or "",
                    "grid_partition": cell or "",
                    "year": int(year or 0),
                    "ingested_at": stamp or "",
                    "deleted": bool(dele),
                }
            )

    # Hash-only rows: dedup protection for items whose provenance was never
    # recorded.  Known hashes (from the source index) are skipped.
    known = (
        {bytes(h) for h in source_tbl.column("id_hash").to_pylist()}
        if (source_tbl is not None and "id_hash" in source_tbl.column_names)
        else set()
    )
    known.update(hash_id(r["stac_id"]) for r in rows if r["stac_id"])
    hash_only = 0
    if hashes_tbl is not None:
        for h in hashes_tbl.column("id_hash").to_pylist():
            hb = bytes(h)
            if hb in known:
                continue
            known.add(hb)
            rows.append(
                {
                    "id_hash": hb,
                    "s3_key": "",
                    "stac_id": "",
                    "grid_partition": "",
                    "year": 0,
                    "ingested_at": "",
                    "deleted": False,
                }
            )
            hash_only += 1

    expected = source_rows + hash_only
    report = {
        "status": "dry-run" if dry_run else "migrated",
        "index_key": index_key,
        "source_rows": source_rows,
        "hash_only_rows": hash_only,
        "expected_rows": expected,
    }
    if dry_run:
        return report

    tbl = pa.Table.from_pylist(rows, schema=_SCHEMA)
    if tbl.num_rows != expected:
        raise RuntimeError(
            f"migration validation failed: wrote {tbl.num_rows} rows, expected {expected}"
        )

    # Validated sidecar first, then a single atomic PUT into place.  The
    # legacy files stay until a green GC run confirms the new index.
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    payload = buf.getvalue()
    sidecar_key = f"{index_key}.migrating"
    obstore.put(store, sidecar_key, payload)
    written = pq.ParquetFile(io.BytesIO(payload)).metadata.num_rows
    if written != expected:
        raise RuntimeError(f"sidecar validation failed: {written} rows, expected {expected}")
    obstore.put(store, index_key, payload)
    obstore.delete(store, sidecar_key)

    with table.transaction() as tx:
        tx.set_properties(**{PROP_INDEX_PATH: f"{warehouse}_index.parquet"})
    report["total_rows"] = tbl.num_rows
    return report
