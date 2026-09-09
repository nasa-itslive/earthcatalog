"""Backfill unified-index pointer coverage from the physical warehouse.

The historical index carries roughly one pointer row per granule while the
warehouse holds one record copy per (granule x cell) — the fan-out spec.
This module recomputes the missing (granule x cell) pointers by anti-joining
a warehouse scan against the existing index and emits the difference as new
additive index parts.

Everything runs against local copies first: stage the index parts into a
work dir, cache the warehouse scan once (the only remote step, read-only),
then build and verify locally.  Upload to the live index is a separate,
explicit step; rollback deletes exactly the emitted parts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import obstore
from obstore.store import ObjectStore

from .index import Index, hash_id

_TRIPLES_FILE = "warehouse_triples.parquet"
_MANIFEST_FILE = "manifest.json"


# ---------------------------------------------------------------------------
# Staging: local copies of the current index parts
# ---------------------------------------------------------------------------


def stage_index_parts(store: ObjectStore, index_key: str, work_dir: Path) -> list[str]:
    """Download every index part into *work_dir*/index preserving relative keys.

    Returns the staged store-relative keys.  Existing local files are kept
    (idempotent re-runs skip the download).
    """
    idx = Index(store, index_key)
    dest_root = work_dir / "index"
    dest_root.mkdir(parents=True, exist_ok=True)
    staged: list[str] = []
    for loc in idx.locations():
        dest = dest_root / loc
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            dest.write_bytes(bytes(obstore.get(store, loc).bytes()))
        staged.append(loc)
    return staged


def staged_locations(work_dir: Path) -> list[str]:
    """Local part paths under *work_dir*/index, as ``read_parquet`` inputs."""
    root = work_dir / "index"
    return sorted(str(p) for p in root.rglob("*.parquet"))


# ---------------------------------------------------------------------------
# Warehouse scan (the one remote, read-only step) -> local triples cache
# ---------------------------------------------------------------------------


def scan_warehouse(
    warehouse_store: ObjectStore,
    warehouse_root: str,
    work_dir: Path,
    con,
) -> dict:
    """Cache (stac_id, grid_partition, year) for every warehouse record.

    Reads only the ``id`` column of each warehouse file; cell and year come
    from the hive path.  Skips the scan when the cache already exists.
    """
    from .rebuild import _list_warehouse_keys

    cache = work_dir / _TRIPLES_FILE
    if cache.exists():
        return {"cached": True, "rows": con.execute(f"SELECT count(*) FROM read_parquet('{cache}')").fetchone()[0]}

    uris = _list_warehouse_keys(warehouse_store, warehouse_root)
    if not uris:
        raise RuntimeError("no warehouse parquet files found")
    loc_list = ", ".join(f"'{u}'" for u in uris)
    work_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                id AS stac_id,
                regexp_extract(filename, '(?:tile|grid_partition)=([^/]+)', 1) AS grid_partition,
                try_cast(nullif(regexp_extract(filename, '(?:year|month|day)=([^/]+)', 1), 'unknown') AS INT) AS year
            FROM read_parquet([{loc_list}], filename=true)
        ) TO '{cache}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{cache}')").fetchone()[0]
    return {"cached": False, "files": len(uris), "rows": int(rows)}


# ---------------------------------------------------------------------------
# Reporting: the dry-run numbers
# ---------------------------------------------------------------------------


def _card_sources(index_locs: list[str]) -> str:
    return ", ".join(f"'{p}'" for p in index_locs)


def report(cache: Path, index_locs: list[str], con) -> dict:
    """Dry-run: what the anti-join would add.  Writes nothing."""
    trips = str(cache)
    cards = _card_sources(index_locs)
    out: dict = {}
    out["warehouse_rows"] = con.execute(f"SELECT count(*) FROM read_parquet('{trips}')").fetchone()[0]
    out["index_rows"] = con.execute(f"SELECT count(*) FROM read_parquet([{cards}])").fetchone()[0]
    out["distinct_keys"] = con.execute(
        f"SELECT count(DISTINCT s3_key) FROM read_parquet([{cards}]) WHERE s3_key <> ''"
    ).fetchone()[0]
    out["missing_pairs"] = con.execute(
        f"""
        SELECT count(*) FROM
          (SELECT DISTINCT stac_id, grid_partition FROM read_parquet('{trips}')) w
        WHERE NOT EXISTS (
          SELECT 1 FROM (SELECT DISTINCT stac_id, grid_partition FROM read_parquet([{cards}])) c
          WHERE c.stac_id = w.stac_id AND c.grid_partition = w.grid_partition
        )
        """
    ).fetchone()[0]
    out["granules_without_key"] = con.execute(
        f"""
        SELECT count(DISTINCT w.stac_id) FROM
          (SELECT DISTINCT stac_id, grid_partition FROM read_parquet('{trips}')) w
        WHERE NOT EXISTS (
          SELECT 1 FROM (SELECT DISTINCT stac_id, s3_key FROM read_parquet([{cards}]) WHERE s3_key <> '') k
          WHERE k.stac_id = w.stac_id
        )
        """
    ).fetchone()[0]
    out["top_cells"] = [
        {"grid_partition": r[0], "missing": r[1]}
        for r in con.execute(
            f"""
            SELECT w.grid_partition, count(*) AS n FROM
              (SELECT DISTINCT stac_id, grid_partition FROM read_parquet('{trips}')) w
            WHERE NOT EXISTS (
              SELECT 1 FROM (SELECT DISTINCT stac_id, grid_partition FROM read_parquet([{cards}])) c
              WHERE c.stac_id = w.stac_id AND c.grid_partition = w.grid_partition
            )
            GROUP BY 1 ORDER BY n DESC LIMIT 5
            """
        ).fetchall()
    ]
    return out


# ---------------------------------------------------------------------------
# Build: emit the missing pointer rows as new additive parts
# ---------------------------------------------------------------------------


@dataclass
class BackfillManifest:
    run_id: str
    chunk_rows: int
    parts: list[dict] = field(default_factory=list)
    rows_written: int = 0

    def save(self, work_dir: Path) -> Path:
        path = work_dir / _MANIFEST_FILE
        path.write_text(json.dumps(self.__dict__, indent=2))
        return path

    @classmethod
    def load(cls, work_dir: Path) -> BackfillManifest:
        data = json.loads((work_dir / _MANIFEST_FILE).read_text())
        return cls(**data)


def _copy_if_absent(con, sql: str, dest: Path) -> Path:
    """Materialize one pipeline stage to *dest* (idempotent across re-runs)."""
    if not dest.exists():
        con.execute(f"COPY ({sql}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return dest


def build(
    cache: Path,
    index_locs: list[str],
    out_store: ObjectStore,
    index_key: str,
    work_dir: Path,
    con,
    *,
    run_id: str,
    chunk_rows: int = 1_000_000,
) -> BackfillManifest:
    """Emit missing (granule x cell) pointers as ``{run_id}--{seq}`` parts.

    Runs as staged, disk-backed queries so no single join materializes the
    whole catalog in memory: (1) distinct warehouse pairs, (2) the missing
    pairs after the anti-join, (3) the s3_key map restricted to the granules
    that actually need a pointer, (4) the ordered, enriched stream.  The
    final stream is ordered, so chunk boundaries are deterministic and an
    interrupted run resumes exactly where it stopped: parts already on the
    store are skipped.  New pointer rows inherit the granule's original
    ``ingested_at`` so per-day provenance is preserved.
    """
    trips = str(cache)
    cards = _card_sources(index_locs)
    wh_pairs = _copy_if_absent(
        con,
        f"SELECT DISTINCT stac_id, grid_partition, year FROM read_parquet('{trips}')",
        work_dir / "bf_wh_pairs.parquet",
    )
    missing = _copy_if_absent(
        con,
        f"""
        SELECT w.stac_id, w.grid_partition, w.year FROM read_parquet('{wh_pairs}') w
        WHERE NOT EXISTS (
          SELECT 1 FROM (SELECT DISTINCT stac_id, grid_partition FROM read_parquet([{cards}])) c
          WHERE c.stac_id = w.stac_id AND c.grid_partition = w.grid_partition
        )
        """,
        work_dir / "bf_missing.parquet",
    )
    keymap = _copy_if_absent(
        con,
        f"""
        SELECT k.stac_id, any_value(k.s3_key) AS s3_key, any_value(k.ingested_at) AS ingested_at
        FROM read_parquet([{cards}]) k
        WHERE k.s3_key <> '' AND k.stac_id IN (SELECT DISTINCT stac_id FROM read_parquet('{missing}'))
        GROUP BY 1
        """,
        work_dir / "bf_keymap.parquet",
    )
    query = f"""
        SELECT w.stac_id,
               coalesce(k.s3_key, '') AS s3_key,
               w.grid_partition,
               w.year,
               k.ingested_at
        FROM read_parquet('{missing}') w
        LEFT JOIN read_parquet('{keymap}') k USING (stac_id)
        ORDER BY w.stac_id, w.grid_partition
    """
    idx = Index(out_store, index_key)
    manifest = BackfillManifest(run_id=run_id, chunk_rows=chunk_rows)
    work_dir.mkdir(parents=True, exist_ok=True)
    reader = con.execute(query).fetch_record_batch(chunk_rows)
    seq = 0
    written = 0
    for batch in reader:
        rows = batch.to_pylist()
        n = len(rows)
        part = f"{run_id}--{seq:04d}"
        if _part_exists(out_store, index_key, part):
            print(f"  resume: {part} already present ({n} rows), skipping")
        else:
            idx.append(rows, part=part)
            print(f"  part {part}: {n} rows")
        manifest.parts.append({"part": part, "rows": n})
        manifest.rows_written += n
        written += n
        seq += 1
        rows.clear()
    manifest.save(work_dir)
    return manifest


def _part_exists(store: ObjectStore, index_key: str, part: str) -> bool:
    try:
        obstore.head(store, f"{index_key}/{part}.parquet")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Verify: the local gate, full (no sampling)
# ---------------------------------------------------------------------------


def verify(cache: Path, index_locs: list[str], con) -> dict:
    """Full cards-vs-copies verification over staged + built parts."""
    trips = str(cache)
    cards = _card_sources(index_locs)
    out: dict = {}
    out["index_rows"] = con.execute(f"SELECT count(*) FROM read_parquet([{cards}])").fetchone()[0]
    out["warehouse_rows"] = con.execute(f"SELECT count(*) FROM read_parquet('{trips}')").fetchone()[0]
    out["distinct_keys"] = con.execute(
        f"SELECT count(DISTINCT s3_key) FROM read_parquet([{cards}]) WHERE s3_key <> ''"
    ).fetchone()[0]
    out["duplicate_pairs"] = con.execute(
        f"""
        SELECT count(*) FROM (
            SELECT s3_key, grid_partition FROM read_parquet([{cards}]) WHERE s3_key <> ''
            GROUP BY 1, 2 HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    row = con.execute(
        f"""
        WITH card_pairs AS (SELECT DISTINCT stac_id, grid_partition FROM read_parquet([{cards}])),
             copy_pairs AS (SELECT DISTINCT stac_id, grid_partition FROM read_parquet('{trips}'))
        SELECT
          (SELECT count(*) FROM (SELECT * FROM card_pairs EXCEPT SELECT * FROM copy_pairs)),
          (SELECT count(*) FROM (SELECT * FROM copy_pairs EXCEPT SELECT * FROM card_pairs))
        """
    ).fetchone()
    out["cards_without_copy"] = int(row[0])
    out["copies_without_card"] = int(row[1])
    return out


# ---------------------------------------------------------------------------
# Upload / rollback: the only steps that touch the live index
# ---------------------------------------------------------------------------


def upload(manifest: BackfillManifest, src_store: ObjectStore, index_key: str, dest_store: ObjectStore) -> int:
    """Copy the manifest's parts from the local staging store to the live index."""
    total = 0
    for entry in manifest.parts:
        key = f"{index_key}/{entry['part']}.parquet"
        data = bytes(obstore.get(src_store, key).bytes())
        obstore.put(dest_store, key, data)
        total += entry["rows"]
    return total


def rollback(manifest: BackfillManifest, dest_store: ObjectStore, index_key: str) -> int:
    """Delete exactly the parts named in the manifest from the live index."""
    removed = 0
    for entry in manifest.parts:
        try:
            obstore.delete(dest_store, f"{index_key}/{entry['part']}.parquet")
            removed += 1
        except Exception:
            pass
    return removed


__all__ = [
    "BackfillManifest",
    "build",
    "hash_id",
    "report",
    "rollback",
    "scan_warehouse",
    "stage_index_parts",
    "staged_locations",
    "upload",
    "verify",
]
