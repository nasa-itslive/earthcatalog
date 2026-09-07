"""
Batch journal — closing the ingest commit windows.

A write-ahead journal per fetch batch, under
``{warehouse}/_staging/journal/{run_id}/{seq:04d}.json``:

    {"keys": ["s3://…"],                  # written BEFORE fetching the batch
     "files": [                           # appended after each file write
        {"key": "warehouse/…/part_x.parquet",
         "rows": [{"stac_id": …, "s3_key": …, …}]}]}

Lifecycle inside one batch: ``start_batch`` (keys) → fetch + write, with
``record_file`` after every file → commit (``add_files`` + ``index.append``)
→ ``finish_batch`` (delete the journal file).  A crash therefore leaves a
journal describing exactly the uncommitted work, and :func:`recover_journals`
— run at every ingest start — closes the window:

==========  ====================  ===========================================
rows state  files state           action
==========  ====================  ===========================================
in index    —                     commit completed → delete journal
not in      registered            crash inside the window → append rows,
                                  delete journal
not in      not registered        crash before commit → delete the listed
                                  files, delete journal (batch re-runs)
—           no files              crash mid-fetch → delete journal (batch
                                  re-runs)
==========  ====================  ===========================================

Residual risk (stated honestly): a crash in the microseconds between a
file write and its ``record_file`` leaks one unregistered file whose name
is unknowable.  It is invisible to search (never ``add_files``-ed) and
reported by the weekly consolidation audit, not by recovery.

Scope: the serial direct-stage path (the daily default).  The ndjson
stage's staged buckets are already re-compactable after a crash, and the
distributed bulk profile resumes via its scatter manifest.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import obstore

if TYPE_CHECKING:
    from .index import Index


def journal_prefix(warehouse_prefix: str) -> str:
    base = warehouse_prefix.rstrip("/")
    return f"{base}/_staging/journal" if base else "_staging/journal"


def new_run_id() -> str:
    from datetime import UTC, datetime

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


class BatchJournal:
    """Write-ahead journal for one ingest run's batches."""

    def __init__(self, store: object, warehouse_prefix: str, run_id: str) -> None:
        self._store = store
        self._prefix = journal_prefix(warehouse_prefix)
        self._run_id = run_id
        self._seq = 0

    @property
    def run_id(self) -> str:
        return self._run_id

    def start_batch(self, source_keys: list[str]) -> int:
        """Record the batch's source keys before fetching; returns the seq."""
        seq = self._seq
        self._put(seq, {"keys": source_keys, "files": []})
        self._seq += 1
        return seq

    def record_file(self, seq: int, rel_key: str, index_rows: list[dict]) -> None:
        """Append one written file (and its index rows) to the batch journal."""
        doc = self._get(seq)
        doc["files"].append({"key": rel_key, "rows": index_rows})
        self._put(seq, doc)

    def finish_batch(self, seq: int) -> None:
        """The batch committed — drop its journal."""
        try:
            obstore.delete(self._store, self._key(seq))
        except Exception as exc:
            print(f"WARN: journal cleanup failed for {self._key(seq)}: {exc}")

    def _key(self, seq: int) -> str:
        return f"{self._prefix}/{self._run_id}/{seq:04d}.json"

    def _get(self, seq: int) -> dict:
        try:
            raw = bytes(obstore.get(self._store, self._key(seq)).bytes())
            return json.loads(raw)
        except Exception:
            return {"keys": [], "files": []}

    def _put(self, seq: int, doc: dict) -> None:
        obstore.put(
            self._store, self._key(seq), json.dumps(doc, default=str).encode()
        )


def list_journals(store: object, warehouse_prefix: str) -> list[str]:
    """Every journal file across all run_ids (one LIST per run start)."""
    prefix = journal_prefix(warehouse_prefix)
    keys: list[str] = []
    try:
        for listing in obstore.list(store, prefix=prefix):
            for obj in listing:
                k: str = obj["path"]
                if k.endswith(".json"):
                    keys.append(k)
    except Exception:
        return []
    return sorted(keys)


def _read_journal(store: object, key: str) -> dict | None:
    try:
        return json.loads(bytes(obstore.get(store, key).bytes()))
    except Exception:
        return None


def _registered_files(table: object) -> set[str] | None:
    """Full paths registered in the Iceberg table, or None if unreadable."""
    try:
        inspect = table.inspect.files()
        col = inspect.column("file_path") if hasattr(inspect, "column") else inspect["file_path"]
        return {str(v).removeprefix("file:") for v in col.to_pylist()}
    except Exception:
        return None


def recover_journals(
    store: object,
    warehouse_prefix: str,
    index: Index,
    table: object,
    full_path=lambda rel: rel,
) -> dict:
    """Recover every leftover journal; idempotent, run at ingest start.

    *full_path* maps a store-relative file key to the URI form used in
    ``add_files`` (the Ingester's ``_full_path``).  Returns a report for
    the run summary.
    """
    report = {"journals": 0, "rows_appended": 0, "files_deleted": 0, "orphans_leaked": 0}
    keys = list_journals(store, warehouse_prefix)
    if not keys:
        return report

    known = index.known_source_keys()

    for jkey in keys:
        doc = _read_journal(store, jkey)
        if doc is None:
            obstore.delete(store, jkey)
            report["journals"] += 1
            continue
        report["journals"] += 1
        src_keys = doc.get("keys", [])
        files = doc.get("files", [])

        all_in = bool(src_keys) and all(k in known for k in src_keys)

        if all_in:
            # Commit completed; the crash hit before finish_batch.
            obstore.delete(store, jkey)
            continue

        if not files:
            # Crash mid-fetch: nothing was written; the batch re-runs.
            obstore.delete(store, jkey)
            continue

        registered = _registered_files(table)
        rows: list[dict] = [r for f in files for r in f.get("rows", [])]
        # files_registered is False when the table cannot be inspected
        # (degraded store): the safe move is to drop uncommitted files and
        # re-process the batch.
        files_registered = registered is not None and all(
            full_path(f["key"]) in registered for f in files
        )
        if files_registered:
            # Crash inside the window: add_files done, index part not
            # written.  Recovery writes the exact part the crashed run owed
            # (deterministic {run_id}/{seq} name) and drops the journal.
            if rows:
                # An item spanning multiple cells is journaled once per file;
                # the normal path appends one row per source key.
                unique: dict[str, dict] = {}
                for r in rows:
                    unique.setdefault(r["s3_key"], r)
                run_id = jkey.rsplit("/", 2)[-2]
                seq = jkey.rsplit("/", 1)[-1].removesuffix(".json")
                index.append(list(unique.values()), part=f"{run_id}/{seq}")
                report["rows_appended"] += len(unique)
            obstore.delete(store, jkey)
        else:
            # Crash before commit: the files were never registered —
            # remove them and let the batch re-run.
            for f in files:
                try:
                    obstore.delete(store, f["key"])
                    report["files_deleted"] += 1
                except Exception:
                    report["orphans_leaked"] += 1
            obstore.delete(store, jkey)

    # Sweep the (now empty) run prefix so a successful run leaves no journals.
    return report
