"""
Inventory-vs-inventory (and inventory-vs-index) diffing via DuckDB.

The daily diff is two out-of-core ``EXCEPT`` queries: DuckDB reads both
inventories directly from S3 (column-pruned, hash-spilling under a memory
cap), so exactness is string comparison — no hash-collision caveat — and
neither the inventories nor the diff ever materialise on the runner beyond
the spill directory.

Outputs are ``(bucket, key, size, last_modified_date)`` Parquet files:

* ``new``: tuples present in *current* but not in *previous* — re-uploads
  under an unchanged key show up here too (changed ``size``/``last_modified``).
* ``old``: the reverse direction — keys that disappeared; GC input later.

``ingest --diff`` then anti-joins the ``new`` file against the unified
index (``keydiff.iter_new_keys``), so already-ingested keys are skipped
and only truly unknown keys are fetched.
"""

from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass, field

DEFAULT_REGION = "us-west-2"
DEFAULT_MAX_MEMORY = "10GB"

_DIFF_COLUMNS = "bucket, key, size, last_modified_date"


@dataclass
class DiffResult:
    """Counts and timings for one diff run."""

    new_rows: int | None = None
    old_rows: int | None = None
    seconds: float = 0.0
    steps: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "new_rows": self.new_rows,
            "old_rows": self.old_rows,
            "seconds": round(self.seconds, 1),
            "steps": {k: round(v, 1) for k, v in self.steps.items()},
        }


def resolve_files(source: str) -> list[str]:
    """Resolve *source* to a list of inventory Parquet URIs.

    Three forms are accepted:

    * ``manifest.json`` (path or s3:// URI) — data files are ``files[].key``
      resolved against the manifest's own bucket (AWS inventory layout).
    * a text file (``.txt``/``.files``/``.list``) with one Parquet URI per
      line, used verbatim — for mirrored/relocated inventory data.
    * anything else is passed to ``read_parquet`` as-is (a single Parquet
      path or a ``*`` glob).
    """
    if source.endswith(".json"):
        return _files_from_manifest(source)
    if source.endswith((".txt", ".files", ".list")):
        return _files_from_list(source)
    return [source]


def _files_from_manifest(manifest_uri: str) -> list[str]:
    import obstore

    if manifest_uri.startswith("s3://"):
        no_scheme = manifest_uri.removeprefix("s3://")
        bucket, key = no_scheme.split("/", 1)
        store = _s3_store(bucket)
        raw = bytes(obstore.get(store, key).bytes())
    else:
        raw = open(manifest_uri, "rb").read()
    manifest = json.loads(raw)
    src_bucket = manifest.get("sourceBucket", "")
    arn = manifest.get("destinationBucket", "")
    bucket = arn.split(":::")[1] if ":::" in arn else (src_bucket or "")
    files = []
    for f in manifest.get("files", []):
        k = f["key"]
        files.append(k if k.startswith("s3://") else f"s3://{bucket}/{k}")
    if not files:
        raise ValueError(f"manifest lists no data files: {manifest_uri}")
    return files


def _files_from_list(list_uri: str) -> list[str]:
    import obstore

    if list_uri.startswith("s3://"):
        no_scheme = list_uri.removeprefix("s3://")
        bucket, key = list_uri.split("/", 1)
        store = _s3_store(bucket)
        text = bytes(obstore.get(store, key).bytes()).decode()
    else:
        text = open(list_uri).read()
    files = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not files:
        raise ValueError(f"file list is empty: {list_uri}")
    return files


def _s3_store(bucket: str):
    from obstore.store import S3Store

    import os

    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or DEFAULT_REGION
    return S3Store(bucket=bucket, region=region)


def _read_parquet(files: list[str], suffix: str) -> str:
    """A relation selecting the diff tuple columns, filtered to *suffix*."""
    return (
        f"SELECT {_DIFF_COLUMNS} FROM read_parquet({files!r}) "
        f"WHERE key LIKE '%{suffix}'"
    )


def _connect(region: str, max_memory: str, temp_directory: str, *, s3: bool):
    import duckdb

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false;")
    con.execute(f"SET max_memory='{max_memory}';")
    con.execute(f"SET temp_directory='{temp_directory}';")
    if s3:
        con.execute("INSTALL aws; LOAD aws;")
        con.execute(f"SET s3_region='{region}';")
        # Anonymous reads for public buckets; env/chain creds are picked up
        # for writes when present.
        try:
            con.execute("CALL load_aws_credentials();")
        except Exception:
            pass
    return con


def run_diff(
    current: str,
    out: str,
    previous: str | None = None,
    against_index: str | None = None,
    out_old: str | None = None,
    suffix: str = ".stac.json",
    region: str = DEFAULT_REGION,
    max_memory: str = DEFAULT_MAX_MEMORY,
    temp_directory: str | None = None,
) -> DiffResult:
    """Diff one inventory against another (or against the index) and write
    the result as Parquet to *out*.

    Give exactly one of *previous* (day-over-day ``EXCEPT``) or
    *against_index* (first-run / bulk mode: keys not yet in the unified
    index).  *out_old* (with *previous*) writes the disappeared-keys file.
    All paths accept ``s3://`` URIs or local paths; ``.json`` inputs are
    AWS manifests, ``.txt``/``.files``/``.list`` inputs are URI-per-line
    lists, anything else is a Parquet path or glob.
    """
    if (previous is None) == (against_index is None):
        raise ValueError("give exactly one of `previous` or `against_index`")
    temp_directory = temp_directory or tempfile.gettempdir()

    t0 = time.perf_counter()
    result = DiffResult()
    cur_files = resolve_files(current)
    cur = _read_parquet(cur_files, suffix)
    s3 = any(f.startswith("s3://") for f in cur_files)
    if previous is not None:
        prev_files = resolve_files(previous)
        prev = _read_parquet(prev_files, suffix)
        s3 = s3 or any(f.startswith("s3://") for f in prev_files)
    s3 = s3 or (against_index or "").startswith("s3://") or out.startswith("s3://") or (
        out_old or ""
    ).startswith("s3://")
    con = _connect(region, max_memory, temp_directory, s3=s3)

    if previous is not None:
        con.execute(f"COPY ({cur} EXCEPT {prev}) TO '{out}' (FORMAT PARQUET)")
        result.new_rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0])
        if out_old:
            con.execute(f"COPY ({prev} EXCEPT {cur}) TO '{out_old}' (FORMAT PARQUET)")
            result.old_rows = int(
                con.execute(f"SELECT count(*) FROM read_parquet('{out_old}')").fetchone()[0]
            )
    else:
        anti = (
            f"SELECT {_DIFF_COLUMNS} FROM ({cur}) c "
            f"ANTI JOIN (SELECT s3_key FROM read_parquet('{against_index}') "
            f"WHERE NOT deleted) i ON i.s3_key = 's3://' || c.bucket || '/' || c.key"
        )
        con.execute(f"COPY ({anti}) TO '{out}' (FORMAT PARQUET)")
        result.new_rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0])

    result.seconds = time.perf_counter() - t0
    return result
