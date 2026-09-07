"""Unit tests for the DuckDB diff engine (earthcatalog/diff.py).

End-to-end coverage (diff → ingest → GC) lives in test_e2e_incremental.py;
these pin the input-resolution forms and the exactness of the EXCEPT.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from earthcatalog.diff import resolve_files, run_diff

_LM = datetime(2026, 9, 5, 1, 0, tzinfo=timezone.utc)


def _write_inventory(path: Path, keys: list[str], sizes: list[int] | None = None) -> str:
    sizes = sizes or [100] * len(keys)
    tbl = pa.table(
        {
            "bucket": ["data-bucket"] * len(keys),
            "key": keys,
            "size": sizes,
            "last_modified_date": [_LM] * len(keys),
        }
    )
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    path.write_bytes(buf.getvalue())
    return str(path)


def _keys_of(parquet_path: str) -> set[str]:
    t = pq.read_table(parquet_path)
    return set(t.column("key").to_pylist())


def test_resolve_files_from_txt_list(tmp_path):
    listing = tmp_path / "day.files"
    listing.write_text(
        "s3://bucket/data/a.parquet\n\ns3://bucket/data/b.parquet\n"
    )
    assert resolve_files(str(listing)) == [
        "s3://bucket/data/a.parquet",
        "s3://bucket/data/b.parquet",
    ]


def test_resolve_files_from_manifest(tmp_path):
    manifest = {
        "sourceBucket": "its-live-data",
        "destinationBucket": "arn:aws:s3:::inv-bucket",
        "files": [{"key": "data/a.parquet"}, {"key": "data/b.parquet"}],
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    assert resolve_files(str(p)) == [
        "s3://inv-bucket/data/a.parquet",
        "s3://inv-bucket/data/b.parquet",
    ]


def test_resolve_files_passthrough_glob():
    assert resolve_files("s3://bucket/data/*.parquet") == ["s3://bucket/data/*.parquet"]


def test_diff_is_exact_and_tuple_keyed(tmp_path):
    """EXCEPT compares (key, size, last_modified): a re-uploaded key with a
    new size shows up on BOTH sides; an untouched key on neither."""
    day1 = _write_inventory(
        tmp_path / "day1.parquet",
        ["a.stac.json", "b.stac.json", "c.stac.json"],
        [1, 2, 3],
    )
    day2 = _write_inventory(
        tmp_path / "day2.parquet",
        ["b.stac.json", "c.stac.json", "d.stac.json"],
        [2, 30, 4],
    )
    out_new = str(tmp_path / "new.parquet")
    out_old = str(tmp_path / "old.parquet")

    result = run_diff(current=day2, previous=day1, out=out_new, out_old=out_old)

    assert result.new_rows == 2  # c (changed) + d (new)
    assert result.old_rows == 2  # a (removed) + c (changed)
    assert _keys_of(out_new) == {"c.stac.json", "d.stac.json"}
    assert _keys_of(out_old) == {"a.stac.json", "c.stac.json"}


def test_diff_suffix_filter(tmp_path):
    day1 = _write_inventory(tmp_path / "day1.parquet", ["a.stac.json", "a.nc"])
    day2 = _write_inventory(tmp_path / "day2.parquet", ["b.stac.json", "b.nc"])
    out = str(tmp_path / "new.parquet")
    result = run_diff(current=day2, previous=day1, out=out)
    assert result.new_rows == 1
    assert _keys_of(out) == {"b.stac.json"}


def test_diff_against_index_mode(tmp_path):
    from earthcatalog.index import Index
    from obstore.store import MemoryStore

    day = _write_inventory(
        tmp_path / "day.parquet", ["known.stac.json", "fresh.stac.json"]
    )
    index = Index(MemoryStore(), "idx.parquet")
    index.append(
        [
            {
                "stac_id": "k",
                "s3_key": "s3://data-bucket/known.stac.json",
                "grid_partition": "c",
                "year": 2020,
            }
        ]
    )
    # Materialise the index to a local parquet the diff engine can read.
    idx_path = str(tmp_path / "index.parquet")
    part = index.locations()[0]  # parts model: data lives under {base}/
    raw = bytes(index._store.get(part).bytes())
    Path(idx_path).write_bytes(raw)

    out = str(tmp_path / "new.parquet")
    result = run_diff(current=day, out=out, against_index=idx_path)
    assert result.new_rows == 1
    assert _keys_of(out) == {"fresh.stac.json"}


def test_run_diff_requires_exactly_one_mode(tmp_path):
    day = _write_inventory(tmp_path / "day.parquet", ["a.stac.json"])
    try:
        run_diff(current=day, out=str(tmp_path / "o.parquet"))
        raised = False
    except ValueError:
        raised = True
    assert raised
    try:
        run_diff(
            current=day,
            out=str(tmp_path / "o.parquet"),
            previous=day,
            against_index=str(tmp_path / "i.parquet"),
        )
        raised = False
    except ValueError:
        raised = True
    assert raised
