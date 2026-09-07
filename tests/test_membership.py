"""Tests for the DuckDB membership anti-join (diff.anti_join).

The unified index is the resume checkpoint: the anti-join yields exactly the
(bucket, key) pairs the index does not already know, streaming, out-of-core.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from earthcatalog.diff import anti_join


_LM = datetime(2026, 9, 5, 1, 0, tzinfo=timezone.utc)


def _write_parquet(path: Path, table: pa.Table) -> str:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    path.write_bytes(buf.getvalue())
    return str(path)


def _left_parquet(path: Path, keys: list[str]) -> str:
    return _write_parquet(
        path,
        pa.table(
            {
                "bucket": ["data-bucket"] * len(keys),
                "key": keys,
                "size": [1] * len(keys),
                "last_modified_date": [_LM] * len(keys),
            }
        ),
    )


def _index_parquet(path: Path, keys: list[str], deleted: list[bool] | None = None) -> str:
    import xxhash

    def hid(s: str) -> bytes:
        return xxhash.xxh3_128(s.encode(), seed=42).digest()

    n = len(keys)
    return _write_parquet(
        path,
        pa.table(
            {
                "id_hash": pa.array([hid(k) for k in keys], type=pa.binary(16)),
                "s3_key": [f"s3://data-bucket/{k}" for k in keys],
                "stac_id": keys,
                "grid_partition": ["cellA"] * n,
                "year": pa.array([2020] * n, type=pa.int32()),
                "ingested_at": [""] * n,
                "deleted": pa.array(deleted or [False] * n, type=pa.bool_()),
            }
        ),
    )


def test_antijoin_parquet_left(tmp_path: Path):
    left = _left_parquet(tmp_path / "left.parquet", ["a.stac.json", "b.stac.json"])
    index = _index_parquet(tmp_path / "index.parquet", ["a.stac.json"])

    got = list(anti_join(left, index))
    assert got == [("data-bucket", "b.stac.json")]


def test_antijoin_iterable_left(tmp_path: Path):
    index = _index_parquet(tmp_path / "index.parquet", ["a.stac.json"])
    pairs = [("data-bucket", "a.stac.json"), ("data-bucket", "b.stac.json")]

    assert list(anti_join(iter(pairs), index)) == [("data-bucket", "b.stac.json")]


def test_antijoin_deleted_rows_are_unknown(tmp_path: Path):
    """A GC'd (soft-deleted) key re-offered by the inventory is new again."""
    left = _left_parquet(tmp_path / "left.parquet", ["a.stac.json"])
    index = _index_parquet(
        tmp_path / "index.parquet", ["a.stac.json"], deleted=[True]
    )
    assert list(anti_join(left, index)) == [("data-bucket", "a.stac.json")]


def test_antijoin_suffix_filter(tmp_path: Path):
    left = _left_parquet(tmp_path / "left.parquet", ["a.stac.json", "a.nc", "b.stac.json"])
    index = _index_parquet(tmp_path / "index.parquet", [])

    got = list(anti_join(left, index))
    assert sorted(k for _, k in got) == ["a.stac.json", "b.stac.json"]


def test_antijoin_limit(tmp_path: Path):
    left = _left_parquet(tmp_path / "left.parquet", ["a.stac.json", "b.stac.json", "c.stac.json"])
    index = _index_parquet(tmp_path / "index.parquet", [])

    got = list(anti_join(left, index, limit=2))
    assert len(got) == 2


def test_antijoin_index_as_path_list(tmp_path: Path):
    """A multi-part index (parts + legacy file) is one read_parquet list."""
    left = _left_parquet(tmp_path / "left.parquet", ["a.stac.json", "b.stac.json", "c.stac.json"])
    part1 = _index_parquet(tmp_path / "idx_part1.parquet", ["a.stac.json"])
    part2 = _index_parquet(tmp_path / "idx_part2.parquet", ["b.stac.json"])

    got = list(anti_join(left, [part1, part2]))
    assert got == [("data-bucket", "c.stac.json")]


def test_antijoin_batched_left_matches_brute_force(tmp_path: Path):
    known = [f"{i}.stac.json" for i in range(0, 100, 2)]
    index = _index_parquet(tmp_path / "index.parquet", known)
    left = _left_parquet(tmp_path / "left.parquet", [f"{i}.stac.json" for i in range(100)])

    got = list(anti_join(left, index, batch_size=7))
    assert {k for _, k in got} == {f"{i}.stac.json" for i in range(1, 100, 2)}


def test_antijoin_rejects_both_left_forms(tmp_path: Path):
    index = _index_parquet(tmp_path / "index.parquet", [])
    with pytest.raises(ValueError):
        anti_join(None, index)
