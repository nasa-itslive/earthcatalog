"""
Tests for inventory reading: CSV, CSV.gz, and Parquet formats.

All tests use local temp files — no S3 access required.
The S3 path of iter_inventory_csv is exercised by patching
_fetch_inventory_bytes so no real network calls are made.
"""

import csv
import gzip
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.inventory import (
    InventoryShard,
    _coerce_last_modified,
    delete_scatter,
    delete_shard_files,
    is_scatter_manifest,
    iter_inventory,
    iter_inventory_csv,
    iter_inventory_manifest,
    iter_inventory_parquet,
    load_inventory_shards,
    scatter_manifest_path,
    write_inventory_shards,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

ROWS = [
    ("my-bucket", "prefix/a.stac.json"),
    ("my-bucket", "prefix/b.stac.json"),
    ("other-bucket", "path/c.stac.json"),
]

_OLD = "2026-01-01T00:00:00.000Z"
_NEW = "2026-04-20T00:00:00.000Z"
_SINCE = datetime(2026, 4, 1, tzinfo=UTC)


def _write_csv(path: Path, rows, with_header: bool = True, lm_dates=None):
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        if with_header:
            if lm_dates is not None:
                writer.writerow(["bucket", "key", "last_modified_date"])
            else:
                writer.writerow(["bucket", "key"])
        for i, row in enumerate(rows):
            if lm_dates is not None:
                writer.writerow(list(row) + [lm_dates[i]])
            else:
                writer.writerow(row)


def _write_csv_gz(path: Path, rows, with_header: bool = True):
    buf = io.StringIO()
    writer = csv.writer(buf)
    if with_header:
        writer.writerow(["bucket", "key"])
    writer.writerows(rows)
    path.write_bytes(gzip.compress(buf.getvalue().encode("utf-8")))


def _write_parquet(path: Path, rows, lm_dates=None):
    data = {
        "bucket": pa.array([r[0] for r in rows], type=pa.string()),
        "key": pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_dates is not None:
        data["last_modified_date"] = pa.array(lm_dates, type=pa.string())
    table = pa.table(data)
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# CSV tests
# ---------------------------------------------------------------------------


class TestIterInventoryCsv:
    @pytest.mark.parametrize(
        ("suffix", "with_header"),
        [
            (".csv", True),
            (".csv", False),
            (".csv.gz", True),
            (".csv.gz", False),
        ],
    )
    def test_csv_variants(self, tmp_path, suffix, with_header):
        p = tmp_path / f"inv{suffix}"
        if suffix == ".csv":
            _write_csv(p, ROWS, with_header=with_header)
        else:
            _write_csv_gz(p, ROWS, with_header=with_header)
        result = list(iter_inventory_csv(str(p)))
        assert result == ROWS

    def test_csv_empty_lines_ignored(self, tmp_path):
        p = tmp_path / "inv.csv"
        p.write_text("bucket,key\n\nmy-bucket,a.stac.json\n\n")
        result = list(iter_inventory_csv(str(p)))
        assert result == [("my-bucket", "a.stac.json")]

    def test_csv_quoted_values(self, tmp_path):
        p = tmp_path / "inv.csv"
        p.write_text('"bucket","key"\n"my-bucket","prefix/a.stac.json"\n')
        result = list(iter_inventory_csv(str(p)))
        assert result == [("my-bucket", "prefix/a.stac.json")]

    def test_dispatch_csv(self, tmp_path):
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS)
        assert list(iter_inventory(str(p))) == ROWS

    def test_dispatch_csv_gz(self, tmp_path):
        p = tmp_path / "inv.csv.gz"
        _write_csv_gz(p, ROWS)
        assert list(iter_inventory(str(p))) == ROWS


# ---------------------------------------------------------------------------
# Parquet tests
# ---------------------------------------------------------------------------


class TestIterInventoryParquet:
    def test_parquet_basic(self, tmp_path):
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)
        result = list(iter_inventory_parquet(str(p)))
        assert result == ROWS

    def test_parquet_preserves_order(self, tmp_path):
        rows = [(f"bucket-{i}", f"key-{i}.stac.json") for i in range(200)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)
        result = list(iter_inventory_parquet(str(p)))
        assert result == rows

    def test_parquet_batch_boundary(self, tmp_path):
        """Rows spanning multiple batches must all be yielded correctly."""
        rows = [("b", f"k-{i}.stac.json") for i in range(250)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)
        # batch_size=100 forces 3 batches for 250 rows
        result = list(iter_inventory_parquet(str(p), batch_size=100))
        assert result == rows

    def test_parquet_extra_columns_ignored(self, tmp_path):
        """Parquet files with extra AWS inventory columns must still work."""
        table = pa.table(
            {
                "bucket": pa.array(["b"], type=pa.string()),
                "key": pa.array(["k.stac.json"], type=pa.string()),
                "size": pa.array([1234], type=pa.int64()),
                "storage_class": pa.array(["STANDARD"], type=pa.string()),
            }
        )
        p = tmp_path / "inv.parquet"
        pq.write_table(table, str(p))
        result = list(iter_inventory_parquet(str(p)))
        assert result == [("b", "k.stac.json")]

    def test_dispatch_parquet(self, tmp_path):
        """iter_inventory dispatches .parquet to the Parquet reader."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)
        assert list(iter_inventory(str(p))) == ROWS

    def test_parquet_empty_file(self, tmp_path):
        """An empty Parquet inventory yields nothing."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, [])
        assert list(iter_inventory_parquet(str(p))) == []


# ---------------------------------------------------------------------------
# S3 CSV path tests (mocked obstore download)
# ---------------------------------------------------------------------------


def _make_csv_bytes(rows, with_header=True) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    if with_header:
        writer.writerow(["bucket", "key"])
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _make_csv_gz_bytes(rows, with_header=True) -> bytes:
    return gzip.compress(_make_csv_bytes(rows, with_header=with_header))


class TestS3CsvPath:
    """iter_inventory_csv with s3:// paths — _fetch_inventory_bytes mocked."""

    @pytest.mark.parametrize(
        ("suffix", "with_header"),
        [
            (".csv", True),
            (".csv", False),
            (".csv.gz", True),
            (".csv.gz", False),
        ],
    )
    def test_s3_csv_variants(self, suffix, with_header):
        raw = _make_csv_bytes(ROWS, with_header=with_header)
        if suffix == ".csv.gz":
            raw = gzip.compress(raw)
        fake_path = f"s3://fake-bucket/inventory{suffix}"
        with patch(
            "earthcatalog.inventory._fetch_inventory_bytes",
            return_value=raw,
        ):
            assert list(iter_inventory_csv(fake_path)) == ROWS

    def test_s3_plain_csv_no_str_copy(self):
        """Verify that the S3 plain-CSV path does NOT call bytes.decode() —
        i.e. it uses TextIOWrapper, not StringIO(raw.decode())."""
        raw = _make_csv_bytes(ROWS, with_header=True)

        # Wrap the bytes object so we can detect .decode() calls
        class _Spy(bytes):
            def __new__(cls, data):
                obj = super().__new__(cls, data)
                obj.decode_called = False
                return obj

            def decode(self, *args, **kwargs):  # type: ignore[override]
                self.decode_called = True
                return super().decode(*args, **kwargs)

        spy = _Spy(raw)
        fake_path = "s3://fake-bucket/inventory.csv"
        with patch(
            "earthcatalog.inventory._fetch_inventory_bytes",
            return_value=spy,
        ):
            list(iter_inventory_csv(fake_path))
        assert not spy.decode_called, (
            "iter_inventory_csv called bytes.decode() — should use TextIOWrapper(BytesIO) instead"
        )


# ---------------------------------------------------------------------------
# since filter — Parquet
# ---------------------------------------------------------------------------


class TestSinceFilterParquet:
    """iter_inventory_parquet with a since= cutoff date."""

    def test_filters_old_rows(self, tmp_path):
        """Rows with last_modified_date < since must be excluded."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_rows_at_exact_cutoff(self, tmp_path):
        """A row whose last_modified_date == since (to the second) must be kept."""
        exact = "2026-04-01T00:00:00.000Z"
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS[:1], lm_dates=[exact])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_keeps_all_when_no_lm_column(self, tmp_path):
        """Graceful degradation: no last_modified_date column → all rows kept."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)  # no lm_dates
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == ROWS

    def test_none_since_returns_all(self, tmp_path):
        """since=None must return all rows regardless of last_modified_date."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _OLD, _OLD])
        result = list(iter_inventory_parquet(str(p), since=None))
        assert result == ROWS

    def test_null_lm_passes_through(self, tmp_path):
        """Rows with a NULL last_modified_date must be passed through."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS[:1], lm_dates=[None])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_dispatch_parquet_with_since(self, tmp_path):
        """iter_inventory propagates since= to the Parquet reader."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(iter_inventory(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]


# ---------------------------------------------------------------------------
# since filter — CSV
# ---------------------------------------------------------------------------


class TestSinceFilterCsv:
    """iter_inventory_csv with a since= cutoff date."""

    def test_filters_old_rows(self, tmp_path):
        """Rows with last_modified_date < since must be excluded."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(iter_inventory_csv(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_all_when_no_lm_column_in_header(self, tmp_path):
        """Graceful degradation: header without last_modified_date → all rows."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=True)  # only bucket, key header
        result = list(iter_inventory_csv(str(p), since=_SINCE))
        assert result == ROWS

    def test_keeps_all_when_no_header(self, tmp_path):
        """Graceful degradation: no header at all → all rows passed through."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=False)
        result = list(iter_inventory_csv(str(p), since=_SINCE))
        assert result == ROWS

    def test_none_since_returns_all(self, tmp_path):
        """since=None must return all rows regardless of last_modified_date."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _OLD, _OLD])
        result = list(iter_inventory_csv(str(p), since=None))
        assert result == ROWS

    def test_dispatch_csv_with_since(self, tmp_path):
        """iter_inventory propagates since= to the CSV reader."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(iter_inventory(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]


# ---------------------------------------------------------------------------
# _coerce_last_modified — datetime vs string
# ---------------------------------------------------------------------------


class TestCoerceLastModified:
    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (None, None),
            ("2026-04-20T12:00:00.000Z", datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)),
            (datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC), None),
            (datetime(2026, 4, 20, 12, 0, 0), None),
            ("not-a-date", None),
        ],
    )
    def test_coerce_last_modified(self, input_val, expected):
        result = _coerce_last_modified(input_val)
        if expected is None and not isinstance(input_val, str):
            assert result == input_val or result.tzinfo == UTC
        elif expected is None:
            assert result is None
        else:
            assert result == expected


# ---------------------------------------------------------------------------
# since filter — Parquet with TIMESTAMP column (datetime objects)
# ---------------------------------------------------------------------------


def _write_parquet_timestamp(path, rows, lm_datetimes=None):
    """Write a Parquet file with last_modified_date as TIMESTAMP(us, UTC)."""
    data = {
        "bucket": pa.array([r[0] for r in rows], type=pa.string()),
        "key": pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_datetimes is not None:
        data["last_modified_date"] = pa.array(
            lm_datetimes,
            type=pa.timestamp("ms", tz="UTC"),
        )
    table = pa.table(data)
    pq.write_table(table, str(path))


class TestSinceFilterParquetTimestamp:
    """iter_inventory_parquet with a TIMESTAMP last_modified_date column."""

    def test_filters_old_rows_timestamp(self, tmp_path):
        """TIMESTAMP rows < since are excluded."""
        p = tmp_path / "inv.parquet"
        old_dt = datetime(2026, 1, 1, tzinfo=UTC)
        new_dt = datetime(2026, 4, 20, tzinfo=UTC)
        _write_parquet_timestamp(p, ROWS, [old_dt, new_dt, new_dt])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_rows_at_exact_cutoff_timestamp(self, tmp_path):
        """TIMESTAMP row exactly at since boundary is kept."""
        p = tmp_path / "inv.parquet"
        _write_parquet_timestamp(p, ROWS[:1], [_SINCE])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_null_lm_timestamp_passes_through(self, tmp_path):
        """NULL TIMESTAMP rows are passed through."""
        p = tmp_path / "inv.parquet"
        _write_parquet_timestamp(p, ROWS[:1], [None])
        result = list(iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]


# ---------------------------------------------------------------------------
# iter_inventory_manifest — mocked obstore
# ---------------------------------------------------------------------------


def _make_manifest_json(dest_bucket: str, data_keys: list[str]) -> bytes:
    """Build a minimal manifest.json bytes blob."""
    manifest = {
        "sourceBucket": "its-live-data",
        "destinationBucket": f"arn:aws:s3:::{dest_bucket}",
        "fileFormat": "Parquet",
        "files": [{"key": k, "MD5checksum": "abc123"} for k in data_keys],
    }
    return json.dumps(manifest).encode()


def _make_parquet_bytes(rows, lm_dates=None) -> bytes:
    """Write rows to an in-memory Parquet buffer and return bytes."""
    data = {
        "bucket": pa.array([r[0] for r in rows], type=pa.string()),
        "key": pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_dates is not None:
        data["last_modified_date"] = pa.array(lm_dates, type=pa.string())
    buf = io.BytesIO()
    pq.write_table(pa.table(data), buf)
    return buf.getvalue()


class TestIterInventoryManifest:
    """iter_inventory_manifest with mocked obstore and _get_authenticated_store."""

    def _run(self, manifest_bytes, data_files_bytes, since=None):
        """
        Patch obstore.get to return manifest_bytes for the manifest key, and
        data_files_bytes[i] for each data key.  _get_authenticated_store is
        patched to return a sentinel string (store is never used directly).
        """

        manifest_bucket = "fake-log-bucket"
        manifest_key = "inventory/manifest.json"
        data_keys = [f"inventory/data/part_{i}.parquet" for i in range(len(data_files_bytes))]
        manifest_blob = _make_manifest_json(manifest_bucket, data_keys)

        def fake_get(store, key):
            class _FakeResult:
                def bytes(self_):
                    if key == manifest_key:
                        return manifest_blob
                    # data file — look up by key suffix
                    idx = int(key.split("part_")[1].split(".")[0])
                    return data_files_bytes[idx]

            return _FakeResult()

        with (
            patch("earthcatalog.inventory.obstore.get", side_effect=fake_get),
            patch(
                "earthcatalog.inventory.get_authenticated_store",
                return_value="fake-store",
            ),
        ):
            return list(
                iter_inventory_manifest(
                    f"s3://{manifest_bucket}/{manifest_key}",
                    since=since,
                )
            )

    def test_single_data_file(self):
        """Single Parquet data file: all rows yielded."""
        data = _make_parquet_bytes(ROWS)
        result = self._run(b"", [data])
        assert result == ROWS

    def test_multiple_data_files(self):
        """Two data files: all rows from both files are yielded."""
        data1 = _make_parquet_bytes(ROWS[:2])
        data2 = _make_parquet_bytes(ROWS[2:])
        result = self._run(b"", [data1, data2])
        assert result == ROWS

    def test_since_filter_propagated(self):
        """since= is forwarded to iter_inventory_parquet for each data file."""
        data = _make_parquet_bytes(ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = self._run(b"", [data], since=_SINCE)
        assert result == [ROWS[1], ROWS[2]]

    def test_dispatch_manifest_json(self, tmp_path):
        """iter_inventory dispatches manifest.json suffix to manifest reader."""
        data = _make_parquet_bytes(ROWS)

        def fake_get(store, key):
            class _R:
                def bytes(self_):
                    if key.endswith("manifest.json"):
                        return _make_manifest_json(
                            "fake-log-bucket", ["inventory/data/part_0.parquet"]
                        )
                    return data

            return _R()

        with (
            patch("earthcatalog.inventory.obstore.get", side_effect=fake_get),
            patch(
                "earthcatalog.inventory.get_authenticated_store",
                return_value="fake-store",
            ),
        ):
            result = list(iter_inventory("s3://fake-log-bucket/inventory/manifest.json"))
        assert result == ROWS


# ---------------------------------------------------------------------------
# write_inventory_shards — distributed scatter
# ---------------------------------------------------------------------------


class TestWriteInventoryShards:
    """Scatter: head streams the inventory and writes fixed-row shard
    parquets; workers read them back via InventoryShard.iter_pairs()."""

    def test_writes_exact_fixed_row_chunks(self, tmp_path):
        """5 matching rows, chunk_size=2 → shards of 2, 2, 1 rows."""
        rows = [("b", f"k{i}.stac.json") for i in range(5)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)

        store = MemoryStore()
        shards = write_inventory_shards(
            str(p), store, staging_prefix="staging/shards/run1", chunk_size=2
        )

        assert len(shards) == 3
        sizes = [len(list(s.iter_pairs())) for s in shards]
        assert sizes == [2, 2, 1], sizes
        # Sequential shard names under the run-scoped prefix.
        assert [s.files[0] for s in shards] == [
            "staging/shards/run1/shard_00000.parquet",
            "staging/shards/run1/shard_00001.parquet",
            "staging/shards/run1/shard_00002.parquet",
        ]
        assert [pair for s in shards for pair in s.iter_pairs()] == rows

    def test_suffix_filter_applied_before_chunking(self, tmp_path):
        """Non-matching keys are dropped before rows are counted into
        chunks, so every shard is exactly chunk_size matching pairs."""
        rows = [("b", "a.stac.json"), ("b", "notes.txt"), ("b", "c.stac.json")]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)

        store = MemoryStore()
        shards = write_inventory_shards(
            str(p), store, staging_prefix="staging/s", chunk_size=2, suffix=".stac.json"
        )

        assert len(shards) == 1
        assert list(shards[0].iter_pairs()) == [("b", "a.stac.json"), ("b", "c.stac.json")]

    def test_since_and_limit_applied(self, tmp_path):
        """since drops old rows; limit truncates the matching stream."""
        rows = [("b", "a.stac.json"), ("b", "b.stac.json"), ("b", "c.stac.json")]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows, lm_dates=[_OLD, _NEW, _NEW])

        store = MemoryStore()
        shards = write_inventory_shards(
            str(p), store, staging_prefix="staging/s", chunk_size=10, since=_SINCE, limit=1
        )

        assert [pair for s in shards for pair in s.iter_pairs()] == [("b", "b.stac.json")]

    def test_manifest_inventory_scattered_regardless_of_part_file_skew(self):
        """Part files of 2 and 1 rows with chunk_size=2 → uniform 2-row
        shards, rebalancing whatever skew the source files had."""
        data1 = _make_parquet_bytes(ROWS[:2])
        data2 = _make_parquet_bytes(ROWS[2:])
        manifest_key = "inventory/manifest.json"
        data_keys = ["inventory/data/part_0.parquet", "inventory/data/part_1.parquet"]
        manifest = _make_manifest_json("fake-log-bucket", data_keys)

        def fake_get(store, key):
            class _R:
                def bytes(self_):
                    if key == manifest_key:
                        return manifest
                    idx = int(key.split("part_")[1].split(".")[0])
                    return [data1, data2][idx]

            return _R()

        store = MemoryStore()
        with (
            patch("earthcatalog.inventory.obstore.get", side_effect=fake_get),
            patch(
                "earthcatalog.inventory.get_authenticated_store",
                return_value="fake-store",
            ),
        ):
            shards = write_inventory_shards(
                "s3://fake-log-bucket/inventory/manifest.json",
                store,
                staging_prefix="staging/s",
                chunk_size=2,
            )

        sizes = [len(list(s.iter_pairs())) for s in shards]
        assert sizes == [2, 1], sizes

    def test_delete_shard_files_removes_written_shards(self, tmp_path):
        """Cleanup deletes exactly the shard files the scatter wrote."""
        rows = [("b", f"k{i}.stac.json") for i in range(3)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)

        store = MemoryStore()
        shards = write_inventory_shards(str(p), store, staging_prefix="staging/s", chunk_size=2)
        assert len(shards) == 2

        deleted = delete_shard_files(store, shards)
        assert deleted == 2

        # delete_shard_files leaves the manifest; delete_scatter (tested
        # separately) removes both.
        remaining = [obj["path"] for batch in store.list(prefix="staging/") for obj in batch]
        assert remaining == ["staging/s/scatter.json"]

    def test_shard_limit_caps_pairs_at_iteration_time(self):
        """A per-shard limit is applied by iter_pairs(), without needing the
        head to materialise anything."""
        shard = InventoryShard(
            pairs=(("b", "a.stac.json"), ("b", "b.stac.json"), ("b", "c.stac.json")),
            limit=2,
        )
        assert list(shard.iter_pairs()) == [("b", "a.stac.json"), ("b", "b.stac.json")]

    def test_scatter_manifest_round_trip(self, tmp_path):
        """write_inventory_shards writes a scatter.json that load_inventory_shards
        reads back; the two-step workflow is a decoupled scatter then map/reduce."""
        rows = [("b", f"k{i}.stac.json") for i in range(5)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)

        store = MemoryStore()
        shards = write_inventory_shards(
            str(p), store, staging_prefix="staging/shards/run1", chunk_size=2
        )
        manifest_key = scatter_manifest_path("staging/shards/run1")
        assert is_scatter_manifest(manifest_key)

        # Head is done — scatter.json is the only thing step 2 needs.
        reloaded = load_inventory_shards(manifest_key, store)
        assert [s.files for s in reloaded] == [s.files for s in shards]
        assert [pair for s in reloaded for pair in s.iter_pairs()] == rows

        # Prefix also accepted (load appends scatter.json).
        assert load_inventory_shards("staging/shards/run1", store)[0].files == shards[0].files

    def test_delete_scatter_removes_shards_and_manifest(self, tmp_path):
        rows = [("b", f"k{i}.stac.json") for i in range(3)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)

        store = MemoryStore()
        shards = write_inventory_shards(str(p), store, staging_prefix="staging/s", chunk_size=2)
        assert len(shards) == 2

        deleted = delete_scatter(store, "staging/s", shards)
        # 2 shard files + 1 manifest
        assert deleted == 3
        remaining = [obj["path"] for batch in store.list(prefix="staging/") for obj in batch]
        assert remaining == []


# ---------------------------------------------------------------------------
# fetch_items_async — concurrent STAC fetch (obstore.get_async)
# ---------------------------------------------------------------------------


class TestFetchItemsAsync:
    def test_fetches_concurrently_and_tags_source(self):
        from earthcatalog.inventory import fetch_items_async

        store = MemoryStore()
        pairs = [("b", f"dir/item-{i}.stac.json") for i in range(5)]
        for _, key in pairs:
            store.put(key, json.dumps({"id": key, "type": "Feature"}).encode())

        # Patch get_store so the async path reads from MemoryStore.
        import earthcatalog.inventory as _invmod

        original = _invmod.get_store
        _invmod.get_store = lambda bucket: store
        try:
            items = fetch_items_async(pairs, concurrency=8)
        finally:
            _invmod.get_store = original

        assert len(items) == 5
        by_id = {it["id"]: it for it in items}
        assert by_id["dir/item-3.stac.json"]["_source_bucket"] == "b"
        assert by_id["dir/item-3.stac.json"]["_source_key"] == "dir/item-3.stac.json"

    def test_skips_missing_keys(self):
        from earthcatalog.inventory import fetch_items_async

        store = MemoryStore()
        pairs = [("b", "present.stac.json"), ("b", "missing.stac.json")]
        store.put("present.stac.json", b'{"id": "present", "type": "Feature"}')

        import earthcatalog.inventory as _invmod

        original = _invmod.get_store
        _invmod.get_store = lambda bucket: store
        try:
            items = fetch_items_async(pairs, concurrency=4)
        finally:
            _invmod.get_store = original

        assert [it["id"] for it in items] == ["present"]
