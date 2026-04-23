"""
Tests for inventory reading: CSV, CSV.gz, and Parquet formats.

All tests use local temp files — no S3 access required.
The S3 path of _iter_inventory_csv is exercised by patching
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

from earthcatalog.pipelines.incremental import (
    _coerce_last_modified,
    _iter_inventory,
    _iter_inventory_csv,
    _iter_inventory_manifest,
    _iter_inventory_parquet,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

ROWS = [
    ("my-bucket", "prefix/a.stac.json"),
    ("my-bucket", "prefix/b.stac.json"),
    ("other-bucket", "path/c.stac.json"),
]

_OLD  = "2026-01-01T00:00:00.000Z"
_NEW  = "2026-04-20T00:00:00.000Z"
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
        "key":    pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_dates is not None:
        data["last_modified_date"] = pa.array(lm_dates, type=pa.string())
    table = pa.table(data)
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# CSV tests
# ---------------------------------------------------------------------------

class TestIterInventoryCsv:
    def test_csv_with_header(self, tmp_path):
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=True)
        result = list(_iter_inventory_csv(str(p)))
        assert result == ROWS

    def test_csv_without_header(self, tmp_path):
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=False)
        result = list(_iter_inventory_csv(str(p)))
        assert result == ROWS

    def test_csv_gz(self, tmp_path):
        p = tmp_path / "inv.csv.gz"
        _write_csv_gz(p, ROWS, with_header=True)
        result = list(_iter_inventory_csv(str(p)))
        assert result == ROWS

    def test_csv_gz_without_header(self, tmp_path):
        p = tmp_path / "inv.csv.gz"
        _write_csv_gz(p, ROWS, with_header=False)
        result = list(_iter_inventory_csv(str(p)))
        assert result == ROWS

    def test_csv_empty_lines_ignored(self, tmp_path):
        p = tmp_path / "inv.csv"
        p.write_text("bucket,key\n\nmy-bucket,a.stac.json\n\n")
        result = list(_iter_inventory_csv(str(p)))
        assert result == [("my-bucket", "a.stac.json")]

    def test_csv_quoted_values(self, tmp_path):
        """AWS S3 Inventory CSVs sometimes double-quote every field."""
        p = tmp_path / "inv.csv"
        p.write_text('"bucket","key"\n"my-bucket","prefix/a.stac.json"\n')
        result = list(_iter_inventory_csv(str(p)))
        assert result == [("my-bucket", "prefix/a.stac.json")]

    def test_dispatch_csv(self, tmp_path):
        """_iter_inventory dispatches .csv to the CSV reader."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS)
        assert list(_iter_inventory(str(p))) == ROWS

    def test_dispatch_csv_gz(self, tmp_path):
        """_iter_inventory dispatches .csv.gz to the CSV reader."""
        p = tmp_path / "inv.csv.gz"
        _write_csv_gz(p, ROWS)
        assert list(_iter_inventory(str(p))) == ROWS


# ---------------------------------------------------------------------------
# Parquet tests
# ---------------------------------------------------------------------------

class TestIterInventoryParquet:
    def test_parquet_basic(self, tmp_path):
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)
        result = list(_iter_inventory_parquet(str(p)))
        assert result == ROWS

    def test_parquet_preserves_order(self, tmp_path):
        rows = [(f"bucket-{i}", f"key-{i}.stac.json") for i in range(200)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)
        result = list(_iter_inventory_parquet(str(p)))
        assert result == rows

    def test_parquet_batch_boundary(self, tmp_path):
        """Rows spanning multiple batches must all be yielded correctly."""
        rows = [("b", f"k-{i}.stac.json") for i in range(250)]
        p = tmp_path / "inv.parquet"
        _write_parquet(p, rows)
        # batch_size=100 forces 3 batches for 250 rows
        result = list(_iter_inventory_parquet(str(p), batch_size=100))
        assert result == rows

    def test_parquet_extra_columns_ignored(self, tmp_path):
        """Parquet files with extra AWS inventory columns must still work."""
        table = pa.table({
            "bucket":        pa.array(["b"], type=pa.string()),
            "key":           pa.array(["k.stac.json"], type=pa.string()),
            "size":          pa.array([1234], type=pa.int64()),
            "storage_class": pa.array(["STANDARD"], type=pa.string()),
        })
        p = tmp_path / "inv.parquet"
        pq.write_table(table, str(p))
        result = list(_iter_inventory_parquet(str(p)))
        assert result == [("b", "k.stac.json")]

    def test_dispatch_parquet(self, tmp_path):
        """_iter_inventory dispatches .parquet to the Parquet reader."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)
        assert list(_iter_inventory(str(p))) == ROWS

    def test_parquet_empty_file(self, tmp_path):
        """An empty Parquet inventory yields nothing."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, [])
        assert list(_iter_inventory_parquet(str(p))) == []


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
    """_iter_inventory_csv with s3:// paths — _fetch_inventory_bytes mocked."""

    def _run(self, raw_bytes: bytes, suffix: str) -> list[tuple[str, str]]:
        fake_path = f"s3://fake-bucket/inventory{suffix}"
        with patch(
            "earthcatalog.pipelines.incremental._fetch_inventory_bytes",
            return_value=raw_bytes,
        ):
            return list(_iter_inventory_csv(fake_path))

    def test_s3_plain_csv_with_header(self):
        """Plain S3 CSV: TextIOWrapper(BytesIO) path — correct rows returned."""
        raw = _make_csv_bytes(ROWS, with_header=True)
        assert self._run(raw, ".csv") == ROWS

    def test_s3_plain_csv_without_header(self):
        """Plain S3 CSV with no header row: first row treated as data."""
        raw = _make_csv_bytes(ROWS, with_header=False)
        assert self._run(raw, ".csv") == ROWS

    def test_s3_csv_gz_with_header(self):
        """Compressed S3 CSV.gz: gzip.open(BytesIO) path — correct rows returned."""
        raw = _make_csv_gz_bytes(ROWS, with_header=True)
        assert self._run(raw, ".csv.gz") == ROWS

    def test_s3_csv_gz_without_header(self):
        """Compressed S3 CSV.gz without header: first row treated as data."""
        raw = _make_csv_gz_bytes(ROWS, with_header=False)
        assert self._run(raw, ".csv.gz") == ROWS

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
            "earthcatalog.pipelines.incremental._fetch_inventory_bytes",
            return_value=spy,
        ):
            list(_iter_inventory_csv(fake_path))
        assert not spy.decode_called, (
            "_iter_inventory_csv called bytes.decode() — "
            "should use TextIOWrapper(BytesIO) instead"
        )


# ---------------------------------------------------------------------------
# since filter — Parquet
# ---------------------------------------------------------------------------

class TestSinceFilterParquet:
    """_iter_inventory_parquet with a since= cutoff date."""

    def test_filters_old_rows(self, tmp_path):
        """Rows with last_modified_date < since must be excluded."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_rows_at_exact_cutoff(self, tmp_path):
        """A row whose last_modified_date == since (to the second) must be kept."""
        exact = "2026-04-01T00:00:00.000Z"
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS[:1], lm_dates=[exact])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_keeps_all_when_no_lm_column(self, tmp_path):
        """Graceful degradation: no last_modified_date column → all rows kept."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS)   # no lm_dates
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == ROWS

    def test_none_since_returns_all(self, tmp_path):
        """since=None must return all rows regardless of last_modified_date."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _OLD, _OLD])
        result = list(_iter_inventory_parquet(str(p), since=None))
        assert result == ROWS

    def test_null_lm_passes_through(self, tmp_path):
        """Rows with a NULL last_modified_date must be passed through."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS[:1], lm_dates=[None])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_dispatch_parquet_with_since(self, tmp_path):
        """_iter_inventory propagates since= to the Parquet reader."""
        p = tmp_path / "inv.parquet"
        _write_parquet(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(_iter_inventory(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]


# ---------------------------------------------------------------------------
# since filter — CSV
# ---------------------------------------------------------------------------

class TestSinceFilterCsv:
    """_iter_inventory_csv with a since= cutoff date."""

    def test_filters_old_rows(self, tmp_path):
        """Rows with last_modified_date < since must be excluded."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(_iter_inventory_csv(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_all_when_no_lm_column_in_header(self, tmp_path):
        """Graceful degradation: header without last_modified_date → all rows."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=True)   # only bucket, key header
        result = list(_iter_inventory_csv(str(p), since=_SINCE))
        assert result == ROWS

    def test_keeps_all_when_no_header(self, tmp_path):
        """Graceful degradation: no header at all → all rows passed through."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, with_header=False)
        result = list(_iter_inventory_csv(str(p), since=_SINCE))
        assert result == ROWS

    def test_none_since_returns_all(self, tmp_path):
        """since=None must return all rows regardless of last_modified_date."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _OLD, _OLD])
        result = list(_iter_inventory_csv(str(p), since=None))
        assert result == ROWS

    def test_dispatch_csv_with_since(self, tmp_path):
        """_iter_inventory propagates since= to the CSV reader."""
        p = tmp_path / "inv.csv"
        _write_csv(p, ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = list(_iter_inventory(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]


# ---------------------------------------------------------------------------
# _coerce_last_modified — datetime vs string
# ---------------------------------------------------------------------------

class TestCoerceLastModified:
    """Unit tests for _coerce_last_modified."""

    def test_none_returns_none(self):
        assert _coerce_last_modified(None) is None

    def test_string_iso_z(self):
        """ISO-8601 string with trailing Z parses correctly."""
        result = _coerce_last_modified("2026-04-20T12:00:00.000Z")
        assert result == datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)

    def test_datetime_with_tz(self):
        """A timezone-aware datetime is returned as-is."""
        dt = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        assert _coerce_last_modified(dt) is dt

    def test_datetime_naive_gets_utc(self):
        """A naive datetime gets UTC attached."""
        dt = datetime(2026, 4, 20, 12, 0, 0)
        result = _coerce_last_modified(dt)
        assert result.tzinfo == UTC
        assert result.replace(tzinfo=None) == dt

    def test_bad_string_returns_none(self):
        assert _coerce_last_modified("not-a-date") is None


# ---------------------------------------------------------------------------
# since filter — Parquet with TIMESTAMP column (datetime objects)
# ---------------------------------------------------------------------------

def _write_parquet_timestamp(path, rows, lm_datetimes=None):
    """Write a Parquet file with last_modified_date as TIMESTAMP(us, UTC)."""
    data = {
        "bucket": pa.array([r[0] for r in rows], type=pa.string()),
        "key":    pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_datetimes is not None:
        data["last_modified_date"] = pa.array(
            lm_datetimes,
            type=pa.timestamp("ms", tz="UTC"),
        )
    table = pa.table(data)
    pq.write_table(table, str(path))


class TestSinceFilterParquetTimestamp:
    """_iter_inventory_parquet with a TIMESTAMP last_modified_date column."""

    def test_filters_old_rows_timestamp(self, tmp_path):
        """TIMESTAMP rows < since are excluded."""
        p = tmp_path / "inv.parquet"
        old_dt = datetime(2026, 1, 1, tzinfo=UTC)
        new_dt = datetime(2026, 4, 20, tzinfo=UTC)
        _write_parquet_timestamp(p, ROWS, [old_dt, new_dt, new_dt])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[1], ROWS[2]]

    def test_keeps_rows_at_exact_cutoff_timestamp(self, tmp_path):
        """TIMESTAMP row exactly at since boundary is kept."""
        p = tmp_path / "inv.parquet"
        _write_parquet_timestamp(p, ROWS[:1], [_SINCE])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]

    def test_null_lm_timestamp_passes_through(self, tmp_path):
        """NULL TIMESTAMP rows are passed through."""
        p = tmp_path / "inv.parquet"
        _write_parquet_timestamp(p, ROWS[:1], [None])
        result = list(_iter_inventory_parquet(str(p), since=_SINCE))
        assert result == [ROWS[0]]


# ---------------------------------------------------------------------------
# _iter_inventory_manifest — mocked obstore
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
        "key":    pa.array([r[1] for r in rows], type=pa.string()),
    }
    if lm_dates is not None:
        data["last_modified_date"] = pa.array(lm_dates, type=pa.string())
    buf = io.BytesIO()
    pq.write_table(pa.table(data), buf)
    return buf.getvalue()


class TestIterInventoryManifest:
    """_iter_inventory_manifest with mocked obstore and _get_authenticated_store."""

    def _run(self, manifest_bytes, data_files_bytes, since=None):
        """
        Patch obstore.get to return manifest_bytes for the manifest key, and
        data_files_bytes[i] for each data key.  _get_authenticated_store is
        patched to return a sentinel string (store is never used directly).
        """

        manifest_bucket = "fake-log-bucket"
        manifest_key    = "inventory/manifest.json"
        data_keys       = [f"inventory/data/part_{i}.parquet"
                           for i in range(len(data_files_bytes))]
        manifest_blob   = _make_manifest_json(manifest_bucket, data_keys)

        def fake_get(store, key):
            class _FakeResult:
                def bytes(self_):
                    if key == manifest_key:
                        return manifest_blob
                    # data file — look up by key suffix
                    idx = int(key.split("part_")[1].split(".")[0])
                    return data_files_bytes[idx]
            return _FakeResult()

        with patch("earthcatalog.pipelines.incremental.obstore.get", side_effect=fake_get), \
             patch("earthcatalog.pipelines.incremental._get_authenticated_store",
                   return_value="fake-store"):
            return list(_iter_inventory_manifest(
                f"s3://{manifest_bucket}/{manifest_key}",
                since=since,
            ))

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
        """since= is forwarded to _iter_inventory_parquet for each data file."""
        data = _make_parquet_bytes(ROWS, lm_dates=[_OLD, _NEW, _NEW])
        result = self._run(b"", [data], since=_SINCE)
        assert result == [ROWS[1], ROWS[2]]

    def test_dispatch_manifest_json(self, tmp_path):
        """_iter_inventory dispatches manifest.json suffix to manifest reader."""
        data = _make_parquet_bytes(ROWS)

        def fake_get(store, key):
            class _R:
                def bytes(self_):
                    if key.endswith("manifest.json"):
                        return _make_manifest_json("fake-log-bucket",
                                                   ["inventory/data/part_0.parquet"])
                    return data
            return _R()

        with patch("earthcatalog.pipelines.incremental.obstore.get", side_effect=fake_get), \
             patch("earthcatalog.pipelines.incremental._get_authenticated_store",
                   return_value="fake-store"):
            result = list(_iter_inventory("s3://fake-log-bucket/inventory/manifest.json"))
        assert result == ROWS
