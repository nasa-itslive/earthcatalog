"""Rebuild-the-table-from-warehouse helpers."""

from __future__ import annotations

import earthcatalog.rebuild as rebuild_mod
from earthcatalog.rebuild import _list_warehouse_keys


class _FakeS3Store:
    """Stands in for a bucket-level S3Store: keys are bucket-relative."""


class _FakeLocalStore:
    """Stands in for a LocalStore scoped to the warehouse root."""


def test_s3_warehouse_keys_are_bucket_relative(monkeypatch):
    """obstore S3 keys already include the warehouse path — the URI must be
    s3://bucket/key, never the warehouse root joined again (the doubled
    prefix 404s every add_files footer read)."""

    def fake_list(store, prefix):
        assert prefix == "test-space/stac/catalog/warehouse/"
        yield [
            {
                "path": "test-space/stac/catalog/warehouse/"
                "grid=h3/level=1/tile=abc/year=2020/part_000000.parquet"
            },
            {"path": "test-space/stac/catalog/warehouse/earthcatalog.db"},  # not hive → skipped
        ]

    monkeypatch.setattr(rebuild_mod.obstore, "list", fake_list)
    paths = _list_warehouse_keys(
        _FakeS3Store(), "s3://its-live-data/test-space/stac/catalog/warehouse"
    )
    assert paths == [
        "s3://its-live-data/test-space/stac/catalog/warehouse/"
        "grid=h3/level=1/tile=abc/year=2020/part_000000.parquet"
    ]


def test_local_warehouse_keys_join_the_root(monkeypatch):
    def fake_list(store, prefix):
        assert prefix == ""
        yield [{"path": "grid_partition=abc/year=2020/part_000000.parquet"}]

    monkeypatch.setattr(rebuild_mod.obstore, "list", fake_list)
    paths = _list_warehouse_keys(_FakeLocalStore(), "/tmp/warehouse")
    assert paths == ["/tmp/warehouse/grid_partition=abc/year=2020/part_000000.parquet"]


def test_legacy_and_v2_layouts_both_registered(monkeypatch):
    def fake_list(store, prefix):
        yield [
            {"path": "grid=h3/level=1/tile=abc/year=2020/part_000000.parquet"},
            {"path": "grid_partition=abc/year=2020/part_000000.parquet"},
            {"path": "grid=h3/level=2/tile=abc/month=2020-05/part_000000.parquet"},
            {"path": "other/part_000000.parquet"},  # not hive → skipped
        ]

    monkeypatch.setattr(rebuild_mod.obstore, "list", fake_list)
    paths = _list_warehouse_keys(_FakeLocalStore(), "/tmp/warehouse")
    assert len(paths) == 3
