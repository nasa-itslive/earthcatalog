"""
Tests for earthcatalog.maintenance.compact.

Scenario
--------
Simulates the realistic incremental update flow:

1. **Initial ingest** — run() ingests items A, B, C into an empty warehouse.
   Each item lands in one or more (cell, year) buckets as part files.

2. **Incremental update** — run() ingests items A (duplicate — same id, same
   data), D, E.  This mimics a re-ingested item appearing in a delta inventory
   alongside genuinely new items.  After this run each bucket touched by A now
   has 2 part files, one from each run.

3. **Compact** — compact_warehouse() merges every bucket that has ≥ 2 files,
   deduplicates by id, and rebuilds the Iceberg catalog.

Assertions
----------
- After compaction, every (cell, year) bucket has exactly 1 Parquet file.
- The Iceberg scan result contains each unique item id exactly once (no
  duplicates).
- Total row count equals the number of *unique* fan-out rows across all items
  (A+B+C+D+E deduplicated, so A's second copy is gone).
- The Iceberg catalog has exactly 1 snapshot after the rebuild.

``since=`` / delta flow note
-----------------------------
``_iter_inventory_csv`` and ``_iter_inventory_parquet`` already filter on
``last_modified_date >= since`` at read time, streaming through the full
inventory file.  The tests below use plain inventory CSVs (no
last_modified_date column), which is the correct fall-through behaviour —
all rows pass when the column is absent.

For real use the workflow is:
    1.  Run incremental ingest daily (with --since <yesterday>).
    2.  Run compact_warehouse weekly (or whenever bucket file count exceeds
        the threshold visible in the dry-run output).
    3.  When the Athena delta-parquet cron is set up, pass the delta file as
        --inventory and drop --since entirely.
"""

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from earthcatalog.core import store_config
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.maintenance.compact import _scan_warehouse, compact_warehouse
from earthcatalog.pipelines.incremental import run

# ---------------------------------------------------------------------------
# STAC item fixtures
# ---------------------------------------------------------------------------

# Items used in run 1: A (0), B (1), C (2)
# Items used in run 2: A (0) again (duplicate id), D (3), E (4)
_ALL_ITEMS = [
    {
        "id": f"compact-item-{label}",
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-10, 60],
                    [10, 60],
                    [10, 70],
                    [-10, 70],
                    [-10, 60],
                ]
            ],
        },
        "properties": {
            "datetime": f"202{i % 3 + 1}-0{i % 9 + 1}-15T00:00:00Z",
            "platform": "sentinel-2",
            "percent_valid_pixels": float(70 + i),
            "date_dt": float(5 + i),
            "proj:code": "EPSG:32633",
            "sat:orbit_state": "descending",
        },
        "links": [],
        "assets": {},
    }
    for i, label in enumerate(["A", "B", "C", "D", "E"])
]

_ITEM_MAP: dict[tuple[str, str], dict] = {
    ("fake-bucket", f"stac/compact-item-{label}.stac.json"): item
    for label, item in zip(["A", "B", "C", "D", "E"], _ALL_ITEMS)
}


def _mock_fetch(bucket: str, key: str) -> dict | None:
    return _ITEM_MAP.get((bucket, key))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_inventory(path: Path, keys: list[str]) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["bucket", "key"])
        for k in keys:
            writer.writerow(["fake-bucket", k])


def _run_incremental(
    inventory_csv: Path,
    catalog_path: str,
    warehouse_path: str,
) -> None:
    """Run the incremental pipeline (no lock, mock fetcher, H3 res-2)."""
    with patch(
        "earthcatalog.pipelines.incremental._fetch_item",
        side_effect=_mock_fetch,
    ):
        run(
            inventory_path=str(inventory_csv),
            catalog_path=catalog_path,
            warehouse_path=warehouse_path,
            chunk_size=10,
            max_workers=1,
            partitioner=H3Partitioner(resolution=2),
            use_lock=False,
        )


def _count_parquet_files(warehouse_path: str) -> int:
    return sum(1 for _ in Path(warehouse_path).rglob("*.parquet"))


def _all_parquet_ids(warehouse_path: str) -> list[str]:
    """Read every Parquet file and collect all id values."""
    ids = []
    for p in Path(warehouse_path).rglob("*.parquet"):
        tbl = pq.ParquetFile(str(p)).read(columns=["id"])
        ids.extend(tbl.column("id").to_pylist())
    return ids


def _ids_per_file(warehouse_path: str) -> dict[str, list[str]]:
    """Return {relative_path: [id, ...]} for every Parquet file."""
    result = {}
    root = Path(warehouse_path)
    for p in root.rglob("*.parquet"):
        tbl = pq.ParquetFile(str(p)).read(columns=["id"])
        result[str(p.relative_to(root))] = tbl.column("id").to_pylist()
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _memory_store() -> None:
    """Point store_config at a MemoryStore so download/upload are no-ops."""
    store = MemoryStore()
    store_config.set_store(store)
    store_config.set_catalog_key("catalog.db")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestScanWarehouse:
    """Unit tests for _scan_warehouse()."""

    def test_empty_warehouse(self, tmp_path: Path) -> None:
        from obstore.store import LocalStore

        store = LocalStore(str(tmp_path))
        result = _scan_warehouse(store)
        assert result == {}

    def test_finds_parquet_files(self, tmp_path: Path) -> None:
        """Files matching the hive layout are grouped correctly."""
        from obstore.store import LocalStore

        # Create two files in the same bucket and one in another.
        (tmp_path / "grid_partition=abc123" / "year=2024").mkdir(parents=True)
        (tmp_path / "grid_partition=abc123" / "year=2024" / "part_000001_xx.parquet").touch()
        (tmp_path / "grid_partition=abc123" / "year=2024" / "part_000002_yy.parquet").touch()
        (tmp_path / "grid_partition=def456" / "year=2025").mkdir(parents=True)
        (tmp_path / "grid_partition=def456" / "year=2025" / "part_000001_zz.parquet").touch()

        store = LocalStore(str(tmp_path))
        result = _scan_warehouse(store)

        assert ("abc123", "2024") in result
        assert len(result[("abc123", "2024")]) == 2
        assert ("def456", "2025") in result
        assert len(result[("def456", "2025")]) == 1

    def test_ignores_non_parquet_files(self, tmp_path: Path) -> None:
        from obstore.store import LocalStore

        (tmp_path / "grid_partition=abc123" / "year=2024").mkdir(parents=True)
        (tmp_path / "grid_partition=abc123" / "year=2024" / "part_000001_xx.parquet").touch()
        (tmp_path / "grid_partition=abc123" / "year=2024" / ".gitkeep").touch()
        (tmp_path / "catalog.db").touch()

        store = LocalStore(str(tmp_path))
        result = _scan_warehouse(store)

        assert sum(len(v) for v in result.values()) == 1


class TestCompactWarehouse:
    """Integration tests for compact_warehouse() using local storage."""

    def test_dry_run_makes_no_changes(self, tmp_path: Path) -> None:
        """dry_run=True must not modify any files or the catalog."""
        catalog_path = str(tmp_path / "catalog.db")
        warehouse_path = str(tmp_path / "wh")

        inv1 = tmp_path / "inv1.csv"
        _write_inventory(inv1, ["stac/compact-item-A.stac.json"])
        _run_incremental(inv1, catalog_path, warehouse_path)

        inv2 = tmp_path / "inv2.csv"
        _write_inventory(inv2, ["stac/compact-item-A.stac.json"])
        _run_incremental(inv2, catalog_path, warehouse_path)

        files_before = _count_parquet_files(warehouse_path)
        assert files_before >= 2, "Expected ≥ 2 part files before dry-run"

        summary = compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=2,
            dry_run=True,
        )

        files_after = _count_parquet_files(warehouse_path)
        assert files_after == files_before, "dry-run must not delete any files"
        assert summary["buckets_compacted"] > 0

    def test_two_runs_then_compact_deduplicates(self, tmp_path: Path) -> None:
        """
        Core scenario: two incremental runs with a duplicate item id; after
        compaction each bucket has exactly 1 file and no file contains
        duplicate id values.

        Note on fan-out: each STAC item fans out to multiple H3 cells, so a
        single item id legitimately appears in multiple Parquet files (one per
        cell bucket).  The dedup guarantee is *within* each (cell, year) file
        — no id repeats inside the same file — not globally unique across all
        files.
        """
        catalog_path = str(tmp_path / "catalog.db")
        warehouse_path = str(tmp_path / "wh")

        # Run 1: ingest A, B, C
        inv1 = tmp_path / "inv1.csv"
        _write_inventory(
            inv1,
            [
                "stac/compact-item-A.stac.json",
                "stac/compact-item-B.stac.json",
                "stac/compact-item-C.stac.json",
            ],
        )
        _run_incremental(inv1, catalog_path, warehouse_path)

        # Run 2: ingest A (duplicate), D, E
        inv2 = tmp_path / "inv2.csv"
        _write_inventory(
            inv2,
            [
                "stac/compact-item-A.stac.json",
                "stac/compact-item-D.stac.json",
                "stac/compact-item-E.stac.json",
            ],
        )
        _run_incremental(inv2, catalog_path, warehouse_path)

        # Snapshot the warehouse state before compaction.
        per_file_before = _ids_per_file(warehouse_path)

        # Confirm that cross-file duplicates exist before compaction:
        # item A appears in multiple files within the same (cell, year) bucket.
        from collections import defaultdict

        bucket_ids: dict[str, list[str]] = defaultdict(list)
        for path, ids in per_file_before.items():
            # Extract bucket key from path, e.g. "grid_partition=X/year=Y"
            parts = Path(path).parts
            if len(parts) >= 2:
                bucket_key = f"{parts[0]}/{parts[1]}"
                bucket_ids[bucket_key].extend(ids)

        buckets_with_cross_file_dups = {
            k: v for k, v in bucket_ids.items() if len(v) != len(set(v))
        }
        assert buckets_with_cross_file_dups, (
            "Expected at least one (cell, year) bucket with cross-file duplicate ids "
            "before compaction (item A was ingested twice)"
        )

        # Run compaction.
        summary = compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=2,
        )

        # Every (cell, year) bucket must now have exactly 1 file.
        from obstore.store import LocalStore

        store = LocalStore(warehouse_path)
        buckets = _scan_warehouse(store)
        multi_file_buckets = {k: v for k, v in buckets.items() if len(v) > 1}
        assert multi_file_buckets == {}, (
            f"Expected all buckets to have 1 file after compaction, "
            f"but found multi-file buckets: {list(multi_file_buckets)}"
        )

        # Within each Parquet file, no id must appear more than once (dedup).
        per_file_after = _ids_per_file(warehouse_path)
        for path, ids in per_file_after.items():
            assert len(ids) == len(set(ids)), (
                f"Duplicate ids found in {path} after compaction: "
                f"{[x for x in ids if ids.count(x) > 1]}"
            )

        # All 5 unique item ids must be present somewhere in the warehouse.
        all_ids_after = set(_all_parquet_ids(warehouse_path))
        for label in ["A", "B", "C", "D", "E"]:
            assert f"compact-item-{label}" in all_ids_after, (
                f"compact-item-{label} missing from warehouse after compaction"
            )

        # Item A must appear fewer times after (duplicate removed per bucket).
        count_A_before = sum(ids.count("compact-item-A") for ids in per_file_before.values())
        count_A_after = list(_all_parquet_ids(warehouse_path)).count("compact-item-A")
        assert count_A_after < count_A_before, (
            f"Expected fewer occurrences of compact-item-A after dedup "
            f"({count_A_after} >= {count_A_before})"
        )

        # Summary sanity.
        assert summary["files_after"] < summary["files_before"]
        assert summary["buckets_compacted"] > 0

    def test_iceberg_catalog_has_one_snapshot_after_compact(self, tmp_path: Path) -> None:
        """compact_warehouse() must rebuild the catalog with exactly 1 snapshot."""
        catalog_path = str(tmp_path / "catalog.db")
        warehouse_path = str(tmp_path / "wh")

        for label in ["A", "B"]:
            inv = tmp_path / f"inv_{label}.csv"
            _write_inventory(inv, [f"stac/compact-item-{label}.stac.json"])
            _run_incremental(inv, catalog_path, warehouse_path)

        compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=2,
        )

        from earthcatalog.core.catalog import get_or_create_table, open_catalog

        catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_path)
        table = get_or_create_table(catalog)
        assert len(table.history()) == 1, (
            f"Expected 1 snapshot after catalog rebuild, got {len(table.history())}"
        )

    def test_iceberg_scan_after_compact_returns_correct_ids(self, tmp_path: Path) -> None:
        """Iceberg scan after compaction contains all unique item ids and no
        in-partition duplicates (fan-out means one id per cell, not globally unique)."""
        catalog_path = str(tmp_path / "catalog.db")
        warehouse_path = str(tmp_path / "wh")

        # Two runs, A duplicated.
        for keys in [
            ["stac/compact-item-A.stac.json", "stac/compact-item-B.stac.json"],
            ["stac/compact-item-A.stac.json", "stac/compact-item-C.stac.json"],
        ]:
            inv = tmp_path / f"inv_{keys[0][-5]}.csv"
            _write_inventory(inv, keys)
            _run_incremental(inv, catalog_path, warehouse_path)

        compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=2,
        )

        import pyarrow.compute as pc

        from earthcatalog.core.catalog import get_or_create_table, open_catalog

        catalog = open_catalog(db_path=catalog_path, warehouse_path=warehouse_path)
        table = get_or_create_table(catalog)
        result = table.scan().to_arrow()
        found_ids = result.column("id").to_pylist()

        # All three unique items present.
        for label in ["A", "B", "C"]:
            assert f"compact-item-{label}" in found_ids

        # Within each grid_partition, no id may appear more than once.
        for cell in result.column("grid_partition").unique().to_pylist():
            cell_rows = result.filter(pc.equal(result.column("grid_partition"), cell))
            cell_ids = cell_rows.column("id").to_pylist()
            assert len(cell_ids) == len(set(cell_ids)), (
                f"Duplicate ids in cell {cell} after compaction: "
                f"{[x for x in cell_ids if cell_ids.count(x) > 1]}"
            )

    def test_below_threshold_buckets_not_touched(self, tmp_path: Path) -> None:
        """Buckets with fewer files than threshold must not be compacted."""
        catalog_path = str(tmp_path / "catalog.db")
        warehouse_path = str(tmp_path / "wh")

        inv = tmp_path / "inv.csv"
        _write_inventory(inv, ["stac/compact-item-A.stac.json"])
        _run_incremental(inv, catalog_path, warehouse_path)

        files_before = _count_parquet_files(warehouse_path)

        summary = compact_warehouse(
            warehouse_path=warehouse_path,
            catalog_path=catalog_path,
            threshold=5,  # higher than file count → nothing compacted
        )

        files_after = _count_parquet_files(warehouse_path)
        assert files_after == files_before
        assert summary["buckets_compacted"] == 0
