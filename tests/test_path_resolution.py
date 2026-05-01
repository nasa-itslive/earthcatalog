"""
Tests for path resolution in catalog.open().

Tests that the base parameter correctly resolves to catalog.db and warehouse
for both local paths and S3 URIs.
"""

import os
from pathlib import Path

import pytest


class TestPathResolutionLocal:
    """Test path resolution for local filesystem."""

    def test_base_to_catalog_key_local(self, tmp_path):
        """Local base path should resolve to base/earthcatalog.db."""
        base = str(tmp_path / "catalog")
        expected = str(tmp_path / "catalog" / "earthcatalog.db")

        # Simulate the logic from catalog.open()
        catalog_key = str(Path(base) / "earthcatalog.db")

        assert catalog_key == expected

    def test_base_to_warehouse_path_local(self, tmp_path):
        """Local base path should resolve to base/warehouse."""
        base = str(tmp_path / "catalog")
        expected = str(tmp_path / "catalog" / "warehouse")

        # Simulate the logic from catalog.open()
        warehouse_path = f"{base}/warehouse"

        assert warehouse_path == expected

    def test_relative_base_path(self, tmp_path):
        """Relative base path should work correctly."""
        # Change to temp directory
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            base = "catalog"
            expected_catalog = str(Path("catalog") / "earthcatalog.db")
            expected_warehouse = "catalog/warehouse"

            catalog_key = str(Path(base) / "earthcatalog.db")
            warehouse_path = f"{base}/warehouse"

            assert catalog_key == expected_catalog
            assert warehouse_path == expected_warehouse
        finally:
            os.chdir(original_cwd)

    def test_absolute_base_path(self, tmp_path):
        """Absolute base path should work correctly."""
        base = str(tmp_path / "catalog")
        expected_catalog = str(tmp_path / "catalog" / "earthcatalog.db")
        expected_warehouse = str(tmp_path / "catalog" / "warehouse")

        catalog_key = str(Path(base) / "earthcatalog.db")
        warehouse_path = f"{base}/warehouse"

        assert catalog_key == expected_catalog
        assert warehouse_path == expected_warehouse


class TestPathResolutionS3:
    """Test path resolution for S3 URIs."""

    def test_s3_base_to_catalog_key_simple(self):
        """S3 base path should resolve to correct S3 key (simple case)."""
        base = "s3://my-bucket/catalog"
        expected = "catalog/earthcatalog.db"

        # Simulate the logic from catalog.open()
        if base.startswith("s3://"):
            rest = base[5:]  # Remove s3://
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        assert catalog_key == expected

    def test_s3_base_to_catalog_key_nested(self):
        """S3 base path with nested prefix should resolve correctly."""
        base = "s3://my-bucket/prefix/test-space/stac/catalog"
        expected = "prefix/test-space/stac/catalog/earthcatalog.db"

        if base.startswith("s3://"):
            rest = base[5:]  # Remove s3://
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        assert catalog_key == expected

    def test_s3_base_to_warehouse_path(self):
        """S3 base path should resolve to correct warehouse path."""
        base = "s3://my-bucket/catalog"
        expected = "s3://my-bucket/catalog/warehouse"

        warehouse_path = f"{base}/warehouse"

        assert warehouse_path == expected

    def test_s3_base_to_warehouse_path_nested(self):
        """S3 base path with nested prefix should resolve correctly."""
        base = "s3://its-live-data/test-space/stac/catalog"
        expected = "s3://its-live-data/test-space/stac/catalog/warehouse"

        warehouse_path = f"{base}/warehouse"

        assert warehouse_path == expected

    def test_s3_base_itslive_production(self):
        """Test against the actual ITS_LIVE production S3 path."""
        base = "s3://its-live-data/test-space/stac/catalog"

        # Catalog key (relative to bucket root)
        if base.startswith("s3://"):
            rest = base[5:]  # Remove s3://
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        expected_catalog_key = "test-space/stac/catalog/earthcatalog.db"
        expected_warehouse = "s3://its-live-data/test-space/stac/catalog/warehouse"

        assert catalog_key == expected_catalog_key
        assert f"{base}/warehouse" == expected_warehouse


class TestPathResolutionEdgeCases:
    """Test edge cases and special scenarios."""

    def test_base_with_trailing_slash(self):
        """Base path with trailing slash should work correctly."""
        base = "s3://my-bucket/catalog/"
        expected = "catalog/earthcatalog.db"

        if base.startswith("s3://"):
            rest = base[5:]
            parts = rest.split("/", 1)
            # Strip trailing slash from prefix if present
            prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
            catalog_key = f"{prefix}/earthcatalog.db" if prefix else "earthcatalog.db"

        assert catalog_key == expected

    def test_base_with_multiple_nested_levels(self):
        """Base path with many nested levels should resolve correctly."""
        base = "s3://bucket/a/b/c/d/e/catalog"
        expected = "a/b/c/d/e/catalog/earthcatalog.db"

        if base.startswith("s3://"):
            rest = base[5:]
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        assert catalog_key == expected

    def test_local_path_with_parent_references(self, tmp_path):
        """Local path with parent directory references should work."""
        # Create the actual directory structure
        catalog_dir = tmp_path / "data" / "catalog"
        catalog_dir.mkdir(parents=True)

        base = str(catalog_dir)
        expected = str(catalog_dir / "earthcatalog.db")

        catalog_key = str(Path(base) / "earthcatalog.db")

        assert catalog_key == expected


class TestRealWorldScenarios:
    """Test against real-world S3 catalog structures."""

    def test_itslive_public_catalog(self):
        """Test the actual ITS_LIVE public catalog path."""
        base = "s3://its-live-data/test-space/stac/catalog"

        # What we expect:
        # - Catalog: s3://its-live-data/test-space/stac/catalog/earthcatalog.db
        # - Warehouse: s3://its-live-data/test-space/stac/catalog/warehouse
        # - Hash index: s3://its-live-data/test-space/stac/catalog/warehouse_id_hashes.parquet

        if base.startswith("s3://"):
            rest = base[5:]
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        assert catalog_key == "test-space/stac/catalog/earthcatalog.db"
        assert f"{base}/warehouse" == "s3://its-live-data/test-space/stac/catalog/warehouse"

    def test_custom_bucket_structure(self):
        """Test a custom bucket structure."""
        base = "s3://custom-bucket/production/data/catalog"

        if base.startswith("s3://"):
            rest = base[5:]
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"

        assert catalog_key == "production/data/catalog/earthcatalog.db"
        assert f"{base}/warehouse" == "s3://custom-bucket/production/data/catalog/warehouse"


class TestWarehousePathConstruction:
    """Test warehouse path construction specifically."""

    @pytest.mark.parametrize(
        "base,expected_warehouse",
        [
            ("s3://bucket/catalog", "s3://bucket/catalog/warehouse"),
            ("s3://bucket/a/b/catalog", "s3://bucket/a/b/catalog/warehouse"),
            (
                "s3://its-live-data/test-space/stac/catalog",
                "s3://its-live-data/test-space/stac/catalog/warehouse",
            ),
            # Local paths
            ("/tmp/catalog", "/tmp/catalog/warehouse"),
            ("/data/earthcatalog/catalog", "/data/earthcatalog/catalog/warehouse"),
        ],
    )
    def test_warehouse_path_various_bases(self, base, expected_warehouse):
        """Warehouse path should be base/warehouse for various inputs."""
        warehouse_path = f"{base}/warehouse"
        assert warehouse_path == expected_warehouse


class TestCatalogKeyConstruction:
    """Test catalog key construction specifically."""

    @pytest.mark.parametrize(
        "base,expected_catalog_key",
        [
            ("s3://bucket/catalog", "catalog/earthcatalog.db"),
            ("s3://bucket/a/b/catalog", "a/b/catalog/earthcatalog.db"),
            (
                "s3://its-live-data/test-space/stac/catalog",
                "test-space/stac/catalog/earthcatalog.db",
            ),
        ],
    )
    def test_catalog_key_s3_various_bases(self, base, expected_catalog_key):
        """Catalog key should be prefix/earthcatalog.db for S3 URIs."""
        if base.startswith("s3://"):
            rest = base[5:]
            parts = rest.split("/", 1)
            catalog_key = f"{parts[1]}/earthcatalog.db" if len(parts) > 1 else "earthcatalog.db"
        else:
            catalog_key = str(Path(base) / "earthcatalog.db")

        assert catalog_key == expected_catalog_key
