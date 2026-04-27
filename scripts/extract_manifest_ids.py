"""
Extract STAC item IDs from an AWS S3 Inventory manifest.

Usage:
    python scripts/extract_manifest_ids.py \
        s3://my-bucket/inventory/its-live-data/manifest.json \
        -o /tmp/manifest_ids.txt

Compare 3 sources:
    PGSTAC: ~40,115,936 IDs (the full pgstac catalog)
    Iceberg: ~38,969,304 IDs (warehouse parquet files)
    Manifest: IDs from AWS S3 Inventory (ground truth for what exists in S3)

Expected hierarchy: S3 (manifest) >= PGSTAC >= Iceberg
"""

import argparse
import sys

from earthcatalog.pipelines.incremental import _iter_inventory_manifest


def key_to_id(key: str) -> str | None:
    """Derive STAC item ID from an S3 key by stripping the .stac.json suffix."""
    if not key.endswith(".stac.json"):
        return None
    return key.rsplit("/", 1)[-1].removesuffix(".stac.json")


def main():
    parser = argparse.ArgumentParser(description="Extract STAC IDs from AWS S3 Inventory manifest")
    parser.add_argument("manifest", help="s3:// URI to manifest.json")
    parser.add_argument("-o", "--output", required=True, help="Output file path")
    parser.add_argument("--limit", type=int, default=None, help="Max items to process")
    args = parser.parse_args()

    print(f"Reading manifest: {args.manifest}", file=sys.stderr)
    count = 0
    written = 0

    with open(args.output, "w") as out:
        for bucket, key in _iter_inventory_manifest(args.manifest):
            item_id = key_to_id(key)
            if item_id is None:
                continue
            count += 1
            out.write(item_id + "\n")
            written += 1
            if count % 100_000 == 0:
                print(f"  processed {count:,}, written {written:,}", file=sys.stderr)
            if args.limit and count >= args.limit:
                break

    print(
        f"Done: {count:,} .stac.json keys processed, {written:,} IDs written to {args.output}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
