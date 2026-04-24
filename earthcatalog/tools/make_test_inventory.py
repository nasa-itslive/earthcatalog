"""
Generate a test S3 Inventory CSV by listing a public S3 prefix with obstore.

Usage:
    python -m earthcatalog.make_test_inventory \
        --prefix test-space/velocity_image_pair/nisar/v02 \
        --limit  2000 \
        --output /tmp/test_inventory.csv
"""

import argparse
import csv

from obstore.store import S3Store

BUCKET = "its-live-data"
REGION = "us-west-2"


def list_json_keys(prefix: str, limit: int) -> list[str]:
    store = S3Store(
        bucket=BUCKET,
        prefix=prefix,
        region=REGION,
        skip_signature=True,
    )

    keys: list[str] = []

    for batch in store.list():
        for entry in batch:
            path = entry["path"]
            if path.endswith(".json"):
                full_key = f"{prefix.rstrip('/')}/{path.lstrip('/')}"
                keys.append(full_key)
            if len(keys) >= limit:
                return keys

    return keys


def write_csv(keys: list[str], output_path: str) -> None:
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["bucket", "key"])
        for key in keys:
            writer.writerow([BUCKET, key])
    print(f"Wrote {len(keys)} rows → {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate test S3 Inventory CSV from public bucket"
    )
    parser.add_argument(
        "--prefix",
        default="test-space/velocity_image_pair/nisar/v02",
        help="S3 prefix to list (default: nisar v02 test space)",
    )
    parser.add_argument(
        "--limit", type=int, default=2000, help="Max JSON files to include (default 2000)"
    )
    parser.add_argument(
        "--output",
        default="/tmp/test_inventory.csv",
        help="Output CSV path (default /tmp/test_inventory.csv)",
    )
    args = parser.parse_args()

    print(f"Listing s3://{BUCKET}/{args.prefix} (limit {args.limit} JSON files)...")
    keys = list_json_keys(args.prefix, args.limit)
    print(f"Found {len(keys)} JSON files.")
    write_csv(keys, args.output)

    print("\nPreview:")
    for k in keys[:3]:
        print(f"  s3://{BUCKET}/{k}")
    if len(keys) > 3:
        print(f"  ... and {len(keys) - 3} more")
