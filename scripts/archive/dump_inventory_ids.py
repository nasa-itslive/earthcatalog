"""
Dump all STAC item IDs from the JSON files referenced in a CSV/Parquet inventory.

Usage:
    python scripts/dump_inventory_ids.py /tmp/missing_inventory.csv.gz -o /tmp/inventory_ids.txt
    python scripts/dump_inventory_ids.py /tmp/missing_inventory.csv.gz -o /tmp/inventory_ids.txt --limit 1000
"""

import argparse
import json
import sys

import obstore
from obstore.store import S3Store


def _get_store(bucket: str) -> S3Store:
    import configparser
    import os

    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    token = os.environ.get("AWS_SESSION_TOKEN")

    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = key_id or cfg[profile].get("aws_access_key_id")
            secret = secret or cfg[profile].get("aws_secret_access_key")
            token = token or cfg[profile].get("aws_session_token") or token

    kwargs = dict(bucket=bucket, region="us-west-2")
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


def _iter_inventory_csv(path: str):
    import gzip

    if path.endswith(".gz"):
        with gzip.open(path, "rt", newline="") as f:
            yield from _parse_csv(f)
    else:
        with open(path, newline="") as f:
            yield from _parse_csv(f)


def _parse_csv(fh):
    import csv

    reader = csv.reader(fh)
    header_seen = False
    for row in reader:
        if not row:
            continue
        if not header_seen:
            header_seen = True
            if row[0].strip('"').lower() == "bucket":
                continue
        yield row[0].strip('"'), row[1].strip('"')


def _fetch_json_id(store: S3Store, key: str) -> str | None:
    try:
        raw = bytes(obstore.get(store, key).bytes())
        doc = json.loads(raw)
        return doc.get("id")
    except Exception as exc:
        print(f"WARN: failed to fetch/extract {key}: {exc}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(description="Dump STAC IDs from inventory")
    parser.add_argument("inventory", help="Path to inventory CSV[.gz]")
    parser.add_argument("-o", "--output", required=True, help="Output file path")
    parser.add_argument("--limit", type=int, default=None, help="Max items to process")
    parser.add_argument("--concurrency", type=int, default=64, help="Concurrent fetches")
    args = parser.parse_args()

    stores: dict[str, S3Store] = {}

    print(f"Reading inventory: {args.inventory}", file=sys.stderr)
    count = 0
    found = 0
    with open(args.output, "w") as out:
        for bucket, key in _iter_inventory_csv(args.inventory):
            if not key.endswith(".stac.json"):
                continue
            count += 1
            if bucket not in stores:
                stores[bucket] = _get_store(bucket)
            item_id = _fetch_json_id(stores[bucket], key)
            if item_id:
                out.write(item_id + "\n")
                found += 1
            if count % 1000 == 0:
                print(f"  processed {count:,}, found {found:,}", file=sys.stderr)
            if args.limit and count >= args.limit:
                break

    print(
        f"Done: {count:,} keys processed, {found:,} IDs written to {args.output}", file=sys.stderr
    )


if __name__ == "__main__":
    main()
