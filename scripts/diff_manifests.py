#!/usr/bin/env python3
"""
Diff two S3 Inventory manifest.json files.

Compares the list of Parquet data files between two manifests, reporting
which files are identical (same key + size + checksum), changed (same key,
different content), added, or removed.

Usage
-----
    python scripts/diff_manifests.py \
      s3://log-bucket/inventory/.../2026-04-24T01-00Z/manifest.json \
      s3://log-bucket/inventory/.../2026-04-27T01-00Z/manifest.json

    python scripts/diff_manifests.py \
      s3://log-bucket/inventory/.../2026-04-24T01-00Z/manifest.json \
      s3://log-bucket/inventory/.../2026-04-27T01-00Z/manifest.json \
      --output /tmp/manifest_diff.json
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
    region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")

    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token

    kwargs: dict = {"bucket": bucket, "region": region}
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


def _fetch_manifest(s3_uri: str) -> dict:
    path = s3_uri.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    store = _get_store(bucket)
    raw = bytes(obstore.get(store, key).bytes())
    return json.loads(raw)


def _file_record(f: dict) -> tuple[str, str, int]:
    return (f["key"], f.get("MD5checksum", ""), f.get("size", 0))


def main() -> None:
    parser = argparse.ArgumentParser(description="Diff two S3 Inventory manifests")
    parser.add_argument("yesterday", help="s3:// URI to older manifest.json")
    parser.add_argument("today", help="s3:// URI to newer manifest.json")
    parser.add_argument("--output", help="Write full diff as JSON")
    args = parser.parse_args()

    print(f"Fetching {args.yesterday} ...")
    old = _fetch_manifest(args.yesterday)
    print(f"Fetching {args.today} ...")
    new = _fetch_manifest(args.today)

    old_files = {_file_record(f): f for f in old.get("files", [])}
    new_files = {_file_record(f): f for f in new.get("files", [])}

    old_keys = {rec[0]: rec for rec in old_files}
    new_keys = {rec[0]: rec for rec in new_files}

    old_key_set = set(old_keys)
    new_key_set = set(new_keys)

    shared = old_key_set & new_key_set
    identical = {k for k in shared if old_keys[k] == new_keys[k]}
    changed = shared - identical
    added = new_key_set - old_key_set
    removed = old_key_set - new_key_set

    print()
    print(f"Yesterday ({old.get('sourceBucket', '?')}): {len(old_files)} file(s)")
    print(f"Today     ({new.get('sourceBucket', '?')}): {len(new_files)} file(s)")
    print()
    print(f"Identical (same key + size + checksum): {len(identical)}")
    print(f"Changed  (same key, different content): {len(changed)}")
    print(f"Added    (new in today):                {len(added)}")
    print(f"Removed  (gone from today):             {len(removed)}")

    if changed:
        print()
        print("Changed files:")
        for k in sorted(changed):
            old_rec = old_keys[k]
            new_rec = new_keys[k]
            print(f"  {k}")
            print(f"    old: size={old_rec[2]}, md5={old_rec[1]}")
            print(f"    new: size={new_rec[2]}, md5={new_rec[1]}")

    if added:
        print()
        print("Added files:")
        for k in sorted(added):
            rec = new_keys[k]
            print(f"  {k}  (size={rec[2]}, md5={rec[1]})")

    if removed:
        print()
        print("Removed files:")
        for k in sorted(removed):
            rec = old_keys[k]
            print(f"  {k}  (size={rec[2]}, md5={rec[1]})")

    if args.output:
        diff = {
            "yesterday_manifest": args.yesterday,
            "today_manifest": args.today,
            "yesterday_file_count": len(old_files),
            "today_file_count": len(new_files),
            "identical": len(identical),
            "changed": [
                {
                    "key": k,
                    "old_size": old_keys[k][2],
                    "old_md5": old_keys[k][1],
                    "new_size": new_keys[k][2],
                    "new_md5": new_keys[k][1],
                }
                for k in sorted(changed)
            ],
            "added": [
                {"key": k, "size": new_keys[k][2], "md5": new_keys[k][1]}
                for k in sorted(added)
            ],
            "removed": [
                {"key": k, "size": old_keys[k][2], "md5": old_keys[k][1]}
                for k in sorted(removed)
            ],
        }
        with open(args.output, "w") as f:
            json.dump(diff, f, indent=2)
        print(f"\nDiff written to {args.output}")


if __name__ == "__main__":
    main()
