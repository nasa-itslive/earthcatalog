"""S3 URI helpers — the one canonical parser.

Roughly a dozen modules need to split ``s3://bucket/key`` into
``(bucket, key)``; before :mod:`earthcatalog.uris` each had its own inline
``removeprefix(...).split(...)`` with subtly different fallback behaviour.
"""

from __future__ import annotations


def parse_s3_uri(uri: str) -> tuple[str, str] | None:
    """Parse an ``s3://`` URI into ``(bucket, key)``.

    Returns ``None`` for anything that is not an ``s3://`` URI (local paths
    pass through via the None convention).  A bucket-only URI yields an
    empty key: ``s3://bucket`` → ``("bucket", "")``.
    """
    if not uri.startswith("s3://"):
        return None
    no_scheme = uri.removeprefix("s3://")
    parts = no_scheme.split("/", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""


def strip_bucket(uri_or_key: str) -> str:
    """Store-relative key from an ``s3://bucket/key`` URI (the ``key`` part).

    Non-S3 strings pass through unchanged — bucket-level obstore stores take
    keys relative to the bucket, callers keep local paths as-is.
    """
    parsed = parse_s3_uri(uri_or_key)
    return parsed[1] if parsed else uri_or_key
