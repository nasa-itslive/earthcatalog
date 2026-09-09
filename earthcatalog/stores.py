"""Object-store construction — one factory instead of ad-hoc S3Store calls.

AWS credentials resolve from the environment first, then
``~/.aws/credentials`` (respecting ``AWS_PROFILE``) — the single copy of
that fallback logic.  Anonymous (unsigned) access is a separate constructor
so public buckets never accidentally pick up ambient credentials.
"""

from __future__ import annotations

import configparser
import os

from obstore.store import S3Store

_DEFAULT_REGION = "us-west-2"


def _region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or _DEFAULT_REGION


def aws_credentials() -> tuple[str, str, str]:
    """``(key_id, secret, token)`` from the environment, else
    ``~/.aws/credentials``.  Empty strings when unresolvable."""
    key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    token = os.environ.get("AWS_SESSION_TOKEN", "")
    if not (key_id and secret):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.aws/credentials"))
        profile = os.environ.get("AWS_PROFILE", "default")
        if profile in cfg:
            key_id = cfg[profile].get("aws_access_key_id", key_id)
            secret = cfg[profile].get("aws_secret_access_key", secret)
            token = cfg[profile].get("aws_session_token", token) or token
    return key_id, secret, token


def make_s3_store(bucket: str, prefix: str = "", *, region: str | None = None) -> S3Store:
    """Authenticated S3 store: env credentials, else ``~/.aws/credentials``.

    *region* defaults to ``AWS_DEFAULT_REGION`` / ``AWS_REGION`` / us-west-2.
    """
    key_id, secret, token = aws_credentials()
    kwargs: dict = dict(bucket=bucket, region=region or _region())
    if prefix:
        kwargs["prefix"] = prefix
    if key_id:
        kwargs["aws_access_key_id"] = key_id
    if secret:
        kwargs["aws_secret_access_key"] = secret
    if token:
        kwargs["aws_session_token"] = token
    return S3Store(**kwargs)


_ANONYMOUS_STORES: dict[str, S3Store] = {}


def make_anonymous_store(bucket: str, *, region: str | None = None) -> S3Store:
    """Unsigned S3 store for public buckets (cached per bucket)."""
    if bucket not in _ANONYMOUS_STORES:
        _ANONYMOUS_STORES[bucket] = S3Store(
            bucket=bucket, region=region or _DEFAULT_REGION, skip_signature=True
        )
    return _ANONYMOUS_STORES[bucket]
