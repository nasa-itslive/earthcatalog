#!/usr/bin/env python3
"""Thin shim — the ingest entry point lives in the package.

Wheels only package ``earthcatalog*``, so the implementation must be
importable without the ``scripts`` directory.  Everything here is
re-exported for backward compatibility with callers that did
``from scripts.ingest import run``.
"""

from earthcatalog.run import main, run

__all__ = ["main", "run"]

if __name__ == "__main__":
    main()
