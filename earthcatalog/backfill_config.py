"""Backfill configuration — folds run_backfill tuning knobs into one object."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime


@dataclass
class BackfillConfig:
    """Tuning knobs for the distributed ingest pipeline.

    Pass a single instance to :meth:`EarthCatalog.bulk_ingest` instead of
    a dozen keyword arguments.
    """

    chunk_size: int = 100_000
    compact_rows: int = 100_000
    limit: int | None = None
    since: datetime | None = None
    staging_prefix: str | None = None
    create_client: Callable[[], object] | None = None
    delta: bool | None = None
    skip_fetch: bool = False
    skip_compact: bool = False
    # "ndjson" stages items to per-(cell, year) NDJSON first (resumable,
    # memory-bounded compaction); "direct" writes GeoParquet immediately.
    stage: str = "ndjson"

    @classmethod
    def from_kwargs(cls, **kwargs) -> BackfillConfig:
        """Build from the legacy keyword arguments (unknown keys ignored)."""
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in kwargs.items() if k in allowed})
