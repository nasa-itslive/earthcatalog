"""Ingest configuration — folds ingest tuning knobs into one object."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime


@dataclass
class IngestConfig:
    """Tuning knobs for the ingest pipeline.

    Pass a single instance to :meth:`EarthCatalog.ingest_inventory`
    instead of a dozen keyword arguments.
    """

    chunk_size: int = 100_000
    limit: int | None = None
    since: datetime | None = None
    staging_prefix: str | None = None
    create_client: Callable[[], object] | None = None
    delta: bool | None = None
    # Daily path: consume a diff Parquet from `earthcatalog diff` instead of
    # a raw inventory manifest.  Mutually exclusive with the inventory path.
    diff: str | None = None
    # Count the keys the run would fetch (diff vs index) and exit — no writes.
    dry_run: bool = False
    skip_fetch: bool = False
    skip_compact: bool = False
    # Distributed only: stop after the scatter step and return the scatter
    # manifest path — workers never idle behind the head's inventory read.
    # Re-invoke with inventory_path=<scatter.json> to run the map/reduce.
    scatter_only: bool = False
    # "ndjson" stages items to per-(cell, year) NDJSON first (resumable,
    # memory-bounded compaction); "direct" writes GeoParquet immediately.
    stage: str = "ndjson"
    # Concurrent in-flight S3 GETs per Dask worker during the STAC fetch (async
    # via obstore.get_async, so this is lightweight — no thread per request).
    fetch_concurrency: int = 256
    # Bounded fetch pool for the serial (daily) path; 1 = strictly serial.
    fetch_workers: int = 16

    @classmethod
    def from_kwargs(cls, **kwargs) -> IngestConfig:
        """Build from the legacy keyword arguments (unknown keys ignored)."""
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in kwargs.items() if k in allowed})


# Back-compat alias — the pre-rename public name.
BackfillConfig = IngestConfig
