# Ingest Workflow

The daily delta workflow keeps the catalog in sync with the ITS_LIVE S3
Inventory.  It runs automatically via GitHub Actions and can also be run
manually.

**Schedule:** `daily_delta` daily; GC Sunday; consolidation Sunday (disabled until AWS secrets are injected).

## How the daily diff works

The unified index (`warehouse_index.parquet`) is the source of truth for what
is already in the catalog.  Every run compares it against the current S3
Inventory (a DuckDB anti-join inside the ingest pipeline) and ingests only
the difference:

1. **Diff** — the pipeline streams the inventory and anti-joins against
   `Index.known_source_keys()`.  Only keys **not** already indexed are
   fetched.
2. **Ingest** — STAC JSONs are fetched, staged as NDJSON, written as
   GeoParquet, and appended to the warehouse + unified index.  The
   `Ingester` re-checks `Index.contains_source_key()` as a second
   idempotency net.

```bash
uv run earthcatalog ingest \
  --inventory s3://…/inventory/2026-04-28T01-00Z/manifest.json \
  --catalog   /tmp/earthcatalog.db \
  --warehouse s3://…/warehouse \
  --mode      auto \
  --scheduler local \
  --workers   4
```

## Recovery

Ingest is resumable — the unified index is the checkpoint, so a failed run can
simply be re-invoked; already-ingested source keys are skipped.

| Situation | What to do |
|-----------|------------|
| Ingest failed mid-run | Re-run the same `earthcatalog ingest` command.  Keys already indexed are skipped; only unfinished work is reprocessed. |
| Need to stage only, compact later | `ingest --mode delta --skip-compact` (fetch + NDJSON only). |
| Resume from staged NDJSON | `ingest --mode delta --skip-fetch` (compact staged NDJSON only). |

## Verify after ingest

```bash
uv run earthcatalog info \
  --catalog   /tmp/earthcatalog.db \
  --warehouse s3://…/warehouse
```

Output shows rows, files, unique cells, and year range.
