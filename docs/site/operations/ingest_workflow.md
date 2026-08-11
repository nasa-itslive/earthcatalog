# Ingest Workflow

The daily delta workflow keeps the catalog in sync with the ITS_LIVE S3
Inventory.  It runs automatically via GitHub Actions and can also be run
manually.

**Schedule:** `daily_delta` at 02:00 UTC daily; GC Sunday 08:00; consolidation Sunday 09:00.

## How the daily diff works

The unified index (`warehouse_index.parquet`) is the source of truth for what
is already in the catalog.  Every daily run compares it against the current
S3 Inventory and ingests only the difference:

1. **Produce delta** — `scripts/daily_delta.py` streams the inventory,
   computes an `xxh3_128(item_id)` hash per `.stac.json` key, and anti-joins
   against `Index.hash_set()`.  Only keys **not** already indexed are written
   to `s3://…/delta/pending/delta_YYYY-MM-DD.parquet`.
2. **Ingest delta** — `earthcatalog backfill --mode delta` consumes that
   delta, fetches STAC JSONs, writes GeoParquet, and appends rows to the
   warehouse + unified index.  The `Ingester` re-checks
   `Index.contains_source_key()` as a second idempotency net.
3. **Mark ingested** — the delta is moved from `pending/` to `ingested/`.

```bash
# Job 1 — compute the diff
uv run python scripts/daily_delta.py \
  "s3://…/inventory/2026-04-28T01-00Z/manifest.json" \
  --warehouse-hash s3://…/warehouse_index.parquet \
  --delta-prefix   s3://…/delta \
  --date 2026-04-28

# Job 2 — ingest the diff
uv run earthcatalog backfill \
  --inventory s3://…/delta/pending/delta_2026-04-28.parquet \
  --catalog   /tmp/earthcatalog.db \
  --warehouse s3://…/warehouse \
  --mode      delta \
  --scheduler local \
  --workers   4
```

## Recovery

Ingest is resumable — the unified index is the checkpoint, so a failed run can
simply be re-invoked; already-ingested source keys are skipped.

| Situation | What to do |
|-----------|------------|
| Job 2 failed mid-run | Re-run the same `backfill --mode delta` command.  Keys already indexed are skipped; only unfinished work is reprocessed. |
| Job 1 wrote a partial delta | Re-run `daily_delta.py` — it is idempotent and merges pending deltas. |
| Need to stage only, compact later | `backfill --mode delta --skip-compact` (fetch + NDJSON only). |
| Resume from staged NDJSON | `backfill --mode delta --skip-fetch` (compact staged NDJSON only). |
| Not sure a delta was ingested | `aws s3 ls s3://…/delta/ingested/` — `.done` marker means it succeeded. |

## Verify after ingest

```bash
uv run python scripts/info.py \
  --catalog   /tmp/earthcatalog.db \
  --warehouse s3://…/warehouse
```

Output shows rows, files, unique cells, and year range.
