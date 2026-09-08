# Configuration

earthcatalog is configured with CLI flags on the `ingest` command (or the
`IngestConfig` dataclass when called from Python).  There is no YAML config
file; flags are preferred for reproducibility in CI.

---

## CLI usage

```bash
uv run earthcatalog ingest \
  --inventory s3://bucket/inventory/full.parquet \
  --warehouse s3://my-bucket/catalog/warehouse \
  --mode full \
  --scheduler local \
  --workers 4 \
  --chunk-size 100000
```

Grid selection for a fresh full build (`--grid h3|s2|utm|geojson`,
`--resolution`, `--boundaries`, `--id-field`) is passed on the command line.

---

## Ingest flags reference

| Flag | Default | Description |
|---|---|---|
| `--inventory` | — | S3 Inventory file path (CSV, Parquet, or manifest.json) |
| `--catalog` | `/tmp/earthcatalog.db` | catalog.db path |
| `--warehouse` | `s3://its-live-data/test-space/stac/catalog/warehouse` | Warehouse root path |
| `--mode` | `auto` | `full` \| `delta` \| `auto` (default: `auto`) |
| `--scheduler` | `synchronous` | Dask scheduler: `synchronous` \| `local` \| `coiled` |
| `--workers` | `4` | Dask local-cluster worker count |
| `--limit` | — | Cap on STAC items to ingest |
| `--chunk-size` | `100000` | Items per fetch chunk |
| `--skip-fetch` | — | Resume: skip fetch + NDJSON staging, only compact staged NDJSON |
| `--skip-compact` | — | Only fetch + stage NDJSON; compact later |
| `--grid` | `h3` | `h3` \| `s2` \| `utm` \| `geojson` (for fresh full builds) |
| `--resolution` | — | Grid resolution (h3/s2; default: h3=1, s2=2) |
| `--boundaries` | — | GeoJSON boundaries path (required for `--grid geojson`) |
| `--id-field` | — | GeoJSON feature property used as the partition key |
| `--catalog-key` | `EARTHCATALOG_CATALOG_KEY` | Object key for the uploaded catalog.db |
| `--lock-key` | `EARTHCATALOG_LOCK_KEY` | Object key for the distributed lock file |

Use `uv run earthcatalog ingest --help` for the full list.

| Resolution | Avg. cell area | Global cells | Recommendation |
|:---:       |:---:           |:---:         |---             |
| 0          | ~4,250,000 km² | 122          | Very coarse; continental scale |
| **1**      | **~607,220 km²** | **842**    | **Production default** |
| 2          | ~86,750 km²    | 5,882        | Sub-regional |
| 3          | ~12,390 km²    | 41,162       | Dense urban datasets |

!!! note "Test resolution"
    Integration tests use **resolution 2** for faster H3 calculations.
    The production ITS_LIVE catalog uses **resolution 1**.

---

## GeoJSON partitioner config

```yaml
grid:
  type:            geojson
  boundaries_path: /path/to/regions.geojson
  id_field:        region_name
```

Each feature in the GeoJSON file becomes a partition cell, identified by the
value of the `id_field` property.

---

## Inventory file format

The inventory (and delta) file tells earthcatalog which STAC items to ingest.
It must contain at minimum two columns — `bucket` and `key` — pointing to
`.stac.json` files on S3.

### Parquet

```parquet
bucket: string    # S3 bucket (e.g. "its-live-data")
key:    string    # S3 object key ending in ".stac.json"
```

Optional column for `since=` filtering:

```
last_modified_date: timestamp  # used when --since is passed
```

### CSV

Same columns, header row required when using `--since`:

```csv
bucket,key,last_modified_date
its-live-data,path/to/item.stac.json,2026-04-28T01:00:00.000Z
```

### Delta files

Delta parquets use the **same schema** as the full inventory — only the rows
differ (new/modified items only). `ec.ingest_inventory()` reads any
supported format.

### Manifest (AWS S3 Inventory)

A `manifest.json` referencing multiple Parquet data files in a private
destination bucket. earthcatalog reads credentials from env vars or
`~/.aws/credentials`.

## Environment variables (S3 store)

earthcatalog reads AWS credentials from standard environment variables when
writing to or reading from a private S3 bucket:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-west-2
```

For the **public ITS_LIVE bucket** no credentials are needed
(`skip_signature=True` is set automatically).

---

## CLI flags reference

### `earthcatalog incremental`

```
--config       Path to YAML config file (optional)
--inventory    Path to S3 Inventory file (CSV, CSV.gz, Parquet, or manifest.json)
--catalog      Path to catalog.db (overrides config)
--warehouse    Path to warehouse root (overrides config)
--since        ISO date — skip items not modified since this date (e.g. 2026-04-01)
--limit        Maximum number of STAC items to ingest (useful for smoke tests)
--chunk-size   Items per batch (default: 500)
--max-workers  Fetch threads (default: 8)
--no-lock      Skip the S3 lock (for local development)
--resolution   H3 resolution (default: 1)
```

### `earthcatalog ingest`

```
--inventory       S3 Inventory file path
--catalog         catalog.db path
--warehouse       Warehouse root path
--mode            full | delta | auto (default: auto)
--scheduler       dask scheduler: synchronous | local | coiled (default: synchronous)
--workers         Dask local-cluster worker count (default: 4)
--limit           Cap on STAC items to ingest
--chunk-size      Items per fetch chunk (default: 100000)
--skip-fetch      Resume: skip fetch + NDJSON staging, only compact staged NDJSON
--skip-compact    Only fetch + stage NDJSON; compact later
--grid            h3 | s2 | utm | geojson (for fresh full builds; default: h3)
--resolution      Grid resolution (h3/s2; default: h3=1, s2=2)
--boundaries      GeoJSON boundaries path (required for --grid geojson)
--id-field        GeoJSON feature property used as the partition key
```
