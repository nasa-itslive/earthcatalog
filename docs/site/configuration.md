# Configuration

earthcatalog can be configured with a YAML file passed via `--config` or
with individual CLI flags.  A config file is recommended for reproducibility.

---

## YAML config file

```yaml
# config/h3_r1.yaml

catalog:
  db_path:   /tmp/earthcatalog.db          # local path to the SQLite catalog
  warehouse: /tmp/earthcatalog_warehouse   # local path to the Parquet warehouse

grid:
  type:       h3
  resolution: 1    # H3 resolution (0–15); 1 = production default

ingest:
  chunk_size:       500    # STAC items per ThreadPoolExecutor batch
  max_workers:      16     # concurrent S3 fetch threads
  batch_add_files:  false  # true = one Iceberg snapshot for entire run (backfill)
```

Pass it with:

```bash
earthcatalog incremental --config config/h3_r1.yaml --inventory /tmp/delta.csv
```

---

## Config sections

### `catalog`

| Key | Type | Required | Default | Description |
|---|---|:---:|---|---|
| `db_path` | string | yes | — | Local filesystem path to `catalog.db` (SQLite) |
| `warehouse` | string | yes | — | Local filesystem path to the Parquet warehouse root |

### `grid`

| Key | Type | Required | Default | Description |
|---|---|:---:|---|---|
| `type` | `h3` \| `geojson` | yes | — | Partitioner type |
| `resolution` | int | no | `1` | H3 resolution (0–15).  Only used when `type = h3` |
| `boundaries_path` | string | no | — | Path to GeoJSON file.  Only used when `type = geojson` |
| `id_field` | string | no | `"id"` | Feature property to use as partition key for GeoJSON |

### `ingest`

| Key | Type | Required | Default | Description |
|---|---|:---:|---|---|
| `chunk_size` | int | no | `500` | Items fetched per batch |
| `max_workers` | int | no | `8` | Parallel S3 fetch threads |
| `batch_add_files` | bool | no | `false` | Accumulate all file paths and register in one Iceberg snapshot at the end of the run.  Recommended for backfill. |

---

## H3 resolution guide

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

### `earthcatalog backfill`

```
--inventory       S3 Inventory file path
--catalog         catalog.db path
--warehouse       Warehouse root path
--scheduler       dask scheduler: synchronous | local | coiled (default: synchronous)
--workers         Dask local-cluster worker count (default: 4)
--limit           Cap on STAC items to ingest
--chunk-size      Items per Dask task (default: 500)
--coiled-n-workers    Coiled cluster size
--coiled-software     Coiled software environment name
--coiled-region       AWS region for Coiled cluster
```
