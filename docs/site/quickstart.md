# Quick Start

Get earthcatalog up and running in five minutes.

---

## Prerequisites

- [mamba](https://mamba.readthedocs.io/) (or conda) for environment management
- Python 3.12+
- AWS credentials (only needed to write to a private S3 bucket; reading ITS_LIVE data is public)

---

## 1. Install

```bash
# Create the conda environment
mamba env create -f environment.yml
mamba activate itslive-ingest

# Install earthcatalog in editable mode
pip install -e .
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run earthcatalog --help
```

---

## 2. Generate a test inventory

```bash
python -m earthcatalog.tools.make_test_inventory \
    --prefix test-space/velocity_image_pair/nisar/v02 \
    --limit  500 \
    --output /tmp/test_inventory.csv
```

This reads the ITS_LIVE public S3 bucket (no credentials needed) and writes
a CSV with `bucket` + `key` columns pointing to `.stac.json` files.

---

## 3. Run the incremental pipeline

=== "With flags"

    ```bash
    earthcatalog incremental \
        --inventory  /tmp/test_inventory.csv \
        --catalog    /tmp/catalog.db \
        --warehouse  /tmp/warehouse \
        --limit      500
    ```

=== "With a config file"

    ```yaml title="config/h3_r1.yaml"
    catalog:
      db_path:   /tmp/catalog.db
      warehouse: /tmp/warehouse

    grid:
      type:       h3
      resolution: 1

    ingest:
      chunk_size:  500
      max_workers: 8
    ```

    ```bash
    earthcatalog incremental \
        --config    config/h3_r1.yaml \
        --inventory /tmp/test_inventory.csv
    ```

---

## 4. Query with DuckDB

```python
import duckdb
from earthcatalog.core.catalog import open_catalog, get_or_create_table

catalog = open_catalog(db_path="/tmp/catalog.db", warehouse_path="/tmp/warehouse")
table   = get_or_create_table(catalog)

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute(f"""
    SELECT id, platform, grid_partition, datetime
    FROM iceberg_scan('{table.metadata_location}')
    WHERE platform = 'sentinel-2'
    LIMIT 20
""").df()

print(df)
```

---

## 5. Inspect the warehouse layout

```bash
find /tmp/warehouse -name "*.parquet" | head -10
# grid_partition=820957fffffffff/year=2021/part_000000_abc.parquet
# grid_partition=820957fffffffff/year=2022/part_000000_def.parquet
# ...
```

Each file covers exactly one `(H3 cell, year)` bucket — matching the
Iceberg `IdentityTransform(grid_partition)` + `YearTransform(datetime)` partition spec.

---

## Next steps

- [Architecture](../architecture.md) — understand how the pipeline works
- [Configuration](../configuration.md) — full YAML reference
- [Backfill pipeline](../pipelines/backfill.md) — process the full historical catalog with Dask
- [Operations: Ingest Guide](../operations/ingest_guide.md) — production runbook
