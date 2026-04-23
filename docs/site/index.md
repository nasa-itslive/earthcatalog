# earthcatalog

**Spatially-partitioned STAC ingest pipeline backed by Apache Iceberg.**

earthcatalog ingests STAC item catalogs from AWS S3 into a spatially-partitioned
[Apache Iceberg](https://iceberg.apache.org/) table.  The resulting catalog can be
queried with [DuckDB](https://duckdb.org/) or any Iceberg-compatible engine using
efficient spatial and temporal predicate pushdown.

---

## What it does

```
S3 Inventory (CSV / Parquet)
        │
        ▼  fetch STAC JSON
fan_out(items, partitioner)  ──→  one row per (item × H3 cell)
        │
        ▼  group_by_partition
write_geoparquet()           ──→  one .parquet per (cell, year)
        │
        ▼
table.add_files()            ──→  PyIceberg SQLite catalog
        │
        ▼
DuckDB / PyIceberg scan
```

- **[Quick Start](quickstart.md)** — install and run in five minutes
- **[Architecture](architecture.md)** — deep dive into the pipeline design
- **[Configuration](configuration.md)** — YAML config reference
- **[Pipelines](pipelines/incremental.md)** — incremental and backfill pipelines
- **[Maintenance](maintenance/compact.md)** — warehouse compaction
- **[API Reference](api/index.md)** — Python API docs
- **[Operations: Ingest Guide](operations/ingest_guide.md)** — operator runbook

---

## Key features

| Feature | Detail |
|---|---|
| **obstore** for all S3 I/O | No credentials needed for public buckets (`skip_signature=True`) |
| **H3 spatial partitioning** | Resolution-1 hex grid (842 global cells); boundary-inclusive fan-out |
| **rustac GeoParquet** | `geo` metadata and `geoarrow.wkb` extension handled automatically |
| **PyIceberg + SQLite** | Zero-infra catalog — no Glue, no REST server |
| **Iceberg partition pruning** | `IdentityTransform(grid_partition)` + `YearTransform(datetime)` |
| **S3 atomic lock** | `If-None-Match: *` conditional write — no DynamoDB required |
| **Dask backfill** | Three-level DAG; Parquet data never crosses the head node |
| **Incremental delta ingest** | `--since <date>` filters inventory by `last_modified_date` |

---

## Project status

> **Pre-release (v0.1.0).**  The schema, partition spec, and CLI flags are
> stable for the ITS_LIVE NISAR velocity-pair catalog.  The PyPI package and
> hosted docs will be published once the initial full backfill is validated.
