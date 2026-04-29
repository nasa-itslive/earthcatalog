#!/usr/bin/env python3
"""
Migrate all legacy GeoParquet files in a local warehouse to the current
rustac schema and zstd(3) compression, in place, using Dask for parallelism.

Usage
-----
    python scripts/migrate_schema.py --warehouse /path/to/warehouse

Each ``grid_partition=*/year=*/part_*.parquet`` file is rewritten in place:
the migrated file is first written to a ``.migrating`` sibling, then
atomically renamed over the original.

Two cases are handled:

1. **Legacy schema** (has ``raw_stac`` column): full migration — NDJSON
   roundtrip through rustac + _normalize_for_iceberg + zstd compression.
2. **New schema, wrong compression** (no ``raw_stac``, compression ≠ ZSTD):
   recompress only — read, write_table with zstd, preserve all metadata.

Files that are already on the new schema *and* zstd are skipped.

Progress is printed to stdout; errors are printed to stderr and processing
continues.  Exit code is non-zero if any errors occurred.

Options
-------
--warehouse     Local path to the warehouse root (required).
--workers       Number of Dask workers (default: CPU count).
--dry-run       Print which files would be migrated, without writing anything.
"""

import argparse
import os
import sys
import traceback
from pathlib import Path


def _needs_dict_fix(parquet_path: str) -> bool:
    """Return True if any column is dictionary-encoded (Iceberg can't read partition values from them)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pq.read_schema(parquet_path)
    return any(pa.types.is_dictionary(schema.field(n).type) for n in schema.names)


def _needs_recompress(parquet_path: str) -> bool:
    """Return True if any column in the first row-group is not ZSTD."""
    import pyarrow.parquet as pq

    meta = pq.read_metadata(parquet_path)
    if meta.num_row_groups == 0:
        return False
    rg = meta.row_group(0)
    return any(rg.column(i).compression != "ZSTD" for i in range(rg.num_columns))


def _needs_bbox_fix(parquet_path: str) -> bool:
    """Return True if the bbox column contains JSON objects ({"xmin":…}) instead of arrays."""
    import json

    import pyarrow.parquet as pq

    schema = pq.read_schema(parquet_path)
    if "bbox" not in schema.names:
        return False
    pf = pq.ParquetFile(parquet_path)
    # Read only bbox column, first row group
    col = pf.read_row_group(0, columns=["bbox"]).column("bbox")
    for v in col:
        raw = v.as_py()
        if raw is None:
            continue
        try:
            parsed = json.loads(raw)
            return isinstance(parsed, dict)
        except (json.JSONDecodeError, TypeError):
            return False
    return False


def _migrate_one(parquet_path: str) -> dict:
    """
    Migrate or recompress a single parquet file in place.

    Returns a dict with keys: path, status ("skipped"|"migrated"|"recompressed"|"error"),
    rows (int), and optionally error (str).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from earthcatalog.core.fix_schema import fix_schema

    try:
        pf = pq.ParquetFile(parquet_path)
        schema_names = pf.schema_arrow.names
    except Exception as exc:
        return {"path": parquet_path, "status": "error", "rows": 0, "error": f"read schema: {exc}"}

    tmp_path = parquet_path + ".migrating"

    # Case 1: legacy schema — full migration
    if "raw_stac" in schema_names:
        try:
            rows = fix_schema(parquet_path, tmp_path)
            os.replace(tmp_path, parquet_path)
            return {"path": parquet_path, "status": "migrated", "rows": rows}
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            return {
                "path": parquet_path,
                "status": "error",
                "rows": 0,
                "error": traceback.format_exc(),
            }

    # Case 2: new schema but wrong compression — recompress only
    if _needs_recompress(parquet_path):
        try:
            file_meta = pf.metadata.metadata
            table = pf.read()
            preserve_keys = (b"geo", b"stac-geoparquet")
            extra = {k: v for k, v in file_meta.items() if k in preserve_keys}
            if extra:
                table = table.replace_schema_metadata({**(table.schema.metadata or {}), **extra})
            with open(tmp_path, "wb") as fh:
                pq.write_table(table, fh, compression="zstd")
            rows = table.num_rows
            os.replace(tmp_path, parquet_path)
            return {"path": parquet_path, "status": "recompressed", "rows": rows}
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            return {
                "path": parquet_path,
                "status": "error",
                "rows": 0,
                "error": traceback.format_exc(),
            }

    # Case 3: bbox stored as JSON object string instead of JSON array — fix in place
    if _needs_bbox_fix(parquet_path):
        try:
            import json

            import pyarrow as pa

            file_meta = pf.metadata.metadata
            table = pf.read()
            bbox_col = table.column("bbox")
            fixed = []
            for i in range(len(bbox_col)):
                raw = bbox_col[i].as_py()
                if raw is None:
                    fixed.append(None)
                else:
                    v = json.loads(raw)
                    fixed.append(json.dumps([v["xmin"], v["ymin"], v["xmax"], v["ymax"]]))
            fixed_bbox = pa.array(fixed, type=pa.string())
            idx = table.schema.get_field_index("bbox")
            table = table.set_column(idx, table.schema.field("bbox"), fixed_bbox)
            preserve_keys = (b"geo", b"stac-geoparquet")
            extra = {k: v for k, v in file_meta.items() if k in preserve_keys}
            if extra:
                table = table.replace_schema_metadata({**(table.schema.metadata or {}), **extra})
            with open(tmp_path, "wb") as fh:
                pq.write_table(table, fh, compression="zstd")
            rows = table.num_rows
            os.replace(tmp_path, parquet_path)
            return {"path": parquet_path, "status": "bbox_fixed", "rows": rows}
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            return {
                "path": parquet_path,
                "status": "error",
                "rows": 0,
                "error": traceback.format_exc(),
            }

    # Already on new schema + zstd + correct bbox
    # Case 4: dictionary-encoded columns or missing grid_partition — fix in place
    if _needs_dict_fix(parquet_path) or "grid_partition" not in pq.read_schema(parquet_path).names:
        try:
            import re

            import pyarrow as pa

            file_meta = pf.metadata.metadata
            table = pf.read()
            new_cols = {}
            new_fields = []
            for name in table.schema.names:
                col = table.column(name)
                field = table.schema.field(name)
                if pa.types.is_dictionary(col.type):
                    col = col.cast(pa.string())
                    field = pa.field(name, pa.string(), metadata=field.metadata)
                new_cols[name] = col
                new_fields.append(field)
            # Inject grid_partition from Hive path if missing
            if "grid_partition" not in new_cols:
                m = re.search(r"grid_partition=([^/]+)", parquet_path)
                if m:
                    cell = m.group(1)
                    new_cols["grid_partition"] = pa.array([cell] * table.num_rows, type=pa.string())
                    new_fields.append(pa.field("grid_partition", pa.string()))
            schema = pa.schema(new_fields, metadata=table.schema.metadata)
            table = pa.table(new_cols, schema=schema)
            preserve_keys = (b"geo", b"stac-geoparquet")
            extra = {k: v for k, v in file_meta.items() if k in preserve_keys}
            if extra:
                table = table.replace_schema_metadata({**(table.schema.metadata or {}), **extra})
            with open(tmp_path, "wb") as fh:
                pq.write_table(table, fh, compression="zstd")
            rows = table.num_rows
            os.replace(tmp_path, parquet_path)
            return {"path": parquet_path, "status": "dict_fixed", "rows": rows}
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            return {
                "path": parquet_path,
                "status": "error",
                "rows": 0,
                "error": traceback.format_exc(),
            }

    # Already on new schema + zstd + correct bbox + plain string columns + grid_partition
    return {"path": parquet_path, "status": "skipped", "rows": 0}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate legacy earthcatalog warehouse to rustac schema + zstd (in place)."
    )
    parser.add_argument(
        "--warehouse",
        required=True,
        help="Local path to the warehouse root (contains grid_partition=*/year=*/*.parquet).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 4,
        help="Number of Dask local workers (default: CPU count).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print files that would be migrated without writing anything.",
    )
    args = parser.parse_args()

    warehouse = Path(args.warehouse)
    if not warehouse.is_dir():
        print(f"ERROR: warehouse path does not exist: {warehouse}", file=sys.stderr)
        sys.exit(1)

    parquet_files = sorted(warehouse.rglob("*.parquet"))
    if not parquet_files:
        print("No parquet files found.")
        return

    print(f"Found {len(parquet_files)} parquet files under {warehouse}")

    if args.dry_run:
        import pyarrow.parquet as pq

        legacy = [p for p in parquet_files if "raw_stac" in pq.read_schema(str(p)).names]
        recompress = [p for p in parquet_files if p not in legacy and _needs_recompress(str(p))]
        bbox_fix = [
            p
            for p in parquet_files
            if p not in legacy and p not in recompress and _needs_bbox_fix(str(p))
        ]
        dict_fix = [
            p
            for p in parquet_files
            if p not in legacy
            and p not in recompress
            and p not in bbox_fix
            and _needs_dict_fix(str(p))
        ]
        print(f"  {len(legacy)} files need full migration (raw_stac → rustac schema)")
        for p in legacy:
            print(f"    migrate     {p}")
        print(f"  {len(recompress)} files need recompression (snappy → zstd)")
        for p in recompress:
            print(f"    recompress  {p}")
        print(f"  {len(bbox_fix)} files need bbox fix (JSON object → JSON array)")
        for p in bbox_fix:
            print(f"    bbox_fix    {p}")
        print(f"  {len(dict_fix)} files need dict fix (dictionary → plain string)")
        for p in dict_fix:
            print(f"    dict_fix    {p}")
        return

    from dask.distributed import Client, LocalCluster, as_completed

    cluster = LocalCluster(n_workers=args.workers, threads_per_worker=1)
    client = Client(cluster)
    print(f"Dask dashboard: {client.dashboard_link}")

    paths = [str(p) for p in parquet_files]
    futures = [client.submit(_migrate_one, p) for p in paths]

    n_migrated = n_recompressed = n_bbox_fixed = n_dict_fixed = n_skipped = n_error = 0
    total_rows = 0

    for future in as_completed(futures):
        result = future.result()
        status = result["status"]
        path = result["path"]
        rows = result["rows"]

        if status == "migrated":
            n_migrated += 1
            total_rows += rows
            print(f"  migrated    {rows:>7} rows  {path}")
        elif status == "recompressed":
            n_recompressed += 1
            total_rows += rows
            print(f"  recompressed {rows:>6} rows  {path}")
        elif status == "bbox_fixed":
            n_bbox_fixed += 1
            total_rows += rows
            print(f"  bbox_fixed  {rows:>7} rows  {path}")
        elif status == "dict_fixed":
            n_dict_fixed += 1
            total_rows += rows
            print(f"  dict_fixed  {rows:>7} rows  {path}")
        elif status == "skipped":
            n_skipped += 1
        else:
            n_error += 1
            print(f"  ERROR       {path}")
            print(f"              {result.get('error', '')}", file=sys.stderr)

    client.close()
    cluster.close()

    print()
    print(
        f"Done. migrated={n_migrated}  recompressed={n_recompressed}  "
        f"bbox_fixed={n_bbox_fixed}  dict_fixed={n_dict_fixed}  "
        f"skipped={n_skipped}  errors={n_error}  rows={total_rows:,}"
    )
    if n_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
