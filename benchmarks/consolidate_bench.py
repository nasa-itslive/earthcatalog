#!/usr/bin/env python3
"""Real-store benchmark: legacy consolidation vs the improved implementation.

Builds a production-shaped scratch warehouse (many small files spread over
many (tile, year) partitions), then runs the frozen baseline
(:mod:`benchmarks.legacy_consolidate`, verbatim ``consolidate.py`` at
c242d18) and the current implementation over **identical** copies, times
each, and verifies they leave the same data behind.

The default backend is real S3 (the point is to measure real round trips on
a GitHub-hosted runner); ``--store local`` runs the same harness against a
LocalStore for quick local smoke tests.  The scratch prefix is disposable:
it is deleted at the end unless ``--keep`` is passed.  Nothing outside the
scratch prefix is ever read or written.

Example (CI)::

    uv run python benchmarks/consolidate_bench.py \
        --store s3 --bucket its-live-data \
        --prefix test-space/stac/bench/consolidate \
        --partitions 120 --files-per-partition 10 --rows-per-file 50 \
        --summary-file "$GITHUB_STEP_SUMMARY" --json-out bench.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import io
import json
import pathlib
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from obstore.store import LocalStore

from earthcatalog.catalog import _open_sqlite, get_or_create
from earthcatalog.config import GridConfig

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _load_legacy():
    """Import the frozen baseline module by path (benchmarks/ is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "legacy_consolidate", _HERE / "legacy_consolidate.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["legacy_consolidate"] = module
    spec.loader.exec_module(module)
    return module


legacy = _load_legacy()
import earthcatalog.consolidate as improved  # noqa: E402

# ---------------------------------------------------------------------------
# scratch-warehouse construction
# ---------------------------------------------------------------------------


def _shape(n_partitions: int):
    """Deterministic (tile, year) pairs — enough variety to rank partitions."""
    for i in range(n_partitions):
        yield f"bench{i:06d}", 2000 + (i % 25)


def _rows(partition_i: int, file_j: int, n_rows: int, cell: str, year: int):
    ts = dt.datetime(year, 6, 15, tzinfo=dt.UTC)
    base = partition_i * 1_000_000 + file_j * 10_000
    return pa.table(
        {
            "id": [f"bench-{base + r}" for r in range(n_rows)],
            "grid_partition": [cell] * n_rows,
            "geometry": [b""] * n_rows,
            "datetime": [ts] * n_rows,
            "platform": ["benchmark"] * n_rows,
            "percent_valid_pixels": list(range(n_rows)),
            "start_datetime": [ts] * n_rows,
            "end_datetime": [ts] * n_rows,
        }
    )


def _parquet_bytes(tbl: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _put_many(store, items: list[tuple[str, bytes]], workers: int = 32) -> None:
    def _put(item):
        key, data = item
        obstore.put(store, key, data)

    with ThreadPoolExecutor(max_workers=min(workers, max(1, len(items)))) as pool:
        list(pool.map(_put, items))


def _partition_keys(prefix: str, args) -> list[str]:
    keys: list[str] = []
    for i, (cell, year) in enumerate(_shape(args.partitions)):
        for j in range(args.files_per_partition):
            keys.append(f"{prefix}/grid=h3/level=1/tile={cell}/year={year}/part_{j:06d}.parquet")
    return keys


def _build(
    store,
    prefix: str,
    args,
    *,
    uri_fn,
    db_path: str,
    write: bool,
):
    """Create a scratch table at *prefix*, optionally writing its files."""
    cat = _open_sqlite(db_path=db_path, warehouse_path=uri_fn(prefix))
    table = get_or_create(cat, grid_config=GridConfig(type="h3", resolution=1))

    keys = _partition_keys(prefix, args)
    if write:
        payloads = []
        idx = 0
        for i, (cell, year) in enumerate(_shape(args.partitions)):
            for j in range(args.files_per_partition):
                payloads.append(
                    (keys[idx], _parquet_bytes(_rows(i, j, args.rows_per_file, cell, year)))
                )
                idx += 1
        _put_many(store, payloads)

    uris = [uri_fn(k) for k in keys]
    for start in range(0, len(uris), 200):
        table.add_files(uris[start : start + 200])
    return table


def _clone(store, src_prefix: str, dst_prefix: str, workers: int = 32) -> None:
    src_keys = [
        obj["path"]
        for batch in obstore.list(store, prefix=src_prefix)
        for obj in batch
        if obj["path"].endswith(".parquet")
    ]

    def _copy(key: str) -> None:
        dst = dst_prefix + key[len(src_prefix) :]
        obstore.copy(store, key, dst)

    with ThreadPoolExecutor(max_workers=min(workers, max(1, len(src_keys)))) as pool:
        list(pool.map(_copy, src_keys))


def _count_parquet(store, prefix: str) -> int:
    return sum(
        1
        for batch in obstore.list(store, prefix=prefix)
        for obj in batch
        if obj["path"].endswith(".parquet")
    )


def _delete_prefix(store, prefix: str, workers: int = 32) -> None:
    keys = [obj["path"] for batch in obstore.list(store, prefix=prefix) for obj in batch]
    if not keys:
        return

    def _del(key: str) -> None:
        try:
            obstore.delete(store, key)
        except FileNotFoundError:
            pass

    with ThreadPoolExecutor(max_workers=min(workers, len(keys))) as pool:
        list(pool.map(_del, keys))


# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------


def _time_legacy(store, table, prefix, args, flush) -> tuple[float, list[dict]]:
    t0 = time.perf_counter()
    reports = legacy.run(
        store,
        table,
        prefix,
        min_files=args.min_files,
        limit_tiles=args.limit_tiles,
    )
    # The current CLI publishes the catalog once, after the run.
    flush()
    return time.perf_counter() - t0, reports


def _time_improved(store, table, prefix, args, flush) -> tuple[float, list[dict]]:
    t0 = time.perf_counter()
    reports = improved.run(
        store,
        table,
        prefix,
        min_files=args.min_files,
        limit_tiles=args.limit_tiles,
        flush_every=args.flush_every or None,
        on_flush=flush,
        fetch_workers=args.fetch_workers,
    )
    return time.perf_counter() - t0, reports


def _verify(table_a, table_b) -> dict:
    count_a = table_a.scan().count()
    count_b = table_b.scan().count()
    ids_a = sorted(table_a.scan().to_arrow().column("id").to_pylist())
    ids_b = sorted(table_b.scan().to_arrow().column("id").to_pylist())
    return {
        "rows_legacy": count_a,
        "rows_improved": count_b,
        "rows_equal": count_a == count_b,
        "ids_equal": ids_a == ids_b,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _make_store(args):
    if args.store == "s3":
        from earthcatalog.stores import make_s3_store

        store = make_s3_store(args.bucket, region=args.region)
        return store, f"s3://{args.bucket}"
    root = pathlib.Path(args.local_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return LocalStore(str(root)), str(root)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--store", choices=["s3", "local"], default="s3")
    p.add_argument("--bucket", default="its-live-data")
    p.add_argument("--region", default="us-west-2")
    p.add_argument("--prefix", default="test-space/stac/bench/consolidate")
    p.add_argument("--local-root", default="/tmp/earthcatalog_bench")
    p.add_argument("--partitions", type=int, default=120)
    p.add_argument("--files-per-partition", type=int, default=10)
    p.add_argument("--rows-per-file", type=int, default=50)
    p.add_argument("--min-files", type=int, default=4)
    p.add_argument("--limit-tiles", type=int, default=None)
    p.add_argument("--flush-every", type=int, default=25)
    p.add_argument("--fetch-workers", type=int, default=8)
    p.add_argument("--keep", action="store_true", help="Keep the scratch prefix.")
    p.add_argument("--json-out", default=None)
    p.add_argument("--summary-file", default=None)
    args = p.parse_args()

    if args.store == "s3":
        if "catalog/warehouse" in args.prefix or args.prefix.endswith("earthcatalog.db"):
            p.error("refusing to run against a production path")
        if "bench" not in args.prefix and "scratch" not in args.prefix:
            p.error("scratch --prefix must contain 'bench' or 'scratch'")

    store, uri_root = _make_store(args)

    def uri_fn(prefix: str) -> str:
        if args.store == "s3":
            return f"{uri_root}/{prefix}"
        return str(pathlib.Path(uri_root) / prefix)

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    base = f"{args.prefix}/{run_id}" if args.store == "s3" else run_id
    prefix_a = f"{base}/legacy/warehouse"
    prefix_b = f"{base}/improved/warehouse"
    catalog_a = f"{base}/legacy/earthcatalog.db"
    catalog_b = f"{base}/improved/earthcatalog.db"
    tmp_dir = tempfile.mkdtemp(prefix="earthcatalog-bench-")

    total_files = args.partitions * args.files_per_partition
    print(
        f"[bench] store={args.store} shape={args.partitions} partitions x "
        f"{args.files_per_partition} files = {total_files} files, "
        f"{args.rows_per_file} rows/file, limit_tiles={args.limit_tiles}",
        flush=True,
    )

    try:
        t0 = time.perf_counter()
        print("[bench] building scratch warehouse A (legacy copy)...", flush=True)
        table_a = _build(
            store,
            prefix_a,
            args,
            uri_fn=uri_fn,
            db_path=f"{tmp_dir}/a.db",
            write=True,
        )
        print(f"[bench] A ready in {time.perf_counter() - t0:.1f}s; cloning to B...", flush=True)
        _clone(store, prefix_a, prefix_b)
        table_b = _build(
            store,
            prefix_b,
            args,
            uri_fn=uri_fn,
            db_path=f"{tmp_dir}/b.db",
            write=False,
        )
        print(f"[bench] scratch warehouse ready in {time.perf_counter() - t0:.1f}s", flush=True)

        planned = improved.plan(table_a, min_files=args.min_files, limit_tiles=args.limit_tiles)
        print(
            f"[bench] consolidating {len(planned)} partition(s) per side (of {args.partitions})",
            flush=True,
        )

        def flush_a() -> None:
            obstore.put(store, catalog_a, pathlib.Path(f"{tmp_dir}/a.db").read_bytes())

        def flush_b() -> None:
            obstore.put(store, catalog_b, pathlib.Path(f"{tmp_dir}/b.db").read_bytes())

        print("[bench] running legacy (baseline)...", flush=True)
        t_legacy, reports_legacy = _time_legacy(store, table_a, prefix_a, args, flush_a)
        print(
            f"[bench] legacy   : {t_legacy:8.1f}s  ({len(reports_legacy)} partitions)",
            flush=True,
        )

        print("[bench] running improved...", flush=True)
        t_improved, reports_improved = _time_improved(store, table_b, prefix_b, args, flush_b)
        print(
            f"[bench] improved : {t_improved:8.1f}s  ({len(reports_improved)} partitions)",
            flush=True,
        )

        check = _verify(table_a, table_b)
        files_a = _count_parquet(store, prefix_a)
        files_b = _count_parquet(store, prefix_b)
        speedup = (t_legacy / t_improved) if t_improved > 0 else float("inf")
        result = {
            "store": args.store,
            "partitions": args.partitions,
            "files_per_partition": args.files_per_partition,
            "rows_per_file": args.rows_per_file,
            "min_files": args.min_files,
            "limit_tiles": args.limit_tiles,
            "flush_every": args.flush_every,
            "fetch_workers": args.fetch_workers,
            "seconds_legacy": round(t_legacy, 2),
            "seconds_improved": round(t_improved, 2),
            "speedup": round(speedup, 2),
            "files_legacy_after": files_a,
            "files_improved_after": files_b,
            **check,
        }
        print("[bench] " + json.dumps(result, indent=2), flush=True)
        if not (check["rows_equal"] and check["ids_equal"] and files_a == files_b):
            print("[bench] VERIFICATION FAILED", file=sys.stderr)
            return 1

        if args.json_out:
            pathlib.Path(args.json_out).write_text(json.dumps(result, indent=2))
        if args.summary_file:
            lines = [
                "### Consolidation benchmark (real store)",
                "",
                "| metric | legacy | improved |",
                "| --- | ---: | ---: |",
                f"| wall clock | {t_legacy:.1f}s | {t_improved:.1f}s |",
                f"| files after | {files_a} | {files_b} |",
                f"| partitions | {len(reports_legacy)} | {len(reports_improved)} |",
                "",
                f"**Speedup: {speedup:.2f}x** — shape {args.partitions} partitions x "
                f"{args.files_per_partition} files ({total_files} files), "
                f"{args.rows_per_file} rows/file; store `{args.store}`; "
                f"rows equal: `{check['rows_equal']}`, ids equal: `{check['ids_equal']}`.",
                "",
            ]
            pathlib.Path(args.summary_file).write_text("\n".join(lines))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not args.keep:
            print(f"[bench] cleaning {base}", flush=True)
            _delete_prefix(store, base)
            if args.store == "local":
                shutil.rmtree(pathlib.Path(uri_root) / base, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
