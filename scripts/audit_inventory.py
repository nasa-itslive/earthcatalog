#!/usr/bin/env python3
"""
Audit and diff STAC item IDs across three sources:
  1. Inventory chunks  (s3://its-live-data/.../ingest/chunks/)
  2. pgSTAC export      (/tmp/pgstac_ids_with_href.csv.gz)
  3. Iceberg catalog    (s3://its-live-data/.../warehouse/)

Strategy: extract each source to a local parquet, then use DuckDB
for the merge — no giant Python sets needed.

pgSTAC export now includes S3 href, so pgstac-only items can be
included in the delta inventory for re-ingestion.

Produces:
  /tmp/pgstac_ids.parquet        — pgstac (id, bucket, key) cached from CSV
  /tmp/audit.parquet             — all unique IDs with bool flags
  /tmp/delta_inventory.parquet   — (bucket, key) rows for items to ingest

Usage
-----
    python scripts/audit_inventory.py
    python scripts/audit_inventory.py --pgstac /tmp/pgstac_ids_with_href.csv.gz
"""

import asyncio
import io
from pathlib import Path

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from obstore.store import S3Store

CHUNK_PREFIX = "test-space/stac/catalog/ingest/chunks"
WAREHOUSE_PREFIX = "test-space/stac/catalog/warehouse"
BUCKET = "its-live-data"
REGION = "us-west-2"

DEFAULT_PGSTAC = "/tmp/pgstac_ids_with_href.csv.gz"
INV_IDS_OUT = "/tmp/inventory_ids.parquet"
WH_IDS_OUT = "/tmp/warehouse_ids.parquet"
PG_IDS_OUT = "/tmp/pgstac_ids.parquet"
AUDIT_OUT = "/tmp/audit.parquet"
DELTA_OUT = "/tmp/delta_inventory.parquet"


def _make_store(prefix: str) -> S3Store:
    return S3Store(bucket=BUCKET, region=REGION, prefix=prefix, skip_signature=True)


def _list_parquets(store: S3Store) -> list[str]:
    return sorted(
        obj["path"]
        for batch in obstore.list(store, prefix="")
        for obj in batch
        if obj["path"].endswith(".parquet")
    )


# ---------------------------------------------------------------------------
# Step 1: inventory chunks → /tmp/inventory_ids.parquet  (id, bucket, key)
# ---------------------------------------------------------------------------

def step1_inventory(store: S3Store) -> None:
    if Path(INV_IDS_OUT).exists():
        n = pq.ParquetFile(INV_IDS_OUT).metadata.num_rows
        print(f"  Using cached {INV_IDS_OUT} ({n:,} rows)")
        return

    keys = _list_parquets(store)
    ids: list[str] = []
    buckets: list[str] = []
    s3keys: list[str] = []

    for key in tqdm.tqdm(keys, desc="Inventory chunks", unit="chunk"):
        raw = memoryview(obstore.get(store, key).bytes()).tobytes()
        tbl = pq.ParquetFile(io.BytesIO(raw)).read()
        for b, k in zip(tbl.column("bucket").to_pylist(), tbl.column("key").to_pylist()):
            fname = k.rsplit("/", 1)[-1]
            ids.append(fname.removesuffix(".stac.json"))
            buckets.append(b)
            s3keys.append(k)

    tbl = pa.table({"id": ids, "bucket": buckets, "key": s3keys})
    pq.write_table(tbl, INV_IDS_OUT)
    print(f"  -> {INV_IDS_OUT}  ({len(tbl):,} rows)")


# ---------------------------------------------------------------------------
# Step 2: warehouse GeoParquet → /tmp/warehouse_ids.parquet  (id,)
# ---------------------------------------------------------------------------

async def _fetch_wh_ids_async(store: S3Store, key: str) -> list[str]:
    raw = memoryview((await obstore.get_async(store, key)).bytes()).tobytes()
    col = pq.ParquetFile(io.BytesIO(raw)).read(["id"]).column("id")
    return col.to_pylist()


async def _fetch_all_wh_async(store: S3Store, keys: list[str], concurrency: int = 64) -> list[str]:
    from asyncio import Semaphore, TaskGroup

    sem = Semaphore(concurrency)
    results: dict[int, list[str]] = {}
    pbar = tqdm.tqdm(total=len(keys), desc="Warehouse parquet", unit="file")

    async def _fetch(idx: int, key: str) -> None:
        async with sem:
            results[idx] = await _fetch_wh_ids_async(store, key)
            pbar.update(1)

    async with TaskGroup() as tg:
        for i, key in enumerate(keys):
            tg.create_task(_fetch(i, key))

    pbar.close()
    all_ids: list[str] = []
    for i in range(len(keys)):
        all_ids.extend(results[i])
    return all_ids


def _dedup_parquet(path: str) -> None:
    import duckdb

    con = duckdb.connect()
    n_before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    n_unique = con.execute(f"SELECT COUNT(DISTINCT id) FROM read_parquet('{path}')").fetchone()[0]
    if n_unique < n_before:
        print(f"  Deduping {path}: {n_before:,} -> {n_unique:,} ({n_before - n_unique:,} dupes)")
        tmp = path + ".dedup"
        con.execute(f"COPY (SELECT DISTINCT id FROM read_parquet('{path}')) TO '{tmp}' (FORMAT PARQUET)")
        con.close()
        Path(tmp).rename(path)
    else:
        print(f"  Already deduped: {n_unique:,} unique rows")
        con.close()


def step2_warehouse(store: S3Store) -> None:
    if Path(WH_IDS_OUT).exists():
        n = pq.ParquetFile(WH_IDS_OUT).metadata.num_rows
        print(f"  Using cached {WH_IDS_OUT} ({n:,} rows)")
        _dedup_parquet(WH_IDS_OUT)
        return


    keys = _list_parquets(store)
    print(f"  Fetching {len(keys):,} warehouse parquet files (concurrent) ...")
    all_ids = asyncio.run(_fetch_all_wh_async(store, keys, concurrency=64))
    tbl = pa.table({"id": all_ids})
    pq.write_table(tbl, WH_IDS_OUT)
    print(f"  -> {WH_IDS_OUT}  ({len(all_ids):,} rows)")


# ---------------------------------------------------------------------------
# Step 3: pgstac CSV → /tmp/pgstac_ids.parquet  (id, bucket, key)
# ---------------------------------------------------------------------------

def step3_pgstac(pgstac_path: str) -> None:
    if Path(PG_IDS_OUT).exists():
        n = pq.ParquetFile(PG_IDS_OUT).metadata.num_rows
        print(f"  Using cached {PG_IDS_OUT} ({n:,} rows)")
        return

    import duckdb

    compression = ", compression='gzip'" if pgstac_path.endswith(".gz") else ""
    print(f"  Converting {pgstac_path} -> {PG_IDS_OUT} ...")

    con = duckdb.connect()
    n = con.execute(
        f"SELECT COUNT(*) FROM read_csv('{pgstac_path}', header=false,"
        f" columns={{'id': 'varchar', 's3_href': 'varchar'}}{compression})"
    ).fetchone()[0]
    print(f"  Read {n:,} rows from pgstac CSV")

    con.execute(f"""
        COPY (
            SELECT
                id,
                split_part(s3_href, '/', 3) AS bucket,
                regexp_replace(s3_href, '^s3://[^/]+/', '') AS key
            FROM read_csv('{pgstac_path}', header=false,
                columns={{'id': 'varchar', 's3_href': 'varchar'}}{compression})
        ) TO '{PG_IDS_OUT}' (FORMAT PARQUET)
    """)
    con.close()

    n = pq.ParquetFile(PG_IDS_OUT).metadata.num_rows
    print(f"  -> {PG_IDS_OUT}  ({n:,} rows)")


# ---------------------------------------------------------------------------
# Step 4: DuckDB merge
# ---------------------------------------------------------------------------

def step4_merge() -> None:
    import duckdb

    con = duckdb.connect()

    print()
    print("Counting ...")
    inv_n = con.execute(f"SELECT COUNT(*) FROM '{INV_IDS_OUT}'").fetchone()[0]
    pg_n = con.execute(f"SELECT COUNT(*) FROM '{PG_IDS_OUT}'").fetchone()[0]
    wh_n = con.execute(f"SELECT COUNT(*) FROM '{WH_IDS_OUT}'").fetchone()[0]
    print(f"  Inventory rows:   {inv_n:,}")
    print(f"  pgSTAC rows:      {pg_n:,}")
    print(f"  Warehouse rows:   {wh_n:,}")

    con.execute(f"CREATE VIEW inv AS SELECT DISTINCT id, bucket, key FROM read_parquet('{INV_IDS_OUT}')")
    con.execute(f"CREATE VIEW wh AS SELECT DISTINCT id FROM read_parquet('{WH_IDS_OUT}')")
    con.execute(f"CREATE VIEW pg AS SELECT DISTINCT id, bucket, key FROM read_parquet('{PG_IDS_OUT}')")

    print()
    print("=" * 64)
    print("Set comparison")
    print("=" * 64)

    inv_only = con.execute("SELECT COUNT(*) FROM inv WHERE id NOT IN (SELECT id FROM pg) AND id NOT IN (SELECT id FROM wh)").fetchone()[0]
    pg_only = con.execute("SELECT COUNT(*) FROM pg WHERE id NOT IN (SELECT id FROM inv) AND id NOT IN (SELECT id FROM wh)").fetchone()[0]
    wh_only = con.execute("SELECT COUNT(*) FROM wh WHERE id NOT IN (SELECT id FROM inv) AND id NOT IN (SELECT id FROM pg)").fetchone()[0]
    inv_pg = con.execute("SELECT COUNT(*) FROM inv i WHERE i.id IN (SELECT id FROM pg) AND i.id NOT IN (SELECT id FROM wh)").fetchone()[0]
    inv_wh = con.execute("SELECT COUNT(*) FROM inv i WHERE i.id NOT IN (SELECT id FROM pg) AND i.id IN (SELECT id FROM wh)").fetchone()[0]
    pg_wh = con.execute("SELECT COUNT(*) FROM pg p WHERE p.id NOT IN (SELECT id FROM inv) AND p.id IN (SELECT id FROM wh)").fetchone()[0]
    all_three = con.execute("SELECT COUNT(*) FROM inv i WHERE i.id IN (SELECT id FROM pg) AND i.id IN (SELECT id FROM wh)").fetchone()[0]

    print(f"  Inventory only:          {inv_only:,}")
    print(f"  pgSTAC only:             {pg_only:,}")
    print(f"  Warehouse only:          {wh_only:,}")
    print(f"  Inventory + pgSTAC:      {inv_pg:,}")
    print(f"  Inventory + warehouse:   {inv_wh:,}")
    print(f"  pgSTAC + warehouse:      {pg_wh:,}")
    print(f"  All three:               {all_three:,}")

    delta_inv = con.execute(
        "SELECT COUNT(*) FROM inv WHERE id NOT IN (SELECT id FROM wh)"
    ).fetchone()[0]
    delta_pg = con.execute(
        "SELECT COUNT(*) FROM pg WHERE id NOT IN (SELECT id FROM inv) AND id NOT IN (SELECT id FROM wh)"
    ).fetchone()[0]
    total_delta = delta_inv + delta_pg
    print()
    print(f"  Delta from inventory (not in warehouse):  {delta_inv:,}")
    print(f"  Delta from pgSTAC only (not in inv/wh):    {delta_pg:,}")
    print(f"  Total items to ingest:                     {total_delta:,}")

    print()
    print("Writing audit parquet ...")
    con.execute(f"""
        COPY (
            WITH all_ids AS (
                SELECT DISTINCT id FROM inv
                UNION
                SELECT DISTINCT id FROM pg
                UNION
                SELECT DISTINCT id FROM wh
            )
            SELECT
                a.id,
                a.id IN (SELECT id FROM inv) AS inventoried,
                a.id IN (SELECT id FROM pg)  AS pgstac_ingested,
                a.id IN (SELECT id FROM wh)  AS parquet_ingested
            FROM all_ids a
            ORDER BY a.id
        ) TO '{AUDIT_OUT}' (FORMAT PARQUET)
    """)
    audit_n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{AUDIT_OUT}')").fetchone()[0]
    print(f"  -> {AUDIT_OUT}  ({audit_n:,} rows)")

    print("Writing delta inventory parquet ...")
    con.execute(f"""
        COPY (
            SELECT bucket, key
            FROM inv
            WHERE id NOT IN (SELECT id FROM wh)
            UNION ALL
            SELECT
                bucket,
                CASE
                    WHEN key LIKE '%.nc' THEN key || '.stac.json'
                    ELSE key
                END AS key
            FROM pg
            WHERE id NOT IN (SELECT id FROM inv) AND id NOT IN (SELECT id FROM wh)
            ORDER BY key
        ) TO '{DELTA_OUT}' (FORMAT PARQUET)
    """)
    delta_n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{DELTA_OUT}')").fetchone()[0]
    print(f"  -> {DELTA_OUT}  ({delta_n:,} rows)")

    print()
    print("Done")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Audit STAC item IDs across sources")
    parser.add_argument("--pgstac", default=DEFAULT_PGSTAC, help="Path to pgstac IDs+href CSV")
    args = parser.parse_args()

    chunk_store = _make_store(CHUNK_PREFIX)
    wh_store = _make_store(WAREHOUSE_PREFIX)

    print("=" * 64)
    print("Step 1/4: Extract inventory chunk IDs -> local parquet")
    print("=" * 64)
    step1_inventory(chunk_store)

    print()
    print("=" * 64)
    print("Step 2/4: Extract warehouse IDs -> local parquet (concurrent)")
    print("=" * 64)
    step2_warehouse(wh_store)

    print()
    print("=" * 64)
    print("Step 3/4: Convert pgstac CSV -> local parquet (id, bucket, key)")
    print("=" * 64)
    step3_pgstac(args.pgstac)

    print()
    print("=" * 64)
    print("Step 4/4: DuckDB merge + write outputs")
    print("=" * 64)
    step4_merge()


if __name__ == "__main__":
    main()
