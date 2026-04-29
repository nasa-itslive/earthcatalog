"""
Migrate a legacy earthcatalog GeoParquet file to the current rustac schema.

Legacy files contain a ``raw_stac`` column with the full STAC item serialised
as a JSON string.  This module streams those JSON strings to a temporary
NDJSON file, then lets rustac read and rewrite it — producing a proper
stac-geoparquet file with native assets/links columns and geo metadata.

The output is post-processed by ``_normalize_for_iceberg`` (same step used by
``write_geoparquet``) so assets/links are JSON strings and null-typed columns
are dropped.  The final file is written with zstd compression via
``pq.write_table``.

``fix_schema(input_path, output_path)``
    Migrate one file.  The output is written to *output_path*; the input is
    never modified.
"""

import tempfile
from pathlib import Path


def fix_schema(input_path: str, output_path: str) -> int:
    """
    Convert a legacy GeoParquet file to the current rustac schema.

    Streams ``raw_stac`` rows to a temporary NDJSON file, calls
    ``rustac.read`` + ``rustac.write`` to produce a proper stac-geoparquet
    file, then post-processes with ``_normalize_for_iceberg`` (assets/links →
    JSON strings, null columns dropped) and writes with zstd(3) compression.

    Parameters
    ----------
    input_path:
        Path to a legacy ``.parquet`` file containing a ``raw_stac`` column.
    output_path:
        Destination path for the rewritten file.

    Returns
    -------
    int
        Number of rows written.

    Raises
    ------
    ValueError
        If *input_path* does not contain a ``raw_stac`` column.
    """
    import asyncio

    import pyarrow.parquet as pq
    import rustac

    from .transform import _normalize_for_iceberg

    pf = pq.ParquetFile(input_path)

    if "raw_stac" not in pf.schema_arrow.names:
        raise ValueError(
            f"Legacy file is missing 'raw_stac' column. Found: {pf.schema_arrow.names}"
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as tmp:
        tmp_path = tmp.name
        n = 0
        for batch in pf.iter_batches(columns=["raw_stac"]):
            for val in batch.column("raw_stac"):
                tmp.write(val.as_py())
                tmp.write("\n")
                n += 1

    # Write to an intermediate temp parquet so rustac doesn't clobber output_path
    # on error, and so we can post-process before writing the final file.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as raw_tmp:
        raw_tmp_path = raw_tmp.name

    try:

        async def _roundtrip():
            items = await rustac.read(tmp_path)
            await rustac.write(raw_tmp_path, items)

        asyncio.run(_roundtrip())

        # Post-process: assets/links → JSON strings, drop null-typed columns,
        # cast whole-number floats → int32.  Preserve file-level geo metadata.
        pf2 = pq.ParquetFile(raw_tmp_path)
        file_meta = pf2.metadata.metadata
        table = _normalize_for_iceberg(pf2.read())
        preserve_keys = (b"geo", b"stac-geoparquet")
        extra = {k: v for k, v in file_meta.items() if k in preserve_keys}
        if extra:
            table = table.replace_schema_metadata({**(table.schema.metadata or {}), **extra})

        with open(output_path, "wb") as fh:
            pq.write_table(table, fh, compression="zstd")

    finally:
        Path(tmp_path).unlink(missing_ok=True)
        Path(raw_tmp_path).unlink(missing_ok=True)

    return n
