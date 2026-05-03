"""
Search — Iceberg-pruned lazy search over rustac.search_sync.

Two classes:

* ``_FileSearchEngine`` — internal engine that prunes files via Iceberg
  and fans out ``rustac.search_sync`` per file.

* ``EarthCatalogItemSearch`` — public, pystac_client-compatible search
  result.  Holds kwargs and defers all I/O until iteration.
"""

from __future__ import annotations

import json
import sys
from contextlib import nullcontext as _nullcontext

_JSON_FIELDS = frozenset({"assets", "links", "bbox", "stac_extensions"})

# ---------------------------------------------------------------------------
# Rehydration — JSON-string fields → native types
# ---------------------------------------------------------------------------


def _rehydrate(item: dict) -> dict:
    """Parse JSON-string fields back to native Python types."""
    for key in _JSON_FIELDS & item.keys():
        val = item[key]
        if isinstance(val, str) and val:
            try:
                item[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    return item


# ---------------------------------------------------------------------------
# stderr filter — drops the noisy rustac "stac_extensions" message
# ---------------------------------------------------------------------------


class _StderrFilter:
    """Wraps ``sys.stderr``, filtering out lines that match *pattern*."""

    def __init__(self, pattern: str):
        self._pattern = pattern
        self._orig = sys.stderr
        self._buf = ""

    def write(self, text: str):
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if self._pattern not in line:
                self._orig.write(line + "\n")

    def flush(self):
        if self._buf and self._pattern not in self._buf:
            self._orig.write(self._buf)
        self._buf = ""
        self._orig.flush()

    @property
    def closed(self) -> bool:
        return self._orig.closed if hasattr(self._orig, "closed") else False

    def __getattr__(self, name):
        return getattr(self._orig, name)


def _suppress_stderr(pattern: str = "stac_extensions field is a string"):
    """Context manager that filters *pattern* from stderr output."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        old = sys.stderr
        sys.stderr = _StderrFilter(pattern)
        try:
            yield
        finally:
            sys.stderr.flush()
            sys.stderr = old

    return _ctx()


# ---------------------------------------------------------------------------
# Internal file-search engine
# ---------------------------------------------------------------------------


def _rustac_search_sync(href, **kwargs):
    import rustac

    return [_rehydrate(it) for it in rustac.search_sync(href, **kwargs)]


class _FileSearchEngine:
    """Fan-out ``rustac.search_sync`` across Iceberg-pruned files."""

    def __init__(self, prune_fn=None):
        self._prune_fn = prune_fn

    def prune(self, **kwargs) -> list[str]:
        if self._prune_fn is None:
            return []
        geom = _extract_geometry(**kwargs)
        if geom is None:
            return []
        start, end = _extract_datetime_range(**kwargs)
        return self._prune_fn(geom, start_datetime=start, end_datetime=end)

    def iter_items(self, files: list[str], **kwargs):
        """Yield lists of items from each file in *files*.

        Each yielded value is a ``list[dict]`` — one per file.
        """
        if not files:
            return

        max_items = kwargs.get("max_items")
        seen = 0

        with _suppress_stderr():
            for f in files:
                remaining = None
                if max_items is not None:
                    remaining = max_items - seen
                    if remaining <= 0:
                        break
                file_kwargs = {**kwargs, "max_items": remaining} if remaining is not None else kwargs
                items = _rustac_search_sync(f, **file_kwargs)
                if items:
                    yield items
                    seen += len(items)

    def search(self, **kwargs) -> list[dict]:
        """Collect all results into a single list (legacy path)."""
        files = self.prune(**kwargs)
        results: list[dict] = []
        max_items = kwargs.get("max_items")
        for batch in self.iter_items(files, **kwargs):
            results.extend(batch)
            if max_items is not None and len(results) >= max_items:
                results = results[:max_items]
                break
        return results

    def search_to_arrow(self, **kwargs):
        import pyarrow as pa

        items = self.search(**kwargs)
        if not items:
            return pa.table({})
        import rustac

        with _suppress_stderr():
            arro3_tbl = rustac.to_arrow(items)
        return pa.RecordBatchReader.from_stream(arro3_tbl).read_all()


# ---------------------------------------------------------------------------
# Public pystac_client-compatible search result
# ---------------------------------------------------------------------------


class EarthCatalogItemSearch:
    """Deferred search result, matching ``pystac_client.ItemSearch``.

    No I/O occurs until ``items()``, ``item_collection()``, or ``pages()``
    is called.

    Parameters
    ----------
    params:
        The raw search kwargs (intersects, bbox, datetime, filter, ...).
    engine:
        A ``_FileSearchEngine`` instance (or equivalent) providing
        ``prune()`` and ``iter_items()``.
    table:
        PyIceberg ``Table`` used by ``matched()`` to estimate result count.
    """

    def __init__(self, params: dict, engine: _FileSearchEngine, table=None, *, anonymous_ctx=None):
        self._params = dict(params)
        self._engine = engine
        self._table = table
        self._anonymous_ctx = anonymous_ctx or _nullcontext
        self._matched: int | None = None
        self._cached_files: list[str] | None = None
        self._cached_collection: list[dict] | None = None

    def get_parameters(self) -> dict:
        """Return the search parameters."""
        return dict(self._params)

    def __iter__(self):
        """Iterate over items as dicts (backward compat: ``list(ec.search(...))``)."""
        return self.items_as_dicts()

    def __repr__(self) -> str:
        parts = []
        for k in ("collections", "max_items", "bbox", "datetime", "intersects", "filter", "ids", "query"):
            v = self._params.get(k)
            if v is not None:
                parts.append(f"{k}={v!r}")
        s = self._manifest_stats()
        if s is not None and s["files"]:
            parts.append(f"files={s['files']}")
            parts.append(f"rows~{s['rows_upper_bound']:,}")
            parts.append(f"data={_format_bytes(s['bytes_upper_bound'])}")
        return f"EarthCatalogItemSearch({', '.join(parts)})"

    def _repr_html_(self) -> str:
        rows = ""
        for k in ("collections", "ids", "bbox", "intersects", "datetime", "filter", "query", "max_items", "sortby"):
            v = self._params.get(k)
            if v is not None:
                val = str(v)
                if len(val) > 80:
                    val = val[:77] + "..."
                rows += f"""
                    <tr><td style='padding:4px 8px;border:none;width:180px;font-weight:600'>{k}</td>
                    <td style='padding:4px 8px;border:none;font-family:monospace;font-size:12px'>{val}</td></tr>"""
        m = self._matched
        if m is not None:
            rows += f"""
                <tr><td style='padding:4px 8px;border:none;width:180px;font-weight:600'>matched (est.)</td>
                <td style='padding:4px 8px;border:none'>{m:,}</td></tr>"""
        s = self._manifest_stats()
        if s is not None:
            rows += f"""
                <tr><td style='padding:4px 8px;border:none;width:180px;font-weight:600'>files</td>
                <td style='padding:4px 8px;border:none'>{s['files']:,}</td></tr>
                <tr><td style='padding:4px 8px;border:none;width:180px;font-weight:600'>data (est.)</td>
                <td style='padding:4px 8px;border:none'>{_format_bytes(s['bytes_upper_bound'])}</td></tr>"""
        return f"""<div style='border:1px solid #ddd;border-radius:4px;padding:12px;max-width:800px;font-family:sans-serif'>
            <div style='font-weight:700;font-size:15px;margin-bottom:8px'>EarthCatalogItemSearch</div>
            <table style='border-collapse:collapse;width:100%;font-size:13px'>{rows}</table></div>"""

    # ------------------------------------------------------------------
    # Count / stats
    # ------------------------------------------------------------------

    def _manifest_stats(self) -> dict | None:
        """Return ``{files, rows_upper_bound, bytes_upper_bound}`` from Iceberg manifest.

        Returns ``None`` when the Iceberg table is unavailable.
        """
        if self._table is None:
            return None
        try:
            files = self._prune()
            if not files:
                return {"files": 0, "rows_upper_bound": 0, "bytes_upper_bound": 0}
            matching = {t.file.file_path for t in self._table.scan().plan_files()} & set(files)
            if not matching:
                return {"files": 0, "rows_upper_bound": 0, "bytes_upper_bound": 0}
            n_files = len(matching)
            n_rows = 0
            n_bytes = 0
            for t in self._table.scan().plan_files():
                if t.file.file_path in matching:
                    n_rows += t.file.record_count
                    n_bytes += t.file.file_size_in_bytes
            return {"files": n_files, "rows_upper_bound": n_rows, "bytes_upper_bound": n_bytes}
        except Exception:
            return None

    def stats(self) -> dict | None:
        """Estimated upper-bound query statistics from Iceberg metadata.

        Returns a dict with:

        * ``files`` — number of Parquet files the search will touch
        * ``rows_upper_bound`` — total rows across those files (pre-filter)
        * ``bytes_upper_bound`` — total bytes of those files on storage

        Zero I/O — reads Iceberg manifest metadata only.
        """
        return self._manifest_stats()

    def matched(self) -> int | None:
        """Estimated upper-bound count of matching items (before CQL2 filtering).

        Reads Iceberg manifest metadata — zero I/O on the actual Parquet
        files.  The returned count is the sum of all rows in the matching
        partition **files**, not the exact number of items after CQL2
        filters are applied.  The actual count will be ≤ this number.

        Returns ``None`` if the Iceberg table is not available.
        """
        import warnings

        if self._matched is not None:
            return self._matched
        m = self._manifest_stats()
        if m is None:
            self._matched = None
            return None
        self._matched = m["rows_upper_bound"]
        if self._matched is not None:
            warnings.warn(
                f"matched() returns an upper bound ({self._matched:,}) — "
                f"the actual count after CQL2 filters may be lower.",
                stacklevel=2,
            )
        return self._matched

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def _prune(self) -> list[str]:
        if self._cached_files is None:
            self._cached_files = self._engine.prune(**self._params)
        return self._cached_files

    def items_as_dicts(self):
        """Yield STAC item dicts lazily across all matching files."""
        files = self._prune()
        if not files:
            return
        seen = 0
        max_items = self._params.get("max_items")
        with self._anonymous_ctx():
            for batch in self._engine.iter_items(files, **self._params):
                for item in batch:
                    yield item
                    seen += 1
                    if max_items is not None and seen >= max_items:
                        return

    def items(self):
        """Yield ``pystac.Item`` objects lazily across all matching files."""
        try:
            import pystac
        except ImportError:
            yield from self.items_as_dicts()
            return
        for d in self.items_as_dicts():
            yield pystac.Item.from_dict(d)

    def pages(self):
        """Yield one file's results at a time as ``list[dict]``.

        Each yielded value is the complete set of matching items from
        a single Iceberg partition file.  Useful for batched processing.
        """
        files = self._prune()
        if not files:
            return
        seen = 0
        max_items = self._params.get("max_items")
        with self._anonymous_ctx():
            for batch in self._engine.iter_items(files, **self._params):
                yield batch
                seen += len(batch)
                if max_items is not None and seen >= max_items:
                    return

    def item_collection(self):
        """Materialize all results into a ``pystac.ItemCollection``."""
        import pystac

        return pystac.ItemCollection(list(self.items()))


# ---------------------------------------------------------------------------
# Geometry / datetime extraction helpers
# ---------------------------------------------------------------------------


def _extract_geometry(**kwargs):
    intersects = kwargs.get("intersects")
    if intersects is not None:
        from shapely.geometry import shape
        return shape(intersects)
    bbox = kwargs.get("bbox")
    if bbox is not None:
        from shapely.geometry import box
        return box(bbox[0], bbox[1], bbox[2], bbox[3])
    return None


def _extract_datetime_range(**kwargs):
    raw = kwargs.get("datetime")
    if raw is None:
        return None, None
    return _parse_datetime_range(raw)


def _parse_datetime_range(raw: str) -> tuple[str | None, str | None]:
    if not raw:
        return None, None
    if "/" in raw:
        parts = raw.split("/", 1)
        start = None if parts[0] in ("", "..") else parts[0]
        end = None if parts[1] in ("", "..") else parts[1]
        return _norm_date(start), _norm_date(end)
    return _norm_date(raw), None


def _norm_date(d: str | None) -> str | None:
    if d is None:
        return None
    if len(d) == 4 and d.isdigit():
        return f"{d}-01-01"
    if len(d) == 7 and d.count("-") == 1:
        return f"{d}-01"
    if len(d) == 10 and d.count("-") == 2:
        return d
    return d


def _format_bytes(n: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
