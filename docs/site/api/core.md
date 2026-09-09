# Core API

The user-facing facade lives in `earthcatalog.facade`; `earthcatalog.catalog`
re-exports it, so `from earthcatalog import EarthCatalog` keeps working.

::: earthcatalog.facade.EarthCatalog

::: earthcatalog.catalog
    options:
      filters:
        - "!^_"

::: earthcatalog.catalog.ICEBERG_SCHEMA

::: earthcatalog.catalog.PARTITION_SPEC

## Search extras

DuckDB-backed searches are module-level functions (the facade only keeps
`search`, `search_to_arrow` and `search_files`):

::: earthcatalog.search.duck_search

::: earthcatalog.search.search_uris

::: earthcatalog.transform

::: earthcatalog.lock

::: earthcatalog.store_config

::: earthcatalog.partitioner

::: earthcatalog.grids
