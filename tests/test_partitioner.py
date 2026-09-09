"""The partitioner owns temporal binning: time_bin + bin_value + the grid
registry that threads GridConfig.time_bin into every partitioner."""

from __future__ import annotations

import pytest

from earthcatalog.config import GridConfig
from earthcatalog.grids import build_partitioner, register_grid
from earthcatalog.grids.h3_partitioner import H3Partitioner
from earthcatalog.grids.s2_partitioner import S2Partitioner
from earthcatalog.grids.utm_partitioner import UTMPartitioner
from earthcatalog.partitioner import AbstractPartitioner, bin_value


class TestTimeBinOwnership:
    @pytest.mark.parametrize("cls", [H3Partitioner, S2Partitioner, UTMPartitioner])
    def test_default_time_bin_is_year(self, cls):
        assert cls().time_bin == "year"

    def test_bin_value_month(self):
        p = H3Partitioner(resolution=2, time_bin="month")
        assert p.bin_value("2025-12-20T00:00:00Z") == "2025-12"

    def test_bin_value_day(self):
        p = S2Partitioner(resolution=2, time_bin="day")
        assert p.bin_value("2025-12-20T00:00:00Z") == "2025-12-20"

    def test_bin_value_unknown_for_missing_datetime(self):
        assert UTMPartitioner().bin_value(None) == "unknown"

    def test_bin_value_unknown_for_unparseable(self):
        assert UTMPartitioner().bin_value("not-a-date") == "unknown"

    def test_bad_time_bin_rejected_at_construction(self):
        with pytest.raises(ValueError, match="time bin"):
            H3Partitioner(time_bin="week")

    def test_module_level_bin_value_still_available(self):
        # schema.py re-exports it for historical callers.
        from earthcatalog.schema import bin_value as schema_bin_value

        assert schema_bin_value is bin_value
        assert bin_value("2025-12-20T00:00:00Z", "month") == "2025-12"


class TestFactoryRegistry:
    def test_factory_passes_time_bin_through(self):
        p = build_partitioner(GridConfig(type="h3", resolution=2, time_bin="month"))
        assert isinstance(p, H3Partitioner)
        assert p.time_bin == "month"

    def test_unknown_grid_lists_known_types(self):
        with pytest.raises(ValueError, match="h3"):
            build_partitioner(GridConfig(type="nope"))

    def test_geojson_without_boundaries_raises(self):
        with pytest.raises(ValueError, match="boundaries_path"):
            build_partitioner(GridConfig(type="geojson"))

    def test_register_grid_extension_point(self):
        class MyGrid(H3Partitioner):
            pass

        register_grid("my_test_grid", lambda cfg, **kw: MyGrid(time_bin=kw["time_bin"]))
        try:
            p = build_partitioner(GridConfig(type="my_test_grid", time_bin="day"))
            assert isinstance(p, MyGrid)
            assert isinstance(p, AbstractPartitioner)
            assert p.time_bin == "day"
        finally:
            from earthcatalog import grids

            grids._REGISTRY.pop("my_test_grid", None)
