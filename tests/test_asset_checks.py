"""Tests for row count asset checks with Snowflake backend."""

from datetime import date

import dagster as dg
import pytest

from dagster_project.defs.assets import _upsert_day
from dagster_project.defs.checks import _check_row_count, make_row_count_check

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in __import__("os").environ,
    reason="Snowflake credentials not available",
)


class TestRowCountCheck:
    def test_returns_checks_definition(self):
        check = make_row_count_check("sleep")
        assert isinstance(check, dg.AssetChecksDefinition)

    def test_warns_when_table_missing(self, snowflake_con):
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.severity.name == "WARN"

    def test_warns_when_table_empty(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [], date(2024, 6, 1))
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.severity.name == "WARN"

    def test_passes_with_data(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [{"id": "t1"}], date(2024, 6, 1))
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.metadata["row_count"].value == 1
