"""Tests for asset check factories."""

import duckdb
import pytest
import dagster as dg

from dagster_project.defs.checks import make_row_count_check


@pytest.fixture()
def con():
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA IF NOT EXISTS oura_raw")
    yield c
    c.close()


class TestRowCountCheck:
    def test_returns_asset_check_definition(self) -> None:
        check = make_row_count_check("sleep")
        assert isinstance(check, dg.AssetChecksDefinition)

    def test_passes_when_table_has_rows(self, con) -> None:
        con.execute("CREATE TABLE oura_raw.sleep (day DATE, partition_date DATE)")
        con.execute("INSERT INTO oura_raw.sleep VALUES ('2024-01-01', '2024-01-01')")

        from dagster_project.defs.checks import _check_row_count

        result = _check_row_count(con, "sleep")
        assert result.passed is True
        assert result.metadata["row_count"].value > 0

    def test_warns_when_table_is_empty(self, con) -> None:
        con.execute("CREATE TABLE oura_raw.sleep (day DATE, partition_date DATE)")

        from dagster_project.defs.checks import _check_row_count

        result = _check_row_count(con, "sleep")
        assert result.passed is True
        assert result.severity == dg.AssetCheckSeverity.WARN

    def test_warns_when_table_does_not_exist(self, con) -> None:
        from dagster_project.defs.checks import _check_row_count

        result = _check_row_count(con, "sleep")
        assert result.passed is True
        assert result.severity == dg.AssetCheckSeverity.WARN
