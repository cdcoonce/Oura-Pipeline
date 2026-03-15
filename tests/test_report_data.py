"""Tests for report data fetching with Snowflake backend."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import polars as pl

from dagster_project.reports.report_data import (
    fetch_sleep_detail_for_period,
    fetch_wellness_for_period,
    fetch_workout_summary_for_period,
)


def _mock_snowflake_result(data: dict) -> tuple[MagicMock, MagicMock]:
    """Create a mock Snowflake connection that returns the given data."""
    mock_cursor = MagicMock()
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame(data)
    mock_con = MagicMock()
    mock_con.cursor.return_value = mock_cursor
    return mock_cursor, mock_con


class TestFetchWellnessForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"DAY": [date(2024, 6, 1)], "READINESS_SCORE": [85], "STEPS": [8000]}
        )
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert isinstance(result, pl.DataFrame)

    def test_lowercases_column_names(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"DAY": [date(2024, 6, 1)], "READINESS_SCORE": [85]}
        )
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert "day" in result.columns
        assert "readiness_score" in result.columns
        assert "DAY" not in result.columns

    def test_uses_parameterized_query(self):
        mock_cursor, mock_con = _mock_snowflake_result({"DAY": []})
        fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        call_args = mock_cursor.execute.call_args
        assert "%s" in call_args[0][0]
        assert date(2024, 6, 1) in call_args[0][1]

    def test_empty_result_returns_empty_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result({"DAY": [], "STEPS": []})
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert len(result) == 0


class TestFetchSleepDetailForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"ID": ["s1"], "DAY": [date(2024, 6, 1)]}
        )
        result = fetch_sleep_detail_for_period(
            mock_con, date(2024, 6, 1), date(2024, 6, 7)
        )
        assert isinstance(result, pl.DataFrame)
        assert "id" in result.columns


class TestFetchWorkoutSummaryForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {
                "ID": ["w1"],
                "DAY": [date(2024, 6, 1)],
                "WORKOUT_ACTIVITY": ["running"],
            }
        )
        result = fetch_workout_summary_for_period(
            mock_con, date(2024, 6, 1), date(2024, 6, 7)
        )
        assert isinstance(result, pl.DataFrame)
        assert "workout_activity" in result.columns
