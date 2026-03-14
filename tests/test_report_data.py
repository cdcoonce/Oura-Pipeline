import pytest
import duckdb
import polars as pl
from datetime import date

from dagster_project.reports.report_data import (
    fetch_wellness_for_period,
    fetch_sleep_detail_for_period,
    fetch_workout_summary_for_period,
)


@pytest.fixture
def con():
    """In-memory DuckDB with mart schemas populated from fixture data."""
    con = duckdb.connect(":memory:")

    # Create schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS oura_marts;")
    con.execute("CREATE SCHEMA IF NOT EXISTS oura_staging;")

    # Create fact_daily_wellness from fixture
    con.execute("""
        CREATE TABLE oura_marts.fact_daily_wellness AS
        SELECT
            CAST('2025-01-01'::DATE + INTERVAL (i) DAY AS DATE) AS day,
            60 + (i % 30) AS readiness_score,
            5000 + (i * 200) AS steps,
            1800 + (i * 50) AS calories,
            70 + (i % 25) AS sleep_score,
            80 + (i % 15) AS sleep_efficiency,
            96.0 + (i % 4) * 0.5 AS avg_spo2_pct,
            i % 5 AS breathing_disturbance_index,
            i * 10 AS stress_high,
            i * 8 AS recovery_high,
            i % 5 AS stress_summary,
            CASE WHEN i % 5 = 0 THEN 'solid'
                 WHEN i % 5 = 1 THEN 'strong'
                 ELSE 'adequate' END AS resilience_level,
            70.0 + (i % 20) AS sleep_recovery_score,
            65.0 + (i % 15) AS daytime_recovery_score,
            60.0 + (i % 25) AS resilience_stress_score
        FROM generate_series(0, 29) AS t(i)
    """)

    # Create fact_sleep_detail
    con.execute("""
        CREATE TABLE oura_marts.fact_sleep_detail AS
        SELECT
            'sleep_' || i AS id,
            CAST('2025-01-01'::DATE + INTERVAL (i) DAY AS DATE) AS day,
            'long_sleep' AS sleep_type,
            ('2025-01-01T22:30:00'::TIMESTAMP + INTERVAL (i) DAY) AS bedtime_start,
            ('2025-01-02T06:30:00'::TIMESTAMP + INTERVAL (i) DAY) AS bedtime_end,
            28800 AS total_sleep_duration,
            5400 AS deep_sleep_duration,
            14400 AS light_sleep_duration,
            7200 AS rem_sleep_duration,
            1800 AS awake_time,
            30600 AS time_in_bed,
            88 AS efficiency,
            600 AS latency,
            55.0 + (i % 10) AS avg_hr,
            30.0 + (i % 20) AS avg_hrv,
            48 + (i % 10) AS lowest_hr,
            14.5 AS avg_breath,
            3 + (i % 5) AS restless_periods,
            'ideal' AS sleep_recommendation,
            -3600 AS optimal_bedtime_start_offset,
            0 AS optimal_bedtime_end_offset
        FROM generate_series(0, 29) AS t(i)
    """)

    # Create stg_workouts
    con.execute("""
        CREATE TABLE oura_staging.stg_workouts AS
        SELECT
            'workout_' || i AS id,
            CAST('2025-01-01'::DATE + INTERVAL (i * 2) DAY AS DATE) AS day,
            CASE WHEN i % 3 = 0 THEN 'running'
                 WHEN i % 3 = 1 THEN 'cycling'
                 ELSE 'strength_training' END AS workout_activity,
            ('2025-01-01T08:00:00'::TIMESTAMP + INTERVAL (i * 2) DAY) AS start_datetime,
            ('2025-01-01T09:00:00'::TIMESTAMP + INTERVAL (i * 2) DAY) AS end_datetime,
            'moderate' AS intensity,
            'manual' AS workout_source,
            300.0 + (i * 25) AS workout_calories,
            5000.0 + (i * 500) AS workout_distance_m,
            NULL AS workout_label,
            CAST('2025-01-01'::DATE + INTERVAL (i * 2) DAY AS DATE) AS partition_date
        FROM generate_series(0, 14) AS t(i)
    """)

    yield con
    con.close()


class TestFetchWellnessForPeriod:
    """Tests for fetch_wellness_for_period."""

    def test_returns_correct_date_range(self, con):
        """Should return only rows within the specified date range."""
        df = fetch_wellness_for_period(con, date(2025, 1, 5), date(2025, 1, 10))
        assert len(df) == 6
        assert df["day"].min() == date(2025, 1, 5)
        assert df["day"].max() == date(2025, 1, 10)

    def test_returns_all_expected_columns(self, con):
        """Should include all fact_daily_wellness columns."""
        df = fetch_wellness_for_period(con, date(2025, 1, 1), date(2025, 1, 7))
        expected_cols = {
            "day", "readiness_score", "steps", "calories", "sleep_score",
            "sleep_efficiency", "avg_spo2_pct", "breathing_disturbance_index",
            "stress_high", "recovery_high", "stress_summary", "resilience_level",
            "sleep_recovery_score", "daytime_recovery_score", "resilience_stress_score",
        }
        assert set(df.columns) == expected_cols

    def test_returns_polars_dataframe(self, con):
        """Return type should be Polars DataFrame, not pandas."""
        df = fetch_wellness_for_period(con, date(2025, 1, 1), date(2025, 1, 7))
        assert isinstance(df, pl.DataFrame)

    def test_empty_range_returns_empty_df_with_schema(self, con):
        """Date range with no data -> empty DataFrame with correct columns."""
        df = fetch_wellness_for_period(con, date(2024, 1, 1), date(2024, 1, 7))
        assert len(df) == 0
        assert "readiness_score" in df.columns

    def test_ordered_by_day(self, con):
        """Results should be sorted by day ascending."""
        df = fetch_wellness_for_period(con, date(2025, 1, 1), date(2025, 1, 30))
        days = df["day"].to_list()
        assert days == sorted(days)


class TestFetchSleepDetailForPeriod:
    """Tests for fetch_sleep_detail_for_period."""

    def test_returns_correct_date_range(self, con):
        """Should filter to requested dates."""
        df = fetch_sleep_detail_for_period(con, date(2025, 1, 1), date(2025, 1, 7))
        assert len(df) == 7
        assert df["day"].min() == date(2025, 1, 1)

    def test_includes_all_sleep_columns(self, con):
        """Should include all fact_sleep_detail columns."""
        df = fetch_sleep_detail_for_period(con, date(2025, 1, 1), date(2025, 1, 7))
        assert "avg_hrv" in df.columns
        assert "sleep_type" in df.columns
        assert "optimal_bedtime_start_offset" in df.columns

    def test_empty_range(self, con):
        """No data for range -> empty DataFrame."""
        df = fetch_sleep_detail_for_period(con, date(2024, 6, 1), date(2024, 6, 7))
        assert len(df) == 0


class TestFetchWorkoutSummaryForPeriod:
    """Tests for fetch_workout_summary_for_period."""

    def test_returns_workouts_in_range(self, con):
        """Should return workout rows within the date range."""
        df = fetch_workout_summary_for_period(con, date(2025, 1, 1), date(2025, 1, 15))
        assert len(df) > 0
        assert "workout_activity" in df.columns

    def test_filters_by_date(self, con):
        """Should not return workouts outside the range."""
        df = fetch_workout_summary_for_period(con, date(2025, 1, 1), date(2025, 1, 2))
        for d in df["day"].to_list():
            assert date(2025, 1, 1) <= d <= date(2025, 1, 2)

    def test_empty_range(self, con):
        """No workouts in range -> empty DataFrame."""
        df = fetch_workout_summary_for_period(con, date(2024, 6, 1), date(2024, 6, 7))
        assert len(df) == 0
