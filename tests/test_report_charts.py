import pytest
import polars as pl
import base64
from datetime import date

from dagster_project.reports.report_charts import (
    generate_daily_scores_chart,
    generate_steps_chart,
    generate_sleep_stages_chart,
    generate_hrv_trend_chart,
    generate_all_charts,
)


@pytest.fixture
def wellness_df() -> pl.DataFrame:
    """7 days of wellness data for chart testing."""
    return pl.DataFrame({
        "day": [date(2025, 1, i) for i in range(1, 8)],
        "readiness_score": [75, 80, 72, 85, 78, 70, 82],
        "sleep_score": [82, 78, 85, 80, 76, 88, 79],
        "steps": [8500, 6200, 9100, 7800, 10200, 5500, 8800],
        "calories": [2100, 1900, 2300, 2000, 2400, 1800, 2200],
    })


@pytest.fixture
def sleep_df() -> pl.DataFrame:
    """7 days of sleep data for chart testing."""
    return pl.DataFrame({
        "day": [date(2025, 1, i) for i in range(1, 8)],
        "sleep_type": ["long_sleep"] * 7,
        "deep_sleep_duration": [5400, 4800, 6000, 5100, 4500, 5700, 5400],
        "light_sleep_duration": [14400, 15000, 13800, 14400, 15600, 14100, 14400],
        "rem_sleep_duration": [7200, 6600, 7800, 7200, 6000, 7500, 7200],
        "awake_time": [1800, 2400, 1200, 1800, 3000, 1500, 1800],
        "avg_hrv": [35.0, 42.0, 38.0, 45.0, 32.0, 48.0, 40.0],
    })


class TestGenerateDailyScoresChart:
    """Tests for generate_daily_scores_chart."""

    def test_returns_valid_base64_png(self, wellness_df):
        """Output should be a valid base64-encoded PNG."""
        result = generate_daily_scores_chart(wellness_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"  # PNG magic bytes

    def test_returns_none_for_single_row(self):
        """< 2 rows -> None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [80],
            "sleep_score": [75],
        })
        assert generate_daily_scores_chart(df) is None

    def test_returns_none_for_empty_df(self):
        """Empty DataFrame -> None."""
        df = pl.DataFrame(schema={
            "day": pl.Date,
            "readiness_score": pl.Int64,
            "sleep_score": pl.Int64,
        })
        assert generate_daily_scores_chart(df) is None

    def test_handles_null_scores(self):
        """Some null values should not crash -- chart renders available data."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 6)],
            "readiness_score": [75, None, 80, None, 85],
            "sleep_score": [80, 82, None, 78, 85],
        })
        result = generate_daily_scores_chart(df)
        assert result is not None


class TestGenerateStepsChart:
    """Tests for generate_steps_chart."""

    def test_returns_valid_base64_png(self, wellness_df):
        """Should produce valid PNG."""
        result = generate_steps_chart(wellness_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_custom_target(self, wellness_df):
        """Should accept custom target_steps parameter without error."""
        result = generate_steps_chart(wellness_df, target_steps=10000)
        assert result is not None

    def test_returns_none_for_no_step_data(self):
        """All null steps -> None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 4)],
            "steps": [None, None, None],
        })
        assert generate_steps_chart(df) is None


class TestGenerateSleepStagesChart:
    """Tests for generate_sleep_stages_chart."""

    def test_returns_valid_base64_png(self, sleep_df):
        """Should produce valid PNG."""
        result = generate_sleep_stages_chart(sleep_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_filters_to_long_sleep(self):
        """Should ignore nap/rest records."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2)],
            "sleep_type": ["long_sleep", "nap", "long_sleep"],
            "deep_sleep_duration": [5400, 1200, 5400],
            "light_sleep_duration": [14400, 2400, 14400],
            "rem_sleep_duration": [7200, 600, 7200],
            "awake_time": [1800, 300, 1800],
        })
        result = generate_sleep_stages_chart(df)
        assert result is not None  # Should render 2 bars, not 3

    def test_returns_none_for_no_long_sleep(self):
        """Only nap records -> None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "sleep_type": ["nap"],
            "deep_sleep_duration": [1200],
            "light_sleep_duration": [2400],
            "rem_sleep_duration": [600],
            "awake_time": [300],
        })
        assert generate_sleep_stages_chart(df) is None


class TestGenerateHrvTrendChart:
    """Tests for generate_hrv_trend_chart."""

    def test_returns_valid_base64_png(self, sleep_df):
        """Should produce valid PNG with rolling average."""
        result = generate_hrv_trend_chart(sleep_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_returns_none_for_single_record(self):
        """< 2 records -> None (can't draw a trend)."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "sleep_type": ["long_sleep"],
            "avg_hrv": [40.0],
        })
        assert generate_hrv_trend_chart(df) is None


class TestGenerateAllCharts:
    """Tests for generate_all_charts."""

    def test_returns_all_four_keys(self, wellness_df, sleep_df):
        """Should return dict with all four chart keys."""
        result = generate_all_charts(wellness_df, sleep_df)
        assert set(result.keys()) == {"daily_scores", "steps", "sleep_stages", "hrv_trend"}

    def test_all_charts_are_valid_or_none(self, wellness_df, sleep_df):
        """Each value should be a base64 string or None."""
        result = generate_all_charts(wellness_df, sleep_df)
        for key, value in result.items():
            if value is not None:
                decoded = base64.b64decode(value)
                assert decoded[:8] == b"\x89PNG\r\n\x1a\n", f"{key} is not a valid PNG"

    def test_individual_failure_does_not_crash_others(self):
        """If one chart's data is bad, others should still generate."""
        wellness = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [75, 80, 72, 85, 78, 70, 82],
            "sleep_score": [82, 78, 85, 80, 76, 88, 79],
            "steps": [None] * 7,  # Steps chart will return None
            "calories": [2000] * 7,
        })
        sleep = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "sleep_type": ["long_sleep"] * 7,
            "deep_sleep_duration": [5400] * 7,
            "light_sleep_duration": [14400] * 7,
            "rem_sleep_duration": [7200] * 7,
            "awake_time": [1800] * 7,
            "avg_hrv": [40.0] * 7,
        })
        result = generate_all_charts(wellness, sleep)
        # Steps should be None, but others should succeed
        assert result["steps"] is None
        assert result["daily_scores"] is not None
