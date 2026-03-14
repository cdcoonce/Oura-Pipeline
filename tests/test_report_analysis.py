import pytest
import polars as pl
from datetime import date
from pathlib import Path

from dagster_project.reports.report_analysis import (
    InsufficientDataError,
    MetricSummary,
    PersonalBest,
    AreaToImprove,
    SleepSummary,
    WorkoutSummary,
    ReportData,
    compute_metric_summaries,
    find_personal_bests,
    identify_areas_to_improve,
    build_sleep_summary,
    build_workout_summary,
    build_report_data,
)

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def wellness_df() -> pl.DataFrame:
    """Load wellness fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "wellness_fixture.csv", try_parse_dates=True)


@pytest.fixture
def sleep_df() -> pl.DataFrame:
    """Load sleep fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "sleep_fixture.csv", try_parse_dates=True)


@pytest.fixture
def workout_df() -> pl.DataFrame:
    """Load workout fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "workout_fixture.csv", try_parse_dates=True)


class TestComputeMetricSummaries:
    """Tests for compute_metric_summaries."""

    def test_returns_summary_for_each_metric(self, wellness_df):
        """Should return one MetricSummary per metric in WELLNESS_METRICS."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        metric_names = {s.name for s in result}
        assert "readiness_score" in metric_names
        assert "sleep_score" in metric_names
        assert "steps" in metric_names

    def test_declining_trend_detected(self, wellness_df):
        """Fixture has higher scores in first half, lower in second half -> declining."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.trend_direction == "declining"
        assert readiness.trend_pct < 0

    def test_stable_trend_for_flat_data(self):
        """Constant values across period -> stable trend."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [75] * 7,
            "sleep_score": [80] * 7,
            "sleep_efficiency": [90] * 7,
            "steps": [9000] * 7,
            "calories": [2400] * 7,
            "avg_spo2_pct": [97.5] * 7,
            "stress_high": [100] * 7,
            "recovery_high": [350] * 7,
        })
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 7))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.trend_direction == "stable"

    def test_days_with_data_tracks_nulls(self, wellness_df):
        """days_with_data should be less than total_days when NULLs present."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.days_with_data < readiness.total_days

    def test_empty_dataframe_returns_empty_list(self):
        """Empty DataFrame -> empty list, no crash."""
        df = pl.DataFrame(schema={"day": pl.Date, "readiness_score": pl.Int64})
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 7))
        assert result == []

    def test_single_day_returns_stable_trend(self):
        """Only 1 day of data -> can't split, trend should be stable with 0% change."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [80],
        })
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 1))
        if result:
            assert result[0].trend_direction == "stable"
            assert result[0].trend_pct == 0.0

    def test_improving_trend_detected(self):
        """Lower values in first half, higher in second -> improving."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 11)],
            "readiness_score": [60, 62, 61, 63, 60, 80, 82, 81, 83, 80],
        })
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 10))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.trend_direction == "improving"
        assert readiness.trend_pct > 0

    def test_min_max_values(self, wellness_df):
        """Min and max values should match actual data extremes."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        steps = next(s for s in result if s.name == "steps")
        assert steps.max_val == 15000
        assert steps.min_val == 5000


class TestFindPersonalBests:
    """Tests for find_personal_bests."""

    def test_finds_max_steps(self, wellness_df, sleep_df):
        """Should identify the day with 15000 steps as a personal best."""
        result = find_personal_bests(wellness_df, sleep_df)
        steps_best = next(pb for pb in result if pb.metric == "steps")
        assert steps_best.value == 15000

    def test_finds_max_sleep_score(self, wellness_df, sleep_df):
        """Should identify the day with sleep_score = 95."""
        result = find_personal_bests(wellness_df, sleep_df)
        sleep_best = next(pb for pb in result if pb.metric == "sleep_score")
        assert sleep_best.value == 95

    def test_finds_max_hrv_from_long_sleep_only(self, wellness_df, sleep_df):
        """HRV personal best should come from long_sleep records only."""
        result = find_personal_bests(wellness_df, sleep_df)
        hrv_best = next(pb for pb in result if pb.metric == "avg_hrv")
        assert hrv_best.value == 65

    def test_finds_lowest_hr(self, wellness_df, sleep_df):
        """Should identify lowest resting heart rate from long_sleep."""
        result = find_personal_bests(wellness_df, sleep_df)
        hr_best = next(pb for pb in result if pb.metric == "lowest_hr")
        assert hr_best.value == 44

    def test_results_sorted_by_metric_name(self, wellness_df, sleep_df):
        """Personal bests should be sorted alphabetically by metric name."""
        result = find_personal_bests(wellness_df, sleep_df)
        metric_names = [pb.metric for pb in result]
        assert metric_names == sorted(metric_names)

    def test_empty_dfs_return_empty_list(self):
        """No data -> no personal bests."""
        empty_w = pl.DataFrame(schema={
            "day": pl.Date, "readiness_score": pl.Int64,
            "steps": pl.Int64, "sleep_score": pl.Int64,
        })
        empty_s = pl.DataFrame(schema={
            "day": pl.Date, "sleep_type": pl.Utf8,
            "avg_hrv": pl.Float64, "lowest_hr": pl.Int64,
        })
        result = find_personal_bests(empty_w, empty_s)
        assert result == []


class TestIdentifyAreasToImprove:
    """Tests for identify_areas_to_improve."""

    def test_below_threshold_triggers_alert(self, wellness_df, sleep_df):
        """Fixture includes sleep_efficiency = 60 day, pulling average below 85 threshold."""
        result = identify_areas_to_improve(wellness_df, sleep_df)
        metrics = {a.metric for a in result}
        assert "sleep_efficiency" in metrics

    def test_declining_trend_triggers_alert(self, wellness_df, sleep_df):
        """Fixture has declining readiness > 10% -> should flag."""
        result = identify_areas_to_improve(wellness_df, sleep_df)
        # readiness_score declines ~18.5% which is > 10%
        # But its average (~72.4) is also above the threshold of 70
        # so it should appear with a decline reason
        declining = [a for a in result if "Declined" in a.reason]
        assert len(declining) >= 1

    def test_no_alerts_for_healthy_data(self):
        """All metrics above thresholds and stable -> empty list."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [85] * 7,
            "sleep_score": [88] * 7,
            "sleep_efficiency": [92] * 7,
            "avg_spo2_pct": [98.0] * 7,
            "steps": [9000] * 7,
            "calories": [2400] * 7,
            "stress_high": [100] * 7,
            "recovery_high": [350] * 7,
        })
        empty_sleep = pl.DataFrame(schema={
            "day": pl.Date, "sleep_type": pl.Utf8,
            "avg_hrv": pl.Float64,
        })
        result = identify_areas_to_improve(df, empty_sleep)
        assert result == []

    def test_threshold_takes_precedence_over_decline(self):
        """When both threshold and decline trigger, threshold reason is used."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 11)],
            "readiness_score": [65, 64, 63, 62, 61, 50, 49, 48, 47, 46],
        })
        empty_sleep = pl.DataFrame(schema={
            "day": pl.Date, "sleep_type": pl.Utf8,
            "avg_hrv": pl.Float64,
        })
        result = identify_areas_to_improve(df, empty_sleep)
        readiness_areas = [a for a in result if a.metric == "readiness_score"]
        assert len(readiness_areas) == 1
        assert "Below target" in readiness_areas[0].reason

    def test_suggestions_present(self, wellness_df, sleep_df):
        """Each area to improve should have a non-empty suggestion."""
        result = identify_areas_to_improve(wellness_df, sleep_df)
        for area in result:
            assert area.suggestion != ""


class TestBuildSleepSummary:
    """Tests for build_sleep_summary."""

    def test_filters_to_long_sleep_only(self, sleep_df):
        """Should only use long_sleep records for averages."""
        result = build_sleep_summary(sleep_df)
        assert result is not None
        assert 5.0 < result.avg_duration_hrs < 10.0

    def test_stage_percentages_sum_to_approximately_100(self, sleep_df):
        """Deep + light + REM + awake should be close to 100%."""
        result = build_sleep_summary(sleep_df)
        assert result is not None
        total = result.deep_pct + result.light_pct + result.rem_pct + result.awake_pct
        assert 95.0 < total < 110.0  # Awake uses time_in_bed denominator

    def test_duration_in_hours(self, sleep_df):
        """Average duration should be around 7.3 hours for fixture data."""
        result = build_sleep_summary(sleep_df)
        assert result is not None
        assert abs(result.avg_duration_hrs - 7.33) < 0.5

    def test_returns_none_for_empty_df(self):
        """No records -> None."""
        df = pl.DataFrame(schema={
            "sleep_type": pl.Utf8, "total_sleep_duration": pl.Int64,
            "deep_sleep_duration": pl.Int64, "light_sleep_duration": pl.Int64,
            "rem_sleep_duration": pl.Int64, "awake_time": pl.Int64,
            "time_in_bed": pl.Int64, "efficiency": pl.Int64,
            "avg_hrv": pl.Float64, "avg_hr": pl.Float64,
        })
        assert build_sleep_summary(df) is None

    def test_returns_none_when_no_long_sleep(self):
        """Only nap records -> None."""
        df = pl.DataFrame({
            "sleep_type": ["nap", "rest"],
            "total_sleep_duration": [1500, 1200],
            "deep_sleep_duration": [300, 240],
            "light_sleep_duration": [900, 720],
            "rem_sleep_duration": [300, 240],
            "awake_time": [300, 360],
            "time_in_bed": [1800, 1560],
            "efficiency": [83, 77],
            "avg_hrv": [30.0, 28.0],
            "avg_hr": [58.0, 60.0],
        })
        assert build_sleep_summary(df) is None


class TestBuildWorkoutSummary:
    """Tests for build_workout_summary."""

    def test_counts_by_activity(self, workout_df):
        """Should group sessions by workout_activity."""
        result = build_workout_summary(workout_df)
        assert result is not None
        assert result.by_activity["running"] == 5
        assert result.by_activity["cycling"] == 4
        assert result.by_activity["strength_training"] == 3
        assert result.by_activity["walking"] == 3

    def test_total_count_matches_rows(self, workout_df):
        """Total count should equal number of workout rows."""
        result = build_workout_summary(workout_df)
        assert result is not None
        assert result.total_count == 15

    def test_total_calories(self, workout_df):
        """Total calories should sum to 5090."""
        result = build_workout_summary(workout_df)
        assert result is not None
        assert result.total_calories == 5090

    def test_returns_none_for_empty_df(self):
        """No workouts -> None."""
        df = pl.DataFrame(schema={
            "workout_activity": pl.Utf8,
            "workout_calories": pl.Float64,
        })
        assert build_workout_summary(df) is None


class TestBuildReportData:
    """Tests for build_report_data (integration)."""

    def test_builds_complete_report(self, wellness_df, sleep_df, workout_df):
        """Should produce a ReportData with all fields populated."""
        result = build_report_data(
            "weekly", date(2025, 1, 1), date(2025, 1, 7),
            wellness_df, sleep_df, workout_df,
        )
        assert result.period_type == "weekly"
        assert len(result.metric_summaries) > 0
        assert result.sleep_summary is not None
        assert result.workout_summary is not None

    def test_insufficient_data_raises(self):
        """< 2 rows of non-null core data -> InsufficientDataError."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [None],
            "sleep_score": [None],
        })
        empty_sleep = pl.DataFrame(schema={"day": pl.Date, "sleep_type": pl.Utf8})
        empty_workout = pl.DataFrame(schema={"workout_activity": pl.Utf8})
        with pytest.raises(InsufficientDataError):
            build_report_data(
                "weekly", date(2025, 1, 1), date(2025, 1, 7),
                df, empty_sleep, empty_workout,
            )

    def test_report_dates_match(self, wellness_df, sleep_df, workout_df):
        """Report start and end dates should match inputs."""
        result = build_report_data(
            "monthly", date(2025, 1, 1), date(2025, 1, 30),
            wellness_df, sleep_df, workout_df,
        )
        assert result.start_date == date(2025, 1, 1)
        assert result.end_date == date(2025, 1, 30)
        assert result.period_type == "monthly"

    def test_personal_bests_present(self, wellness_df, sleep_df, workout_df):
        """Report should contain personal bests."""
        result = build_report_data(
            "monthly", date(2025, 1, 1), date(2025, 1, 30),
            wellness_df, sleep_df, workout_df,
        )
        assert len(result.personal_bests) > 0

    def test_frozen_dataclass(self, wellness_df, sleep_df, workout_df):
        """ReportData should be immutable (frozen=True)."""
        result = build_report_data(
            "monthly", date(2025, 1, 1), date(2025, 1, 30),
            wellness_df, sleep_df, workout_df,
        )
        with pytest.raises(AttributeError):
            result.period_type = "weekly"
