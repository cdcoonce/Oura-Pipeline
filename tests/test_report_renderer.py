import pytest
from datetime import date

from dagster_project.reports.report_analysis import (
    MetricSummary, PersonalBest, AreaToImprove,
    SleepSummary, WorkoutSummary, ReportData,
)
from dagster_project.reports.report_renderer import render_report


@pytest.fixture
def sample_report_data() -> ReportData:
    """Minimal but complete ReportData for rendering tests."""
    return ReportData(
        period_type="weekly",
        start_date=date(2025, 1, 6),
        end_date=date(2025, 1, 12),
        metric_summaries=[
            MetricSummary("readiness_score", 78.5, 70.0, 85.0, "improving", 5.2, 7, 7),
            MetricSummary("sleep_score", 82.0, 76.0, 88.0, "stable", 1.1, 7, 7),
            MetricSummary("steps", 8200.0, 5500.0, 10200.0, "declining", -8.3, 7, 7),
        ],
        personal_bests=[
            PersonalBest("steps", 10200.0, date(2025, 1, 10)),
            PersonalBest("sleep_score", 88.0, date(2025, 1, 11)),
        ],
        areas_to_improve=[
            AreaToImprove("steps", "Declined 8.3% over period", 8200.0, "Try a daily walk after lunch"),
        ],
        sleep_summary=SleepSummary(
            avg_duration_hrs=7.5, avg_efficiency=88.0, avg_hrv=42.0,
            avg_hr=56.0, deep_pct=20.0, light_pct=50.0, rem_pct=25.0, awake_pct=5.0,
        ),
        workout_summary=WorkoutSummary(
            total_count=4, total_calories=1200.0,
            by_activity={"running": 2, "cycling": 1, "strength_training": 1},
        ),
    )


@pytest.fixture
def sample_charts() -> dict[str, str | None]:
    """Minimal chart dict — use placeholder base64 for testing."""
    # 1x1 red pixel PNG as base64 (valid PNG, minimal size)
    pixel = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="
    return {
        "daily_scores": pixel,
        "steps": pixel,
        "sleep_stages": pixel,
        "hrv_trend": None,  # Simulate missing chart
    }


class TestRenderReport:
    """Tests for render_report."""

    def test_returns_html_string(self, sample_report_data, sample_charts):
        """Output should be an HTML document."""
        html = render_report(sample_report_data, sample_charts)
        assert "<!DOCTYPE html>" in html
        assert "</html>" in html

    def test_contains_period_header(self, sample_report_data, sample_charts):
        """Should display 'Weekly Health Report' for weekly period."""
        html = render_report(sample_report_data, sample_charts)
        assert "Weekly Health Report" in html

    def test_contains_date_range(self, sample_report_data, sample_charts):
        """Should display the date range in the header."""
        html = render_report(sample_report_data, sample_charts)
        assert "January 06" in html
        assert "January 12, 2025" in html

    def test_contains_metric_table(self, sample_report_data, sample_charts):
        """Should render metrics in an HTML table."""
        html = render_report(sample_report_data, sample_charts)
        assert "<table>" in html
        assert "78.5" in html  # readiness_score mean
        assert "Readiness Score" in html  # prettified metric name

    def test_contains_trend_indicators(self, sample_report_data, sample_charts):
        """Should render trend arrows with CSS classes."""
        html = render_report(sample_report_data, sample_charts)
        assert "trend-improving" in html
        assert "trend-declining" in html

    def test_contains_personal_bests(self, sample_report_data, sample_charts):
        """Should list personal bests."""
        html = render_report(sample_report_data, sample_charts)
        assert "10200.0" in html  # steps best
        assert "Personal Bests" in html

    def test_contains_sleep_summary(self, sample_report_data, sample_charts):
        """Should display sleep metrics."""
        html = render_report(sample_report_data, sample_charts)
        assert "7.5" in html  # avg duration
        assert "42.0" in html  # avg HRV

    def test_contains_workout_summary(self, sample_report_data, sample_charts):
        """Should display workout count and breakdown."""
        html = render_report(sample_report_data, sample_charts)
        assert "4" in html  # total sessions
        assert "Running" in html

    def test_contains_areas_to_improve(self, sample_report_data, sample_charts):
        """Should display improvement suggestions."""
        html = render_report(sample_report_data, sample_charts)
        assert "Areas to Improve" in html
        assert "daily walk" in html

    def test_embeds_available_charts(self, sample_report_data, sample_charts):
        """Charts that are present should be embedded as base64 img tags."""
        html = render_report(sample_report_data, sample_charts)
        assert 'src="data:image/png;base64,' in html
        assert sample_charts["daily_scores"] in html

    def test_skips_missing_charts(self, sample_report_data, sample_charts):
        """Charts that are None should not render img tags for that section."""
        html = render_report(sample_report_data, sample_charts)
        # hrv_trend is None — should not have HRV Trend section
        assert "HRV Trend" not in html

    def test_monthly_period_type(self, sample_charts):
        """Monthly reports should display 'Monthly Health Report'."""
        data = ReportData(
            period_type="monthly",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            metric_summaries=[], personal_bests=[],
            areas_to_improve=[], sleep_summary=None, workout_summary=None,
        )
        html = render_report(data, sample_charts)
        assert "Monthly Health Report" in html

    def test_handles_no_workout_data(self, sample_charts):
        """workout_summary = None should skip the workout section."""
        data = ReportData(
            period_type="weekly",
            start_date=date(2025, 1, 6),
            end_date=date(2025, 1, 12),
            metric_summaries=[], personal_bests=[],
            areas_to_improve=[], sleep_summary=None, workout_summary=None,
        )
        html = render_report(data, sample_charts)
        assert "Activity & Workouts" not in html

    def test_contains_generated_timestamp(self, sample_report_data, sample_charts):
        """Footer should have a generation timestamp."""
        html = render_report(sample_report_data, sample_charts)
        assert "Generated by Oura Pipeline" in html
