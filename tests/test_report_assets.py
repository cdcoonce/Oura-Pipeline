import calendar
from datetime import date

import dagster as dg

from dagster_project.defs.report_assets import (
    _previous_month_range,
    _previous_week_range,
    monthly_health_report,
    weekly_health_report,
)


class TestPreviousWeekRange:
    """Tests for _previous_week_range."""

    def test_returns_monday_to_sunday(self):
        """Start should be Monday, end should be Sunday."""
        start, end = _previous_week_range()
        assert start.weekday() == 0  # Monday
        assert end.weekday() == 6  # Sunday

    def test_span_is_7_days(self):
        """Range should span exactly 7 days."""
        start, end = _previous_week_range()
        assert (end - start).days == 6  # inclusive range

    def test_end_is_before_today(self):
        """End date should be before today."""
        _, end = _previous_week_range()
        assert end < date.today()


class TestPreviousMonthRange:
    """Tests for _previous_month_range."""

    def test_start_is_first_of_month(self):
        """Start should be day 1."""
        start, _ = _previous_month_range()
        assert start.day == 1

    def test_end_is_last_of_month(self):
        """End should be last day of the previous month."""
        start, end = _previous_month_range()
        _, last_day = calendar.monthrange(start.year, start.month)
        assert end.day == last_day

    def test_same_month_and_year(self):
        """Start and end should be in the same month and year."""
        start, end = _previous_month_range()
        assert start.month == end.month
        assert start.year == end.year

    def test_end_is_before_today(self):
        """End date should be before today."""
        _, end = _previous_month_range()
        assert end < date.today()


class TestScheduleConfiguration:
    """Tests for schedule definitions."""

    def test_weekly_schedule_cron(self):
        """Weekly schedule should run Monday 17:00 UTC (10 AM MST)."""
        from dagster_project.defs.schedules import weekly_report_schedule

        assert weekly_report_schedule.cron_schedule == "0 17 * * 1"

    def test_monthly_schedule_cron(self):
        """Monthly schedule should run 1st of month 17:00 UTC (10 AM MST)."""
        from dagster_project.defs.schedules import monthly_report_schedule

        assert monthly_report_schedule.cron_schedule == "0 17 1 * *"

    def test_schedules_default_stopped(self):
        """Both schedules should start in STOPPED state."""
        from dagster_project.defs.schedules import (
            monthly_report_schedule,
            weekly_report_schedule,
        )

        assert weekly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
        assert (
            monthly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
        )


class TestAssetDefinitions:
    """Tests for report asset configuration."""

    def test_weekly_asset_exists(self):
        """weekly_health_report should be a Dagster asset."""
        assert hasattr(weekly_health_report, "key")

    def test_monthly_asset_exists(self):
        """monthly_health_report should be a Dagster asset."""
        assert hasattr(monthly_health_report, "key")

    def test_assets_in_reports_group(self):
        """Both assets should be in the 'reports' group."""
        assert weekly_health_report.group_names_by_key
        assert monthly_health_report.group_names_by_key
        for group in weekly_health_report.group_names_by_key.values():
            assert group == "reports"
        for group in monthly_health_report.group_names_by_key.values():
            assert group == "reports"


class TestDefinitionsLoad:
    """Test that the Dagster definitions load without error."""

    def test_definitions_load(self):
        """Verify that importing definitions doesn't crash."""
        from dagster_project.defs import report_assets

        assert report_assets is not None
