"""Tests for the daily Oura ingestion schedule."""

import dagster as dg

from dagster_project.defs.schedules import (
    daily_oura_job,
    daily_oura_schedule,
    monthly_report_schedule,
    weekly_report_schedule,
)


class TestDailyOuraSchedule:
    def test_schedule_runs_at_6am_phoenix(self) -> None:
        # This schedule is unresolved until Definitions are built (the asset
        # selection's partitions_def isn't known yet), so hour/minute are all
        # that's available here. Its timezone comes from the job's
        # DailyPartitionsDefinition(timezone="America/Phoenix") in assets.py
        # and is verified once resolved (see definitions load check).
        assert daily_oura_schedule.hour_of_day == 6
        assert daily_oura_schedule.minute_of_hour == 0

    def test_schedule_targets_correct_job(self) -> None:
        assert daily_oura_schedule.job is daily_oura_job

    def test_job_selects_raw_and_dbt_groups(self) -> None:
        expected = dg.AssetSelection.groups(
            "oura_raw_daily",
            "oura_raw",
            "staging",
            "intermediate",
            "marts",
        )
        assert daily_oura_job.selection == expected


class TestReportSchedules:
    def test_weekly_report_runs_after_daily_job(self) -> None:
        assert weekly_report_schedule.cron_schedule == "30 6 * * 1"
        assert weekly_report_schedule.execution_timezone == "America/Phoenix"

    def test_monthly_report_runs_after_daily_job(self) -> None:
        assert monthly_report_schedule.cron_schedule == "0 7 1 * *"
        assert monthly_report_schedule.execution_timezone == "America/Phoenix"

    def test_report_schedules_default_stopped(self) -> None:
        """Both report schedules should start in STOPPED state."""
        assert weekly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
        assert (
            monthly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
        )
