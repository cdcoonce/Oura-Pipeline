"""Tests for the daily Oura ingestion schedule."""

import dagster as dg

from dagster_project.defs.schedules import daily_oura_job, daily_oura_schedule


class TestDailyOuraSchedule:
    def test_schedule_runs_at_9am_mst(self) -> None:
        assert daily_oura_schedule.hour_of_day == 16
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
