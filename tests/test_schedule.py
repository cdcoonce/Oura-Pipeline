"""Tests for the daily Oura ingestion schedule."""

from dagster_project.defs.schedules import daily_oura_job, daily_oura_schedule


class TestDailyOuraSchedule:
    def test_schedule_runs_at_6am(self) -> None:
        assert daily_oura_schedule.hour_of_day == 6
        assert daily_oura_schedule.minute_of_hour == 0

    def test_schedule_targets_correct_job(self) -> None:
        assert daily_oura_schedule.job is daily_oura_job

    def test_job_selects_raw_asset_groups(self) -> None:
        assert daily_oura_job.selection is not None
