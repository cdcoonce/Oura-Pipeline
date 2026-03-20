import dagster as dg

from .report_assets import monthly_health_report, weekly_health_report

daily_oura_job = dg.define_asset_job(
    name="daily_oura_job",
    selection=dg.AssetSelection.groups(
        "oura_raw_daily",
        "oura_raw",
        "staging",
        "intermediate",
        "marts",
    ),
)

daily_oura_schedule = dg.build_schedule_from_partitioned_job(
    job=daily_oura_job,
    hour_of_day=16,
    minute_of_hour=0,
)

weekly_report_job = dg.define_asset_job(
    name="weekly_report_job",
    selection=dg.AssetSelection.assets(weekly_health_report),
)

monthly_report_job = dg.define_asset_job(
    name="monthly_report_job",
    selection=dg.AssetSelection.assets(monthly_health_report),
)

weekly_report_schedule = dg.ScheduleDefinition(
    job=weekly_report_job,
    cron_schedule="0 17 * * 1",  # Monday 10 AM MST (after daily job)
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

monthly_report_schedule = dg.ScheduleDefinition(
    job=monthly_report_job,
    cron_schedule="0 17 1 * *",  # 1st of month 10 AM MST (after daily job)
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
