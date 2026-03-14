import dagster as dg

daily_oura_job = dg.define_asset_job(
    name="daily_oura_job",
    selection=dg.AssetSelection.groups("oura_raw_daily", "oura_raw"),
)

daily_oura_schedule = dg.build_schedule_from_partitioned_job(
    job=daily_oura_job,
    hour_of_day=6,
    minute_of_hour=0,
)
