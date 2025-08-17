import dagster as dg
from .defs.assets import (
    oura_sleep_raw, oura_readiness_raw, oura_activity_raw, oura_heartrate_raw, load_into_duckdb
)
from .defs.resources import oura_client, duckdb_conn, data_dir

assets = [oura_sleep_raw, oura_readiness_raw, oura_activity_raw, oura_heartrate_raw, load_into_duckdb]

daily_job = dg.define_asset_job("daily_oura_job", selection=[a.key for a in assets])

schedule = dg.ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 9 * * *",  # 9:00 AM Phoenix
)

defs = dg.Definitions(
    assets=assets,
    resources={
        "oura_client": oura_client,      # key used above
        "duckdb_conn": duckdb_conn,      # key used in load_into_duckdb
        "data_dir": data_dir,            # key used above
    },
    schedules=[schedule],
)