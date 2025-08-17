import dagster as dg
import polars as pl
from datetime import datetime
from typing import Iterable, Dict, Any
from .resources import OuraAPI, DuckDBResource

partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")


def _day(context: dg.AssetExecutionContext):
    return datetime.fromisoformat(context.partition_key).date()


def _upsert_day(con, table: str, rows: Iterable[Dict[str, Any]], day) -> int:
    """
    Idempotent load into oura_raw.<table> using a synthetic partition_date column.
    Deletes existing rows for that day, then inserts fresh.
    """
    df = pl.from_dicts(list(rows)) if rows else pl.DataFrame()
    # Ensure table exists
    con.execute("CREATE SCHEMA IF NOT EXISTS oura_raw;")
    if df.is_empty():
        con.execute(f"CREATE TABLE IF NOT EXISTS oura_raw.{table} (partition_date DATE)")
        return 0
    if "partition_date" not in df.columns:
        df = df.with_columns(pl.lit(str(day)).str.strptime(pl.Date, strict=False).alias("partition_date"))
    con.register("tmp_df", df.to_arrow())
    con.execute(f"CREATE TABLE IF NOT EXISTS oura_raw.{table} AS SELECT * FROM tmp_df WHERE 1=0;")
    con.execute(f"DELETE FROM oura_raw.{table} WHERE partition_date = ?", [day])
    con.execute(f"INSERT INTO oura_raw.{table} SELECT * FROM tmp_df;")
    con.unregister("tmp_df")
    return df.height


# ---------- DAILY SUMMARY ASSETS ----------

@dg.asset(
    name="oura_sleep_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_sleep_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("sleep", day, day)
    n = _upsert_day(con, "sleep", rows, day)
    return f"sleep:{day} rows={n}"


@dg.asset(
    name="oura_activity_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_activity_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("activity", day, day)
    n = _upsert_day(con, "activity", rows, day)
    return f"activity:{day} rows={n}"


@dg.asset(
    name="oura_readiness_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_readiness_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("readiness", day, day)
    n = _upsert_day(con, "readiness", rows, day)
    return f"readiness:{day} rows={n}"


@dg.asset(
    name="oura_spo2_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_spo2_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("spo2", day, day)
    n = _upsert_day(con, "spo2", rows, day)
    return f"spo2:{day} rows={n}"


@dg.asset(
    name="oura_stress_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_stress_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("stress", day, day)
    n = _upsert_day(con, "stress", rows, day)
    return f"stress:{day} rows={n}"


@dg.asset(
    name="oura_resilience_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_resilience_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("resilience", day, day)
    n = _upsert_day(con, "resilience", rows, day)
    return f"resilience:{day} rows={n}"


# ---------- GRANULAR / EVENT-LEVEL ASSETS ----------

@dg.asset(
    name="oura_heartrate_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_heartrate_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_heartrate(day, day)
    n = _upsert_day(con, "heartrate", rows, day)
    return f"heartrate:{day} rows={n}"


@dg.asset(
    name="oura_sleep_periods_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_sleep_periods_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_sleep_periods(day, day)
    n = _upsert_day(con, "sleep_periods", rows, day)
    return f"sleep_periods:{day} rows={n}"


@dg.asset(
    name="oura_sleep_time_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_sleep_time_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_sleep_time(day, day)
    n = _upsert_day(con, "sleep_time", rows, day)
    return f"sleep_time:{day} rows={n}"


@dg.asset(
    name="oura_workouts_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_workouts_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_workouts(day, day)
    n = _upsert_day(con, "workouts", rows, day)
    return f"workouts:{day} rows={n}"


@dg.asset(
    name="oura_sessions_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_sessions_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_sessions(day, day)
    n = _upsert_day(con, "sessions", rows, day)
    return f"sessions:{day} rows={n}"


@dg.asset(
    name="oura_tags_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_tags_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_tags(day, day)
    n = _upsert_day(con, "tags", rows, day)
    return f"tags:{day} rows={n}"


@dg.asset(
    name="oura_rest_mode_periods_raw",
    partitions_def=partitions,
    group_name="oura_raw",
    required_resource_keys={"oura_api", "duckdb"},
)
def oura_rest_mode_periods_raw(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    duck: DuckDBResource = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_rest_mode_periods(day, day)
    n = _upsert_day(con, "rest_mode_periods", rows, day)
    return f"rest_mode_periods:{day} rows={n}"