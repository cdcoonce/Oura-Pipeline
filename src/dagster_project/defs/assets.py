import dagster as dg
import polars as pl
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable

from .resources import DuckDBResource, OuraAPI

partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")


def _day(context: dg.AssetExecutionContext) -> date:
    return datetime.fromisoformat(context.partition_key).date()


VALID_TABLES = frozenset(
    {
        "sleep",
        "activity",
        "readiness",
        "spo2",
        "stress",
        "resilience",
        "heartrate",
        "sleep_periods",
        "sleep_time",
        "workouts",
        "sessions",
        "tags",
        "rest_mode_periods",
    }
)

# Expected column schemas for each raw table (Oura API v2 response shapes).
# Used to create properly-typed empty tables when the API returns no data,
# so downstream dbt models can reference all expected columns.
TABLE_SCHEMAS: Dict[str, str] = {
    "sleep": """
        id VARCHAR,
        contributors STRUCT(
            deep_sleep BIGINT, efficiency BIGINT, latency BIGINT,
            rem_sleep BIGINT, restfulness BIGINT, timing BIGINT,
            total_sleep BIGINT
        ),
        day VARCHAR, score BIGINT, "timestamp" VARCHAR,
        partition_date DATE
    """,
    "activity": """
        id VARCHAR, day VARCHAR, class_5_min VARCHAR,
        score BIGINT, active_calories BIGINT, average_met_minutes DOUBLE,
        contributors STRUCT(
            meet_daily_targets BIGINT, move_every_hour BIGINT,
            recovery_time BIGINT, stay_active BIGINT, training_frequency BIGINT,
            training_volume BIGINT
        ),
        equivalent_walking_distance BIGINT, high_activity_met_minutes DOUBLE,
        high_activity_time BIGINT, inactivity_alerts BIGINT,
        low_activity_met_minutes DOUBLE, low_activity_time BIGINT,
        medium_activity_met_minutes DOUBLE, medium_activity_time BIGINT,
        met STRUCT(interval DOUBLE, items DOUBLE[], "timestamp" VARCHAR),
        meters_to_target BIGINT, non_wear_time BIGINT,
        resting_time BIGINT, sedentary_met_minutes DOUBLE,
        sedentary_time BIGINT, steps BIGINT, target_calories BIGINT,
        target_meters BIGINT, total_calories BIGINT, "timestamp" VARCHAR,
        partition_date DATE
    """,
    "readiness": """
        id VARCHAR,
        contributors STRUCT(
            activity_balance BIGINT, body_temperature BIGINT,
            hrv_balance INTEGER, previous_day_activity BIGINT,
            previous_night BIGINT, recovery_index BIGINT,
            resting_heart_rate BIGINT, sleep_balance INTEGER,
            sleep_regularity INTEGER
        ),
        day VARCHAR, score BIGINT, temperature_deviation DOUBLE,
        temperature_trend_deviation DOUBLE, "timestamp" VARCHAR,
        partition_date DATE
    """,
    "spo2": """
        id VARCHAR, day VARCHAR,
        spo2_percentage STRUCT(average DOUBLE),
        breathing_disturbance_index BIGINT,
        partition_date DATE
    """,
    "stress": """
        id VARCHAR, day VARCHAR, stress_high BIGINT,
        recovery_high BIGINT, day_summary INTEGER,
        partition_date DATE
    """,
    "resilience": """
        id VARCHAR, day VARCHAR, level VARCHAR,
        contributors STRUCT(
            sleep_recovery DOUBLE, daytime_recovery DOUBLE, stress DOUBLE
        ),
        partition_date DATE
    """,
    "heartrate": """
        "timestamp" VARCHAR, bpm BIGINT, source VARCHAR,
        partition_date DATE
    """,
    "sleep_periods": """
        id VARCHAR, day VARCHAR, bedtime_start VARCHAR, bedtime_end VARCHAR,
        type VARCHAR, total_sleep_duration BIGINT,
        deep_sleep_duration BIGINT, light_sleep_duration BIGINT,
        rem_sleep_duration BIGINT, awake_time BIGINT, time_in_bed BIGINT,
        efficiency BIGINT, latency BIGINT,
        average_heart_rate DOUBLE, average_hrv DOUBLE,
        lowest_heart_rate BIGINT, average_breath DOUBLE,
        restless_periods BIGINT,
        partition_date DATE
    """,
    "sleep_time": """
        id VARCHAR, day VARCHAR,
        optimal_bedtime STRUCT(
            start_offset INTEGER, end_offset INTEGER, day_tz INTEGER
        ),
        recommendation VARCHAR, status VARCHAR,
        partition_date DATE
    """,
    "workouts": """
        id VARCHAR, day VARCHAR, activity VARCHAR,
        start_datetime VARCHAR, end_datetime VARCHAR,
        intensity VARCHAR, source VARCHAR,
        calories DOUBLE, distance DOUBLE, label VARCHAR,
        partition_date DATE
    """,
    "sessions": """
        id VARCHAR, day VARCHAR,
        start_datetime VARCHAR, end_datetime VARCHAR,
        type VARCHAR, mood VARCHAR,
        partition_date DATE
    """,
    "tags": """
        id VARCHAR, day VARCHAR,
        "timestamp" VARCHAR, text VARCHAR, tags VARCHAR[],
        partition_date DATE
    """,
    "rest_mode_periods": """
        id VARCHAR, start_day VARCHAR, start_time VARCHAR,
        end_day VARCHAR, end_time VARCHAR,
        partition_date DATE
    """,
}


def _upsert_day(con, table: str, rows: Iterable[Dict[str, Any]], day: date) -> int:
    """
    Idempotent load into oura_raw.<table> using a synthetic partition_date column.
    Deletes existing rows for that day, then inserts fresh.
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table!r}")
    df = pl.from_dicts(list(rows)) if rows else pl.DataFrame()
    con.execute("CREATE SCHEMA IF NOT EXISTS oura_raw;")
    if df.is_empty():
        schema = TABLE_SCHEMAS.get(table, "partition_date DATE")
        con.execute(f"CREATE TABLE IF NOT EXISTS oura_raw.{table} ({schema});")
        return 0
    if "partition_date" not in df.columns:
        df = df.with_columns(
            pl.lit(str(day)).str.strptime(pl.Date, strict=False).alias("partition_date")
        )
    con.register("tmp_df", df.to_arrow())
    con.execute(
        f"CREATE TABLE IF NOT EXISTS oura_raw.{table} AS SELECT * FROM tmp_df WHERE 1=0;"
    )
    con.execute(f"DELETE FROM oura_raw.{table} WHERE partition_date = ?", [day])
    con.execute(f"INSERT INTO oura_raw.{table} SELECT * FROM tmp_df;")
    con.unregister("tmp_df")
    return df.height


# ---------------------------------------------------------------------------
# Asset factories
# ---------------------------------------------------------------------------


def _make_daily_asset(kind: str) -> Callable:
    """Factory for daily summary assets that use OuraAPI.fetch_daily."""

    @dg.asset(
        partitions_def=partitions,
        key=dg.AssetKey(["oura_raw", kind]),
        group_name="oura_raw_daily",
        kinds={"duckdb", "API"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        oura_api: OuraAPI,
        duckdb: DuckDBResource,
    ) -> dg.MaterializeResult:
        con = duckdb.get_connection()
        day = _day(context)
        rows = oura_api.fetch_daily(kind, day, day)
        count = _upsert_day(con, kind, rows, day)
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(count),
                "partition_date": dg.MetadataValue.text(str(day)),
            }
        )

    _asset.__name__ = f"oura_{kind}_raw"
    _asset.__qualname__ = f"oura_{kind}_raw"
    return _asset


def _make_granular_asset(table: str, fetch_method: str) -> Callable:
    """Factory for granular/event-level assets that use a specific fetch method."""

    @dg.asset(
        partitions_def=partitions,
        key=dg.AssetKey(["oura_raw", table]),
        group_name="oura_raw",
        kinds={"duckdb", "API"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        oura_api: OuraAPI,
        duckdb: DuckDBResource,
    ) -> dg.MaterializeResult:
        con = duckdb.get_connection()
        day = _day(context)
        rows = getattr(oura_api, fetch_method)(day, day)
        count = _upsert_day(con, table, rows, day)
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(count),
                "partition_date": dg.MetadataValue.text(str(day)),
            }
        )

    _asset.__name__ = f"oura_{table}_raw"
    _asset.__qualname__ = f"oura_{table}_raw"
    return _asset


# ---------- DAILY SUMMARY ASSETS ----------

oura_sleep_raw = _make_daily_asset("sleep")
oura_activity_raw = _make_daily_asset("activity")
oura_readiness_raw = _make_daily_asset("readiness")
oura_spo2_raw = _make_daily_asset("spo2")
oura_stress_raw = _make_daily_asset("stress")
oura_resilience_raw = _make_daily_asset("resilience")

# ---------- GRANULAR / EVENT-LEVEL ASSETS ----------

oura_heartrate_raw = _make_granular_asset("heartrate", "fetch_heartrate")
oura_sleep_periods_raw = _make_granular_asset("sleep_periods", "fetch_sleep_periods")
oura_sleep_time_raw = _make_granular_asset("sleep_time", "fetch_sleep_time")
oura_workouts_raw = _make_granular_asset("workouts", "fetch_workouts")
oura_sessions_raw = _make_granular_asset("sessions", "fetch_sessions")
oura_tags_raw = _make_granular_asset("tags", "fetch_tags")
oura_rest_mode_periods_raw = _make_granular_asset(
    "rest_mode_periods", "fetch_rest_mode_periods"
)
