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
        con.execute(
            f"CREATE TABLE IF NOT EXISTS oura_raw.{table} (partition_date DATE)"
        )
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
        con.close()
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
        con.close()
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
