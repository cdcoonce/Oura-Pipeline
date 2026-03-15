import json
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any, Callable, Dict

import dagster as dg
import snowflake.connector

from .resources import OuraAPI, SnowflakeResource

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


def _upsert_day(
    con: snowflake.connector.SnowflakeConnection,
    table: str,
    rows: Iterable[Dict[str, Any]] | None,
    day: date,
) -> int:
    """
    Idempotent load into oura_raw.<table> using DELETE + batched INSERT.

    Each row is stored as a VARIANT (raw_data) plus a partition_date DATE.
    Uses executemany() for efficient batching — critical for heartrate data
    which has ~2000 rows/day.

    Parameters
    ----------
    con : SnowflakeConnection
        Active Snowflake connection.
    table : str
        Target table name (must be in VALID_TABLES whitelist).
    rows : Iterable[dict] | None
        API response rows to insert.
    day : date
        Partition date for idempotent upsert.

    Returns
    -------
    int
        Number of rows inserted.
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table!r}")

    cursor = con.cursor()
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS oura_raw.{table} "
        f"(raw_data VARIANT, partition_date DATE)"
    )
    cursor.execute(
        f"DELETE FROM oura_raw.{table} WHERE partition_date = %s",
        (day,),
    )

    row_list = list(rows) if rows else []
    if not row_list:
        return 0

    # Stage as VARCHAR via executemany (fast bulk insert), then
    # convert to VARIANT in one shot — avoids per-row round trips
    # which cause heartrate (~2000 rows/day) to hang.
    cursor.execute("CREATE TEMPORARY TABLE _tmp_load (json_str VARCHAR, dt DATE)")
    cursor.executemany(
        "INSERT INTO _tmp_load VALUES (%s, %s)",
        [(json.dumps(row), day) for row in row_list],
    )
    cursor.execute(
        f"INSERT INTO oura_raw.{table} (raw_data, partition_date) "
        f"SELECT PARSE_JSON(json_str), dt FROM _tmp_load"
    )
    cursor.execute("DROP TABLE _tmp_load")
    return len(row_list)


# ---------------------------------------------------------------------------
# Asset factories
# ---------------------------------------------------------------------------


def _make_daily_asset(kind: str) -> Callable:
    """Factory for daily summary assets that use OuraAPI.fetch_daily."""

    @dg.asset(
        partitions_def=partitions,
        key=dg.AssetKey(["oura_raw", kind]),
        group_name="oura_raw_daily",
        kinds={"snowflake", "API"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        oura_api: OuraAPI,
        snowflake: SnowflakeResource,
    ) -> dg.MaterializeResult:
        con = snowflake.get_connection()
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
        kinds={"snowflake", "API"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        oura_api: OuraAPI,
        snowflake: SnowflakeResource,
    ) -> dg.MaterializeResult:
        con = snowflake.get_connection()
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
