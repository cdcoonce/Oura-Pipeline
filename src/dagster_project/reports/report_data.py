"""Data fetching functions for health report generation.

Queries Snowflake mart and staging tables for a given date range
and returns typed Polars DataFrames.
"""

from datetime import date

import polars as pl
import snowflake.connector


def fetch_wellness_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """
    Fetch daily wellness data for a date range.

    Queries oura_marts.fact_daily_wellness WHERE day BETWEEN start_date AND end_date.
    Uses parameterized query (not f-string) for date values.

    Parameters
    ----------
    con : SnowflakeConnection
        Active Snowflake connection.
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).

    Returns
    -------
    pl.DataFrame
        Columns: day, readiness_score, steps, calories, sleep_score,
        sleep_efficiency, avg_spo2_pct, breathing_disturbance_index,
        stress_high, recovery_high, stress_summary, resilience_level,
        sleep_recovery_score, daytime_recovery_score, resilience_stress_score.
        Empty DataFrame with correct schema if no rows match.
    """
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_marts.fact_daily_wellness "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)


def fetch_sleep_detail_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """
    Fetch sleep detail data for a date range.

    Queries oura_marts.fact_sleep_detail WHERE day BETWEEN start_date AND end_date.
    Includes all sleep types (long_sleep, rest, nap) -- filtering to primary
    sleep is the analysis layer's responsibility.

    Parameters
    ----------
    con : SnowflakeConnection
        Active Snowflake connection.
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).

    Returns
    -------
    pl.DataFrame
        Columns: id, day, sleep_type, bedtime_start, bedtime_end,
        total_sleep_duration, deep_sleep_duration, light_sleep_duration,
        rem_sleep_duration, awake_time, time_in_bed, efficiency, latency,
        avg_hr, avg_hrv, lowest_hr, avg_breath, restless_periods,
        sleep_recommendation, optimal_bedtime_start_offset,
        optimal_bedtime_end_offset.
        Empty DataFrame with correct schema if no rows match.
    """
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_marts.fact_sleep_detail "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)


def fetch_workout_summary_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """
    Fetch workout data for a date range.

    Queries oura_staging.stg_workouts WHERE day BETWEEN start_date AND end_date.
    Note: queries staging layer directly since no mart model exists for workouts.

    Parameters
    ----------
    con : SnowflakeConnection
        Active Snowflake connection.
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).

    Returns
    -------
    pl.DataFrame
        Columns: id, day, workout_activity, start_datetime, end_datetime,
        intensity, workout_source, workout_calories, workout_distance_m,
        workout_label, partition_date.
        Empty DataFrame with correct schema if no rows match.
    """
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_staging.stg_workouts "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)
