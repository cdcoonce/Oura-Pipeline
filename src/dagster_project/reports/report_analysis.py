"""Pure analysis functions for health report generation.

This module contains dataclasses and functions that operate on Polars
DataFrames to produce structured report data. No I/O is performed here;
all data arrives as function arguments.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class InsufficientDataError(Exception):
    """Raised when fewer than 2 days of data exist for the requested period."""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MetricSummary:
    """Summary statistics and trend for a single metric over a period."""

    name: str
    mean: float
    min_val: float
    max_val: float
    trend_direction: str  # "improving" | "declining" | "stable"
    trend_pct: float  # % change from first half mean to second half mean
    days_with_data: int
    total_days: int


@dataclass(frozen=True)
class PersonalBest:
    """A peak value achieved for a metric during the period."""

    metric: str
    value: float
    day: date


@dataclass(frozen=True)
class AreaToImprove:
    """A metric flagged for attention due to decline or below-threshold average."""

    metric: str
    reason: str
    current_avg: float
    suggestion: str


@dataclass(frozen=True)
class SleepSummary:
    """Aggregated sleep metrics for the period (primary sleep only)."""

    avg_duration_hrs: float
    avg_efficiency: float
    avg_hrv: float
    avg_hr: float
    deep_pct: float
    light_pct: float
    rem_pct: float
    awake_pct: float


@dataclass(frozen=True)
class WorkoutSummary:
    """Aggregated workout metrics for the period."""

    total_count: int
    total_calories: float
    by_activity: dict[str, int]  # activity_type -> session count


@dataclass(frozen=True)
class ReportData:
    """Complete report data ready for rendering."""

    period_type: str  # "weekly" | "monthly"
    start_date: date
    end_date: date
    metric_summaries: list[MetricSummary]
    personal_bests: list[PersonalBest]
    areas_to_improve: list[AreaToImprove]
    sleep_summary: SleepSummary | None
    workout_summary: WorkoutSummary | None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WELLNESS_METRICS: list[str] = [
    "readiness_score",
    "sleep_score",
    "sleep_efficiency",
    "steps",
    "calories",
    "avg_spo2_pct",
    "stress_high",
    "recovery_high",
]

METRIC_THRESHOLDS: dict[str, float] = {
    "sleep_efficiency": 85.0,
    "readiness_score": 70.0,
    "sleep_score": 70.0,
    "avg_spo2_pct": 95.0,
}

TREND_THRESHOLD_PCT: float = 5.0
DECLINE_ALERT_PCT: float = 10.0

_SUGGESTIONS: dict[str, str] = {
    "sleep_efficiency": (
        "Consider consistent bedtime and limiting screen time before bed"
    ),
    "readiness_score": (
        "Focus on recovery days and stress management techniques"
    ),
    "sleep_score": (
        "Prioritize sleep hygiene: dark room, cool temperature, regular schedule"
    ),
    "avg_spo2_pct": (
        "Monitor breathing patterns and consider consulting a physician"
    ),
    "steps": "Try adding a short walk after meals to increase daily movement",
    "calories": "Review activity levels and ensure adequate energy expenditure",
    "stress_high": "Incorporate relaxation practices such as meditation or deep breathing",
    "recovery_high": "Allow more rest between intense training sessions",
    "avg_hrv": "Focus on aerobic fitness and stress reduction to improve HRV",
}


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def compute_metric_summaries(
    wellness_df: pl.DataFrame,
    start_date: date,
    end_date: date,
) -> list[MetricSummary]:
    """Compute mean/min/max/trend for each metric in WELLNESS_METRICS.

    Trend detection: split the period at the midpoint. Compare mean of
    first half to mean of second half. Change > TREND_THRESHOLD_PCT is
    "improving" (if positive) or "declining" (if negative), else "stable".

    Parameters
    ----------
    wellness_df : pl.DataFrame
        Rows from fact_daily_wellness for the period.
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).

    Returns
    -------
    list[MetricSummary]
        One summary per metric. Metrics with zero non-null values are excluded.
    """
    if wellness_df.is_empty():
        return []

    total_days = wellness_df.height
    midpoint_ordinal = (start_date.toordinal() + end_date.toordinal()) // 2
    midpoint = date.fromordinal(midpoint_ordinal)

    first_half = wellness_df.filter(pl.col("day") <= midpoint)
    second_half = wellness_df.filter(pl.col("day") > midpoint)

    summaries: list[MetricSummary] = []

    for metric in WELLNESS_METRICS:
        if metric not in wellness_df.columns:
            continue

        non_null = wellness_df.select(pl.col(metric).drop_nulls())
        days_with_data = non_null.height
        if days_with_data == 0:
            continue

        col = wellness_df[metric]
        mean_val = float(col.mean())  # type: ignore[arg-type]
        min_val = float(col.min())  # type: ignore[arg-type]
        max_val = float(col.max())  # type: ignore[arg-type]

        # Trend detection
        first_vals = first_half.select(pl.col(metric).drop_nulls())
        second_vals = second_half.select(pl.col(metric).drop_nulls())

        if first_vals.is_empty() or second_vals.is_empty():
            trend_direction = "stable"
            trend_pct = 0.0
        else:
            first_mean = float(first_vals[metric].mean())  # type: ignore[arg-type]
            second_mean = float(second_vals[metric].mean())  # type: ignore[arg-type]

            if first_mean == 0:
                trend_pct = 0.0
            else:
                trend_pct = ((second_mean - first_mean) / first_mean) * 100.0

            if trend_pct > TREND_THRESHOLD_PCT:
                trend_direction = "improving"
            elif trend_pct < -TREND_THRESHOLD_PCT:
                trend_direction = "declining"
            else:
                trend_direction = "stable"

        summaries.append(
            MetricSummary(
                name=metric,
                mean=round(mean_val, 2),
                min_val=round(min_val, 2),
                max_val=round(max_val, 2),
                trend_direction=trend_direction,
                trend_pct=round(trend_pct, 2),
                days_with_data=days_with_data,
                total_days=total_days,
            )
        )

    return summaries


def find_personal_bests(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> list[PersonalBest]:
    """Identify peak values for key metrics during the period.

    Metrics checked:
    - readiness_score (max, from wellness_df)
    - sleep_score (max, from wellness_df)
    - steps (max, from wellness_df)
    - avg_hrv (max, from sleep_df, long_sleep only)
    - lowest_hr (min, from sleep_df, long_sleep only)

    Parameters
    ----------
    wellness_df : pl.DataFrame
        Rows from fact_daily_wellness.
    sleep_df : pl.DataFrame
        Rows from fact_sleep_detail.

    Returns
    -------
    list[PersonalBest]
        One entry per metric that has non-null data. Sorted by metric name.
    """
    bests: list[PersonalBest] = []

    # Wellness max metrics
    wellness_max_metrics = ["readiness_score", "sleep_score", "steps"]
    for metric in wellness_max_metrics:
        if metric not in wellness_df.columns:
            continue
        non_null = wellness_df.filter(pl.col(metric).is_not_null())
        if non_null.is_empty():
            continue
        max_val = float(non_null[metric].max())  # type: ignore[arg-type]
        best_row = non_null.filter(pl.col(metric) == max_val).head(1)
        best_day = best_row["day"][0]
        bests.append(PersonalBest(metric=metric, value=max_val, day=best_day))

    # Sleep metrics (long_sleep only)
    if not sleep_df.is_empty() and "sleep_type" in sleep_df.columns:
        long_sleep = sleep_df.filter(pl.col("sleep_type") == "long_sleep")

        # Max avg_hrv
        if "avg_hrv" in long_sleep.columns:
            non_null_hrv = long_sleep.filter(pl.col("avg_hrv").is_not_null())
            if not non_null_hrv.is_empty():
                max_hrv = float(non_null_hrv["avg_hrv"].max())  # type: ignore[arg-type]
                hrv_row = non_null_hrv.filter(pl.col("avg_hrv") == max_hrv).head(1)
                bests.append(
                    PersonalBest(
                        metric="avg_hrv",
                        value=max_hrv,
                        day=hrv_row["day"][0],
                    )
                )

        # Min lowest_hr
        if "lowest_hr" in long_sleep.columns:
            non_null_hr = long_sleep.filter(pl.col("lowest_hr").is_not_null())
            if not non_null_hr.is_empty():
                min_hr = float(non_null_hr["lowest_hr"].min())  # type: ignore[arg-type]
                hr_row = non_null_hr.filter(pl.col("lowest_hr") == min_hr).head(1)
                bests.append(
                    PersonalBest(
                        metric="lowest_hr",
                        value=min_hr,
                        day=hr_row["day"][0],
                    )
                )

    return sorted(bests, key=lambda pb: pb.metric)


def identify_areas_to_improve(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> list[AreaToImprove]:
    """Flag metrics needing attention.

    Triggers:
    1. Average below METRIC_THRESHOLDS -> reason: "Below target of {threshold}"
    2. Declining trend > DECLINE_ALERT_PCT -> reason: "Declined {pct}% over period"

    Suggestions are hardcoded per metric.

    Parameters
    ----------
    wellness_df : pl.DataFrame
        Rows from fact_daily_wellness.
    sleep_df : pl.DataFrame
        Rows from fact_sleep_detail (used for HRV decline detection).

    Returns
    -------
    list[AreaToImprove]
        Deduplicated by metric (threshold trigger takes precedence).
    """
    areas: dict[str, AreaToImprove] = {}

    # Check threshold violations
    for metric, threshold in METRIC_THRESHOLDS.items():
        if metric not in wellness_df.columns:
            continue
        non_null = wellness_df.filter(pl.col(metric).is_not_null())
        if non_null.is_empty():
            continue
        avg = float(non_null[metric].mean())  # type: ignore[arg-type]
        if avg < threshold:
            suggestion = _SUGGESTIONS.get(metric, "Review this metric with your health provider")
            areas[metric] = AreaToImprove(
                metric=metric,
                reason=f"Below target of {threshold}",
                current_avg=round(avg, 2),
                suggestion=suggestion,
            )

    # Check declining trends > DECLINE_ALERT_PCT
    if not wellness_df.is_empty() and "day" in wellness_df.columns:
        days = wellness_df["day"]
        min_day = days.min()
        max_day = days.max()
        if min_day is not None and max_day is not None:
            midpoint_ordinal = (min_day.toordinal() + max_day.toordinal()) // 2
            midpoint = date.fromordinal(midpoint_ordinal)
            first_half = wellness_df.filter(pl.col("day") <= midpoint)
            second_half = wellness_df.filter(pl.col("day") > midpoint)

            all_metrics_to_check = list(WELLNESS_METRICS)

            for metric in all_metrics_to_check:
                if metric in areas:
                    continue  # threshold already takes precedence
                if metric not in wellness_df.columns:
                    continue

                first_vals = first_half.select(pl.col(metric).drop_nulls())
                second_vals = second_half.select(pl.col(metric).drop_nulls())

                if first_vals.is_empty() or second_vals.is_empty():
                    continue

                first_mean = float(first_vals[metric].mean())  # type: ignore[arg-type]
                second_mean = float(second_vals[metric].mean())  # type: ignore[arg-type]

                if first_mean == 0:
                    continue

                pct_change = ((second_mean - first_mean) / first_mean) * 100.0

                if pct_change < -DECLINE_ALERT_PCT:
                    avg = float(
                        wellness_df.filter(pl.col(metric).is_not_null())[metric].mean()  # type: ignore[arg-type]
                    )
                    suggestion = _SUGGESTIONS.get(
                        metric, "Review this metric with your health provider"
                    )
                    areas[metric] = AreaToImprove(
                        metric=metric,
                        reason=f"Declined {abs(round(pct_change, 1))}% over period",
                        current_avg=round(avg, 2),
                        suggestion=suggestion,
                    )

    return list(areas.values())


def build_sleep_summary(sleep_df: pl.DataFrame) -> SleepSummary | None:
    """Build aggregated sleep summary from sleep detail data.

    Filters to sleep_type = 'long_sleep' only (primary sleep periods).
    Duration converted from seconds to hours.
    Stage percentages computed as (stage_duration / total_sleep_duration) * 100.
    Awake percentage computed as (awake_time / time_in_bed) * 100.

    Parameters
    ----------
    sleep_df : pl.DataFrame
        Rows from fact_sleep_detail.

    Returns
    -------
    SleepSummary | None
        Aggregated summary, or None if no long_sleep records exist.
    """
    if sleep_df.is_empty():
        return None

    if "sleep_type" not in sleep_df.columns:
        return None

    long_sleep = sleep_df.filter(pl.col("sleep_type") == "long_sleep")
    if long_sleep.is_empty():
        return None

    avg_duration_sec = float(long_sleep["total_sleep_duration"].mean())  # type: ignore[arg-type]
    avg_duration_hrs = round(avg_duration_sec / 3600.0, 2)

    avg_efficiency = round(float(long_sleep["efficiency"].mean()), 2)  # type: ignore[arg-type]
    avg_hrv = round(float(long_sleep["avg_hrv"].mean()), 2)  # type: ignore[arg-type]
    avg_hr = round(float(long_sleep["avg_hr"].mean()), 2)  # type: ignore[arg-type]

    # Stage percentages relative to total_sleep_duration
    deep_pcts = (
        long_sleep["deep_sleep_duration"].cast(pl.Float64)
        / long_sleep["total_sleep_duration"].cast(pl.Float64)
        * 100.0
    )
    light_pcts = (
        long_sleep["light_sleep_duration"].cast(pl.Float64)
        / long_sleep["total_sleep_duration"].cast(pl.Float64)
        * 100.0
    )
    rem_pcts = (
        long_sleep["rem_sleep_duration"].cast(pl.Float64)
        / long_sleep["total_sleep_duration"].cast(pl.Float64)
        * 100.0
    )

    # Awake percentage relative to time_in_bed
    awake_pcts = (
        long_sleep["awake_time"].cast(pl.Float64)
        / long_sleep["time_in_bed"].cast(pl.Float64)
        * 100.0
    )

    return SleepSummary(
        avg_duration_hrs=avg_duration_hrs,
        avg_efficiency=avg_efficiency,
        avg_hrv=avg_hrv,
        avg_hr=avg_hr,
        deep_pct=round(float(deep_pcts.mean()), 2),  # type: ignore[arg-type]
        light_pct=round(float(light_pcts.mean()), 2),  # type: ignore[arg-type]
        rem_pct=round(float(rem_pcts.mean()), 2),  # type: ignore[arg-type]
        awake_pct=round(float(awake_pcts.mean()), 2),  # type: ignore[arg-type]
    )


def build_workout_summary(workout_df: pl.DataFrame) -> WorkoutSummary | None:
    """Build aggregated workout summary.

    Groups by workout_activity to compute session counts.
    Sums workout_calories across all sessions.

    Parameters
    ----------
    workout_df : pl.DataFrame
        Rows from stg_workouts.

    Returns
    -------
    WorkoutSummary | None
        Aggregated summary, or None if DataFrame is empty.
    """
    if workout_df.is_empty():
        return None

    total_count = workout_df.height

    total_calories = float(workout_df["workout_calories"].sum())  # type: ignore[arg-type]

    activity_counts = (
        workout_df.group_by("workout_activity")
        .agg(pl.len().alias("count"))
        .sort("workout_activity")
    )
    by_activity: dict[str, int] = {
        row["workout_activity"]: row["count"]
        for row in activity_counts.to_dicts()
    }

    return WorkoutSummary(
        total_count=total_count,
        total_calories=total_calories,
        by_activity=by_activity,
    )


def build_report_data(
    period_type: str,
    start_date: date,
    end_date: date,
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
    workout_df: pl.DataFrame,
) -> ReportData:
    """Orchestrate all analysis into a single ReportData object.

    Parameters
    ----------
    period_type : str
        One of "weekly" or "monthly".
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).
    wellness_df : pl.DataFrame
        Rows from fact_daily_wellness.
    sleep_df : pl.DataFrame
        Rows from fact_sleep_detail.
    workout_df : pl.DataFrame
        Rows from stg_workouts.

    Returns
    -------
    ReportData
        Complete report data ready for rendering.

    Raises
    ------
    InsufficientDataError
        If wellness_df has fewer than 2 non-null rows for both
        readiness_score and sleep_score.
    """
    # Check for sufficient data
    readiness_non_null = 0
    sleep_score_non_null = 0

    if "readiness_score" in wellness_df.columns:
        readiness_non_null = wellness_df.filter(
            pl.col("readiness_score").is_not_null()
        ).height

    if "sleep_score" in wellness_df.columns:
        sleep_score_non_null = wellness_df.filter(
            pl.col("sleep_score").is_not_null()
        ).height

    if readiness_non_null < 2 and sleep_score_non_null < 2:
        raise InsufficientDataError(
            f"Need at least 2 non-null rows for readiness_score or sleep_score, "
            f"got {readiness_non_null} and {sleep_score_non_null}"
        )

    metric_summaries = compute_metric_summaries(wellness_df, start_date, end_date)
    personal_bests = find_personal_bests(wellness_df, sleep_df)
    areas_to_improve = identify_areas_to_improve(wellness_df, sleep_df)
    sleep_summary = build_sleep_summary(sleep_df)
    workout_summary = build_workout_summary(workout_df)

    return ReportData(
        period_type=period_type,
        start_date=start_date,
        end_date=end_date,
        metric_summaries=metric_summaries,
        personal_bests=personal_bests,
        areas_to_improve=areas_to_improve,
        sleep_summary=sleep_summary,
        workout_summary=workout_summary,
    )
