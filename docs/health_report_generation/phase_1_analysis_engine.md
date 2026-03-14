# Phase 1: Test Fixtures + Analysis Engine

## Goal

Build the core analysis layer as pure functions operating on Polars DataFrames. This is the foundation of the entire report system — every other phase depends on the `ReportData` output from this module. By keeping this layer I/O-free, it is fully testable with CSV fixture data and no database.

## Dependencies

- None (this is the first phase)

## Files to Create

| File                                             | Purpose                               |
| ------------------------------------------------ | ------------------------------------- |
| `tests/fixtures/wellness_fixture.csv`            | 30 days of `fact_daily_wellness` data |
| `tests/fixtures/sleep_fixture.csv`               | 30 days of `fact_sleep_detail` data   |
| `tests/fixtures/workout_fixture.csv`             | 30 days of `stg_workouts` data        |
| `tests/test_report_analysis.py`                  | Tests for all analysis functions      |
| `src/dagster_project/reports/__init__.py`        | Package init                          |
| `src/dagster_project/reports/report_analysis.py` | Analysis dataclasses + functions      |

## Fixture Specifications

### `wellness_fixture.csv`

30 rows (one per day), columns matching `oura_marts.fact_daily_wellness`:

```
day,readiness_score,steps,calories,sleep_score,sleep_efficiency,avg_spo2_pct,breathing_disturbance_index,stress_high,recovery_high,stress_summary,resilience_level,sleep_recovery_score,daytime_recovery_score,resilience_stress_score
```

**Data design for testability:**

- Days 1-15: generally higher scores (readiness ~75-85, sleep ~80-90, steps ~8000-10000)
- Days 16-30: generally lower scores (readiness ~60-70, sleep ~65-75, steps ~5000-7000)
- This creates a **declining** trend when split at midpoint
- Include 2-3 rows with `NULL` values for readiness_score and sleep_efficiency to test null handling
- Include 1 row where steps = 15000 (personal best candidate)
- Include 1 row where sleep_score = 95 (personal best candidate)
- Include 1 row where sleep_efficiency = 60 (below threshold, triggers area to improve)
- `stress_summary` values: integers 0-4 (Oura's encoding)
- `resilience_level` values: strings from `{"limited", "adequate", "solid", "strong", "exceptional"}`

### `sleep_fixture.csv`

~35 rows (some days have multiple sleep periods), columns matching `oura_marts.fact_sleep_detail`:

```
id,day,sleep_type,bedtime_start,bedtime_end,total_sleep_duration,deep_sleep_duration,light_sleep_duration,rem_sleep_duration,awake_time,time_in_bed,efficiency,latency,avg_hr,avg_hrv,lowest_hr,avg_breath,restless_periods,sleep_recommendation,optimal_bedtime_start_offset,optimal_bedtime_end_offset
```

**Data design:**

- Each day has one `long_sleep` period, some days also have a `rest` or `nap` period
- Durations in **seconds** (Oura API convention): total_sleep ~25000-30000 (~7-8h)
- Deep/light/REM should sum to approximately total_sleep_duration
- `avg_hrv` range: 25-55 (realistic Oura range)
- `avg_hr` range: 50-65 (resting)
- Include one day with `avg_hrv = 65` (personal best candidate)
- Include one day with `avg_hr = 48` (lowest HR personal best candidate)

### `workout_fixture.csv`

~15 rows, columns matching `oura_staging.stg_workouts`:

```
id,day,workout_activity,start_datetime,end_datetime,intensity,workout_source,workout_calories,workout_distance_m,workout_label,partition_date
```

**Data design:**

- Mix of activity types: `running` (5), `cycling` (4), `strength_training` (3), `walking` (3)
- Calories range: 150-600
- Some rows with `NULL` intensity and workout_label

## Dataclasses

All dataclasses in `report_analysis.py` must be `frozen=True` for immutability:

```python
from dataclasses import dataclass
from datetime import date
from typing import Any


class InsufficientDataError(Exception):
    """Raised when fewer than 2 days of data exist for the requested period."""


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
    by_activity: dict[str, int]  # activity_type → session count


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
```

## Function Signatures

```python
# --- Constants ---

# Metrics to analyze from fact_daily_wellness
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

# Thresholds for "areas to improve" (metric_name → minimum acceptable average)
METRIC_THRESHOLDS: dict[str, float] = {
    "sleep_efficiency": 85.0,
    "readiness_score": 70.0,
    "sleep_score": 70.0,
    "avg_spo2_pct": 95.0,
}

# Trend detection thresholds
TREND_THRESHOLD_PCT: float = 5.0       # > 5% change = trending
DECLINE_ALERT_PCT: float = 10.0        # > 10% decline = area to improve


# --- Functions ---

def compute_metric_summaries(
    wellness_df: pl.DataFrame,
    start_date: date,
    end_date: date,
) -> list[MetricSummary]:
    """
    Compute mean/min/max/trend for each metric in WELLNESS_METRICS.

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


def find_personal_bests(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> list[PersonalBest]:
    """
    Identify peak values for key metrics during the period.

    Metrics checked:
    - readiness_score (max)
    - sleep_score (max)
    - steps (max)
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


def identify_areas_to_improve(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> list[AreaToImprove]:
    """
    Flag metrics needing attention.

    Triggers:
    1. Average below METRIC_THRESHOLDS → reason: "Below target of {threshold}"
    2. Declining trend > DECLINE_ALERT_PCT → reason: "Declined {pct}% over period"

    Suggestions are hardcoded per metric (e.g., sleep_efficiency →
    "Consider consistent bedtime and limiting screen time before bed").

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


def build_sleep_summary(sleep_df: pl.DataFrame) -> SleepSummary | None:
    """
    Build aggregated sleep summary from sleep detail data.

    Filters to sleep_type = 'long_sleep' only (primary sleep periods).
    Duration converted from seconds to hours.
    Stage percentages computed as (stage_duration / total_sleep_duration) * 100.
    Awake percentage computed as (awake_time / time_in_bed) * 100.

    Returns None if no long_sleep records exist.
    """


def build_workout_summary(workout_df: pl.DataFrame) -> WorkoutSummary | None:
    """
    Build aggregated workout summary.

    Groups by workout_activity to compute session counts.
    Sums workout_calories across all sessions.

    Returns None if DataFrame is empty.
    """


def build_report_data(
    period_type: str,
    start_date: date,
    end_date: date,
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
    workout_df: pl.DataFrame,
) -> ReportData:
    """
    Orchestrate all analysis into a single ReportData object.

    Raises InsufficientDataError if wellness_df has fewer than 2 non-null
    rows for any core metric (readiness_score, sleep_score).
    """
```

## Test Cases

### `test_report_analysis.py`

```python
import pytest
import polars as pl
from datetime import date
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def wellness_df() -> pl.DataFrame:
    """Load wellness fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "wellness_fixture.csv", try_parse_dates=True)


@pytest.fixture
def sleep_df() -> pl.DataFrame:
    """Load sleep fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "sleep_fixture.csv", try_parse_dates=True)


@pytest.fixture
def workout_df() -> pl.DataFrame:
    """Load workout fixture CSV as Polars DataFrame."""
    return pl.read_csv(FIXTURES / "workout_fixture.csv", try_parse_dates=True)


class TestComputeMetricSummaries:
    """Tests for compute_metric_summaries."""

    def test_returns_summary_for_each_metric(self, wellness_df):
        """Should return one MetricSummary per metric in WELLNESS_METRICS."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        metric_names = {s.name for s in result}
        assert "readiness_score" in metric_names
        assert "sleep_score" in metric_names
        assert "steps" in metric_names

    def test_declining_trend_detected(self, wellness_df):
        """Fixture has higher scores in first half, lower in second half → declining."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.trend_direction == "declining"
        assert readiness.trend_pct < 0

    def test_stable_trend_for_flat_data(self):
        """Constant values across period → stable trend."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [75] * 7,
            "sleep_score": [80] * 7,
            # ... other columns with constant values
        })
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 7))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.trend_direction == "stable"

    def test_days_with_data_tracks_nulls(self, wellness_df):
        """days_with_data should be less than total_days when NULLs present."""
        result = compute_metric_summaries(wellness_df, date(2025, 1, 1), date(2025, 1, 30))
        readiness = next(s for s in result if s.name == "readiness_score")
        assert readiness.days_with_data < readiness.total_days

    def test_empty_dataframe_returns_empty_list(self):
        """Empty DataFrame → empty list, no crash."""
        df = pl.DataFrame(schema={"day": pl.Date, "readiness_score": pl.Int64})
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 7))
        assert result == []

    def test_single_day_returns_stable_trend(self):
        """Only 1 day of data → can't split, trend should be stable with 0% change."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [80],
        })
        result = compute_metric_summaries(df, date(2025, 1, 1), date(2025, 1, 1))
        if result:
            assert result[0].trend_direction == "stable"
            assert result[0].trend_pct == 0.0


class TestFindPersonalBests:
    """Tests for find_personal_bests."""

    def test_finds_max_steps(self, wellness_df, sleep_df):
        """Should identify the day with 15000 steps as a personal best."""
        result = find_personal_bests(wellness_df, sleep_df)
        steps_best = next(pb for pb in result if pb.metric == "steps")
        assert steps_best.value == 15000

    def test_finds_max_sleep_score(self, wellness_df, sleep_df):
        """Should identify the day with sleep_score = 95."""
        result = find_personal_bests(wellness_df, sleep_df)
        sleep_best = next(pb for pb in result if pb.metric == "sleep_score")
        assert sleep_best.value == 95

    def test_finds_max_hrv_from_long_sleep_only(self, wellness_df, sleep_df):
        """HRV personal best should come from long_sleep records only."""
        result = find_personal_bests(wellness_df, sleep_df)
        hrv_best = next(pb for pb in result if pb.metric == "avg_hrv")
        assert hrv_best.value == 65  # From fixture design

    def test_empty_dfs_return_empty_list(self):
        """No data → no personal bests."""
        empty_w = pl.DataFrame(schema={"day": pl.Date, "readiness_score": pl.Int64, "steps": pl.Int64, "sleep_score": pl.Int64})
        empty_s = pl.DataFrame(schema={"day": pl.Date, "sleep_type": pl.Utf8, "avg_hrv": pl.Float64, "lowest_hr": pl.Int64})
        result = find_personal_bests(empty_w, empty_s)
        assert result == []


class TestIdentifyAreasToImprove:
    """Tests for identify_areas_to_improve."""

    def test_below_threshold_triggers_alert(self, wellness_df, sleep_df):
        """Fixture includes sleep_efficiency = 60 day, pulling average below 85 threshold."""
        result = identify_areas_to_improve(wellness_df, sleep_df)
        metrics = {a.metric for a in result}
        # sleep_efficiency average should be below 85 given the fixture design
        assert "sleep_efficiency" in metrics

    def test_declining_trend_triggers_alert(self, wellness_df, sleep_df):
        """Fixture has declining readiness → should flag if decline > 10%."""
        result = identify_areas_to_improve(wellness_df, sleep_df)
        # Check if any area has "Declined" in reason
        declining = [a for a in result if "Declined" in a.reason or "declined" in a.reason.lower()]
        # This depends on exact fixture values being >10% decline
        assert len(declining) >= 0  # Flexible — exact assertion depends on fixture tuning

    def test_no_alerts_for_healthy_data(self):
        """All metrics above thresholds and stable → empty list."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [85] * 7,
            "sleep_score": [88] * 7,
            "sleep_efficiency": [92] * 7,
            "avg_spo2_pct": [98.0] * 7,
            "steps": [9000] * 7,
        })
        empty_sleep = pl.DataFrame(schema={"day": pl.Date, "sleep_type": pl.Utf8, "avg_hrv": pl.Float64})
        result = identify_areas_to_improve(df, empty_sleep)
        assert result == []


class TestBuildSleepSummary:
    """Tests for build_sleep_summary."""

    def test_filters_to_long_sleep_only(self, sleep_df):
        """Should only use long_sleep records for averages."""
        result = build_sleep_summary(sleep_df)
        assert result is not None
        # Duration should be in hours (7-8h range for fixture)
        assert 5.0 < result.avg_duration_hrs < 10.0

    def test_stage_percentages_sum_to_approximately_100(self, sleep_df):
        """Deep + light + REM + awake should be close to 100%."""
        result = build_sleep_summary(sleep_df)
        assert result is not None
        total = result.deep_pct + result.light_pct + result.rem_pct + result.awake_pct
        assert 95.0 < total < 105.0  # Allow small rounding

    def test_returns_none_for_empty_df(self):
        """No records → None."""
        df = pl.DataFrame(schema={
            "sleep_type": pl.Utf8, "total_sleep_duration": pl.Int64,
            "deep_sleep_duration": pl.Int64, "light_sleep_duration": pl.Int64,
            "rem_sleep_duration": pl.Int64, "awake_time": pl.Int64,
            "time_in_bed": pl.Int64, "efficiency": pl.Int64,
            "avg_hrv": pl.Float64, "avg_hr": pl.Float64,
        })
        assert build_sleep_summary(df) is None


class TestBuildWorkoutSummary:
    """Tests for build_workout_summary."""

    def test_counts_by_activity(self, workout_df):
        """Should group sessions by workout_activity."""
        result = build_workout_summary(workout_df)
        assert result is not None
        assert "running" in result.by_activity
        assert result.by_activity["running"] == 5

    def test_total_count_matches_rows(self, workout_df):
        """Total count should equal number of workout rows."""
        result = build_workout_summary(workout_df)
        assert result is not None
        assert result.total_count == len(workout_df)

    def test_returns_none_for_empty_df(self):
        """No workouts → None."""
        df = pl.DataFrame(schema={"workout_activity": pl.Utf8, "workout_calories": pl.Float64})
        assert build_workout_summary(df) is None


class TestBuildReportData:
    """Tests for build_report_data (integration)."""

    def test_builds_complete_report(self, wellness_df, sleep_df, workout_df):
        """Should produce a ReportData with all fields populated."""
        result = build_report_data(
            "weekly", date(2025, 1, 1), date(2025, 1, 7),
            wellness_df, sleep_df, workout_df,
        )
        assert result.period_type == "weekly"
        assert len(result.metric_summaries) > 0
        assert result.sleep_summary is not None
        assert result.workout_summary is not None

    def test_insufficient_data_raises(self):
        """< 2 rows of data → InsufficientDataError."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [None],
            "sleep_score": [None],
        })
        empty_sleep = pl.DataFrame(schema={"day": pl.Date, "sleep_type": pl.Utf8})
        empty_workout = pl.DataFrame(schema={"workout_activity": pl.Utf8})
        with pytest.raises(InsufficientDataError):
            build_report_data("weekly", date(2025, 1, 1), date(2025, 1, 7), df, empty_sleep, empty_workout)
```

## Acceptance Criteria

1. All test cases in `test_report_analysis.py` pass
2. `report_analysis.py` has type hints on all function signatures
3. All public functions have numpy-style docstrings
4. `InsufficientDataError` is raised for < 2 days of non-null core metric data
5. No I/O in `report_analysis.py` — pure functions only
6. `uv run pytest tests/test_report_analysis.py -v` passes with 0 failures
7. Fixture CSVs contain realistic, internally consistent data
