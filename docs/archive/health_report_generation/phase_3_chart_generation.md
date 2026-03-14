# Phase 3: Chart Generation

## Goal

Build matplotlib chart functions that generate embedded PNG images as base64 strings. These charts are the visual core of the email report — daily score trends, step counts, sleep stage breakdowns, and HRV trends. Each function must handle empty/sparse data gracefully and work in headless (serverless) environments.

## Dependencies

- Phase 1 complete (uses Polars DataFrames with the same schemas as analysis fixtures)

## Files to Create

| File                                           | Purpose                                 |
| ---------------------------------------------- | --------------------------------------- |
| `tests/test_report_charts.py`                  | Tests for chart generation functions    |
| `src/dagster_project/reports/report_charts.py` | matplotlib chart → base64 PNG functions |

## Modified Files

| File             | Change                                |
| ---------------- | ------------------------------------- |
| `pyproject.toml` | Add `matplotlib>=3.8` to dependencies |

## Function Signatures

```python
import polars as pl
import matplotlib
matplotlib.use("Agg")  # Must be set before importing pyplot — required for headless/serverless

import base64
import io
import logging
from typing import Optional

import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# Shared style constants
FIGURE_DPI: int = 150
FIGURE_WIDTH: float = 8.0
FIGURE_HEIGHT: float = 3.5
COLOR_READINESS: str = "#4A90D9"
COLOR_SLEEP: str = "#7B68EE"
COLOR_STEPS: str = "#50C878"
COLOR_STEPS_TARGET: str = "#FF6B6B"
COLOR_DEEP: str = "#1a237e"
COLOR_LIGHT: str = "#64b5f6"
COLOR_REM: str = "#9c27b0"
COLOR_AWAKE: str = "#ff9800"
COLOR_HRV: str = "#00897b"
COLOR_HRV_AVG: str = "#e91e63"


def _fig_to_base64(fig: plt.Figure) -> str:
    """
    Convert a matplotlib Figure to a base64-encoded PNG string.

    Writes the figure to an in-memory bytes buffer, encodes as base64,
    closes the figure to free memory, and returns the encoded string.
    """


def generate_daily_scores_chart(wellness_df: pl.DataFrame) -> str | None:
    """
    Line chart with readiness_score and sleep_score overlaid.

    X-axis: day (formatted as "Mon 1/5", "Tue 1/6", etc.)
    Y-axis: score (0-100 range)
    Two lines: readiness (blue) and sleep (purple) with legend.

    Parameters
    ----------
    wellness_df : pl.DataFrame
        Must contain columns: day, readiness_score, sleep_score.

    Returns
    -------
    str | None
        Base64-encoded PNG string, or None if DataFrame has < 2 rows
        or both score columns are entirely null.
    """


def generate_steps_chart(
    wellness_df: pl.DataFrame,
    target_steps: int = 8000,
) -> str | None:
    """
    Bar chart of daily steps with a horizontal target line.

    X-axis: day (formatted as "Mon 1/5", etc.)
    Y-axis: steps
    Bars colored green, target line colored red and dashed.
    Bars above target get full opacity, below target get 0.6 opacity.

    Parameters
    ----------
    wellness_df : pl.DataFrame
        Must contain columns: day, steps.
    target_steps : int
        Daily step target (default 8000). Rendered as horizontal dashed line.

    Returns
    -------
    str | None
        Base64-encoded PNG, or None if no step data.
    """


def generate_sleep_stages_chart(sleep_df: pl.DataFrame) -> str | None:
    """
    Stacked bar chart showing sleep stage breakdown per night.

    Filters to sleep_type = 'long_sleep' only.
    X-axis: day
    Y-axis: hours
    Stacked segments: deep (navy), light (blue), REM (purple), awake (orange).

    Durations converted from seconds to hours for display.

    Parameters
    ----------
    sleep_df : pl.DataFrame
        Must contain columns: day, sleep_type, deep_sleep_duration,
        light_sleep_duration, rem_sleep_duration, awake_time.

    Returns
    -------
    str | None
        Base64-encoded PNG, or None if no long_sleep records.
    """


def generate_hrv_trend_chart(sleep_df: pl.DataFrame) -> str | None:
    """
    Line chart of nightly HRV with 3-day rolling average overlay.

    Filters to sleep_type = 'long_sleep' only.
    X-axis: day
    Y-axis: avg_hrv (ms)
    Raw HRV as semi-transparent line, rolling average as bold line.

    Parameters
    ----------
    sleep_df : pl.DataFrame
        Must contain columns: day, sleep_type, avg_hrv.

    Returns
    -------
    str | None
        Base64-encoded PNG, or None if < 2 long_sleep records with avg_hrv.
    """


def generate_all_charts(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> dict[str, str | None]:
    """
    Generate all four charts, catching failures individually.

    Each chart is wrapped in a try/except. If a chart fails, its value
    is None and a warning is logged — other charts still generate.

    Parameters
    ----------
    wellness_df : pl.DataFrame
        From fetch_wellness_for_period.
    sleep_df : pl.DataFrame
        From fetch_sleep_detail_for_period.

    Returns
    -------
    dict[str, str | None]
        Keys: "daily_scores", "steps", "sleep_stages", "hrv_trend".
        Values: base64 PNG string or None if that chart failed.
    """
```

## Implementation Notes

### Headless Rendering

`matplotlib.use("Agg")` **must** be called before any `import matplotlib.pyplot`. This is required for Dagster Cloud Serverless (no display server). Place it at the module top level.

### Memory Management

Every chart function must call `plt.close(fig)` after encoding to prevent memory leaks. The `_fig_to_base64` helper handles this:

```python
def _fig_to_base64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")
```

### Date Formatting

Use `matplotlib.dates.DateFormatter` for x-axis labels. For weekly reports (7 days), show each day. For monthly reports (28-31 days), show every 3rd or 5th day to avoid overlap. The functions receive the DataFrame and can determine density from row count.

### Chart Styling

Keep it clean and readable in email clients:

- White background
- Minimal gridlines (y-axis only, light gray)
- No title (the HTML template provides section headers)
- Legend inside the chart area (upper left or lower right)
- Tight layout (`bbox_inches="tight"`)

## Test Cases

### `test_report_charts.py`

```python
import pytest
import polars as pl
import base64
from datetime import date

from dagster_project.reports.report_charts import (
    generate_daily_scores_chart,
    generate_steps_chart,
    generate_sleep_stages_chart,
    generate_hrv_trend_chart,
    generate_all_charts,
)


@pytest.fixture
def wellness_df() -> pl.DataFrame:
    """7 days of wellness data for chart testing."""
    return pl.DataFrame({
        "day": [date(2025, 1, i) for i in range(1, 8)],
        "readiness_score": [75, 80, 72, 85, 78, 70, 82],
        "sleep_score": [82, 78, 85, 80, 76, 88, 79],
        "steps": [8500, 6200, 9100, 7800, 10200, 5500, 8800],
        "calories": [2100, 1900, 2300, 2000, 2400, 1800, 2200],
    })


@pytest.fixture
def sleep_df() -> pl.DataFrame:
    """7 days of sleep data for chart testing."""
    return pl.DataFrame({
        "day": [date(2025, 1, i) for i in range(1, 8)],
        "sleep_type": ["long_sleep"] * 7,
        "deep_sleep_duration": [5400, 4800, 6000, 5100, 4500, 5700, 5400],
        "light_sleep_duration": [14400, 15000, 13800, 14400, 15600, 14100, 14400],
        "rem_sleep_duration": [7200, 6600, 7800, 7200, 6000, 7500, 7200],
        "awake_time": [1800, 2400, 1200, 1800, 3000, 1500, 1800],
        "avg_hrv": [35.0, 42.0, 38.0, 45.0, 32.0, 48.0, 40.0],
    })


class TestGenerateDailyScoresChart:
    """Tests for generate_daily_scores_chart."""

    def test_returns_valid_base64_png(self, wellness_df):
        """Output should be a valid base64-encoded PNG."""
        result = generate_daily_scores_chart(wellness_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"  # PNG magic bytes

    def test_returns_none_for_single_row(self):
        """< 2 rows → None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "readiness_score": [80],
            "sleep_score": [75],
        })
        assert generate_daily_scores_chart(df) is None

    def test_returns_none_for_empty_df(self):
        """Empty DataFrame → None."""
        df = pl.DataFrame(schema={
            "day": pl.Date,
            "readiness_score": pl.Int64,
            "sleep_score": pl.Int64,
        })
        assert generate_daily_scores_chart(df) is None

    def test_handles_null_scores(self):
        """Some null values should not crash — chart renders available data."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 6)],
            "readiness_score": [75, None, 80, None, 85],
            "sleep_score": [80, 82, None, 78, 85],
        })
        result = generate_daily_scores_chart(df)
        assert result is not None


class TestGenerateStepsChart:
    """Tests for generate_steps_chart."""

    def test_returns_valid_base64_png(self, wellness_df):
        """Should produce valid PNG."""
        result = generate_steps_chart(wellness_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_custom_target(self, wellness_df):
        """Should accept custom target_steps parameter without error."""
        result = generate_steps_chart(wellness_df, target_steps=10000)
        assert result is not None

    def test_returns_none_for_no_step_data(self):
        """All null steps → None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 4)],
            "steps": [None, None, None],
        })
        assert generate_steps_chart(df) is None


class TestGenerateSleepStagesChart:
    """Tests for generate_sleep_stages_chart."""

    def test_returns_valid_base64_png(self, sleep_df):
        """Should produce valid PNG."""
        result = generate_sleep_stages_chart(sleep_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_filters_to_long_sleep(self):
        """Should ignore nap/rest records."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2)],
            "sleep_type": ["long_sleep", "nap", "long_sleep"],
            "deep_sleep_duration": [5400, 1200, 5400],
            "light_sleep_duration": [14400, 2400, 14400],
            "rem_sleep_duration": [7200, 600, 7200],
            "awake_time": [1800, 300, 1800],
        })
        result = generate_sleep_stages_chart(df)
        assert result is not None  # Should render 2 bars, not 3

    def test_returns_none_for_no_long_sleep(self):
        """Only nap records → None."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "sleep_type": ["nap"],
            "deep_sleep_duration": [1200],
            "light_sleep_duration": [2400],
            "rem_sleep_duration": [600],
            "awake_time": [300],
        })
        assert generate_sleep_stages_chart(df) is None


class TestGenerateHrvTrendChart:
    """Tests for generate_hrv_trend_chart."""

    def test_returns_valid_base64_png(self, sleep_df):
        """Should produce valid PNG with rolling average."""
        result = generate_hrv_trend_chart(sleep_df)
        assert result is not None
        decoded = base64.b64decode(result)
        assert decoded[:8] == b"\x89PNG\r\n\x1a\n"

    def test_returns_none_for_single_record(self):
        """< 2 records → None (can't draw a trend)."""
        df = pl.DataFrame({
            "day": [date(2025, 1, 1)],
            "sleep_type": ["long_sleep"],
            "avg_hrv": [40.0],
        })
        assert generate_hrv_trend_chart(df) is None


class TestGenerateAllCharts:
    """Tests for generate_all_charts."""

    def test_returns_all_four_keys(self, wellness_df, sleep_df):
        """Should return dict with all four chart keys."""
        result = generate_all_charts(wellness_df, sleep_df)
        assert set(result.keys()) == {"daily_scores", "steps", "sleep_stages", "hrv_trend"}

    def test_all_charts_are_valid_or_none(self, wellness_df, sleep_df):
        """Each value should be a base64 string or None."""
        result = generate_all_charts(wellness_df, sleep_df)
        for key, value in result.items():
            if value is not None:
                decoded = base64.b64decode(value)
                assert decoded[:8] == b"\x89PNG\r\n\x1a\n", f"{key} is not a valid PNG"

    def test_individual_failure_does_not_crash_others(self):
        """If one chart's data is bad, others should still generate."""
        wellness = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "readiness_score": [75, 80, 72, 85, 78, 70, 82],
            "sleep_score": [82, 78, 85, 80, 76, 88, 79],
            "steps": [None] * 7,  # Steps chart will return None
            "calories": [2000] * 7,
        })
        sleep = pl.DataFrame({
            "day": [date(2025, 1, i) for i in range(1, 8)],
            "sleep_type": ["long_sleep"] * 7,
            "deep_sleep_duration": [5400] * 7,
            "light_sleep_duration": [14400] * 7,
            "rem_sleep_duration": [7200] * 7,
            "awake_time": [1800] * 7,
            "avg_hrv": [40.0] * 7,
        })
        result = generate_all_charts(wellness, sleep)
        # Steps should be None, but others should succeed
        assert result["steps"] is None
        assert result["daily_scores"] is not None
```

## Acceptance Criteria

1. All tests in `test_report_charts.py` pass
2. `matplotlib.use("Agg")` set at module top level (before pyplot import)
3. Every chart function calls `plt.close(fig)` (via `_fig_to_base64` helper)
4. Each function returns `None` (not raises) for insufficient data
5. `generate_all_charts` never crashes — individual failures are caught and logged
6. Generated PNGs are valid (PNG magic bytes header check)
7. Chart dimensions and DPI produce clean images at email-friendly sizes (~600px wide)
8. Type hints on all function signatures, numpy-style docstrings
9. `matplotlib>=3.8` added to `pyproject.toml`
10. `uv run pytest tests/test_report_charts.py -v` passes with 0 failures
