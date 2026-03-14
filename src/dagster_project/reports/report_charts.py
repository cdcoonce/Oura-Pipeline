import matplotlib
matplotlib.use("Agg")  # Must be set before importing pyplot -- required for headless/serverless

import base64
import io
import logging
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import polars as pl

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


def _format_day_label(d) -> str:
    """Format a date as 'Mon 1/5' style label."""
    return d.strftime("%a %-m/%-d")


def _apply_chart_style(ax: plt.Axes) -> None:
    """
    Apply shared chart styling.

    White background, y-axis gridlines only (light gray), no title.
    """
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")
    ax.yaxis.grid(True, color="#E0E0E0", linewidth=0.5)
    ax.xaxis.grid(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _set_date_ticks(ax: plt.Axes, dates: list, labels: list) -> None:
    """
    Set x-axis tick positions and labels.

    For >10 days, show every Nth day to avoid overlap.
    """
    n = len(dates)
    if n > 10:
        step = max(n // 7, 2)
        tick_indices = list(range(0, n, step))
        if (n - 1) not in tick_indices:
            tick_indices.append(n - 1)
        ax.set_xticks([dates[i] for i in tick_indices])
        ax.set_xticklabels([labels[i] for i in tick_indices], rotation=45, ha="right", fontsize=8)
    else:
        ax.set_xticks(dates)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)


def _fig_to_base64(fig: plt.Figure) -> str:
    """
    Convert a matplotlib Figure to a base64-encoded PNG string.

    Writes the figure to an in-memory bytes buffer, encodes as base64,
    closes the figure to free memory, and returns the encoded string.

    Parameters
    ----------
    fig : plt.Figure
        The matplotlib figure to convert.

    Returns
    -------
    str
        Base64-encoded PNG string.
    """
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_daily_scores_chart(wellness_df: pl.DataFrame) -> Optional[str]:
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
    if wellness_df.height < 2:
        return None

    readiness_all_null = wellness_df["readiness_score"].is_null().all()
    sleep_all_null = wellness_df["sleep_score"].is_null().all()
    if readiness_all_null and sleep_all_null:
        return None

    days = wellness_df["day"].to_list()
    labels = [_format_day_label(d) for d in days]
    x_positions = list(range(len(days)))

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    _apply_chart_style(ax)

    # Plot readiness score, skipping nulls
    readiness = wellness_df["readiness_score"].to_list()
    valid_r = [(x, v) for x, v in zip(x_positions, readiness) if v is not None]
    if valid_r:
        rx, rv = zip(*valid_r)
        ax.plot(rx, rv, color=COLOR_READINESS, marker="o", markersize=4,
                linewidth=1.5, label="Readiness")

    # Plot sleep score, skipping nulls
    sleep = wellness_df["sleep_score"].to_list()
    valid_s = [(x, v) for x, v in zip(x_positions, sleep) if v is not None]
    if valid_s:
        sx, sv = zip(*valid_s)
        ax.plot(sx, sv, color=COLOR_SLEEP, marker="o", markersize=4,
                linewidth=1.5, label="Sleep")

    ax.set_ylim(0, 100)
    _set_date_ticks(ax, x_positions, labels)
    ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()

    return _fig_to_base64(fig)


def generate_steps_chart(
    wellness_df: pl.DataFrame,
    target_steps: int = 8000,
) -> Optional[str]:
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
    steps = wellness_df["steps"].to_list()
    if all(v is None for v in steps):
        return None

    days = wellness_df["day"].to_list()
    labels = [_format_day_label(d) for d in days]
    x_positions = list(range(len(days)))

    # Replace None with 0 for bar rendering
    step_values = [v if v is not None else 0 for v in steps]
    alphas = [1.0 if v >= target_steps else 0.6 for v in step_values]

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    _apply_chart_style(ax)

    bars = ax.bar(x_positions, step_values, color=COLOR_STEPS, width=0.6)
    for bar, alpha in zip(bars, alphas):
        bar.set_alpha(alpha)

    ax.axhline(y=target_steps, color=COLOR_STEPS_TARGET, linestyle="--",
               linewidth=1.5, label=f"Target ({target_steps:,})")

    _set_date_ticks(ax, x_positions, labels)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()

    return _fig_to_base64(fig)


def generate_sleep_stages_chart(sleep_df: pl.DataFrame) -> Optional[str]:
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
    long_sleep = sleep_df.filter(pl.col("sleep_type") == "long_sleep")
    if long_sleep.height == 0:
        return None

    days = long_sleep["day"].to_list()
    labels = [_format_day_label(d) for d in days]
    x_positions = list(range(len(days)))

    # Convert seconds to hours
    deep = [v / 3600 if v is not None else 0 for v in long_sleep["deep_sleep_duration"].to_list()]
    light = [v / 3600 if v is not None else 0 for v in long_sleep["light_sleep_duration"].to_list()]
    rem = [v / 3600 if v is not None else 0 for v in long_sleep["rem_sleep_duration"].to_list()]
    awake = [v / 3600 if v is not None else 0 for v in long_sleep["awake_time"].to_list()]

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    _apply_chart_style(ax)

    ax.bar(x_positions, deep, width=0.6, color=COLOR_DEEP, label="Deep")
    bottom_light = deep
    ax.bar(x_positions, light, width=0.6, color=COLOR_LIGHT, label="Light",
           bottom=bottom_light)
    bottom_rem = [d + l for d, l in zip(deep, light)]
    ax.bar(x_positions, rem, width=0.6, color=COLOR_REM, label="REM",
           bottom=bottom_rem)
    bottom_awake = [d + l + r for d, l, r in zip(deep, light, rem)]
    ax.bar(x_positions, awake, width=0.6, color=COLOR_AWAKE, label="Awake",
           bottom=bottom_awake)

    ax.set_ylabel("Hours", fontsize=9)
    _set_date_ticks(ax, x_positions, labels)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()

    return _fig_to_base64(fig)


def generate_hrv_trend_chart(sleep_df: pl.DataFrame) -> Optional[str]:
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
    long_sleep = sleep_df.filter(pl.col("sleep_type") == "long_sleep")
    # Filter out null HRV values
    valid_hrv = long_sleep.filter(pl.col("avg_hrv").is_not_null())
    if valid_hrv.height < 2:
        return None

    valid_hrv = valid_hrv.sort("day")
    days = valid_hrv["day"].to_list()
    labels = [_format_day_label(d) for d in days]
    x_positions = list(range(len(days)))
    hrv_values = valid_hrv["avg_hrv"].to_list()

    # Compute 3-day rolling average
    rolling_avg = []
    for i in range(len(hrv_values)):
        window_start = max(0, i - 2)
        window = hrv_values[window_start:i + 1]
        rolling_avg.append(sum(window) / len(window))

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    _apply_chart_style(ax)

    ax.plot(x_positions, hrv_values, color=COLOR_HRV, marker="o", markersize=4,
            linewidth=1.0, alpha=0.5, label="Nightly HRV")
    ax.plot(x_positions, rolling_avg, color=COLOR_HRV_AVG, linewidth=2.0,
            label="3-day Average")

    ax.set_ylabel("HRV (ms)", fontsize=9)
    _set_date_ticks(ax, x_positions, labels)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    return _fig_to_base64(fig)


def generate_all_charts(
    wellness_df: pl.DataFrame,
    sleep_df: pl.DataFrame,
) -> dict[str, Optional[str]]:
    """
    Generate all four charts, catching failures individually.

    Each chart is wrapped in a try/except. If a chart fails, its value
    is None and a warning is logged -- other charts still generate.

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
    results: dict[str, Optional[str]] = {
        "daily_scores": None,
        "steps": None,
        "sleep_stages": None,
        "hrv_trend": None,
    }

    try:
        results["daily_scores"] = generate_daily_scores_chart(wellness_df)
    except Exception:
        logger.warning("Failed to generate daily scores chart", exc_info=True)

    try:
        results["steps"] = generate_steps_chart(wellness_df)
    except Exception:
        logger.warning("Failed to generate steps chart", exc_info=True)

    try:
        results["sleep_stages"] = generate_sleep_stages_chart(sleep_df)
    except Exception:
        logger.warning("Failed to generate sleep stages chart", exc_info=True)

    try:
        results["hrv_trend"] = generate_hrv_trend_chart(sleep_df)
    except Exception:
        logger.warning("Failed to generate HRV trend chart", exc_info=True)

    return results
