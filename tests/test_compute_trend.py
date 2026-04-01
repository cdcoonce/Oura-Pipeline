"""Tests for _compute_trend helper extracted from report_analysis."""

import polars as pl
import pytest

from dagster_project.reports.report_analysis import _compute_trend


class TestComputeTrend:
    """Tests for the _compute_trend helper function."""

    def test_stable_when_values_unchanged(self) -> None:
        """Equal first and second half means -> stable with 0% change."""
        first_half = pl.DataFrame({"score": [80, 80, 80]})
        second_half = pl.DataFrame({"score": [80, 80, 80]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "stable"
        assert pct == 0.0

    def test_improving_when_second_half_higher(self) -> None:
        """Second half mean > first half mean by more than threshold -> improving."""
        first_half = pl.DataFrame({"score": [60, 60, 60]})
        second_half = pl.DataFrame({"score": [80, 80, 80]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "improving"
        assert pct > 0

    def test_declining_when_second_half_lower(self) -> None:
        """Second half mean < first half mean by more than threshold -> declining."""
        first_half = pl.DataFrame({"score": [80, 80, 80]})
        second_half = pl.DataFrame({"score": [60, 60, 60]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "declining"
        assert pct < 0

    def test_stable_when_first_half_empty(self) -> None:
        """Empty first half -> stable with 0% change."""
        first_half = pl.DataFrame({"score": pl.Series([], dtype=pl.Int64)})
        second_half = pl.DataFrame({"score": [80, 80]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "stable"
        assert pct == 0.0

    def test_stable_when_second_half_empty(self) -> None:
        """Empty second half -> stable with 0% change."""
        first_half = pl.DataFrame({"score": [80, 80]})
        second_half = pl.DataFrame({"score": pl.Series([], dtype=pl.Int64)})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "stable"
        assert pct == 0.0

    def test_stable_when_first_mean_is_zero(self) -> None:
        """First half mean of zero -> stable with 0% to avoid division by zero."""
        first_half = pl.DataFrame({"score": [0, 0, 0]})
        second_half = pl.DataFrame({"score": [80, 80]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "stable"
        assert pct == 0.0

    def test_percentage_calculation_correct(self) -> None:
        """Trend percentage should be ((second_mean - first_mean) / first_mean) * 100."""
        first_half = pl.DataFrame({"score": [100, 100]})
        second_half = pl.DataFrame({"score": [110, 110]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert pct == pytest.approx(10.0)

    def test_handles_null_values_in_data(self) -> None:
        """Null values should be dropped before computing means."""
        first_half = pl.DataFrame({"score": [80, None, 80]})
        second_half = pl.DataFrame({"score": [60, 60, None]})
        direction, pct = _compute_trend(first_half, second_half, "score")
        assert direction == "declining"
        assert pct < 0
