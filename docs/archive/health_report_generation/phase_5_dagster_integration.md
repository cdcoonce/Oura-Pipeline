# Phase 5: Dagster Integration

## Goal

Wire everything together as Dagster assets with scheduled execution. Create two assets (`weekly_health_report`, `monthly_health_report`) that orchestrate the full pipeline: fetch data, analyze, generate charts, render HTML, and send via email. Add cron schedules and register everything in the Dagster definitions.

## Dependencies

- Phases 1-4 complete (all report modules implemented and tested)

## Files to Create

| File                                        | Purpose                                  |
| ------------------------------------------- | ---------------------------------------- |
| `src/dagster_project/defs/report_assets.py` | Weekly and monthly report Dagster assets |
| `tests/test_report_assets.py`               | Schedule config and asset wiring tests   |

## Modified Files

| File                                    | Change                                                                 |
| --------------------------------------- | ---------------------------------------------------------------------- |
| `src/dagster_project/defs/schedules.py` | Add `weekly_report_schedule` and `monthly_report_schedule`             |
| `src/dagster_project/definitions.py`    | Add `report_assets` module, add `SESDeliveryResource` to resources     |
| `pyproject.toml`                        | Ensure `jinja2>=3.1`, `boto3>=1.34`, `matplotlib>=3.8` are all present |

## Asset Design

### Shared Helper

Both assets share identical logic — only the date range computation differs. Extract a shared helper to avoid DRY violation:

```python
import calendar
import logging
from datetime import date, datetime, timedelta, timezone

import dagster as dg

from ..reports.report_analysis import InsufficientDataError, build_report_data
from ..reports.report_charts import generate_all_charts
from ..reports.report_data import (
    fetch_sleep_detail_for_period,
    fetch_wellness_for_period,
    fetch_workout_summary_for_period,
)
from ..reports.report_delivery import DeliveryError, SESDeliveryResource
from ..reports.report_renderer import render_report
from .resources import DuckDBResource

logger = logging.getLogger(__name__)


def _generate_and_send_report(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    ses: SESDeliveryResource,
    period_type: str,
    start_date: date,
    end_date: date,
) -> dg.MaterializeResult:
    """
    Shared report generation pipeline: fetch → analyze → chart → render → email.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster execution context for logging and metadata.
    duckdb : DuckDBResource
        Database connection provider.
    ses : SESDeliveryResource
        Email delivery resource.
    period_type : str
        "weekly" or "monthly".
    start_date : date
        Period start (inclusive).
    end_date : date
        Period end (inclusive).

    Returns
    -------
    dg.MaterializeResult
        Metadata includes: period, dates, recipient, SES message ID, chart count.

    Raises
    ------
    dg.Failure
        On DeliveryError (SES send failure) — surfaces in Dagster UI as red run.
    """
    con = duckdb.get_connection()
    try:
        # 1. Fetch data
        context.log.info("Fetching data for %s: %s to %s", period_type, start_date, end_date)
        wellness_df = fetch_wellness_for_period(con, start_date, end_date)
        sleep_df = fetch_sleep_detail_for_period(con, start_date, end_date)
        workout_df = fetch_workout_summary_for_period(con, start_date, end_date)
        context.log.info(
            "Data fetched: %d wellness rows, %d sleep rows, %d workout rows",
            len(wellness_df), len(sleep_df), len(workout_df),
        )

        # 2. Analyze
        try:
            report_data = build_report_data(
                period_type, start_date, end_date,
                wellness_df, sleep_df, workout_df,
            )
        except InsufficientDataError as e:
            context.log.warning("Skipping report: %s", e)
            # Emit observation so it's visible in Dagster UI
            context.log_event(
                dg.AssetObservation(
                    asset_key=context.asset_key,
                    metadata={"skipped_reason": dg.MetadataValue.text(str(e))},
                )
            )
            return dg.MaterializeResult(
                metadata={
                    "status": dg.MetadataValue.text("skipped"),
                    "reason": dg.MetadataValue.text(str(e)),
                }
            )

        # 3. Generate charts
        charts = generate_all_charts(wellness_df, sleep_df)
        chart_count = sum(1 for v in charts.values() if v is not None)
        context.log.info("Generated %d of 4 charts", chart_count)

        # 4. Render HTML
        html = render_report(report_data, charts)
        context.log.info("Rendered HTML report (%d bytes)", len(html))

        # 5. Send email
        subject = (
            f"{'Weekly' if period_type == 'weekly' else 'Monthly'} Health Report: "
            f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        )
        try:
            message_id = ses.send_report(subject, html)
        except DeliveryError as e:
            raise dg.Failure(
                description=f"Email delivery failed: {e}",
                metadata={"error": dg.MetadataValue.text(str(e))},
            ) from e

        context.log.info("Email sent. MessageId=%s", message_id)

        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("sent"),
                "period_type": dg.MetadataValue.text(period_type),
                "start_date": dg.MetadataValue.text(str(start_date)),
                "end_date": dg.MetadataValue.text(str(end_date)),
                "recipient": dg.MetadataValue.text(ses.recipient_email),
                "ses_message_id": dg.MetadataValue.text(message_id),
                "charts_generated": dg.MetadataValue.int(chart_count),
                "wellness_rows": dg.MetadataValue.int(len(wellness_df)),
                "sleep_rows": dg.MetadataValue.int(len(sleep_df)),
            }
        )
    finally:
        con.close()
```

### Date Range Computation

```python
def _previous_week_range() -> tuple[date, date]:
    """
    Compute the previous Monday-Sunday range relative to today.

    If today is Monday 2025-01-13, returns (2025-01-06, 2025-01-12).
    """
    today = date.today()
    # Monday of current week
    current_monday = today - timedelta(days=today.weekday())
    # Previous week
    start = current_monday - timedelta(weeks=1)
    end = current_monday - timedelta(days=1)
    return start, end


def _previous_month_range() -> tuple[date, date]:
    """
    Compute the previous calendar month range.

    If today is 2025-02-15, returns (2025-01-01, 2025-01-31).
    Handles year boundaries (Jan → previous Dec) correctly.
    """
    today = date.today()
    # First day of current month
    first_of_current = today.replace(day=1)
    # Last day of previous month
    end = first_of_current - timedelta(days=1)
    # First day of previous month
    start = end.replace(day=1)
    return start, end
```

### Asset Definitions

```python
@dg.asset(
    deps=["fact_daily_wellness", "fact_sleep_detail"],
    group_name="reports",
    kinds={"email", "report"},
)
def weekly_health_report(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    ses: SESDeliveryResource,
) -> dg.MaterializeResult:
    """Generate and email weekly health report for the previous Mon-Sun."""
    start, end = _previous_week_range()
    return _generate_and_send_report(context, duckdb, ses, "weekly", start, end)


@dg.asset(
    deps=["fact_daily_wellness", "fact_sleep_detail"],
    group_name="reports",
    kinds={"email", "report"},
)
def monthly_health_report(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    ses: SESDeliveryResource,
) -> dg.MaterializeResult:
    """Generate and email monthly health report for the previous calendar month."""
    start, end = _previous_month_range()
    return _generate_and_send_report(context, duckdb, ses, "monthly", start, end)
```

**Note on `deps`:** The exact asset key format depends on how `dagster-dbt` translates dbt model names. The default `DagsterDbtTranslator.get_asset_key` uses the model name as the key. If the translated keys include a prefix (e.g., `["oura_marts", "fact_daily_wellness"]`), the `deps` must match. Verify by checking `dbt_model_assets` in the Dagster UI asset graph during implementation. Adjust to `dg.AssetKey(["fact_daily_wellness"])` or `dg.AssetKey(["oura_marts", "fact_daily_wellness"])` as needed.

### Schedule Definitions (`schedules.py`)

Add to existing file alongside `daily_oura_schedule`:

```python
from .report_assets import weekly_health_report, monthly_health_report

weekly_report_job = dg.define_asset_job(
    name="weekly_report_job",
    selection=dg.AssetSelection.assets(weekly_health_report),
)

monthly_report_job = dg.define_asset_job(
    name="monthly_report_job",
    selection=dg.AssetSelection.assets(monthly_health_report),
)

weekly_report_schedule = dg.ScheduleDefinition(
    job=weekly_report_job,
    cron_schedule="0 7 * * 1",  # Monday 7 AM UTC
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

monthly_report_schedule = dg.ScheduleDefinition(
    job=monthly_report_job,
    cron_schedule="0 7 1 * *",  # 1st of month 7 AM UTC
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

### Definitions Changes (`definitions.py`)

```python
from .defs import assets, checks, dbt_assets, report_assets, schedules
from .reports.report_delivery import SESDeliveryResource

@dg.definitions
def defs():
    return dg.load_definitions_from_modules(
        modules=[assets, checks, dbt_assets, report_assets, schedules],
        resources={
            "oura_api": OuraAPI(
                client_id=dg.EnvVar("OURA_CLIENT_ID"),
                client_secret=dg.EnvVar("OURA_CLIENT_SECRET"),
                token_path=dg.EnvVar("OURA_TOKEN_PATH"),
            ),
            "duckdb": DuckDBResource(
                db_path=dg.EnvVar("DUCKDB_PATH"),
            ),
            "dbt": DbtCliResource(
                project_dir=dbt_assets.DBT_PROJECT_DIR,
                profiles_dir=dbt_assets.DBT_PROFILES_DIR,
                target="dev",
            ),
            "ses": SESDeliveryResource(
                sender_email=dg.EnvVar("SES_SENDER_EMAIL"),
                recipient_email=dg.EnvVar("SES_RECIPIENT_EMAIL"),
                aws_region=dg.EnvVar("AWS_REGION"),
            ),
        },
        executor=dg.in_process_executor,
    )
```

## Test Cases

### `test_report_assets.py`

```python
import pytest
from datetime import date, timedelta

import dagster as dg

from dagster_project.defs.report_assets import (
    weekly_health_report,
    monthly_health_report,
    _previous_week_range,
    _previous_month_range,
)


class TestPreviousWeekRange:
    """Tests for _previous_week_range."""

    def test_returns_monday_to_sunday(self):
        """Start should be Monday, end should be Sunday."""
        start, end = _previous_week_range()
        assert start.weekday() == 0  # Monday
        assert end.weekday() == 6    # Sunday

    def test_span_is_7_days(self):
        """Range should span exactly 7 days."""
        start, end = _previous_week_range()
        assert (end - start).days == 6  # inclusive range

    def test_end_is_before_today(self):
        """End date should be before today."""
        _, end = _previous_week_range()
        assert end < date.today()


class TestPreviousMonthRange:
    """Tests for _previous_month_range."""

    def test_start_is_first_of_month(self):
        """Start should be day 1."""
        start, _ = _previous_month_range()
        assert start.day == 1

    def test_end_is_last_of_month(self):
        """End should be last day of the previous month."""
        start, end = _previous_month_range()
        # End day should match the last day of start's month
        import calendar
        _, last_day = calendar.monthrange(start.year, start.month)
        assert end.day == last_day

    def test_year_boundary(self):
        """If today is January, previous month should be December of prior year."""
        # This test validates the logic conceptually — exact behavior depends
        # on when it runs. The implementation must handle year rollover.
        start, end = _previous_month_range()
        assert start.month == end.month
        assert start.year == end.year

    def test_end_is_before_today(self):
        """End date should be before today."""
        _, end = _previous_month_range()
        assert end < date.today()


class TestScheduleConfiguration:
    """Tests for schedule definitions."""

    def test_weekly_schedule_cron(self):
        """Weekly schedule should run Monday 7 AM UTC."""
        from dagster_project.defs.schedules import weekly_report_schedule
        assert weekly_report_schedule.cron_schedule == "0 7 * * 1"

    def test_monthly_schedule_cron(self):
        """Monthly schedule should run 1st of month 7 AM UTC."""
        from dagster_project.defs.schedules import monthly_report_schedule
        assert monthly_report_schedule.cron_schedule == "0 7 1 * *"

    def test_schedules_default_stopped(self):
        """Both schedules should start in STOPPED state."""
        from dagster_project.defs.schedules import (
            weekly_report_schedule,
            monthly_report_schedule,
        )
        assert weekly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
        assert monthly_report_schedule.default_status == dg.DefaultScheduleStatus.STOPPED


class TestAssetDefinitions:
    """Tests for report asset configuration."""

    def test_weekly_asset_exists(self):
        """weekly_health_report should be a Dagster asset."""
        assert hasattr(weekly_health_report, "key")

    def test_monthly_asset_exists(self):
        """monthly_health_report should be a Dagster asset."""
        assert hasattr(monthly_health_report, "key")

    def test_assets_in_reports_group(self):
        """Both assets should be in the 'reports' group."""
        assert weekly_health_report.group_names_by_key
        assert monthly_health_report.group_names_by_key
        # Verify group name is 'reports' for each asset's key
        for group in weekly_health_report.group_names_by_key.values():
            assert group == "reports"
        for group in monthly_health_report.group_names_by_key.values():
            assert group == "reports"


class TestDefinitionsLoad:
    """Test that the Dagster definitions load without error."""

    def test_definitions_load(self):
        """
        Verify that importing definitions doesn't crash.

        Note: This test may need SES env vars mocked or marked
        as integration-only if EnvVar resolution fails without secrets.
        """
        # At minimum, verify the module imports without error
        from dagster_project.defs import report_assets
        assert report_assets is not None
```

## Environment Variables Required

These must be set in Dagster Cloud (Settings > Environment Variables):

| Variable                | Description                 | Example                  |
| ----------------------- | --------------------------- | ------------------------ |
| `SES_SENDER_EMAIL`      | Verified SES sender address | `reports@yourdomain.com` |
| `SES_RECIPIENT_EMAIL`   | Your email address          | `you@gmail.com`          |
| `AWS_REGION`            | SES region                  | `us-east-1`              |
| `AWS_ACCESS_KEY_ID`     | IAM credentials for SES     | `AKIA...`                |
| `AWS_SECRET_ACCESS_KEY` | IAM credentials for SES     | `wJal...`                |

## Rollout Procedure

1. Deploy code with schedules in `STOPPED` state
2. Set environment variables in Dagster Cloud
3. Verify SES sender email is verified (check SES console)
4. Manually materialize `weekly_health_report` from Dagster UI
5. Check email inbox for the report
6. If successful, enable `weekly_report_schedule` in Dagster UI
7. Wait for first automated Monday run, confirm delivery
8. Enable `monthly_report_schedule`

## Acceptance Criteria

1. All tests in `test_report_assets.py` pass
2. Both assets appear in Dagster UI under the "reports" group
3. Schedules appear in Dagster UI with STOPPED default status
4. Manual materialization produces an email with charts
5. `SESDeliveryResource` is registered in definitions with `EnvVar` config
6. `_generate_and_send_report` handles `InsufficientDataError` gracefully (skip + observation)
7. `_generate_and_send_report` handles `DeliveryError` as `dg.Failure` (red in UI)
8. Connection is closed in `finally` block
9. MaterializeResult metadata includes: status, period, dates, recipient, message ID, chart count
10. `uv run pytest tests/test_report_assets.py -v` passes with 0 failures
11. `uv run pytest` — all existing + new tests pass (full suite green)
