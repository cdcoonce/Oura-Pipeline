"""Dagster assets for weekly and monthly health report generation.

Orchestrates the full pipeline: fetch data, analyze, generate charts,
render HTML, and send via email. Two assets share a common helper,
differing only in date range computation.
"""

import logging
from datetime import date, timedelta

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
from .resources import SnowflakeResource

logger = logging.getLogger(__name__)


def _previous_week_range() -> tuple[date, date]:
    """Compute the previous Monday-Sunday range relative to today.

    Returns
    -------
    tuple[date, date]
        (start, end) where start is Monday and end is Sunday of the
        previous week.

    Examples
    --------
    If today is Monday 2025-01-13, returns (2025-01-06, 2025-01-12).
    """
    today = date.today()
    current_monday = today - timedelta(days=today.weekday())
    start = current_monday - timedelta(weeks=1)
    end = current_monday - timedelta(days=1)
    return start, end


def _previous_month_range() -> tuple[date, date]:
    """Compute the previous calendar month range.

    Returns
    -------
    tuple[date, date]
        (start, end) where start is the 1st and end is the last day
        of the previous calendar month.

    Examples
    --------
    If today is 2025-02-15, returns (2025-01-01, 2025-01-31).
    Handles year boundaries (Jan -> previous Dec) correctly.
    """
    today = date.today()
    first_of_current = today.replace(day=1)
    end = first_of_current - timedelta(days=1)
    start = end.replace(day=1)
    return start, end


def _generate_and_send_report(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    ses: SESDeliveryResource,
    period_type: str,
    start_date: date,
    end_date: date,
) -> dg.MaterializeResult:
    """Shared report generation pipeline: fetch -> analyze -> chart -> render -> email.

    Parameters
    ----------
    context : dg.AssetExecutionContext
        Dagster execution context for logging and metadata.
    snowflake : SnowflakeResource
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
        On DeliveryError (SES send failure) -- surfaces in Dagster UI as red run.
    """
    con = snowflake.get_connection()
    try:
        # 1. Fetch data
        context.log.info(
            "Fetching data for %s: %s to %s", period_type, start_date, end_date
        )
        wellness_df = fetch_wellness_for_period(con, start_date, end_date)
        sleep_df = fetch_sleep_detail_for_period(con, start_date, end_date)
        workout_df = fetch_workout_summary_for_period(con, start_date, end_date)
        context.log.info(
            "Data fetched: %d wellness rows, %d sleep rows, %d workout rows",
            len(wellness_df),
            len(sleep_df),
            len(workout_df),
        )

        # 2. Analyze
        try:
            report_data = build_report_data(
                period_type,
                start_date,
                end_date,
                wellness_df,
                sleep_df,
                workout_df,
            )
        except InsufficientDataError as e:
            context.log.warning("Skipping report: %s", e)
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


@dg.asset(
    deps=["fact_daily_wellness", "fact_sleep_detail"],
    group_name="reports",
    kinds={"email", "report"},
)
def weekly_health_report(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    ses: SESDeliveryResource,
) -> dg.MaterializeResult:
    """Generate and email weekly health report for the previous Mon-Sun."""
    start, end = _previous_week_range()
    return _generate_and_send_report(context, snowflake, ses, "weekly", start, end)


@dg.asset(
    deps=["fact_daily_wellness", "fact_sleep_detail"],
    group_name="reports",
    kinds={"email", "report"},
)
def monthly_health_report(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    ses: SESDeliveryResource,
) -> dg.MaterializeResult:
    """Generate and email monthly health report for the previous calendar month."""
    start, end = _previous_month_range()
    return _generate_and_send_report(context, snowflake, ses, "monthly", start, end)
