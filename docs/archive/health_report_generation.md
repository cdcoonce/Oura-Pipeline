# Plan: Weekly/Monthly Health Report Generation

## Context

The Oura pipeline ingests 13 endpoints daily into DuckDB, transforms through dbt staging/mart layers, and produces two analytics tables: `fact_daily_wellness` and `fact_sleep_detail`. Currently there's no consumer of this data. This feature auto-generates rich HTML health reports with embedded trend charts, delivered via email on weekly and monthly schedules.

**Key constraint:** The pipeline runs on Dagster Cloud Serverless (ephemeral containers) — no persistent filesystem. Reports must be delivered, not stored on disk.

## Architecture

```
┌─────────────┐    ┌────────────────┐    ┌───────────────┐    ┌────────────────┐    ┌─────────────────┐
│ report_data │───▶│ report_analysis │───▶│ report_charts │───▶│ report_renderer │───▶│ report_delivery │
│ (SQL→Polars)│    │ (pure funcs)   │    │ (matplotlib)  │    │ (Jinja2→HTML)  │    │ (SES email)     │
└─────────────┘    └────────────────┘    └───────────────┘    └────────────────┘    └─────────────────┘
```

- **Five-layer pipeline**: data fetching → analysis → chart generation → HTML rendering → email delivery
- **Jinja2 HTML templates** with embedded base64 chart images (email clients render HTML, not markdown)
- **Single template** (`report.html.j2`) with period-aware conditional blocks (avoids DRY violation)
- **Amazon SES** for email delivery (same AWS ecosystem as Dagster Cloud)
- **Unpartitioned Dagster assets** with cron schedules (weekly Monday 7AM UTC, monthly 1st 7AM UTC)
- **matplotlib** for 4 chart types: daily scores, steps, sleep stages, HRV trend

## Charts

| Chart        | Type               | Data Source           | Description                            |
| ------------ | ------------------ | --------------------- | -------------------------------------- |
| Daily Scores | Line (overlaid)    | `fact_daily_wellness` | Readiness + sleep score over period    |
| Steps        | Bar + target line  | `fact_daily_wellness` | Daily steps with configurable target   |
| Sleep Stages | Stacked bar        | `fact_sleep_detail`   | Deep/light/REM/awake per night         |
| HRV Trend    | Line + rolling avg | `fact_sleep_detail`   | Nightly HRV with 3-day rolling average |

Each chart function returns a base64-encoded PNG string. Charts that fail (empty data, unexpected shape) are skipped gracefully with a text fallback in the template.

## File Changes

### New Files

| File                                                   | Purpose                                                           |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| `src/dagster_project/reports/__init__.py`              | Package init                                                      |
| `src/dagster_project/reports/report_data.py`           | SQL queries → Polars DataFrames for a date range                  |
| `src/dagster_project/reports/report_analysis.py`       | Pure functions: trends, personal bests, areas to improve          |
| `src/dagster_project/reports/report_charts.py`         | matplotlib chart generation → base64 PNG strings                  |
| `src/dagster_project/reports/report_renderer.py`       | Jinja2 HTML rendering                                             |
| `src/dagster_project/reports/report_delivery.py`       | SES email send                                                    |
| `src/dagster_project/reports/templates/report.html.j2` | Single period-aware HTML template                                 |
| `src/dagster_project/defs/report_assets.py`            | `weekly_health_report` and `monthly_health_report` Dagster assets |
| `tests/test_report_analysis.py`                        | Analysis function tests (pure, no DB)                             |
| `tests/test_report_data.py`                            | SQL query tests (in-memory DuckDB)                                |
| `tests/test_report_charts.py`                          | Chart generation tests (valid base64 output)                      |
| `tests/test_report_renderer.py`                        | HTML rendering tests                                              |
| `tests/test_report_delivery.py`                        | SES delivery tests (mocked boto3)                                 |
| `tests/test_report_assets.py`                          | Schedule config + asset wiring tests                              |
| `tests/fixtures/wellness_fixture.csv`                  | 30 days of sample `fact_daily_wellness` data                      |
| `tests/fixtures/sleep_fixture.csv`                     | 30 days of sample `fact_sleep_detail` data                        |
| `tests/fixtures/workout_fixture.csv`                   | 30 days of sample `stg_workouts` data                             |

### Modified Files

| File                                    | Change                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------- |
| `src/dagster_project/defs/schedules.py` | Add `weekly_report_schedule` and `monthly_report_schedule`                |
| `src/dagster_project/definitions.py`    | Add `report_assets` to `load_definitions_from_modules`, add `SESResource` |
| `pyproject.toml`                        | Add `jinja2>=3.1`, `boto3>=1.34`, `matplotlib>=3.8`                       |

## Module Design

### `report_data.py` — Data Fetching

```python
def fetch_wellness_for_period(con: DuckDBPyConnection, start_date: date, end_date: date) -> pl.DataFrame
def fetch_sleep_detail_for_period(con: DuckDBPyConnection, start_date: date, end_date: date) -> pl.DataFrame
def fetch_workout_summary_for_period(con: DuckDBPyConnection, start_date: date, end_date: date) -> pl.DataFrame
```

Parameterized SQL against `oura_marts.fact_daily_wellness`, `oura_marts.fact_sleep_detail`, and `oura_staging.stg_workouts`. Returns Polars DataFrames.

### `report_analysis.py` — Analysis (Pure Functions)

**Dataclasses (all frozen):**

```python
@dataclass(frozen=True)
class MetricSummary:
    name: str
    mean: float
    min_val: float
    max_val: float
    trend_direction: str        # "improving" | "declining" | "stable"
    trend_pct: float            # % change first half vs second half
    days_with_data: int
    total_days: int

@dataclass(frozen=True)
class PersonalBest:
    metric: str
    value: float
    day: date

@dataclass(frozen=True)
class AreaToImprove:
    metric: str
    reason: str
    current_avg: float
    suggestion: str

@dataclass(frozen=True)
class SleepSummary:
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
    total_count: int
    total_calories: float
    by_activity: dict[str, int]  # activity_type → count

@dataclass(frozen=True)
class ReportData:
    period_type: str            # "weekly" | "monthly"
    start_date: date
    end_date: date
    metric_summaries: list[MetricSummary]
    personal_bests: list[PersonalBest]
    areas_to_improve: list[AreaToImprove]
    sleep_summary: SleepSummary | None
    workout_summary: WorkoutSummary | None
```

**Key functions:**

- `compute_metric_summaries(wellness_df, start_date, end_date)` — For each numeric metric: mean/min/max + trend. Trend: split period at midpoint, compare means. >5% = improving/declining, else stable. Tracks `days_with_data` vs `total_days`.
- `find_personal_bests(wellness_df, sleep_df)` — Max readiness, sleep score, steps; lowest resting HR; highest HRV
- `identify_areas_to_improve(wellness_df, sleep_df)` — Flag declining trends (>10% drop) or below-threshold averages (sleep efficiency <85%, readiness <70)
- `build_sleep_summary(sleep_df)` — Typed `SleepSummary` dataclass. Filters to `sleep_type = 'long_sleep'` for primary sleep only.
- `build_workout_summary(workout_df)` — Typed `WorkoutSummary` dataclass
- `build_report_data(...)` — Orchestrates all functions into a `ReportData`

**Custom exception:**

```python
class InsufficientDataError(Exception):
    """Raised when fewer than 2 days of data exist for the requested period."""
```

### `report_charts.py` — Chart Generation

```python
def generate_daily_scores_chart(wellness_df: pl.DataFrame) -> str | None
def generate_steps_chart(wellness_df: pl.DataFrame, target_steps: int = 8000) -> str | None
def generate_sleep_stages_chart(sleep_df: pl.DataFrame) -> str | None
def generate_hrv_trend_chart(sleep_df: pl.DataFrame) -> str | None
def generate_all_charts(wellness_df: pl.DataFrame, sleep_df: pl.DataFrame) -> dict[str, str | None]
```

Each function returns a base64-encoded PNG string or `None` on failure. `generate_all_charts` wraps all four with try/except per chart and logs warnings for failures. Uses `matplotlib.use('Agg')` for headless rendering (required in serverless).

### `report_renderer.py` — HTML Rendering

```python
def render_report(report_data: ReportData, charts: dict[str, str | None]) -> str
```

Loads `report.html.j2` template with Jinja2 (autoescaping enabled). Template has conditional blocks: `{% if report_data.period_type == 'monthly' %}` for month-specific sections (week-over-week comparison). Charts rendered as `<img src="data:image/png;base64,{{ chart }}">` with `{% if chart %}` guards.

### `report_delivery.py` — Email Delivery

```python
class SESDeliveryResource(dg.ConfigurableResource):
    sender_email: str
    recipient_email: str
    aws_region: str = "us-east-1"

    def send_report(self, subject: str, html_body: str) -> str:
        """Send HTML email via SES. Returns SES MessageId."""
```

Uses `boto3.client('ses')`. AWS credentials come from the environment (Dagster Cloud secrets or IAM role). Raises `DeliveryError` wrapping `botocore.exceptions.ClientError` with actionable message.

### Report Template Sections (`report.html.j2`)

1. **Header** — period type badge, date range, generation timestamp
2. **Key Metrics Summary** — HTML table with avg/best/worst/trend per metric, data coverage (N/M days)
3. **Trend Charts** — Daily scores chart, steps chart (with target line)
4. **Personal Bests** — bulleted list with dates
5. **Sleep Summary** — duration, efficiency, HRV, HR, stage breakdown chart
6. **HRV Trend** — HRV chart with rolling average
7. **Activity & Workouts** — count, calories, breakdown by type
8. **Areas to Improve** — flagged metrics with reasons and suggestions
9. **Week-over-Week Comparison** (monthly only) — table comparing each week's averages
10. **Footer** — "Generated by Oura Pipeline" + timestamp

### Dagster Assets (`report_assets.py`)

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
    # 1. Compute date range (previous Mon-Sun)
    # 2. Fetch data via report_data functions
    # 3. Build ReportData via report_analysis (catch InsufficientDataError → skip)
    # 4. Generate charts via report_charts
    # 5. Render HTML via report_renderer
    # 6. Send via ses.send_report()
    # 7. Return MaterializeResult with metadata (period, recipient, message_id)

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
```

Both assets share a common `_generate_and_send_report(context, con, ses, period_type, start_date, end_date)` helper to avoid DRY violation.

### Schedules (`schedules.py` additions)

```python
weekly_report_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job("weekly_report_job", selection=dg.AssetSelection.assets(weekly_health_report)),
    cron_schedule="0 7 * * 1",       # Monday 7 AM UTC
    default_status=dg.DefaultScheduleStatus.STOPPED,  # Enable manually after first successful test
)
monthly_report_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job("monthly_report_job", selection=dg.AssetSelection.assets(monthly_health_report)),
    cron_schedule="0 7 1 * *",       # 1st of month 7 AM UTC
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

### `definitions.py` Changes

```python
from .defs import assets, checks, dbt_assets, report_assets, schedules
from .reports.report_delivery import SESDeliveryResource

# Add to resources dict:
"ses": SESDeliveryResource(
    sender_email=dg.EnvVar("SES_SENDER_EMAIL"),
    recipient_email=dg.EnvVar("SES_RECIPIENT_EMAIL"),
    aws_region=dg.EnvVar("AWS_REGION"),
),
```

## Error Handling

| Failure                             | Handling                                                              | Dagster Visibility                    |
| ----------------------------------- | --------------------------------------------------------------------- | ------------------------------------- |
| **< 2 days of data**                | Raise `InsufficientDataError`, asset catches and skips send           | `AssetObservation` with reason logged |
| **Partial data (e.g., 3/7 days)**   | Report includes "N of M days" in each `MetricSummary`                 | Visible in report content             |
| **All-null metric column**          | Analysis filters nulls, reports "No data" for that metric             | Visible in report content             |
| **Chart generation failure**        | Per-chart try/except, returns `None`, template shows text fallback    | `context.log.warning()`               |
| **SES send failure**                | Catch `ClientError`, wrap as `DeliveryError`, raise Dagster `Failure` | Red in Dagster UI with error details  |
| **SES not configured**              | Validate env vars at resource init, clear error message               | Dagster resource config error         |
| **Date range edge (year boundary)** | Use `calendar.monthrange` + explicit year math                        | Covered by unit tests                 |

## AWS SES Setup (Pre-Implementation)

1. **Verify sender email** in SES console (or verify entire domain)
2. **Check sandbox status** — if in sandbox, also verify recipient email. Request production access if needed.
3. **Add Dagster Cloud secrets:**
   - `SES_SENDER_EMAIL` — verified sender address
   - `SES_RECIPIENT_EMAIL` — your email
   - `AWS_REGION` — SES region (e.g., `us-east-1`)
   - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — IAM user with `ses:SendEmail` permission (or use Dagster Cloud's IAM role if available)

## Implementation Order (TDD)

### Phase 1: Test Fixtures

1. Create `tests/fixtures/wellness_fixture.csv` — 30 days of realistic `fact_daily_wellness` data (include some null days for edge cases)
2. Create `tests/fixtures/sleep_fixture.csv` — 30 days of `fact_sleep_detail` data (multiple sleep periods per day)
3. Create `tests/fixtures/workout_fixture.csv` — 30 days of `stg_workouts` data (varying activities)

### Phase 2: Analysis Engine (Pure Functions)

4. Write `test_report_analysis.py`:
   - Test `MetricSummary` computation: improving/declining/stable trends, days_with_data tracking
   - Test `find_personal_bests`: correct max/min identification
   - Test `identify_areas_to_improve`: threshold triggers, declining trend triggers
   - Test `build_sleep_summary`: stage % calculation, primary sleep filtering
   - Test `build_workout_summary`: by-activity grouping
   - Edge cases: empty DataFrame, single-day period, all-null columns
   - Test `InsufficientDataError` raised for < 2 days
5. Implement `report_analysis.py` until all tests pass

### Phase 3: Data Fetching

6. Write `test_report_data.py` — in-memory DuckDB with mart schemas, verify date filtering and column mapping
7. Implement `report_data.py` until tests pass

### Phase 4: Chart Generation

8. Write `test_report_charts.py`:
   - Test each chart function returns valid base64 PNG string (decode and check PNG header)
   - Test graceful `None` return for empty DataFrames
   - Test `generate_all_charts` wraps failures without crashing
9. Implement `report_charts.py` with `matplotlib.use('Agg')` until tests pass

### Phase 5: HTML Rendering

10. Write `test_report_renderer.py` — build `ReportData` + chart dict, render, assert HTML contains expected elements (`<table>`, `<img>`, metric values, conditional sections)
11. Create `report.html.j2` template
12. Implement `report_renderer.py` until tests pass

### Phase 6: Email Delivery

13. Write `test_report_delivery.py` — mock `boto3.client('ses')`, verify `send_email` called with correct args, test `DeliveryError` on `ClientError`
14. Implement `report_delivery.py` until tests pass

### Phase 7: Dagster Integration

15. Write `test_report_assets.py`:
    - Schedule cron validation (Monday 7AM, 1st 7AM)
    - Default status is STOPPED
    - Job asset selection
16. Implement `report_assets.py` with shared `_generate_and_send_report` helper
17. Update `schedules.py`, `definitions.py`
18. Add `jinja2>=3.1`, `boto3>=1.34`, `matplotlib>=3.8` to `pyproject.toml`

## Observability

- **MaterializeResult metadata**: period dates, recipient email, SES message ID, chart count, days of data
- **AssetObservation**: emitted when report is skipped due to insufficient data (visible in Dagster UI)
- **Logging**: `logger.info` at each pipeline stage with timing (data fetched in Xms, N charts generated, email sent)
- **Dagster Failure**: raised on SES errors with full error description (visible as red run in UI)

## Verification

1. `uv run pytest` — all existing + new tests pass
2. `uv run pytest --cov=src --cov-report=term-missing` — verify coverage of new modules
3. Configure SES secrets in Dagster Cloud
4. Manually materialize `weekly_health_report` in Dagster UI — verify email arrives with charts
5. Verify schedules appear in Dagster UI (status: STOPPED)
6. Enable schedules, wait for Monday run, confirm automated delivery

## Future Enhancements (Deferred)

- **Goal tracking** — user-defined targets in a `goals.yaml` config, measured in reports
- **Streak tracking** — consecutive days hitting targets
- **Day-of-week analysis** — heatmap of best/worst days per metric
- **Prior period comparison** — "this week vs last week" side-by-side in weekly reports
- **Bedtime consistency score** — variance analysis from `bedtime_start`
- **PDF export** — `weasyprint` conversion from HTML
- **Dashboard** — Streamlit app reusing analysis + chart modules
