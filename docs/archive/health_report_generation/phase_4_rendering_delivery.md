# Phase 4: HTML Rendering + Email Delivery

## Goal

Build the Jinja2 HTML renderer and Amazon SES email delivery layer. The renderer takes a `ReportData` object and chart dict and produces a self-contained HTML email. The delivery layer sends it via SES. Together, these complete the report pipeline from data to inbox.

## Dependencies

- Phase 1 complete (`ReportData` and related dataclasses)
- Phase 3 complete (chart dict format: `dict[str, str | None]`)

## Files to Create

| File                                                   | Purpose                                 |
| ------------------------------------------------------ | --------------------------------------- |
| `src/dagster_project/reports/templates/report.html.j2` | Single period-aware HTML email template |
| `src/dagster_project/reports/report_renderer.py`       | Jinja2 → HTML string                    |
| `src/dagster_project/reports/report_delivery.py`       | SES email delivery resource             |
| `tests/test_report_renderer.py`                        | HTML rendering tests                    |
| `tests/test_report_delivery.py`                        | SES delivery tests (mocked boto3)       |

## Modified Files

| File             | Change                                           |
| ---------------- | ------------------------------------------------ |
| `pyproject.toml` | Add `jinja2>=3.1`, `boto3>=1.34` to dependencies |

---

## Report Renderer

### Function Signatures

```python
import logging
from pathlib import Path

import jinja2

from .report_analysis import ReportData

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def render_report(
    report_data: ReportData,
    charts: dict[str, str | None],
) -> str:
    """
    Render a complete HTML report from analysis data and charts.

    Loads report.html.j2 from the templates directory. Uses Jinja2
    with autoescaping enabled (defense in depth). The template uses
    conditional blocks for period-specific sections.

    Parameters
    ----------
    report_data : ReportData
        Complete analysis output from build_report_data.
    charts : dict[str, str | None]
        Keys: "daily_scores", "steps", "sleep_stages", "hrv_trend".
        Values: base64 PNG string or None.

    Returns
    -------
    str
        Complete HTML string ready for email body.
    """
```

### Template Structure (`report.html.j2`)

The template must be a **self-contained HTML email** — inline CSS only (no external stylesheets), no JavaScript, tables for layout (email client compatibility).

```html
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      /* Inline styles for email compatibility */
      body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        max-width: 640px;
        margin: 0 auto;
        padding: 20px;
        background-color: #f5f5f5;
        color: #333;
      }
      .header {
        background: linear-gradient(135deg, #4a90d9, #7b68ee);
        color: white;
        padding: 24px;
        border-radius: 12px;
        margin-bottom: 24px;
      }
      .card {
        background: white;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 16px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      th {
        text-align: left;
        padding: 8px 12px;
        border-bottom: 2px solid #e0e0e0;
        font-size: 13px;
        color: #666;
      }
      td {
        padding: 8px 12px;
        border-bottom: 1px solid #f0f0f0;
      }
      .trend-improving {
        color: #50c878;
      }
      .trend-declining {
        color: #ff6b6b;
      }
      .trend-stable {
        color: #999;
      }
      .chart-img {
        width: 100%;
        max-width: 600px;
        height: auto;
      }
      .personal-best {
        color: #ffd700;
        font-weight: bold;
      }
      .footer {
        text-align: center;
        color: #999;
        font-size: 12px;
        margin-top: 32px;
      }
    </style>
  </head>
  <body>
    <!-- 1. Header -->
    <div class="header">
      <h1>
        {{ "Weekly" if report_data.period_type == "weekly" else "Monthly" }}
        Health Report
      </h1>
      <p>
        {{ report_data.start_date.strftime('%B %d') }} — {{
        report_data.end_date.strftime('%B %d, %Y') }}
      </p>
    </div>

    <!-- 2. Key Metrics Summary -->
    <div class="card">
      <h2>Key Metrics</h2>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Average</th>
            <th>Best</th>
            <th>Worst</th>
            <th>Trend</th>
            <th>Data</th>
          </tr>
        </thead>
        <tbody>
          {% for m in report_data.metric_summaries %}
          <tr>
            <td>{{ m.name | replace("_", " ") | title }}</td>
            <td>{{ "%.1f" | format(m.mean) }}</td>
            <td>{{ "%.1f" | format(m.max_val) }}</td>
            <td>{{ "%.1f" | format(m.min_val) }}</td>
            <td class="trend-{{ m.trend_direction }}">
              {% if m.trend_direction == "improving" %}&#9650;{% elif
              m.trend_direction == "declining" %}&#9660;{% else %}&#9654;{%
              endif %} {{ "%.1f" | format(m.trend_pct | abs) }}%
            </td>
            <td>{{ m.days_with_data }}/{{ m.total_days }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <!-- 3. Trend Charts -->
    {% if charts.daily_scores %}
    <div class="card">
      <h2>Daily Scores</h2>
      <img
        class="chart-img"
        src="data:image/png;base64,{{ charts.daily_scores }}"
        alt="Daily scores chart"
      />
    </div>
    {% endif %} {% if charts.steps %}
    <div class="card">
      <h2>Steps</h2>
      <img
        class="chart-img"
        src="data:image/png;base64,{{ charts.steps }}"
        alt="Steps chart"
      />
    </div>
    {% endif %}

    <!-- 4. Personal Bests -->
    {% if report_data.personal_bests %}
    <div class="card">
      <h2>Personal Bests</h2>
      <ul>
        {% for pb in report_data.personal_bests %}
        <li>
          <span class="personal-best"
            >{{ pb.metric | replace("_", " ") | title }}</span
          >: {{ "%.1f" | format(pb.value) }} on {{ pb.day.strftime('%A, %b %d')
          }}
        </li>
        {% endfor %}
      </ul>
    </div>
    {% endif %}

    <!-- 5. Sleep Summary -->
    {% if report_data.sleep_summary %}
    <div class="card">
      <h2>Sleep Summary</h2>
      <table>
        <tr>
          <td>Average Duration</td>
          <td>
            {{ "%.1f" | format(report_data.sleep_summary.avg_duration_hrs) }}h
          </td>
        </tr>
        <tr>
          <td>Average Efficiency</td>
          <td>
            {{ "%.0f" | format(report_data.sleep_summary.avg_efficiency) }}%
          </td>
        </tr>
        <tr>
          <td>Average HRV</td>
          <td>{{ "%.1f" | format(report_data.sleep_summary.avg_hrv) }} ms</td>
        </tr>
        <tr>
          <td>Average Heart Rate</td>
          <td>{{ "%.1f" | format(report_data.sleep_summary.avg_hr) }} bpm</td>
        </tr>
        <tr>
          <td>Deep / Light / REM / Awake</td>
          <td>
            {{ "%.0f" | format(report_data.sleep_summary.deep_pct) }}% / {{
            "%.0f" | format(report_data.sleep_summary.light_pct) }}% / {{ "%.0f"
            | format(report_data.sleep_summary.rem_pct) }}% / {{ "%.0f" |
            format(report_data.sleep_summary.awake_pct) }}%
          </td>
        </tr>
      </table>
    </div>
    {% endif %}

    <!-- 6. Sleep Stages Chart -->
    {% if charts.sleep_stages %}
    <div class="card">
      <h2>Sleep Stages</h2>
      <img
        class="chart-img"
        src="data:image/png;base64,{{ charts.sleep_stages }}"
        alt="Sleep stages chart"
      />
    </div>
    {% endif %}

    <!-- 7. HRV Trend -->
    {% if charts.hrv_trend %}
    <div class="card">
      <h2>HRV Trend</h2>
      <img
        class="chart-img"
        src="data:image/png;base64,{{ charts.hrv_trend }}"
        alt="HRV trend chart"
      />
    </div>
    {% endif %}

    <!-- 8. Workouts -->
    {% if report_data.workout_summary %}
    <div class="card">
      <h2>Activity & Workouts</h2>
      <p>
        <strong>{{ report_data.workout_summary.total_count }}</strong> sessions
        &middot;
        <strong
          >{{ "%.0f" | format(report_data.workout_summary.total_calories)
          }}</strong
        >
        total calories
      </p>
      <ul>
        {% for activity, count in
        report_data.workout_summary.by_activity.items() %}
        <li>
          {{ activity | replace("_", " ") | title }}: {{ count }} sessions
        </li>
        {% endfor %}
      </ul>
    </div>
    {% endif %}

    <!-- 9. Areas to Improve -->
    {% if report_data.areas_to_improve %}
    <div class="card">
      <h2>Areas to Improve</h2>
      {% for area in report_data.areas_to_improve %}
      <div style="margin-bottom: 12px;">
        <strong>{{ area.metric | replace("_", " ") | title }}</strong>
        <span style="color: #999;"
          >(avg: {{ "%.1f" | format(area.current_avg) }})</span
        >
        <br /><span style="color: #FF6B6B;">{{ area.reason }}</span> <br /><em
          >{{ area.suggestion }}</em
        >
      </div>
      {% endfor %}
    </div>
    {% endif %}

    <!-- 10. Footer -->
    <div class="footer">
      <p>Generated by Oura Pipeline &middot; {{ generated_at }}</p>
    </div>
  </body>
</html>
```

### Template Variables

The `render_report` function passes these variables to the template:

| Variable       | Type                     | Source                                                              |
| -------------- | ------------------------ | ------------------------------------------------------------------- |
| `report_data`  | `ReportData`             | Passed directly — template accesses all fields                      |
| `charts`       | `dict[str, str \| None]` | Chart base64 strings from Phase 3                                   |
| `generated_at` | `str`                    | Current UTC timestamp, formatted as `"March 14, 2026 at 07:00 UTC"` |

---

## Email Delivery

### Resource Class

```python
import logging
from typing import Any

import boto3
import dagster as dg
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DeliveryError(Exception):
    """Raised when email delivery fails."""


class SESDeliveryResource(dg.ConfigurableResource):
    """Amazon SES email delivery resource for health reports."""

    sender_email: str
    recipient_email: str
    aws_region: str = "us-east-1"

    def send_report(self, subject: str, html_body: str) -> str:
        """
        Send an HTML email via Amazon SES.

        Parameters
        ----------
        subject : str
            Email subject line (e.g., "Weekly Health Report: Jan 1-7, 2025").
        html_body : str
            Complete HTML string (output of render_report).

        Returns
        -------
        str
            SES MessageId for tracking delivery.

        Raises
        ------
        DeliveryError
            Wraps botocore.exceptions.ClientError with actionable message.
            Includes the original error code and message from SES.
        """
        client = boto3.client("ses", region_name=self.aws_region)
        try:
            response = client.send_email(
                Source=self.sender_email,
                Destination={"ToAddresses": [self.recipient_email]},
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}},
                },
            )
            message_id = response["MessageId"]
            logger.info(
                "Email sent successfully. MessageId=%s, Recipient=%s",
                message_id,
                self.recipient_email,
            )
            return message_id
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_msg = e.response["Error"]["Message"]
            raise DeliveryError(
                f"SES send failed [{error_code}]: {error_msg}. "
                f"Sender={self.sender_email}, Recipient={self.recipient_email}"
            ) from e
```

---

## Test Cases

### `test_report_renderer.py`

````python
import pytest
from datetime import date

from dagster_project.reports.report_analysis import (
    MetricSummary, PersonalBest, AreaToImprove,
    SleepSummary, WorkoutSummary, ReportData,
)
from dagster_project.reports.report_renderer import render_report


@pytest.fixture
def sample_report_data() -> ReportData:
    """Minimal but complete ReportData for rendering tests."""
    return ReportData(
        period_type="weekly",
        start_date=date(2025, 1, 6),
        end_date=date(2025, 1, 12),
        metric_summaries=[
            MetricSummary("readiness_score", 78.5, 70.0, 85.0, "improving", 5.2, 7, 7),
            MetricSummary("sleep_score", 82.0, 76.0, 88.0, "stable", 1.1, 7, 7),
            MetricSummary("steps", 8200.0, 5500.0, 10200.0, "declining", -8.3, 7, 7),
        ],
        personal_bests=[
            PersonalBest("steps", 10200.0, date(2025, 1, 10)),
            PersonalBest("sleep_score", 88.0, date(2025, 1, 11)),
        ],
        areas_to_improve=[
            AreaToImprove("steps", "Declined 8.3% over period", 8200.0, "Try a daily walk after lunch"),
        ],
        sleep_summary=SleepSummary(
            avg_duration_hrs=7.5, avg_efficiency=88.0, avg_hrv=42.0,
            avg_hr=56.0, deep_pct=20.0, light_pct=50.0, rem_pct=25.0, awake_pct=5.0,
        ),
        workout_summary=WorkoutSummary(
            total_count=4, total_calories=1200.0,
            by_activity={"running": 2, "cycling": 1, "strength_training": 1},
        ),
    )


@pytest.fixture
def sample_charts() -> dict[str, str | None]:
    """Minimal chart dict — use placeholder base64 for testing."""
    # 1x1 red pixel PNG as base64 (valid PNG, minimal size)
    pixel = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="
    return {
        "daily_scores": pixel,
        "steps": pixel,
        "sleep_stages": pixel,
        "hrv_trend": None,  # Simulate missing chart
    }


class TestRenderReport:
    """Tests for render_report."""

    def test_returns_html_string(self, sample_report_data, sample_charts):
        """Output should be an HTML document."""
        html = render_report(sample_report_data, sample_charts)
        assert "<!DOCTYPE html>" in html
        assert "</html>" in html

    def test_contains_period_header(self, sample_report_data, sample_charts):
        """Should display 'Weekly Health Report' for weekly period."""
        html = render_report(sample_report_data, sample_charts)
        assert "Weekly Health Report" in html

    def test_contains_date_range(self, sample_report_data, sample_charts):
        """Should display the date range in the header."""
        html = render_report(sample_report_data, sample_charts)
        assert "January 06" in html
        assert "January 12, 2025" in html

    def test_contains_metric_table(self, sample_report_data, sample_charts):
        """Should render metrics in an HTML table."""
        html = render_report(sample_report_data, sample_charts)
        assert "<table>" in html
        assert "78.5" in html  # readiness_score mean
        assert "Readiness Score" in html  # prettified metric name

    def test_contains_trend_indicators(self, sample_report_data, sample_charts):
        """Should render trend arrows with CSS classes."""
        html = render_report(sample_report_data, sample_charts)
        assert "trend-improving" in html
        assert "trend-declining" in html

    def test_contains_personal_bests(self, sample_report_data, sample_charts):
        """Should list personal bests."""
        html = render_report(sample_report_data, sample_charts)
        assert "10200.0" in html  # steps best
        assert "Personal Bests" in html

    def test_contains_sleep_summary(self, sample_report_data, sample_charts):
        """Should display sleep metrics."""
        html = render_report(sample_report_data, sample_charts)
        assert "7.5" in html  # avg duration
        assert "42.0" in html  # avg HRV

    def test_contains_workout_summary(self, sample_report_data, sample_charts):
        """Should display workout count and breakdown."""
        html = render_report(sample_report_data, sample_charts)
        assert "4" in html  # total sessions
        assert "Running" in html

    def test_contains_areas_to_improve(self, sample_report_data, sample_charts):
        """Should display improvement suggestions."""
        html = render_report(sample_report_data, sample_charts)
        assert "Areas to Improve" in html
        assert "daily walk" in html

    def test_embeds_available_charts(self, sample_report_data, sample_charts):
        """Charts that are present should be embedded as base64 img tags."""
        html = render_report(sample_report_data, sample_charts)
        assert 'src="data:image/png;base64,' in html
        assert sample_charts["daily_scores"] in html

    def test_skips_missing_charts(self, sample_report_data, sample_charts):
        """Charts that are None should not render img tags for that section."""
        html = render_report(sample_report_data, sample_charts)
        # hrv_trend is None — should not have HRV Trend section
        assert "HRV Trend" not in html

    def test_monthly_period_type(self, sample_charts):
        """Monthly reports should display 'Monthly Health Report'."""
        data = ReportData(
            period_type="monthly",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            metric_summaries=[], personal_bests=[],
            areas_to_improve=[], sleep_summary=None, workout_summary=None,
        )
        html = render_report(data, sample_charts)
        assert "Monthly Health Report" in html

    def test_handles_no_workout_data(self, sample_charts):
        """workout_summary = None should skip the workout section."""
        data = ReportData(
            period_type="weekly",
            start_date=date(2025, 1, 6),
            end_date=date(2025, 1, 12),
            metric_summaries=[], personal_bests=[],
            areas_to_improve=[], sleep_summary=None, workout_summary=None,
        )
        html = render_report(data, sample_charts)
        assert "Activity & Workouts" not in html

    def test_contains_generated_timestamp(self, sample_report_data, sample_charts):
        """Footer should have a generation timestamp."""
        html = render_report(sample_report_data, sample_charts)
        assert "Generated by Oura Pipeline" in html


### `test_report_delivery.py`

```python
import pytest
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError

from dagster_project.reports.report_delivery import (
    SESDeliveryResource,
    DeliveryError,
)


@pytest.fixture
def ses_resource() -> SESDeliveryResource:
    """SES resource with test config."""
    return SESDeliveryResource(
        sender_email="reports@example.com",
        recipient_email="user@example.com",
        aws_region="us-east-1",
    )


class TestSESDeliveryResource:
    """Tests for SESDeliveryResource.send_report."""

    @patch("dagster_project.reports.report_delivery.boto3")
    def test_sends_email_successfully(self, mock_boto3, ses_resource):
        """Should call SES send_email with correct parameters."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "test-msg-123"}

        result = ses_resource.send_report("Test Subject", "<html>body</html>")

        assert result == "test-msg-123"
        mock_boto3.client.assert_called_once_with("ses", region_name="us-east-1")
        mock_client.send_email.assert_called_once()

        call_kwargs = mock_client.send_email.call_args[1]
        assert call_kwargs["Source"] == "reports@example.com"
        assert call_kwargs["Destination"]["ToAddresses"] == ["user@example.com"]
        assert call_kwargs["Message"]["Subject"]["Data"] == "Test Subject"
        assert call_kwargs["Message"]["Body"]["Html"]["Data"] == "<html>body</html>"

    @patch("dagster_project.reports.report_delivery.boto3")
    def test_raises_delivery_error_on_client_error(self, mock_boto3, ses_resource):
        """SES ClientError should be wrapped as DeliveryError."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.side_effect = ClientError(
            {"Error": {"Code": "MessageRejected", "Message": "Email address not verified"}},
            "SendEmail",
        )

        with pytest.raises(DeliveryError, match="MessageRejected"):
            ses_resource.send_report("Subject", "<html>body</html>")

    @patch("dagster_project.reports.report_delivery.boto3")
    def test_delivery_error_includes_sender_and_recipient(self, mock_boto3, ses_resource):
        """Error message should include sender and recipient for debugging."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue", "Message": "Bad email"}},
            "SendEmail",
        )

        with pytest.raises(DeliveryError, match="reports@example.com"):
            ses_resource.send_report("Subject", "<html></html>")

    @patch("dagster_project.reports.report_delivery.boto3")
    def test_returns_message_id(self, mock_boto3, ses_resource):
        """Should return the SES MessageId for tracking."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "abc-def-ghi"}

        msg_id = ses_resource.send_report("Subject", "<html></html>")
        assert msg_id == "abc-def-ghi"
````

## Acceptance Criteria

1. All tests in `test_report_renderer.py` and `test_report_delivery.py` pass
2. HTML template uses inline CSS only (no external stylesheets)
3. Jinja2 autoescaping is enabled
4. Template handles all `None` optional fields gracefully (no render crash)
5. `DeliveryError` wraps `ClientError` with sender/recipient/error code context
6. `SESDeliveryResource` is a `dg.ConfigurableResource` (injectable by Dagster)
7. `jinja2>=3.1` and `boto3>=1.34` added to `pyproject.toml`
8. Type hints on all function signatures, numpy-style docstrings
9. `uv run pytest tests/test_report_renderer.py tests/test_report_delivery.py -v` passes with 0 failures
