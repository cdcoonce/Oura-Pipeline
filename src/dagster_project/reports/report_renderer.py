"""HTML report rendering using Jinja2 templates.

Loads a self-contained HTML email template and populates it with
analysis data and chart images for delivery via email.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import jinja2

from .report_analysis import ReportData

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def render_report(
    report_data: ReportData,
    charts: dict[str, str | None],
) -> str:
    """Render a complete HTML report from analysis data and charts.

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
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("report.html.j2")

    generated_at = datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")

    html = template.render(
        report_data=report_data,
        charts=charts,
        generated_at=generated_at,
    )

    logger.info("Rendered %s report (%d chars)", report_data.period_type, len(html))
    return html
