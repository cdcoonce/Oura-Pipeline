"""Amazon SES email delivery for health reports.

Provides a Dagster-injectable resource that sends HTML emails via
Amazon SES, with structured error handling for delivery failures.
"""

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
        """Send an HTML email via Amazon SES.

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
