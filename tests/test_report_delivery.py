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
