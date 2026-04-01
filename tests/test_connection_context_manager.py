"""Tests for SnowflakeResource.connection() context manager."""

from unittest.mock import MagicMock, patch

from dagster_project.defs.resources import SnowflakeResource


class TestConnectionContextManager:
    """Tests for the connection() context manager on SnowflakeResource."""

    def test_connection_yields_open_connection(self) -> None:
        """Context manager should yield the connection from get_connection."""
        resource = SnowflakeResource(
            account="test",
            user="test",
            private_key="dGVzdA==",
        )
        mock_con = MagicMock()
        with patch.object(SnowflakeResource, "get_connection", return_value=mock_con):
            with resource.connection() as con:
                assert con is mock_con

    def test_connection_closes_on_normal_exit(self) -> None:
        """Connection should be closed after exiting the context manager."""
        resource = SnowflakeResource(
            account="test",
            user="test",
            private_key="dGVzdA==",
        )
        mock_con = MagicMock()
        with patch.object(SnowflakeResource, "get_connection", return_value=mock_con):
            with resource.connection():
                pass
        mock_con.close.assert_called_once()

    def test_connection_closes_on_exception(self) -> None:
        """Connection should be closed even when an exception occurs."""
        resource = SnowflakeResource(
            account="test",
            user="test",
            private_key="dGVzdA==",
        )
        mock_con = MagicMock()
        with patch.object(SnowflakeResource, "get_connection", return_value=mock_con):
            try:
                with resource.connection():
                    raise ValueError("boom")
            except ValueError:
                pass
        mock_con.close.assert_called_once()
