"""Tests for OuraAPI with Snowflake-backed token storage."""

import json
import logging
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from dagster_project.defs.resources import OuraAPI, SnowflakeResource


def _make_api() -> OuraAPI:
    """Create an OuraAPI instance with dummy config for unit tests.

    Uses model_construct() to bypass Pydantic frozen validation,
    since ConfigurableResource inherits from BaseModel with frozen=True.
    """
    mock_sf = MagicMock(spec=SnowflakeResource)
    return OuraAPI.model_construct(
        client_id="test_id",
        client_secret="test_secret",
        snowflake=mock_sf,
    )


class TestLoadTokens:
    def test_returns_token_dict_from_snowflake(self):
        """_load_tokens queries Snowflake and returns parsed JSON."""
        token_json = json.dumps(
            {
                "access_token": "test_access",
                "refresh_token": "test_refresh",
                "expires_in": 86400,
                "obtained_at": int(time.time()),
            }
        )
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (token_json,)
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        tokens = api._load_tokens()
        assert tokens["access_token"] == "test_access"
        assert tokens["refresh_token"] == "test_refresh"
        mock_con.close.assert_called_once()

    def test_raises_when_no_tokens_exist(self):
        """_load_tokens raises FileNotFoundError when table is empty."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        with pytest.raises(FileNotFoundError, match="No OAuth tokens"):
            api._load_tokens()
        mock_con.close.assert_called_once()


class TestSaveTokens:
    def test_inserts_token_json_into_snowflake(self):
        """_save_tokens INSERTs PARSE_JSON'd token data."""
        mock_cursor = MagicMock()
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        tokens = {"access_token": "new", "refresh_token": "new_r"}
        api._save_tokens(tokens)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "PARSE_JSON" in call_args[0][0]
        assert json.dumps(tokens) in call_args[0][1]
        mock_con.close.assert_called_once()


class TestGetAccessToken:
    def test_returns_valid_token_without_refresh(self):
        """Valid, non-expired token returned directly."""
        now = int(time.time())
        tokens = {
            "access_token": "valid_token",
            "refresh_token": "refresh",
            "expires_in": 86400,
            "obtained_at": now,
        }
        api = _make_api()
        api._load_tokens = MagicMock(return_value=tokens)
        api._save_tokens = MagicMock()

        result = api._get_access_token()
        assert result == "valid_token"
        api._save_tokens.assert_not_called()

    def test_refreshes_expired_token(self, mocker):
        """Expired token triggers refresh via POST."""
        old_tokens = {
            "access_token": "old",
            "refresh_token": "refresh_tok",
            "expires_in": 86400,
            "obtained_at": 0,  # long expired
        }
        new_tokens = {
            "access_token": "new_access",
            "refresh_token": "new_refresh",
            "expires_in": 86400,
        }
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = new_tokens

        mocker.patch("requests.post", return_value=mock_resp)

        api = _make_api()
        api._load_tokens = MagicMock(return_value=old_tokens)
        api._save_tokens = MagicMock()

        result = api._get_access_token()
        assert result == "new_access"
        api._save_tokens.assert_called_once()

    def test_refresh_failure_raises_with_reseed_instructions(self, mocker):
        """Failed refresh raises RuntimeError with clear re-seed message."""
        old_tokens = {
            "access_token": "old",
            "refresh_token": "bad_refresh",
            "expires_in": 86400,
            "obtained_at": 0,
        }
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 400
        mock_resp.text = "invalid_grant"

        mocker.patch("requests.post", return_value=mock_resp)

        api = _make_api()
        api._load_tokens = MagicMock(return_value=old_tokens)

        with pytest.raises(RuntimeError, match="refresh token is likely expired"):
            api._get_access_token()


class TestGetHttpErrorHandling:
    """_get returns empty dict on HTTP errors instead of raising."""

    def test_http_error_returns_empty_dict(self, caplog):
        """A 4xx/5xx response returns {} and logs a warning."""
        now = int(time.time())
        tokens = {
            "access_token": "valid_token",
            "refresh_token": "refresh",
            "expires_in": 86400,
            "obtained_at": now,
        }
        api = _make_api()
        api._load_tokens = MagicMock(return_value=tokens)

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=404)
        )

        with patch(
            "dagster_project.defs.resources.requests.get", return_value=mock_resp
        ):
            with caplog.at_level(logging.WARNING):
                result = api._get(
                    "/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"}
                )

        assert result == {}
        assert "returned HTTP error" in caplog.text

    def test_fetch_daily_returns_empty_on_http_error(self):
        """fetch_daily returns empty list when API returns an HTTP error."""
        from datetime import date

        now = int(time.time())
        tokens = {
            "access_token": "valid_token",
            "refresh_token": "refresh",
            "expires_in": 86400,
            "obtained_at": now,
        }
        api = _make_api()
        api._load_tokens = MagicMock(return_value=tokens)

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=404)
        )

        with patch(
            "dagster_project.defs.resources.requests.get", return_value=mock_resp
        ):
            result = api.fetch_daily("sleep", date(2025, 1, 1), date(2025, 1, 1))

        assert result == []
