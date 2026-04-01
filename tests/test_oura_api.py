"""Tests for OuraAPI with Snowflake-backed token storage."""

import json
import logging
import time
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from dagster_project.defs.resources import OuraAPI, SnowflakeResource


def _make_mock_con() -> MagicMock:
    """Create a mock Snowflake connection."""
    return MagicMock()


def _make_api(mock_con: MagicMock | None = None) -> OuraAPI:
    """Create an OuraAPI instance with dummy config for unit tests.

    Uses model_construct() to bypass Pydantic frozen validation,
    since ConfigurableResource inherits from BaseModel with frozen=True.
    If ``mock_con`` is provided, the nested SnowflakeResource's
    ``connection()`` context manager will yield it.
    """
    mock_sf = MagicMock(spec=SnowflakeResource)
    if mock_con is not None:

        @contextmanager
        def _fake_connection():
            yield mock_con

        mock_sf.connection = _fake_connection
    return OuraAPI.model_construct(
        client_id="test_id",
        client_secret="test_secret",
        snowflake=mock_sf,
    )


class TestLoadTokens:
    def test_returns_token_dict_from_snowflake(self) -> None:
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
        mock_con = _make_mock_con()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api(mock_con)

        tokens = api._load_tokens()
        assert tokens["access_token"] == "test_access"
        assert tokens["refresh_token"] == "test_refresh"

    def test_raises_when_no_tokens_exist(self) -> None:
        """_load_tokens raises FileNotFoundError when table is empty."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_con = _make_mock_con()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api(mock_con)

        with pytest.raises(FileNotFoundError, match="No OAuth tokens"):
            api._load_tokens()


class TestSaveTokens:
    def test_inserts_token_json_into_snowflake(self) -> None:
        """_save_tokens INSERTs PARSE_JSON'd token data."""
        mock_cursor = MagicMock()
        mock_con = _make_mock_con()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api(mock_con)

        tokens = {"access_token": "new", "refresh_token": "new_r"}
        api._save_tokens(tokens)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "PARSE_JSON" in call_args[0][0]
        assert json.dumps(tokens) in call_args[0][1]


class TestGetAccessToken:
    def test_returns_valid_token_without_refresh(self) -> None:
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

    def test_refreshes_expired_token(self, mocker) -> None:
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

    def test_refresh_failure_raises_with_reseed_instructions(self, mocker) -> None:
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
    """_get handles HTTP errors based on status code severity."""

    def _make_api_with_valid_token(self):
        now = int(time.time())
        tokens = {
            "access_token": "valid_token",
            "refresh_token": "refresh",
            "expires_in": 86400,
            "obtained_at": now,
        }
        api = _make_api()
        api._load_tokens = MagicMock(return_value=tokens)
        return api

    def _mock_http_error(self, status_code: int, body: str = "error"):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.text = body
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=status_code)
        )
        return mock_resp

    def test_404_returns_empty_dict(self, caplog) -> None:
        """A 404 returns {} and logs a warning (non-critical client error)."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(404, "Not Found"),
        ):
            with caplog.at_level(logging.WARNING):
                result = api._get(
                    "/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"}
                )

        assert result == {}
        assert "treating as empty data" in caplog.text

    def test_401_raises_runtime_error(self) -> None:
        """A 401 raises RuntimeError — auth errors must not be silent."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(401, "Unauthorized"),
        ):
            with pytest.raises(RuntimeError, match="auth error"):
                api._get("/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"})

    def test_403_raises_runtime_error(self) -> None:
        """A 403 raises RuntimeError — forbidden must not be silent."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(403, "Forbidden"),
        ):
            with pytest.raises(RuntimeError, match="auth error"):
                api._get("/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"})

    def test_429_raises_runtime_error(self) -> None:
        """A 429 raises RuntimeError — rate limits should trigger retry."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(429, "Too Many Requests"),
        ):
            with pytest.raises(RuntimeError, match="rate limited"):
                api._get("/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"})

    def test_500_raises_runtime_error(self) -> None:
        """A 500 raises RuntimeError — server errors should trigger retry."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(500, "Internal Server Error"),
        ):
            with pytest.raises(RuntimeError, match="server error"):
                api._get("/v2/usercollection/daily_sleep", {"start_date": "2025-01-01"})

    def test_fetch_daily_returns_empty_on_404(self) -> None:
        """fetch_daily returns empty list when API returns a 404."""
        api = self._make_api_with_valid_token()

        with patch(
            "dagster_project.defs.resources.requests.get",
            return_value=self._mock_http_error(404, "Not Found"),
        ):
            result = api.fetch_daily("sleep", date(2025, 1, 1), date(2025, 1, 1))

        assert result == []


class TestExclusiveEndDateAdjustment:
    """Endpoints with exclusive end_date get +1 day so single-day queries work."""

    @pytest.fixture()
    def api_with_mock_response(self) -> tuple[OuraAPI, MagicMock]:
        """Create an OuraAPI with valid tokens and a successful mock response.

        Returns
        -------
        tuple[OuraAPI, MagicMock]
            The configured API instance and a mock response whose
            ``json.return_value`` can be overridden per-test if needed.
        """
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
        mock_resp.ok = True
        mock_resp.json.return_value = {"data": [{"id": "item_1"}]}
        mock_resp.raise_for_status = MagicMock()

        return api, mock_resp

    def test_exclusive_daily_endpoint_adjusts_end_date(
        self, api_with_mock_response
    ) -> None:
        """fetch_daily adds +1 day for exclusive endpoints like daily_activity."""
        api, mock_resp = api_with_mock_response

        with patch(
            "dagster_project.defs.resources.requests.get", return_value=mock_resp
        ) as mock_get:
            api.fetch_daily("activity", date(2026, 3, 14), date(2026, 3, 14))

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["end_date"] == date(2026, 3, 15)
        assert kwargs["params"]["start_date"] == date(2026, 3, 14)

    def test_inclusive_daily_endpoint_keeps_end_date(
        self, api_with_mock_response
    ) -> None:
        """fetch_daily does NOT adjust end_date for inclusive endpoints like daily_sleep."""
        api, mock_resp = api_with_mock_response

        with patch(
            "dagster_project.defs.resources.requests.get", return_value=mock_resp
        ) as mock_get:
            api.fetch_daily("sleep", date(2026, 3, 14), date(2026, 3, 14))

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["end_date"] == date(2026, 3, 14)

    def test_exclusive_granular_endpoint_adjusts_end_date(
        self, api_with_mock_response
    ) -> None:
        """Granular fetch methods adjust end_date for exclusive endpoints."""
        api, mock_resp = api_with_mock_response

        exclusive_methods = [
            ("fetch_sleep_periods", date(2026, 3, 15)),
            ("fetch_workouts", date(2026, 3, 15)),
            ("fetch_sessions", date(2026, 3, 15)),
            ("fetch_tags", date(2026, 3, 15)),
            ("fetch_rest_mode_periods", date(2026, 3, 15)),
        ]

        for method_name, expected_end in exclusive_methods:
            with patch(
                "dagster_project.defs.resources.requests.get", return_value=mock_resp
            ) as mock_get:
                getattr(api, method_name)(date(2026, 3, 14), date(2026, 3, 14))

            _, kwargs = mock_get.call_args
            assert kwargs["params"]["end_date"] == expected_end, (
                f"{method_name} should adjust end_date to {expected_end}"
            )

    def test_inclusive_granular_endpoint_keeps_end_date(
        self, api_with_mock_response
    ) -> None:
        """Inclusive granular endpoints (heartrate, sleep_time) keep end_date as-is."""
        api, mock_resp = api_with_mock_response

        inclusive_methods = ["fetch_heartrate", "fetch_sleep_time"]

        for method_name in inclusive_methods:
            with patch(
                "dagster_project.defs.resources.requests.get", return_value=mock_resp
            ) as mock_get:
                getattr(api, method_name)(date(2026, 3, 14), date(2026, 3, 14))

            _, kwargs = mock_get.call_args
            assert kwargs["params"]["end_date"] == date(2026, 3, 14), (
                f"{method_name} should NOT adjust end_date"
            )
