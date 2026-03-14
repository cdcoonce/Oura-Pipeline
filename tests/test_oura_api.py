"""Tests for OuraAPI._get_access_token."""

import json
from unittest.mock import patch, MagicMock

import pytest

from dagster_project.defs.resources import OuraAPI


@pytest.fixture()
def valid_token_file(tmp_path):
    """Write a token file whose token is still valid (expires far in the future)."""
    token_data = {
        "access_token": "valid-access-token",
        "refresh_token": "valid-refresh-token",
        "expires_in": 86400,
        "obtained_at": 1_000_000,
    }
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps(token_data))
    return path, token_data


@pytest.fixture()
def expired_token_file(tmp_path):
    """Write a token file whose token has already expired."""
    token_data = {
        "access_token": "expired-access-token",
        "refresh_token": "expired-refresh-token",
        "expires_in": 86400,
        "obtained_at": 1_000_000,
    }
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps(token_data))
    return path, token_data


class TestGetAccessToken:
    """Tests for OuraAPI._get_access_token."""

    @patch("dagster_project.defs.resources.time.time", return_value=1_050_000)
    def test_valid_token_returned_without_http_request(
        self, mock_time, valid_token_file
    ):
        """A non-expired token is returned directly; no HTTP call is made."""
        path, token_data = valid_token_file

        api = OuraAPI(
            client_id="test-id",
            client_secret="test-secret",
            token_path=str(path),
        )

        with patch("dagster_project.defs.resources.requests.post") as mock_post:
            result = api._get_access_token()

        assert result == "valid-access-token"
        mock_post.assert_not_called()

    @patch("dagster_project.defs.resources.time.time", return_value=2_000_000)
    def test_expired_token_triggers_refresh(self, mock_time, expired_token_file):
        """An expired token triggers a refresh POST and saves new tokens."""
        path, token_data = expired_token_file

        refreshed_response = {
            "access_token": "new-access-token",
            "refresh_token": "new-refresh-token",
            "expires_in": 86400,
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = refreshed_response

        api = OuraAPI(
            client_id="test-id",
            client_secret="test-secret",
            token_path=str(path),
        )

        with patch(
            "dagster_project.defs.resources.requests.post", return_value=mock_resp
        ) as mock_post:
            result = api._get_access_token()

        assert result == "new-access-token"
        mock_post.assert_called_once()

        # Verify the saved file contains the refreshed tokens with obtained_at
        saved = json.loads(path.read_text())
        assert saved["access_token"] == "new-access-token"
        assert saved["refresh_token"] == "new-refresh-token"
        assert saved["obtained_at"] == 2_000_000

    def test_missing_token_file_raises(self, tmp_path):
        """A missing token file raises FileNotFoundError."""
        missing_path = tmp_path / "nonexistent.json"

        api = OuraAPI(
            client_id="test-id",
            client_secret="test-secret",
            token_path=str(missing_path),
        )

        with pytest.raises(FileNotFoundError, match="Token file not found"):
            api._get_access_token()
