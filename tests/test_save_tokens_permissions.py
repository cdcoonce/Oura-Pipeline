"""Tests for save_tokens file permission security."""

import json
import os

from oura_oauth_cli import save_tokens


class TestSaveTokensPermissions:
    """Tests for save_tokens writing files with restricted permissions."""

    def test_saved_file_has_0600_permissions(self, tmp_path) -> None:
        """Token file should have 0o600 (owner read/write only) permissions."""
        token_path = str(tmp_path / "tokens.json")
        tokens = {"access_token": "secret", "refresh_token": "also_secret"}
        save_tokens(token_path, tokens)

        mode = os.stat(token_path).st_mode & 0o777
        assert mode == 0o600, f"Expected 0o600 but got {oct(mode)}"

    def test_saved_file_contains_correct_data(self, tmp_path) -> None:
        """Token file should contain the correct JSON data."""
        token_path = str(tmp_path / "tokens.json")
        tokens = {"access_token": "secret", "refresh_token": "also_secret"}
        save_tokens(token_path, tokens)

        with open(token_path) as f:
            saved = json.load(f)
        assert saved == tokens

    def test_overwrites_existing_file_with_restricted_permissions(
        self, tmp_path
    ) -> None:
        """Overwriting an existing file should still set 0o600 permissions."""
        token_path = str(tmp_path / "tokens.json")
        # Create file with wide-open permissions
        with open(token_path, "w") as f:
            json.dump({"old": "data"}, f)
        os.chmod(token_path, 0o644)

        tokens = {"access_token": "new_secret"}
        save_tokens(token_path, tokens)

        mode = os.stat(token_path).st_mode & 0o777
        assert mode == 0o600
