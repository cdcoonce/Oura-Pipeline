"""Tests for the OAuth CLI writing tokens to Snowflake (the pipeline's token store)."""

import base64
import json
import os
import sys
from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

import oura_oauth_cli
from dagster_project.defs.resources import OuraAPI

ACCESS = "ACCESS-TOKEN-VALUE-do-not-print"
REFRESH = "REFRESH-TOKEN-VALUE-do-not-print"


@pytest.fixture(scope="module")
def private_key_b64() -> str:
    """A real base64 PEM key so SnowflakeResource's key decoding actually runs."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    return base64.b64encode(pem).decode()


@pytest.fixture()
def cli_env(monkeypatch, tmp_path, private_key_b64):
    """Full CLI environment; cwd is tmp_path so default token paths land there."""
    monkeypatch.chdir(tmp_path)
    for name, value in {
        "OURA_CLIENT_ID": "cid",
        "OURA_CLIENT_SECRET": "csecret",
        "OURA_REDIRECT_URI": "http://127.0.0.1:8765/callback",
        "OURA_SCOPES": "daily heartrate",
        "SNOWFLAKE_ACCOUNT": "acct",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PRIVATE_KEY": private_key_b64,
        "SNOWFLAKE_WAREHOUSE": "WH",
        "SNOWFLAKE_DATABASE": "OURA",
        "SNOWFLAKE_ROLE": "ROLE",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("OURA_TOKEN_PATH", raising=False)
    monkeypatch.setattr(sys, "argv", ["oura_oauth_cli.py"])
    return tmp_path


class FakeSnowflake:
    """Stands in for snowflake.connector.connect: INSERT stores, SELECT returns latest."""

    def __init__(self, readback_override: dict | None = None) -> None:
        self.rows: list[str] = []
        self.statements: list[str] = []
        self.connect_kwargs: list[dict] = []
        self.readback_override = readback_override

    def connect(self, **kwargs):
        self.connect_kwargs.append(kwargs)
        con = MagicMock()
        cursor = MagicMock()
        con.cursor.return_value = cursor

        def execute(sql, params=None):
            self.statements.append(sql)
            if sql.lstrip().upper().startswith("INSERT"):
                self.rows.append(params[0])

        def fetchone():
            if self.readback_override is not None:
                return (json.dumps(self.readback_override),)
            return (self.rows[-1],) if self.rows else None

        cursor.execute.side_effect = execute
        cursor.fetchone.side_effect = fetchone
        return con


@pytest.fixture()
def fake_sf(mocker) -> FakeSnowflake:
    fake = FakeSnowflake()
    mocker.patch(
        "dagster_project.defs.resources.snowflake.connector.connect",
        side_effect=fake.connect,
    )
    return fake


@pytest.fixture()
def browser(mocker):
    """Simulate the local callback server receiving ?code=... after browser consent."""
    server = MagicMock()
    server.code = "auth-code-123"
    thread = MagicMock()
    run_server = mocker.patch.object(
        oura_oauth_cli, "run_local_callback_server", return_value=(server, thread)
    )
    mocker.patch.object(oura_oauth_cli.webbrowser, "open")
    mocker.patch("builtins.input", side_effect=AssertionError("no manual paste"))
    return run_server


@pytest.fixture()
def token_post(mocker):
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.side_effect = lambda: {
        "access_token": ACCESS,
        "refresh_token": REFRESH,
        "expires_in": 86400,
        "token_type": "bearer",
    }
    return mocker.patch.object(oura_oauth_cli.requests, "post", return_value=resp)


def _grant_types(post_mock) -> list[str]:
    return [c.kwargs["data"]["grant_type"] for c in post_mock.call_args_list]


class TestSuccessfulExchangeWritesSnowflake:
    def test_saves_via_ouraapi_save_tokens_with_obtained_at(
        self, cli_env, fake_sf, browser, token_post, mocker
    ) -> None:
        spy = mocker.spy(OuraAPI, "_save_tokens")

        oura_oauth_cli.main()

        assert _grant_types(token_post) == ["authorization_code"]
        spy.assert_called_once()
        saved = spy.call_args.args[1]
        assert saved["refresh_token"] == REFRESH
        assert isinstance(saved["obtained_at"], int) and saved["obtained_at"] > 0

        # Same SQL path the pipeline reads from, then a read-back verification.
        inserts = [s for s in fake_sf.statements if "INSERT" in s.upper()]
        assert len(inserts) == 1 and "OURA.CONFIG.OAUTH_TOKENS" in inserts[0]
        assert json.loads(fake_sf.rows[0])["obtained_at"] == saved["obtained_at"]
        last = fake_sf.statements[-1]
        assert last.lstrip().upper().startswith("SELECT")
        assert "OURA.CONFIG.OAUTH_TOKENS" in last

    def test_connects_with_snowflake_env_vars(
        self, cli_env, fake_sf, browser, token_post
    ) -> None:
        oura_oauth_cli.main()

        kwargs = fake_sf.connect_kwargs[0]
        assert kwargs["account"] == "acct"
        assert kwargs["user"] == "user"
        assert kwargs["warehouse"] == "WH"
        assert kwargs["database"] == "OURA"
        assert kwargs["role"] == "ROLE"
        assert isinstance(kwargs["private_key"], bytes)

    def test_output_never_contains_token_values(
        self, cli_env, fake_sf, browser, token_post, capsys
    ) -> None:
        oura_oauth_cli.main()

        out = capsys.readouterr()
        for secret in (ACCESS, REFRESH, "csecret", "auth-code-123"):
            assert secret not in out.out
            assert secret not in out.err


class TestReadBackVerification:
    def test_mismatched_readback_exits_nonzero_without_leaking(
        self, cli_env, mocker, browser, token_post, capsys
    ) -> None:
        fake = FakeSnowflake(
            readback_override={"access_token": "other", "refresh_token": "OTHER-RT"}
        )
        mocker.patch(
            "dagster_project.defs.resources.snowflake.connector.connect",
            side_effect=fake.connect,
        )

        with pytest.raises(SystemExit) as exc:
            oura_oauth_cli.main()

        assert exc.value.code not in (0, None)
        out = capsys.readouterr()
        for secret in (ACCESS, REFRESH, "OTHER-RT"):
            assert secret not in out.out
            assert secret not in out.err


class TestNoLocalRefreshShortcut:
    def test_existing_token_file_does_not_trigger_refresh(
        self, cli_env, fake_sf, browser, token_post, monkeypatch
    ) -> None:
        token_file = cli_env / "old_tokens.json"
        token_file.write_text(json.dumps({"refresh_token": "stale-already-used"}))
        monkeypatch.setenv("OURA_TOKEN_PATH", str(token_file))
        default_file = cli_env / "data" / "tokens" / "oura_tokens.json"
        default_file.parent.mkdir(parents=True)
        default_file.write_text(json.dumps({"refresh_token": "stale-already-used"}))

        oura_oauth_cli.main()

        assert "refresh_token" not in _grant_types(token_post)
        assert _grant_types(token_post) == ["authorization_code"]
        browser.assert_called_once()


class TestLocalTokenFile:
    def test_not_written_by_default(
        self, cli_env, fake_sf, browser, token_post, monkeypatch
    ) -> None:
        target = cli_env / "should_not_exist.json"
        monkeypatch.setenv("OURA_TOKEN_PATH", str(target))

        oura_oauth_cli.main()

        assert not target.exists()
        assert not (cli_env / "data").exists()

    def test_token_file_flag_writes_0600_copy_and_warns(
        self, cli_env, fake_sf, browser, token_post, capsys, monkeypatch
    ) -> None:
        target = cli_env / "backup" / "tokens.json"
        monkeypatch.setattr(
            sys, "argv", ["oura_oauth_cli.py", "--token-file", str(target)]
        )

        oura_oauth_cli.main()

        assert json.loads(target.read_text())["refresh_token"] == REFRESH
        assert os.stat(target).st_mode & 0o777 == 0o600
        assert "plaintext" in capsys.readouterr().err.lower()


class TestConfigValidatedBeforeBrowser:
    @pytest.mark.parametrize(
        "missing", ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY"]
    )
    def test_missing_snowflake_env_exits_before_authorization(
        self, cli_env, fake_sf, browser, token_post, monkeypatch, missing
    ) -> None:
        monkeypatch.delenv(missing)

        with pytest.raises(SystemExit):
            oura_oauth_cli.main()

        browser.assert_not_called()
        token_post.assert_not_called()
