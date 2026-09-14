"""Authorize the Oura app in a browser and store fresh tokens in Snowflake.

The pipeline's ``OuraAPI`` resource reads tokens only from
``OURA.CONFIG.OAUTH_TOKENS``, so this CLI writes there through the same
``OuraAPI._save_tokens`` path and reads the row back to verify.

There is deliberately no "refresh from a local file" shortcut: Oura refresh
tokens are single-use and the pipeline rotates them in Snowflake on every
refresh, so any locally saved refresh token is almost always already spent,
and replaying it fails with ``invalid_grant`` (and can revoke the grant).
"""

import argparse
import os
import sys
import json
import time
import threading
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from dotenv import load_dotenv

from dagster_project.defs.resources import OuraAPI, SnowflakeResource

load_dotenv()


AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"
TOKEN_TABLE = "OURA.CONFIG.OAUTH_TOKENS"


def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    val = os.getenv(name, default)
    if required and not val:
        print(f"ERROR: Missing required env var: {name}", file=sys.stderr)
        sys.exit(1)
    return val


def build_authorize_url(
    client_id: str, redirect_uri: str, scopes: str, state: str = "localdev"
) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scopes,
        "state": state,
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def exchange_code_for_tokens(
    code: str, client_id: str, client_secret: str, redirect_uri: str
) -> dict:
    """Exchange an OAuth2 authorization code for access and refresh tokens.

    Parameters
    ----------
    code : str
        The authorization code received from the Oura OAuth callback.
    client_id : str
        Oura application client ID.
    client_secret : str
        Oura application client secret.
    redirect_uri : str
        The redirect URI registered with the Oura application.

    Returns
    -------
    dict
        Token response dict including ``access_token``, ``refresh_token``,
        and ``obtained_at`` (Unix timestamp of when the tokens were received).
    """
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = int(time.time())
    return tokens


def save_tokens(path: str, tokens: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(tokens, f, indent=2)
    os.chmod(path, 0o600)
    print(f"Saved tokens to: {path}")


def build_token_store(client_id: str, client_secret: str) -> OuraAPI:
    """Build the pipeline's ``OuraAPI`` resource from ``SNOWFLAKE_*`` env vars.

    Warehouse, database, and role fall back to ``SnowflakeResource`` defaults
    when unset. Exits before any browser authorization if required vars are
    missing, so consent is never spent on a run that cannot store tokens.
    """
    snowflake_config = {
        "account": env("SNOWFLAKE_ACCOUNT"),
        "user": env("SNOWFLAKE_USER"),
        "private_key": env("SNOWFLAKE_PRIVATE_KEY"),
    }
    for field, name in (
        ("warehouse", "SNOWFLAKE_WAREHOUSE"),
        ("database", "SNOWFLAKE_DATABASE"),
        ("role", "SNOWFLAKE_ROLE"),
    ):
        value = env(name, required=False)
        if value:
            snowflake_config[field] = value
    return OuraAPI(
        client_id=client_id,
        client_secret=client_secret,
        snowflake=SnowflakeResource(**snowflake_config),
    )


def preflight_token_store(token_store: OuraAPI) -> None:
    """Confirm Snowflake is reachable and the token table readable before consent."""
    try:
        with token_store.snowflake.connection() as con:
            con.cursor().execute(f"SELECT COUNT(*) FROM {TOKEN_TABLE}")
    except Exception as e:
        print(
            f"ERROR: Cannot reach {TOKEN_TABLE} in Snowflake "
            f"({type(e).__name__}: {e}). Fix the SNOWFLAKE_* settings before "
            "authorizing.",
            file=sys.stderr,
        )
        sys.exit(1)


def store_tokens_in_snowflake(token_store: OuraAPI, tokens: dict) -> None:
    """Insert tokens via ``OuraAPI._save_tokens`` and verify the newest row.

    Compares refresh tokens only; no token value is ever printed.
    """
    token_store._save_tokens(tokens)
    stored = token_store._load_tokens()
    if stored.get("refresh_token") != tokens["refresh_token"]:
        print(
            f"ERROR: Wrote tokens, but the newest row in {TOKEN_TABLE} holds a "
            "different refresh token. If a pipeline run refreshed at the same "
            "moment this is benign; otherwise check the table and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Verified: newest row in {TOKEN_TABLE} holds the new refresh token.")


class _CodeHandler(BaseHTTPRequestHandler):
    """Minimal handler that grabs ?code=... on GET /callback and stores it on the server object."""

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        error = qs.get("error", [None])[0]
        if error:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"OAuth error: {error}".encode())
            self.server.code = None  # type: ignore[attr-defined]
            return
        if code:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"You can close this tab and return to the terminal.")
            self.server.code = code  # type: ignore[attr-defined]
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing ?code=. Did you approve the app?")
            self.server.code = None  # type: ignore[attr-defined]

    def log_message(self, *args, **kwargs):
        # keep console clean
        return


def run_local_callback_server(redirect_uri: str) -> tuple[HTTPServer, threading.Thread]:
    """
    Start a tiny HTTP server bound to the host:port in redirect_uri and wait for one callback.
    Returns the server and handler thread.
    """
    u = urlparse(redirect_uri)
    host = u.hostname or "127.0.0.1"
    port = u.port or 80

    httpd = HTTPServer((host, port), _CodeHandler)
    httpd.code = None  # type: ignore[attr-defined]

    # Serve one request in a background thread so we can open the browser
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    return httpd, t


def obtain_authorization_code(authorize_url: str, redirect_uri: str) -> str | None:
    """Run browser consent, via the local callback server when possible."""
    print("\nAuthorize URL:")
    print(authorize_url)

    # Try auto-callback if redirect_uri points to localhost with a path
    code = None
    try:
        u = urlparse(redirect_uri)
        if (
            u.scheme in ("http", "https")
            and (u.hostname in ("127.0.0.1", "localhost"))
            and u.path
        ):
            print("Starting local callback server and opening your browser...")
            server, thread = run_local_callback_server(redirect_uri)
            webbrowser.open(authorize_url)
            # Wait for the one request to be handled
            thread.join(timeout=300)  # 5 minutes
            code = getattr(server, "code", None)  # type: ignore[attr-defined]
    except Exception as e:
        print(f"(Local callback server not used: {e})")

    if not code:
        # Manual fallback
        print("\nIf your browser didn't open or you prefer manual:")
        print("1) Visit the authorize URL above")
        print("2) Approve the app")
        print("3) Copy the 'code' parameter from the redirected URL and paste it here.")
        code = input("Paste code: ").strip()
    return code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"Authorize the Oura app in a browser and store fresh OAuth tokens "
            f"in Snowflake ({TOKEN_TABLE}), where the pipeline reads them."
        )
    )
    parser.add_argument(
        "--token-file",
        metavar="PATH",
        help=(
            "Also write a plaintext copy of the tokens to PATH (mode 0600). "
            "Off by default. Never refresh from this file: the pipeline "
            "rotates the single-use refresh token in Snowflake."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Read config from env (matches your Dagster EnvVar setup)
    client_id = env("OURA_CLIENT_ID")
    client_secret = env("OURA_CLIENT_SECRET")
    redirect_uri = env("OURA_REDIRECT_URI")  # e.g. http://127.0.0.1:8765/callback
    scopes = env("OURA_SCOPES")  # e.g. "daily heartrate workout session tag spo2"

    # Validate the token store before spending a browser consent.
    token_store = build_token_store(client_id, client_secret)
    preflight_token_store(token_store)

    if os.getenv("OURA_TOKEN_PATH"):
        print(
            "Note: OURA_TOKEN_PATH is ignored; tokens go to Snowflake. "
            "Use --token-file PATH for a local copy.",
            file=sys.stderr,
        )

    authorize_url = build_authorize_url(client_id, redirect_uri, scopes)
    code = obtain_authorization_code(authorize_url, redirect_uri)
    if not code:
        print("ERROR: No code obtained.")
        sys.exit(1)

    tokens = exchange_code_for_tokens(code, client_id, client_secret, redirect_uri)
    if not tokens.get("refresh_token"):
        print(
            "ERROR: Token response has no refresh_token; nothing stored.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Write the optional local copy first so a Snowflake failure doesn't lose tokens.
    if args.token_file:
        print(
            f"WARNING: writing plaintext OAuth tokens to {args.token_file}. "
            "Delete it once Snowflake is verified; never refresh from it.",
            file=sys.stderr,
        )
        save_tokens(args.token_file, tokens)

    store_tokens_in_snowflake(token_store, tokens)
    print("Done. The pipeline will use these tokens on its next run.")


if __name__ == "__main__":
    main()
