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

load_dotenv()


AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"


def env(name: str, required: bool = True, default: str | None = None) -> str:
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


def refresh_with_refresh_token(
    refresh_token: str, client_id: str, client_secret: str
) -> dict:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
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
    with open(path, "w") as f:
        json.dump(tokens, f, indent=2)
    print(f"Saved tokens to: {path}")


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


def main():
    # Read config from env (matches your Dagster EnvVar setup)
    client_id = env("OURA_CLIENT_ID")
    client_secret = env("OURA_CLIENT_SECRET")
    redirect_uri = env("OURA_REDIRECT_URI")  # e.g. http://127.0.0.1:8765/callback
    scopes = env("OURA_SCOPES")  # e.g. "daily heartrate workout session tag spo2"
    token_path = env(
        "OURA_TOKEN_PATH", required=False, default="data/tokens/oura_tokens.json"
    )

    # If we already have a refresh token, you can refresh in-place (optional)
    if os.path.exists(token_path):
        try:
            with open(token_path) as f:
                existing = json.load(f)
            if "refresh_token" in existing:
                print("Found existing refresh_token; refreshing...")
                new_tokens = refresh_with_refresh_token(
                    existing["refresh_token"], client_id, client_secret
                )
                save_tokens(token_path, new_tokens)
                return
        except Exception as e:
            print(f"Warning: existing token file unreadable, continuing fresh: {e}")

    # Build auth URL
    authorize_url = build_authorize_url(client_id, redirect_uri, scopes)
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

    if not code:
        print("ERROR: No code obtained.")
        sys.exit(1)

    tokens = exchange_code_for_tokens(code, client_id, client_secret, redirect_uri)
    save_tokens(token_path, tokens)
    print("Done. You can now run your Dagster assets.")


if __name__ == "__main__":
    main()
