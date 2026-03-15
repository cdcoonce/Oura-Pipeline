"""
Quick diagnostic: hit every Oura API endpoint for recent dates
and report status codes + row counts.

Usage:
    uv run python scripts/diagnose_endpoints.py
"""

import json
import os
import sys
import time
from datetime import date, timedelta

import requests

# ── Load env ────────────────────────────────────────────────────────
# Support .env if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

TOKEN_URL = "https://api.ouraring.com/oauth/token"
BASE_URL = "https://api.ouraring.com"

# ── Token refresh ───────────────────────────────────────────────────


def get_access_token() -> str:
    """Try Snowflake-stored tokens first, fall back to env var."""
    # Quick path: use a personal access token if set
    pat = os.getenv("OURA_ACCESS_TOKEN")
    if pat:
        return pat

    # Otherwise refresh via OAuth
    import base64
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    pem_bytes = base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY"])
    pk = load_pem_private_key(pem_bytes, password=None)
    pk_bytes = pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pk_bytes,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OURA"),
        role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORM"),
    )
    cursor = con.cursor()
    cursor.execute(
        "SELECT token_data FROM OURA.CONFIG.OAUTH_TOKENS "
        "ORDER BY updated_at DESC LIMIT 1"
    )
    row = cursor.fetchone()
    con.close()
    if row is None:
        print("ERROR: No OAuth tokens in OURA.CONFIG.OAUTH_TOKENS")
        sys.exit(1)

    tokens = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])

    # Check if token is still valid
    expires_in = int(tokens.get("expires_in", 0))
    obtained_at = int(tokens.get("obtained_at", 0))
    now = int(time.time())

    if obtained_at + expires_in - 60 > now and "access_token" in tokens:
        return tokens["access_token"]

    # Refresh
    client_id = os.environ["OURA_CLIENT_ID"]
    client_secret = os.environ["OURA_CLIENT_SECRET"]
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    if not resp.ok:
        print(f"ERROR: Token refresh failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    refreshed = resp.json()
    return refreshed["access_token"]


# ── Endpoints to test ───────────────────────────────────────────────

DAILY_ENDPOINTS = {
    "sleep": "/v2/usercollection/daily_sleep",
    "activity": "/v2/usercollection/daily_activity",
    "readiness": "/v2/usercollection/daily_readiness",
    "spo2": "/v2/usercollection/daily_spo2",
    "stress": "/v2/usercollection/daily_stress",
    "resilience": "/v2/usercollection/daily_resilience",
}

GRANULAR_ENDPOINTS = {
    "heartrate": "/v2/usercollection/heartrate",
    "sleep_periods": "/v2/usercollection/sleep",
    "sleep_time": "/v2/usercollection/sleep_time",
    "workouts": "/v2/usercollection/workout",
    "sessions": "/v2/usercollection/session",
    "tags": "/v2/usercollection/tag",
    "rest_mode_periods": "/v2/usercollection/rest_mode_period",
}


def diagnose():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Test with last 7 days to maximize chance of finding data
    end = date.today()
    start = end - timedelta(days=7)
    params = {"start_date": str(start), "end_date": str(end)}

    all_endpoints = {**DAILY_ENDPOINTS, **GRANULAR_ENDPOINTS}

    print(f"\n{'=' * 65}")
    print("  Oura API Endpoint Diagnostic")
    print(f"  Date range: {start} → {end}")
    print(f"{'=' * 65}\n")
    print(f"  {'Endpoint':<22} {'Status':>6}  {'Rows':>5}  Notes")
    print(f"  {'-' * 22} {'-' * 6}  {'-' * 5}  {'-' * 20}")

    issues = []

    for name, path in all_endpoints.items():
        resp = requests.get(
            f"{BASE_URL}{path}",
            headers=headers,
            params=params,
            timeout=30,
        )

        status = resp.status_code
        rows = 0
        notes = ""

        if resp.ok:
            body = resp.json()
            data = body.get("data", [])
            rows = len(data)
            if rows == 0:
                notes = "OK but no data"
        else:
            # Include the error detail
            try:
                error_body = resp.json()
                notes = f"ERROR: {error_body.get('detail', resp.text[:50])}"
            except Exception:
                notes = f"ERROR: {resp.text[:50]}"
            issues.append((name, status, notes))

        status_icon = "✓" if resp.ok and rows > 0 else "⚠" if resp.ok else "✗"
        print(f"  {status_icon} {name:<20} {status:>6}  {rows:>5}  {notes}")

    print(f"\n{'=' * 65}")
    if issues:
        print(f"\n  ⚠ {len(issues)} endpoint(s) returned HTTP errors:\n")
        for name, status, detail in issues:
            print(f"    {name}: HTTP {status} — {detail}")
    else:
        print("\n  All endpoints returned HTTP 200.")
        empty = [
            name
            for name, path in all_endpoints.items()
            if name not in [i[0] for i in issues]
        ]
        # Already printed above, just summarize
    print()


if __name__ == "__main__":
    diagnose()
