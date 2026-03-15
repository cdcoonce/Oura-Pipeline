"""
Diagnose: compare single-day vs multi-day queries for empty endpoints.
This mimics exactly what the Dagster pipeline does (start_date == end_date).

Usage:
    uv run python scripts/diagnose_single_day.py
"""

import json
import os
import sys
import time
from datetime import date, timedelta

import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

BASE_URL = "https://api.ouraring.com"


def get_access_token() -> str:
    pat = os.getenv("OURA_ACCESS_TOKEN")
    if pat:
        return pat

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
        print("ERROR: No OAuth tokens found")
        sys.exit(1)
    tokens = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])

    expires_in = int(tokens.get("expires_in", 0))
    obtained_at = int(tokens.get("obtained_at", 0))
    now = int(time.time())
    if obtained_at + expires_in - 60 > now and "access_token" in tokens:
        return tokens["access_token"]

    resp = requests.post(
        "https://api.ouraring.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id": os.environ["OURA_CLIENT_ID"],
            "client_secret": os.environ["OURA_CLIENT_SECRET"],
        },
        timeout=30,
    )
    if not resp.ok:
        print(f"ERROR: Token refresh failed: {resp.text}")
        sys.exit(1)
    return resp.json()["access_token"]


# Endpoints that are empty in Snowflake but showed data in 7-day diagnostic
PROBLEM_ENDPOINTS = {
    "activity (daily)": "/v2/usercollection/daily_activity",
    "sleep_periods (granular)": "/v2/usercollection/sleep",
    "workouts (granular)": "/v2/usercollection/workout",
}

# Control group: endpoints that DO have data in Snowflake
CONTROL_ENDPOINTS = {
    "sleep (daily)": "/v2/usercollection/daily_sleep",
    "heartrate (granular)": "/v2/usercollection/heartrate",
}


def diagnose():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Test the last 4 days, one day at a time (like the pipeline does)
    today = date.today()
    test_dates = [today - timedelta(days=i) for i in range(1, 5)]

    all_endpoints = {**PROBLEM_ENDPOINTS, **CONTROL_ENDPOINTS}

    print(f"\n{'=' * 75}")
    print("  Single-Day Query Diagnostic (mimics pipeline behavior)")
    print(f"{'=' * 75}\n")

    for endpoint_name, path in all_endpoints.items():
        is_problem = endpoint_name in PROBLEM_ENDPOINTS
        label = "PROBLEM" if is_problem else "CONTROL"
        print(f"  [{label}] {endpoint_name}: {path}")

        for day in test_dates:
            # This is exactly what the pipeline does: start_date == end_date
            params = {"start_date": str(day), "end_date": str(day)}
            resp = requests.get(
                f"{BASE_URL}{path}",
                headers=headers,
                params=params,
                timeout=30,
            )

            if resp.ok:
                body = resp.json()
                data = body.get("data", [])
                row_count = len(data)
                icon = "✓" if row_count > 0 else "·"
                print(f"    {icon} {day}  HTTP {resp.status_code}  {row_count} rows")
                if row_count > 0 and is_problem:
                    # Show first row sample to verify it's real data
                    print(f"      sample: {json.dumps(data[0])[:120]}...")
            else:
                print(f"    ✗ {day}  HTTP {resp.status_code}  {resp.text[:80]}")

        print()

    # Also test: what does the pipeline see with date objects vs strings?
    print(f"{'=' * 75}")
    print("  Parameter type check (str vs date object)")
    print(f"{'=' * 75}\n")
    test_day = today - timedelta(days=1)
    for label, params in [
        ("str params", {"start_date": str(test_day), "end_date": str(test_day)}),
        ("date params", {"start_date": test_day, "end_date": test_day}),
    ]:
        resp = requests.get(
            f"{BASE_URL}/v2/usercollection/daily_activity",
            headers=headers,
            params=params,
            timeout=30,
        )
        data = resp.json().get("data", []) if resp.ok else []
        print(f"  {label}: HTTP {resp.status_code}, {len(data)} rows")
        print(f"    actual URL: {resp.url}")

    print()


if __name__ == "__main__":
    diagnose()
