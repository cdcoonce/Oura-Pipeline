# src/dagster_project/defs/resources.py
import base64
import json
import logging
import time
from datetime import date, timedelta
from typing import Any, Dict, Iterable

import dagster as dg
import requests
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.ouraring.com"
TOKEN_URL = "https://api.ouraring.com/oauth/token"

# v2 daily collection endpoint map
DAILY_MAP = {
    "sleep": "daily_sleep",
    "activity": "daily_activity",
    "readiness": "daily_readiness",
    "spo2": "daily_spo2",
    "stress": "daily_stress",
    "resilience": "daily_resilience",
}

# Oura API endpoints that treat end_date as exclusive (data up to but NOT
# including end_date). When start_date == end_date these return zero rows.
# Fix: pass end_date = start_date + 1 day so a single-day query works.
EXCLUSIVE_END_DATE_ENDPOINTS = frozenset(
    {
        "daily_activity",
        "daily_resilience",
        "sleep",  # sleep periods (granular)
        "workout",
        "session",
        "tag",
        "rest_mode_period",
    }
)


class SnowflakeResource(dg.ConfigurableResource):
    """Snowflake connection provider using key-pair authentication."""

    account: str
    user: str
    private_key: str  # base64-encoded PEM private key
    warehouse: str = "COMPUTE_WH"
    database: str = "OURA"
    schema_name: str = "OURA_RAW"
    role: str = "TRANSFORM"

    def _get_private_key_bytes(self) -> bytes:
        """Decode base64 PEM → DER bytes for snowflake-connector-python."""
        pem_bytes = base64.b64decode(self.private_key)
        pk = load_pem_private_key(pem_bytes, password=None)
        return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Open a Snowflake connection with key-pair auth."""
        logger.info(
            "Connecting to Snowflake account=%s warehouse=%s database=%s role=%s",
            self.account,
            self.warehouse,
            self.database,
            self.role,
        )
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            private_key=self._get_private_key_bytes(),
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_name,
            role=self.role,
        )


class OuraAPI(dg.ConfigurableResource):
    """OAuth2-backed Oura API with Snowflake-backed token storage."""

    client_id: str
    client_secret: str
    snowflake: SnowflakeResource

    def _get_token_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get a Snowflake connection for token operations."""
        return self.snowflake.get_connection()

    # ----- token helpers -----
    def _load_tokens(self) -> Dict[str, Any]:
        """Load the most recent OAuth tokens from Snowflake."""
        con = self._get_token_connection()
        try:
            cursor = con.cursor()
            cursor.execute(
                "SELECT token_data FROM OURA.CONFIG.OAUTH_TOKENS "
                "ORDER BY updated_at DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row is None:
                raise FileNotFoundError(
                    "No OAuth tokens found in OURA.CONFIG.OAUTH_TOKENS. "
                    "Seed tokens using the setup SQL from Phase 0."
                )
            token_data = row[0]
            if isinstance(token_data, str):
                return json.loads(token_data)
            return dict(token_data)
        finally:
            con.close()

    def _save_tokens(self, tokens: Dict[str, Any]) -> None:
        """Persist refreshed OAuth tokens to Snowflake."""
        con = self._get_token_connection()
        try:
            cursor = con.cursor()
            cursor.execute(
                "INSERT INTO OURA.CONFIG.OAUTH_TOKENS (token_data) "
                "SELECT PARSE_JSON(%s)",
                (json.dumps(tokens),),
            )
            logger.info("Saved refreshed OAuth tokens to Snowflake")
        finally:
            con.close()

    def _get_access_token(self) -> str:
        tokens = self._load_tokens()
        expires_in = int(tokens.get("expires_in", 0))
        obtained_at = int(tokens.get("obtained_at", 0))
        now = int(time.time())
        if obtained_at + expires_in - 60 > now and "access_token" in tokens:
            return tokens["access_token"]

        # refresh
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(
                f"OAuth token refresh failed ({resp.status_code}): "
                f"{resp.text}. "
                "The refresh token is likely expired or revoked. "
                "Re-run 'uv run python src/oura_oauth_cli.py' to get new "
                "tokens and re-seed them into OURA.CONFIG.OAUTH_TOKENS."
            )
        refreshed_tokens = resp.json()
        refreshed_tokens["obtained_at"] = int(time.time())
        self._save_tokens(refreshed_tokens)
        return refreshed_tokens["access_token"]

    # ----- request helper -----
    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        access_token = self._get_access_token()
        response = requests.get(
            f"{BASE_URL}{path}",
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=30,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            status = response.status_code
            body = response.text[:500]
            logger.error(
                "%s returned HTTP %s — body: %s",
                path,
                status,
                body,
            )
            # Auth errors should fail loudly so they're not masked
            if status in (401, 403):
                raise RuntimeError(
                    f"Oura API auth error on {path} (HTTP {status}): {body}. "
                    "Check OAuth token scopes and refresh token validity."
                )
            # Rate limiting — raise so Dagster can retry the partition
            if status == 429:
                raise RuntimeError(
                    f"Oura API rate limited on {path} (HTTP 429): {body}"
                )
            # Server errors — raise for Dagster retry
            if status >= 500:
                raise RuntimeError(
                    f"Oura API server error on {path} (HTTP {status}): {body}"
                )
            # Other client errors (404, 422, etc.) — log and return empty
            logger.warning(
                "%s returned HTTP %s — treating as empty data",
                path,
                status,
            )
            return {}
        return response.json()

    # ----- date helpers -----
    @staticmethod
    def _end_date_for(endpoint: str, end: date) -> date:
        """Adjust end_date for endpoints that treat it as exclusive.

        Some Oura v2 endpoints use an exclusive end_date — a query with
        start_date == end_date returns zero rows.  For those endpoints we
        add one day so single-day partition queries return the expected data.
        """
        if endpoint in EXCLUSIVE_END_DATE_ENDPOINTS:
            return end + timedelta(days=1)
        return end

    # ----- public API (daily) -----
    def fetch_daily(
        self, kind: str, start: date, end: date
    ) -> Iterable[Dict[str, Any]]:
        endpoint = DAILY_MAP.get(kind, kind)
        return self._get(
            f"/v2/usercollection/{endpoint}",
            {"start_date": start, "end_date": self._end_date_for(endpoint, end)},
        ).get("data", [])

    # ----- public API (granular / event-level) -----
    def fetch_heartrate(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/heartrate",
            {"start_date": start, "end_date": self._end_date_for("heartrate", end)},
        ).get("data", [])

    def fetch_sleep_periods(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/sleep",
            {"start_date": start, "end_date": self._end_date_for("sleep", end)},
        ).get("data", [])

    def fetch_sleep_time(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/sleep_time",
            {"start_date": start, "end_date": self._end_date_for("sleep_time", end)},
        ).get("data", [])

    def fetch_workouts(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/workout",
            {"start_date": start, "end_date": self._end_date_for("workout", end)},
        ).get("data", [])

    def fetch_sessions(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/session",
            {"start_date": start, "end_date": self._end_date_for("session", end)},
        ).get("data", [])

    def fetch_tags(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/tag",
            {"start_date": start, "end_date": self._end_date_for("tag", end)},
        ).get("data", [])

    def fetch_rest_mode_periods(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/rest_mode_period",
            {
                "start_date": start,
                "end_date": self._end_date_for("rest_mode_period", end),
            },
        ).get("data", [])
