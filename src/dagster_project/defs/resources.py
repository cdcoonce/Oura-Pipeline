# src/dagster_project/defs/resources.py
import json
import os
import time
from datetime import date
from typing import Any, Dict, Iterable

import dagster as dg
import duckdb
import requests

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


class OuraAPI(dg.ConfigurableResource):
    """OAuth2-backed Oura API (pulls/refreshes tokens automatically)."""

    client_id: str
    client_secret: str
    token_path: str = "data/tokens/oura_tokens.json"

    # ----- token helpers -----
    def _load_tokens(self) -> Dict[str, Any]:
        if not os.path.exists(self.token_path):
            raise FileNotFoundError(f"Token file not found: {self.token_path}")
        with open(self.token_path) as f:
            return json.load(f)

    def _save_tokens(self, tokens: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.token_path), exist_ok=True)
        with open(self.token_path, "w") as f:
            json.dump(tokens, f, indent=2)

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
        resp.raise_for_status()
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
        response.raise_for_status()
        return response.json()

    # ----- public API (daily) -----
    def fetch_daily(
        self, kind: str, start: date, end: date
    ) -> Iterable[Dict[str, Any]]:
        endpoint = DAILY_MAP.get(kind, kind)
        return self._get(
            f"/v2/usercollection/{endpoint}",
            {"start_date": start, "end_date": end},
        ).get("data", [])

    # ----- public API (granular / event-level) -----
    def fetch_heartrate(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/heartrate", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_sleep_periods(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/sleep", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_sleep_time(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/sleep_time", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_workouts(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/workout", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_sessions(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/session", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_tags(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/tag", {"start_date": start, "end_date": end}
        ).get("data", [])

    def fetch_rest_mode_periods(self, start: date, end: date) -> list[Dict[str, Any]]:
        return self._get(
            "/v2/usercollection/rest_mode_period",
            {"start_date": start, "end_date": end},
        ).get("data", [])


class DuckDBResource(dg.ConfigurableResource):
    """DuckDB connection provider."""

    db_path: str = "data/oura.duckdb"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.db_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS oura_raw;")
        return con
