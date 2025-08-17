# src/oura_client/client.py
import requests
from datetime import date
from typing import Dict, Any, Iterable
from .token_store import get_access_token

BASE = "https://api.ouraring.com"

DAILY_MAP = {
    "sleep": "daily_sleep",          # daily summaries
    "activity": "daily_activity",
    "readiness": "daily_readiness",
    # if you ever want sleep periods (not daily summary), call a separate method for "sleep" raw.
}

class OuraClient:
    def __init__(self) -> None:
        pass

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {get_access_token()}"}

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.get(f"{BASE}{path}", headers=self._headers(), params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def fetch_daily(self, kind: str, start: date, end: date) -> Iterable[Dict[str, Any]]:
        endpoint = DAILY_MAP.get(kind, kind)  # map to daily_* if known
        return self._get(f"/v2/usercollection/{endpoint}",
                         {"start_date": start, "end_date": end}).get("data", [])

    def fetch_heartrate(self, start: date, end: date) -> Iterable[Dict[str, Any]]:
        return self._get("/v2/usercollection/heartrate",
                         {"start_date": start, "end_date": end}).get("data", [])