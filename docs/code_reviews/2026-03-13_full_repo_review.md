# Code Review: oura-pipeline (Full Repo)

**Date:** 2026-03-13
**Status:** Needs Attention
**Files Reviewed:** 6 Python, 6 SQL/YAML, 1 TOML

---

## Summary

| Severity | Count |
| -------- | ----- |
| ERROR    | 3     |
| WARNING  | 6     |
| INFO     | 4     |

---

## ERRORS (Must Fix)

### E1. Bug: Return type mismatch in `run_local_callback_server`

**File:** `src/oura_oauth_cli.py:107-122`

The function signature declares `-> str | None` but actually returns a `(httpd, t)` tuple. This is a runtime type violation and will confuse any caller relying on the signature.

```python
# Current (broken)
def run_local_callback_server(redirect_uri: str) -> str | None:
    ...
    return httpd, t  # returns tuple, not str | None

# Fix
def run_local_callback_server(redirect_uri: str) -> tuple[HTTPServer, threading.Thread]:
    ...
    return httpd, t
```

### E2. SQL Injection via f-string interpolation

**File:** `src/dagster_project/defs/assets.py:23-30`

The `table` parameter in `_upsert_day` is directly interpolated into SQL via f-strings. While this is currently only called with hardcoded string literals, this pattern is dangerous if the function is ever reused or if table names come from external input.

```python
# Current (unsafe pattern)
con.execute(f"CREATE TABLE IF NOT EXISTS oura_raw.{table} ...")
con.execute(f"DELETE FROM oura_raw.{table} WHERE ...")
con.execute(f"INSERT INTO oura_raw.{table} SELECT ...")

# Fix: validate table name against allowlist
VALID_TABLES = {"sleep", "activity", "readiness", "spo2", "stress", "resilience",
                "heartrate", "sleep_periods", "sleep_time", "workouts",
                "sessions", "tags", "rest_mode_periods"}

def _upsert_day(con, table: str, rows: Iterable[Dict[str, Any]], day) -> int:
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table}")
    ...
```

### E3. No tests exist

**Directory:** `tests/`

The `tests/` directory is empty. There are no unit or integration tests for any of the Python code. The README references `uv run pytest` but there's nothing to run.

At minimum, tests should cover:

- `_upsert_day` logic (empty rows, deduplication, partition_date injection)
- `OuraAPI._get_access_token` (token expiry logic, refresh flow)
- `OuraTranslator` source mapping and group assignment

---

## WARNINGS (Should Fix)

### W1. Massive DRY violation in asset definitions

**File:** `src/dagster_project/defs/assets.py`

All 13 asset functions are near-identical copy-paste. Each follows the exact same pattern: get API, get DuckDB, get connection, get day, fetch, upsert, return string. Only the endpoint kind/table name differs.

```python
# Current: 13 copies of this pattern (257 lines)
@dg.asset(...)
def oura_sleep_raw(context):
    api = context.resources.oura_api
    duck = context.resources.duckdb
    con = duck.get_connection()
    day = _day(context)
    rows = api.fetch_daily("sleep", day, day)
    n = _upsert_day(con, "sleep", rows, day)
    return f"sleep:{day} rows={n}"

# Fix: factory function (~40 lines total)
def _make_daily_asset(kind: str, group: str = "oura_raw_daily"):
    @dg.asset(
        partitions_def=partitions,
        key=dg.AssetKey(["oura_raw", kind]),
        group_name=group,
        required_resource_keys={"oura_api", "duckdb"},
        kinds={"duckdb", "API"},
    )
    def _asset(context: dg.AssetExecutionContext) -> str:
        api: OuraAPI = context.resources.oura_api
        con: DuckDBResource = context.resources.duckdb.get_connection()
        day = _day(context)
        rows = api.fetch_daily(kind, day, day)
        count = _upsert_day(con, kind, rows, day)
        return f"{kind}:{day} rows={count}"
    _asset.__name__ = f"oura_{kind}_raw"
    return _asset

oura_sleep_raw = _make_daily_asset("sleep")
oura_activity_raw = _make_daily_asset("activity")
# ... etc
```

A similar factory could handle the granular assets that use specific `fetch_*` methods.

### W2. Incomplete dbt sources declaration

**File:** `dbt_oura/models/sources.yml`

Only 4 sources are declared (sleep, activity, readiness, heartrate), but Dagster creates 13 raw tables. Missing sources: `spo2`, `stress`, `resilience`, `sleep_periods`, `sleep_time`, `workouts`, `sessions`, `tags`, `rest_mode_periods`.

This means the `OuraTranslator.get_asset_key_for_source()` mapping only works for 4 of 13 tables, and dbt has no visibility into the other 9 raw tables.

### W3. Duplicate/unnecessary dependencies in `pyproject.toml`

**File:** `pyproject.toml:11-22`

- `dotenv` (line 18) and `python-dotenv` (line 17) are both listed. `dotenv` is a different, older package — likely a mistake. Only `python-dotenv` is needed.
- `pathlib` (line 19) is part of the Python stdlib since 3.4. No need to install it as a dependency.

### W4. Multiple imports on one line

**File:** `src/dagster_project/defs/resources.py:2`

```python
# Current
import os, time, json, requests, dagster as dg, duckdb

# Fix
import json
import os
import time

import dagster as dg
import duckdb
import requests
```

Ruff flagged this as E401 (auto-fixable with `ruff check --fix`).

### W5. Hardcoded DuckDB path inconsistency

**File:** `dbt_oura/profiles.yml:6`

The dbt profile hardcodes `path: data/oura.duckdb`, while Dagster reads the path from `EnvVar("DUCKDB_PATH")`. If someone sets `DUCKDB_PATH` to a different location, dbt and Dagster will target different databases.

Consider using an environment variable in the dbt profile:

```yaml
path: "{{ env_var('DUCKDB_PATH', 'data/oura.duckdb') }}"
```

### W6. DuckDB connection never closed

**File:** `src/dagster_project/defs/resources.py:108-111` and `assets.py` (all assets)

`DuckDBResource.get_connection()` returns a raw connection that is never explicitly closed. Each asset call creates a new connection via `duck.get_connection()` without any cleanup. Consider making `DuckDBResource` a context manager or using `yield` in a Dagster resource lifecycle.

---

## INFO (Consider Fixing)

### I1. Short/unclear variable names in `resources.py`

**File:** `src/dagster_project/defs/resources.py:32-60`

Several parameter and variable names are unnecessarily abbreviated:

| Current | Suggested          | Location    |
| ------- | ------------------ | ----------- |
| `t`     | `tokens`           | Line 32, 38 |
| `exp`   | `expires_in`       | Line 39     |
| `when`  | `obtained_at`      | Line 40     |
| `newt`  | `refreshed_tokens` | Line 57     |
| `tok`   | `access_token`     | Line 64     |
| `r`     | `response`         | Line 65     |

### I2. Missing type hints on public methods

**File:** `src/dagster_project/defs/resources.py:83-102`

All public `fetch_*` methods lack return type annotations:

```python
# Current
def fetch_heartrate(self, start: date, end: date):

# Fix
def fetch_heartrate(self, start: date, end: date) -> list[dict[str, Any]]:
```

Also `_upsert_day`'s `con` and `day` parameters lack type hints.

### I3. Local variables use SCREAMING_SNAKE_CASE

**File:** `src/oura_oauth_cli.py:127-131`

`CLIENT_ID`, `CLIENT_SECRET`, `REDIRECT_URI`, `SCOPES`, `TOKEN_PATH` are local variables inside `main()`, not module-level constants. Convention is `snake_case` for locals.

### I4. Redundant `CREATE SCHEMA` call

**File:** `src/dagster_project/defs/assets.py:21`

`_upsert_day` calls `CREATE SCHEMA IF NOT EXISTS oura_raw` on every invocation, even though `DuckDBResource.get_connection()` already does this (line 110 of resources.py). This is harmless but redundant.

---

## Architecture Notes

The overall architecture is clean and well-structured: Dagster orchestration with daily partitions, DuckDB as the local warehouse, dbt for transformations, and a custom translator bridging the two. The OAuth2 CLI is a nice standalone utility.

**Priorities for next steps:**

1. Fix the `run_local_callback_server` return type bug (E1)
2. Add table name validation to `_upsert_day` (E2)
3. Add tests (E3)
4. Refactor assets to use a factory pattern (W1)
5. Complete the dbt sources declaration (W2)
6. Clean up `pyproject.toml` dependencies (W3)
