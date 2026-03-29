# Phase 1: Core Migration (TDD)

**Scope:** Tasks 1–8 — dependencies, SnowflakeResource, token storage, `_upsert_day()` rewrite, definitions/assets/checks wiring, and report module migration.
**Depends on:** Phase 0 (Snowflake account, key pair, secrets all configured).
**Enables:** Phase 2 (dbt migration) and Phase 3 (cleanup).
**Methodology:** Every production function is preceded by a failing test. Red → Green → Refactor.

---

## 1.1 — Update Dependencies (Task 1)

> TDD exception: configuration file, no behavior to test.

### File: `pyproject.toml`

**Remove:**

```toml
"duckdb>=1.0",
"dbt-duckdb>=1.7",
```

**Add:**

```toml
"snowflake-connector-python[pandas]>=3.6",
"dbt-snowflake>=1.7",
"cryptography>=42.0",
```

**Keep for now (audit in Phase 3):**

- `polars>=0.20` — used by `report_data.py` (Polars DataFrames returned from queries).
- `pyarrow>=21.0.0` — transitive dependency. Audit after migration.

**Add pytest marker:**

```toml
[tool.pytest.ini_options]
markers = [
    "snowflake: tests requiring Snowflake credentials (deselect with -m 'not snowflake')",
]
```

### Steps

1. Edit `pyproject.toml`.
2. Run `uv lock && uv sync`.
3. Verify: `python -c "import snowflake.connector; print(snowflake.connector.__version__)"`.

---

## 1.2 — Test Infrastructure: Snowflake Fixtures

Before writing any production code, set up the test fixtures that all subsequent RED steps depend on.

### File: `tests/conftest.py`

```python
"""Shared test fixtures for Snowflake integration tests."""

import base64
import os

import pytest
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

SNOWFLAKE_AVAILABLE = "SNOWFLAKE_ACCOUNT" in os.environ


def _get_private_key_bytes() -> bytes:
    """Decode base64 PEM → DER bytes."""
    pem_bytes = base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY"])
    pk = load_pem_private_key(pem_bytes, password=None)
    return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


@pytest.fixture()
def snowflake_con():
    """Snowflake connection to OURA_TEST.OURA_RAW. Skipped if no credentials."""
    if not SNOWFLAKE_AVAILABLE:
        pytest.skip("Snowflake credentials not available")

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_get_private_key_bytes(),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="OURA_TEST",
        schema="OURA_RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM"),
    )
    yield con

    # Cleanup: drop any tables created during the test
    cursor = con.cursor()
    cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'OURA_RAW' AND table_catalog = 'OURA_TEST'"
    )
    for (table_name,) in cursor.fetchall():
        cursor.execute(f"DROP TABLE IF EXISTS oura_raw.{table_name}")
    con.close()
```

### Verify Fixtures Work

```bash
# Should skip gracefully if no creds, or connect if creds present
uv run pytest tests/conftest.py -v --collect-only
```

---

## 1.3 — RED: `_upsert_day()` Tests (Tasks 4–5)

Write failing tests for the new Snowflake-backed `_upsert_day()` **before** writing the function.

### File: `tests/test_upsert_day.py` — Full Rewrite

```python
"""Tests for _upsert_day() with Snowflake backend."""

import json
from datetime import date

import pytest

from dagster_project.defs.assets import VALID_TABLES, _upsert_day

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in __import__("os").environ,
    reason="Snowflake credentials not available",
)


class TestUpsertDayInsert:
    """Basic insert operations."""

    def test_creates_table_and_inserts_rows(self, snowflake_con):
        rows = [{"id": "abc", "score": 85, "day": "2024-06-01"}]
        count = _upsert_day(snowflake_con, "sleep", rows, date(2024, 6, 1))

        assert count == 1
        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 1

    def test_return_value_matches_row_count(self, snowflake_con):
        rows = [
            {"id": f"r{i}", "bpm": 60 + i, "timestamp": "2024-06-01T00:00:00"}
            for i in range(5)
        ]
        count = _upsert_day(snowflake_con, "heartrate", rows, date(2024, 6, 1))
        assert count == 5

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.heartrate")
        assert cursor.fetchone()[0] == 5

    def test_raw_data_is_valid_json(self, snowflake_con):
        """Verify raw_data is stored as parseable VARIANT."""
        rows = [{"id": "x1", "score": 90, "day": "2024-06-01"}]
        _upsert_day(snowflake_con, "sleep", rows, date(2024, 6, 1))

        cursor = snowflake_con.cursor()
        cursor.execute(
            "SELECT raw_data:id::varchar, raw_data:score::int "
            "FROM oura_raw.sleep"
        )
        row = cursor.fetchone()
        assert row[0] == "x1"
        assert row[1] == 90


class TestUpsertDayEmpty:
    """Empty input handling."""

    def test_empty_list_returns_zero(self, snowflake_con):
        count = _upsert_day(snowflake_con, "sleep", [], date(2024, 6, 1))
        assert count == 0

    def test_empty_creates_table(self, snowflake_con):
        _upsert_day(snowflake_con, "activity", [], date(2024, 6, 1))

        cursor = snowflake_con.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'OURA_RAW' AND table_name = 'ACTIVITY'"
        )
        assert cursor.fetchone()[0] == 1

    def test_none_rows_returns_zero(self, snowflake_con):
        count = _upsert_day(snowflake_con, "sleep", None, date(2024, 6, 1))
        assert count == 0


class TestUpsertDayIdempotent:
    """DELETE + INSERT idempotency."""

    def test_same_day_replaces_rows(self, snowflake_con):
        day = date(2024, 6, 1)
        _upsert_day(snowflake_con, "sleep", [{"id": "old"}], day)
        _upsert_day(snowflake_con, "sleep", [{"id": "new"}], day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT raw_data:id::varchar FROM oura_raw.sleep")
        ids = [r[0] for r in cursor.fetchall()]
        assert ids == ["new"]

    def test_different_days_coexist(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [{"id": "d1"}], date(2024, 6, 1))
        _upsert_day(snowflake_con, "sleep", [{"id": "d2"}], date(2024, 6, 2))

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 2

    def test_rerun_does_not_duplicate(self, snowflake_con):
        day = date(2024, 6, 1)
        row = [{"id": "same"}]
        _upsert_day(snowflake_con, "sleep", row, day)
        _upsert_day(snowflake_con, "sleep", row, day)
        _upsert_day(snowflake_con, "sleep", row, day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 1


class TestUpsertDayPartitionDate:
    """Partition date injection."""

    def test_adds_partition_date(self, snowflake_con):
        day = date(2024, 6, 15)
        _upsert_day(snowflake_con, "sleep", [{"id": "p1"}], day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT partition_date FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == day


class TestUpsertDayInvalidTable:
    """Table name validation (SQL injection guard)."""

    def test_invalid_table_raises(self, snowflake_con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(snowflake_con, "not_a_table", [{"x": 1}], date(2024, 6, 1))

    def test_sql_injection_attempt(self, snowflake_con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(
                snowflake_con,
                "sleep; DROP TABLE oura_raw.sleep; --",
                [{"x": 1}],
                date(2024, 6, 1),
            )
```

### Verify RED

```bash
uv run pytest tests/test_upsert_day.py -v
# Expected: ALL FAIL (ImportError or TypeError — _upsert_day still uses DuckDB)
```

---

## 1.4 — GREEN: `_upsert_day()` Implementation (Tasks 4–5)

Write minimal code to make the tests from 1.3 pass.

### File: `src/dagster_project/defs/assets.py`

**Remove:**

- `import polars as pl`
- The entire `TABLE_SCHEMAS` dictionary (~200 lines)

**Add/Rewrite `_upsert_day()`:**

```python
import json
from collections.abc import Iterable
from datetime import date
from typing import Any

def _upsert_day(
    con: snowflake.connector.SnowflakeConnection,
    table: str,
    rows: Iterable[dict[str, Any]],
    day: date,
) -> int:
    """
    Idempotent load into oura_raw.<table> using DELETE + batched INSERT.

    Each row is stored as a VARIANT (raw_data) plus a partition_date DATE.
    Uses executemany() for efficient batching — critical for heartrate data
    which has ~2000 rows/day.

    Parameters
    ----------
    con : SnowflakeConnection
        Active Snowflake connection.
    table : str
        Target table name (must be in VALID_TABLES whitelist).
    rows : Iterable[dict]
        API response rows to insert.
    day : date
        Partition date for idempotent upsert.

    Returns
    -------
    int
        Number of rows inserted.
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table!r}")

    cursor = con.cursor()
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS oura_raw.{table} "
        f"(raw_data VARIANT, partition_date DATE)"
    )
    cursor.execute(
        f"DELETE FROM oura_raw.{table} WHERE partition_date = %s",
        (day,),
    )

    row_list = list(rows) if rows else []
    if not row_list:
        return 0

    params = [(json.dumps(row), day) for row in row_list]
    cursor.executemany(
        f"INSERT INTO oura_raw.{table} (raw_data, partition_date) "
        f"SELECT PARSE_JSON(%s), %s",
        params,
    )
    return len(row_list)
```

### Verify GREEN

```bash
uv run pytest tests/test_upsert_day.py -v
# Expected: ALL PASS
```

---

## 1.5 — RED: SnowflakeResource Tests (Task 2)

Write tests for `SnowflakeResource` before implementing it.

### File: `tests/test_snowflake_resource.py` (NEW)

```python
"""Tests for SnowflakeResource."""

import os

import pytest

from dagster_project.defs.resources import SnowflakeResource

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in os.environ,
    reason="Snowflake credentials not available",
)


class TestSnowflakeResource:
    def test_get_connection_returns_open_connection(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key=os.environ["SNOWFLAKE_PRIVATE_KEY"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "OURA"),
            role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM"),
        )
        con = resource.get_connection()
        try:
            cursor = con.cursor()
            cursor.execute("SELECT CURRENT_ROLE()")
            role = cursor.fetchone()[0]
            assert role == "TRANSFORM"
        finally:
            con.close()

    def test_get_connection_sets_database(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key=os.environ["SNOWFLAKE_PRIVATE_KEY"],
            database="OURA_TEST",
        )
        con = resource.get_connection()
        try:
            cursor = con.cursor()
            cursor.execute("SELECT CURRENT_DATABASE()")
            assert cursor.fetchone()[0] == "OURA_TEST"
        finally:
            con.close()

    def test_invalid_private_key_raises(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key="not-valid-base64-pem",
        )
        with pytest.raises(Exception):
            resource.get_connection()
```

### Verify RED

```bash
uv run pytest tests/test_snowflake_resource.py -v
# Expected: ImportError — SnowflakeResource doesn't exist yet
```

---

## 1.6 — GREEN: SnowflakeResource Implementation (Task 2)

### File: `src/dagster_project/defs/resources.py`

**Remove:** `import duckdb`, `from pathlib import Path`, entire `DuckDBResource` class.

**Add:**

```python
import base64
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    NoEncryption,
    PrivateFormat,
)

class SnowflakeResource(dg.ConfigurableResource):
    """Snowflake connection provider using key-pair authentication."""

    account: str
    user: str
    private_key: str          # base64-encoded PEM private key
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
            self.account, self.warehouse, self.database, self.role,
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
```

### Verify GREEN

```bash
uv run pytest tests/test_snowflake_resource.py -v
# Expected: ALL PASS
```

---

## 1.7 — RED: OuraAPI Token Storage Tests (Task 3) — LAUNCH BLOCKER

Write tests for the Snowflake-backed `_load_tokens()` / `_save_tokens()` before rewriting them.

### File: `tests/test_oura_api.py` — Rewrite Token Tests

Token tests use mocks (appropriate here — we don't want to read/write production OAuth tokens, and the test database doesn't have a `CONFIG` schema).

```python
"""Tests for OuraAPI with Snowflake-backed token storage."""

import json
import time
from unittest.mock import MagicMock

import pytest

from dagster_project.defs.resources import OuraAPI


class TestLoadTokens:
    def test_returns_token_dict_from_snowflake(self):
        """_load_tokens queries Snowflake and returns parsed JSON."""
        token_json = json.dumps({
            "access_token": "test_access",
            "refresh_token": "test_refresh",
            "expires_in": 86400,
            "obtained_at": int(time.time()),
        })
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (token_json,)
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        tokens = api._load_tokens()
        assert tokens["access_token"] == "test_access"
        assert tokens["refresh_token"] == "test_refresh"
        mock_con.close.assert_called_once()

    def test_raises_when_no_tokens_exist(self):
        """_load_tokens raises FileNotFoundError when table is empty."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        with pytest.raises(FileNotFoundError, match="No OAuth tokens"):
            api._load_tokens()
        mock_con.close.assert_called_once()


class TestSaveTokens:
    def test_inserts_token_json_into_snowflake(self):
        """_save_tokens INSERTs PARSE_JSON'd token data."""
        mock_cursor = MagicMock()
        mock_con = MagicMock()
        mock_con.cursor.return_value = mock_cursor

        api = _make_api()
        api._get_token_connection = MagicMock(return_value=mock_con)

        tokens = {"access_token": "new", "refresh_token": "new_r"}
        api._save_tokens(tokens)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "PARSE_JSON" in call_args[0][0]
        assert json.dumps(tokens) in call_args[0][1]
        mock_con.close.assert_called_once()


class TestGetAccessToken:
    def test_returns_valid_token_without_refresh(self):
        """Valid, non-expired token returned directly."""
        now = int(time.time())
        tokens = {
            "access_token": "valid_token",
            "refresh_token": "refresh",
            "expires_in": 86400,
            "obtained_at": now,
        }
        api = _make_api()
        api._load_tokens = MagicMock(return_value=tokens)
        api._save_tokens = MagicMock()

        result = api._get_access_token()
        assert result == "valid_token"
        api._save_tokens.assert_not_called()

    def test_refreshes_expired_token(self, mocker):
        """Expired token triggers refresh via POST."""
        old_tokens = {
            "access_token": "old",
            "refresh_token": "refresh_tok",
            "expires_in": 86400,
            "obtained_at": 0,  # long expired
        }
        new_tokens = {
            "access_token": "new_access",
            "refresh_token": "new_refresh",
            "expires_in": 86400,
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = new_tokens
        mock_resp.raise_for_status = MagicMock()

        mocker.patch("requests.post", return_value=mock_resp)

        api = _make_api()
        api._load_tokens = MagicMock(return_value=old_tokens)
        api._save_tokens = MagicMock()

        result = api._get_access_token()
        assert result == "new_access"
        api._save_tokens.assert_called_once()


def _make_api() -> OuraAPI:
    """Create an OuraAPI instance with dummy config for unit tests."""
    # Use __new__ + manual field setting to avoid ConfigurableResource init
    api = OuraAPI.__new__(OuraAPI)
    api.client_id = "test_id"
    api.client_secret = "test_secret"
    return api
```

### Verify RED

```bash
uv run pytest tests/test_oura_api.py -v
# Expected: FAIL — _load_tokens still reads from file, _get_token_connection doesn't exist
```

---

## 1.8 — GREEN: OuraAPI Token Storage Implementation (Task 3)

### File: `src/dagster_project/defs/resources.py`

Rewrite `OuraAPI` to use Snowflake-backed token storage.

**Replace `token_path` with `snowflake` nested resource:**

```python
class OuraAPI(dg.ConfigurableResource):
    """Oura Ring API v2 client with Snowflake-backed OAuth token storage."""

    client_id: str
    client_secret: str
    snowflake: SnowflakeResource

    def _get_token_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get a Snowflake connection for token operations."""
        return self.snowflake.get_connection()
```

**Rewrite `_load_tokens()`:**

```python
def _load_tokens(self) -> dict[str, Any]:
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
```

**Rewrite `_save_tokens()`:**

```python
def _save_tokens(self, tokens: dict[str, Any]) -> None:
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
```

### Verify GREEN

```bash
uv run pytest tests/test_oura_api.py -v
# Expected: ALL PASS
```

---

## 1.9 — RED: Asset Check Tests (Task 8)

Write tests for the Snowflake-backed `_check_row_count` before updating it.

### File: `tests/test_asset_checks.py` — Rewrite

```python
"""Tests for row count asset checks with Snowflake backend."""

from datetime import date

import pytest

from dagster_project.defs.assets import _upsert_day
from dagster_project.defs.checks import _check_row_count, make_row_count_check

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in __import__("os").environ,
    reason="Snowflake credentials not available",
)


class TestRowCountCheck:
    def test_returns_checks_definition(self):
        check = make_row_count_check("sleep")
        assert check is not None

    def test_warns_when_table_missing(self, snowflake_con):
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.severity.name == "WARN"

    def test_warns_when_table_empty(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [], date(2024, 6, 1))
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.severity.name == "WARN"

    def test_passes_with_data(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [{"id": "t1"}], date(2024, 6, 1))
        result = _check_row_count(snowflake_con, "sleep")
        assert result.passed is True
        assert result.metadata["row_count"].value == 1
```

### Verify RED

```bash
uv run pytest tests/test_asset_checks.py -v
# Expected: FAIL — _check_row_count still uses DuckDB SQL syntax
```

---

## 1.10 — GREEN: Asset Checks Implementation (Task 8)

### File: `src/dagster_project/defs/checks.py`

```python
import snowflake.connector
from .resources import SnowflakeResource

def _check_row_count(
    con: snowflake.connector.SnowflakeConnection,
    table: str,
) -> dg.AssetCheckResult:
    """Run a row count check against a Snowflake table."""
    cursor = con.cursor()

    # Snowflake stores unquoted identifiers in UPPERCASE
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'OURA_RAW' AND table_name = %s",
        (table.upper(),),
    )
    exists = cursor.fetchone()[0] > 0

    if not exists:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"row_count": dg.MetadataValue.int(0)},
            description=f"Table oura_raw.{table} does not exist yet.",
        )

    cursor.execute(f"SELECT COUNT(*) FROM oura_raw.{table}")
    count = cursor.fetchone()[0]

    if count == 0:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"row_count": dg.MetadataValue.int(count)},
            description=f"Table oura_raw.{table} exists but has no rows.",
        )

    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(count)},
    )


def make_row_count_check(table: str) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=dg.AssetKey(["oura_raw", table]),
        description=f"Checks that oura_raw.{table} has at least one row.",
    )
    def _check(snowflake: SnowflakeResource) -> dg.AssetCheckResult:
        con = snowflake.get_connection()
        result = _check_row_count(con, table)
        con.close()
        return result
    return _check
```

### Verify GREEN

```bash
uv run pytest tests/test_asset_checks.py -v
# Expected: ALL PASS
```

---

## 1.11 — RED: Report Data Tests (Task — Report Migration)

Write tests for the Snowflake-backed report data functions before rewriting them.

### File: `tests/test_report_data.py` — Rewrite

Report data functions are thin SQL wrappers. Mock the Snowflake cursor (appropriate — we're testing the fetch/transform logic, not Snowflake SQL execution).

```python
"""Tests for report data fetching with Snowflake backend."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest

from dagster_project.reports.report_data import (
    fetch_sleep_detail_for_period,
    fetch_wellness_for_period,
    fetch_workout_summary_for_period,
)


class TestFetchWellnessForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"DAY": [date(2024, 6, 1)], "READINESS_SCORE": [85], "STEPS": [8000]}
        )
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert isinstance(result, pl.DataFrame)

    def test_lowercases_column_names(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"DAY": [date(2024, 6, 1)], "READINESS_SCORE": [85]}
        )
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert "day" in result.columns
        assert "readiness_score" in result.columns
        assert "DAY" not in result.columns

    def test_uses_parameterized_query(self):
        mock_cursor, mock_con = _mock_snowflake_result({"DAY": []})
        fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        call_args = mock_cursor.execute.call_args
        assert "%s" in call_args[0][0]
        assert date(2024, 6, 1) in call_args[0][1]

    def test_empty_result_returns_empty_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result({"DAY": [], "STEPS": []})
        result = fetch_wellness_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert len(result) == 0


class TestFetchSleepDetailForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"ID": ["s1"], "DAY": [date(2024, 6, 1)]}
        )
        result = fetch_sleep_detail_for_period(mock_con, date(2024, 6, 1), date(2024, 6, 7))
        assert isinstance(result, pl.DataFrame)
        assert "id" in result.columns


class TestFetchWorkoutSummaryForPeriod:
    def test_returns_polars_dataframe(self):
        mock_cursor, mock_con = _mock_snowflake_result(
            {"ID": ["w1"], "DAY": [date(2024, 6, 1)], "WORKOUT_ACTIVITY": ["running"]}
        )
        result = fetch_workout_summary_for_period(
            mock_con, date(2024, 6, 1), date(2024, 6, 7)
        )
        assert isinstance(result, pl.DataFrame)
        assert "workout_activity" in result.columns


def _mock_snowflake_result(data: dict) -> tuple[MagicMock, MagicMock]:
    """Create a mock Snowflake connection that returns the given data."""
    mock_cursor = MagicMock()
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame(data)
    mock_con = MagicMock()
    mock_con.cursor.return_value = mock_cursor
    return mock_cursor, mock_con
```

### Verify RED

```bash
uv run pytest tests/test_report_data.py -v
# Expected: FAIL — report_data.py still imports DuckDBPyConnection, uses fetch_arrow_table()
```

---

## 1.12 — GREEN: Report Module Implementation

### File: `src/dagster_project/reports/report_data.py`

Full rewrite — replace DuckDB connection type and query execution:

```python
"""Data fetching functions for health report generation.

Queries Snowflake mart and staging tables for a given date range
and returns typed Polars DataFrames.
"""

from datetime import date

import polars as pl
import snowflake.connector


def fetch_wellness_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Fetch daily wellness data for a date range."""
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_marts.fact_daily_wellness "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)


def fetch_sleep_detail_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Fetch sleep detail data for a date range."""
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_marts.fact_sleep_detail "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)


def fetch_workout_summary_for_period(
    con: snowflake.connector.SnowflakeConnection,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Fetch workout data for a date range."""
    cursor = con.cursor()
    cursor.execute(
        "SELECT * FROM oura_staging.stg_workouts "
        "WHERE day BETWEEN %s AND %s ORDER BY day",
        (start_date, end_date),
    )
    pdf = cursor.fetch_pandas_all()
    pdf.columns = [c.lower() for c in pdf.columns]
    return pl.from_pandas(pdf)
```

### File: `src/dagster_project/defs/report_assets.py`

```python
# Replace import:
from .resources import SnowflakeResource  # was DuckDBResource

# Update signatures:
def _generate_and_send_report(
    context, snowflake: SnowflakeResource, ses, period_type, start_date, end_date
):
    con = snowflake.get_connection()
    # ... rest unchanged

def weekly_health_report(context, snowflake: SnowflakeResource, ses):
    ...

def monthly_health_report(context, snowflake: SnowflakeResource, ses):
    ...
```

### Verify GREEN

```bash
uv run pytest tests/test_report_data.py -v
# Expected: ALL PASS
```

---

## 1.13 — Wire Up: definitions.py + Asset Factories (Tasks 6–7)

> These are wiring changes — updating parameter names and imports. No new behavior to test beyond what's already covered by the `_upsert_day()` and check tests.

### File: `src/dagster_project/definitions.py`

- Import `SnowflakeResource` instead of `DuckDBResource`.
- Replace `"duckdb"` resource key with `"snowflake"`.
- Add `snowflake` nested resource to `OuraAPI` config.
- Remove `OURA_TOKEN_PATH` reference.

```python
from .defs.resources import SnowflakeResource, OuraAPI

resources={
    "snowflake": SnowflakeResource(
        account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
        user=dg.EnvVar("SNOWFLAKE_USER"),
        private_key=dg.EnvVar("SNOWFLAKE_PRIVATE_KEY"),
        warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=dg.EnvVar("SNOWFLAKE_DATABASE"),
        role=dg.EnvVar("SNOWFLAKE_ROLE"),
    ),
    "oura_api": OuraAPI(
        client_id=dg.EnvVar("OURA_CLIENT_ID"),
        client_secret=dg.EnvVar("OURA_CLIENT_SECRET"),
        snowflake=SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("SNOWFLAKE_USER"),
            private_key=dg.EnvVar("SNOWFLAKE_PRIVATE_KEY"),
            warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=dg.EnvVar("SNOWFLAKE_DATABASE"),
            role=dg.EnvVar("SNOWFLAKE_ROLE"),
        ),
    ),
    # dbt and ses unchanged
}
```

### File: `src/dagster_project/defs/assets.py` — Factory Updates

- Replace `duckdb` param with `snowflake` in `_make_daily_asset()` and `_make_granular_asset()`.
- Update `kinds={"snowflake", "API"}`.
- Update imports.

### Verify

```bash
uv run pytest -v
# Expected: ALL PASS (full suite)
```

---

## 1.14 — Update GitHub Actions Workflows

> TDD exception: configuration files.

### Files: `.github/workflows/deploy.yml`, `.github/workflows/branch_deployments.yml`

Add Snowflake env vars to the dbt parse step:

```yaml
env:
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
  SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
  SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
  SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
```

---

## Implementation Order (TDD Sequence)

```
1.1   Dependencies (pyproject.toml)              config — no test needed
1.2   Test fixtures (conftest.py)                infrastructure for all tests
1.3   RED:   _upsert_day() tests                 ← tests written, ALL FAIL
1.4   GREEN: _upsert_day() implementation        ← minimal code, ALL PASS
1.5   RED:   SnowflakeResource tests             ← tests written, ALL FAIL
1.6   GREEN: SnowflakeResource implementation    ← minimal code, ALL PASS
1.7   RED:   OuraAPI token tests                 ← tests written, ALL FAIL
1.8   GREEN: OuraAPI token implementation        ← minimal code, ALL PASS
1.9   RED:   Asset check tests                   ← tests written, ALL FAIL
1.10  GREEN: Asset check implementation          ← minimal code, ALL PASS
1.11  RED:   Report data tests                   ← tests written, ALL FAIL
1.12  GREEN: Report module implementation        ← minimal code, ALL PASS
1.13  Wire up definitions + asset factories      wiring — covered by existing tests
1.14  GitHub Actions workflows                   config — no test needed
```

**Rule:** Never proceed from a RED step to the next RED step. Every RED must be followed by its GREEN.

---

## Files Modified in Phase 1

| File                                         | Action                                                | TDD Step |
| -------------------------------------------- | ----------------------------------------------------- | -------- |
| `pyproject.toml`                             | Remove duckdb deps, add snowflake deps, add marker    | 1.1      |
| `tests/conftest.py`                          | Snowflake connection fixture                          | 1.2      |
| `tests/test_upsert_day.py`                   | Full rewrite (RED)                                    | 1.3      |
| `src/dagster_project/defs/assets.py`         | Rewrite `_upsert_day()`, remove TABLE_SCHEMAS (GREEN) | 1.4      |
| `tests/test_snowflake_resource.py`           | **NEW** (RED)                                         | 1.5      |
| `src/dagster_project/defs/resources.py`      | Replace DuckDBResource (GREEN)                        | 1.6      |
| `tests/test_oura_api.py`                     | Rewrite token tests (RED)                             | 1.7      |
| `src/dagster_project/defs/resources.py`      | Rewrite OuraAPI token methods (GREEN)                 | 1.8      |
| `tests/test_asset_checks.py`                 | Rewrite (RED)                                         | 1.9      |
| `src/dagster_project/defs/checks.py`         | Update for Snowflake (GREEN)                          | 1.10     |
| `tests/test_report_data.py`                  | Rewrite (RED)                                         | 1.11     |
| `src/dagster_project/reports/report_data.py` | Rewrite for Snowflake cursor (GREEN)                  | 1.12     |
| `src/dagster_project/defs/report_assets.py`  | Replace DuckDBResource (GREEN)                        | 1.12     |
| `src/dagster_project/definitions.py`         | Rewire resources                                      | 1.13     |
| `src/dagster_project/defs/assets.py`         | Update factories                                      | 1.13     |
| `.github/workflows/deploy.yml`               | Add Snowflake env vars                                | 1.14     |
| `.github/workflows/branch_deployments.yml`   | Add Snowflake env vars                                | 1.14     |

## Full Suite Verification

```bash
# After all Phase 1 steps:
uv run pytest -v
# Expected: ALL PASS

# Without Snowflake credentials (fast mode):
uv run pytest -m "not snowflake" -v
```
