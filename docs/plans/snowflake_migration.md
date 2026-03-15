# Snowflake Migration Plan

## Goal

Replace DuckDB (file-based, ephemeral in Dagster Cloud) with Snowflake so data persists across serverless runs.

## Design Decisions

### Raw layer approach: VARIANT columns

Instead of replicating DuckDB's typed STRUCT schemas in Snowflake, land each API response as a **VARIANT** (semi-structured JSON) column plus `partition_date DATE`. This is idiomatic Snowflake and:

- Eliminates brittle schema maintenance when Oura adds new API fields
- Simplifies `_upsert_day()` — no more `TABLE_SCHEMAS` dict
- Lets dbt staging models handle all type extraction via `:` notation

Each raw table becomes: `(raw_data VARIANT, partition_date DATE)`

**Trade-off:** VARIANT paths that reference nonexistent fields return NULL silently (unlike DuckDB STRUCTs which error). Mitigated by dbt `not_null` tests on key columns (see task 11).

### Connection approach: snowflake-connector-python

Use `snowflake-connector-python` directly (same pattern as current DuckDBResource). No need for SQLAlchemy or dagster-snowflake — keeps it simple and consistent.

### Authentication: key-pair

Use Snowflake key-pair auth (no password to rotate, works well in CI/CD). Private key stored as a Dagster Cloud secret env var (base64-encoded PEM).

**Implementation note:** The `snowflake-connector-python` `private_key` parameter expects DER-encoded bytes, not a PEM string. Use `cryptography.hazmat.primitives.serialization` to load the PEM and extract DER bytes:

```python
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, NoEncryption, PrivateFormat,
)
import base64

pem_bytes = base64.b64decode(self.private_key)
pk = load_pem_private_key(pem_bytes, password=None)
der_bytes = pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
```

### Token persistence: Snowflake table

OAuth tokens are stored in a `config.oauth_tokens` Snowflake table instead of a local JSON file. This is a **launch blocker** — without it, `OuraAPI._load_tokens()` raises `FileNotFoundError` on every run in Dagster Cloud's ephemeral filesystem.

---

## Pre-flight Checklist (before first deploy)

Before merging the migration PR, ensure:

- [ ] Snowflake account created and setup SQL executed (task 16)
- [ ] Key pair generated (`openssl genrsa`, extract public key, assign to SF user)
- [ ] Dagster Cloud env vars configured: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_ROLE`, `OURA_CLIENT_ID`, `OURA_CLIENT_SECRET`
- [ ] GitHub Actions secrets added: same Snowflake vars (for CI `dbt parse`)
- [ ] Initial OAuth tokens seeded into `config.oauth_tokens` table manually

---

## Tasks

### 1. Update dependencies (`pyproject.toml`)

**Remove:**

- `duckdb>=1.0`
- `dbt-duckdb>=1.7`

**Add:**

- `snowflake-connector-python[pandas]>=3.6`
- `dbt-snowflake>=1.7`
- `cryptography>=42.0` (for private key parsing; may already be a transitive dep)

**Keep for now (audit after migration is stable):**

- `polars>=0.20` — no longer used by `_upsert_day()` but may be used elsewhere
- `pyarrow>=21.0.0` — same; audit after migration

### 2. Create `SnowflakeResource` (replace `DuckDBResource`)

In `src/dagster_project/defs/resources.py`:

```python
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, NoEncryption, PrivateFormat,
)
import base64

class SnowflakeResource(dg.ConfigurableResource):
    """Snowflake connection provider."""
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
        logger.info(
            "Connecting to Snowflake account=%s warehouse=%s database=%s role=%s",
            self.account, self.warehouse, self.database, self.role,
        )
        con = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            private_key=self._get_private_key_bytes(),
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_name,
            role=self.role,
        )
        return con
```

### 3. Migrate OuraAPI token storage to Snowflake

**LAUNCH BLOCKER** — must be implemented for the pipeline to work in Dagster Cloud.

Replace file-based `_load_tokens()` / `_save_tokens()` with Snowflake-backed storage:

- Create table: `config.oauth_tokens (token_data VARIANT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP())`
- `_load_tokens()`: `SELECT token_data FROM config.oauth_tokens ORDER BY updated_at DESC LIMIT 1`
- `_save_tokens()`: `INSERT INTO config.oauth_tokens (token_data) SELECT parse_json(%s)`
- `OuraAPI` needs access to a Snowflake connection — pass `SnowflakeResource` as a dependency or accept a connection parameter

**Schema setup** (add to task 16):

```sql
CREATE SCHEMA IF NOT EXISTS OURA.CONFIG;
GRANT USAGE ON SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
```

**Seed initial tokens** (one-time manual step):

```sql
INSERT INTO config.oauth_tokens (token_data)
SELECT parse_json('<paste JSON from local data/tokens/oura_tokens.json>');
```

### 4. Rewrite `_upsert_day()` for Snowflake

Current approach: Polars DataFrame → DuckDB `register("tmp_df")` → DELETE/INSERT.

New approach: JSON serialization → batched `executemany()` → DELETE + INSERT.

```python
def _upsert_day(con, table: str, rows: Iterable[Dict[str, Any]], day: date) -> int:
    """Idempotent load into oura_raw.<table> using DELETE + batched INSERT."""
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table!r}")
    cursor = con.cursor()
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS oura_raw.{table} "
        f"(raw_data VARIANT, partition_date DATE)"
    )
    cursor.execute(
        f"DELETE FROM oura_raw.{table} WHERE partition_date = %s", (day,)
    )
    row_list = list(rows) if rows else []
    if not row_list:
        return 0
    params = [(json.dumps(row), day) for row in row_list]
    cursor.executemany(
        f"INSERT INTO oura_raw.{table} (raw_data, partition_date) "
        f"SELECT parse_json(%s), %s",
        params,
    )
    return len(row_list)
```

**Why `executemany()`:** Heartrate data has ~2000 rows/day. Row-by-row INSERT would mean 2000 network round-trips per partition. `executemany()` batches these into a single request. Over a 400-day backfill, this avoids 800K individual INSERTs for heartrate alone.

### 5. Remove `TABLE_SCHEMAS` dict

No longer needed — raw tables are just `(raw_data VARIANT, partition_date DATE)`.

### 6. Update `definitions.py`

- Import `SnowflakeResource` instead of `DuckDBResource`
- Wire new config fields from env vars:

```python
"snowflake": SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    user=dg.EnvVar("SNOWFLAKE_USER"),
    private_key=dg.EnvVar("SNOWFLAKE_PRIVATE_KEY"),
    warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
    database=dg.EnvVar("SNOWFLAKE_DATABASE"),
    role=dg.EnvVar("SNOWFLAKE_ROLE"),
),
```

### 7. Update asset factories (`assets.py`)

- Change resource parameter from `duckdb: DuckDBResource` → `snowflake: SnowflakeResource`
- Change `kinds={"duckdb", "API"}` → `kinds={"snowflake", "API"}`
- Update `_upsert_day()` calls (connection is now a Snowflake connection)

### 8. Update asset checks (`checks.py`)

- Change resource from `duckdb: DuckDBResource` → `snowflake: SnowflakeResource`
- Update `information_schema` query for Snowflake syntax:

```sql
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'OURA_RAW' AND table_name = %s
```

Note: Snowflake uses UPPERCASE identifiers by default. Table names in queries to `information_schema` must match the case used at creation time.

### 9. Update dbt profile (`dbt_oura/profiles.yml`)

```yaml
oura_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH', '') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORM') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'OURA') }}"
      schema: oura_marts
      threads: 4
```

Update `dbt_project.yml` profile reference from `oura_duckdb` → `oura_snowflake`.

### 10. Update dbt staging models (STRUCT → VARIANT)

Since raw data is now `VARIANT`, ALL 13 staging models change from:

```sql
SELECT id, day::date as day, score ...
FROM {{ source('oura_raw', 'sleep') }}
```

To:

```sql
SELECT
    raw_data:id::varchar as id,
    raw_data:day::date as day,
    raw_data:score::int as score,
    ...
FROM {{ source('oura_raw', 'sleep') }}
```

Models with STRUCT access also change dot notation to colon notation:

| DuckDB syntax                  | Snowflake syntax                             |
| ------------------------------ | -------------------------------------------- |
| `contributors.deep_sleep`      | `raw_data:contributors:deep_sleep::int`      |
| `spo2_percentage.average`      | `raw_data:spo2_percentage:average::float`    |
| `optimal_bedtime.start_offset` | `raw_data:optimal_bedtime:start_offset::int` |

### 11. Add dbt `not_null` tests on staging models

**Why:** VARIANT paths that reference nonexistent fields return NULL silently. With DuckDB's typed STRUCTs, a typo would fail at query time. dbt `not_null` tests restore that safety net.

Add `schema.yml` files (or extend existing ones) in `dbt_oura/models/staging/` with `not_null` tests on key columns for each staging model:

```yaml
models:
  - name: stg_sleep
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: score
        tests: [not_null]
```

Apply to all 13 staging models. At minimum: `id` (or primary identifier), `day`, and `partition_date` for every model. Add `score` or the primary metric column where applicable.

### 12. Update dbt `sources.yml`

Source table definitions stay the same (same table names), but column definitions should reflect the new `raw_data VARIANT` + `partition_date DATE` schema.

### 13. Update tests

**Strategy:** Use a real Snowflake `OURA_TEST` database/schema (per CLAUDE.md: "prefer real code over mocks"). Tests create/drop tiny tables and clean up after themselves.

- Replace `duckdb.connect(":memory:")` fixture with a Snowflake connection fixture:

```python
@pytest.fixture()
def con():
    """Snowflake connection to test schema. Skipped if no credentials."""
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        # ... key-pair auth
        database="OURA_TEST",
        schema="OURA_RAW",
    )
    yield con
    # cleanup: DROP tables created during test
    con.close()
```

- Skip in CI if credentials unavailable:

```python
pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in os.environ,
    reason="Snowflake credentials not available",
)
```

- Rewrite all assertions for Snowflake SQL behavior (uppercase column names, VARIANT types)

**Test matrix:**

| Test                                  | What it validates                    |
| ------------------------------------- | ------------------------------------ |
| `test_creates_table_and_inserts_rows` | Basic INSERT via executemany works   |
| `test_return_value_matches_row_count` | Count matches actual rows            |
| `test_empty_list_returns_zero`        | Empty input creates table, returns 0 |
| `test_same_day_replaces_rows`         | DELETE + INSERT idempotency          |
| `test_different_days_coexist`         | Partition isolation                  |
| `test_rerun_does_not_duplicate`       | Triple-run produces 1 row            |
| `test_adds_partition_date`            | partition_date injected into VARIANT |
| `test_invalid_table_raises`           | VALID_TABLES whitelist works         |
| `test_sql_injection_attempt`          | Malicious table name rejected        |
| `test_check_row_count_*`              | Asset checks work with Snowflake SQL |

**Snowflake setup for tests (one-time):**

```sql
CREATE DATABASE IF NOT EXISTS OURA_TEST;
CREATE SCHEMA IF NOT EXISTS OURA_TEST.OURA_RAW;
GRANT USAGE ON DATABASE OURA_TEST TO ROLE TRANSFORM;
GRANT ALL ON SCHEMA OURA_TEST.OURA_RAW TO ROLE TRANSFORM;
```

### 14. Update `.env.example`

Remove `DUCKDB_PATH`. Add:

```
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=oura_pipeline
SNOWFLAKE_PRIVATE_KEY=<base64-encoded-PEM-private-key>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=OURA
SNOWFLAKE_ROLE=TRANSFORM
```

### 15. Remove `max_concurrent_runs: 1`

This was a DuckDB file-locking workaround. Snowflake handles concurrency natively.

### 16. Snowflake setup (manual / one-time)

```sql
-- Database and schemas
CREATE DATABASE IF NOT EXISTS OURA;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_RAW;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_STAGING;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_MARTS;
CREATE SCHEMA IF NOT EXISTS OURA.CONFIG;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Role and user
CREATE ROLE IF NOT EXISTS TRANSFORM;
CREATE USER IF NOT EXISTS OURA_PIPELINE
    DEFAULT_ROLE = TRANSFORM
    DEFAULT_WAREHOUSE = COMPUTE_WH
    RSA_PUBLIC_KEY = '<paste-public-key>';

-- Grants (least-privilege)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT USAGE ON DATABASE OURA TO ROLE TRANSFORM;

-- oura_raw: create + read + write (for _upsert_day)
GRANT USAGE ON SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT CREATE TABLE ON SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT SELECT, INSERT, DELETE ON FUTURE TABLES IN SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;

-- oura_staging + oura_marts: full access (for dbt)
GRANT ALL ON SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;

-- config: read + write (for OAuth tokens)
GRANT USAGE ON SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT CREATE TABLE ON SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;

-- Assign role to user
GRANT ROLE TRANSFORM TO USER OURA_PIPELINE;

-- Token storage table
CREATE TABLE IF NOT EXISTS OURA.CONFIG.OAUTH_TOKENS (
    token_data VARIANT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Test database (for pytest)
CREATE DATABASE IF NOT EXISTS OURA_TEST;
CREATE SCHEMA IF NOT EXISTS OURA_TEST.OURA_RAW;
GRANT USAGE ON DATABASE OURA_TEST TO ROLE TRANSFORM;
GRANT ALL ON SCHEMA OURA_TEST.OURA_RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA_TEST.OURA_RAW TO ROLE TRANSFORM;
```

### 17. Update project context (`project.md`)

Update `.claude/docs/project.md` to reflect:

- Tech stack: DuckDB → Snowflake, dbt-duckdb → dbt-snowflake
- Data flow diagram: DuckDB references → Snowflake
- Environment variables: new Snowflake vars, remove DUCKDB_PATH
- Architecture patterns: VARIANT raw layer, Snowflake-backed token storage

### 18. Add Snowflake credentials to GitHub Actions

Add these secrets to the GitHub repository (Settings → Secrets → Actions):

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PRIVATE_KEY`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_ROLE`

Required for `dbt parse` during CI manifest generation in the deploy workflow.

---

## Order of Operations

```
Phase 0: Manual setup
  1. Snowflake account + setup SQL (task 16)
  2. Key pair generation
  3. Seed initial OAuth tokens
  4. Add secrets to Dagster Cloud + GitHub Actions (task 18)

Phase 1: Core migration
  5. Dependencies (task 1)
  6. SnowflakeResource (task 2)
  7. Token storage migration (task 3)       ← LAUNCH BLOCKER
  8. _upsert_day() rewrite (tasks 4-5)
  9. Wire up definitions + assets + checks (tasks 6-8)

Phase 2: dbt migration
  10. dbt profile + project config (task 9)
  11. dbt staging models → VARIANT syntax (task 10)
  12. dbt not_null tests (task 11)
  13. dbt sources.yml (task 12)

Phase 3: Tests + cleanup
  14. Rewrite tests for Snowflake (task 13)
  15. Update .env.example (task 14)
  16. Remove max_concurrent_runs (task 15)
  17. Update project.md (task 17)

Phase 4: Verification (post-deploy)
  18. Materialize 1 partition of 1 asset manually
  19. Check Snowflake raw table
  20. Run dbt manually (dbt run + dbt test)
  21. Check staging + mart tables
  22. Backfill all partitions
```

## Rollback Plan

If the migration breaks post-deploy:

1. `git revert <merge-commit>` on `main`
2. Push → auto-deploys the DuckDB version
3. Snowflake data persists independently (no cleanup needed)
4. Local `dagster dev` works immediately with the DuckDB file

**Rollback time: ~5 minutes.** Reversibility: 3/5 (easy to roll back code, but data is now in Snowflake).

## Risks

- **Snowflake costs:** XSMALL warehouse with daily runs of ~13 small queries is negligible (~$0.01/day)
- **VARIANT query performance:** At this data volume (hundreds of rows), zero concern
- **Key-pair auth setup:** One-time complexity, but more secure than password
- **Silent NULLs from VARIANT:** Mitigated by dbt `not_null` tests (task 11)
- **CI dbt parse:** May require live Snowflake connection — mitigated by adding creds to GitHub Actions secrets (task 18)

## Future Work

- **dlt adoption:** Replace custom `_upsert_day()` with dlt for schema evolution, retries, and normalized loading. Track as separate plan in `docs/plans/dlt_adoption.md` after migration is stable.
- **Dependency audit:** After migration, grep for `polars` and `pyarrow` imports. Remove from `pyproject.toml` if unused.
