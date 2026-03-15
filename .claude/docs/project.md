# Oura Pipeline — Project Context

Dagster-orchestrated ELT pipeline that pulls personal health data from the Oura Ring API (v2), lands it in Snowflake as raw VARIANT JSON, and transforms it with dbt.

## Tech Stack

- **Python >=3.10** — core language
- **Dagster** — asset-based orchestration with daily partitions
- **dagster-dbt** — dbt integration (runs dbt models as Dagster assets)
- **dbt-core + dbt-snowflake** — SQL transformations (staging → marts)
- **Snowflake** — cloud data warehouse (OURA database)
- **snowflake-connector-python[pandas]** — database connectivity
- **cryptography** — key-pair authentication (base64 PEM → DER)
- **Polars** — DataFrame operations in report modules
- **requests** — HTTP client for Oura API
- **python-dotenv** — env loading
- **uv** — package manager
- **Hatchling** — build backend

## Project Layout

```text
src/
  dagster_project/
    definitions.py          # Dagster entry point — wires assets, resources, dbt
    defs/
      assets.py             # 13 partitioned raw assets (one per Oura endpoint)
      resources.py          # OuraAPI (OAuth2 + token refresh), SnowflakeResource
      checks.py             # Asset checks (row count validation)
      report_assets.py      # Health report generation assets
      dbt_translator.py     # Maps dbt sources/models to Dagster asset keys + groups
    reports/
      report_data.py        # Snowflake queries for report generation
  oura_oauth_cli.py         # Standalone OAuth2 CLI for initial token acquisition
dbt_oura/
  dbt_project.yml           # dbt project config (profile: oura_snowflake)
  profiles.yml              # Snowflake connection profile (key-pair auth)
  models/
    sources.yml             # dbt sources (oura_raw schema, VARIANT columns)
    staging/                # 13 stg_* models (VARIANT → typed columns)
      schema.yml            # not_null tests for all staging models
    marts/                  # fact_daily_wellness, fact_sleep_detail
```

## Data Flow

```text
Oura Ring API (v2)  ──→  OuraAPI resource  ──→  JSON responses
                                ↓
                    json.dumps()  ──→  _upsert_day()
                                ↓
                    Snowflake OURA.OURA_RAW.*  (13 tables, raw_data VARIANT + partition_date DATE)
                                ↓
                    dbt staging  (13 stg_* models, VARIANT → typed columns)
                                ↓
                    dbt marts  (fact_daily_wellness, fact_sleep_detail)
```

## Data Sources

- **Oura API v2** — OAuth2 authenticated; endpoints: daily_sleep, daily_activity, daily_readiness, daily_spo2, daily_stress, daily_resilience, heartrate, sleep (periods), sleep_time, workout, session, tag, rest_mode_period
- **Snowflake** (`OURA` database) — schemas: `OURA_RAW` (raw VARIANT landing), `CONFIG` (OAuth tokens), `OURA_STAGING`, `OURA_MARTS`

## Environment Variables

- `OURA_CLIENT_ID` / `OURA_CLIENT_SECRET` — OAuth2 credentials
- `SNOWFLAKE_ACCOUNT` — Snowflake account identifier
- `SNOWFLAKE_USER` — Snowflake service user
- `SNOWFLAKE_PRIVATE_KEY` — base64-encoded PEM private key
- `SNOWFLAKE_WAREHOUSE` — compute warehouse (default: `COMPUTE_WH`)
- `SNOWFLAKE_DATABASE` — target database (default: `OURA`)
- `SNOWFLAKE_ROLE` — Snowflake role (default: `TRANSFORM`)

## Test Markers

- `uv run pytest` — run all tests
- `uv run pytest --cov=src --cov-report=term-missing` — with coverage
- `uv run pytest -m snowflake` — Snowflake integration tests only
- `uv run pytest -m "not snowflake"` — skip Snowflake tests (no credentials needed)

## Key Architecture Patterns

- **Snowflake key-pair authentication** (`resources.py`): Base64 PEM → DER bytes via `cryptography` library.
- **VARIANT raw layer** (`assets.py`): All raw data stored as `(raw_data VARIANT, partition_date DATE)` — schema-on-read.
- **Snowflake-backed OAuth token storage** (`resources.py`): Tokens stored in `CONFIG.OAUTH_TOKENS` table, not filesystem.
- **Idempotent daily upsert** (`assets.py:_upsert_day`): DELETE + INSERT by `partition_date` ensures re-runs are safe.
- **Daily partitions** (`assets.py`): All 13 raw assets share a `DailyPartitionsDefinition(start_date="2024-01-01")`.
- **Custom dbt translator** (`dbt_translator.py`): Maps dbt sources to `["oura_raw", <table>]` asset keys; groups models by name prefix (stg*/fact*/dim\_).
- **OAuth2 with auto-refresh** (`resources.py`): Token refresh happens transparently inside `_get_access_token()`.
- **not_null dbt tests** (`schema.yml`): Safety net for VARIANT path correctness — wrong paths return NULL and fail tests.
