# Oura Pipeline — Project Context

Dagster-orchestrated ELT pipeline that pulls personal health data from the Oura Ring API (v2), lands it in DuckDB, and transforms it with dbt.

## Tech Stack

- **Python >=3.10** — core language
- **Dagster** — asset-based orchestration with daily partitions
- **dagster-dbt** — dbt integration (runs dbt models as Dagster assets)
- **dbt-core + dbt-duckdb** — SQL transformations (staging → marts)
- **DuckDB** — local analytical database (`data/oura.duckdb`)
- **Polars** — DataFrame construction for raw API responses
- **PyArrow** — Arrow interchange between Polars and DuckDB
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
      resources.py          # OuraAPI (OAuth2 + token refresh), DuckDBResource
      dbt_translator.py     # Maps dbt sources/models to Dagster asset keys + groups
  oura_oauth_cli.py         # Standalone OAuth2 CLI for initial token acquisition
dbt_oura/
  dbt_project.yml           # dbt project config (profile: oura_duckdb)
  profiles.yml              # DuckDB connection profile
  models/
    sources.yml             # dbt sources (oura_raw schema)
    staging/                # stg_sleep, stg_activity, stg_readiness, stg_heartrate
    marts/                  # fact_daily_wellness
data/
  oura.duckdb               # DuckDB database (gitignored)
  tokens/                   # OAuth tokens (gitignored)
```

## Data Flow

```text
Oura Ring API (v2)  ──→  OuraAPI resource  ──→  JSON responses
                                ↓
                    Polars DataFrame  ──→  _upsert_day()
                                ↓
                    DuckDB oura_raw.*  (13 tables, daily partitioned)
                                ↓
                    dbt staging  (stg_sleep, stg_activity, stg_readiness, stg_heartrate)
                                ↓
                    dbt marts  (fact_daily_wellness)
```

## Data Sources

- **Oura API v2** — OAuth2 authenticated; endpoints: daily_sleep, daily_activity, daily_readiness, daily_spo2, daily_stress, daily_resilience, heartrate, sleep (periods), sleep_time, workout, session, tag, rest_mode_period
- **DuckDB** (`data/oura.duckdb`) — local warehouse; schemas: `oura_raw` (raw landing), `oura_staging`, `oura_marts`

## Environment Variables

- `OURA_CLIENT_ID` / `OURA_CLIENT_SECRET` — OAuth2 credentials
- `OURA_TOKEN_PATH` — path to token JSON (default: `data/tokens/oura_tokens.json`)
- `DUCKDB_PATH` — path to DuckDB file (default: `data/oura.duckdb`)

## Test Markers

- `uv run pytest` — run all tests
- `uv run pytest --cov=src --cov-report=term-missing` — with coverage

## Key Architecture Patterns

- **Dagster resource pattern** (`resources.py`): `OuraAPI` and `DuckDBResource` are `ConfigurableResource` classes injected via `required_resource_keys`.
- **Idempotent daily upsert** (`assets.py:_upsert_day`): Delete-then-insert by `partition_date` ensures re-runs are safe.
- **Daily partitions** (`assets.py`): All 13 raw assets share a `DailyPartitionsDefinition(start_date="2024-01-01")`.
- **Custom dbt translator** (`dbt_translator.py`): Maps dbt sources to `["oura_raw", <table>]` asset keys; groups models by name prefix (stg*/fact*/dim\_).
- **OAuth2 with auto-refresh** (`resources.py`): Token refresh happens transparently inside `_get_access_token()`.
