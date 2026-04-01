# Code Review Remediation Plan

**Created:** 2026-03-31
**Source:** Combined DAA code reviews (dbt, Python source, tests/CI)
**Reports:** `docs/code_reviews/2026-03-31_*.md`

---

## Work Streams

Four independent streams, safe for parallel subagent execution (no file overlap).

---

### Stream 1: CI/CD Pipeline Fixes

**Files:** `.github/workflows/branch_deployments.yml`, `.github/workflows/deploy.yml`

| # | Task | Severity | Details |
|---|------|----------|---------|
| 1.1 | Add pytest step to both CI workflows | ERROR | Tests exist but never run in CI. Add a `test` job that runs `uv run pytest tests/ -x --ignore=tests/test_snowflake_resource.py --ignore=tests/test_upsert_day.py --ignore=tests/test_asset_checks.py` before the deploy jobs. The test job should gate deployment. |
| 1.2 | Fix `DAGSTER_CLOUD_URL` to use `https://` | ERROR | Both workflows use `http://` which transmits the API token in cleartext. Change to `https://`. |
| 1.3 | Fix `deploy.yml` checkout ref | ERROR | `github.head_ref` is empty on push events. Remove the `ref` field or use `${{ github.sha }}`. |
| 1.4 | Add `permissions` block to `deploy.yml` | WARNING | Match the explicit permissions from `branch_deployments.yml`. |
| 1.5 | Pin dbt versions in CI workflows | WARNING | `pip install dbt-core dbt-snowflake` installs unpinned versions. Pin to match project dependencies. |
| 1.6 | Remove duplicate branch trigger in `deploy.yml` | INFO | Triggers on both `main` and `master`. Remove whichever is not used. |

---

### Stream 2: dbt Model Quality

**Files:** `dbt_oura/models/staging/schema.yml`, `dbt_oura/models/sources.yml`, `dbt_oura/models/marts/fact_daily_wellness.sql`, `dbt_oura/models/marts/fact_sleep_detail.sql`, new `dbt_oura/models/marts/schema.yml`, staging `.sql` files

| # | Task | Severity | Details |
|---|------|----------|---------|
| 2.1 | Add `unique` tests on all PK/grain columns | ERROR | No `unique` tests exist anywhere. Add `unique` alongside `not_null` on every `id` column and `day` column in `staging/schema.yml`. |
| 2.2 | Create `marts/schema.yml` with tests and descriptions | ERROR | Neither mart model has any YAML. Create schema file with `not_null`, `unique` tests and model/column descriptions for `fact_sleep_detail` and `fact_daily_wellness`. |
| 2.3 | Refactor `fact_daily_wellness.sql` to use a date spine | ERROR | Full outer join chain can produce duplicate days. Replace with date spine CTE + left joins. |
| 2.4 | Add source freshness to `sources.yml` | WARNING | No `loaded_at_field` or `freshness` blocks. Add `partition_date` as loaded_at with warn/error thresholds. |
| 2.5 | Cast timestamp columns to proper types in staging models | WARNING | `stg_sleep_periods` (bedtime_start/end), `stg_workouts` (start/end_datetime), `stg_sessions` (start/end_datetime), `stg_heartrate` (ts), `stg_sleep` (timestamp) store timestamps as `varchar`. Cast to `timestamp_ntz` or `timestamp_tz`. |
| 2.6 | Add deduplication logic to staging models | WARNING | Add `qualify row_number() over (partition by id order by partition_date desc) = 1` to models with `id` columns, and equivalent for `day`-keyed models. |
| 2.7 | Add `accepted_values` tests on enum columns | WARNING | `resilience_level`, `sleep_type`, `intensity`, `session_type`, `mood` lack accepted_values tests. |
| 2.8 | Add model and column descriptions to all YAML | WARNING | Zero `description` fields across `sources.yml` and `staging/schema.yml`. Add descriptions to all models and key columns. |
| 2.9 | Rename abbreviated CTE aliases | INFO | `fact_sleep_detail.sql` uses `sp`/`st`, `fact_daily_wellness.sql` uses `act`/`ready`/`res`. Rename to descriptive names per project standards. |

---

### Stream 3: Python Source Fixes

**Files:** `src/dagster_project/defs/assets.py`, `src/dagster_project/defs/checks.py`, `src/dagster_project/defs/resources.py`, `src/dagster_project/defs/dbt_assets.py`, `src/dagster_project/defs/dbt_translator.py`, `src/dagster_project/defs/report_assets.py`, `src/dagster_project/reports/report_analysis.py`, `src/dagster_project/reports/report_charts.py`, `src/dagster_project/reports/report_delivery.py`, `src/dagster_project/definitions.py`, `src/oura_oauth_cli.py`

| # | Task | Severity | Details |
|---|------|----------|---------|
| 3.1 | Add `SnowflakeResource.connection()` context manager and fix connection leaks | ERROR | `assets.py` (both factory functions) and `checks.py` open connections without `try/finally`. Add a `@contextmanager` `connection()` method to `SnowflakeResource` in `resources.py`, then use `with` blocks in `assets.py` and `checks.py`. `report_assets.py` already uses `try/finally` — update it to use the new context manager too. |
| 3.2 | Fix OAuth token file permissions | ERROR | `oura_oauth_cli.py:80-84` writes tokens with default 0644. Use `os.open(path, os.O_WRONLY \| os.O_CREAT \| os.O_TRUNC, 0o600)` or `Path.chmod(0o600)` after write. |
| 3.3 | Deduplicate SnowflakeResource in `definitions.py` | WARNING | SnowflakeResource is instantiated twice with identical EnvVar params. Instantiate once and pass to both `"snowflake"` resource and `OuraAPI`. |
| 3.4 | Remove unused imports (ruff F401) | WARNING | `report_charts.py:10` — unused `matplotlib.dates`. `report_delivery.py:8` — unused `typing.Any`. Run `uv run ruff check src/ --fix`. |
| 3.5 | Rename ambiguous variable `l` in `report_charts.py` | WARNING | Lines 247, 250 use `l` which is ambiguous (ruff E741). Rename to `light_val`. |
| 3.6 | Add missing type annotations | WARNING | `dbt_translator.py` method params, `report_charts.py:31` param `d`, `oura_oauth_cli.py:20` return type (`str \| None`), `assets.py` factory return types, `dbt_assets.py:42` return type. |
| 3.7 | Add missing docstrings | WARNING | `resources.py:_get_access_token`, `oura_oauth_cli.py:exchange_code_for_tokens` and `refresh_with_refresh_token`, `dbt_translator.py` overridden methods. Numpy-style per project standards. |
| 3.8 | Extract shared trend computation in `report_analysis.py` | WARNING | Midpoint-based trend logic duplicated between `compute_metric_summaries` (lines 180-224) and `identify_areas_to_improve` (lines 360-404). Extract a shared `_compute_trend()` helper. |
| 3.9 | Add `atexit` cleanup for temp key file in `dbt_assets.py` | INFO | `_ensure_key_file` writes a PEM tempfile but never cleans up. Add `atexit.register(os.unlink, path)`. |
| 3.10 | Modernize typing imports | INFO | `assets.py` and `resources.py` use `Dict` from `typing`. Replace with builtin `dict` for Python 3.10+ consistency. |

---

### Stream 4: Test Quality Fixes

**Files:** `tests/conftest.py`, `tests/test_report_analysis.py`, `tests/test_oura_api.py`, `tests/test_snowflake_resource.py`, `tests/test_report_assets.py`, `tests/test_schedule.py`

| # | Task | Severity | Details |
|---|------|----------|---------|
| 4.1 | Remove unused imports in `test_report_analysis.py` | WARNING | 6 unused imports (F401): `MetricSummary`, `PersonalBest`, `AreaToImprove`, `SleepSummary`, `WorkoutSummary`, `ReportData`. Run `uv run ruff check tests/ --fix`. |
| 4.2 | Fix SQL injection pattern in `conftest.py` cleanup | WARNING | Line 52 uses f-string `DROP TABLE`. Use quoted identifiers: `f'DROP TABLE IF EXISTS oura_raw."{table_name}"'`. |
| 4.3 | Extract duplicated setup in `TestExclusiveEndDateAdjustment` | WARNING | 4 test methods in `test_oura_api.py` repeat identical token/mock setup. Extract into a fixture or setUp method. |
| 4.4 | Narrow exception type in `test_snowflake_resource.py:55` | WARNING | `pytest.raises(Exception)` is too broad. Use the specific expected exception type. |
| 4.5 | Fix conditional assertion in `test_report_analysis.py:97-98` | INFO | `if result:` silently passes when list is empty. Assert non-empty or assert empty explicitly. |
| 4.6 | Consolidate duplicate schedule assertions | INFO | `test_schedule.py` and `test_report_assets.py` both verify the same cron schedules. Remove duplicates from one file. |
| 4.7 | Standardize `-> None` return type hints on test functions | INFO | Most test functions lack return type annotations. Add `-> None` consistently. |
| 4.8 | Move `from datetime import date` to top-level in `test_oura_api.py` | INFO | Currently imported inside individual test methods. |

---

## Execution Order

Streams 1-4 are independent and can run in parallel. Within each stream, tasks are ordered by priority (ERROR first, then WARNING, then INFO). Each subagent should work top-to-bottom within its stream.

## Verification

After all streams complete:
1. `uv run ruff check src/ tests/` — should be clean
2. `uv run pytest tests/ -x --ignore=tests/test_snowflake_resource.py --ignore=tests/test_upsert_day.py --ignore=tests/test_asset_checks.py` — all tests pass
3. No uncommitted changes remain
