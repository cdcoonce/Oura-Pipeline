# Python Source Code Review -- 2026-03-31

**Reviewer:** Claude (automated DAA review)
**Scope:** All Python files under `src/`
**Files reviewed:** 17

---

## 1. Summary

Overall, the codebase is well-structured with clean separation of concerns across the Dagster definitions, reports module, and OAuth CLI. The asset factory pattern in `assets.py` is effective and DRY. Docstrings are thorough and use numpy-style consistently. SQL injection is mitigated via a `VALID_TABLES` whitelist and parameterized queries.

The main issues are resource leaks (Snowflake connections not closed on exceptions), duplicated SnowflakeResource configuration, and a handful of missing type annotations. There are no security vulnerabilities, but several patterns could lead to silent failures in production.

| Severity | Count |
|----------|-------|
| Blocking | 3 |
| Warning  | 12 |
| Suggestion | 8 |

---

## 2. Ruff Findings

```
$ uv run ruff check src/

F401  src/dagster_project/reports/report_charts.py:10   -- `matplotlib.dates` imported but unused
E741  src/dagster_project/reports/report_charts.py:247  -- Ambiguous variable name: `l`
E741  src/dagster_project/reports/report_charts.py:250  -- Ambiguous variable name: `l`
F401  src/dagster_project/reports/report_delivery.py:8  -- `typing.Any` imported but unused

Found 4 errors (2 fixable with --fix).
```

Additionally, `ruff check --select I001` flags unsorted import blocks in `report_analysis.py` and `report_charts.py`.

---

## 3. Findings by File

### `src/dagster_project/definitions.py`

**[warning]** `definitions.py:14-31` -- Duplicated SnowflakeResource configuration.

The `SnowflakeResource` is instantiated twice with identical `EnvVar` parameters: once as the `"snowflake"` resource and again nested inside `OuraAPI`. If any env var name changes, both must be updated in lockstep. Consider instantiating once and passing the same instance to both:

```python
sf = SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    ...
)
# then:
"snowflake": sf,
"oura_api": OuraAPI(client_id=..., client_secret=..., snowflake=sf),
```

---

### `src/dagster_project/defs/assets.py`

**[blocking]** `assets.py:118-122,149-153` -- Snowflake connection not closed if `_upsert_day` or `fetch_*` raises.

In both `_make_daily_asset` and `_make_granular_asset`, the connection is opened on line 118/149 and closed on line 122/153, but there is no `try/finally` block. If `oura_api.fetch_daily()`, `_upsert_day()`, or `getattr(oura_api, fetch_method)()` raises an exception, `con.close()` is never called. This will leak Snowflake connections over time, potentially exhausting the connection pool.

Fix: wrap the body in `try/finally` (as is correctly done in `report_assets.py:104-182`).

**[warning]** `assets.py:86` -- Temporary table name collision risk.

`_upsert_day` creates a temporary table named `_tmp_load`. If two partitions were materialized concurrently on the same connection (unlikely with `in_process_executor`, but possible if the executor changes), the temp table would collide. Consider using a unique name (e.g., `_tmp_load_{table}_{day}`).

**[suggestion]** `assets.py:4` -- `typing.Dict` is imported but `dict` (lowercase) is sufficient on Python 3.10+.

The project uses `from __future__ import annotations` in other files. For consistency, prefer `dict[str, Any]` over `Dict[str, Any]` in function signatures.

**[suggestion]** `assets.py:104,135` -- Factory functions lack return type annotations.

`_make_daily_asset` and `_make_granular_asset` return `Callable` but the actual return type is a Dagster asset function. The type hint `Callable` is overly broad -- consider `dg.AssetsDefinition` or just documenting the return more precisely.

---

### `src/dagster_project/defs/checks.py`

**[blocking]** `checks.py:57-60` -- Snowflake connection not closed on exception.

Same pattern as `assets.py`. If `_check_row_count` raises (e.g., Snowflake query timeout), `con.close()` is never called.

**[warning]** `checks.py:33` -- Unvalidated table name in f-string SQL.

Line 33 uses `f"SELECT COUNT(*) FROM oura_raw.{table}"`. Although `make_row_count_check` is only called with values from `VALID_TABLES` (line 68), `_check_row_count` itself does not validate `table`. If called directly in a test or future code path, this would be a SQL injection vector. Consider adding the same `if table not in VALID_TABLES` guard that `_upsert_day` uses.

---

### `src/dagster_project/defs/dbt_assets.py`

**[warning]** `dbt_assets.py:34-38` -- Temp key file is never cleaned up.

`_ensure_key_file` writes a PEM file to a tempfile but never deletes it. Over many process restarts, temp files accumulate on disk. Consider using `atexit.register(os.unlink, path)` or a context manager.

**[warning]** `dbt_assets.py:38` -- Setting `os.environ` is process-global side effect.

Mutating `os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]` at module scope (via the `@dbt_assets` decorator calling `_ensure_key_file`) is a global side effect. This is likely fine for a single-process Dagster deployment but could cause issues in test environments or multi-threaded scenarios.

**[suggestion]** `dbt_assets.py:42` -- `dbt_model_assets` is missing a return type annotation.

---

### `src/dagster_project/defs/dbt_translator.py`

**[warning]** `dbt_translator.py:15` -- `OuraTranslator` class has no docstring on overridden methods.

The class docstring is good, but `get_asset_key_for_source` and `get_group_name` lack individual docstrings. Project standards require numpy-style docstrings on public methods.

**[warning]** `dbt_translator.py:22-27,30-37` -- Missing type annotations on method parameters.

Both `get_asset_key_for_source(self, dbt_source_props)` and `get_group_name(self, dbt_resource_props)` lack type hints for their parameters. The Dagster base class types these as `Mapping[str, Any]`.

---

### `src/dagster_project/defs/report_assets.py`

**[suggestion]** `report_assets.py:27-44,47-65` -- `_previous_week_range` and `_previous_month_range` use `date.today()` directly.

This makes unit testing difficult since the output depends on the current date. Consider accepting an optional `today` parameter defaulting to `date.today()` for testability.

Good: `_generate_and_send_report` correctly uses `try/finally` for connection cleanup (line 104/182).

---

### `src/dagster_project/defs/resources.py`

**[warning]** `resources.py:99,121` -- `Dict` from `typing` used instead of builtin `dict`.

Lines 99 and 121 use `Dict[str, Any]` -- prefer `dict[str, Any]` for Python 3.10+ consistency with the rest of the codebase.

**[warning]** `resources.py:136-165` -- `_get_access_token` has no docstring.

This is a non-trivial method that handles token expiration logic and refresh flow. It deserves a numpy-style docstring explaining the refresh strategy.

**[suggestion]** `resources.py:140` -- Token expiration check is fragile.

The expression `obtained_at + expires_in - 60 > now` will consider the token valid if `obtained_at` is 0 and `expires_in` is 0 (both defaults), since `0 + 0 - 60 = -60 > now` is false. This is correct by accident but confusing. Consider explicit early return: `if not tokens.get("access_token"): <refresh>`.

**[suggestion]** `resources.py:226-279` -- Repetitive fetch methods.

The eight `fetch_*` methods follow an identical pattern differing only in the URL path and endpoint name. This is a good candidate for a generic `_fetch_collection` helper to reduce repetition. However, the current approach is readable and explicit, so this is low priority.

---

### `src/dagster_project/defs/schedules.py`

**[suggestion]** `schedules.py` -- No issues. Clean and well-structured.

---

### `src/dagster_project/reports/__init__.py`

Empty file. No issues.

---

### `src/dagster_project/reports/report_analysis.py`

**[warning]** `report_analysis.py:359-404` -- Duplicated trend computation logic.

The midpoint-based trend calculation in `identify_areas_to_improve` (lines 360-404) duplicates the same logic from `compute_metric_summaries` (lines 180-224). Extract a shared `_compute_trend(first_half, second_half, metric)` helper to eliminate this duplication.

**[suggestion]** `report_analysis.py:198-199` -- `col.mean()` includes null values in Polars.

`wellness_df[metric].mean()` in Polars skips nulls by default, so this is correct. However, `min()` and `max()` also skip nulls, meaning the `days_with_data` count (line 193-194) may differ from the actual values used in mean/min/max. This is a minor semantic inconsistency, not a bug.

Good: Frozen dataclasses, clear separation of analysis from I/O, and comprehensive docstrings.

---

### `src/dagster_project/reports/report_charts.py`

**[warning]** `report_charts.py:10` -- Unused import `matplotlib.dates as mdates` (ruff F401).

**[warning]** `report_charts.py:247,250` -- Ambiguous variable name `l` (ruff E741).

Rename to `light_val` or `lv` to avoid confusion with `1` (one):
```python
bottom_rem = [d + lv for d, lv in zip(deep, light)]
```

**[warning]** `report_charts.py:31` -- `_format_day_label` lacks type annotation for parameter `d`.

Should be `def _format_day_label(d: date) -> str:`.

**[suggestion]** `report_charts.py:33` -- `%-m/%-d` format codes are platform-dependent.

The `%-m` and `%-d` format codes (no zero-padding) work on Linux/macOS but fail on Windows. If this will only ever run on Linux (likely for a data pipeline), this is fine. Otherwise, consider using `d.strftime("%a") + f" {d.month}/{d.day}"`.

---

### `src/dagster_project/reports/report_data.py`

No issues. Clean parameterized queries, proper use of `fetch_pandas_all()` with column lowercasing, and thorough docstrings.

---

### `src/dagster_project/reports/report_delivery.py`

**[warning]** `report_delivery.py:8` -- Unused import `typing.Any` (ruff F401).

Good: Proper error wrapping with `from e`, actionable error messages, and clean Dagster resource pattern.

---

### `src/dagster_project/reports/report_renderer.py`

No issues. Clean, simple, and well-documented.

---

### `src/oura_oauth_cli.py`

**[blocking]** `oura_oauth_cli.py:80-84` -- Token file written without restricted permissions.

`save_tokens` writes OAuth tokens (including refresh tokens) to disk with default file permissions (typically 0644). Anyone with read access to the filesystem can read these tokens. Use `os.open` with `0o600` (as done in `dbt_assets.py:37`) or set permissions after write.

**[warning]** `oura_oauth_cli.py:20` -- `env` function return type is `str` but can return `None`.

When `required=False` and `default=None`, the function returns `None`, but the return type annotation says `str`. The return type should be `str | None`, or the function should handle the `None` case differently.

**[warning]** `oura_oauth_cli.py:41-58,61-77` -- `exchange_code_for_tokens` and `refresh_with_refresh_token` lack docstrings.

These are public functions per the module's API surface.

**[suggestion]** `oura_oauth_cli.py:146-158` -- Broad `except Exception` when reading existing token file.

Line 157 catches all exceptions when attempting to read and refresh tokens. This could mask real errors (e.g., network issues during refresh, invalid JSON). Consider catching `(json.JSONDecodeError, KeyError, OSError)` specifically and letting `requests.HTTPError` propagate.

---

## 4. Cross-Cutting Concerns

### 4.1 Connection Leak Pattern (blocking)

Three locations open Snowflake connections without `try/finally` protection:
- `src/dagster_project/defs/assets.py` (both factory functions)
- `src/dagster_project/defs/checks.py` (`make_row_count_check`)

`report_assets.py` correctly uses `try/finally`. Apply the same pattern everywhere. Consider a context manager on `SnowflakeResource`:

```python
from contextlib import contextmanager

@contextmanager
def connection(self):
    con = self.get_connection()
    try:
        yield con
    finally:
        con.close()
```

### 4.2 Legacy `typing` Imports

`assets.py` and `resources.py` use `Dict` and `Iterable` from `typing`, while `report_analysis.py` and `report_renderer.py` use modern `dict[str, ...]` syntax. Standardize on the modern form (`from collections.abc import Iterable`, `dict` instead of `Dict`). The project targets Python 3.10+.

### 4.3 Testability of Date-Dependent Functions

`_previous_week_range()` and `_previous_month_range()` in `report_assets.py` call `date.today()` directly, making them hard to test without mocking. Injecting `today` as a parameter (with a default) would improve testability.

### 4.4 Missing Test Coverage

This review did not audit test files, but based on the source code, the following areas would benefit from dedicated tests:
- `_upsert_day` with empty input, None input, and whitelist violation
- `_get_access_token` refresh flow (expired token, failed refresh)
- `_ensure_key_file` idempotency
- `_previous_week_range` / `_previous_month_range` boundary conditions
- `save_tokens` file permission check

---

## 5. Recommendations (Prioritized)

1. **[blocking] Fix connection leaks** -- Add `try/finally` to all Snowflake connection usage in `assets.py` and `checks.py`. Better yet, add a `SnowflakeResource.connection()` context manager and use `with` blocks project-wide.

2. **[blocking] Restrict token file permissions** in `oura_oauth_cli.py` -- Use `os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)` to prevent other users from reading OAuth tokens.

3. **[warning] Deduplicate SnowflakeResource** in `definitions.py` -- Instantiate once and reuse.

4. **[warning] Remove unused imports** -- Fix the 2 ruff F401 findings (`mdates`, `Any`).

5. **[warning] Rename ambiguous variable `l`** in `report_charts.py` to `light_val`.

6. **[warning] Add missing type annotations** -- `dbt_translator.py` method parameters, `report_charts.py:31`, `oura_oauth_cli.py:20` return type.

7. **[warning] Add missing docstrings** -- `_get_access_token`, `exchange_code_for_tokens`, `refresh_with_refresh_token`, translator methods.

8. **[warning] Extract shared trend computation** from `report_analysis.py` to eliminate duplication between `compute_metric_summaries` and `identify_areas_to_improve`.

9. **[suggestion] Add `atexit` cleanup** for the temp key file in `dbt_assets.py`.

10. **[suggestion] Make date helpers testable** by accepting an optional `today` parameter.
