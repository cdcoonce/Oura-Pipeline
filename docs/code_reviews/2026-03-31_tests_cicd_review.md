# Code Review: Tests & CI/CD Configuration

**Date:** 2026-03-31  
**Reviewer:** Claude (DAA Code Review)  
**Scope:** All files under `tests/` and `.github/workflows/`

---

## 1. Summary

The test suite is in **good overall health**. Tests are well-organized into classes by behavior, use descriptive names, cover edge cases (empty inputs, null data, boundary conditions), and test security-critical paths like SQL injection prevention. Fixture data is realistic and stored in CSV files rather than hardcoded inline.

**Key strengths:**
- Strong test isolation for pure functions (report_analysis, report_charts, report_renderer, dbt_translator)
- Good use of parametrized tests where applicable
- SQL injection guard tests on `_upsert_day`
- Frozen dataclass immutability is verified
- Chart tests validate PNG magic bytes rather than just checking for non-None

**Key concerns:**
- 6 unused imports flagged by ruff in `test_report_analysis.py`
- Module-level import ordering violation in `conftest.py`
- SQL injection vulnerability in the `conftest.py` cleanup logic
- No test workflow in CI -- tests never run in the pipeline
- Several source modules have no test coverage at all
- Heavy mocking in `test_oura_api.py` and `test_report_delivery.py` contradicts the project's "prefer real code over mocks" guideline (though justified for external services)

---

## 2. Ruff Findings

```
tests/conftest.py:11:1       E402  Module level import not at top of file
tests/test_report_analysis.py:8:5   F401  MetricSummary imported but unused
tests/test_report_analysis.py:9:5   F401  PersonalBest imported but unused
tests/test_report_analysis.py:10:5  F401  AreaToImprove imported but unused
tests/test_report_analysis.py:11:5  F401  SleepSummary imported but unused
tests/test_report_analysis.py:12:5  F401  WorkoutSummary imported but unused
tests/test_report_analysis.py:13:5  F401  ReportData imported but unused
```

**7 total errors, 6 auto-fixable.** The E402 in `conftest.py` is caused by calling `load_dotenv()` before subsequent imports, which is an intentional pattern to load environment variables before checking them. The F401 errors in `test_report_analysis.py` are straightforward unused imports that should be removed.

---

## 3. Test Coverage Analysis

### Source modules WITH test coverage

| Source Module | Test File | Coverage Quality |
|---|---|---|
| `defs/dbt_translator.py` | `test_dbt_translator.py` | Good -- all public methods, edge cases |
| `defs/assets.py` (`_upsert_day`) | `test_upsert_day.py` | Excellent -- insert, idempotency, SQL injection |
| `defs/checks.py` | `test_asset_checks.py` | Good -- requires Snowflake |
| `defs/resources.py` (OuraAPI) | `test_oura_api.py` | Good -- auth, refresh, HTTP errors, date handling |
| `defs/resources.py` (SnowflakeResource) | `test_snowflake_resource.py` | Adequate -- requires Snowflake |
| `defs/schedules.py` | `test_schedule.py` | Adequate -- cron values and job selection |
| `defs/report_assets.py` | `test_report_assets.py` | Good -- date ranges, schedule config, asset groups |
| `reports/report_analysis.py` | `test_report_analysis.py` | Excellent -- thorough edge cases |
| `reports/report_charts.py` | `test_report_charts.py` | Good -- PNG validation, null handling |
| `reports/report_data.py` | `test_report_data.py` | Adequate -- basic happy path and column casing |
| `reports/report_delivery.py` | `test_report_delivery.py` | Good -- SES success/failure paths |
| `reports/report_renderer.py` | `test_report_renderer.py` | Good -- comprehensive HTML content checks |

### Source modules WITHOUT test coverage

| Source Module | Risk Level | Notes |
|---|---|---|
| `definitions.py` | Low | Dagster wiring; integration tested implicitly |
| `defs/__init__.py` | Low | Likely empty or re-exports |
| `defs/dbt_assets.py` | Medium | Contains `_ensure_key_file()` and dbt asset definitions |
| `reports/__init__.py` | Low | Likely empty |
| `oura_oauth_cli.py` | Medium | CLI tool with HTTP server; harder to unit test |

---

## 4. Findings by File

### `tests/conftest.py`

**[blocking]** `conftest.py:52` -- SQL injection vulnerability in test cleanup.

The cleanup logic uses an f-string to construct a DROP TABLE statement from a value read from `information_schema.tables`. While this value comes from Snowflake metadata rather than user input, it is a dangerous pattern that bypasses parameterized queries. If any table name contained special characters, this would break or execute unintended SQL.

```python
# Current (vulnerable pattern):
cursor.execute(f"DROP TABLE IF EXISTS oura_raw.{table_name}")

# Recommended:
cursor.execute(f'DROP TABLE IF EXISTS oura_raw."{table_name}"')
```

Using quoted identifiers protects against names with special characters. Since this is test cleanup code operating on a test database, the real-world risk is low, but it sets a bad example given that the production code in `_upsert_day` carefully validates table names.

**[warning]** `conftest.py:10-16` -- Import ordering (ruff E402).

`load_dotenv()` is called at module level before subsequent imports. This is intentional (to populate `os.environ` before the `SNOWFLAKE_AVAILABLE` check), but could be restructured by moving the check into the fixture itself rather than at module scope.

**[suggestion]** `conftest.py:1` -- Docstring says "Shared test fixtures for Snowflake integration tests" but this conftest also loads `.env` for all tests. Consider updating the docstring to reflect its broader role.

**[suggestion]** `conftest.py:28` -- The `snowflake_con` fixture yields a connection but the cleanup drops ALL tables in the `OURA_RAW` schema, not just tables created during the test. If the schema had pre-existing tables, they would be dropped. Consider tracking which tables are created during the test and only dropping those.

---

### `tests/test_report_analysis.py`

**[warning]** `test_report_analysis.py:8-13` -- Six unused imports flagged by ruff (F401).

`MetricSummary`, `PersonalBest`, `AreaToImprove`, `SleepSummary`, `WorkoutSummary`, and `ReportData` are imported but never referenced directly in test code. These types are returned by the functions under test and verified via attribute access, but the imports are not needed. Remove them to satisfy the linter.

**[suggestion]** `test_report_analysis.py:97-98` -- Conditional assertion weakens the test.

```python
if result:
    assert result[0].trend_direction == "stable"
```

If `compute_metric_summaries` returns an empty list for single-day data, this test silently passes without asserting anything. Either assert that `result` is non-empty, or assert that it IS empty if that is the expected behavior.

---

### `tests/test_report_charts.py`

**[suggestion]** `test_report_charts.py:16-38` -- The `wellness_df` and `sleep_df` fixtures duplicate the same concept as in `test_report_analysis.py` but with different inline data rather than using the shared CSV fixtures. This is acceptable since chart tests need specific controlled data, but worth noting the duplication. If more test files need wellness/sleep fixtures, consider consolidating into `conftest.py`.

No other issues. The PNG magic byte validation pattern is well done.

---

### `tests/test_report_delivery.py`

**[warning]** `test_report_delivery.py` -- Heavy use of `unittest.mock` throughout.

Every test patches `boto3` with MagicMock. The project's CLAUDE.md states "Prefer real code over mocks." For an AWS SES integration, mocking is the pragmatic choice (you do not want tests sending real emails), so this is justified. However, consider using `moto` (AWS mock library) for higher-fidelity SES testing if the project depends on more AWS services in the future.

**[suggestion]** `test_report_delivery.py:36` -- `assert_called_once()` without checking arguments is followed by a separate argument check. This is fine, but `assert_called_once_with(...)` would combine both assertions.

---

### `tests/test_report_renderer.py`

No issues found. This is a well-structured test file with clear, descriptive test names. Good coverage of edge cases (missing workout data, missing charts, different period types). The use of a minimal but complete `ReportData` fixture is clean.

---

### `tests/test_asset_checks.py`

**[suggestion]** `test_asset_checks.py:12-14` -- Uses `__import__("os")` inline in the `skipif` marker. This works but is less readable than importing `os` at the top of the file. The same pattern appears in `test_upsert_day.py`. The `test_snowflake_resource.py` file does it the cleaner way with a top-level `import os`.

---

### `tests/test_oura_api.py`

**[warning]** `test_oura_api.py:256-378` -- The `TestExclusiveEndDateAdjustment` class has significant duplicated setup. Each test method creates the same token dict, constructs the API, and sets up a mock response. This boilerplate appears 4 times and should be extracted into a fixture or `setUp` method.

```python
# Repeated in 4 tests:
now = int(time.time())
tokens = {
    "access_token": "valid_token",
    "refresh_token": "refresh",
    "expires_in": 86400,
    "obtained_at": now,
}
api = _make_api()
api._load_tokens = MagicMock(return_value=tokens)
mock_resp = MagicMock()
mock_resp.ok = True
mock_resp.json.return_value = {"data": [{"id": "..."}]}
mock_resp.raise_for_status = MagicMock()
```

**[suggestion]** `test_oura_api.py:241,258,285,312` -- `from datetime import date` is imported inside individual test methods rather than at the top of the file. The module already imports at the top level in `_make_api` scope but `date` is not in the top-level imports. Move it to the top.

**[suggestion]** `test_oura_api.py:157-176` -- `_make_api_with_valid_token` and `_mock_http_error` are helper methods on the test class `TestGetHttpErrorHandling`. These are fine, but `_make_api_with_valid_token` duplicates logic also used in `TestExclusiveEndDateAdjustment`. Consider making it a shared fixture or module-level helper.

---

### `tests/test_report_data.py`

**[warning]** `test_report_data.py` -- Thin test coverage.

Each of the three fetch functions (`fetch_wellness_for_period`, `fetch_sleep_detail_for_period`, `fetch_workout_summary_for_period`) has only 1-4 tests. Missing coverage includes:
- Error handling when Snowflake connection fails
- Behavior when `fetch_pandas_all` returns unexpected column names
- Date boundary edge cases (start_date == end_date, start_date > end_date)

**[suggestion]** `test_report_data.py:16-22` -- The `_mock_snowflake_result` helper returns `(mock_cursor, mock_con)` but `mock_cursor` is only used in one test (`test_uses_parameterized_query`). In all other tests, only `mock_con` is used and `mock_cursor` is assigned to `_`. Consider returning just the connection and providing the cursor through it.

---

### `tests/test_dbt_translator.py`

No issues found. Clean parametrized tests, good edge case coverage (empty name, non-model resource types). Well-structured.

---

### `tests/test_snowflake_resource.py`

**[warning]** `test_snowflake_resource.py:55` -- Bare `pytest.raises(Exception)` is too broad.

```python
with pytest.raises(Exception):
    resource.get_connection()
```

This will pass for ANY exception, including unrelated errors. Match the specific exception type that `get_connection` should raise for an invalid private key (likely `ValueError` or `cryptography.exceptions.UnsupportedAlgorithm`).

**[suggestion]** `test_snowflake_resource.py` -- All tests require live Snowflake credentials. There are no unit tests for `SnowflakeResource` that can run without credentials (e.g., testing that config validation works, that missing fields raise errors).

---

### `tests/test_upsert_day.py`

No issues found. Excellent test organization by behavior (insert, empty, idempotent, partition date, invalid table). SQL injection test is a good security practice.

---

### `tests/test_report_assets.py`

**[suggestion]** `test_report_assets.py:17-31` -- `_previous_week_range()` and `_previous_month_range()` are tested without specifying a reference date, meaning they depend on the current system clock. If these functions accept an optional `today` parameter for testing, that would make tests deterministic. If not, these tests could theoretically produce different results depending on when they run (e.g., running on a Monday vs Wednesday).

**[suggestion]** `test_report_assets.py:109-116` -- `TestDefinitionsLoad.test_definitions_load` just asserts that the module import does not crash. This is a smoke test with low value but no harm.

---

### `tests/test_schedule.py`

**[suggestion]** `test_schedule.py:33-37` -- The `TestReportSchedules` tests duplicate assertions already present in `test_report_assets.py` `TestScheduleConfiguration`. The same cron schedules are verified in both files. Remove the duplicates from one file.

---

## 5. CI/CD Findings

### `.github/workflows/branch_deployments.yml`

**[blocking]** No test step exists in the workflow. Tests are never run as part of the CI/CD pipeline. Branch deployments go directly from checkout to Dagster Cloud deployment without running `pytest`. This means broken code can be deployed to branch environments without any automated quality gate.

**Recommendation:** Add a test job that runs before the deploy jobs:

```yaml
jobs:
  test:
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install uv && uv sync
      - run: uv run pytest tests/ -x --ignore=tests/test_snowflake_resource.py --ignore=tests/test_upsert_day.py --ignore=tests/test_asset_checks.py
```

**[warning]** `branch_deployments.yml:1` -- `DAGSTER_CLOUD_URL` uses `http://` instead of `https://`. This sends the API token over an unencrypted connection. Dagster Cloud should support HTTPS.

**[warning]** `branch_deployments.yml:43-44` -- `ref: ${{ github.head_ref }}` is used for checkout. On `pull_request` events, this checks out the head branch directly rather than the merge commit (`github.event.pull_request.head.sha`). This is fine for deployment but means the deployed code may not reflect the merged state.

**[suggestion]** `branch_deployments.yml:57` -- `pip install dbt-core dbt-snowflake` installs unpinned versions. These should be pinned to match the project's dependency lock file to avoid version drift between CI and local development.

**[suggestion]** `branch_deployments.yml` -- No dependency caching configured. Adding `pip` or `uv` caching would speed up builds.

**[warning]** `branch_deployments.yml` -- No `permissions` restriction on the Docker deploy job (`dagster_cloud_docker_deploy`). The top-level permissions block sets `contents: read` and `pull-requests: write`, which applies to both jobs. This is acceptable but worth noting -- the Docker deploy job likely does not need `pull-requests: write`.

### `.github/workflows/deploy.yml`

**[blocking]** Same as branch deployments: no test step before production deployment. Pushing to `main` or `master` triggers a deploy with zero automated testing.

**[warning]** `deploy.yml:1` -- Same `http://` issue for `DAGSTER_CLOUD_URL`.

**[warning]** `deploy.yml:42` -- `ref: ${{ github.head_ref }}` is used, but this workflow triggers on `push` events where `github.head_ref` is empty (it is only populated for pull request events). This should use `${{ github.sha }}` or simply omit the `ref` field to check out the pushed commit. This may cause the checkout to default to the default branch rather than the actual pushed commit.

**[warning]** `deploy.yml` -- No explicit `permissions` block. Defaults to whatever the repository's default workflow permissions are (usually `read` for everything, `write` for contents and packages). Adding explicit permissions is a security best practice.

**[suggestion]** `deploy.yml:6` -- Triggers on both `main` and `master`. Unless the repo uses both branch names, one of these can be removed to avoid confusion.

**[suggestion]** `deploy.yml:57` -- Same unpinned `dbt-core dbt-snowflake` install as branch deployments.

---

## 6. Cross-Cutting Concerns

### No CI test execution
The most significant finding across the entire review: **tests exist but are never run in CI**. There is no GitHub Actions workflow that executes `pytest`. This means all test quality analysis above is theoretical -- passing tests locally does not guarantee they pass on every commit.

### Snowflake-dependent tests cannot run in CI
Several test files (`test_upsert_day.py`, `test_asset_checks.py`, `test_snowflake_resource.py`) require live Snowflake credentials. These are correctly skipped when credentials are absent, but it means a CI test run would only execute the unit tests. Consider adding a `pytest.mark.integration` marker for Snowflake tests and documenting the split.

### Duplicated fixture patterns
The `wellness_df` and `sleep_df` fixtures are defined in both `test_report_analysis.py` (from CSV) and `test_report_charts.py` (inline). If more test files need similar fixtures, they should be centralized in `conftest.py`.

### Inconsistent `__import__("os")` pattern
`test_asset_checks.py` and `test_upsert_day.py` use `__import__("os").environ` in `pytest.mark.skipif`, while `test_snowflake_resource.py` uses a normal `import os`. Standardize on the cleaner approach.

### Missing type hints on test functions
The project standard requires type hints on all function signatures. Most test functions lack return type annotations (`-> None`). Only `test_schedule.py` consistently uses `-> None`. The fixture functions in `test_report_analysis.py` do include return type hints, which is good.

### No `conftest.py` fixtures for non-Snowflake tests
The shared `conftest.py` only provides the `snowflake_con` fixture. Chart and analysis tests define their own fixtures locally. As the test suite grows, shared fixtures in `conftest.py` (or a second conftest in a subdirectory) would reduce duplication.

---

## 7. Recommendations (Prioritized)

### P0 -- Must fix

1. **Add a CI test workflow.** Create `.github/workflows/test.yml` that runs `uv run pytest` on pull requests and pushes to main. This is the single highest-impact improvement. Without it, the entire test suite provides zero automated protection.

2. **Fix `DAGSTER_CLOUD_URL` to use HTTPS.** Both workflow files use `http://` which transmits the API token in cleartext. Change to `https://charles-likes-data.dagster.plus`.

3. **Fix `deploy.yml` checkout ref.** `github.head_ref` is empty on push events. Remove the `ref` field or use `${{ github.sha }}`.

### P1 -- Should fix

4. **Remove unused imports in `test_report_analysis.py`.** Run `uv run ruff check tests/ --fix` to auto-fix the 6 F401 violations.

5. **Fix SQL injection pattern in `conftest.py` cleanup.** Use quoted identifiers in the DROP TABLE statement.

6. **Extract duplicated setup in `TestExclusiveEndDateAdjustment`.** Create a fixture or shared helper for the repeated token/mock setup.

7. **Narrow the exception type in `test_snowflake_resource.py:55`.** Replace `pytest.raises(Exception)` with the specific expected exception.

8. **Add `permissions` block to `deploy.yml`.** Match the explicit permissions from `branch_deployments.yml`.

### P2 -- Nice to have

9. **Pin dbt versions in CI workflows.** Use `pip install dbt-core==X.Y.Z dbt-snowflake==X.Y.Z`.

10. **Add dependency caching to CI workflows.**

11. **Standardize `-> None` return type hints on all test functions.**

12. **Fix the conditional assertion in `test_report_analysis.py:97-98`.** Either assert the list is non-empty or change the test to assert empty.

13. **Consolidate duplicate schedule assertions between `test_schedule.py` and `test_report_assets.py`.**

14. **Add unit tests for `SnowflakeResource` config validation that don't require credentials.**

15. **Add `pytest.mark.integration` marker for Snowflake-dependent tests and document the marker in the project context.**

16. **Move `from datetime import date` to top-level imports in `test_oura_api.py`.**

17. **Consider adding tests for `dbt_assets.py:_ensure_key_file()` and `oura_oauth_cli.py`.**

---

*Review generated by Claude (DAA Code Review) on 2026-03-31.*
