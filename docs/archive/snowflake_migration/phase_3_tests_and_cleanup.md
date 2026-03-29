# Phase 3: Cleanup

**Scope:** Tasks 14–15, 17 — update `.env.example`, remove DuckDB concurrency workaround, update project context, remove dead code.
**Depends on:** Phase 1 (core migration + tests) and Phase 2 (dbt migration + tests).
**Enables:** Phase 4 (verification).

> All test rewrites were completed in Phase 1 (pytest) and Phase 2 (dbt schema.yml). This phase is cleanup only — no new behavior, no new tests needed.

---

## 3.1 — Update `.env.example` (Task 14)

> TDD exception: configuration file.

### File: `.env.example`

```env
# Oura OAuth2
OURA_CLIENT_ID=
OURA_CLIENT_SECRET=
OURA_REDIRECT_URI=http://127.0.0.1:8765/callback
OURA_SCOPES="email personal daily heartrate workout session tag spo2 stress resilience"

# Snowflake
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=OURA_PIPELINE
SNOWFLAKE_PRIVATE_KEY=<base64-encoded-PEM-private-key>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=OURA
SNOWFLAKE_ROLE=TRANSFORM

# dbt (local dev — path to .p8 key file)
SNOWFLAKE_PRIVATE_KEY_PATH=snowflake_key.p8

# Dagster
DAGSTER_HOME=/path/to/repo/.dagster

# Reports (optional)
SES_SENDER_EMAIL=
SES_RECIPIENT_EMAIL=
AWS_REGION=us-east-1
```

### Removed

- `OURA_TOKEN_PATH` — tokens now in Snowflake `CONFIG.OAUTH_TOKENS`
- `DUCKDB_PATH` — no longer used
- `OURA_DATA_DIR` — raw data dir no longer needed

---

## 3.2 — Remove `max_concurrent_runs` Workaround (Task 15)

### Context

The parent migration plan mentions removing `max_concurrent_runs: 1`, a DuckDB file-locking workaround.

### Action

- Search for `max_concurrent_runs` in the codebase. If found in code, remove it.
- If set in Dagster Cloud UI (Deployment Settings), update to allow default concurrency.
- The `in_process_executor` in `definitions.py` may also be DuckDB-related — evaluate whether `multiprocess_executor` is appropriate. However, `in_process_executor` is simpler and fine for this workload size. **Leave it unless there's a specific need.**

### Where to Search

- `dagster_cloud.yaml`
- `src/dagster_project/definitions.py`
- `src/dagster_project/defs/schedules.py`

---

## 3.3 — Remove Dead DuckDB References

Grep the entire codebase for any remaining DuckDB references:

```bash
grep -ri duckdb src/ tests/ dbt_oura/ pyproject.toml .env.example
grep -ri DUCKDB_PATH src/ tests/ .env.example
grep -ri OURA_TOKEN_PATH src/ tests/ .env.example
grep -ri "from pathlib import Path" src/dagster_project/defs/resources.py
```

**Expected removals:**

- Any lingering `import duckdb` statements
- `DuckDBResource` references in type hints or comments
- `DUCKDB_PATH` / `OURA_TOKEN_PATH` in any file
- `dbt-duckdb` in `pyproject.toml` (should already be gone from Phase 1)
- `data/oura.duckdb` from `.gitignore` (optional — harmless to leave)

---

## 3.4 — Update Project Context (Task 17)

### File: `.claude/docs/project.md`

Update the following sections:

**Tech Stack:**

```
- DuckDB → Snowflake
- dbt-duckdb → dbt-snowflake
- Polars (still used in report modules for DataFrame operations)
- snowflake-connector-python[pandas] (database connectivity)
- cryptography (key-pair auth)
```

**Data Flow Diagram:**

```
Oura Ring API (v2) → OuraAPI → JSON responses
                        ↓
                  json.dumps() → _upsert_day()
                        ↓
                  Snowflake OURA.OURA_RAW.* (13 tables, VARIANT + partition_date)
                        ↓
                  dbt staging (13 stg_* models, VARIANT → typed columns)
                        ↓
                  dbt marts (fact_daily_wellness, fact_sleep_detail)
```

**Environment Variables:**

Remove `DUCKDB_PATH`, `OURA_TOKEN_PATH`, `OURA_DATA_DIR`.
Add all `SNOWFLAKE_*` vars.

**Architecture Patterns:**

- Snowflake key-pair authentication (base64 PEM → DER)
- VARIANT raw layer (schema-on-read)
- Snowflake-backed OAuth token storage (`CONFIG.OAUTH_TOKENS`)
- `executemany()` batched inserts for heartrate data
- `not_null` dbt tests as VARIANT safety net

---

## Implementation Order

All steps in Phase 3 are independent and can be done in parallel:

```
3.1  .env.example            config update
3.2  max_concurrent_runs     search + remove if found
3.3  Dead DuckDB references  grep + delete
3.4  project.md              documentation update (do last — documents final state)
```

---

## Files Modified in Phase 3

| File                              | Action                                               |
| --------------------------------- | ---------------------------------------------------- |
| `.env.example`                    | Remove DuckDB vars, add Snowflake vars               |
| `.claude/docs/project.md`         | Update tech stack, data flow, env vars, architecture |
| Various (if grep finds leftovers) | Remove dead DuckDB references                        |

## Validation

```bash
# Full pytest suite still passes
uv run pytest -v

# No DuckDB references remain
grep -ri duckdb src/ tests/ dbt_oura/ pyproject.toml .env.example
# Expected: no output

# Dagster definitions still load
uv run dagster definitions validate
```
