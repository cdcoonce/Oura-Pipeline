# Phase 4: Verification (Post-Deploy)

**Scope:** End-to-end validation after merging the migration PR to `main` and deploying to Dagster Cloud.
**Depends on:** Phases 0–3 complete, PR merged, Dagster Cloud deployment successful.
**Goal:** Confirm data flows from Oura API → Snowflake raw → dbt staging → dbt marts with no regressions.

---

## 4.1 — Pre-Deployment Checklist

Before merging, confirm all items from Phase 0 are done:

- [ ] Snowflake account + setup SQL executed (all schemas, tables, grants exist)
- [ ] Key pair assigned to `OURA_PIPELINE` user
- [ ] OAuth tokens seeded in `CONFIG.OAUTH_TOKENS`
- [ ] Dagster Cloud env vars set (`SNOWFLAKE_*`, `OURA_CLIENT_ID`, `OURA_CLIENT_SECRET`)
- [ ] GitHub Actions secrets set (`SNOWFLAKE_*`)
- [ ] Tests pass locally: `uv run pytest`
- [ ] `dagster definitions validate` passes
- [ ] dbt parses: `cd dbt_oura && dbt parse --profiles-dir .`

---

## 4.2 — Deploy and Verify CI

### Steps

1. **Merge PR to `main`.**
2. **Monitor GitHub Actions `deploy.yml` run:**
   - Confirm `dbt parse` step succeeds (generates manifest with Snowflake profile).
   - Confirm Dagster Cloud deployment succeeds.
3. **Check Dagster Cloud UI:**
   - Navigate to the deployment. Verify all assets load (13 raw + 13 staging + 2 marts + 2 reports).
   - Verify the `snowflake` resource appears in Resources.
   - Verify schedules are visible (`daily_oura_schedule`, `weekly_report_schedule`, `monthly_report_schedule`).

### Troubleshooting

| Symptom                 | Likely Cause                                | Fix                                             |
| ----------------------- | ------------------------------------------- | ----------------------------------------------- |
| `dbt parse` fails in CI | Missing Snowflake secrets in GitHub Actions | Add secrets per Phase 0.5                       |
| Assets fail to load     | Import error (DuckDB reference lingering)   | Grep for `duckdb` in `src/`                     |
| Resource not found      | `definitions.py` wiring mismatch            | Verify resource key is `snowflake` not `duckdb` |

---

## 4.3 — Materialize 1 Partition of 1 Asset

### Steps

1. **In Dagster Cloud UI**, navigate to the `oura_raw/readiness` asset (simplest — single row per day).
2. **Select a recent partition** (e.g., yesterday's date).
3. **Click "Materialize"** and monitor the run.
4. **Verify in run logs:**
   - "Connecting to Snowflake account=..." log message appears.
   - `dagster/row_count` metadata shows expected count (usually 1 for readiness).
   - No errors.

### Verify in Snowflake

```sql
-- Check the raw table was created and has data
SELECT COUNT(*) FROM OURA.OURA_RAW.READINESS;
-- Expect: 1 (for the single partition)

-- Verify VARIANT data is correct
SELECT
    raw_data:day::varchar AS day,
    raw_data:score::int AS score,
    partition_date
FROM OURA.OURA_RAW.READINESS;
-- Expect: day matches partition, score is a reasonable integer (1-100)
```

### Troubleshooting

| Symptom                              | Likely Cause                                     | Fix                                        |
| ------------------------------------ | ------------------------------------------------ | ------------------------------------------ |
| "FileNotFoundError: No OAuth tokens" | Tokens not seeded in `CONFIG.OAUTH_TOKENS`       | Run seed SQL from Phase 0.3                |
| Connection timeout                   | Wrong account identifier or network/firewall     | Verify `SNOWFLAKE_ACCOUNT` format          |
| "Invalid table name"                 | Asset passes wrong table name to `_upsert_day()` | Check asset factory table name mapping     |
| 0 rows returned from API             | Token expired, Oura API issue                    | Check API response in logs, re-seed tokens |

---

## 4.4 — Run dbt Manually

### Steps

After at least one raw asset is materialized, run dbt to build staging and mart models.

**Option A: Via Dagster UI**

1. Navigate to any dbt staging asset (e.g., `stg_readiness`).
2. Click "Materialize" — this triggers `dbt build` for the selected model.

**Option B: Via CLI (local, with Snowflake creds)**

```bash
cd dbt_oura

# Run all staging models
dbt run --select staging --profiles-dir .

# Run all mart models
dbt run --select marts --profiles-dir .

# Run all not_null tests
dbt test --profiles-dir .
```

### Verify in Snowflake

```sql
-- Staging table exists and has typed columns
SELECT * FROM OURA.OURA_STAGING.STG_READINESS LIMIT 5;
-- Expect: day (DATE), readiness_score (NUMBER), partition_date (DATE)

-- Mart table exists
SELECT * FROM OURA.OURA_MARTS.FACT_DAILY_WELLNESS LIMIT 5;
-- Expect: day, readiness_score, steps, calories, sleep_score, etc.

-- dbt not_null tests passed (check dbt output for "Pass")
```

### Troubleshooting

| Symptom                           | Likely Cause                                         | Fix                                          |
| --------------------------------- | ---------------------------------------------------- | -------------------------------------------- |
| "Object does not exist" on source | Raw table not yet created                            | Materialize the raw asset first              |
| Column type mismatch              | VARIANT cast wrong (e.g., `::int` on a string field) | Check staging model VARIANT paths            |
| not_null test fails               | VARIANT path typo (returns NULL silently)            | Compare path to actual JSON keys in raw_data |
| "Table already exists"            | Schema conflict from previous partial run            | `dbt run --full-refresh` to recreate         |

---

## 4.5 — Verify Staging + Mart Tables

### Spot-Check Queries

Run these against Snowflake to validate data integrity across the full pipeline:

```sql
-- 1. Raw → Staging column mapping is correct
SELECT
    r.raw_data:score::int AS raw_score,
    s.readiness_score AS staged_score
FROM OURA.OURA_RAW.READINESS r
JOIN OURA.OURA_STAGING.STG_READINESS s
    ON r.partition_date = s.partition_date
LIMIT 5;
-- Expect: raw_score == staged_score for all rows

-- 2. Mart joins are working
SELECT COUNT(*) FROM OURA.OURA_MARTS.FACT_DAILY_WELLNESS
WHERE day IS NOT NULL;
-- Expect: > 0

-- 3. Sleep detail mart
SELECT COUNT(*) FROM OURA.OURA_MARTS.FACT_SLEEP_DETAIL
WHERE id IS NOT NULL;
-- Expect: > 0

-- 4. No orphaned NULLs from VARIANT typos
SELECT
    COUNT(*) AS total_rows,
    COUNT(readiness_score) AS non_null_score
FROM OURA.OURA_STAGING.STG_READINESS;
-- Expect: total_rows == non_null_score
```

---

## 4.6 — Backfill All Partitions

### Steps

1. **In Dagster Cloud UI**, select all 13 raw assets.
2. **Use "Backfill" feature** to materialize all partitions from `2024-01-01` to today.
3. **Monitor progress:**
   - Watch for rate limiting from Oura API (the existing code handles pagination).
   - Heartrate backfill is the heaviest (~2000 rows/day × 400+ days).
   - Snowflake XSMALL warehouse is sufficient — queries are tiny.
4. **After backfill completes**, run a full dbt build:
   ```bash
   cd dbt_oura && dbt build --profiles-dir .
   ```

### Backfill Monitoring Queries

```sql
-- Row counts per table (compare to expected)
SELECT 'sleep' AS tbl, COUNT(*) AS rows FROM OURA.OURA_RAW.SLEEP
UNION ALL SELECT 'activity', COUNT(*) FROM OURA.OURA_RAW.ACTIVITY
UNION ALL SELECT 'readiness', COUNT(*) FROM OURA.OURA_RAW.READINESS
UNION ALL SELECT 'heartrate', COUNT(*) FROM OURA.OURA_RAW.HEARTRATE
UNION ALL SELECT 'spo2', COUNT(*) FROM OURA.OURA_RAW.SPO2
UNION ALL SELECT 'stress', COUNT(*) FROM OURA.OURA_RAW.STRESS
UNION ALL SELECT 'resilience', COUNT(*) FROM OURA.OURA_RAW.RESILIENCE
UNION ALL SELECT 'sleep_periods', COUNT(*) FROM OURA.OURA_RAW.SLEEP_PERIODS
UNION ALL SELECT 'sleep_time', COUNT(*) FROM OURA.OURA_RAW.SLEEP_TIME
UNION ALL SELECT 'workouts', COUNT(*) FROM OURA.OURA_RAW.WORKOUTS
UNION ALL SELECT 'sessions', COUNT(*) FROM OURA.OURA_RAW.SESSIONS
UNION ALL SELECT 'tags', COUNT(*) FROM OURA.OURA_RAW.TAGS
UNION ALL SELECT 'rest_mode_periods', COUNT(*) FROM OURA.OURA_RAW.REST_MODE_PERIODS
ORDER BY tbl;

-- Partition coverage (are there gaps?)
SELECT
    MIN(partition_date) AS earliest,
    MAX(partition_date) AS latest,
    COUNT(DISTINCT partition_date) AS days_covered
FROM OURA.OURA_RAW.SLEEP;
-- Expect: earliest near 2024-01-01, latest near today
```

### Cost Check

After backfill, verify warehouse costs are reasonable:

```sql
-- Credit usage for the past 7 days
SELECT
    DATE_TRUNC('day', start_time) AS day,
    SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'COMPUTE_WH'
    AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

---

## 4.7 — Verify Schedules

### Steps

1. **Enable the daily schedule** in Dagster Cloud UI if not already active.
2. **Wait for the next 6 AM UTC run** (or trigger manually).
3. **Confirm** the run:
   - Materializes all 13 raw assets for the latest partition.
   - Triggers dbt build for downstream models.
   - No errors in the run log.

### Report Schedule (Optional)

If reports are desired, enable `weekly_report_schedule` and/or `monthly_report_schedule`. These depend on mart tables being populated, so only enable after backfill is complete.

---

## 4.8 — Post-Migration Cleanup

After the migration is verified and stable (recommend waiting ~1 week of successful daily runs):

### Code Cleanup

- [ ] Grep for any remaining `duckdb` references in `src/`: `grep -ri duckdb src/`
- [ ] Grep for `DUCKDB_PATH` or `OURA_TOKEN_PATH` references
- [ ] Remove `polars` from `pyproject.toml` if unused (grep for `import polars` — still used in reports)
- [ ] Remove `pyarrow` from `pyproject.toml` if unused (grep for `import pyarrow`)
- [ ] Delete local `data/oura.duckdb` file
- [ ] Delete local `data/tokens/` directory

### Dagster Cloud Cleanup

- [ ] Remove `DUCKDB_PATH` env var from Dagster Cloud
- [ ] Remove `OURA_TOKEN_PATH` env var from Dagster Cloud

### Documentation

- [ ] Move `docs/plans/snowflake_migration.md` to `docs/archive/`
- [ ] Move `docs/plans/snowflake_migration/` phase docs to `docs/archive/snowflake_migration/`
- [ ] Update `CLAUDE.md` if any development workflow changed

---

## Rollback Plan

If critical issues are found at any point during verification:

1. **Revert the merge commit:**
   ```bash
   git revert <merge-commit-hash>
   git push origin main
   ```
2. **Auto-deploys** the DuckDB version back to Dagster Cloud (~5 min).
3. **Snowflake data persists** independently — no cleanup needed.
4. **Local `dagster dev`** works immediately with the DuckDB file.

### Partial Rollback (dbt only)

If raw ingestion works but dbt has issues:

- Revert only the dbt changes (profiles.yml, staging models).
- Raw data in Snowflake is fine — just needs correct dbt models to query it.

---

## Success Criteria

The migration is complete when:

- [ ] All 13 raw assets materialize successfully for at least 7 consecutive days
- [ ] All dbt models build without errors
- [ ] All dbt `not_null` tests pass
- [ ] Report generation works (if enabled)
- [ ] `uv run pytest` passes (with Snowflake credentials)
- [ ] No DuckDB references remain in the codebase
- [ ] Snowflake costs are within expected range (~$0.01/day)
