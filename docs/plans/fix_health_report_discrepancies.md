# Fix Health Report Data Discrepancies

## Problem

Querying Snowflake revealed three data discrepancies affecting health reports:

1. **Duplicate rows in `fact_daily_wellness`** — 6 days (Mar 23-28) are doubled due to
   `stg_resilience` having duplicate rows that fan out through the LEFT JOIN. The staging
   QUALIFY dedup is correct in code but the table hasn't been rebuilt.
2. **No defensive dedup in the mart** — `fact_daily_wellness` trusts staging to be unique
   per day. Any staging duplicate fans out across all columns.
3. **Missing uniqueness test on `stg_sleep.day`** — `stg_sleep` tests uniqueness on `id`
   but not on `day`, which is the join key used by `fact_daily_wellness`.

## Fixes

### 1. Add defensive QUALIFY to `fact_daily_wellness.sql`

Add `QUALIFY ROW_NUMBER() OVER (PARTITION BY day ORDER BY day) = 1` as a safety net
at the end of the final SELECT. This ensures 1 row per day regardless of staging state.

### 2. Add `unique` test on `stg_sleep.day`

The staging schema.yml tests `id` for uniqueness but `day` is the actual join key used
by `fact_daily_wellness`. Add a `unique` test on `day` to catch dedup failures at test time.

### 3. Verify all staging models have `unique` tests on their join keys

All staging models joined by fact_daily_wellness should have `unique` on `day`:
- stg_sleep: has unique on `id`, **missing on `day`** ← fix
- stg_activity: has unique on `day` ✓
- stg_readiness: has unique on `day` ✓
- stg_spo2: has unique on `day` ✓
- stg_stress: has unique on `day` ✓
- stg_resilience: has unique on `day` ✓

## Out of Scope (operational)

- Running `dbt run` to rebuild tables (resolves current duplicates)
- Investigating March 3-9 data gap (likely ring not worn)
- Investigating workout ingestion 4-day lag
