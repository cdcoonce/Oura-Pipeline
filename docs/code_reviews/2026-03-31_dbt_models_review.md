# dbt Models Code Review

**Date:** 2026-03-31
**Reviewer:** Claude (DAA Code Review)
**Scope:** All dbt models, YAML configuration, and project setup under `dbt_oura/`

---

## 1. Summary

The dbt project is well-organized with a clean two-layer DAG (staging -> marts), consistent naming conventions, and proper use of `source()` / `ref()` throughout. Staging models follow a uniform pattern of extracting fields from a `raw_data` variant column, and the two mart models compose staging models into useful analytical tables.

**Overall health: Good, with targeted improvements needed.**

The main areas of concern are:
- Missing `unique` tests on primary keys across all models
- No schema/test YAML for mart models at all
- Potential correctness issue in `fact_daily_wellness.sql` join strategy
- No source freshness configured
- No model or column descriptions anywhere

---

## 2. Findings by File

### 2.1 `dbt_oura/dbt_project.yml`

**[INFO]** `dbt_project.yml:14-17` -- Default materialization is `table` for all models.

This is fine for a personal analytics project with low data volumes. If data grows, consider `incremental` for staging models that parse append-only raw data, and `view` for lightweight staging models to avoid redundant storage.

---

### 2.2 `dbt_oura/profiles.yml`

**[INFO]** `profiles.yml:8` -- `private_key_path` defaults to empty string when env var is unset.

If `SNOWFLAKE_PRIVATE_KEY_PATH` is not set, the empty string default will likely cause a confusing Snowflake connection error rather than a clear "missing auth" message. This is acceptable for a single-developer project but worth noting.

**[INFO]** `profiles.yml:1-13` -- Only a `dev` target is defined.

No `prod` target exists. If production runs use the same configuration via environment variables this is fine, but having an explicit `prod` target with `threads: 1` or different defaults is a common safeguard.

---

### 2.3 `dbt_oura/models/sources.yml`

**[WARNING]** `sources.yml:1-60` -- No source freshness configuration.

None of the 13 source tables have `loaded_at_field` or `freshness` blocks. Adding freshness checks (e.g., on `partition_date`) would allow `dbt source freshness` to detect stale data before downstream models silently produce outdated results.

```yaml
# Example addition per table:
- name: sleep
  loaded_at_field: partition_date
  freshness:
    warn_after: {count: 2, period: day}
    error_after: {count: 7, period: day}
```

**[WARNING]** `sources.yml:1-60` -- No column descriptions on any source table.

Every table lists `raw_data` and `partition_date` columns but none have `description` fields. This makes the auto-generated dbt docs unhelpful for anyone unfamiliar with the Oura API schema.

**[INFO]** `sources.yml:1-60` -- No source-level tests.

Consider adding `not_null` tests on `raw_data` and `partition_date` at the source level so issues are caught before staging models run.

---

### 2.4 `dbt_oura/models/staging/schema.yml`

**[ERROR]** `schema.yml:1-125` -- No `unique` tests on any primary key column.

Models like `stg_sleep`, `stg_sleep_periods`, `stg_workouts`, `stg_sessions`, `stg_tags`, and `stg_rest_mode_periods` have `id` columns with only `not_null` tests. Without `unique` tests, duplicate rows from the raw layer will silently propagate into marts, causing inflated counts and incorrect joins. This is the highest-priority finding in this review.

Fix: Add `unique` tests alongside `not_null` on every `id` column.

```yaml
- name: id
  tests: [not_null, unique]
```

For models keyed on `day` (e.g., `stg_activity`, `stg_readiness`, `stg_spo2`, `stg_stress`, `stg_resilience`), add `unique` tests on the `day` column as well, since these models assume one row per day.

**[WARNING]** `schema.yml:1-125` -- No model descriptions.

None of the 13 staging models have a `description` field. This makes dbt docs auto-generation less useful and makes onboarding harder.

**[WARNING]** `schema.yml:1-125` -- No column descriptions.

No columns across any staging model have descriptions. At minimum, columns with non-obvious names (e.g., `efficiency`, `latency`, `breathing_disturbance_index`, `stress_high`, `recovery_high`) should be documented.

**[WARNING]** `schema.yml:70` -- `resilience_level` lacks an `accepted_values` test.

The Oura API returns a known set of resilience levels. An `accepted_values` test would catch unexpected values from API changes.

**[WARNING]** `schema.yml:81` -- `sleep_type` lacks an `accepted_values` test.

Sleep period types from the Oura API are a known enum (e.g., `long_sleep`, `rest`, `nap`). An `accepted_values` test would flag unexpected values.

---

### 2.5 Staging SQL Models (all 13 files)

**[INFO]** All staging models -- Consistent, clean pattern.

All staging models follow the same structure: CTE wrapping `source()`, then `select` with explicit casts from the variant column. This is good practice -- it is easy to read and maintain.

**[WARNING]** `stg_heartrate.sql:9` -- `raw_data:timestamp::date as day` silently truncates timezone information.

Casting a timestamp string directly to `date` in Snowflake uses the session timezone. If the Oura API returns UTC timestamps but the user is in a different timezone, this could assign heart rate readings to the wrong calendar day. Was this intentional?

**[WARNING]** `stg_sleep_periods.sql:9-10` -- `bedtime_start` and `bedtime_end` are cast to `varchar` instead of `timestamp_tz` or `timestamp_ntz`.

These are clearly datetime values. Keeping them as strings prevents time-based calculations in downstream models (e.g., computing actual sleep duration, filtering by bedtime hour). The same issue applies to `start_datetime`/`end_datetime` in `stg_workouts.sql:8-9` and `stg_sessions.sql:8-9`.

**[INFO]** `stg_tags.sql:11` -- `raw_data:tags::varchar` flattens an array to a string.

If the Oura API returns `tags` as a JSON array, casting to `varchar` produces a string like `["tag1","tag2"]`. This is fine for display but will require parsing in downstream models if you ever need to filter by individual tags. Consider using `lateral flatten` if per-tag analysis is needed in the future.

**[INFO]** All staging models -- No deduplication logic.

If the raw layer can contain duplicate records (e.g., from overlapping API pulls), the staging models will pass duplicates through. This is related to the missing `unique` tests above. Consider adding `qualify row_number() over (partition by id order by partition_date desc) = 1` to models with an `id` column.

---

### 2.6 `dbt_oura/models/marts/fact_sleep_detail.sql`

**[INFO]** `fact_sleep_detail.sql:1-31` -- Clean, well-structured model.

The join between `stg_sleep_periods` and `stg_sleep_time` on `day` is straightforward and easy to follow.

**[WARNING]** `fact_sleep_detail.sql:4-5` -- CTE aliases `sp` and `st` are abbreviated.

While short, these aliases are used in a small model so readability impact is minor. However, the project's code style guidelines state "Descriptive variable names (`private_key_bytes` not `pkb`)". Consider `sleep_periods` and `sleep_time` for consistency with the project standard.

**[WARNING]** `fact_sleep_detail.sql:30` -- `left join st on sp.day = st.day` may fan out rows.

If `stg_sleep_periods` has multiple rows per day (e.g., a main sleep and a nap), and `stg_sleep_time` has one row per day, the join will duplicate the sleep_time columns for each period. This is probably intentional, but if `stg_sleep_time` could also have multiple rows per day, this becomes a many-to-many join producing a cartesian product. The missing `unique` test on `stg_sleep_time.day` means this risk is unvalidated.

---

### 2.7 `dbt_oura/models/marts/fact_daily_wellness.sql`

**[WARNING]** `fact_daily_wellness.sql:27-32` -- Inconsistent join predicates create a subtle correctness risk.

The first three joins use `ready.day` as the anchor:
```sql
full outer join sleep  on sleep.day  = ready.day
full outer join act    on act.day    = ready.day
```

But the last three joins switch to `coalesce(sleep.day, act.day, ready.day)`:
```sql
full outer join spo2   on spo2.day   = coalesce(sleep.day, act.day, ready.day)
full outer join stress on stress.day = coalesce(sleep.day, act.day, ready.day)
```

This asymmetry means: if a day exists in `sleep` or `act` but NOT in `ready`, the first two joins will fail to match (because `ready.day` is null), but the last three joins will find it via coalesce. This is technically correct as written -- the coalesce compensates for the full outer join nulls. However, the inconsistency is confusing and error-prone. A cleaner pattern would be to use a date spine CTE or consistently coalesce in all join conditions.

**[ERROR]** `fact_daily_wellness.sql:27-32` -- Full outer join chain without a date spine can produce duplicate days.

If `sleep` has a day that `ready` does not, and `act` also has that same day but `ready` does not, the `sleep` row joins on `ready.day` (null, no match) and the `act` row also joins on `ready.day` (null, no match). This could result in separate rows for the same calendar day -- one with sleep data and one with activity data -- rather than a single merged row.

The standard fix is to build a date spine from the union of all source days, then left join each source onto it:

```sql
with date_spine as (
  select day from stg_readiness
  union
  select day from stg_sleep
  union
  select day from stg_activity
  -- etc.
)
select
  date_spine.day,
  ...
from date_spine
left join stg_readiness on ...
left join stg_sleep on ...
```

**[WARNING]** `fact_daily_wellness.sql:4-9` -- Abbreviated CTE aliases (`act`, `ready`, `res`).

Same project style concern as `fact_sleep_detail.sql`. The CLAUDE.md standard prefers descriptive names.

---

### 2.8 `dbt_oura/models/marts/` -- No `schema.yml`

**[ERROR]** No YAML schema file exists for mart models.

Neither `fact_sleep_detail` nor `fact_daily_wellness` has any test or documentation YAML. This means:
- No tests on mart output columns (e.g., `not_null` on `day`, `unique` on `id` in `fact_sleep_detail`)
- No model descriptions
- No column descriptions
- dbt docs will show these models as undocumented

---

### 2.9 `dbt_oura/macros/generate_schema_name.sql`

**[INFO]** `generate_schema_name.sql:1-12` -- Clean override, well-commented.

The macro correctly overrides the default dbt behavior of prepending the target schema. The comment explains why. Good.

---

## 3. Cross-Cutting Concerns

### 3.1 No `unique` tests anywhere in the project

No model has a `unique` test on any column. This is the single most impactful gap. Without uniqueness guarantees, any duplication in the raw layer will silently propagate through staging into marts, producing incorrect aggregations and fan-out joins.

**Priority: HIGH**

### 3.2 No documentation (descriptions) on any model or column

Across `sources.yml` and `staging/schema.yml`, there are zero `description` fields. The mart models have no YAML at all. For a personal project this is low risk, but it makes the project harder to hand off or return to after time away.

**Priority: MEDIUM**

### 3.3 Timestamp columns stored as varchar

Multiple staging models cast timestamp/datetime values to `varchar` instead of proper timestamp types: `stg_sleep.timestamp`, `stg_heartrate.ts`, `stg_sleep_periods.bedtime_start/end`, `stg_workouts.start_datetime/end_datetime`, `stg_sessions.start_datetime/end_datetime`, `stg_sleep_time.sleep_time_status` (this one is legitimately a string).

Storing these as strings prevents native time-based operations in SQL and forces downstream consumers to re-parse them.

**Priority: MEDIUM**

### 3.4 No deduplication in staging models

If the raw data ingestion process can produce duplicate records (common with API-based pipelines pulling overlapping date ranges), the staging layer should deduplicate. Models with `id` columns should use `qualify row_number() over (partition by id order by partition_date desc) = 1`. Models keyed by `day` should use a similar pattern.

**Priority: MEDIUM**

### 3.5 All models materialized as `table`

Every model -- staging and marts -- is materialized as a full table rebuild. For a small personal dataset this is fine, but as data grows, staging models are good candidates for `incremental` materialization keyed on `partition_date`.

**Priority: LOW**

### 3.6 No macros for repeated SQL patterns

Every staging model repeats the same CTE-from-source pattern. While each model's column list is unique, the boilerplate could be reduced with a macro if the project grows. Not urgent for 13 models.

**Priority: LOW**

---

## 4. Recommendations (Prioritized)

| Priority | Recommendation | Affected Files |
|----------|---------------|----------------|
| 1 - HIGH | Add `unique` tests on all primary key / grain columns (`id`, `day`) | `staging/schema.yml`, new `marts/schema.yml` |
| 2 - HIGH | Create `marts/schema.yml` with tests and descriptions for `fact_sleep_detail` and `fact_daily_wellness` | New file |
| 3 - HIGH | Refactor `fact_daily_wellness.sql` to use a date spine instead of chained full outer joins | `marts/fact_daily_wellness.sql` |
| 4 - MEDIUM | Add source freshness configuration to `sources.yml` | `sources.yml` |
| 5 - MEDIUM | Cast timestamp columns to proper `timestamp_tz` or `timestamp_ntz` types in staging models | `stg_sleep_periods.sql`, `stg_workouts.sql`, `stg_sessions.sql`, `stg_heartrate.sql`, `stg_sleep.sql` |
| 6 - MEDIUM | Add deduplication logic (`qualify row_number()`) to staging models | All staging `.sql` files |
| 7 - MEDIUM | Add model and column descriptions across all YAML files | `sources.yml`, `staging/schema.yml`, new `marts/schema.yml` |
| 8 - MEDIUM | Add `accepted_values` tests on enum columns (`resilience_level`, `sleep_type`, `intensity`, `session_type`, `mood`) | `staging/schema.yml` |
| 9 - LOW | Rename abbreviated CTE aliases to descriptive names per project style standards | `fact_sleep_detail.sql`, `fact_daily_wellness.sql` |
| 10 - LOW | Consider `incremental` materialization for staging models if data volume grows | `dbt_project.yml` or individual model configs |
