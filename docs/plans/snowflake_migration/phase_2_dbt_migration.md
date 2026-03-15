# Phase 2: dbt Migration (Tests-First)

**Scope:** Tasks 9–12 — dbt profile, project config, staging model rewrites (DuckDB column access → Snowflake VARIANT syntax), `not_null` tests, and sources.yml update.
**Depends on:** Phase 1 (SnowflakeResource exists, raw tables use `raw_data VARIANT + partition_date DATE` schema).
**Enables:** Phase 4 verification (`dbt run` + `dbt test`).
**Methodology:** dbt config files (profiles, project, sources) are TDD exceptions per `tdd.md`. But `schema.yml` not_null tests are written **before** the staging models — they serve as the RED step. The staging model rewrites are the GREEN step that makes those tests pass.

---

## 2.1 — Config: dbt Profile (Task 9)

> TDD exception: configuration file.

### File: `dbt_oura/profiles.yml`

**Replace entire file:**

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

### Key Decision: `private_key_path` vs `private_key_content`

- **Local dev:** `private_key_path` pointing to the `.p8` file.
- **Dagster Cloud / CI:** dbt-snowflake >=1.7 supports `private_key_content` (base64-encoded PEM) — no file management needed.

**If dbt-snowflake supports `private_key_content`** (verify after install):

```yaml
oura_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_content: "{{ env_var('SNOWFLAKE_PRIVATE_KEY', '') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORM') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'OURA') }}"
      schema: oura_marts
      threads: 4
```

---

## 2.2 — Config: dbt Project (Task 9)

> TDD exception: configuration file.

### File: `dbt_oura/dbt_project.yml`

**Change profile reference:**

```yaml
# OLD:
profile: "oura_duckdb"

# NEW:
profile: "oura_snowflake"
```

No other changes needed.

---

## 2.3 — Config: dbt Sources (Task 12)

> TDD exception: configuration file.

### File: `dbt_oura/models/sources.yml`

Update column definitions to reflect the new raw schema. Table names stay the same.

```yaml
version: 2

sources:
  - name: oura_raw
    database: "{{ env_var('SNOWFLAKE_DATABASE', 'OURA') }}"
    schema: oura_raw
    tables:
      - name: sleep
        columns:
          - name: raw_data
          - name: partition_date
      - name: activity
        columns:
          - name: raw_data
          - name: partition_date
      - name: readiness
        columns:
          - name: raw_data
          - name: partition_date
      - name: heartrate
        columns:
          - name: raw_data
          - name: partition_date
      - name: spo2
        columns:
          - name: raw_data
          - name: partition_date
      - name: stress
        columns:
          - name: raw_data
          - name: partition_date
      - name: resilience
        columns:
          - name: raw_data
          - name: partition_date
      - name: sleep_periods
        columns:
          - name: raw_data
          - name: partition_date
      - name: sleep_time
        columns:
          - name: raw_data
          - name: partition_date
      - name: workouts
        columns:
          - name: raw_data
          - name: partition_date
      - name: sessions
        columns:
          - name: raw_data
          - name: partition_date
      - name: tags
        columns:
          - name: raw_data
          - name: partition_date
      - name: rest_mode_periods
        columns:
          - name: raw_data
          - name: partition_date
```

**Key change:** Added `database: "{{ env_var('SNOWFLAKE_DATABASE', 'OURA') }}"` so Snowflake resolves the correct database.

---

## 2.4 — RED: `not_null` Tests (Task 11) — Write Before Staging Models

This is the TDD step for the dbt layer. Write the `schema.yml` with `not_null` tests first. These tests define the contract that each staging model must satisfy. When run against the old DuckDB-style models (which reference columns that no longer exist in the VARIANT raw schema), they will fail — that's our RED.

### File: `dbt_oura/models/staging/schema.yml` (NEW)

```yaml
version: 2

models:
  - name: stg_sleep
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: sleep_score
        tests: [not_null]

  - name: stg_activity
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: steps
        tests: [not_null]

  - name: stg_readiness
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: readiness_score
        tests: [not_null]

  - name: stg_heartrate
    columns:
      - name: ts
        tests: [not_null]
      - name: bpm
        tests: [not_null]
      - name: partition_date
        tests: [not_null]

  - name: stg_spo2
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: avg_spo2_pct
        tests: [not_null]

  - name: stg_stress
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: stress_high
        tests: [not_null]

  - name: stg_resilience
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: resilience_level
        tests: [not_null]

  - name: stg_sleep_periods
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: sleep_type
        tests: [not_null]

  - name: stg_sleep_time
    columns:
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]

  - name: stg_workouts
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
      - name: workout_activity
        tests: [not_null]

  - name: stg_sessions
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]

  - name: stg_tags
    columns:
      - name: id
        tests: [not_null]
      - name: day
        tests: [not_null]
      - name: partition_date
        tests: [not_null]

  - name: stg_rest_mode_periods
    columns:
      - name: id
        tests: [not_null]
      - name: partition_date
        tests: [not_null]
```

### Design Rationale

- **Every model** gets `partition_date` not_null (always injected by `_upsert_day()`).
- **Every model with `id`** gets `id` not_null.
- **Every model with `day`** gets `day` not_null.
- **Primary metric columns** tested where the Oura API guarantees them (`sleep_score`, `readiness_score`, `steps`, `bpm`).
- `stg_sleep_time` only tests `day` and `partition_date` — `optimal_bedtime` fields can legitimately be NULL.
- `stg_rest_mode_periods` skips `start_day`/`end_day` — may not always be populated.

### Verify RED

```bash
cd dbt_oura

# Build the staging models (still using old DuckDB-style SQL against Snowflake VARIANT raw tables)
dbt run --select staging --profiles-dir .
# Expected: FAIL — old models reference columns that don't exist in VARIANT schema

# Even if models somehow build, tests will fail:
dbt test --select staging --profiles-dir .
# Expected: FAIL — columns return NULL because VARIANT paths are wrong
```

---

## 2.5 — GREEN: Rewrite Staging Models for VARIANT Syntax (Task 10)

Write the staging models that make the `not_null` tests pass. All 13 models change from direct column access to Snowflake VARIANT `:` notation.

### Syntax Translation Rules

| Pattern             | DuckDB (before)  | Snowflake (after)                           |
| ------------------- | ---------------- | ------------------------------------------- |
| Top-level field     | `column_name`    | `raw_data:column_name::TYPE`                |
| Nested STRUCT field | `parent.child`   | `raw_data:parent:child::TYPE`               |
| Date cast           | `day::date`      | `raw_data:day::date`                        |
| Partition date      | `partition_date` | `partition_date` (real column, not VARIANT) |

### Type Mapping

| DuckDB type      | Snowflake VARIANT cast |
| ---------------- | ---------------------- |
| VARCHAR / text   | `::varchar`            |
| BIGINT / integer | `::int`                |
| DOUBLE / float   | `::float`              |
| DATE             | `::date`               |
| TIMESTAMP        | `::timestamp`          |

---

### `stg_sleep.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sleep') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:score::int as sleep_score,
  raw_data:contributors:deep_sleep::int as deep_sleep_score,
  raw_data:contributors:efficiency::int as efficiency_score,
  raw_data:contributors:latency::int as latency_score,
  raw_data:contributors:rem_sleep::int as rem_sleep_score,
  raw_data:contributors:restfulness::int as restfulness_score,
  raw_data:contributors:timing::int as timing_score,
  raw_data:contributors:total_sleep::int as total_sleep_score,
  raw_data:timestamp::varchar as timestamp,
  partition_date
from src
```

### `stg_activity.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'activity') }}
)
select
  raw_data:day::date as day,
  raw_data:steps::int as steps,
  raw_data:total_calories::int as calories,
  partition_date
from src
```

### `stg_readiness.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'readiness') }}
)
select
  raw_data:day::date as day,
  raw_data:score::int as readiness_score,
  partition_date
from src
```

### `stg_heartrate.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'heartrate') }}
)
select
  raw_data:timestamp::varchar as ts,
  raw_data:bpm::int as bpm,
  raw_data:timestamp::date as day,
  partition_date
from src
```

**Note:** The original model used `date(ts)` for `day`. In Snowflake, `raw_data:timestamp::date` extracts the date portion from the ISO timestamp string directly.

### `stg_spo2.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'spo2') }}
)
select
  raw_data:day::date as day,
  raw_data:spo2_percentage:average::float as avg_spo2_pct,
  raw_data:breathing_disturbance_index::float as breathing_disturbance_index,
  partition_date
from src
```

### `stg_stress.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'stress') }}
)
select
  raw_data:day::date as day,
  raw_data:stress_high::int as stress_high,
  raw_data:recovery_high::int as recovery_high,
  raw_data:day_summary::varchar as stress_summary,
  partition_date
from src
```

### `stg_resilience.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'resilience') }}
)
select
  raw_data:day::date as day,
  raw_data:level::varchar as resilience_level,
  raw_data:contributors:sleep_recovery::float as sleep_recovery_score,
  raw_data:contributors:daytime_recovery::float as daytime_recovery_score,
  raw_data:contributors:stress::float as stress_score,
  partition_date
from src
```

### `stg_sleep_periods.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sleep_periods') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:bedtime_start::varchar as bedtime_start,
  raw_data:bedtime_end::varchar as bedtime_end,
  raw_data:type::varchar as sleep_type,
  raw_data:total_sleep_duration::int as total_sleep_duration,
  raw_data:deep_sleep_duration::int as deep_sleep_duration,
  raw_data:light_sleep_duration::int as light_sleep_duration,
  raw_data:rem_sleep_duration::int as rem_sleep_duration,
  raw_data:awake_time::int as awake_time,
  raw_data:time_in_bed::int as time_in_bed,
  raw_data:efficiency::int as efficiency,
  raw_data:latency::int as latency,
  raw_data:average_heart_rate::float as avg_hr,
  raw_data:average_hrv::float as avg_hrv,
  raw_data:lowest_heart_rate::int as lowest_hr,
  raw_data:average_breath::float as avg_breath,
  raw_data:restless_periods::int as restless_periods,
  partition_date
from src
```

### `stg_sleep_time.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sleep_time') }}
)
select
  raw_data:day::date as day,
  raw_data:optimal_bedtime:start_offset::int as optimal_bedtime_start_offset,
  raw_data:optimal_bedtime:end_offset::int as optimal_bedtime_end_offset,
  raw_data:optimal_bedtime:day_tz::int as optimal_bedtime_tz_offset,
  raw_data:recommendation::varchar as sleep_recommendation,
  raw_data:status::varchar as sleep_time_status,
  partition_date
from src
```

### `stg_workouts.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'workouts') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:activity::varchar as workout_activity,
  raw_data:start_datetime::varchar as start_datetime,
  raw_data:end_datetime::varchar as end_datetime,
  raw_data:intensity::varchar as intensity,
  raw_data:source::varchar as workout_source,
  raw_data:calories::float as workout_calories,
  raw_data:distance::float as workout_distance_m,
  raw_data:label::varchar as workout_label,
  partition_date
from src
```

### `stg_sessions.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sessions') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:start_datetime::varchar as start_datetime,
  raw_data:end_datetime::varchar as end_datetime,
  raw_data:type::varchar as session_type,
  raw_data:mood::varchar as mood,
  partition_date
from src
```

### `stg_tags.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'tags') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:timestamp::varchar as tagged_at,
  raw_data:text::varchar as note_text,
  raw_data:tags::varchar as tags,
  partition_date
from src
```

**Note:** `tags` in the Oura API is an array. `::varchar` gives the JSON string representation. Use `LATERAL FLATTEN(raw_data:tags)` if downstream unnesting is needed.

### `stg_rest_mode_periods.sql`

```sql
{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'rest_mode_periods') }}
)
select
  raw_data:id::varchar as id,
  raw_data:start_day::date as start_day,
  raw_data:start_time::varchar as start_time,
  raw_data:end_day::date as end_day,
  raw_data:end_time::varchar as end_time,
  partition_date
from src
```

### Verify GREEN

```bash
cd dbt_oura

# Build staging models with new VARIANT syntax
dbt run --select staging --profiles-dir .
# Expected: ALL PASS — models query raw_data VARIANT correctly

# Run not_null tests
dbt test --select staging --profiles-dir .
# Expected: ALL PASS — key columns are not null
```

---

## 2.6 — Mart Models: No Changes Required

The two mart models (`fact_daily_wellness.sql` and `fact_sleep_detail.sql`) use `{{ ref('stg_*') }}` exclusively — they never touch raw tables directly. Since the staging models produce the same output column names and types as before, the marts require **zero changes**.

### Column Name Preservation Verification

| Mart column                                      | Source staging model | Preserved? |
| ------------------------------------------------ | -------------------- | ---------- |
| `readiness_score`                                | `stg_readiness`      | Yes        |
| `steps`, `calories`                              | `stg_activity`       | Yes        |
| `sleep_score`, `efficiency_score`                | `stg_sleep`          | Yes        |
| `avg_spo2_pct`, `breathing_disturbance_index`    | `stg_spo2`           | Yes        |
| `stress_high`, `recovery_high`, `stress_summary` | `stg_stress`         | Yes        |
| `resilience_level`, `sleep_recovery_score`, etc. | `stg_resilience`     | Yes        |
| `sleep_type`, `avg_hr`, `avg_hrv`, etc.          | `stg_sleep_periods`  | Yes        |
| `sleep_recommendation`, `optimal_bedtime_*`      | `stg_sleep_time`     | Yes        |

---

## Implementation Order (TDD Sequence)

```
2.1  profiles.yml             config — no test needed (must be first for dbt parse)
2.2  dbt_project.yml          config — profile reference update
2.3  sources.yml              config — raw table schema updated
2.4  schema.yml (not_null)    RED — tests define the contract, will fail against old models
2.5  All 13 staging models    GREEN — VARIANT syntax makes the not_null tests pass
2.6  Verify marts still work  no changes, just confirm dbt build passes
```

**The key TDD insight:** Step 2.4 (schema.yml) is the RED step. Step 2.5 (staging model rewrites) is the GREEN step. The not_null tests act as the specification — if a VARIANT path is wrong, the column returns NULL and the test fails.

---

## Files Modified in Phase 2

| File                                                | Action                                  | TDD Step |
| --------------------------------------------------- | --------------------------------------- | -------- |
| `dbt_oura/profiles.yml`                             | Replace DuckDB profile with Snowflake   | Config   |
| `dbt_oura/dbt_project.yml`                          | Change profile reference                | Config   |
| `dbt_oura/models/sources.yml`                       | Add database, update column definitions | Config   |
| `dbt_oura/models/staging/schema.yml`                | **NEW** — not_null tests                | RED      |
| `dbt_oura/models/staging/stg_sleep.sql`             | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_activity.sql`          | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_readiness.sql`         | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_heartrate.sql`         | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_spo2.sql`              | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_stress.sql`            | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_resilience.sql`        | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_sleep_periods.sql`     | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_sleep_time.sql`        | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_workouts.sql`          | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_sessions.sql`          | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_tags.sql`              | VARIANT syntax                          | GREEN    |
| `dbt_oura/models/staging/stg_rest_mode_periods.sql` | VARIANT syntax                          | GREEN    |

## Full dbt Verification

```bash
cd dbt_oura

# Parse (validates all SQL + YAML is valid)
dbt parse --profiles-dir .

# Build everything (staging + marts)
dbt build --profiles-dir .

# Run all tests (not_null + any others)
dbt test --profiles-dir .
```
