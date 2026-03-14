# Build Remaining dbt Models

## Current State

**Existing staging models (4):** `stg_sleep`, `stg_activity`, `stg_readiness`, `stg_heartrate`
**Existing marts (1):** `fact_daily_wellness` (joins sleep + activity + readiness)
**Raw sources (13):** All 13 endpoints are ingested — 9 lack staging models.

## Missing Staging Models

### Task 1: stg_spo2 (daily)

Source: `oura_raw.spo2`

```sql
select
  day::date as day,
  spo2_percentage.average as avg_spo2_pct,
  breathing_disturbance_index,
  partition_date
from {{ source('oura_raw', 'spo2') }}
```

---

### Task 2: stg_stress (daily)

Source: `oura_raw.stress`

```sql
select
  day::date as day,
  stress_high,
  recovery_high,
  day_summary as stress_summary,
  partition_date
from {{ source('oura_raw', 'stress') }}
```

---

### Task 3: stg_resilience (daily)

Source: `oura_raw.resilience`

```sql
select
  day::date as day,
  level as resilience_level,
  contributors.sleep_recovery as sleep_recovery_score,
  contributors.daytime_recovery as daytime_recovery_score,
  contributors.stress as stress_score,
  partition_date
from {{ source('oura_raw', 'resilience') }}
```

---

### Task 4: stg_sleep_periods (event-level)

Source: `oura_raw.sleep_periods`

```sql
select
  id,
  day::date as day,
  bedtime_start,
  bedtime_end,
  type as sleep_type,
  total_sleep_duration,
  deep_sleep_duration,
  light_sleep_duration,
  rem_sleep_duration,
  awake_time,
  time_in_bed,
  efficiency,
  latency,
  average_heart_rate as avg_hr,
  average_hrv as avg_hrv,
  lowest_heart_rate as lowest_hr,
  average_breath as avg_breath,
  restless_periods,
  partition_date
from {{ source('oura_raw', 'sleep_periods') }}
```

---

### Task 5: stg_sleep_time (daily)

Source: `oura_raw.sleep_time`

```sql
select
  day::date as day,
  optimal_bedtime.start_offset as optimal_bedtime_start_offset,
  optimal_bedtime.end_offset as optimal_bedtime_end_offset,
  optimal_bedtime.day_tz as optimal_bedtime_tz_offset,
  recommendation as sleep_recommendation,
  status as sleep_time_status,
  partition_date
from {{ source('oura_raw', 'sleep_time') }}
```

---

### Task 6: stg_workouts (event-level)

Source: `oura_raw.workouts`

```sql
select
  id,
  day::date as day,
  activity as workout_activity,
  start_datetime,
  end_datetime,
  intensity,
  source as workout_source,
  calories as workout_calories,
  distance as workout_distance_m,
  label as workout_label,
  partition_date
from {{ source('oura_raw', 'workouts') }}
```

---

### Task 7: stg_sessions (event-level)

Source: `oura_raw.sessions`

```sql
select
  id,
  day::date as day,
  start_datetime,
  end_datetime,
  type as session_type,
  mood,
  partition_date
from {{ source('oura_raw', 'sessions') }}
```

Note: `heart_rate`, `heart_rate_variability`, and `motion_count` are nested SampleModel objects (interval + items array). Excluding from staging — they would need unpacking into child tables if needed later.

---

### Task 8: stg_tags (event-level)

Source: `oura_raw.tags`

```sql
select
  id,
  day::date as day,
  timestamp as tagged_at,
  text as note_text,
  tags,
  partition_date
from {{ source('oura_raw', 'tags') }}
```

---

### Task 9: stg_rest_mode_periods (event-level)

Source: `oura_raw.rest_mode_periods`

```sql
select
  id,
  start_day::date as start_day,
  start_time,
  end_day::date as end_day,
  end_time,
  partition_date
from {{ source('oura_raw', 'rest_mode_periods') }}
```

Note: `episodes` is a nested array of objects. Excluding from staging — would need unnesting into a child table if needed later.

---

## Updated Marts

### Task 10: Update fact_daily_wellness

Enrich the existing mart by joining the new daily staging models.

```sql
select
  coalesce(sleep.day, act.day, ready.day, spo2.day, stress.day, res.day) as day,
  ready.readiness_score,
  act.steps,
  act.calories,
  sleep.total_sleep_duration,
  sleep.efficiency as sleep_efficiency,
  spo2.avg_spo2_pct,
  spo2.breathing_disturbance_index,
  stress.stress_high,
  stress.recovery_high,
  stress.stress_summary,
  res.resilience_level,
  res.sleep_recovery_score,
  res.daytime_recovery_score,
  res.stress_score as resilience_stress_score
from ...
```

---

### Task 11: Add fact_sleep_detail mart

A detailed sleep mart joining sleep periods with sleep time recommendations.

```sql
select
  sp.id,
  sp.day,
  sp.sleep_type,
  sp.bedtime_start,
  sp.bedtime_end,
  sp.total_sleep_duration,
  sp.deep_sleep_duration,
  sp.light_sleep_duration,
  sp.rem_sleep_duration,
  sp.awake_time,
  sp.efficiency,
  sp.avg_hr,
  sp.avg_hrv,
  sp.lowest_hr,
  st.sleep_recommendation,
  st.optimal_bedtime_start_offset,
  st.optimal_bedtime_end_offset
from stg_sleep_periods sp
left join stg_sleep_time st on sp.day = st.day
```

---

## Execution Order

1. **Tasks 1–3** — Daily staging models (spo2, stress, resilience) — independent, can be done in parallel
2. **Tasks 4–9** — Event-level staging models — independent, can be done in parallel
3. **Task 10** — Update fact_daily_wellness (depends on Tasks 1–3)
4. **Task 11** — Add fact_sleep_detail (depends on Tasks 4–5)

## Testing Strategy

- After each staging model, run `dbt build` to verify it compiles and runs
- Verify column types match expectations with a `dbt test` or manual query
- Run full `uv run pytest` to ensure Dagster integration remains intact
