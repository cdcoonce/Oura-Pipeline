{{ config(materialized='table') }}

with
sleep as (select * from {{ ref('stg_sleep') }}),
activity as (select * from {{ ref('stg_activity') }}),
readiness as (select * from {{ ref('stg_readiness') }}),
spo2 as (select * from {{ ref('stg_spo2') }}),
stress as (select * from {{ ref('stg_stress') }}),
resilience as (select * from {{ ref('stg_resilience') }}),

date_spine as (
  select day from sleep
  union
  select day from activity
  union
  select day from readiness
)

select
  date_spine.day,
  readiness.readiness_score,
  activity.steps,
  activity.calories,
  sleep.sleep_score,
  sleep.efficiency_score as sleep_efficiency,
  spo2.avg_spo2_pct,
  spo2.breathing_disturbance_index,
  stress.stress_high,
  stress.recovery_high,
  stress.stress_summary,
  resilience.resilience_level,
  resilience.sleep_recovery_score,
  resilience.daytime_recovery_score,
  resilience.stress_score as resilience_stress_score
from date_spine
left join readiness on readiness.day = date_spine.day
left join sleep on sleep.day = date_spine.day
left join activity on activity.day = date_spine.day
left join spo2 on spo2.day = date_spine.day
left join stress on stress.day = date_spine.day
left join resilience on resilience.day = date_spine.day
qualify row_number() over (partition by date_spine.day order by date_spine.day) = 1
