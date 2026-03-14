{{ config(materialized='table') }}

with
sleep as (select * from {{ ref('stg_sleep') }}),
act   as (select * from {{ ref('stg_activity') }}),
ready as (select * from {{ ref('stg_readiness') }}),
spo2  as (select * from {{ ref('stg_spo2') }}),
stress as (select * from {{ ref('stg_stress') }}),
res   as (select * from {{ ref('stg_resilience') }})

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
from ready
full outer join sleep  on sleep.day  = ready.day
full outer join act    on act.day    = ready.day
full outer join spo2   on spo2.day   = coalesce(sleep.day, act.day, ready.day)
full outer join stress on stress.day = coalesce(sleep.day, act.day, ready.day)
full outer join res    on res.day    = coalesce(sleep.day, act.day, ready.day)
