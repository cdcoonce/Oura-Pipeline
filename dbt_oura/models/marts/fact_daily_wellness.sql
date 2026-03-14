{{ config(materialized='table') }}

with
sleep as (select * from {{ ref('stg_sleep') }}),
act   as (select * from {{ ref('stg_activity') }}),
ready as (select * from {{ ref('stg_readiness') }})

select
  coalesce(sleep.day, act.day, ready.day) as day,
  ready.readiness_score,
  act.steps,
  act.calories,
  sleep.total_sleep_duration
from ready
full outer join sleep on sleep.day = ready.day
full outer join act   on act.day   = ready.day