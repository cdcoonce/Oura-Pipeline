{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'sleep_periods') }}
)
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
from src
