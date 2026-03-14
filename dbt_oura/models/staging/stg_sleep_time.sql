{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'sleep_time') }}
)
select
  day::date as day,
  optimal_bedtime.start_offset as optimal_bedtime_start_offset,
  optimal_bedtime.end_offset as optimal_bedtime_end_offset,
  optimal_bedtime.day_tz as optimal_bedtime_tz_offset,
  recommendation as sleep_recommendation,
  status as sleep_time_status,
  partition_date
from src
