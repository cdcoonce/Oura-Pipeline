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
