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
