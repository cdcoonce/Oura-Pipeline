{{ config(materialized='table') }}

with
sleep_periods as (select * from {{ ref('stg_sleep_periods') }}),
sleep_time as (select * from {{ ref('stg_sleep_time') }})

select
  sleep_periods.id,
  sleep_periods.day,
  sleep_periods.sleep_type,
  sleep_periods.bedtime_start,
  sleep_periods.bedtime_end,
  sleep_periods.total_sleep_duration,
  sleep_periods.deep_sleep_duration,
  sleep_periods.light_sleep_duration,
  sleep_periods.rem_sleep_duration,
  sleep_periods.awake_time,
  sleep_periods.time_in_bed,
  sleep_periods.efficiency,
  sleep_periods.latency,
  sleep_periods.avg_hr,
  sleep_periods.avg_hrv,
  sleep_periods.lowest_hr,
  sleep_periods.avg_breath,
  sleep_periods.restless_periods,
  sleep_time.sleep_recommendation,
  sleep_time.optimal_bedtime_start_offset,
  sleep_time.optimal_bedtime_end_offset
from sleep_periods
left join sleep_time on sleep_periods.day = sleep_time.day
