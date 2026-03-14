{{ config(materialized='table') }}

with
sp as (select * from {{ ref('stg_sleep_periods') }}),
st as (select * from {{ ref('stg_sleep_time') }})

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
  sp.time_in_bed,
  sp.efficiency,
  sp.latency,
  sp.avg_hr,
  sp.avg_hrv,
  sp.lowest_hr,
  sp.avg_breath,
  sp.restless_periods,
  st.sleep_recommendation,
  st.optimal_bedtime_start_offset,
  st.optimal_bedtime_end_offset
from sp
left join st on sp.day = st.day
