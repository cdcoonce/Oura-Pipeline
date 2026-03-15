{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sleep') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:score::int as sleep_score,
  raw_data:contributors:deep_sleep::int as deep_sleep_score,
  raw_data:contributors:efficiency::int as efficiency_score,
  raw_data:contributors:latency::int as latency_score,
  raw_data:contributors:rem_sleep::int as rem_sleep_score,
  raw_data:contributors:restfulness::int as restfulness_score,
  raw_data:contributors:timing::int as timing_score,
  raw_data:contributors:total_sleep::int as total_sleep_score,
  raw_data:timestamp::varchar as timestamp,
  partition_date
from src
