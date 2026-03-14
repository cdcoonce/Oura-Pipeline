{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'sleep') }}
)
select
  id,
  day::date as day,
  score as sleep_score,
  contributors.deep_sleep as deep_sleep_score,
  contributors.efficiency as efficiency_score,
  contributors.latency as latency_score,
  contributors.rem_sleep as rem_sleep_score,
  contributors.restfulness as restfulness_score,
  contributors.timing as timing_score,
  contributors.total_sleep as total_sleep_score,
  timestamp,
  partition_date
from src