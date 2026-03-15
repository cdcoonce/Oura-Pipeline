{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'resilience') }}
)
select
  raw_data:day::date as day,
  raw_data:level::varchar as resilience_level,
  raw_data:contributors:sleep_recovery::float as sleep_recovery_score,
  raw_data:contributors:daytime_recovery::float as daytime_recovery_score,
  raw_data:contributors:stress::float as stress_score,
  partition_date
from src
