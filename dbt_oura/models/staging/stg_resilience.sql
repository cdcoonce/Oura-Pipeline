{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'resilience') }}
)
select
  day::date as day,
  level as resilience_level,
  contributors.sleep_recovery as sleep_recovery_score,
  contributors.daytime_recovery as daytime_recovery_score,
  contributors.stress as stress_score,
  partition_date
from src
