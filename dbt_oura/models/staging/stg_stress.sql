{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'stress') }}
)
select
  raw_data:day::date as day,
  raw_data:stress_high::int as stress_high,
  raw_data:recovery_high::int as recovery_high,
  raw_data:day_summary::varchar as stress_summary,
  partition_date
from src
