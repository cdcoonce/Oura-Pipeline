{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'stress') }}
)
select
  day::date as day,
  stress_high,
  recovery_high,
  day_summary as stress_summary,
  partition_date
from src
