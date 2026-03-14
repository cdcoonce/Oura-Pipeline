{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'sleep') }}
)
select
  -- adapt columns to your actual raw schema; examples:
  day::date as day,
  total_sleep_duration,
  efficiency,
  partition_date
from src