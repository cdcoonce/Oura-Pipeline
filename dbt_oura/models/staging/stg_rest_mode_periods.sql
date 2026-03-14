{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'rest_mode_periods') }}
)
select
  id,
  start_day::date as start_day,
  start_time,
  end_day::date as end_day,
  end_time,
  partition_date
from src
