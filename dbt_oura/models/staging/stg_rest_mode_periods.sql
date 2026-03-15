{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'rest_mode_periods') }}
)
select
  raw_data:id::varchar as id,
  raw_data:start_day::date as start_day,
  raw_data:start_time::varchar as start_time,
  raw_data:end_day::date as end_day,
  raw_data:end_time::varchar as end_time,
  partition_date
from src
