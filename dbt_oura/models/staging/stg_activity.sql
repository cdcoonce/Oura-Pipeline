{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'activity') }}
)
select
  raw_data:day::date as day,
  raw_data:steps::int as steps,
  raw_data:total_calories::int as calories,
  partition_date
from src
