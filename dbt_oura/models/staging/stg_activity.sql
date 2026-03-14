{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'activity') }}
)
select
  day::date as day,
  steps,
  calories,
  partition_date
from src