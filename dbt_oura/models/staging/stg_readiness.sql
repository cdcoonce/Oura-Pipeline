{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'readiness') }}
)
select
  day::date as day,
  score as readiness_score,
  partition_date
from src