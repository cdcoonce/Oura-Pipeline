{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'readiness') }}
)
select
  raw_data:day::date as day,
  raw_data:score::int as readiness_score,
  partition_date
from src
