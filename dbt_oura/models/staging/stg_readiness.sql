{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'readiness') }}
)
select
  raw_data:day::date as day,
  raw_data:score::int as readiness_score,
  partition_date
from src
qualify row_number() over (partition by raw_data:day::date order by partition_date desc) = 1
