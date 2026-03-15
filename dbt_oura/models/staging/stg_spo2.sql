{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'spo2') }}
)
select
  raw_data:day::date as day,
  raw_data:spo2_percentage:average::float as avg_spo2_pct,
  raw_data:breathing_disturbance_index::float as breathing_disturbance_index,
  partition_date
from src
