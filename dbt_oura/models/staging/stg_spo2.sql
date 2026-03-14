{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'spo2') }}
)
select
  day::date as day,
  spo2_percentage.average as avg_spo2_pct,
  breathing_disturbance_index,
  partition_date
from src
