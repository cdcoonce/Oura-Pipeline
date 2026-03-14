{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'heartrate') }}
)
select
  timestamp as ts,
  bpm,
  date(ts) as day,
  partition_date
from src