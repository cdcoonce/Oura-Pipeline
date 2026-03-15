{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'heartrate') }}
)
select
  raw_data:timestamp::varchar as ts,
  raw_data:bpm::int as bpm,
  raw_data:timestamp::date as day,
  partition_date
from src
