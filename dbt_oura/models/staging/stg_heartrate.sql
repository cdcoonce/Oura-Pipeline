{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'heartrate') }}
)
select
  raw_data:timestamp::timestamp_ntz as ts,
  raw_data:bpm::int as bpm,
  raw_data:timestamp::date as day,
  partition_date
from src
qualify row_number() over (partition by raw_data:timestamp::timestamp_ntz order by partition_date desc) = 1
