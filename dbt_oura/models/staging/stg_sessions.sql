{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'sessions') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:start_datetime::timestamp_ntz as start_datetime,
  raw_data:end_datetime::timestamp_ntz as end_datetime,
  raw_data:type::varchar as session_type,
  raw_data:mood::varchar as mood,
  partition_date
from src
qualify row_number() over (partition by raw_data:id::varchar order by partition_date desc) = 1
