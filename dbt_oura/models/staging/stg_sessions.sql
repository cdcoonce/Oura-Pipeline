{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'sessions') }}
)
select
  id,
  day::date as day,
  start_datetime,
  end_datetime,
  type as session_type,
  mood,
  partition_date
from src
