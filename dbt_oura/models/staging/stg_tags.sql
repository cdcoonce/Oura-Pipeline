{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'tags') }}
)
select
  id,
  day::date as day,
  timestamp as tagged_at,
  text as note_text,
  tags,
  partition_date
from src
