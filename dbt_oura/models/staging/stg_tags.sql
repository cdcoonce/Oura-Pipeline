{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'tags') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:timestamp::varchar as tagged_at,
  raw_data:text::varchar as note_text,
  raw_data:tags::varchar as tags,
  partition_date
from src
