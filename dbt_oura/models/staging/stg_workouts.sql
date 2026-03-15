{{ config(materialized='table') }}

with src as (
  select * from {{ source('oura_raw', 'workouts') }}
)
select
  raw_data:id::varchar as id,
  raw_data:day::date as day,
  raw_data:activity::varchar as workout_activity,
  raw_data:start_datetime::varchar as start_datetime,
  raw_data:end_datetime::varchar as end_datetime,
  raw_data:intensity::varchar as intensity,
  raw_data:source::varchar as workout_source,
  raw_data:calories::float as workout_calories,
  raw_data:distance::float as workout_distance_m,
  raw_data:label::varchar as workout_label,
  partition_date
from src
