{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('oura_raw', 'workouts') }}
)
select
  id,
  day::date as day,
  activity as workout_activity,
  start_datetime,
  end_datetime,
  intensity,
  source as workout_source,
  calories as workout_calories,
  distance as workout_distance_m,
  label as workout_label,
  partition_date
from src
