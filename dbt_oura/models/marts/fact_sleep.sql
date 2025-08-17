{{ config(materialized='table') }}

select
  day::date as sleep_date,
  coalesce(score, 0) as sleep_score,
  *
from {{ ref('stg_oura_sleep') }}