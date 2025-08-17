{{ config(materialized='view') }}

select
  *
from oura_raw.sleep