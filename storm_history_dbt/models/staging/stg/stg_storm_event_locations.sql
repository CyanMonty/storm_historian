{{ config(materialized='view') }}

with src as (
  select * from {{ ref('base_storm_event__locations') }}
)

select
  -- keys
  try_cast(EPISODE_ID as bigint) as episode_id,
  try_cast(EVENT_ID as bigint)   as event_id,
  try_cast(LOCATION_INDEX as integer) as location_index,

  -- location details
  try_cast(RANGE as double)                        as range,
  nullif(trim(cast(AZIMUTH as varchar)), '')       as azimuth,
  nullif(trim(cast(LOCATION as varchar)), '')      as location,
  
  -- coordinates
  try_cast(LATITUDE as double)  as latitude,
  try_cast(LONGITUDE as double) as longitude,

  -- metadata
  etl_inserted_at
from src
