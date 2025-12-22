{{ config(materialized='view') }}

with src as (
  select * from {{ ref('stg_base_storm_event_fatalities') }}
)

select
  -- keys
  try_cast(FATALITY_ID as bigint) as fatality_id,
  try_cast(EVENT_ID as bigint)    as event_id,

  -- yyyymm + day + HHMM
  try_cast(FAT_YEARMONTH as integer) as fat_yearmonth,
  try_cast(FAT_DAY as integer)       as fat_day,
  lpad(nullif(trim(cast(FAT_TIME as varchar)), ''), 4, '0') as fat_time_hhmm,

  -- typed attrs
  nullif(trim(cast(FATALITY_TYPE as varchar)), '') as fatality_type,
  try_cast(FATALITY_AGE as integer)                as fatality_age,
  nullif(trim(cast(FATALITY_SEX as varchar)), '')  as fatality_sex,
  nullif(trim(cast(FATALITY_LOCATION as varchar)), '') as fatality_location,

  -- parse "02/20/1951 15:00:00"
  case
    when nullif(trim(cast(FATALITY_DATE as varchar)), '') is null then null
    else try_strptime(trim(cast(FATALITY_DATE as varchar)), '%m/%d/%Y %H:%M:%S')
  end as fatality_date_time,

  try_cast(EVENT_YEARMONTH as integer) as event_yearmonth,

  -- metadata
  etl_inserted_at
from src
