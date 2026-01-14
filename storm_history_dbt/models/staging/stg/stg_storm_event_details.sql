{{ config(materialized='view') }}

with src as (
  select * from {{ ref('base_storm_event__details') }}
),

clean as (
  select
    -- ids / keys
    try_cast(EPISODE_ID as bigint) as episode_id,
    try_cast(EVENT_ID as bigint)   as event_id,

    -- yyyymm + day + HHMM (preserve as normalized string)
    try_cast(BEGIN_YEARMONTH as integer) as begin_yearmonth,
    try_cast(BEGIN_DAY as integer)       as begin_day,
    lpad(nullif(trim(cast(BEGIN_TIME as varchar)), ''), 4, '0') as begin_time_hhmm,

    try_cast(END_YEARMONTH as integer)   as end_yearmonth,
    try_cast(END_DAY as integer)         as end_day,
    lpad(nullif(trim(cast(END_TIME as varchar)), ''), 4, '0')   as end_time_hhmm,

    -- state / location
    nullif(trim(cast(STATE as varchar)), '')         as state,
    try_cast(STATE_FIPS as integer)                  as state_fips,
    try_cast(YEAR as integer)                        as year,
    nullif(trim(cast(MONTH_NAME as varchar)), '')    as month_name,
    nullif(trim(cast(EVENT_TYPE as varchar)), '')    as event_type,

    nullif(trim(cast(CZ_TYPE as varchar)), '')       as cz_type,
    try_cast(CZ_FIPS as integer)                     as cz_fips,
    nullif(trim(cast(CZ_NAME as varchar)), '')       as cz_name,
    nullif(trim(cast(WFO as varchar)), '')           as wfo,

    -- NOAA date strings like "28-APR-50 14:45:00"
    case
      when nullif(trim(cast(BEGIN_DATE_TIME as varchar)), '') is null then null
      else try_strptime(trim(cast(BEGIN_DATE_TIME as varchar)), '%d-%b-%y %H:%M:%S')
    end as begin_date_time,

    nullif(trim(cast(CZ_TIMEZONE as varchar)), '') as cz_timezone,

    case
      when nullif(trim(cast(END_DATE_TIME as varchar)), '') is null then null
      else try_strptime(trim(cast(END_DATE_TIME as varchar)), '%d-%b-%y %H:%M:%S')
    end as end_date_time,

    -- impacts
    try_cast(INJURIES_DIRECT as integer)   as injuries_direct,
    try_cast(INJURIES_INDIRECT as integer) as injuries_indirect,
    try_cast(DEATHS_DIRECT as integer)     as deaths_direct,
    try_cast(DEATHS_INDIRECT as integer)   as deaths_indirect,

    -- money (keep raw; parse to numeric in an intermediate model)
    nullif(trim(cast(DAMAGE_PROPERTY as varchar)), '') as damage_property_raw,
    nullif(trim(cast(DAMAGE_CROPS as varchar)), '')    as damage_crops_raw,

    -- misc
    nullif(trim(cast(SOURCE as varchar)), '')         as source,
    try_cast(MAGNITUDE as double)                     as magnitude,
    nullif(trim(cast(MAGNITUDE_TYPE as varchar)), '') as magnitude_type,
    nullif(trim(cast(FLOOD_CAUSE as varchar)), '')    as flood_cause,
    nullif(trim(cast(CATEGORY as varchar)), '')       as category,

    -- tornado fields
    nullif(trim(cast(TOR_F_SCALE as varchar)), '')          as tor_f_scale,
    try_cast(TOR_LENGTH as double)                          as tor_length,
    try_cast(TOR_WIDTH as double)                           as tor_width,
    nullif(trim(cast(TOR_OTHER_WFO as varchar)), '')        as tor_other_wfo,
    nullif(trim(cast(TOR_OTHER_CZ_STATE as varchar)), '')   as tor_other_cz_state,
    try_cast(TOR_OTHER_CZ_FIPS as integer)                  as tor_other_cz_fips,
    nullif(trim(cast(TOR_OTHER_CZ_NAME as varchar)), '')    as tor_other_cz_name,

    -- ranges / azimuth / named locations
    try_cast(BEGIN_RANGE as double)                         as begin_range,
    nullif(trim(cast(BEGIN_AZIMUTH as varchar)), '')        as begin_azimuth,
    nullif(trim(cast(BEGIN_LOCATION as varchar)), '')       as begin_location,
    try_cast(END_RANGE as double)                           as end_range,
    nullif(trim(cast(END_AZIMUTH as varchar)), '')          as end_azimuth,
    nullif(trim(cast(END_LOCATION as varchar)), '')         as end_location,

    -- coordinates
    try_cast(BEGIN_LAT as double) as begin_lat,
    try_cast(BEGIN_LON as double) as begin_lon,
    try_cast(END_LAT as double)   as end_lat,
    try_cast(END_LON as double)   as end_lon,

    -- narratives
    nullif(trim(cast(EPISODE_NARRATIVE as varchar)), '') as episode_narrative,
    nullif(trim(cast(EVENT_NARRATIVE as varchar)), '')   as event_narrative,
    nullif(trim(cast(DATA_SOURCE as varchar)), '')       as data_source,

    -- metadata
    etl_inserted_at

  from src
)

select * from clean