{{ config(
    materialized='table',
    tags=['fact']
) }}

/*
    Fact table for storm event fatalities
    Grain: One row per fatality record (fatality_id)
*/

with fatalities as (
  select * from {{ ref('stg_storm_event_fatalities') }}
),

events as (
  select 
    event_id,
    episode_id,
    event_type,
    state,
    state_fips,
    cz_name,
    begin_date_time as event_begin_date_time
  from {{ ref('stg_storm_event_details') }}
),

event_types as (
  select * from {{ ref('dim_event_types') }}
),

joined as (
  select
    f.fatality_id,
    f.event_id,
    
    -- Dimension foreign keys
    et.event_type_key,
    
    -- Event context
    e.episode_id,
    e.event_type,
    e.state,
    e.state_fips,
    e.cz_name,
    e.event_begin_date_time,
    
    -- Fatality timing
    f.fatality_date_time,
    f.fat_yearmonth,
    f.fat_day,
    f.fat_time_hhmm,
    f.event_yearmonth,
    
    -- Fatality details
    f.fatality_type,  -- Direct or Indirect
    f.fatality_age,
    f.fatality_sex,
    f.fatality_location,
    
    -- Metadata
    f.etl_inserted_at
    
  from fatalities f
  left join events e
    on f.event_id = e.event_id
  left join event_types et
    on e.event_type = et.event_type
)

select * from joined
