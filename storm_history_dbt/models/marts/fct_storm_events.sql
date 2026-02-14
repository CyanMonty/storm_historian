{{ config(
    materialized='table',
    tags=['fact']
) }}

/*
    Core fact table for storm events with all metrics and foreign keys to dimensions
    Grain: One row per storm event_id
*/

with events as (
  select * from {{ ref('int_storm_event_details__damage_parsed') }}
),

locations as (
  select * from {{ ref('dim_locations') }}
),

event_types as (
  select * from {{ ref('dim_event_types') }}
),

-- Aggregate location data
event_locations as (
  select 
    event_id,
    count(*) as location_count,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude
  from {{ ref('stg_storm_event_locations') }}
  where event_id is not null
  group by event_id
),

-- Aggregate fatality data
event_fatalities_agg as (
  select
    event_id,
    count(*) as fatality_count
  from {{ ref('stg_storm_event_fatalities') }}
  where event_id is not null
  group by event_id
),

joined as (
  select
    e.event_id,
    e.episode_id,
    
    -- Dimension foreign keys
    l.location_key,
    et.event_type_key,
    
    -- Date/Time fields
    e.begin_date_time,
    e.end_date_time,
    e.year,
    e.month_name,
    e.begin_yearmonth,
    e.begin_day,
    e.begin_time_hhmm,
    e.end_yearmonth,
    e.end_day,
    e.end_time_hhmm,
    
    -- Location details
    e.state,
    e.state_fips,
    e.cz_type,
    e.cz_fips,
    e.cz_name,
    e.wfo,
    e.cz_timezone,
    
    -- Event characteristics
    e.event_type,
    e.source,
    e.magnitude,
    e.magnitude_type,
    e.category,
    
    -- Impact metrics
    e.injuries_direct,
    e.injuries_indirect,
    e.deaths_direct,
    e.deaths_indirect,
    coalesce(e.injuries_direct, 0) + coalesce(e.injuries_indirect, 0) as total_injuries,
    coalesce(e.deaths_direct, 0) + coalesce(e.deaths_indirect, 0) as total_deaths,
    
    -- Damage metrics (parsed from intermediate model)
    e.damage_property_amount,
    e.damage_crops_amount,
    e.total_damage_amount,
    
    -- Tornado-specific fields
    e.tor_f_scale,
    e.tor_length,
    e.tor_width,
    e.tor_other_wfo,
    e.tor_other_cz_state,
    e.tor_other_cz_fips,
    e.tor_other_cz_name,
    
    -- Flood-specific
    e.flood_cause,
    
    -- Begin location
    e.begin_range,
    e.begin_azimuth,
    e.begin_location,
    e.begin_lat,
    e.begin_lon,
    
    -- End location
    e.end_range,
    e.end_azimuth,
    e.end_location,
    e.end_lat,
    e.end_lon,
    
    -- Aggregated metrics from related tables
    coalesce(el.location_count, 0) as location_point_count,
    el.avg_latitude,
    el.avg_longitude,
    coalesce(ef.fatality_count, 0) as fatality_record_count,
    
    -- Narratives
    e.episode_narrative,
    e.event_narrative,
    e.data_source,
    
    -- Metadata
    e.etl_inserted_at
    
  from events e
  left join locations l
    on e.state_fips = l.state_fips
    and e.cz_fips = l.cz_fips
  left join event_types et
    on e.event_type = et.event_type
  left join event_locations el
    on e.event_id = el.event_id
  left join event_fatalities_agg ef
    on e.event_id = ef.event_id
)

select * from joined
