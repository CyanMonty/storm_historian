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

-- Location aggregates extracted to intermediate for independent testability
event_locations as (
  select * from {{ ref('int_storm_event__location_agg') }}
),

-- Fatality aggregates extracted to intermediate for independent testability
event_fatalities_agg as (
  select * from {{ ref('int_storm_event__fatality_agg') }}
),

joined as (
  select
    e.event_id,
    e.episode_id,
    
    -- Dimension foreign keys — generated inline (same logic as the dim tables)
    -- avoids two full table joins that add no new information.
    {{ dbt_utils.generate_surrogate_key(['e.state_fips', 'e.cz_type', 'e.cz_fips']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key(['e.event_type']) }} as event_type_key,
    
    -- Event type category (surfaced from dim_event_types logic inline)
    case
      when e.event_type in ('Tornado', 'Funnel Cloud', 'Waterspout')                                               then 'Tornado/Funnel'
      when e.event_type in ('Thunderstorm Wind', 'High Wind', 'Strong Wind')                                       then 'Wind'
      when e.event_type = 'Hail'                                                                                    then 'Hail'
      when e.event_type in ('Flash Flood', 'Flood', 'Coastal Flood', 'Lakeshore Flood')                            then 'Flood'
      when e.event_type in ('Winter Storm', 'Ice Storm', 'Winter Weather', 'Heavy Snow', 'Blizzard')               then 'Winter Weather'
      when e.event_type in ('Hurricane', 'Tropical Storm', 'Tropical Depression')                                  then 'Tropical'
      when e.event_type = 'Lightning'                                                                               then 'Lightning'
      when e.event_type = 'Wildfire'                                                                                then 'Fire'
      when e.event_type in ('Drought', 'Excessive Heat', 'Heat', 'Cold/Wind Chill', 'Extreme Cold/Wind Chill', 'Frost/Freeze') then 'Temperature Extreme'
      else 'Other'
    end as event_category,
    
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
    coalesce(el.location_point_count, 0) as location_point_count,
    el.avg_latitude,
    el.avg_longitude,
    el.has_suspect_longitude,
    coalesce(ef.fatality_record_count, 0) as fatality_record_count,
    ef.has_duplicate_fatality_ids,
    
    -- Narratives
    e.episode_narrative,
    e.event_narrative,
    e.data_source,
    
    -- Metadata
    e.etl_inserted_at
    
  from events e
  left join event_locations el
    on e.event_id = el.event_id
  left join event_fatalities_agg ef
    on e.event_id = ef.event_id
)

select * from joined
