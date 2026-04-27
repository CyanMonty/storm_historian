{{ config(
    materialized='table',
    tags=['aggregate', 'mart']
) }}

/*
    Aggregated metrics by state and year for performance and easy reporting
    Grain: One row per state per year
*/

with events as (
  select * from {{ ref('fct_storm_events') }}
),

aggregated as (
  select
    state,
    state_fips,
    year,
    
    -- Event counts
    count(*) as total_events,
    count(distinct event_type) as unique_event_types,
    count(distinct episode_id) as total_episodes,
    
    -- Event type breakdowns
    sum(case when event_type = 'Tornado' then 1 else 0 end) as tornado_count,
    sum(case when event_type like '%Flood%' then 1 else 0 end) as flood_count,
    sum(case when event_type like '%Wind%' then 1 else 0 end) as wind_count,
    sum(case when event_type = 'Hail' then 1 else 0 end) as hail_count,
    sum(case when event_type like '%Winter%' or event_type like '%Snow%' or event_type like '%Ice%' then 1 else 0 end) as winter_weather_count,
    
    -- Impact metrics
    sum(total_injuries) as total_injuries,
    sum(total_deaths) as total_deaths,
    sum(injuries_direct) as total_injuries_direct,
    sum(injuries_indirect) as total_injuries_indirect,
    sum(deaths_direct) as total_deaths_direct,
    sum(deaths_indirect) as total_deaths_indirect,
    
    -- Damage metrics (in millions for easier reading)
    round(sum(damage_property_amount) / 1000000.0, 2) as total_property_damage_millions,
    round(sum(damage_crops_amount) / 1000000.0, 2) as total_crop_damage_millions,
    round(sum(total_damage_amount) / 1000000.0, 2) as total_damage_millions,
    
    -- Statistical measures
    round(avg(total_damage_amount), 2) as avg_damage_per_event,
    max(total_damage_amount) as max_damage_single_event,
    
    -- Tornado-specific metrics
    -- Numeric severity score maps both F-scale (pre-2007) and EF-scale (2007+)
    -- to the same 0–10 ordinal so MAX() is semantically correct across eras.
    -- Alphabetic MAX on the raw string is wrong: 'F5' > 'EF5' lexicographically.
    max(
        case tor_f_scale
            when 'EF5' then 10  when 'F5' then 10
            when 'EF4' then 8   when 'F4' then 8
            when 'EF3' then 6   when 'F3' then 6
            when 'EF2' then 4   when 'F2' then 4
            when 'EF1' then 2   when 'F1' then 2
            when 'EF0' then 0   when 'F0' then 0
        end
    )                                                                       as max_tornado_severity,
    max(tor_f_scale)                                                        as strongest_tornado_scale_display,
    sum(case when tor_f_scale in ('EF4', 'EF5', 'F4', 'F5') then 1 else 0 end) as violent_tornado_count,
    
    -- Location metrics
    sum(location_point_count) as total_location_points,
    sum(fatality_record_count) as total_fatality_records,
    
    -- Metadata
    max(etl_inserted_at) as last_updated
    
  from events
  group by state, state_fips, year
)

-- ORDER BY removed: row order is not guaranteed on a materialized table
-- and adds unnecessary sort cost on every build. Apply ordering at query time.
select * from aggregated
