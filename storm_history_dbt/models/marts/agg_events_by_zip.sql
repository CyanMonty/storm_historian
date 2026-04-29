{{ config(
    materialized='table',
    tags=['aggregate', 'mart']
) }}

/*
    Aggregated storm event metrics by ZIP code (ZCTA).
    Grain: One row per zip_code.

    Methodology:
    - County-level NOAA events (cz_type = 'C') are expanded to all ZIPs that
      overlap that county via the zip_county_crosswalk seed (many-to-many).
    - Zone ('Z') and Marine ('M') events cannot be mapped to ZIP codes and are excluded.
    - A single event may appear in multiple ZIP rows when its county spans several ZIPs.
      This is intentional: a storm in Harris County TX affected all ZIP codes there.
    - area_ratio is included so consumers can optionally filter to dominant-county
      ZIPs only (area_ratio >= 0.5) to avoid over-counting in cross-county ZIPs.
*/

with events as (
    select * from {{ ref('fct_storm_events') }}
    where cz_type = 'C'
      and state_fips is not null
      and cz_fips is not null
),

crosswalk as (
    select * from {{ ref('zip_county_crosswalk') }}
),

-- Expand each county-level event to every ZIP that overlaps that county
expanded as (
    select
        xwalk.zip_code,
        xwalk.county_fips_full,
        xwalk.county_name,
        xwalk.area_ratio,
        e.event_id,
        e.year,
        e.event_type,
        e.event_category,
        e.begin_date_time,
        e.total_injuries,
        e.total_deaths,
        e.damage_property_amount,
        e.damage_crops_amount,
        e.total_damage_amount,
        e.tor_f_scale,
        e.location_point_count,
        e.etl_inserted_at
    from events e
    inner join crosswalk xwalk
        on  lpad(cast(e.state_fips as varchar), 2, '0') = xwalk.state_fips
        and lpad(cast(e.cz_fips as varchar), 3, '0')    = xwalk.county_fips
),

aggregated as (
    select
        zip_code,
        county_fips_full,
        county_name,

        -- Event counts
        count(*)                            as total_events,
        count(distinct year)                as years_with_events,
        min(year)                           as first_event_year,
        max(year)                           as last_event_year,

        -- Event type breakdowns
        count(distinct event_type)          as unique_event_types,
        sum(case when event_category = 'Tornado/Funnel'       then 1 else 0 end) as tornado_count,
        sum(case when event_category = 'Flood'                then 1 else 0 end) as flood_count,
        sum(case when event_category = 'Wind'                 then 1 else 0 end) as wind_count,
        sum(case when event_category = 'Hail'                 then 1 else 0 end) as hail_count,
        sum(case when event_category = 'Winter Weather'       then 1 else 0 end) as winter_weather_count,
        sum(case when event_category = 'Tropical'             then 1 else 0 end) as tropical_count,
        sum(case when event_category = 'Lightning'            then 1 else 0 end) as lightning_count,
        sum(case when event_category = 'Temperature Extreme'  then 1 else 0 end) as temp_extreme_count,
        sum(case when event_category = 'Fire'                 then 1 else 0 end) as fire_count,

        -- Impact metrics
        sum(total_injuries)                 as total_injuries,
        sum(total_deaths)                   as total_deaths,

        -- Damage metrics
        round(sum(coalesce(damage_property_amount, 0)) / 1000000.0, 2) as total_property_damage_millions,
        round(sum(coalesce(damage_crops_amount, 0))    / 1000000.0, 2) as total_crop_damage_millions,
        round(sum(coalesce(total_damage_amount, 0))    / 1000000.0, 2) as total_damage_millions,
        round(avg(coalesce(total_damage_amount, 0)),   2)               as avg_damage_per_event,
        max(coalesce(total_damage_amount, 0))                           as max_damage_single_event,

        -- Tornado severity
        max(
            case tor_f_scale
                when 'EF5' then 10  when 'F5' then 10
                when 'EF4' then 8   when 'F4' then 8
                when 'EF3' then 6   when 'F3' then 6
                when 'EF2' then 4   when 'F2' then 4
                when 'EF1' then 2   when 'F1' then 2
                when 'EF0' then 0   when 'F0' then 0
            end
        )                                                               as max_tornado_severity,

        -- Metadata
        max(etl_inserted_at)                as last_updated

    from expanded
    group by zip_code, county_fips_full, county_name
)

select * from aggregated
