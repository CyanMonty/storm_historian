{{ config(
    materialized='table',
    tags=['dimension']
) }}

/*
    Dimension table for ZIP codes (ZCTAs).
    Grain: One row per ZIP code.

    Each ZIP is mapped to its dominant county (highest land area_ratio) from the
    Census 2020 ZCTA-to-county crosswalk. The location_key FK to dim_locations
    is valid only for county-level NOAA entries (cz_type = 'C'); null means no
    NOAA events exist for the dominant county of that ZIP.

    The full seed (zip_county_crosswalk) is referenced directly by
    agg_events_by_zip which needs ALL ZIP×county pairs to expand county events.
*/

with ranked as (
    select
        *,
        row_number() over (
            partition by zip_code
            order by area_ratio desc, county_fips_full asc
        ) as county_rank,
        count(*) over (partition by zip_code) as county_count
    from {{ ref('zip_county_crosswalk') }}
),

dominant as (
    select
        zip_code,
        county_fips_full,
        state_fips,
        county_fips,
        county_name,
        area_ratio,
        county_count,
        county_count > 1 as is_multi_county
    from ranked
    where county_rank = 1
),

-- Only county-type locations are joinable to ZIP codes via FIPS
locations as (
    select * from {{ ref('dim_locations') }}
    where cz_type = 'C'
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['d.zip_code']) }} as zip_key,
        d.zip_code,
        d.county_fips_full,
        d.state_fips,
        d.county_fips,
        d.county_name,
        d.area_ratio        as dominant_county_area_ratio,
        d.county_count      as counties_in_zip,
        d.is_multi_county,

        -- FK to dim_locations; null when county has no NOAA events on record
        loc.location_key,
        loc.state           as state_abbr,
        loc.cz_name         as county_name_noaa

    from dominant d
    left join locations loc
        on  lpad(cast(loc.state_fips as varchar), 2, '0') = d.state_fips
        and lpad(cast(loc.cz_fips as varchar), 3, '0')    = d.county_fips
)

select * from final
