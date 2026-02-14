{{ config(
    materialized='table',
    tags=['dimension']
) }}

/*
    Dimension table for geographic locations (states and counties)
    Grain: One row per unique state/county combination
*/

with locations as (
  select distinct
    state,
    state_fips,
    cz_type,
    cz_fips,
    cz_name,
    cz_timezone
  from {{ ref('stg_storm_event_details') }}
  where state is not null
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['state_fips', 'cz_fips']) }} as location_key,
    state,
    state_fips,
    cz_type,  -- C = County, Z = Forecast Zone, M = Marine
    cz_fips,
    cz_name,
    cz_timezone
  from locations
)

select * from final
