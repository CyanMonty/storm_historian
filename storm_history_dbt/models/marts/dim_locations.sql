{{ config(
    materialized='table',
    tags=['dimension']
) }}

/*
    Dimension table for geographic locations (states and counties)
    Grain: One row per unique state/county combination
*/

with locations as (
  select
    state_fips,
    cz_type,
    cz_fips,
    -- Pick one canonical name/timezone per key via min().
    -- The same FIPS combo can appear with minor name variants across years
    -- (e.g. 'ST. LUCIE' vs 'ST LUCIE'). min() is deterministic and stable.
    min(state)       as state,
    min(cz_name)     as cz_name,
    min(cz_timezone) as cz_timezone
  from {{ ref('stg_storm_event_details') }}
  where state is not null
    and state_fips is not null
  group by state_fips, cz_type, cz_fips
),

final as (
  select
    -- cz_type is included in the key because FIPS codes are namespaced separately
    -- for counties (C) and forecast zones (Z) — the same FIPS number refers to
    -- different geographic entities depending on type.
    {{ dbt_utils.generate_surrogate_key(['state_fips', 'cz_type', 'cz_fips']) }} as location_key,
    state,
    state_fips,
    cz_type,  -- C = County, Z = Forecast Zone, M = Marine
    cz_fips,
    cz_name,
    cz_timezone
  from locations
)

select * from final
