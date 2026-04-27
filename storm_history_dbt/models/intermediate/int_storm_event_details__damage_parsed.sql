{{ config(materialized='view') }}

/*
    Intermediate model to parse damage values from text format to numeric.
    NOAA uses format like "10K", "5.5M", "1.2B" for property and crop damage.
*/

with src as (
  select * from {{ ref('stg_storm_event_details') }}
),

parse_damage as (
  select
    *,
    -- 'T' (trace) = damage too small to quantify; treated as $1 by convention.
    -- See macros/parse_noaa_damage.sql for full parsing rules.
    {{ parse_noaa_damage('damage_property_raw') }}  as damage_property_amount,
    upper(damage_property_raw) = 'T'                as damage_property_is_trace,
    {{ parse_noaa_damage('damage_crops_raw') }}      as damage_crops_amount,
    upper(damage_crops_raw) = 'T'                   as damage_crops_is_trace
  from src
),

final as (
  select
    {{ dbt_utils.star(from=ref('stg_storm_event_details'), except=["damage_property_raw", "damage_crops_raw"]) }},
    damage_property_amount,
    damage_property_is_trace,
    damage_crops_amount,
    damage_crops_is_trace,
    coalesce(damage_property_amount, 0) + coalesce(damage_crops_amount, 0) as total_damage_amount
  from parse_damage
)

select * from final
