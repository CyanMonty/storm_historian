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
    
    -- Parse property damage
    case
      when damage_property_raw is null then null
      when upper(damage_property_raw) like '%K' then 
        try_cast(replace(upper(damage_property_raw), 'K', '') as double) * 1000
      when upper(damage_property_raw) like '%M' then 
        try_cast(replace(upper(damage_property_raw), 'M', '') as double) * 1000000
      when upper(damage_property_raw) like '%B' then 
        try_cast(replace(upper(damage_property_raw), 'B', '') as double) * 1000000000
      else try_cast(damage_property_raw as double)
    end as damage_property_amount,
    
    -- Parse crop damage
    case
      when damage_crops_raw is null then null
      when upper(damage_crops_raw) like '%K' then 
        try_cast(replace(upper(damage_crops_raw), 'K', '') as double) * 1000
      when upper(damage_crops_raw) like '%M' then 
        try_cast(replace(upper(damage_crops_raw), 'M', '') as double) * 1000000
      when upper(damage_crops_raw) like '%B' then 
        try_cast(replace(upper(damage_crops_raw), 'B', '') as double) * 1000000000
      else try_cast(damage_crops_raw as double)
    end as damage_crops_amount

  from src
),

final as (
  select
    {{ dbt_utils.star(from=ref('stg_storm_event_details'), except=["damage_property_raw", "damage_crops_raw"]) }},
    damage_property_amount,
    damage_crops_amount,
    coalesce(damage_property_amount, 0) + coalesce(damage_crops_amount, 0) as total_damage_amount
  from parse_damage
)

select * from final
