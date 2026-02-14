{{ config(
    materialized='table',
    tags=['dimension']
) }}

/*
    Dimension table for storm event types
    Grain: One row per unique event type
*/

with event_types as (
  select distinct
    event_type
  from {{ ref('stg_storm_event_details') }}
  where event_type is not null
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['event_type']) }} as event_type_key,
    event_type,
    -- Add categorization logic for event types
    case
      when event_type in ('Tornado', 'Funnel Cloud', 'Waterspout') then 'Tornado/Funnel'
      when event_type in ('Thunderstorm Wind', 'High Wind', 'Strong Wind') then 'Wind'
      when event_type in ('Hail') then 'Hail'
      when event_type in ('Flash Flood', 'Flood', 'Coastal Flood', 'Lakeshore Flood') then 'Flood'
      when event_type in ('Winter Storm', 'Ice Storm', 'Winter Weather', 'Heavy Snow', 'Blizzard') then 'Winter Weather'
      when event_type in ('Hurricane', 'Tropical Storm', 'Tropical Depression') then 'Tropical'
      when event_type in ('Lightning') then 'Lightning'
      when event_type in ('Wildfire') then 'Fire'
      when event_type in ('Drought', 'Excessive Heat', 'Heat', 'Cold/Wind Chill', 'Extreme Cold/Wind Chill', 'Frost/Freeze') then 'Temperature Extreme'
      else 'Other'
    end as event_category
  from event_types
)

select * from final
