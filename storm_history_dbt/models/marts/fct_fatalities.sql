{{ config(
    materialized='table',
    tags=['fact']
) }}

/*
    Fact table for storm event fatalities
    Grain: One row per fatality record (fatality_id)

    NOAA data quality notes:
    - fatality_id is NOT globally unique across annual extract files. NOAA
      revises prior years in later publications, which can produce the same
      fatality_id in multiple download batches. See int_storm_event__fatality_agg
      for a per-event flag that identifies affected events.
    - Fatality records whose event_id is absent from fct_storm_events (~0.3%)
      retain null event context columns due to the left join below.
*/

with fatalities as (
    select * from {{ ref('stg_storm_event_fatalities') }}
),

-- Source event context from fct_storm_events rather than staging directly.
-- This ensures a single source of truth: if staging logic changes, the fact
-- tables agree without dual-maintenance.
events as (
    select
        event_id,
        episode_id,
        event_type,
        event_type_key,
        location_key,
        state,
        state_fips,
        cz_name,
        begin_date_time as event_begin_date_time
    from {{ ref('fct_storm_events') }}
),

joined as (
    select
        f.fatality_id,
        f.event_id,

        -- Dimension foreign keys (resolved in fct_storm_events, re-used here)
        e.event_type_key,
        e.location_key,

        -- Event context
        e.episode_id,
        e.event_type,
        e.state,
        e.state_fips,
        e.cz_name,
        e.event_begin_date_time,

        -- Fatality timing
        f.fatality_date_time,
        f.fat_yearmonth,
        f.fat_day,
        f.fat_time_hhmm,
        f.event_yearmonth,

        -- Fatality details
        f.fatality_type,      -- Direct or Indirect
        f.fatality_age,
        f.fatality_sex,
        f.fatality_location,

        -- Metadata
        f.etl_inserted_at

    from fatalities f
    left join events e on f.event_id = e.event_id
)

select * from joined
