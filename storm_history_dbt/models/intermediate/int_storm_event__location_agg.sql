{{ config(materialized='view') }}

/*
    Aggregates location point data per event.
    Extracted from fct_storm_events to keep fact models free of inline aggregation.

    NOAA data quality notes:
    - Orphaned location records (event_id present in locations but absent from
      details) are a known NOAA issue in ~0.3% of rows, excluded here intentionally.
    - US/territory longitudes should be negative. Positive longitude values in the
      source data indicate a sign-flip error in NOAA's extract. The corrected
      longitude from stg_storm_event_locations is used for avg_longitude here.
*/

with locations as (
    select * from {{ ref('stg_storm_event_locations') }}
    where event_id is not null
)

select
    event_id,
    count(*)                               as location_point_count,
    avg(latitude)                          as avg_latitude,
    avg(longitude_corrected)               as avg_longitude,
    -- Flags whether any of this event's location points had a suspect
    -- (positive) longitude, so downstream consumers can filter or audit.
    bool_or(longitude_suspect)             as has_suspect_longitude

from locations
group by event_id
