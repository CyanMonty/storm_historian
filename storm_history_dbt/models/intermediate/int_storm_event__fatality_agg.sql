{{ config(materialized='view') }}

/*
    Aggregates fatality record counts per event.
    Extracted from fct_storm_events to keep fact models free of inline aggregation.

    NOAA data quality notes:
    - fatality_id is NOT globally unique across annual extract files. NOAA
      re-issues historical years with corrections, which can produce the same
      fatality_id in multiple download batches. The flag below identifies events
      where this duplication is present within the loaded dataset.
    - Orphaned fatality records (event_id absent from details) are excluded;
      see assert_no_orphaned_fatalities for monitoring.
*/

with fatalities as (
    select * from {{ ref('stg_storm_event_fatalities') }}
    where event_id is not null
)

select
    event_id,
    count(*)                                    as fatality_record_count,
    count(distinct fatality_id)                 as unique_fatality_id_count,
    -- True when the same fatality_id appears more than once for this event,
    -- which indicates duplicate rows from overlapping NOAA annual extracts.
    count(*) > count(distinct fatality_id)      as has_duplicate_fatality_ids

from fatalities
group by event_id
