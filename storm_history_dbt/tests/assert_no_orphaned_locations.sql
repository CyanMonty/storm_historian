{{- config(severity='warn') -}}
/*
  Data Quality Test: Check for orphaned location records
  Location records should reference valid events.

  NOAA note: ~0.3% of location records reference EVENT_IDs absent from the
  details extract. This results from NOAA publishing locations and details in
  separate annual files that are not always in perfect sync. Configured as warn
  rather than error because this is a known source limitation, not a model bug.
*/

select
    l.event_id,
    l.location_index
from {{ ref('stg_storm_event_locations') }} l
left join {{ ref('stg_storm_event_details') }} e
    on l.event_id = e.event_id
where e.event_id is null
