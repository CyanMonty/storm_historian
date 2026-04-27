{{- config(severity='warn') -}}
/*
  Data Quality Test: Check for orphaned fatality records
  Fatalities should reference valid events.

  NOAA note: ~0.3% of fatality records reference EVENT_IDs absent from the
  details extract. This results from NOAA publishing fatalities and details in
  separate annual files that are not always in perfect sync. Configured as warn
  rather than error because this is a known source limitation, not a model bug.
*/

select
    f.fatality_id,
    f.event_id
from {{ ref('stg_storm_event_fatalities') }} f
left join {{ ref('stg_storm_event_details') }} e
    on f.event_id = e.event_id
where e.event_id is null
