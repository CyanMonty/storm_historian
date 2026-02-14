/*
  Data Quality Test: Check for orphaned fatality records
  Fatalities should reference valid events
*/

select
    f.fatality_id,
    f.event_id
from {{ ref('stg_storm_event_fatalities') }} f
left join {{ ref('stg_storm_event_details') }} e
    on f.event_id = e.event_id
where e.event_id is null
