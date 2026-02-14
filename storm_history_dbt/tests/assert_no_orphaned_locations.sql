/*
  Data Quality Test: Check for orphaned location records
  Location records should reference valid events
*/

select
    l.event_id,
    l.location_index
from {{ ref('stg_storm_event_locations') }} l
left join {{ ref('stg_storm_event_details') }} e
    on l.event_id = e.event_id
where e.event_id is null
