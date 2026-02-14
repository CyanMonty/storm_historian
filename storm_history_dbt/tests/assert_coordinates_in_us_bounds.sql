/*
  Data Quality Test: Check for events with coordinates outside US bounds
  Valid US coordinates: roughly 24-50°N latitude, -125 to -65°W longitude
  (allows for Alaska, Hawaii, and territories with some buffer)
*/

select
    event_id,
    begin_lat,
    begin_lon,
    end_lat,
    end_lon
from {{ ref('fct_storm_events') }}
where 
    (begin_lat is not null and (begin_lat < 15 or begin_lat > 72))
    or (begin_lon is not null and (begin_lon < -180 or begin_lon > -60))
    or (end_lat is not null and (end_lat < 15 or end_lat > 72))
    or (end_lon is not null and (end_lon < -180 or end_lon > -60))
