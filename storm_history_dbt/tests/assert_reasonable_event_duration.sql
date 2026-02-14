/*
  Data Quality Test: Check for events with unreasonably long durations
  Events lasting more than 30 days are flagged for review
*/

select
    event_id,
    begin_date_time,
    end_date_time,
    datediff('day', begin_date_time, end_date_time) as duration_days
from {{ ref('fct_storm_events') }}
where 
    end_date_time is not null
    and begin_date_time is not null
    and datediff('day', begin_date_time, end_date_time) > 30
