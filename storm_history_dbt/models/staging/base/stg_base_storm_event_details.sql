{{ config(materialized='view') }}

select 
    *,
    current_timestamp as etl_inserted_at
from read_csv_auto(
    '{{ env_var("STORM_EVENT_DETAILS_CSV_PATH", "data/raw/storm_event_details/*.csv.gz") }}',
    union_by_name=true,
    header=true,
    ignore_errors=true
)