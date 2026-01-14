{{ config(
    materialized = "table",
    tags = ["base"],
    meta = {
      "raw_dataset": "storm_event_locations",
      "grain": "row"
    },
    depends_on = {
      "exposure": ["raw_files_catalog"]
    }
) }}

select
    *,
    current_timestamp as etl_inserted_at
from read_csv(
    '{{ env_var("STORM_EVENT_LOCATIONS_CSV_PATH", "data/raw/storm_event_locations/*.csv.gz") }}',
    header = true,
    union_by_name = true,
    ignore_errors = true,
    all_varchar = true
)