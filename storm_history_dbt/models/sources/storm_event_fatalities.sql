select 
    *,
    current_timestamp as etl_inserted_at
from read_csv_auto('{{ env_var("STORM_EVENT_FATALITIES_CSV_PATH", "data/raw/storm_event_fatalities/*.csv.gz") }}')