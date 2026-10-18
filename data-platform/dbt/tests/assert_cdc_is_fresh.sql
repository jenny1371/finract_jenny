{{ config(severity='warn') }}
-- If nothing has changed in the lake for max_cdc_lag_hours, either the business is quiet or capture is down.
-- Only a warning: the pipeline cannot tell which. The Dagster freshness check on the raw layer is the alarm.
select max(to_timestamp(ts_ms / 1000.0)) as newest_change
from {{ source('lake', 'payments') }}
having max(to_timestamp(ts_ms / 1000.0)) < now() - interval '{{ var("max_cdc_lag_hours") }} hours'
