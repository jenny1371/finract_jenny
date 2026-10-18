{{ config(materialized='incremental', unique_key='pk', incremental_strategy='delete+insert', post_hook=staging_post_hook()) }}
with state as ({{ current_state(source('lake', 'fineract_savings_account')) }})
select
    pk,
    op = 'd'                                                          as _is_deleted,
    lsn                                                               as _lsn,
    "offset"                                                          as _offset,
    ingested_at                                                       as _ingested_at,
    json_extract_string(after, '$.id')::bigint                        as savings_account_id,
    json_extract_string(after, '$.client_id')::bigint                 as client_id,
    upper(json_extract_string(after, '$.currency_code'))              as currency,
    json_extract_string(after, '$.status_enum')::int                  as status_enum
from state
