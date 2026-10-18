{{ config(materialized='incremental', unique_key='pk', incremental_strategy='delete+insert', post_hook=staging_post_hook()) }}
with state as ({{ current_state(source('lake', 'orders')) }})
select
    pk,
    op = 'd'                                                          as _is_deleted,
    lsn                                                               as _lsn,
    "offset"                                                          as _offset,
    ingested_at                                                       as _ingested_at,
    json_extract_string(after, '$.id')::uuid                          as order_id,
    json_extract_string(after, '$.merchant_id')                       as merchant_id,
    json_extract_string(after, '$.payment_id')::uuid                  as payment_id,
    json_extract_string(after, '$.amount')::bigint                    as amount_cents,
    upper(json_extract_string(after, '$.currency'))                   as currency,
    json_extract_string(after, '$.status')                            as status,
    json_extract_string(after, '$.created_at')::timestamptz           as created_at,
    to_timestamp(changed_at_ms / 1000.0)                              as changed_at
from state
