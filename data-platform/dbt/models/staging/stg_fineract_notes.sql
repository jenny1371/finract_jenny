{{ config(materialized='incremental', unique_key='pk', incremental_strategy='delete+insert', post_hook=staging_post_hook()) }}
with state as ({{ current_state(source('lake', 'fineract_note')) }})
select
    pk,
    op = 'd'                                                                          as _is_deleted,
    lsn                                                                               as _lsn,
    "offset"                                                                          as _offset,
    ingested_at                                                                       as _ingested_at,
    json_extract_string(after, '$.id')::bigint                                        as note_id,
    json_extract_string(after, '$.savings_account_transaction_id')::bigint            as transaction_id,
    json_extract_string(after, '$.note')                                              as note
from state
