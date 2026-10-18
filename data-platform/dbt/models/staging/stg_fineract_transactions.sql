{{ config(materialized='incremental', unique_key='pk', incremental_strategy='delete+insert', post_hook=staging_post_hook()) }}
-- Debezium sends the DATE columns of Fineract as days since 1970-01-01 and DECIMAL columns as strings (exact).
with state as ({{ current_state(source('lake', 'fineract_savings_transaction')) }})
select
    pk,
    op = 'd'                                                                          as _is_deleted,
    lsn                                                                               as _lsn,
    "offset"                                                                          as _offset,
    ingested_at                                                                       as _ingested_at,
    json_extract_string(after, '$.id')::bigint                                        as transaction_id,
    json_extract_string(after, '$.savings_account_id')::bigint                        as savings_account_id,
    json_extract_string(after, '$.transaction_type_enum')::int                        as transaction_type_enum,   -- 1 deposit, 2 withdrawal
    json_extract_string(after, '$.is_reversed')::boolean                              as is_reversed,
    date '1970-01-01' + json_extract_string(after, '$.transaction_date')::int         as transaction_date,
    json_extract_string(after, '$.amount')::decimal(19, 6)                            as amount
from state
