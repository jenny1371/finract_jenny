with state as ({{ current_state(source('lake', 'ledger_entries')) }})
select
    json_extract_string(after, '$.id')::uuid                          as ledger_entry_id,
    json_extract_string(after, '$.payment_id')::uuid                  as payment_id,
    json_extract_string(after, '$.merchant_id')                       as merchant_id,
    json_extract_string(after, '$.amount')::bigint                    as amount_cents,
    upper(json_extract_string(after, '$.currency'))                   as currency,
    json_extract_string(after, '$.status')                            as status,
    json_extract_string(after, '$.fineract_txn_id')                   as fineract_txn_id,
    json_extract_string(after, '$.created_at')::timestamptz           as created_at,
    to_timestamp(changed_at_ms / 1000.0)                              as changed_at
from state
