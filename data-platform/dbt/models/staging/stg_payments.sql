with state as ({{ current_state(source('lake', 'payments')) }})
select
    json_extract_string(after, '$.id')::uuid                          as payment_id,
    json_extract_string(after, '$.order_id')::uuid                    as order_id,
    json_extract_string(after, '$.amount')::bigint                    as amount_cents,
    upper(json_extract_string(after, '$.currency'))                   as currency,
    json_extract_string(after, '$.status')                            as status,
    json_extract_string(after, '$.stripe_intent_id')                  as stripe_intent_id,
    json_extract_string(after, '$.created_at')::timestamptz           as created_at,
    json_extract_string(after, '$.updated_at')::timestamptz           as updated_at,
    to_timestamp(changed_at_ms / 1000.0)                              as changed_at
from state
