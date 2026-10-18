with state as ({{ current_state(source('lake', 'fineract_note')) }})
select
    json_extract_string(after, '$.id')::bigint                                        as note_id,
    json_extract_string(after, '$.savings_account_transaction_id')::bigint            as transaction_id,
    json_extract_string(after, '$.note')                                              as note
from state
where json_extract_string(after, '$.savings_account_transaction_id') is not null
