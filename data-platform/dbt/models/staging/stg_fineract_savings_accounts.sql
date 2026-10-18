with state as ({{ current_state(source('lake', 'fineract_savings_account')) }})
select
    json_extract_string(after, '$.id')::bigint                        as savings_account_id,
    json_extract_string(after, '$.client_id')::bigint                 as client_id,
    upper(json_extract_string(after, '$.currency_code'))              as currency,
    json_extract_string(after, '$.status_enum')::int                  as status_enum
from state
