-- The services enforce amount > 0 with a CHECK constraint. A non-positive amount in the lake means the capture or the
-- parsing is wrong, so it must stop the pipeline before anything is reported.
select 'payments' as source, payment_id::varchar as id, amount_cents from {{ ref('stg_payments') }} where amount_cents <= 0
union all
select 'ledger_entries', ledger_entry_id::varchar, amount_cents from {{ ref('stg_ledger_entries') }} where amount_cents <= 0
union all
select 'ledger_deposits', transaction_id::varchar, amount_cents from {{ ref('int_ledger_deposits') }} where amount_cents <= 0
