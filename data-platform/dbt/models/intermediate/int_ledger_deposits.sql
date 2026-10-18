-- Deposits in the books. The ledger-service writes the note "payment <payment id>" on every deposit; that is how a
-- Fineract transaction is tied back to a payment. Reversed (undone) deposits do not count, same as the service.
select
    n.transaction_id,
    t.savings_account_id,
    a.currency,
    t.transaction_date,
    round(t.amount * 100)::bigint                                                     as amount_cents,
    regexp_extract(n.note, 'payment ([0-9a-fA-F-]{36})', 1)::uuid                     as payment_id
from {{ ref('stg_fineract_transactions') }} t
join {{ ref('stg_fineract_notes') }} n using (transaction_id)
left join {{ ref('stg_fineract_savings_accounts') }} a using (savings_account_id)
where t.transaction_type_enum = 1
  and not t.is_reversed
  and regexp_matches(n.note, 'payment [0-9a-fA-F-]{36}')
