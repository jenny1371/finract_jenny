-- Daily transaction volume per currency, by the day the payment was created (UTC), with what reached the ledger.
with payments as (
    select created_at::date as day, currency,
           count(*) filter (where status = 'CAPTURED')                    as captured_count,
           coalesce(sum(amount_cents) filter (where status = 'CAPTURED'), 0) as captured_cents,
           count(*) filter (where status <> 'CAPTURED')                   as not_captured_count
    from {{ ref('stg_payments') }}
    group by 1, 2
),
booked as (
    select p.created_at::date as day, l.currency,
           count(*)                  as booked_count,
           sum(l.amount_cents)       as booked_cents
    from {{ ref('stg_ledger_entries') }} l
    join {{ ref('stg_payments') }} p using (payment_id)
    where l.status = 'BOOKED'
    group by 1, 2
)
select
    p.day,
    p.currency,
    p.captured_count,
    p.captured_cents,
    p.not_captured_count,
    coalesce(b.booked_count, 0)                                        as ledger_booked_count,
    coalesce(b.booked_cents, 0)                                        as ledger_booked_cents,
    p.captured_cents - coalesce(b.booked_cents, 0)                     as unbooked_cents
from payments p
left join booked b using (day, currency)
order by p.day, p.currency
