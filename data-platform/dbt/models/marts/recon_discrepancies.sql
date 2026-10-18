-- Batch reconciliation of the payments (Stripe, as recorded by payment-service) against the Fineract ledger.
-- Same four kinds of difference, same rules, same grace period as reconciliation-service, but computed over the whole
-- lookback window from the lake instead of one HTTP page at a time. It only finds differences; it never fixes any.
{% set as_of = "cast('" ~ var('as_of') ~ "' as timestamptz)" if var('as_of') else "now()" %}

with params as (
    select
        {{ as_of }}                                                               as as_of,
        {{ as_of }} - interval '{{ var("recon_grace_seconds") }} seconds'         as grace_cutoff,
        ({{ as_of }})::date - {{ var("recon_lookback_days") }}                    as window_start
),

charges as (                      -- money Stripe collected, old enough that its booking should be done
    select p.payment_id, p.amount_cents, p.currency, p.created_at::date as business_date
    from {{ ref('stg_payments') }} p, params
    where p.status = 'CAPTURED'
      and p.created_at < params.grace_cutoff
      and p.created_at::date >= params.window_start
),

deposits as (                     -- what the books say, per payment
    select
        payment_id,
        count(*)                                  as deposit_count,
        sum(amount_cents)                         as deposit_cents,
        min(currency)                             as currency,
        min(transaction_date)                     as business_date,
        list(transaction_id order by transaction_id) as transaction_ids
    from {{ ref('int_ledger_deposits') }}, params
    where transaction_date >= params.window_start
    group by payment_id
),

differences as (
    select c.payment_id, 'MISSING_IN_LEDGER' as type, c.business_date, c.amount_cents as amount_at_risk_cents,
           'Stripe collected ' || c.amount_cents || ' ' || c.currency || ' but the ledger has no deposit' as detail
    from charges c left join deposits d using (payment_id)
    where d.payment_id is null

    union all
    select c.payment_id, 'DUPLICATE_BOOKING', c.business_date, d.deposit_cents - c.amount_cents,
           d.deposit_count || ' ledger deposits for one payment: ' || cast(d.transaction_ids as varchar)
    from charges c join deposits d using (payment_id)
    where d.deposit_count > 1

    union all
    select c.payment_id, 'AMOUNT_MISMATCH', c.business_date, abs(d.deposit_cents - c.amount_cents),
           'Stripe ' || c.amount_cents || ' ' || c.currency || ' vs ledger ' || d.deposit_cents || ' ' || d.currency
    from charges c join deposits d using (payment_id)
    where d.deposit_count = 1 and (d.deposit_cents <> c.amount_cents or upper(d.currency) <> c.currency)

    union all
    select d.payment_id, 'MISSING_IN_STRIPE', d.business_date, d.deposit_cents,
           'Ledger deposit ' || cast(d.transaction_ids as varchar) || ' has no collected payment in Stripe'
    from deposits d
    where d.payment_id not in (select payment_id from {{ ref('stg_payments') }} where status = 'CAPTURED')
)

select payment_id, type, business_date, amount_at_risk_cents, detail, (select as_of from params) as detected_as_of
from differences
