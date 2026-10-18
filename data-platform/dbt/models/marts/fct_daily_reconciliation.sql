-- Per day: how many payments were reconciled, how many differences were found, and the share of payments affected.
-- diff_rate = payments with at least one difference / payments checked. MISSING_IN_STRIPE has no payment, so it is
-- counted in missing_in_stripe but not in the rate.
with checked as (
    select created_at::date as day, count(*) as payments_checked
    from {{ ref('stg_payments') }}
    where status = 'CAPTURED'
      and created_at < {% if var('as_of') %}cast('{{ var("as_of") }}' as timestamptz){% else %}now(){% endif %}
                       - interval '{{ var("recon_grace_seconds") }} seconds'
      and created_at::date >= {% if var('as_of') %}cast('{{ var("as_of") }}' as date){% else %}current_date{% endif %} - {{ var("recon_lookback_days") }}
    group by 1
),
found as (
    select
        business_date as day,
        count(distinct payment_id) filter (where type <> 'MISSING_IN_STRIPE') as payments_with_difference,
        count(*) filter (where type = 'MISSING_IN_LEDGER')                    as missing_in_ledger,
        count(*) filter (where type = 'AMOUNT_MISMATCH')                      as amount_mismatch,
        count(*) filter (where type = 'DUPLICATE_BOOKING')                    as duplicate_booking,
        count(*) filter (where type = 'MISSING_IN_STRIPE')                    as missing_in_stripe,
        sum(amount_at_risk_cents)                                             as amount_at_risk_cents
    from {{ ref('recon_discrepancies') }}
    group by 1
)
select
    coalesce(c.day, f.day)                                                     as day,
    coalesce(c.payments_checked, 0)                                            as payments_checked,
    coalesce(f.payments_with_difference, 0)                                    as payments_with_difference,
    coalesce(f.missing_in_ledger, 0)                                           as missing_in_ledger,
    coalesce(f.amount_mismatch, 0)                                             as amount_mismatch,
    coalesce(f.duplicate_booking, 0)                                           as duplicate_booking,
    coalesce(f.missing_in_stripe, 0)                                           as missing_in_stripe,
    coalesce(f.amount_at_risk_cents, 0)                                        as amount_at_risk_cents,
    case when coalesce(c.payments_checked, 0) = 0 then null
         else round(coalesce(f.payments_with_difference, 0)::double / c.payments_checked, 4) end as diff_rate
from checked c
full outer join found f using (day)
order by 1
