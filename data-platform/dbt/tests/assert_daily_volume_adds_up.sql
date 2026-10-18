-- The daily volume table must add up to the captured payments it was built from: no day or currency may be lost.
with from_table as (select coalesce(sum(captured_cents), 0) as cents, coalesce(sum(captured_count), 0) as n
                    from {{ ref('fct_daily_transaction_volume') }}),
     from_staging as (select coalesce(sum(amount_cents), 0) as cents, count(*) as n
                      from {{ ref('stg_payments') }} where status = 'CAPTURED')
select t.cents as table_cents, s.cents as staging_cents, t.n as table_count, s.n as staging_count
from from_table t, from_staging s
where t.cents <> s.cents or t.n <> s.n
