-- The difference rate is a share of payments: 0 to 1. Anything else means the numerator and denominator disagree.
select day, payments_checked, payments_with_difference, diff_rate
from {{ ref('fct_daily_reconciliation') }}
where diff_rate < 0 or diff_rate > 1
   or payments_with_difference > payments_checked
