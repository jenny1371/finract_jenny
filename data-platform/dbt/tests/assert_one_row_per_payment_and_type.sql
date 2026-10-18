-- A difference is reported once per (payment, kind). More than one row means a join fanned out.
select payment_id, type, count(*) as rows_found
from {{ ref('recon_discrepancies') }}
group by 1, 2
having count(*) > 1
