# Load testing

Goal: numbers for the CV, and the real bottleneck. Method: change ONE thing, re-run, compare.

## Setup
1. Infra + Fineract: `docker compose -f docker-compose.yml -f docker-compose.infra.yml up -d`
2. Services. Payment-service runs with the simulated gateway so Stripe is not part of the measurement:
   `STRIPE_SIMULATED=true STRIPE_SIMULATED_LATENCY_MS=150` (see `scripts/run-service.ps1`).
3. Dashboards: Grafana http://localhost:3000 (admin/admin), dashboard "Payments platform".
4. Run: see the header of `order-flow.js`.

## What to read after a run

End-to-end time from order creation to COMPLETED (psql into the `orders` database):

```sql
SELECT count(*)                                                    AS completed,
       round(avg(extract(epoch FROM saga_updated_at - created_at))::numeric, 2)                         AS avg_s,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM saga_updated_at - created_at)))::numeric, 2) AS p50_s,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM saga_updated_at - created_at)))::numeric, 2) AS p99_s
FROM orders WHERE status = 'COMPLETED' AND created_at > now() - interval '15 minutes';

SELECT status, count(*) FROM orders WHERE created_at > now() - interval '15 minutes' GROUP BY status;
```

Candidate bottlenecks to check, in this order (each has a panel):
1. Fineract: `ledger_fineract_deposit_seconds` p99 and `ledger_fineract_conflicts_total` (one hot account serializes writes).
2. Outbox poll interval: `outbox.poll-ms` adds up to that much latency per hop (4 hops per order).
3. Kafka consumers: `saga.concurrency`, `ledger concurrency`, partition count.
4. DB connection pool: `hikaricp_connections_pending`.
5. Our API: `http_server_requests_seconds` p99 for POST /orders.

## Results log (fill in)
| Date | Change | Rate | API p99 | End-to-end p99 | Failures | Notes |
|---|---|---|---|---|---|---|
| 2026-10-06 | Baseline, sequential (no load), simulated Stripe 150 ms, real Fineract, outbox poll 300 ms | 1 order at a time | 20-70 ms (n=10) | median 2.9 s, max 3.7 s (first order 11 s: cold start) | 0 | Not a load test; per-hop cost before tuning. Suspects: 4 outbox hops x poll interval, Kafka consumer latency. |
| 2026-10-06 | Load run 1, BEFORE fixes: 1 merchant (1 Fineract account), saga with one shared consumer pool, outbox poll 300 ms, watchdog fixed 60 s | 10/s x 60 s (617 orders) | p99 31 ms | p50 297 s, p99 353 s, all done after 26 min | 0 (0 manual review) | Saga at ~4/s ceiling (head-of-line blocking); watchdog re-sent commands for merely queued orders (`already_booked` 163, 1000+ extra HTTP calls). |
| 2026-10-06 | Load run 2, AFTER fixes: same load, same single account | 10/s x 60 s (601 orders) | p99 206 ms (laptop under memory pressure) | p50 75 s, p99 160 s, all done after 3.7 min | 0 | Backlog now sits entirely in BOOKING: one Fineract account serializes at ~2.7 deposits/s. Drain 7x faster, p99 -55%. |
| 2026-10-06 | Load run 3, AFTER fixes: 10 merchants, each with its own Fineract account | 10/s x 60 s (600 orders) | p99 31 ms | **p50 0.8 s, p99 1.0 s, no backlog** | 0 | The remaining limit at this load is gone; the next test should raise the rate (20, 40/s) to find the next bottleneck. |

Conditions for every row: laptop (15.6 GB RAM, free memory 0.6-3 GB during runs), simulated payment processor with fixed 150 ms latency (not real Stripe), single Fineract node, single Postgres, 256 MB heaps. Absolute numbers are not production numbers; compare rows against each other.
