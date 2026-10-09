# Problems solved in this platform, with evidence

Every number below was measured here, not estimated. How to reproduce each one is in `perf/README.md`, `scripts/`
and the named tests. **Conditions for all measurements:** a laptop (15.6 GB RAM, 0.6-3 GB free during runs), one
Fineract node, one Postgres, one Kafka broker, 256 MB service heaps, and a *simulated* payment processor with a fixed
150 ms latency (real Stripe was used only for functional checks). Absolute values are not production values; compare
the rows to each other.

## 1. The saga collapsed at 10 orders per second
**Symptom.** A 60-second burst of 10 orders/s (about 600 orders) took 26 minutes to finish. No order failed, but
end-to-end p50 was 297 s and p99 353 s. API latency stayed at 31 ms p99, so the API looked healthy while the
backlog grew.

**How it was found.** k6 for the load, Prometheus for metrics, SQL on the saga table for end-to-end time, Kafka
consumer-group lag, and a thread dump (`jstack`) of the saga service. CPU was near 0 % and the connection pool was
idle, so the system was *waiting*, not busy.

**Root causes (three, found one after another).**
1. *Head-of-line blocking.* One consumer pool read three topics. Handling `OrderCreated` makes a synchronous HTTP call
   (~300 ms), so the fast events (`PaymentAuthorized`, `LedgerBooked`) queued behind them. Capacity was about
   3 threads / 0.7 s = 4 orders/s.
2. *The recovery watchdog amplified the load.* It treated "waiting in a queue" as "stuck" and re-sent commands
   after a fixed 60 s: 163 duplicate booking commands and about 1000 extra HTTP calls in one run (1051 authorize calls
   for 606 orders, 1162 capture calls for 617), which made the backlog worse (a positive feedback loop).
3. *Outbox polling at 300 ms*, paid four times per order.

**Fixes.** One consumer (own thread pool, retry topics, dead-letter topic) per topic; the watchdog now waits twice as
long after every re-drive (`stuck-after x 2^attempts`); outbox poll 300 -> 100 ms; ledger consumers 3 -> 6.

**Result (same load, same single ledger account).**

| | Before | After |
|---|---|---|
| Time to finish ~600 orders | 1559 s | **220 s** |
| End-to-end p50 / p99 | 297 s / 353 s | **75 s / 160 s** |
| Failed / needing a human | 0 / 0 | 0 / 0 |

**Tests.** `SagaTest` (11 scenarios, including a lost reply that the watchdog re-drives and then escalates).

## 2. One hot ledger account capped throughput; spreading load removed the cap
After fix 1 the entire backlog sat in the booking step. Concurrent writes to one Fineract savings account hit its
optimistic lock (observed ~7 % HTTP 409 at 20-way concurrency; by design, FINERACT-2000), so the ledger service
serializes writes per account. That is correct, but it puts a ceiling of roughly 2.7-4.7 bookings/s on one account.

Spreading the same load over 10 merchants (each mapped to its own Fineract account, `merchant_accounts` table):

| Same 10 orders/s for 60 s | 1 account | 10 accounts |
|---|---|---|
| End-to-end p50 / p99 | 75 s / 160 s | **0.8 s / 1.0 s** |
| Backlog after the burst | 3.7 minutes | **none** |

**Takeaway.** The limit is per account, not per system, so the merchant-to-account mapping is a scaling design
decision, not a detail. Tests: `LedgerFaultTest.hotAccountWritesAreSerialized`.

## 3. Reconciliation cost grew with account history
The first reconciliation read a savings account with `?associations=transactions`, which returns every transaction
ever. Measured on an account with 1323 transactions: **3.8 s and 6.1 MB**, growing without bound. Fineract already
has a paged endpoint (`/transactions/search` with `offset`, `limit`, `fromDate`). Switching to it:
**0.24 s and 241 KB per page of 200**, and the cost now scales with the reconciliation window, not the account's
lifetime. Verified against a real Fineract (`FineractDepositSourceLiveTest`, opt-in) and with a paging unit test
(`FineractDepositSourceTest`, 450 rows over 3 pages).

## 4. Defects that fault-injection tests found before they could reach production
Each was found by deliberately breaking a dependency (pausing a container, failing mid-transaction, delivering a
message twice) and then fixed:

| Defect | Found by | Fix |
|---|---|---|
| The same command delivered to two consumers at once produced two result events | `LedgerFaultTest.sameEventDeliveredTwiceBooksOnce` | per-payment lock plus an atomic status flip decides who emits |
| DLQ replay re-read its own re-failed messages and looped (498 iterations) | replay test with a poison message | replay only what existed when it started; skip unparseable messages |
| A webhook racing the in-flight authorize call raised a false conflict | `WebhookTest.webhookArrivingWhileTheAuthorizeCall...` | compare-and-set that treats "already in the target state" as success |
| Redis and Kafka default timeouts are 60 s, so one hung dependency hung every request | `OrderResilienceTest` (container pause) | 500 ms Redis timeout, bounded Kafka send, fail-open rate limiter |
| An unreachable Stripe/Fineract could look like "everything is missing" in reconciliation | `ReconciliationServiceTest.stripeOutageFails...` | a source error fails the whole run; no conclusions are drawn |
| Payments stuck in PENDING after a processor timeout had no owner | `PendingResolverTest` | background resolver re-runs the same call with the same processor key; gives up (fails) before the key expires |

## 5. Booking into an external ledger exactly once in effect
Kafka delivers at least once and Fineract can time out after applying a request. The booking path therefore uses an
idempotency key whose rule depends on what the failure tells us:
- *unknown outcome* (timeout, 5xx): retry with the **same** key, so Fineract cannot apply it twice;
- *definite 409* (Fineract rolled back): retry with a **new** key (otherwise Fineract replays the failure);
- *definite rejection* (4xx): straight to the dead-letter queue, no pointless retries.

Compensation is part of the design: authorize -> book -> capture, and a failed capture undoes the ledger entry before
releasing the authorization. A late `BookLedger` after an `UndoLedger` is refused. Verified with scripted failures
(`LedgerFaultTest`, `SagaTest`) and against a real Fineract (deposit with note, `command=undo`).

## 6. Data lifecycle
De-duplication tables (`processed_events`, `webhook_events`) are purged in batches with a retention longer than
anything that can redeliver (`RetentionCleaner`; tests `LedgerRetentionTest`, `PendingResolverTest`). A redelivery
after the purge is still safe because the ledger entry's own state prevents a second booking.

## Limits, stated plainly
- Single node, simulated payment processor, memory-constrained laptop. Treat the results as relative.
- The load test has not yet gone past 10 orders/s with 10 accounts; the next bottleneck is not yet known.
- Authentication (JWT), multi-tenant isolation, outbound merchant webhooks, audit log and cloud deployment are not built.
- Reconciliation reports differences; it does not repair them (see `docs/DESIGN.md` for why).

## CV-ready statements (each is accurate under the conditions above)
- Cut the time to drain a 600-order burst from 26 min to 3.7 min (p99 353 s -> 160 s) by removing head-of-line
  blocking in Kafka saga consumers and giving the recovery watchdog exponential back-off.
- Kept end-to-end p99 at 1.0 s at 10 orders/s by spreading bookings across per-merchant ledger accounts after finding
  the per-account write ceiling (~3 bookings/s) in load tests.
- Reduced reconciliation read cost from 3.8 s / 6.1 MB to 0.24 s / 241 KB per page by moving to paged, date-filtered queries.
- Built an orchestrated payment saga (authorize, ledger booking, capture) with compensation, idempotency, transactional
  outbox, retry/DLQ and rate limiting; about 100 automated tests, including fault injection against real Postgres, Kafka and Redis.
