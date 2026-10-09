# Payment platform on Apache Fineract

A payment platform built around [Apache Fineract](https://github.com/apache/fineract) as the core ledger:
Stripe (test mode) -> payment-service -> Kafka -> ledger-service -> Fineract, with a saga that compensates on failure
and a reconciliation service that compares Stripe with the ledger.

It has two goals: a distributed-systems portfolio project, and real contributions upstream to Apache Fineract.
Everything below was measured, with the conditions stated. Nothing is claimed that was not run.

## What it does

```
client -> order-service (saga orchestrator, idempotent API, outbox)
              | Kafka (commands / replies / retry / DLQ)
              |-> payment-service -> Stripe test mode (authorize / capture / refund + webhook)
              |-> ledger-service  -> Fineract API (booking with idempotency key, undo for compensation)
              `-> reconciliation-service: compares Stripe and Fineract, reports differences only
shared: libs/platform-common
infra:  Kafka (KRaft), Postgres per service, Redis, Keycloak, Prometheus / Grafana / Zipkin, Docker Compose
```

4 services and 1 shared library, about 3,400 lines of Java, 105 automated tests, including fault injection against real
Postgres, Kafka and Redis (pausing containers, delivering a message twice, failing mid-transaction).

Design: authorize -> book in the ledger -> capture. A failed capture undoes the ledger entry before releasing the
authorization. Idempotency keys, transactional outbox, retry topics with a dead-letter queue, per-merchant rate
limiting, and a background resolver for payments stuck in PENDING. Details: [docs/DESIGN.md](docs/DESIGN.md),
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Measured results

Conditions for every number: one laptop (15.6 GB RAM), one Fineract node, one Postgres, one Kafka broker, 256 MB service
heaps, and a **simulated** payment processor with a fixed 150 ms latency. Absolute values are not production values;
compare the rows to each other. Full method and reproduction: [docs/PROBLEMS-SOLVED.md](docs/PROBLEMS-SOLVED.md),
[perf/README.md](perf/README.md).

| Problem found by load / fault testing | Before | After |
|---|---|---|
| Saga at 10 orders/s: head-of-line blocking in one shared consumer pool, and a watchdog that re-sent commands that were only queued | 600 orders took 1559 s; end-to-end p50 / p99 = 297 s / 353 s (API p99 was only 31 ms, so the API looked healthy) | 220 s; p50 / p99 = 75 s / 160 s |
| One hot ledger account (Fineract serializes writes per account) | p50 / p99 = 75 s / 160 s, 3.7 min backlog | with 10 merchant accounts: 0.8 s / 1.0 s, no backlog |
| Reconciliation read the full transaction history of an account | 3.8 s, 6.1 MB (1,323 transactions, grows without bound) | 0.24 s, 241 KB per page of 200 (paged, date-filtered) |

## Contributions to Apache Fineract

Found with the probe scripts in [scripts/](scripts/) (20+ failure scenarios against a live Fineract on PostgreSQL).
Fixes were verified on a rebuilt image, before and after, with the same script.

| JIRA | Pull request | Change | Evidence | Status |
|---|---|---|---|---|
| FINERACT-2829 | [#6587](https://github.com/apache/fineract/pull/6587) | Document the `disabled` field in the roles OpenAPI responses (the ticket said `status`; the API returns `disabled`) | Compared real responses with the OpenAPI classes | Merged |
| FINERACT-2902 | [#6591](https://github.com/apache/fineract/pull/6591) | Savings-to-savings transfers lock both accounts in ascending id order, which removes a deadlock between opposite-direction transfers | 60 crossing transfers: **4 succeeded, 56 DB deadlocks** before; **60 succeeded, 0 deadlocks** after (`pg_stat_database.deadlocks`); money conserved | Open |
| FINERACT-2900 | [#6590](https://github.com/apache/fineract/pull/6590) | New setting `max-batches-per-run` so the send-events job drains a backlog within one run (default 1, behavior unchanged) | 12,000 queued events: 1,000 per minute with the default; all sent in one run (about 4 s) with 20 batches | Open |
| FINERACT-2485 | comment, then dev list | Idempotency behavior the new design leaves undefined (same key with a different amount or a different account) | Scripted against a real instance; cause located in `CommandSourceService.findCommandSource` | Discussion |
| FINERACT-1997 | — | Failed Batch API requests leave no audit row with `enclosingTransaction=true` | Reproduced on current `develop`: without the flag both requests are written to `m_portfolio_command_source` (failed one with status 5); with it, no row at all. Earlier attempts #4791 (merged), #4845 (revert), #4847 (closed) | Reproduced |

Tested on PostgreSQL; savings-to-savings transfers.

### Local branches
Reproduced on a live instance, re-tested on a rebuilt image. In [jenny1371/fineract](https://github.com/jenny1371/fineract/branches).

| Branch | Problem | Verified result |
|---|---|---|
| `savings-amount-validation` | A deposit of 0.005 answers 200 and posts a 0.00 transaction; 10^15 answers 403 with a raw SQL "numeric field overflow" | Now 400 with no balance change; 0.01 and 100.50 still accepted; 6 new unit tests |
| `negative-offset-500` | `offset=-5` on `/clients`, `/savingsaccounts`, `/loans` answers HTTP 500 | Now 400 `validation.msg.pagination.offset.must.not.be.negative`; 3 unit tests |
| `db-outage-503` | Database down: HTTP 500 from the authentication filter | Now 503 with `Retry-After: 5`, recovers by itself; 4 unit tests |
| `events-job-reports-failure` | Kafka down: the send-events job run is recorded as COMPLETED / success | Run history shows `failed` with the Kafka error, events stay `TO_BE_SENT`, next run after recovery sends them; 3 new unit tests |

### Ruled out by measurement
- **Parallel creation of event messages** (`events-parallel-message-creation`): 13 unit tests passed, but in a real
  Fineract it failed with an EclipseLink `NullPointerException` and sent 3,000 of 12,000 events.
- **Jitter in the command retry back-off** (`retry-jitter`): no measurable effect (50 deposits/s: 87.8 % vs 89.1 %
  failed; 15/s: 8.5 % vs 7.5 %, within run-to-run noise). The limit is the serial throughput of one account row.

### Probe results
[scripts/probe-fineract-20-cases.sh](scripts/probe-fineract-20-cases.sh), [probe-fineract-reliability.sh](scripts/probe-fineract-reliability.sh),
[probe-crossing-transfers.sh](scripts/probe-crossing-transfers.sh) and the idempotency probes ran 20+ scenarios.
Correct: concurrent withdrawals never overdraw (3 rounds); the same Idempotency-Key sent 10 times concurrently
posts once; balances stay consistent under concurrency; a client that aborts and retries with the same key posts once;
bad input mostly gives clean 4xx. Also observed: Idempotency-Key longer than 50 characters answers 403 with a
raw SQL error (related to #6566, someone else's work); whitespace-only key silently disables idempotency;
`/actuator/health` did not answer within 30 s while the database was down; a second undo of the same transaction answers 200.

More in [docs/UPSTREAM-CONTRIBUTIONS.md](docs/UPSTREAM-CONTRIBUTIONS.md).

## Next steps
JWT / Keycloak integration in the services, multi-tenant isolation, outbound merchant webhooks, an audit log, cloud
deployment, and load tests with real Stripe. The load test has not gone past 10 orders/s with 10 accounts, so the next
bottleneck is unknown.

## Run
Fineract only:

    docker compose up -d
    # API: http://localhost:8443/fineract-provider/api/v1 (header Fineract-Platform-TenantId: default, basic auth mifos / password)
    # Explore with docs/fineract-api.http

Full infra (Postgres :5433, Kafka :9092, Redis :6380, Keycloak :8180, Zipkin :9411, Prometheus :9090, Grafana :3000):

    docker compose -f docker-compose.yml -f docker-compose.infra.yml up -d

Services (Java 21, Maven):

    mvn -B verify
    mvn -pl services/order-service spring-boot:run

Ports: order 8081, payment 8082, ledger 8083, reconciliation 8084. Metrics at `/actuator/prometheus`.
