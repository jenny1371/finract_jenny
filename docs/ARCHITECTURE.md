# Architecture

Fineract is the **core ledger** ("the bank"). Our platform sits in front of it.

```
client -> order-service -> payment-service -> Stripe (test mode)
                               |  (outbox -> Kafka: payment.succeeded)
                               v
                         ledger-service -> Fineract API (idempotent booking)
                               |  (Kafka: ledger.booked / ledger.failed)
                               v
              notification / reconciliation-service  <- Fineract + Stripe records
```

| Service | Port | DB | Role |
|---|---|---|---|
| order-service | 8081 | orders | create/track orders |
| payment-service | 8082 | payments | Stripe charge/refund, outbox publisher |
| ledger-service | 8083 | ledger | consume events, book to Fineract, Redis balance cache |
| reconciliation-service | 8084 | reconciliation | scheduled job: Stripe txns vs Fineract journal entries, alert on mismatch |

Infra: Kafka (KRaft), Redis, Keycloak (OAuth2/JWT), Prometheus/Grafana/Zipkin, Postgres (one DB per service).

## Key design questions (answer each with an ADR in docs/adr/)
- Stripe charge succeeded but Fineract booking fails: retry, or refund (Saga compensation)?
- Idempotency: key = Stripe PaymentIntent id, carried end to end into the Fineract call.
- Outbox pattern in payment-service so "charge recorded" and "event published" are atomic.
- Retry topics + DLQ for the ledger-service consumer.
