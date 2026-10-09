# Design: what each part does, what goes wrong, and which test proves the answer

Fineract is the ledger ("the bank"). Everything else is a payment platform in front of it.
All numbers below come from `mvn verify` (90 tests, real Postgres/Kafka/Redis via Testcontainers; Stripe and
Fineract are replaced by scriptable fakes that can time out, answer 409/500, or apply a request and then hang).

## Architecture

```
                          Idempotency-Key, X-Merchant-Id (rate limited)
  client ───────────────────────────────────────────────────────────────┐
                                                                         ▼
                                                           ┌─────────────────────────┐
                                                           │      order-service      │
                                                           │  orders + SAGA ORCHESTR.│
                                                           └──┬───────▲─────────┬────┘
                                      (1) OrderCreated        │       │         │ (5) capture / cancel (HTTP, idempotent)
                                          via outbox ─► Kafka │       │         ▼
                                                              │       │   ┌───────────────────┐        ┌────────────┐
                                                              │       │   │  payment-service  │◄──────►│   Stripe   │
                         (2) authorize (HTTP, stable key) ────┼───────┼──►│ state machine,    │ manual │ (test mode)│
                                                              │       │   │ webhook endpoint  │ capture└─────┬──────┘
                                                              │       │   └─────────┬─────────┘              │ webhooks
                                                              │       │  PaymentAuthorized/Failed/          │ (signed)
                                                              │       │  Captured/Canceled (outbox)         ▼
                                                              │       └────────────Kafka◄────────── payment-service
                                  (3) BookLedger / UndoLedger │
                                      command via outbox      ▼
                                                  ┌──────────────────────┐   Idempotency-Key   ┌─────────────────┐
                                                  │    ledger-service    │────────────────────►│ Apache Fineract │
                                                  │ retry topics + DLQ   │◄────────────────────│  (savings acct) │
                                                  └──────────┬───────────┘   deposit / undo    └────────▲────────┘
                                                             │ LedgerBooked / LedgerBookingFailed /      │
                                                             │ LedgerUndone / LedgerUndoFailed (outbox)  │ read deposits
                                                             ▼ (4) back to the saga                      │
                                                          Kafka                                          │
                                                                                                         │
                          ┌──────────────────────────┐   charges (Stripe API)                            │
                          │   reconciliation-service │◄───────────────────────────────────────────────── Stripe
                          │ compare, alert, no auto- │───────────────────────────────────────────────────┘
                          │ fix; FAILED run ≠ clear  │
                          └──────────────────────────┘

  Shared library (libs/platform-common): IdempotencyGuard, OutboxPoller, RateLimiter + filter.
  Infra: Postgres (one DB per service), Kafka (KRaft), Redis. Observability: Micrometer → Prometheus → Grafana, Zipkin.
```

Happy path: **authorize → book in ledger → capture**. Booking happens before capture on purpose: if the ledger
cannot book, only the authorization is released (no money moved, no fee). If capture is refused after booking,
the ledger entry is undone first, then the authorization is released.

```
CREATED → AUTHORIZING → BOOKING → CAPTURING → COMPLETED
              │            │          │
        PAYMENT_FAILED     │   (capture refused)
                           ▼          ▼
                   (ledger failed)  UNDOING_LEDGER → VOIDING → CANCELED
                           └────────────────────────────▲
   anything unrecoverable → MANUAL_REVIEW
```

## Part by part

### 1. API idempotency — `libs/platform-common/IdempotencyGuard`
Redis `SETNX` lock + response cache is the fast path; the Postgres primary key on the key is the authority.

| Situation | Decision | Test |
|---|---|---|
| Customer double-clicks (same key, same body) | replay the first response, `Idempotent-Replayed: true` | `OrderIdempotencyAndOutboxTest.sameKeySameRequestReplays…` |
| Same key, first request still running | **409** (never block a thread waiting) | `requestInFlightGets409` |
| Same key, different body | **422** (request hash stored with the key) | `sameKeyDifferentRequestIsRejectedWith422` |
| 10 simultaneous requests, same key | exactly one order | `concurrentRequestsWithSameKeyCreateExactlyOneOrder` |
| Redis hangs | fall back to the DB key, still correct, fail fast (500 ms timeout) | `OrderResilienceTest.redisOutageFallsBackToDatabaseIdempotency` |
| Failure in the middle of the transaction | everything rolled back, lock released, retry works | `failureMidTransactionRollsBackEverythingAndRetrySucceeds` |
| Missing / invalid input | 400 | `missingKeyIs400`, `invalidBodyIs400` |

Keys live 24 h (same as Stripe). Stripe itself gets a stable key per (payment, operation), see §4.

### 2. Transactional outbox — `libs/platform-common/OutboxPoller`
State change and event are written in one DB transaction; a poller publishes with `FOR UPDATE SKIP LOCKED`.

| Situation | Decision | Test |
|---|---|---|
| Kafka down when an order arrives | order accepted, event waits in the outbox, delivered after recovery | `OrderResilienceTest.kafkaOutageDoesNotLoseOrdersOrEvents` |
| 4 pollers (replicas) at once, 250 rows | each row published exactly once | `concurrentPollersNeverPublishSameRowTwice` |
| Crash between send and commit | event re-sent (at-least-once) with the same event id, so consumers dedupe | covered by §3 |
| Same order's events must stay ordered | Kafka key = orderId | `PaymentFaultTest.lifecycleEventsAreKeyedByOrderAndInOrder` |

### 3. Consumer idempotency + retry + DLQ — `ledger-service` (`LedgerService`, `PaymentEventListener`)
| Situation | Decision | Test |
|---|---|---|
| Same command delivered twice (even on two consumers at once) | `processed_events` + per-payment lock + atomic status flip; one Fineract call, one reply | `LedgerFaultTest.sameEventDeliveredTwiceBooksOnce` |
| Saga re-sends because it lost the reply | answer again, do not touch Fineract | `samePaymentUnderADifferentEventIdBooksOnce` |
| Fineract returns 409 (optimistic lock, see FINERACT-2000) | retry with a **new** key | `conflictsAreRetriedWithANewKeyEachTime` |
| Fineract applied the deposit but the answer timed out | retry with the **same** key; never double-books | `timeoutAfterApplyIsRetriedWithTheSameKeyAndNeverDoubleBooks` |
| Fineract down | retry topics with exponential backoff, then recovers | `fineractOutageRecoversThroughRetryTopics` |
| Fineract down for good | bounded retries, DLQ, `LedgerBookingFailed` to the saga | `permanentOutageEndsInFailedAndEmitsLedgerBookingFailed` |
| Fineract says 4xx / unknown merchant / garbage message | straight to DLQ, no pointless retries; queue never blocks | `rejectedByFineract…`, `unknownMerchant…`, `poisonMessageDoesNotBlockTheQueue` |
| 24 payments hit one Fineract account | per-account lock keeps Fineract writes serial (avoids our own 409 storm) | `hotAccountWritesAreSerialized` |
| After the cause is fixed | `POST /admin/dlq/replay`: replays only what was there when called, skips poison, cannot loop | `dlqReplayBooksAfterTheCauseIsFixed` |

### 4. Payment lifecycle + Stripe — `payment-service` (`PaymentService`, `StripePaymentGateway`)
Every transition is a compare-and-set; the winner calls Stripe, losers get 409; no DB connection is held during the call.

| Situation | Decision | Test |
|---|---|---|
| Card declined | `FAILED`, cannot be captured | `PaymentFaultTest.declinedCardEndsInFailedAndCannotBeCaptured` |
| Stripe timeout (money may have moved) | stays `PENDING`, 503; retry with the same key resumes, Stripe acted once | `timeoutThenRetryWithSameKeyResumesWithoutDoubleCharge` |
| Process dies mid-flow | `PENDING` row is resumed by the next request | `crashLeavesPendingAndANewRequestResumesTheSamePayment` |
| 10 simultaneous captures | exactly one reaches Stripe | `concurrentCapturesReachTheProcessorOnce` |
| Capture times out | claim held, re-claimable after the claim goes stale, same Stripe key | `captureTimeoutIsRetryableAfterClaimGoesStale` |
| Cancel after capture / capture after cancel | 409 | `cannotCancelAfterCaptureOrCaptureAfterCancel` |

### 5. Webhooks are the truth — `payment-service` (`WebhookController`, `StripeSignatureVerifier`)
| Situation | Decision | Test |
|---|---|---|
| Forged, unsigned, tampered or replayed (old timestamp) request | 400, nothing changes | `WebhookTest.forgedStaleOrMissingSignaturesAreRejectedAndChangeNothing` |
| Timeout left a payment `PENDING` | webhook resolves it to AUTHORIZED or FAILED | `webhookResolvesATimedOutAuthorizationTo…` |
| Stripe retries the same event | applied once (event id stored in the same transaction), still 2xx | `sameEventDeliveredTwiceIsAppliedOnce` |
| Late / re-ordered events | only forward transitions; ignored otherwise | `lateAuthorizationEventAfterTheFinalState…`, `failedThenLateAuthorized…` |
| Webhook beats our own slow call | HTTP call finishes cleanly, no second event | `webhookArrivingWhileTheAuthorizeCallIsStillRunning…` |

### 6. Saga — `order-service/saga` (`OrderSaga`, `SagaWatchdog`)
| Situation | Decision | Test |
|---|---|---|
| Normal order | authorize → book → capture → `COMPLETED` | `SagaTest.happyPath…` |
| Declined card | `PAYMENT_FAILED`, ledger untouched | `declinedCard…` |
| Ledger cannot book | void the authorization, never capture | `ledgerFailureVoidsTheAuthorizationAndNeverCaptures` |
| Capture refused after booking | undo the ledger entry, then void | `captureRefusedUndoesTheLedgerThenVoids` |
| Undo fails / void refused | `MANUAL_REVIEW` (never retried forever) | `failedLedgerUndoNeedsAHuman`, `refusedVoidNeedsAHuman` |
| payment-service down at the start | retried with the same idempotency key | `paymentServiceOutageAtStartIsRetriedWithTheSameKey` |
| Duplicate / out-of-order / late events | compare-and-set makes them harmless | `duplicateOutOfOrderAndLateEventsAreHarmless` |
| A reply is lost | watchdog re-sends the command, escalates after N tries | `lostLedgerReplyIsRedrivenThenEscalated` |
| Retries exhausted | DLQ handler → `MANUAL_REVIEW` | `exhaustedCaptureRetriesEndInManualReview` |
| Late `BookLedger` after an undo (ledger side) | refused | `LedgerFaultTest.lateBookLedgerAfterUndoIsRefused` |

### 7. Per-merchant rate limiting — `platform-common/RateLimiter` (Lua token bucket in Redis)
| Situation | Decision | Test |
|---|---|---|
| Burst over capacity | 429 + `Retry-After` | `RateLimitTest.burstBeyondCapacity…` |
| One merchant floods | others unaffected (separate buckets) | `oneMerchantFloodingDoesNotAffectAnother` |
| Bucket refills | allowed again later | `bucketRefillsOverTime` |
| Big merchant | per-merchant override | `perMerchantOverrideRaisesTheLimit` |
| 40 simultaneous requests, bucket of 5 | exactly 5 pass (script is atomic, Redis clock) | `concurrentRequestsCannotOverspendABucket` |
| Redis down | **fail open** (metric counted): blocking all payments is worse | `redisOutageFailsOpen` |
| Junk merchant id | 400, never reaches Redis keys | `junkMerchantId…` |

Known gap: the merchant comes from the `X-Merchant-Id` header, which can be spoofed. It must come from a verified JWT.

### 8. Reconciliation — `reconciliation-service`
Stripe is the evidence money was collected; Fineract is the books. Compared per payment id. **Alert only, no auto-fix**
(see "why not auto-fix" below).

| Situation | Decision | Test |
|---|---|---|
| Collected but not booked / booked but not collected / amount or currency differs / booked twice | one discrepancy each, per (payment, type) | `ReconcilerTest` (9 cases) |
| Same difference found on every run | one row, not many | `differencesAreRecordedOnceEvenWhenRunRepeatedly` |
| **Stripe or Fineract unreachable** | run is `FAILED`, no conclusions, nothing invented | `stripeOutageFailsTheRunWithoutInventingDiscrepancies` |
| Work in flight (we book before we capture) | alert only after the grace period | `alertsOnlyAfterTheGracePeriod` |
| Difference goes away | auto-`RESOLVED`; if it returns, reopened with a fresh grace period | `aDifferenceThatDisappearsIsResolvedAutomatically` |
| Real Fineract JSON | parsed and pinned by a test | `FineractDepositSourceTest` |

Why not auto-fix: with only two numbers the program cannot know which side is wrong (e.g. Stripe 100.00 vs ledger 99.00
is a bug if our code truncated, but correct if support refunded 1.00). The one safe auto-fix is *replaying an action
that is already decided and idempotent* (re-send `BookLedger` for a captured payment). Everything else should need approval.

## Not built yet (be honest in interviews)
Keycloak/JWT, Spring Cloud Gateway, row-level-security multi-tenancy, outbound merchant webhooks, audit log, Terraform/AWS,
k6 results + Grafana dashboards, auto-repair of "collected but not booked", MCP server.

## Verified against the real thing
- Fineract: `Idempotency-Key` works under concurrency; hot-account contention returns 409 by design
  ([FINERACT-2000](https://issues.apache.org/jira/browse/FINERACT-2000)); deposit with a note, and `command=undo`
  (sets `reversed: true`) work with the request shapes used here. See `docs/fineract-findings.md`.
- Stripe test mode: authorize (manual capture) → capture, replay by key, declined card.
