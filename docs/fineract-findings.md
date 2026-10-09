# Fineract findings log
| Date | Symptom (with numbers) | Root cause | JIRA | PR | Status |
|---|---|---|---|---|---|

## Observations (2026-10-06, apache/fineract:latest docker image, single node, Postgres 16)
Reproduce with `scripts/probe-fineract-idempotency.ps1`.

1. **Idempotency-Key works under concurrency.** 10 parallel deposits, same key -> 10x HTTP 200, exactly 1 posting
   (server logs `IdempotentCommandProcessSucceedException` for the replays). We can rely on it end to end.
2. **Hot-account contention -> intermittent 409 with an EMPTY body.** 80 concurrent deposits (no key) to ONE savings
   account: 74x200, 6x409 (~7.5%). Cause in logs: EclipseLink `OptimisticLockException` on `SavingsAccount`;
   `SynchronousCommandProcessingService.executeWithRetry` (resilience4j) retries, then gives up.
   - **Known / by design:** [FINERACT-2000](https://issues.apache.org/jira/browse/FINERACT-2000) (fixed in 1.9.0, PR #3592)
     changed the status after exhausted internal retries from 423 to 409. Per that ticket, callers are expected to retry
     with a NEW idempotency key. So this is NOT a new bug.
   - Design impact: ledger-service must retry 409s (new key per attempt, derived from eventId + attempt) and should
     serialize writes per Fineract account (Kafka key = account id) to avoid causing the contention itself.
   - Still unchecked (only these could be new upstream material): (a) 409 body is empty - confirm whether JIRA covers it;
     (b) is the retry count/backoff configurable and documented; (c) does a failed keyed request poison that key for later retries.
