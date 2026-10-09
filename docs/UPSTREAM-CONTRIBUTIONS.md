# Contributions prepared for Apache Fineract

Status as of 2026-10-09: PR #6587 (FINERACT-2829) is merged; PRs #6590 (FINERACT-2900) and #6591 (FINERACT-2902) are open. The state column in the table below and the sections after it were written earlier and may lag behind.
Each item says what is ready, what evidence backs it, and what is still missing.
Facts were checked against `apache/fineract` `develop` at commit `4684c66` (2026-10-02) and a running instance of the
image built from it. Read `CONTRIBUTING.md` first: JIRA key in the PR title, `./gradlew spotlessApply`.

| # | Contribution | Evidence | State |
|---|---|---|---|
| 1 | FINERACT-2829: document the `disabled` field in the roles OpenAPI responses | API returns it, OpenAPI omits it | **PR opened: https://github.com/apache/fineract/pull/6587** (2026-10-06); build and format check passed locally; waiting for CI and review |
| 2 | FINERACT-2485: reproducible evidence for idempotency edge cases that the design leaves unspecified | scripted, run on a real instance, root cause located in code | **comment posted** (2026-10-06) |
| 3 | FINERACT-1997: reproduction of failed Batch API requests missing from the audit trail with `enclosingTransaction=true` | scripted steps and observed rows | ready to post as a JIRA comment |
| 4 | External events: throughput ceiling of the "Send Asynchronous Events" job | measured: 1000 events/min with defaults, 7,488 events in 1.1 s with batch size 10000 | **ticket FINERACT-2900 created** (2026-10-06); change + 4 unit tests on branch `events-drain-batches` (pushed to the fork, no PR yet); before/after benchmark of a built image pending |
| 5 | Command retry without jitter under account contention | measured 65 % failures at 50 deposits/s over 10 accounts; effect of jitter is a hypothesis | needs a custom build to test |

---
## 2. FINERACT-2485: idempotency behaviour that is currently undefined (and what it does today)

**Method.** `scripts/probe-fineract-idempotency-edge-cases.ps1` against `apache/fineract` built from `develop`
(`4684c66`), PostgreSQL, `POST /savingsaccounts/{id}/transactions?command=deposit` with the `Idempotency-Key` header.

| Scenario | Observed | Why it matters |
|---|---|---|
| Same key, same body, twice | one posting, second call replays (HTTP 200, same `resourceId`) | correct |
| Same key, **different amount** (1, then 2) | HTTP 200, **only 1 posted**, response is the first call's | the caller believes 2 was recorded |
| Same key on a **different account** (A, then B) | HTTP 200 for B, but **B's balance did not change**; the body is A's response (A's `savingsId`, `clientId`) | a payment is silently dropped and another resource's identifiers are returned |
| Failed request (400), same key, corrected body | HTTP 400 again, nothing posted, empty body | consistent with FINERACT-2000 ("use a new key"), but the replayed failure has no body |
| 1000-character key | HTTP 403 | looks like a width problem (see #6566); 400 would be expected |
| Empty key header value | treated as no key (two postings) | acceptable, worth documenting |

**Root cause in the code.** `CommandSourceService.findCommandSource` looks up
`findByActionNameAndEntityNameAndIdempotencyKey(wrapper.actionName(), wrapper.entityName(), idempotencyKey)`: the
identity of a request is (action, entity type, key). It contains neither the target resource nor anything about the
payload, so any DEPOSIT on any SAVINGSACCOUNT with the same key is "the same request".

**Draft comment for FINERACT-2485 (paste after the JIRA account is approved):**

> Thanks for working on this. While testing idempotency against current `develop` (4684c66) I found several behaviours
> the design does not define yet, which could be turned into acceptance tests. Method: scripted calls to
> `POST /savingsaccounts/{id}/transactions?command=deposit` with an `Idempotency-Key` header; script attached.
>
> 1. Same key, different amount (1 then 2): HTTP 200, only the first is posted, the response is the first one.
> 2. Same key on a different account (A then B): HTTP 200 for B, B's balance unchanged, the response contains A's
>    `savingsId`/`clientId`.
> 3. Failed request (400) then a corrected retry with the same key: HTTP 400 again, empty body, nothing posted.
> 4. A 1000-character key returns 403 (400 would be expected; possibly related to #6566).
>
> Cause: `findCommandSource` matches on (actionName, entityName, idempotencyKey) only, so the target resource and the
> payload are not part of the identity. The design in this ticket uses (idempotency_key, tenant_id); I would like to
> suggest defining what happens for 1 and 2 explicitly, for example: include the HTTP method and resource path in the
> identity, and answer 409/422 when the same key arrives with a different resource or a different payload fingerprint
> (a canonical-JSON or key-field hash avoids the attribute-ordering problem mentioned earlier). Failed requests could
> replay the stored error body. I am happy to contribute a parameterized integration test covering these cases
> (including parallel requests with one key and a key reused across accounts) if that would help; please tell me where
> you would like it, so I do not conflict with the ongoing work.

*Note on framing:* the project's `SECURITY.md` models callers as authenticated back-office users, and asks that
security reports be private. This is presented as a data-integrity gap in a public design ticket, not as a
vulnerability. Decide before posting whether you prefer to ask the PMC privately first.

---
## 3. FINERACT-1997: failed Batch API requests and the audit trail

**Reproduction (real instance, same code as `4684c66`).** `POST /batches` with two sub-requests: a valid deposit and a
deposit to a non-existent account (a validation error gives the same result).

| Variant | `m_portfolio_command_source` rows |
|---|---|
| no `enclosingTransaction` | both written; the failed one with `status = 5` |
| `?enclosingTransaction=true`, second request fails | **none**, not even for the failed request (the batch is rolled back) |

**History (checked on GitHub).** PR #4791 (fix, merged) was reverted by #4845; #4847 (a second attempt, 334 additions)
was closed without merging. Hypothesis, not verified in code: the audit insert shares the transaction that is rolled
back, so a fix has to persist the failure audit in a separate transaction.

**Draft comment for FINERACT-1997:**

> I could still reproduce this on current `develop` (4684c66, PostgreSQL). `POST /batches` with two requests, the second
> failing: without `enclosingTransaction` both appear in `m_portfolio_command_source` (the failed one with status 5);
> with `?enclosingTransaction=true` there is no row at all, because the whole batch is rolled back, so the failed
> request leaves no audit trace. Related earlier work: #4791 (merged), #4845 (revert), #4847 (closed unmerged). My
> guess is that the audit insert participates in the rolled-back transaction and would need its own transaction.
> I am not claiming the ticket; I wanted to record that the reproduction is current and how to trigger it. Could a
> maintainer say whether the revert reason is documented anywhere?

---
## 1. FINERACT-2829: `disabled` missing in the roles OpenAPI responses
The issue text says `GetRolesResponse` lacks `status` for `/system/roles-and-permissions`. That path does not exist
and the API has no `status`; `GET /roles`, `/roles/{id}` and `/roles/{id}/permissions` all return `"disabled": true|false`
(from `RoleData`/`RolePermissionsData`), while `RolesApiResourceSwagger` declares only `id`, `name`, `description`.
Change: add `@Schema(example = "false") public Boolean disabled;` to `GetRolesResponse`, `GetRolesRoleIdResponse` and
`GetRolesRoleIdPermissionsResponse`.

**Commit / PR title:** `FINERACT-2829: Document disabled field in roles OpenAPI responses`

**PR description draft.** The issue reports a missing `status` field for roles. The endpoint named there does not exist
and the real responses contain `disabled` (boolean), which the OpenAPI classes do not declare. This adds it to the
three roles response classes. Verified against a running instance: `GET /roles`, `/roles/1`, `/roles/1/permissions`.


## 4. External events: sending throughput (MEASURED 2026-10-06)

**Setup.** `apache/fineract` built from `develop` (`4684c66`), PostgreSQL, Kafka producer enabled
(`perf/docker-compose.events-bench.yml`). All 187 event types are disabled by default; `SavingsDepositBusinessEvent`
was enabled. The backlog was created by copying 12,488 existing rows in `m_external_event` as `TO_BE_SENT`
(depositing through the API could not saturate the sender, see "contention" below).

| Configuration | Result |
|---|---|
| Defaults: job `0 0/1 * * * ?`, `external-event-batch-size` = 1000 | exactly **1000 events per run, one run per minute** (steps of 1000 at 31 s, 92 s, 142 s, 203 s, 264 s): about **17 events/s**; the 12.5k backlog would take 12.5 minutes |
| Run duration at the defaults | 0.12-0.28 s per run (job history) |
| Batch size 10000, same cron | the remaining 7,488 events were sent **in one run, 1.08 s** (about 6,900 events/s) |

**Conclusion.** The sender can move thousands of events per second, but with the defaults the job uses well under 1 % of
that: the ceiling comes from "one batch per run" (`SendAsynchronousEventsTasklet` returns `FINISHED` after one batch)
times a one-minute schedule, not from processing cost. Operators must raise the batch size and/or the schedule, which
is not obvious (the tuning knobs exist since FINERACT-2066 but defaults and sizing are not documented).

**Possible contributions, smallest first.**
1. Document the ceiling and sizing guidance (events/day = batch size x runs/day), with this measurement.
2. Let the tasklet keep reading while it gets a full batch (return `RepeatStatus.CONTINUABLE`, bounded by a maximum
   number of batches or run time per execution), so a backlog drains in one run without configuration.
3. Raise the default batch size. Check first whether the one-batch-per-run behaviour is intentional (bounded run time
   per scheduler slot). This needs a code change plus a benchmark of the modified build; not done yet.
Event selection uses no row lock (`findByStatusOrderByBusinessDateAscIdAsc`); duplicate sends are tolerated by
design (at-least-once, each message carries an idempotency key).

**Draft JIRA comment / improvement ticket:**

> While benchmarking external events I measured the throughput ceiling of the "Send Asynchronous Events" job on
> current `develop`. With the defaults (cron every minute, `external-event-batch-size` 1000) exactly 1000 events are
> sent per run, i.e. about 17 events/s or 1.4M/day, although one run only takes 0.12-0.28 s. With batch size 10000 a
> backlog of 7,488 events was sent in a single run in 1.1 s (about 6,900/s). So the ceiling is configuration, not
> capacity: the tasklet reads one batch per execution. Would the project be open to (a) documenting the sizing, and/or
> (b) letting the tasklet continue while full batches are read (bounded by a max number of batches), so the backlog
> drains without tuning? I can contribute the benchmark harness and a PR if there is interest.

## 5. Contention on one account: failures and latency (MEASURED, hypothesis for the fix)
Depositing straight into Fineract at an offered 50 requests/s spread over 10 accounts (5 req/s per account): **65 %
of requests failed, mean latency 5.6 s, p95 8.7 s, only ~13 deposits/s succeeded**. All failures were
`OptimisticLockException` on `SavingsAccount` (the command retry gives up after 3 attempts). The retry
configuration is `max-attempts=3`, `wait-duration=1s`, exponential backoff x2 (1 s, 2 s), built with
`IntervalFunction.ofExponentialBackoff(...)`: deterministic, **no jitter**, and no jitter option exists in
`RetryInstanceProperties`. Requests that collided at the same moment retry at the same moments again.
**Hypothesis (not tested):** randomized backoff (`IntervalFunction.ofExponentialRandomBackoff`) would lower the failure
rate under contention. Test plan: add an optional randomization factor, build an image
(`./gradlew :fineract-provider:jibDockerBuild`), repeat `perf/fineract-deposits.js` at 50/s with and without it, compare
failure rate and latency. Caveat: this laptop was memory constrained, so only the relative change would be meaningful.
## Lessons from preparing these (useful when choosing future issues)
JIRA status was unreliable for almost every "beginner-friendly" candidate (several were already fixed or had open PRs).
Always check the code on `develop` and search GitHub PRs for the JIRA key before starting.

## Measured effect of the FINERACT-2900 change (2026-10-06, custom image built from branch `events-drain-batches`)
Same backlog of 12,000 `TO_BE_SENT` events, default batch size 1000, default cron (every minute), PostgreSQL + Kafka.

| Setting | Result |
|---|---|
| Control: `max-batches-per-run=1` (today's behavior) | exactly 1000 events per run, runs 60 s apart (9 s, 69 s, 129 s): 12,000 events would take 12 minutes. No regression. |
| `max-batches-per-run=20` | all 12,000 events sent in one run in about 4 s (job duration 3.9 s) once the scheduler fired |

Per-batch timing from the tasklet's DEBUG log (1000 events): read 10-15 ms, **message creation 150-300 ms**, Kafka send ~25 ms
(30,000-50,000 msg/s), mark as sent 10-20 ms. Message creation (Avro serialization, one event after another) is about 80 % of the time
and is the next target (parallelising it needs the tenant ThreadLocal context passed to worker threads, as `markEventsAsSent` already does).

## Process lessons
- **All commits must be GPG-signed** (`CONTRIBUTING.md#signing-your-commits`); the "Verify Commit Signatures" check fails otherwise.
  An Ed25519 signing key was created and its public half added to the GitHub account; the private key stays on the local machine.
  In `fineract-upstream`: `commit.gpgsign=true`, `core.autocrlf=false` (Fineract requires LF; spotless fails on CRLF).
- Build needs JDK 25, `VERSIONING_DISABLE=true` for a shallow clone, and about 3 GB of Gradle heap.

## Parallel message creation: tried, does NOT work, abandoned (2026-10-06)

Branch `events-parallel-message-creation` (fork only, never a PR, no JIRA ticket) spread the per-aggregate message creation over the event thread pool. All 13 unit tests passed, but they use mocks.

Run in a real Fineract (custom image, 12,000 SavingsDepositBusinessEvent backlog, 10 distinct aggregates, pool size 8, `max-batches-per-run=20`):

| | parallel off (control) | parallel on |
|---|---|---|
| message creation per 1000-event batch | 87-99 ms | 5-47 ms (looked faster) |
| events sent | 12,000 in 26 s (includes waiting for the cron; MAX_BATCHES=20, parallel off) | **3,000, then the run failed** |

The run failed with `NullPointerException: ... DescriptorEvent.getDescriptor() because "event" is null` from EclipseLink (`UnitOfWork`) inside `messageFactory.createMessage`, so message creation (or its data enrichers) touches EclipseLink state that is not safe to use from these worker threads. The failure handling worked as designed (nothing sent, nothing marked, events stayed TO_BE_SENT), but the feature is broken.

Conclusion: do not propose it. The unit tests with mocks could not show this; only the real-image run did. Message creation is already only about 90 ms per 1000 events, so the real gain was small anyway. The FINERACT-2900 drain change (`max-batches-per-run=20`: 12,000 events sent within one cron slot, about 4 s of job time; default is one batch per minute) stays the contribution.

## Retry jitter on command retries: no measurable gain, not proposed (2026-10-06)

Branch `retry-jitter` in `fineract-upstream` (local only, never pushed): new `fineract.retry.instances.executeCommand.exponential-backoff-jitter`
(default 0 = unchanged), 3 new unit tests pass. Real-instance test with `perf/retry-jitter-bench.sh` (k6 deposits spread over 10 accounts,
60 s, same image, only the jitter differs; one run per setting):

| Load | jitter 0 (today) | jitter 0.5 |
|---|---|---|
| 50 deposits/s | 87.8 % failed, p95 13.5 s | 89.1 % failed, p95 15.1 s |
| 15 deposits/s | 8.5 % failed (75/885), p95 5.6 s | 7.5 % failed (66/885), p95 4.7 s |

All failures were `OptimisticLockException` answered as 409 after the 3 attempts were used up. At 50/s the accounts are simply saturated
(5 deposits/s per account), and jitter cannot fix that. At 15/s the difference (8.5 % vs 7.5 %) is within what one run can show. Conclusion:
the hypothesis "jitter reduces contention failures" is **not supported**; do not open a ticket. The real limit is the serial throughput of
one account row.

## Send Asynchronous Events: failed runs are recorded as COMPLETED (2026-10-06)

Reproduced on develop-equivalent behavior (custom image, `max-batches-per-run=1`): Kafka stopped, 5 events `TO_BE_SENT`, wait for the cron.
Result: the tasklet logs `Error occurred while processing events` and the job run is `COMPLETED`/exit `COMPLETED` (observed for several
consecutive runs; across 734 historical runs none was ever anything but COMPLETED, including the runs that failed during the parallel experiment).
The 5 events stay `TO_BE_SENT` (nothing is lost), but nothing that watches job runs can tell.

Change on branch `events-job-reports-failure` (local only, commit `9314b44`, 3 new unit tests pass): the tasklet sets `ExitStatus.FAILED` with the
exception as description when it caught one, and the failure to mark sent events is raised instead of only logged.
Same test with the change, Kafka stopped: job run `COMPLETED` with **exit code `FAILED`**, step exit `FAILED` with message
`org.springframework.kafka.KafkaException: Send failed ...`; the 5 events are still `TO_BE_SENT`; the run before the failure, with Kafka up, is `COMPLETED/COMPLETED`.
Note: the Spring Batch *status* stays COMPLETED, only the *exit code* is FAILED. Open question for maintainers: is the always-COMPLETED
behavior intentional? Not proposed yet.

## Generated reliability probe (2026-10-06): what was tested, what was found, what was fixed

`scripts/probe-fineract-reliability.sh` runs textbook failure cases against a live Fineract (savings deposits/withdrawals, concurrent). Results, measured:

| Case | Result on develop-equivalent |
|---|---|
| two identical deposits (same amount, same moment), different keys | both posted (+14). No confusion: identity is the key/account, not name or amount |
| same amount on two accounts at once | both posted correctly |
| same Idempotency-Key sent 10 times concurrently | all answered 200, exactly **one** posting (+5): the race is handled |
| two concurrent withdrawals of 75% of the balance (3 rounds) | never overdrawn; in 2 rounds exactly one succeeded; in 1 round both were rejected (403) although one could have succeeded |
| 20 concurrent deposits to one account without key | only 3 of 20 answered 200, 17 answered 409 (optimistic lock after the retries; by design per FINERACT-2000); balance moved by exactly 3 (consistent) |
| overdraft withdrawal, unknown account, bad JSON, empty body, wrong tenant, no auth | clean 4xx, no 5xx |
| two clients with identical name and birth date | both created, different ids |
| **deposit of 0.005** | **HTTP 200 and a transaction of 0.00 was posted** (amount 0 is rejected with 400) |
| **deposit of 10^15** | **HTTP 403 with the raw SQL error "numeric field overflow ... precision 19, scale 6"** (should be 400) |

Fix on branch `savings-amount-validation` (local only, commit `7f5bac6`): reject an amount that is zero after rounding (deposit and withdrawal) and
amounts above 9999999999999 (validator). 6 new unit tests pass. Verified on a rebuilt image: 0.005, withdrawal 0.004, 10^15 and 999999999999999999 now
answer **400** with no balance change; 0.01 and 100.50 are still accepted; re-running the whole probe shows no regression.
Not fixed / not claimed: the 409 under same-account concurrency (design decision, FINERACT-2000), the case where both concurrent withdrawals were rejected (seen once, cause not investigated).

## Reliability: a failed "Send Asynchronous Events" run is recorded as a success (2026-10-06)

Found by reading the code and confirmed on a real instance. `SendAsynchronousEventsTasklet.execute` catches every exception, logs it and returns
`FINISHED`. Kafka was stopped (checked: no kafka container running) and 5 events were put in `TO_BE_SENT`:

| | develop behaviour (image without the change) | with the change (branch `events-job-reports-failure`, commit c40af52) |
|---|---|---|
| Spring Batch `batch_job_execution` | `COMPLETED / COMPLETED` (an earlier variant that only set the exit status gave `COMPLETED / FAILED`) | `FAILED / FAILED`, message `KafkaException: Failed to construct kafka producer` |
| Fineract run history API (`/jobs/36/runhistory`) | `"status":"success"` | `"status":"failed"` with the error message |
| events | 5 stay `TO_BE_SENT` | 5 stay `TO_BE_SENT` (no change, nothing lost) |

After Kafka was started again the next run sent the 5 events (0 `TO_BE_SENT` left) and the run history shows `success` again.
Fineract decides "failed" from the Spring Batch `BatchStatus` (`JobStarter`: it throws when the status is FAILED), so setting only the exit
status is not enough; the tasklet has to rethrow. The same change also stops swallowing a failure to *mark* events as sent (those events were
already sent and will be sent again). 8 unit tests in `SendAsynchronousEventsTaskletTest` pass (3 new).

Open question for maintainers (to put in the ticket, not assume): is the "always COMPLETED" behaviour intentional, for example so the job never
shows as failed every minute while a broker is down?

Mistake worth remembering: while testing, another process switched the `fineract-upstream` checkout to another branch, so one image was built from
the wrong code and gave a misleading result. The verified run used a separate `git worktree` for the branch.

## 20 distinct failure scenarios (2026-10-06): `scripts/probe-fineract-20-cases.sh`

Run against a live instance; "FINDING" = HTTP 5xx or a hang. Measured:

| # | Scenario | Result |
|---|---|---|
| 1-4 | impossible date (29 Feb 2027), date not matching dateFormat, before activation date, far future | all 400, nothing posted |
| 5 | deposit into a not-yet-approved account | 400 |
| 6 | undo the same transaction twice (sequential) | both 200, balance changed once (-1). The 2nd undo reports success though nothing happened; not changed |
| 7 | undo the same transaction 5x concurrently | 3x200 + 2x409, balance changed exactly once |
| 8 | withdraw the whole balance | my expectation was wrong: a backdated (yesterday) withdrawal is checked against the balance *on that date*, "Insufficient account balance" is correct |
| 9 | SQL injection / script / 5000 chars / emoji / format strings in externalId | no 5xx (5000 chars: 400) |
| 10 | odd Idempotency-Keys | keys longer than **50** chars: 403 with the raw SQL error `value too long for type character varying(50)` (column is varchar(50); related to #6566, not touched); a whitespace-only key silently disables idempotency (posted twice) |
| 11 | duplicate JSON key, unknown field, array body, amount as string | 400, 400, 400, 200 (string accepted) |
| 12 | wrong Content-Type | 415 |
| 13 | 3 MB body | 400, server healthy |
| 14 | GET/PUT/PATCH/DELETE on the deposit endpoint | 405 |
| 15 | pagination abuse | **`offset=-5` on /clients, /savingsaccounts, /loans: HTTP 500** (fixed, below) |
| 16 | weird path ids | 4xx |
| 17 | 3 concurrent clients with the same externalId | exactly one created (rows=1), the others 403 (data-integrity mapping) |
| 18 | 20 crossing transfers A<->B | 1-3 succeed, rest 409; money conserved; the DB log shows `deadlock detected` many times (crossing lock order), not fixed |
| 19 | 15 deposits + 15 withdrawals concurrently | balance == sum of the 200s (consistent); 80 % answered 409 under single-account contention |
| 20 | client aborts, retries with the same key (3 rounds) | each posted exactly once |
| 21 | database stopped, then restarted | see below |

Fixes, each verified on a rebuilt image:
- **Negative offset -> 500**: branch `negative-offset-500` (commit `407d8f2`). Now 400 with `validation.msg.pagination.offset.must.not.be.negative` on all three endpoints; offset 0/2 and no offset unchanged; 3 unit tests.
- **Database down -> 500 after 20 s**: branch `db-outage-503` (commits `cd1f36f`, `f55e111`). The exception escapes `TenantAwareBasicAuthenticationFilter` (loading the tenant), before the JAX-RS layer. Now **503 with `Retry-After: 5`** for read and write requests (still after the 20 s pool timeout, not changed); service recovers by itself, first request after the DB is healthy returns 200. 2+2 unit tests. The two JAX-RS mappers in `cd1f36f` were NOT reached in this scenario and are not verified on a real instance (they only matter if the DB drops in the middle of a request).
Also observed while the DB was down: `/actuator/health` did not answer within 30 s (hang instead of DOWN). Not investigated.

## Crossing account transfers deadlock (2026-10-06) - the largest finding so far

`scripts/probe-crossing-transfers.sh` (10 transfers A->B and 10 B->A at the same time, 3 rounds, `POST /accounttransfers`, savings to savings):

| | develop-equivalent | with lock-order change |
|---|---|---|
| transfers succeeded | **4 of 60** (the rest 409) | **60 of 60** |
| DB deadlocks (`pg_stat_database.deadlocks`) | **56** | **0** |
| money conserved | yes | yes |

Postgres log: a cycle of 4 processes on `UPDATE "m_savings_account" ... WHERE id = ? AND version = ?`: each transfer updates its source account first and then waits for the destination.
Fix (branch `transfer-lock-order`, commit `4a015f2`/amended, local only): lock both accounts in ascending id order at the start of the transfer
(`SavingsAccountRepository.findAllLockedOrderedById`, `SavingsAccountRepositoryWrapper.lockInIdOrder`, called from `create()` and `transferFunds()`).
First attempt only patched `transferFunds()`: no effect (55 deadlocks, 5/60), because the API goes through `create()`. Verified after the fix:
20 transfers in one direction 20/20 and balances exactly -20/+20; two independent crossing pairs (4<->5, 6<->7) 40/40, 0 deadlocks; transfer above the balance 403 and unchanged;
missing account 404; 18 related existing unit tests + 4 new ones pass.
Not claimed: the loan-related transfer branches (savings to loan, loan to savings) were not touched or measured; behaviour on MySQL/MariaDB not tested.
