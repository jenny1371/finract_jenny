# Where could we contribute to Apache Fineract? (evidence-based, 2026-10-06)

Status labels: **VERIFIED** = read in the real source / JIRA / observed on a running Fineract.
**HYPOTHESIS** = plausible, not yet proven. Do not present a hypothesis as a finding.

## What was checked and ruled out (so nobody re-does it)
| Idea | Result |
|---|---|
| "Concurrent writes to one savings account return 409" | VERIFIED real, but by design: [FINERACT-2000](https://issues.apache.org/jira/browse/FINERACT-2000). Not a bug. |
| "No pagination for savings transactions" | WRONG. `GET /savingsaccounts/{id}/transactions/search` has `offset`, `limit`, date and amount filters. |
| "Interest posting is single-threaded" | WRONG. [FINERACT-1442](https://issues.apache.org/jira/browse/FINERACT-1442) parallelised it (1.6.0). |
| "External events are lost when Kafka fails" | WRONG. The producer waits for all acks (`allOf.get(timeout)`), failure throws, events stay `TO_BE_SENT` and are re-sent (at-least-once). Correct design. |

## Candidates, mapped to the kind of CV line you want

### A. Idempotency hardening: join FINERACT-2485 as the tester   (CV: "Redis cache", reliability)
- VERIFIED: [FINERACT-2485](https://issues.apache.org/jira/browse/FINERACT-2485) "standardize and harden idempotency" is In Progress,
  owned by a committer, coding "substantially complete", design uses `INSERT ... ON CONFLICT DO NOTHING`, an L1 Caffeine + L2 Redis cache, and a servlet filter.
- Do NOT start a competing implementation. Do contribute: concurrency and fault-injection tests (duplicate key under
  parallel requests, failed request then retry with the same key, key reuse with a different body, Redis down),
  and review comments with reproducible results. We already have the harness (`scripts/probe-fineract-idempotency.ps1`,
  the fault-injection pattern in `OrderResilienceTest`).
- Open question to test (HYPOTHESIS): does a failed keyed request "poison" the key for a corrected retry?
- Effort: low-medium. Chance of being accepted: high (tests and review are welcome). Needs coordination with the owner first (JIRA comment).

### B. External-events sending throughput   (CV: "event pipeline throughput", Uber-style)
- VERIFIED on a running Fineract (default image): job "Send Asynchronous Events" runs `0 0/1 * * * ?` (every minute);
  `external-event-batch-size` = 1000; the tasklet does ONE batch per run then returns `FINISHED`. Ceiling with defaults:
  about 1000 events/min = 17/s = 1.4M/day. Event selection is `findByStatusOrderByBusinessDateAscIdAsc` with no row lock.
- VERIFIED: tuning knobs exist since [FINERACT-2066](https://issues.apache.org/jira/browse/FINERACT-2066) (batch partition size, max 25K for PostgreSQL).
- HYPOTHESIS: draining the backlog inside one run (repeat while a full batch was read) would raise the ceiling without config changes;
  a multi-instance setup could send duplicates (consumers must dedupe by the message idempotency key; Fineract provides one).
  It may be intentional (bounded job duration), so the first step is a benchmark, not a PR:
  enable events + Kafka, create N deposits, measure drain rate for default vs tuned settings.
- Deliverable if it holds: benchmark harness + sizing documentation, and possibly a small change. Effort: medium. Needs a local Fineract build for the code change.

### C. Small, confirmed-open issues: fastest way to learn the contribution process   (CV: merged PR, credibility)
All three were Open in JIRA when checked (VERIFIED from the JIRA API):
- [FINERACT-1997](https://issues.apache.org/jira/browse/FINERACT-1997): failed requests inside a Batch API request are not logged in the audit trail (bug, bounded scope).
- [FINERACT-2850](https://issues.apache.org/jira/browse/FINERACT-2850): data validators report errors against the wrong parameter name (bug, small).
- [FINERACT-2827](https://issues.apache.org/jira/browse/FINERACT-2827): OpenAPI gaps for Admin Products (documentation-level).
Check each for an assignee and recent comments before starting. Effort: low. Chance of merge: good for the first one or two.

### D. Transaction latency   (CV: "reduced transaction latency by X%")
- Observed on OUR laptop under load: deposit about 214 ms average. HYPOTHESIS that this is mostly environment (shared CPU/RAM, one JVM, one Postgres).
  Not isolated from our own stack yet. Step 1: k6 directly against Fineract on several accounts; step 2: enable SQL logging / `pg_stat_statements`
  and count queries per deposit; look for redundant round trips. Payoff could be large, success is uncertain, effort high (needs a Fineract build and profiling).

### E. Not worth it now
Cloud/Terraform reference deployments (low code value for ASF), "hot account 409 with empty body" (unverified on current `develop`; reproduce first).

## Suggested order
1. C (one small PR, learn the workflow: fork, JIRA key, `spotlessApply`, review cycle).
2. A (comment on FINERACT-2485 with our reproducible probes; offer tests).
3. B (benchmark first; decide from numbers).
4. D only if B or the load test points at Fineract itself.

## Practical constraints
- Building Fineract needs Gradle and several GB of RAM; this machine is tight (WSL limited to 8 GB). Plan for it or use a cloud dev environment.

## Verification log, 2026-10-06 (why "check the code, not just JIRA" matters)
JIRA status was unreliable for almost every "beginner-friendly" candidate. Each was checked against `develop` (checkout `4684c66`, 2026-10-02) and GitHub PRs:

| Issue | JIRA says | Reality |
|---|---|---|
| FINERACT-2689 NPE closing withholding-tax deposit | Open, unassigned | Already fixed and merged (PR #6126, 2026-07-16) |
| FINERACT-2594 FK violation in migration | Open, unassigned | Already fixed (duplicate of FINERACT-2595) |
| FINERACT-2665, 2683, 2443 | Open | Already merged (#6059, #6120, #6186) |
| FINERACT-2688, 2682, 2048, 1183, 2639, 2828, 2830, 2832 | Open, unassigned | Open PRs exist |
| FINERACT-2827 OpenAPI gaps (admin products) | Open, unassigned | Mostly already fixed; PR #6453 open for the rest |
| FINERACT-1997 failed Batch API requests not audited | Open, unassigned | **Still reproducible with `enclosingTransaction=true`** (no audit row at all), but the fix was merged (#4791), reverted (#4845), re-attempted (#4847, never merged). Not a first PR. |
| FINERACT-2829, 2831, 2833 OpenAPI gaps | Open, unassigned | No PR found. 2829 checked below. |

### FINERACT-1997 reproduction (real Fineract, same code as the checkout)
`POST /batches` with two requests (one valid, one failing):
- without `enclosingTransaction`: both are in `m_portfolio_command_source` (the failure with `status=5`). Fine.
- with `?enclosingTransaction=true`: the whole batch is rolled back and **no audit row is written at all**, not even for the failed request.
Likely cause (not confirmed in code): the audit insert shares the rolled-back transaction; a fix needs the failure audit written in a separate transaction.

### FINERACT-2829 (chosen first PR): the issue text is slightly wrong
Issue says `GetRolesResponse` lacks `status` for `/system/roles-and-permissions`. In reality that path does not exist and the API returns `disabled`:
`GET /roles`, `/roles/{id}` and `/roles/{id}/permissions` all return `"disabled": false|true`, but `RolesApiResourceSwagger` declares only `id`, `name`, `description` (and `permissionUsageData`).
Fix prepared in the checkout (not yet committed): add `Boolean disabled` to `GetRolesResponse`, `GetRolesRoleIdResponse`, `GetRolesRoleIdPermissionsResponse`.
