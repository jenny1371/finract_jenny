# What to post on ASF JIRA (copy and paste; log in at issues.apache.org/jira)

JIRA username: jennyliu. Post these yourself with your own login. Order matters: do 1, then 2, then the two new tickets (3, 4).
Keep the tone factual. Do not describe anything as a vulnerability.

---
## 1. Claim FINERACT-2829 (comment, and press "Assign to me" if the button exists)
https://issues.apache.org/jira/browse/FINERACT-2829

```
I'd like to take this. While checking against current develop I found that the response classes do not lack "status": the endpoint named here (/system/roles-and-permissions) does not exist, and GET /roles, /roles/{id} and /roles/{id}/permissions return "disabled" (boolean), which GetRolesResponse, GetRolesRoleIdResponse and GetRolesRoleIdPermissionsResponse do not declare. A 6-line PR that adds it: https://github.com/jenny1371/fineract/tree/FINERACT-2829-roles-openapi-disabled (I will open the pull request now).
```

## 2. Comment on FINERACT-2485 (idempotency edge cases)
https://issues.apache.org/jira/browse/FINERACT-2485 , text is in `docs/UPSTREAM-CONTRIBUTIONS.md`, section 2
("Draft comment for FINERACT-2485"). Attach `scripts/probe-fineract-idempotency-edge-cases.ps1`.

## 3. Create a new ticket: external-event sending throughput
Project: Fineract. Issue type: Improvement. Component: whatever the form offers for "Events" or "Batch jobs", otherwise leave blank.
Summary:
```
Send Asynchronous Events job sends at most one batch per run: default ceiling is 1000 events per minute
```
Description:
```
Measured on current develop (4684c66), PostgreSQL, Kafka producer enabled. SavingsDepositBusinessEvent enabled, a backlog of 12,488 TO_BE_SENT events.

Defaults (job "Send Asynchronous Events" cron 0 0/1 * * * ?, external-event-batch-size = 1000): exactly 1000 events were sent per run, one run per minute, i.e. about 17 events/s or 1.4M events/day. A run takes only 0.12-0.28 s.
With external-event-batch-size = 10000 the remaining 7,488 events were sent in a single run in 1.08 s (about 6,900 events/s).

So the ceiling comes from the configuration, not from processing cost: SendAsynchronousEventsTasklet reads one batch per execution and returns RepeatStatus.FINISHED. Operators have to know to raise the batch size or the schedule.

Proposal: add fineract.events.external.max-batches-per-run (default 1, current behavior unchanged). A run reads up to that many batches, marks each as sent before reading the next, and stops as soon as a batch is not full. A failed send leaves the failed batch TO_BE_SENT and stops the run, as today.

Question for maintainers: is the one-batch-per-run behavior intentional (bounding the run time per scheduler slot)? If not, should the default be raised later?

I have a branch with the change and unit tests and a benchmark harness (docker compose override + k6 script) and will open a PR once there is a ticket number.
```

## 4. Create a new ticket: jitter for command retry (only after the measurement exists; not ready yet)
Do not post until the experiment in `docs/UPSTREAM-CONTRIBUTIONS.md` section 5 has before/after numbers.

## 5. FINERACT-1997 (comment)
Text is in `docs/UPSTREAM-CONTRIBUTIONS.md`, section 3 ("Draft comment for FINERACT-1997").
