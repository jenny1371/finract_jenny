# Draft: dev list post for FINERACT-2485 (NOT sent)

James Dailey replied on FINERACT-2485 (2026-10-06): "Please take to dev list". Send to dev@fineract.apache.org
(subscribe first: dev-subscribe@fineract.apache.org). Send it yourself from your own mail account.

Subject: [FINERACT-2485] Idempotency key identity: behaviour for same key + different payload / different resource

Hi all,

On FINERACT-2485 James asked me to bring this to the list. While testing idempotency on current develop (4684c66)
I scripted calls to POST /savingsaccounts/{id}/transactions?command=deposit with an Idempotency-Key header
(script: scripts/probe-fineract-idempotency-edge-cases.ps1 in https://github.com/jenny1371/finract_jenny, to be attached).

Observed:
1. Same key, different amount (1 then 2): HTTP 200, only the first is posted, the response is the first one.
2. Same key on a different account (A then B): HTTP 200 for B, B's balance unchanged, the response contains A's savingsId/clientId.
3. Failed request (400) then a corrected retry with the same key: HTTP 400 again, empty body, nothing posted.
4. A 1000-character key returns 403 (400 would be expected).

Cause: CommandSourceService.findCommandSource matches on (actionName, entityName, idempotencyKey) only, so the target
resource and the payload are not part of the identity.

Question for the list: should a repeated key with a different resource or payload be answered with 409/422 instead of
replaying the first response, and should the identity include the HTTP method and resource path? I am happy to contribute
a parameterized integration test for these cases once the intended behaviour is agreed. This is a data-integrity
question, not a security report.

Thanks,
Jenny Liu (jennyliu on JIRA, jenny1371 on GitHub)
