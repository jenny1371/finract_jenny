# Draft (NOT posted): JIRA ticket and PR for the negative offset 500

JIRA: https://issues.apache.org/jira/secure/CreateIssue!default.jspa  (Project Apache Fineract, Type Bug, Priority Major, Component Client or leave empty, Affects Version 1.16.0 or Unknown; everything else empty; switch the Description editor to "Text").

Summary:
```
GET list endpoints answer HTTP 500 for a negative offset (/clients, /savingsaccounts, /loans)
```
Description:
```
Measured on current develop (3f9f2a2), PostgreSQL.

GET /fineract-provider/api/v1/clients?offset=-5&limit=5 answers HTTP 500 (body: {"status":500,"error":"Internal Server Error"}). The same happens for /savingsaccounts?offset=-1&limit=5 and /loans?offset=-1&limit=5. offset=0, a positive offset and no offset work.

Cause: the offset is put into the SQL as "limit 5 offset -5". The database rejects it (DataIntegrityViolationException) and nothing maps it to a client error. The three endpoints share SearchParameters (and PaginationParameters for others), so the fix is in one place.

Proposal: SearchParameters.getOffset() and PaginationParameters.getOffset() raise a PlatformApiDataValidationException (HTTP 400, validation.msg.pagination.offset.must.not.be.negative) when the offset is negative. With the change the three requests above answer 400; offsets 0, 2 and none are unchanged. Other list endpoints that use these two classes should behave the same; I tested only the three above.
I have a branch with the change and 3 unit tests.
```
After the ticket exists: replace FINERACT-TBD in the commit message with the key, re-sign, force-push the branch and fill the PR form.
Branch: https://github.com/jenny1371/fineract/tree/negative-offset-500
