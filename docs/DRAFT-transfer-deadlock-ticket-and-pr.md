# Draft (NOT posted): JIRA ticket and PR for the crossing-transfer deadlock

Post yourself, in this order: 1) create the JIRA ticket, 2) put its key in the commit message/PR title, 3) open the PR from branch `transfer-lock-order` on your fork.
The commit message still says FINERACT-TBD; it has to be changed to the real key before the PR (git commit --amend, signed).

## JIRA ticket
Type: Bug. Summary:
```
Concurrent account transfers in opposite directions deadlock: 4 of 60 succeed, the others are answered with 409
```
Description:
```
Measured on current develop (3f9f2a2), PostgreSQL, savings account to savings account transfers via POST /accounttransfers.

Test: 10 transfers A->B and 10 transfers B->A started at the same time, 3 rounds. Result: 4 of 60 transfers succeeded, the other 56 were answered with HTTP 409. pg_stat_database reported 56 deadlocks for the run. The PostgreSQL log shows cycles of four sessions, all on
UPDATE "m_savings_account" SET ... WHERE (("id" = ?) AND ("version" = ?))
Money is conserved (the sum of both balances did not change), so this is an availability problem, not a correctness problem.

Cause: a transfer updates its source account first and then the destination account, without locking both in a defined order. Two transfers over the same pair of accounts in opposite directions each hold one row and wait for the other.

Proposal: lock both accounts at the start of the transfer in ascending id order (SELECT ... FOR UPDATE ORDER BY id), whatever the direction. With that change the same test gave 60 of 60 succeeded and 0 deadlocks; 20 transfers in one direction gave 20 of 20 with balances moved by exactly 20; a transfer above the balance is still rejected with 403.

Questions for maintainers: is an explicit pessimistic lock acceptable here, given that the rest of the savings code relies on optimistic locking? I have only tested PostgreSQL and the savings to savings case.
I have a branch with the change and unit tests and the test script.
```

## PR title / body
Title: `FINERACT-XXXX: Lock both accounts in id order in savings-to-savings transfers`
Body: same facts as the ticket (measured table: 4/60 and 56 deadlocks before, 60/60 and 0 after), the files changed (SavingsAccountRepository, SavingsAccountRepositoryWrapper, SavingsAccountAssembler, AccountTransfersWritePlatformServiceImpl), the 4 unit tests, and the PR template checklist as in #6587.
