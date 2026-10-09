# Contributing to Apache Fineract (for credit)

GitHub: jenny1371. Upstream: https://github.com/apache/fineract

## One-time setup
1. Fork apache/fineract to `jenny1371/fineract` (GitHub UI).
2. `git clone https://github.com/jenny1371/fineract.git ../fineract-upstream` (keep it outside this repo).
3. `git remote add upstream https://github.com/apache/fineract.git`
4. Join dev@fineract.apache.org; get an ASF JIRA account (project FINERACT).
5. Read upstream CONTRIBUTING.md: commit messages need the `FINERACT-<n>:` prefix, PRs need green CI and `./gradlew spotlessApply`.

## Workflow for each finding
1. Reproduce with a minimal case + k6 numbers (before/after) from `perf/`.
2. Search JIRA for an existing issue; otherwise open one with repro, environment and metrics.
3. Branch `FINERACT-xxxx-short-desc` off upstream/develop, add a failing test first, fix, open PR.
4. Put the JIRA key in the commit message and PR title. Respond to review promptly.
5. Record the result in `docs/fineract-findings.md` (symptom, root cause, PR link, numbers).

Good first targets while learning: "good first issue" JIRA tickets, flaky tests, docs;
then performance findings from the load test (slow journal-entry queries, missing indexes, N+1).
