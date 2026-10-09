# Roadmap (check off as you go)

- [x] 1. Fineract up via docker compose; manual deposit via `scripts/first-deposit.ps1`
- [x] 2. order + payment services, Stripe test payment (manual capture)
- [x] 3. Kafka + ledger-service: booking into Fineract
- [x] 4. Saga compensation, idempotency, outbox, consumer dedup, retry + DLQ, DLQ replay
- [x] 5. Stripe webhooks, per-merchant rate limiting, reconciliation (alert only)
- [ ] 6. Load test with numbers (`perf/`), Grafana dashboards: find the bottleneck (DB pool? consumers? poll interval?)
- [ ] 7. Keycloak JWT (merchant from the token, not a header), admin role for `/admin/**`
- [ ] 8. Auto-repair tier 1 for reconciliation (re-send BookLedger), with dry-run, caps and an audit trail
- [ ] 9. Optional: multi-tenancy (RLS), outbound merchant webhooks, audit log, Terraform/kind, MCP server
- [ ] 10. Upstream: turn findings from load tests into JIRA issues / PRs (see CONTRIBUTING-UPSTREAM.md)

See [DESIGN.md](DESIGN.md) for how each part works and which test covers each failure.
