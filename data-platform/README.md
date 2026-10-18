# Data platform: change data capture, batch reconciliation, metrics

Everything the payment platform writes (orders, payments, ledger entries, and Fineract's own ledger tables) is captured
from the database logs, kept in a lake, reconciled as a scheduled batch job with data-quality checks, and turned into
daily metrics. The services are not touched: they do not know any of this exists.

```
Postgres (orders, payments, ledger)  ─┐                                     ┌─> recon_discrepancies
Fineract Postgres (savings tx, notes) ─┤  Debezium ─> Kafka ─> lake_writer ─> Parquet ─> dbt (DuckDB) ─┼─> fct_daily_reconciliation
                                       │  (WAL, pgoutput)     (raw layer)             staging → marts  └─> fct_daily_transaction_volume
                                       └─ orchestrated by Dagster: capture → dbt build (models + tests), daily at 02:00 UTC
```

| Part | Where | What it does |
|---|---|---|
| CDC | [`../docker-compose.cdc.yml`](../docker-compose.cdc.yml), [`cdc/`](cdc/) | Debezium 2.7 on Kafka Connect, 4 connectors (one per source database: orders, payments, ledger, Fineract), `wal_level=logical` on both Postgres. Topics `cdc.<db>.public.<table>`. Money is sent as exact strings, never floats. |
| Raw layer | [`lake_writer/`](lake_writer/writer.py) | Drains the topics into Parquet, partitioned by day of the change. Offsets are committed only for rows already on disk (at-least-once); files appear atomically. |
| Transform and quality | [`dbt/`](dbt/) | dbt on DuckDB reading the Parquet files. Staging turns change events into current state (de-duplicates redelivered records, newest change wins, deletes disappear). 10 models, 35 tests. |
| Batch reconciliation | [`dbt/models/marts/recon_discrepancies.sql`](dbt/models/marts/recon_discrepancies.sql) | The four differences of reconciliation-service (`MISSING_IN_LEDGER`, `MISSING_IN_STRIPE`, `AMOUNT_MISMATCH`, `DUPLICATE_BOOKING`) with the same grace period, computed over the whole lookback window in SQL. Only finds differences, never fixes. |
| Metrics | [`dbt/models/marts/`](dbt/models/marts/) | `fct_daily_transaction_volume` (captured volume per day and currency, and how much reached the ledger), `fct_daily_reconciliation` (payments checked, differences by type, amount at risk, difference rate). |
| Orchestration | [`orchestration/`](orchestration/payments_pipeline/definitions.py) | Dagster: `raw_cdc_lake` then the dbt assets, every dbt test as an asset check, a schedule at 02:00 UTC. A blocking check stops the run when a Debezium connector is not running. |

## What was run, and what came out

Everything below ran on one laptop against real Postgres 16, Kafka 3.9, Debezium 2.7, and Fineract (the same containers as
the platform), with the demo data written by [`scripts/seed_demo.py`](scripts/seed_demo.py): about 60 payments over 7
days, 2 merchants, with differences planted on purpose. This is a correctness test, not a load test: no throughput claim
is made.

| Test | Result |
|---|---|
| Debezium connectors started against the real databases | all `RUNNING` (the run was made with a 5th connector for the reconciliation-service tables; it was dropped from the repo because nothing reads those tables yet) |
| Capture of the demo data | 448 change events in the first drain: payments 49, orders 49, ledger 47, Fineract transactions 148, notes 48, accounts 107; a second drain found nothing new |
| **Batch reconciliation vs the planted truth** ([`verify_expected.py`](scripts/verify_expected.py)) | exact: 2 `MISSING_IN_LEDGER` (one is a deposit that was undone in Fineract), 1 `AMOUNT_MISMATCH`, 1 `DUPLICATE_BOOKING`, 1 `MISSING_IN_STRIPE`; the payment inside the grace period not reported; daily volume correct for all 6 planned days |
| New activity after the first run | payments 49 → 58, differences updated on the next run |
| `UPDATE` in the source database (payment amount 10000 → 10500) | next run reports `AMOUNT_MISMATCH` (`Stripe 10500 USD vs ledger 10000 USD`); updating it back, the difference disappears |
| `DELETE` of a payment that a ledger entry and an order still reference | the run **fails** on the `relationships` tests; nothing is published |
| Debezium stopped, a change made meanwhile | the run **fails** at the blocking check `all_connectors_running`, dbt does not run; after Debezium restarts it resumes from its replication slot and the change appears (nothing lost) |
| Same run repeated | same result (idempotent) |
| Offline tests ([`tests/`](tests/)), 15 | pass: lake_writer parsing and file writing; the real dbt project on a synthetic lake with a redelivered Kafka record, an update, a reversed deposit, a duplicate booking, a payment inside the grace period, and a negative amount that must stop the build |

The Dagster run evaluates 33 asset checks: 32 dbt tests and the connector check. The other 3 dbt tests span several tables
(amounts positive, daily volume adds up, freshness), so Dagster does not attach them to one asset; they still run inside
`dbt build` and a failure stops the run (shown by an offline test that feeds a negative amount into the lake). The dbt part
of a run takes about 2 s at this size.

Sample of the metrics layer on the demo data ([`scripts/metrics_report.py`](scripts/metrics_report.py)):

```
Daily reconciliation (payments vs Fineract ledger)
day          checked with diff missing amount  dup no pay   at risk diff rate
2026-10-12         8         1       1      0    0      0     25.00     12.5%
2026-10-13         8         1       0      1    0      0      1.00     12.5%
2026-10-14         8         1       0      0    1      0      7.99     12.5%
2026-10-15        11         1       1      0    0      2     73.99      9.1%
```

## Data-quality checks

- Staging: unique and not-null keys; amounts present; currency in the supported list; ledger `payment_id` unique and
  existing in payments (`UNIQUE(payment_id)` is what makes re-emitted events harmless); order → payment link (warning).
- Marts: difference type in the four known values; one row per (payment, type); difference rate between 0 and 1 and never
  more differences than payments; **the daily volume adds up to the captured payments it was built from**; amounts
  positive everywhere.
- Capture: all Debezium connectors running (blocking). The newest change in the lake not older than 24 h (warning).

## Run it

```bash
# 1. Platform databases, Kafka, Debezium, Fineract (the services apply their own Flyway migrations when they start;
#    without them, apply services/*/src/main/resources/db/migration/V*.sql to the orders, payments, ledger databases)
docker compose -f docker-compose.yml -f docker-compose.infra.yml -f docker-compose.cdc.yml up -d postgres kafka fineract-db connect fineract
python data-platform/cdc/register.py            # needs FINERACT_DB_PASSWORD (from .env) in the environment

# 2. Python environment
python -m venv data-platform/.venv && data-platform/.venv/bin/pip install -r data-platform/requirements.txt   # Scripts/ on Windows

# 3. Demo activity with planted differences, then the pipeline
python data-platform/scripts/seed_demo.py --expect data-platform/.demo-expected.json
cd data-platform/orchestration && PYTHONPATH=. python ../scripts/run_pipeline.py     # capture -> dbt build -> checks
python ../scripts/metrics_report.py --warehouse ../warehouse/payments.duckdb
python ../scripts/verify_expected.py --expect ../.demo-expected.json --warehouse ../warehouse/payments.duckdb   # pin the clock first: dbt build --vars "{as_of: '<as_of in the json>'}"

# Dagster UI, schedule and run history
dagster dev -m payments_pipeline.definitions
```

Offline tests, no Docker: `python -m pytest data-platform/tests`.

## Decisions and limits

- **The warehouse is DuckDB over Parquet files**, because that is something this laptop can really run and test. The task
  mentions BigQuery, Snowflake or Iceberg: moving there means loading the same raw layer into that system and changing the
  dbt adapter; the models use a few DuckDB functions (`json_extract_string`, `read_parquet`, `distinct on`). **That move has
  not been done or tested here.**
- **"Stripe" in the reconciliation is the `payments` table**, the platform's own record of what Stripe confirmed. The
  online reconciliation-service still compares with Stripe's API directly; this batch job is the second, independent check
  over history and does not call Stripe.
- **Coupled to Fineract's table layout** (`m_savings_account_transaction`, `m_note`, `m_savings_account`) and to the note
  `payment <id>` that ledger-service writes. A Fineract upgrade that changes those tables breaks staging; the dbt tests
  would say so.
- **Dates:** the raw layer is partitioned by the day of the change, not by business date. Business dates come from the
  columns (`created_at`, `transaction_date`).
- **At-least-once capture** is made safe by de-duplicating on (topic, partition, offset) in staging. A bug found while
  testing: an early version committed Kafka offsets for records it had not written; fixed to commit only what is on disk.
- Debezium's JSON converter must have schemas disabled in the connector config; the Debezium image ignores the
  environment variables for this. The writer also accepts the wrapped form.
- **Not done:** schema evolution handling, backfills of history from before capture started (the connectors take an initial
  snapshot of each table when first registered), PII handling, retention of the raw layer, performance at volume, an Airflow
  version of the DAG.
