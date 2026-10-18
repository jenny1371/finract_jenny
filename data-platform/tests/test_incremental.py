"""Incremental staging: a run that reads only the new files must give exactly what a full rebuild gives, apply updates
and deletes that arrive later, and never let a stale redelivered event move a row backwards."""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_pipeline_on_fixture import AS_OF, P, ROOT, Lake, ledger, payment  # noqa: E402

FIRST = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)      # when the first files were written
SECOND = datetime(2026, 10, 18, 9, 0, tzinfo=timezone.utc)      # a day later: far outside the 15 minute lookback
THIRD = datetime(2026, 10, 19, 9, 0, tzinfo=timezone.utc)       # and another day later
LATE_ID = "00000000-0000-0000-0000-00000000000a"

PAY = ("payments__payments", "cdc.payments.public.payments")
LED = ("ledger__ledger_entries", "cdc.ledger.public.ledger_entries")
TXN = ("fineract_default__m_savings_account_transaction", "cdc.fineract_default.public.m_savings_account_transaction")
NOTE = ("fineract_default__m_note", "cdc.fineract_default.public.m_note")
ACCT = ("fineract_default__m_savings_account", "cdc.fineract_default.public.m_savings_account")
ORD = ("orders__orders", "cdc.orders.public.orders")


def dbt_build(lake: Path, warehouse: Path, *extra: str) -> subprocess.CompletedProcess:
    env = {"WAREHOUSE_PATH": str(warehouse), "PATH": os.environ["PATH"], "SYSTEMROOT": os.environ.get("SYSTEMROOT", "")}
    return subprocess.run([sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", ".", "--no-use-colors",
                           *extra, "--vars", json.dumps({"lake_path": str(lake), "as_of": AS_OF})],
                          cwd=ROOT / "dbt", env=env, capture_output=True, text=True)


def deposit(lake: Lake, txn_id: int, payment_name: str, cents: int):
    lake.event(*TXN, str(txn_id), "c", {"id": txn_id, "savings_account_id": 1, "transaction_type_enum": 1,
                                        "is_reversed": False, "transaction_date": 20743, "amount": f"{cents / 100:.6f}"})
    lake.event(*NOTE, str(txn_id), "c", {"id": txn_id, "savings_account_transaction_id": txn_id,
                                         "note": f"payment {P[payment_name]}"})


def state(warehouse: Path, sql: str):
    return duckdb.connect(str(warehouse), read_only=True).execute(sql).fetchall()


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    work = tmp_path_factory.mktemp("incremental")
    lake, incremental = Lake(work), work / "incremental.duckdb"

    # first files: three payments, one deposit for each of two of them
    lake.event(*ORD, "10000000-0000-0000-0000-000000000000", "c", {"id": "10000000-0000-0000-0000-000000000000",
               "merchant_id": "m", "payment_id": None, "amount": 1, "currency": "USD", "status": "COMPLETED",
               "created_at": "2026-10-17T10:00:00.000000Z"})
    lake.event(*ACCT, "1", "c", {"id": 1, "client_id": 1, "currency_code": "USD", "status_enum": 300})
    lake.event(*PAY, P["clean"], "c", payment("clean", 1000))
    lake.event(*PAY, P["updated"], "c", payment("updated", 6000))
    lake.event(*PAY, P["failed"], "c", payment("failed", 700, status="FAILED"))
    lake.event(*LED, P["clean"], "c", ledger("clean", 1000))
    deposit(lake, 1, "clean", 1000)
    deposit(lake, 2, "updated", 6000)
    lake.write("batch-1", ingested_at=FIRST)
    first = dbt_build(work, incremental)

    # a day later: an update, a delete, a new payment, and the very first event of "updated" delivered AGAIN
    lake.event(*PAY, P["updated"], "u", payment("updated", 6500), at="2026-10-18T08:00:00", lsn=900_000)
    lake.event(*PAY, P["failed"], "d", before=payment("failed", 700, status="FAILED"), at="2026-10-18T08:01:00", lsn=900_001)
    lake.event(*PAY, LATE_ID, "c", {**payment("clean", 2000), "id": LATE_ID, "created_at": "2026-10-18T08:00:00.000000Z"},
               at="2026-10-18T08:00:00", lsn=900_002)
    lake.event(*PAY, P["updated"], "c", payment("updated", 6000), at="2026-10-17T10:00:00", lsn=1_001)   # stale, older lsn
    lake.write("batch-2", ingested_at=SECOND)
    second = dbt_build(work, incremental)

    # an unrelated file an hour later moves the table's watermark past the batch that holds the update, so the next
    # incremental window no longer contains it
    other_id = "00000000-0000-0000-0000-00000000000b"
    lake.event(*PAY, other_id, "c", {**payment("clean", 100), "id": other_id, "created_at": "2026-10-18T09:00:00.000000Z"},
               at="2026-10-18T09:00:00", lsn=900_010)
    lake.write("batch-2b", ingested_at=SECOND.replace(hour=10))
    assert dbt_build(work, incremental).returncode == 0

    # another day later a file arrives that holds ONLY a stale redelivery of the first event of "updated". Within one
    # file the newest change per key wins anyway; here the newer row was processed in an earlier run and is not in this
    # window, so only the guard in the staging macro (a row is replaced only by a newer change) keeps the row from
    # going back to 6000.
    lake.event(*PAY, P["updated"], "c", payment("updated", 6000), at="2026-10-17T10:00:00", lsn=1_002)
    lake.write("batch-3", ingested_at=THIRD)
    third = dbt_build(work, incremental)

    full = work / "full.duckdb"                                 # the same lake, built from scratch
    rebuilt = dbt_build(work, full)
    return work, incremental, full, first, second, third, rebuilt


def test_all_builds_pass(runs):
    for result in runs[3:]:
        assert result.returncode == 0, result.stdout[-2500:]


def test_update_delete_and_new_row_are_applied_and_the_stale_redelivery_is_ignored(runs):
    _, incremental, *_ = runs
    amounts = dict(state(incremental, "select payment_id::varchar, amount_cents from stg_payments"))
    assert amounts[P["updated"]] == 6500            # neither stale 6000 redelivery won, in the same file or a later one
    assert P["failed"] not in amounts               # deleted
    assert amounts[LATE_ID] == 2000                 # new
    assert P["clean"] in amounts
    found = {(p, t) for p, t in state(incremental, "select payment_id::varchar, type from recon_discrepancies")}
    assert (P["updated"], "AMOUNT_MISMATCH") in found      # 6500 paid, 6000 booked
    assert (LATE_ID, "MISSING_IN_LEDGER") in found


def test_incremental_result_equals_a_full_rebuild(runs):
    _, incremental, full, *_ = runs
    for table, columns in [("stg_payments", "payment_id, amount_cents, status"),
                           ("stg_ledger_entries", "payment_id, amount_cents, status"),
                           ("stg_fineract_transactions", "transaction_id, amount, is_reversed"),
                           ("recon_discrepancies", "payment_id, type, amount_at_risk_cents"),
                           ("fct_daily_transaction_volume", "day, captured_count, captured_cents"),
                           ("fct_daily_reconciliation", "day, payments_checked, payments_with_difference")]:
        sql = f"select {columns} from {table} order by all"
        assert state(incremental, sql) == state(full, sql), table


def test_old_files_are_not_read_again(runs):
    """Delete the first payments file, as a retention job might, and run again: a full rebuild would lose those rows,
    an incremental run keeps them because it only reads files newer than what it already processed."""
    work, incremental, *_ = runs
    first_file = next((work / "raw" / "payments__payments").rglob("batch-1.parquet"))
    first_file.unlink()
    assert dbt_build(work, incremental).returncode == 0
    ids = {p for (p,) in state(incremental, "select payment_id::varchar from stg_payments")}
    assert {P["clean"], P["updated"], LATE_ID} <= ids
