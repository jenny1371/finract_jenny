"""Runs the real dbt project over a small synthetic lake and checks the result against facts known in advance.
No Docker needed, so it runs in CI. It covers what the end-to-end run cannot enumerate cheaply: at-least-once delivery
(the same Kafka record twice), updates, deletes, reversed deposits and the grace period."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lake_writer.writer import SCHEMA  # noqa: E402

AS_OF = "2026-10-18T12:00:00+00:00"
P = {name: f"00000000-0000-0000-0000-00000000000{i}" for i, name in enumerate(
    ["clean", "missing", "mismatch", "duplicate", "undone", "updated", "inflight", "stripe_missing", "failed"], start=1)}


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


class Lake:
    """Writes raw change events the way lake_writer does, with one Kafka offset counter per topic."""

    def __init__(self, root: Path):
        self.root, self.offsets, self.rows = root, {}, {}

    def event(self, directory, topic, pk, op, after=None, before=None, at="2026-10-17T10:00:00", lsn=None, offset=None):
        offsets = self.offsets.setdefault(topic, iter(range(10_000)))
        offset = next(offsets) if offset is None else offset
        self.rows.setdefault(directory, []).append({
            "topic": topic, "partition": 0, "offset": offset, "op": op, "ts_ms": ms(at),
            "lsn": lsn if lsn is not None else 1000 + offset, "pk": json.dumps({"id": pk}),
            "before": json.dumps(before) if before else None, "after": json.dumps(after) if after else None,
            "ingested_at": datetime.now(timezone.utc)})

    def write(self, name="part-0", ingested_at=None):
        """One file per table for the events collected so far; `ingested_at` is when the writer 'wrote' it."""
        for directory, rows in self.rows.items():
            if ingested_at is not None:
                for row in rows:
                    row["ingested_at"] = ingested_at
            target = self.root / "raw" / directory / "dt=2026-10-17"
            target.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), target / f"{name}.parquet")
        self.rows = {}


def payment(i, amount, status="CAPTURED", created="2026-10-17T10:00:00.000000Z"):
    return {"id": P[i], "order_id": "10000000-0000-0000-0000-000000000000", "amount": amount, "currency": "USD",
            "status": status, "stripe_intent_id": "pi_" + i, "created_at": created, "updated_at": created}


def ledger(i, amount, status="BOOKED"):
    return {"id": "20000000-0000-0000-0000-0000000000" + str(abs(hash(i)) % 90 + 10), "payment_id": P[i],
            "merchant_id": "m", "amount": amount, "currency": "USD", "status": status, "fineract_txn_id": None,
            "created_at": "2026-10-17T10:00:00.000000Z"}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    work = tmp_path_factory.mktemp("lake")
    lake = Lake(work)
    pay, led = ("payments__payments", "cdc.payments.public.payments"), ("ledger__ledger_entries", "cdc.ledger.public.ledger_entries")
    txn = ("fineract_default__m_savings_account_transaction", "cdc.fineract_default.public.m_savings_account_transaction")
    note = ("fineract_default__m_note", "cdc.fineract_default.public.m_note")
    acct = ("fineract_default__m_savings_account", "cdc.fineract_default.public.m_savings_account")
    orders = ("orders__orders", "cdc.orders.public.orders")

    lake.event(*orders, "10000000-0000-0000-0000-000000000000", "r", {"id": "10000000-0000-0000-0000-000000000000",
               "merchant_id": "m", "payment_id": None, "amount": 1, "currency": "USD", "status": "COMPLETED",
               "created_at": "2026-10-17T10:00:00.000000Z"})
    lake.event(*acct, "1", "r", {"id": 1, "client_id": 1, "currency_code": "USD", "status_enum": 300})

    n = {"txn": 0}

    def deposit(payment_name, cents, reversed_=False):
        n["txn"] += 1
        t = n["txn"]
        lake.event(*txn, str(t), "c", {"id": t, "savings_account_id": 1, "transaction_type_enum": 1, "is_reversed": False,
                                       "transaction_date": 20743, "amount": f"{cents / 100:.6f}"})   # 20743 = 2026-10-17
        lake.event(*note, str(t), "c", {"id": t, "savings_account_transaction_id": t, "note": f"payment {P[payment_name]}"})
        if reversed_:
            lake.event(*txn, str(t), "u", {"id": t, "savings_account_id": 1, "transaction_type_enum": 1, "is_reversed": True,
                                           "transaction_date": 20743, "amount": f"{cents / 100:.6f}"}, at="2026-10-17T11:00:00")
        return t

    for name, cents in [("clean", 1000), ("missing", 2000), ("mismatch", 3000), ("duplicate", 4000), ("undone", 5000),
                        ("updated", 6000), ("inflight", 999)]:
        lake.event(*pay, P[name], "c", payment(name, cents, created="2026-10-18T11:58:00.000000Z" if name == "inflight"
                                                else "2026-10-17T10:00:00.000000Z"))
    lake.event(*pay, P["failed"], "c", payment("failed", 700, status="FAILED"))
    lake.event(*pay, P["updated"], "u", payment("updated", 6500), before=payment("updated", 6000), at="2026-10-17T12:00:00")
    # the same Kafka record delivered twice (at-least-once): must count once
    lake.event(*pay, P["clean"], "c", payment("clean", 1000), offset=0, lsn=1000)

    deposit("clean", 1000)
    deposit("mismatch", 3100)
    deposit("duplicate", 4000), deposit("duplicate", 4000)
    deposit("undone", 5000, reversed_=True)
    deposit("updated", 6000)            # the payment was changed to 6500 afterwards: a real difference
    deposit("stripe_missing", 3300)     # a deposit for which no payment exists
    lake.event(*led, P["clean"], "c", ledger("clean", 1000))
    lake.write()

    db = work / "payments.duckdb"
    env = {"WAREHOUSE_PATH": str(db), "PATH": __import__("os").environ["PATH"], "SYSTEMROOT": __import__("os").environ.get("SYSTEMROOT", "")}
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", ".", "--no-use-colors", "--vars",
         json.dumps({"lake_path": str(work), "as_of": AS_OF})],
        cwd=ROOT / "dbt", env=env, capture_output=True, text=True)
    return db, result


def test_dbt_build_passes_all_models_and_tests(warehouse):
    _, result = warehouse
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-1000:]
    assert "ERROR=0" in result.stdout


def found(warehouse):
    db, _ = warehouse
    con = duckdb.connect(str(db), read_only=True)
    out = {}
    for payment_id, kind in con.execute("select payment_id::varchar, type from recon_discrepancies").fetchall():
        out.setdefault(kind, set()).add(payment_id)
    return out


def test_every_kind_of_difference_is_found_exactly(warehouse):
    result = found(warehouse)
    assert result == {
        "MISSING_IN_LEDGER": {P["missing"], P["undone"]},     # no deposit at all, and a deposit that was reversed
        "AMOUNT_MISMATCH": {P["mismatch"], P["updated"]},     # the update to 6500 must be seen, not the first image
        "DUPLICATE_BOOKING": {P["duplicate"]},
        "MISSING_IN_STRIPE": {P["stripe_missing"]},
    }


def test_clean_failed_and_in_flight_payments_are_not_reported(warehouse):
    reported = {p for ids in found(warehouse).values() for p in ids}
    assert not reported & {P["clean"], P["failed"], P["inflight"]}


def test_redelivered_event_does_not_double_count(warehouse):
    db, _ = warehouse
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("select count(*) from stg_payments").fetchone()[0] == 8
    captured = con.execute("select sum(captured_count), sum(captured_cents) from fct_daily_transaction_volume").fetchone()
    assert captured == (7, 1000 + 2000 + 3000 + 4000 + 5000 + 6500 + 999)


def test_difference_rate_counts_payments_not_rows(warehouse):
    db, _ = warehouse
    day, checked, with_diff, rate = duckdb.connect(str(db), read_only=True).execute(
        "select day::varchar, payments_checked, payments_with_difference, diff_rate from fct_daily_reconciliation"
        " where day = date '2026-10-17'").fetchone()
    assert (checked, with_diff) == (6, 5)             # 6 old captured payments, 5 of them with a difference (clean is the 6th)
    assert rate == pytest.approx(5 / 6, abs=1e-4)


def test_a_non_positive_amount_in_the_lake_stops_the_build(tmp_path):
    """The services forbid amount <= 0 with a CHECK constraint, so it can only appear if capture or parsing is wrong.
    The cross-table test assert_amounts_are_positive must make `dbt build` fail, which fails the Dagster run."""
    import os
    lake = Lake(tmp_path)
    lake.event("payments__payments", "cdc.payments.public.payments", P["clean"], "c", payment("clean", -5))
    for directory, topic in [("orders__orders", "cdc.orders.public.orders"),
                             ("ledger__ledger_entries", "cdc.ledger.public.ledger_entries"),
                             ("fineract_default__m_savings_account", "cdc.fineract_default.public.m_savings_account"),
                             ("fineract_default__m_savings_account_transaction",
                              "cdc.fineract_default.public.m_savings_account_transaction"),
                             ("fineract_default__m_note", "cdc.fineract_default.public.m_note")]:
        lake.event(directory, topic, "x", "c", {"id": 1, "payment_id": None, "savings_account_id": 1, "client_id": 1,
                   "currency_code": "USD", "status_enum": 300, "transaction_type_enum": 2, "is_reversed": False,
                   "transaction_date": 20743, "amount": "1.0", "savings_account_transaction_id": None, "note": "",
                   "merchant_id": "m", "currency": "USD", "status": "COMPLETED", "created_at": "2026-10-17T10:00:00.000000Z"})
    lake.write()
    env = {"WAREHOUSE_PATH": str(tmp_path / "w.duckdb"), "PATH": os.environ["PATH"], "SYSTEMROOT": os.environ.get("SYSTEMROOT", "")}
    result = subprocess.run([sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", ".", "--no-use-colors", "--vars",
                             json.dumps({"lake_path": str(tmp_path), "as_of": AS_OF})], cwd=ROOT / "dbt", env=env,
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert "FAIL" in result.stdout and "assert_amounts_are_positive" in result.stdout
