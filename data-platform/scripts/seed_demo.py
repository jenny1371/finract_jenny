"""Writes demo business activity into the REAL platform databases and the REAL Fineract, so the CDC pipeline has
something to capture. It does what the services do (orders, payments, ledger entries in Postgres; deposits with the
note "payment <id>" in Fineract through its API) and plants a known set of differences. The expected result is written
to an expectations file; the pipeline test compares what the batch reconciliation finds with it.

    python data-platform/scripts/seed_demo.py --days 6 --per-day 8 --expect data-platform/.demo-expected.json

Not a load generator and not part of the services: it exists to test the data pipeline end to end.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import random
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg

FINERACT = os.environ.get("FINERACT_URL", "http://localhost:8443/fineract-provider/api/v1")
PLATFORM_DSN = os.environ.get("PLATFORM_DSN", "host=localhost port=5433 user=postgres password=postgres")
AUTH = "Basic " + base64.b64encode(b"mifos:password").decode()
FMT = {"dateFormat": "dd MMMM yyyy", "locale": "en"}


def fineract(method: str, path: str, body: dict | None = None):
    request = urllib.request.Request(
        f"{FINERACT}/{path}", method=method, data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": AUTH, "Fineract-Platform-TenantId": "default", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Fineract {method} {path} -> {e.code}: {e.read()[:300]!r}")


def fdate(d: date) -> str:
    return d.strftime("%d %B %Y")


def ensure_product_and_payment_type() -> tuple[int, int]:
    product = next((p["id"] for p in fineract("GET", "savingsproducts") if p["name"] == "Wallet"), None)
    if product is None:
        product = fineract("POST", "savingsproducts", {
            "name": "Wallet", "shortName": "WLT", "currencyCode": "USD", "digitsAfterDecimal": 2, "inMultiplesOf": 0,
            "nominalAnnualInterestRate": 0, "interestCompoundingPeriodType": 1, "interestPostingPeriodType": 4,
            "interestCalculationType": 1, "interestCalculationDaysInYearType": 365, "accountingRule": 1,
            "locale": "en"})["resourceId"]
    payment_type = next((p["id"] for p in fineract("GET", "paymenttypes") if p["name"] == "Stripe"), None)
    if payment_type is None:
        payment_type = fineract("POST", "paymenttypes", {"name": "Stripe", "description": "Stripe card payment",
                                                         "isCashPayment": False, "position": 1})["resourceId"]
    return product, payment_type


def create_merchant(name: str, product: int, opened: date) -> tuple[int, int]:
    client = fineract("POST", "clients", {"officeId": 1, "legalFormId": 1, "firstname": "Demo", "lastname": name,
                                          "active": True, "activationDate": fdate(opened),
                                          "submittedOnDate": fdate(opened), **FMT})
    account = fineract("POST", "savingsaccounts", {"clientId": client["clientId"], "productId": product,
                                                   "submittedOnDate": fdate(opened), **FMT})["savingsId"]
    fineract("POST", f"savingsaccounts/{account}?command=approve", {"approvedOnDate": fdate(opened), **FMT})
    fineract("POST", f"savingsaccounts/{account}?command=activate", {"activatedOnDate": fdate(opened), **FMT})
    return client["clientId"], account


def deposit(account: int, payment_type: int, payment_id: uuid.UUID, cents: int, day: date) -> int:
    return fineract("POST", f"savingsaccounts/{account}/transactions?command=deposit", {
        "transactionDate": fdate(day), "transactionAmount": f"{cents / 100:.2f}", "paymentTypeId": payment_type,
        "note": f"payment {payment_id}", **FMT})["resourceId"]


class Platform:
    def __init__(self):
        self.conn = {db: psycopg.connect(f"{PLATFORM_DSN} dbname={db}", autocommit=True)
                     for db in ("orders", "payments", "ledger")}

    def payment(self, merchant: str, cents: int, at: datetime, status="CAPTURED") -> uuid.UUID:
        order_id, payment_id = uuid.uuid4(), uuid.uuid4()
        self.conn["orders"].execute(
            "insert into orders (id, amount, currency, status, created_at, merchant_id, payment_method, payment_id,"
            " saga_updated_at) values (%s, %s, 'USD', %s, %s, %s, 'pm_card_visa', %s, %s)",
            (order_id, cents, "COMPLETED" if status == "CAPTURED" else "PAYMENT_FAILED", at, merchant, payment_id, at))
        self.conn["payments"].execute(
            "insert into payments (id, order_id, amount, currency, status, stripe_intent_id, created_at, updated_at)"
            " values (%s, %s, %s, 'USD', %s, %s, %s, %s)",
            (payment_id, order_id, cents, status, "pi_" + payment_id.hex[:24], at, at))
        return payment_id

    def ledger_entry(self, payment_id, merchant, cents, at, status, txn):
        self.conn["ledger"].execute(
            "insert into ledger_entries (id, payment_id, order_id, merchant_id, amount, currency, status,"
            " fineract_txn_id, created_at, updated_at) values (%s, %s, %s, %s, %s, 'USD', %s, %s, %s, %s)",
            (uuid.uuid4(), payment_id, uuid.uuid4(), merchant, cents, status, str(txn) if txn else None, at, at))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=6)
    parser.add_argument("--per-day", type=int, default=8)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--expect", default="data-platform/.demo-expected.json")
    args = parser.parse_args()
    rnd = random.Random(args.seed)
    today = date.today()
    opened = today - timedelta(days=args.days + 2)
    product, payment_type = ensure_product_and_payment_type()
    merchants = {f"demo-{i}": create_merchant(f"M{i}-{uuid.uuid4().hex[:6]}", product, opened) for i in range(2)}
    platform = Platform()
    expected: dict[str, list[str]] = {"MISSING_IN_LEDGER": [], "MISSING_IN_STRIPE": [], "AMOUNT_MISMATCH": [],
                                      "DUPLICATE_BOOKING": []}
    clean = 0
    daily: dict[str, int] = {}

    def at(day: date, hour: int) -> datetime:
        return datetime(day.year, day.month, day.day, hour, rnd.randint(0, 59), tzinfo=timezone.utc)

    for offset in range(args.days, 0, -1):
        day = today - timedelta(days=offset)
        for i in range(args.per_day):
            merchant = f"demo-{i % 2}"
            account = merchants[merchant][1]
            cents = rnd.choice([1250, 4999, 10000, 2500, 799, 15000])
            when = at(day, 9 + i)
            kind = "clean"
            if offset == args.days - 1 and i == 0:
                kind = "MISSING_IN_LEDGER"
            elif offset == args.days - 2 and i == 1:
                kind = "AMOUNT_MISMATCH"
            elif offset == args.days - 3 and i == 2:
                kind = "DUPLICATE_BOOKING"
            elif offset == args.days - 4 and i == 3:
                kind = "UNDONE_DEPOSIT"          # booked, then reversed: not in the books
            elif offset == args.days - 4 and i == 4:
                kind = "FAILED_PAYMENT"          # never captured: nothing to reconcile
            status = "FAILED" if kind == "FAILED_PAYMENT" else "CAPTURED"
            pid = platform.payment(merchant, cents, when, status)
            txn = None
            if kind in ("clean", "AMOUNT_MISMATCH", "DUPLICATE_BOOKING", "UNDONE_DEPOSIT"):
                booked = cents + 100 if kind == "AMOUNT_MISMATCH" else cents
                txn = deposit(account, payment_type, pid, booked, day)
                if kind == "DUPLICATE_BOOKING":
                    deposit(account, payment_type, pid, cents, day)
                if kind == "UNDONE_DEPOSIT":
                    fineract("POST", f"savingsaccounts/{account}/transactions/{txn}?command=undo",
                             {**FMT, "transactionDate": fdate(day), "transactionAmount": 0})
            if status == "CAPTURED":
                platform.ledger_entry(pid, merchant, cents, when, "FAILED" if kind == "MISSING_IN_LEDGER" else "BOOKED",
                                      None if kind == "MISSING_IN_LEDGER" else txn)
                daily[str(day)] = daily.get(str(day), 0) + cents
            if kind in expected:
                expected[kind].append(str(pid))
            elif kind == "UNDONE_DEPOSIT":
                expected["MISSING_IN_LEDGER"].append(str(pid))
            elif kind == "clean":
                clean += 1
    # a deposit in the books for which no payment exists
    orphan = uuid.uuid4()
    deposit(merchants["demo-0"][1], payment_type, orphan, 3300, today - timedelta(days=2))
    expected["MISSING_IN_STRIPE"].append(str(orphan))
    # a payment captured a minute ago: its booking is still in flight and must not be reported (grace period)
    seeded_at = datetime.now(timezone.utc)
    in_flight = platform.payment("demo-1", 999, seeded_at - timedelta(seconds=60))
    # "now" for the reconciliation under test: pinning it to the seed time makes the expectation reproducible,
    # because the in-flight payment becomes an overdue difference once the grace period has passed.
    result = {"as_of": seeded_at.isoformat(), "clean": clean, "expected_discrepancies": expected, "in_flight_not_reported": str(in_flight),
              "captured_cents_by_day": daily, "merchants": {k: v[1] for k, v in merchants.items()}}
    with open(args.expect, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps({k: len(v) for k, v in expected.items()} | {"clean": clean}))


if __name__ == "__main__":
    main()
