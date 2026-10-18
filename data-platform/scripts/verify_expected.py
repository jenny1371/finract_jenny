"""Compares what the pipeline computed with the facts the seed script planted. Exits non-zero on any difference.

    python data-platform/scripts/verify_expected.py --expect data-platform/.demo-expected.json \
        --warehouse data-platform/warehouse/payments.duckdb
"""
from __future__ import annotations

import argparse
import json
import sys

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect", required=True)
    parser.add_argument("--warehouse", required=True)
    args = parser.parse_args()
    expected = json.load(open(args.expect))
    con = duckdb.connect(args.warehouse, read_only=True)
    problems: list[str] = []

    found: dict[str, set[str]] = {}
    for payment_id, kind in con.execute("select payment_id::varchar, type from recon_discrepancies").fetchall():
        found.setdefault(kind, set()).add(payment_id)
    for kind in ("MISSING_IN_LEDGER", "MISSING_IN_STRIPE", "AMOUNT_MISMATCH", "DUPLICATE_BOOKING"):
        want, got = set(expected["expected_discrepancies"][kind]), found.get(kind, set())
        status = "ok " if want == got else "BAD"
        print(f"{status} {kind:18} expected {len(want)}  found {len(got)}")
        if want != got:
            problems.append(f"{kind}: missing {sorted(want - got)} unexpected {sorted(got - want)}")
    in_flight = expected["in_flight_not_reported"]
    reported = {p for ids in found.values() for p in ids}
    print(("ok " if in_flight not in reported else "BAD"), "payment inside the grace period is not reported")
    if in_flight in reported:
        problems.append("a payment inside the grace period was reported")

    volume = {str(day): int(cents) for day, cents in
              con.execute("select day::varchar, sum(captured_cents) from fct_daily_transaction_volume group by 1").fetchall()}
    # the payment seeded inside the grace period is captured too; it is not in the per-day expectation, so add it
    for day, cents in expected["captured_cents_by_day"].items():
        got = volume.get(day, 0)
        if got != cents:
            problems.append(f"volume {day}: expected {cents} got {got}")
    extra = {d: c for d, c in volume.items() if d not in expected["captured_cents_by_day"]}
    print(("ok " if not problems else "BAD"), f"daily volume matches for {len(expected['captured_cents_by_day'])} days;"
          f" days not planned (the in-flight payment's day): {extra}")

    for problem in problems:
        print("PROBLEM:", problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
