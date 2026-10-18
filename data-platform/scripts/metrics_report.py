"""Prints the analytics layer: daily transaction volume and daily reconciliation difference rate.

    python data-platform/scripts/metrics_report.py [--warehouse data-platform/warehouse/payments.duckdb] [--days 14]

The same two tables are what a dashboard (Grafana with a DuckDB or Postgres data source, Metabase, Superset) would read:
fct_daily_transaction_volume and fct_daily_reconciliation.
"""
from __future__ import annotations

import argparse

import duckdb


def money(cents) -> str:
    return f"{(cents or 0) / 100:,.2f}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default="data-platform/warehouse/payments.duckdb")
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    con = duckdb.connect(args.warehouse, read_only=True)

    print("Daily transaction volume (by payment creation day, UTC)")
    print(f"{'day':<12}{'cur':<5}{'captured':>9}{'volume':>12}{'not captured':>14}{'booked in ledger':>18}{'unbooked':>12}")
    for day, cur, n, cents, other, booked, _, unbooked in con.execute(
            "select day, currency, captured_count, captured_cents, not_captured_count, ledger_booked_count, ledger_booked_cents,"
            " unbooked_cents from fct_daily_transaction_volume order by day desc limit ?", [args.days]).fetchall()[::-1]:
        print(f"{day!s:<12}{cur:<5}{n:>9}{money(cents):>12}{other:>14}{booked:>18}{money(unbooked):>12}")

    print("\nDaily reconciliation (payments vs Fineract ledger)")
    print(f"{'day':<12}{'checked':>8}{'with diff':>10}{'missing':>8}{'amount':>7}{'dup':>5}{'no pay':>7}{'at risk':>10}{'diff rate':>10}")
    for row in con.execute(
            "select day, payments_checked, payments_with_difference, missing_in_ledger, amount_mismatch, duplicate_booking,"
            " missing_in_stripe, amount_at_risk_cents, diff_rate from fct_daily_reconciliation order by day desc limit ?",
            [args.days]).fetchall()[::-1]:
        day, checked, with_diff, miss, amount, dup, no_pay, risk, rate = row
        rate_text = "-" if rate is None else f"{rate:.1%}"
        print(f"{day!s:<12}{checked:>8}{with_diff:>10}{miss:>8}{amount:>7}{dup:>5}{no_pay:>7}{money(risk):>10}{rate_text:>10}")


if __name__ == "__main__":
    main()
