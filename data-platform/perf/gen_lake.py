"""Generates a synthetic raw lake of N payments in exactly the format lake_writer produces, plus the known answer.

    python data-platform/perf/gen_lake.py --payments 100000 --lake data-platform/perf/lake-100000

Why synthetic: the dbt scaling test needs millions of change events, and pushing them through Postgres and Debezium
would measure the capture path, not dbt. The capture path is measured separately (bench_cdc_latency.py, bench_writer.py).

Per payment the lake holds the change history a real run produces: payments (insert + update to CAPTURED), orders
(insert + update), ledger entries (insert + update to BOOKED), one Fineract deposit and its note, so about 8 events per
payment. Payments are spread over 28 days. Planted differences, with known counts (written to expected.json):
    1 % no deposit (MISSING_IN_LEDGER)   1 % deposit 1.00 too high (AMOUNT_MISMATCH)
    1 % deposited twice (DUPLICATE_BOOKING)   0.5 % of N deposits whose payment does not exist (MISSING_IN_STRIPE)
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import duckdb

DAYS = 28
BASE = "timestamp '2026-09-20 00:00:00'"            # first day; all payments fall inside the 30-day window before as_of


def build(payments: int, lake: Path, seed_days: int = DAYS, first: int = 1, base: str = BASE, offset_base: int = 0,
          tag: str = "data", append: bool = False) -> dict:
    """payments numbered first .. first+payments-1, spread over seed_days starting at `base`. Appending: pass a later
    base, first = existing count + 1, and offset_base above every existing Kafka offset."""
    if first == 1 and lake.exists():
        shutil.rmtree(lake)
    con = duckdb.connect()
    con.execute("set memory_limit = '6GB'")
    n = payments
    missing, mismatch, duplicate = n // 100, n // 100, n // 100          # n % 100 == 0 keeps the counts exact
    orphans = 0 if append else n // 200

    # one row per payment with everything derived from its number
    con.execute(f"""
        create table p as
        select
            i,
            md5('payment-' || i)::uuid                               as payment_id,
            md5('order-' || i)::uuid                                 as order_id,
            md5('ledger-' || i)::uuid                                as ledger_id,
            (100 + (i * 37) % 9900)::bigint                          as cents,
            epoch_ms({base} + to_seconds(((i - {first}) * {seed_days} * 86400) // {n}))      as created_ms,
            1 + (i % 50)                                             as account,
            case i % 100 when 1 then 'missing' when 2 then 'mismatch' when 3 then 'duplicate' else 'clean' end as kind
        from range({first}, {first + n}) t(i)
    """)

    def write(directory: str, topic: str, sql: str):
        """sql yields (pk_json, op, ts_ms, after_json); adds the lake_writer columns and writes one file set per day."""
        target = lake / "raw" / directory
        target.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            copy (
                select '{topic}' as topic, 0::int as partition,
                       (row_number() over (order by ts_ms, pk) - 1 + {offset_base})::bigint as "offset",
                       op, ts_ms::bigint as ts_ms, (ts_ms + 400)::bigint as kafka_ts_ms,
                       (ts_ms * 1000 + row_number() over (order by ts_ms, pk))::bigint as lsn,
                       pk, null::varchar as before, after,
                       make_timestamp((ts_ms + 2500) * 1000) as ingested_at_,
                       strftime(make_timestamp(ts_ms * 1000), '%Y-%m-%d') as dt
                from ({sql})
            ) to '{target.as_posix()}' (format parquet, partition_by (dt), compression zstd, overwrite_or_ignore, filename_pattern '{tag}_{{i}}')
        """.replace("ingested_at_,", "ingested_at,"))

    ts = lambda col, plus: f"({col} + {plus})"      # noqa: E731
    iso = lambda col, plus: f"strftime(make_timestamp(({col} + {plus}) * 1000), '%Y-%m-%dT%H:%M:%S.000000Z')"  # noqa: E731

    pay = lambda status, plus, op: f"""
        select json_object('id', payment_id)::varchar as pk, '{op}' as op, {ts('created_ms', plus)} as ts_ms,
               json_object('id', payment_id, 'order_id', order_id, 'amount', cents, 'currency', 'USD', 'status', '{status}',
                           'stripe_intent_id', 'pi_' || substr(replace(payment_id::varchar, '-', ''), 1, 24),
                           'created_at', {iso('created_ms', 0)}, 'updated_at', {iso('created_ms', plus)})::varchar as after
        from p"""
    write("payments__payments", "cdc.payments.public.payments",
          pay("AUTHORIZED", 0, "c") + " union all " + pay("CAPTURED", 2000, "u"))

    order = lambda status, plus, op: f"""
        select json_object('id', order_id)::varchar as pk, '{op}' as op, {ts('created_ms', plus)} as ts_ms,
               json_object('id', order_id, 'merchant_id', 'm-' || (account % 10), 'payment_id', payment_id, 'amount', cents,
                           'currency', 'USD', 'status', '{status}', 'created_at', {iso('created_ms', 0)})::varchar as after
        from p"""
    write("orders__orders", "cdc.orders.public.orders", order("PAYMENT_PENDING", 0, "c") + " union all " + order("COMPLETED", 5000, "u"))

    led = lambda status, plus, op: f"""
        select json_object('id', ledger_id)::varchar as pk, '{op}' as op, {ts('created_ms', plus)} as ts_ms,
               json_object('id', ledger_id, 'payment_id', payment_id, 'merchant_id', 'm-' || (account % 10), 'amount', cents,
                           'currency', 'USD', 'status', '{status}', 'fineract_txn_id', i::varchar,
                           'created_at', {iso('created_ms', 0)})::varchar as after
        from p where kind <> 'missing'"""
    write("ledger__ledger_entries", "cdc.ledger.public.ledger_entries",
          led("PENDING", 2500, "c") + " union all " + led("BOOKED", 4000, "u"))

    # Fineract: transaction id = i for the deposits of payments; extra deposits for duplicates and for orphans
    con.execute(f"""
        create table d as
        select i as txn_id, payment_id, i as src, account,
               cents + case when kind = 'mismatch' then 100 else 0 end as cents, created_ms + 3000 as ms
        from p where kind <> 'missing'
        union all
        select 10000000 + i, payment_id, i, account, cents, created_ms + 3500 from p where kind = 'duplicate'
        union all
        select 20000000 + k, md5('orphan-' || k)::uuid, k, 1 + (k % 50), 3300,
               epoch_ms({BASE} + to_seconds((k * {seed_days} * 86400) // {orphans + 1})) from range(1, {orphans + 1}) t(k)
    """)
    deposit_day = lambda ms: f"(({ms}) // 86400000)::int"   # noqa: E731
    write("fineract_default__m_savings_account_transaction", "cdc.fineract_default.public.m_savings_account_transaction", f"""
        select json_object('id', txn_id)::varchar as pk, 'c' as op, ms as ts_ms,
               json_object('id', txn_id, 'savings_account_id', account, 'transaction_type_enum', 1, 'is_reversed', false,
                           'transaction_date', {deposit_day('ms')}, 'amount', printf('%.6f', cents / 100.0))::varchar as after
        from d""")
    write("fineract_default__m_note", "cdc.fineract_default.public.m_note", f"""
        select json_object('id', txn_id)::varchar as pk, 'c' as op, ms + 50 as ts_ms,
               json_object('id', txn_id, 'savings_account_transaction_id', txn_id, 'note', 'payment ' || payment_id)::varchar as after
        from d""")
    if not append:                                   # the accounts exist once
        write("fineract_default__m_savings_account", "cdc.fineract_default.public.m_savings_account", f"""
            select json_object('id', k)::varchar as pk, 'c' as op, epoch_ms({BASE}) as ts_ms,
                   json_object('id', k, 'client_id', k, 'currency_code', 'USD', 'status_enum', 300)::varchar as after
            from range(1, 51) t(k)""")
    if append:
        return {"payments": n, "appended": True}

    events = con.execute(f"select count(*) from read_parquet('{lake.as_posix()}/raw/*/*/*.parquet')").fetchone()[0]
    size_mb = sum(f.stat().st_size for f in lake.rglob("*.parquet")) / 1e6
    files = len(list(lake.rglob("*.parquet")))
    expected = {"payments": n, "events": events, "files": files, "lake_mb": round(size_mb, 1),
                "MISSING_IN_LEDGER": missing, "AMOUNT_MISMATCH": mismatch, "DUPLICATE_BOOKING": duplicate,
                "MISSING_IN_STRIPE": orphans, "as_of": "2026-10-19T00:00:00+00:00"}
    (lake / "expected.json").write_text(json.dumps(expected, indent=2))
    return expected


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--payments", type=int, required=True)
    parser.add_argument("--lake", required=True)
    args = parser.parse_args()
    started = time.time()
    print(json.dumps(build(args.payments, Path(args.lake))), f"in {time.time() - started:.1f}s")
