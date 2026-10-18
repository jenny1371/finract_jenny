"""CDC latency at a steady write rate: from the commit in the source database to the Parquet file on disk.

    python data-platform/perf/bench_cdc_latency.py --rate 1000 --seconds 45 --flush-seconds 2

Rows are written straight into the payments database (no API, so the backend is not what is measured), in one
transaction per 100 ms tick. For every row the lake holds three timestamps:
    ts_ms        commit time in Postgres (from the WAL, as Debezium reports it)
    kafka_ts_ms  when Debezium produced the Kafka record
    ingested_at  when lake_writer wrote the file that holds the row
so the end-to-end latency splits into  Debezium = kafka_ts_ms - ts_ms  and  writer = ingested_at - kafka_ts_ms.
Everything runs on one machine and one clock (the offset to the Postgres container is measured and printed).
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parents[1]
PG = "host=localhost port=5433 user=postgres password=postgres dbname=payments"


def clock_offset_ms(conn) -> float:
    """Postgres clock minus this machine's clock, in ms (positive: Postgres is ahead), from the midpoint of a round trip."""
    best = None
    for _ in range(20):
        t0 = time.time()
        remote = conn.execute("select extract(epoch from clock_timestamp())").fetchone()[0]
        t1 = time.time()
        sample = (float(remote) - (t0 + t1) / 2) * 1000, (t1 - t0) * 1000
        best = sample if best is None or sample[1] < best[1] else best
    return best[0]


def run(rate: int, seconds: int, flush_seconds: float, batch_size: int, keep: bool) -> dict:
    tag = "perf-" + uuid.uuid4().hex[:8]
    lake = Path(tempfile.mkdtemp(prefix="cdc-latency-", dir=ROOT / "perf"))
    conn = psycopg.connect(PG, autocommit=True)
    offset = clock_offset_ms(conn)
    writer = subprocess.Popen(
        [sys.executable, "-m", "lake_writer.writer", "--follow", "--flush-seconds", str(flush_seconds), "--lake", str(lake),
         "--group", tag, "--start", "latest", "--batch-size", str(batch_size)], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(20)                                   # the writer joins its new group and starts at the end of the topics
    tick, per_tick = 0.1, max(1, rate // 10)
    sent, started = 0, time.time()
    while time.time() - started < seconds:
        tick_start = time.time()
        with conn.transaction():
            conn.cursor().executemany(
                "insert into payments (id, order_id, amount, currency, status, failure_code, payment_method)"
                " values (gen_random_uuid(), gen_random_uuid(), 1000, 'USD', 'CAPTURED', %s, 'pm_card_visa')",
                [(tag,)] * per_tick)
        sent += per_tick
        time.sleep(max(0.0, tick - (time.time() - tick_start)))
    achieved = sent / (time.time() - started)

    pattern = str(lake / "raw" / "payments__payments" / "*" / "*.parquet").replace("\\", "/")
    landed, deadline = 0, time.time() + 120
    while time.time() < deadline:
        files = list((lake / "raw" / "payments__payments").glob("*/*.parquet"))
        landed = duckdb.connect().execute(
            f"select count(*) from read_parquet('{pattern}') where json_extract_string(after, '$.failure_code') = ?",
            [tag]).fetchone()[0] if files else 0
        if landed >= sent:
            break
        time.sleep(1)
    writer.terminate()
    writer.wait(timeout=30)

    q = duckdb.connect().execute(f"""
        with r as (
            select ts_ms - {offset:.3f} as commit_ms, kafka_ts_ms, epoch_ms(ingested_at) as landed_ms
            from read_parquet('{pattern}') where json_extract_string(after, '$.failure_code') = ?
        )
        select
            quantile_cont(landed_ms - commit_ms, 0.5), quantile_cont(landed_ms - commit_ms, 0.99), max(landed_ms - commit_ms),
            quantile_cont(kafka_ts_ms - commit_ms, 0.5), quantile_cont(kafka_ts_ms - commit_ms, 0.99),
            quantile_cont(landed_ms - kafka_ts_ms, 0.5), quantile_cont(landed_ms - kafka_ts_ms, 0.99)
        from r""", [tag]).fetchone()
    files = len(list((lake / "raw" / "payments__payments").glob("*/*.parquet")))
    size = sum(f.stat().st_size for f in (lake / "raw" / "payments__payments").glob("*/*.parquet"))
    result = {"target_rows_per_s": rate, "achieved_rows_per_s": round(achieved), "flush_seconds": flush_seconds,
              "rows_sent": sent, "rows_landed": landed, "clock_offset_ms": round(offset, 1),
              "end_to_end_ms": {"p50": round(q[0]), "p99": round(q[1]), "max": round(q[2])},
              "debezium_ms": {"p50": round(q[3]), "p99": round(q[4])},
              "writer_ms": {"p50": round(q[5]), "p99": round(q[6])},
              "files": files, "kb_per_file": round(size / max(files, 1) / 1024, 1)}
    if not keep:
        shutil.rmtree(lake, ignore_errors=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--flush-seconds", type=float, default=2.0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.rate, args.seconds, args.flush_seconds, args.batch_size, args.keep)))
