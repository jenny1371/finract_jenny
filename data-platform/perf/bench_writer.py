"""Writer throughput: build a backlog in Kafka first, then measure how fast lake_writer clears it.

    python data-platform/perf/bench_writer.py --rows 300000 --batch-sizes 1000,5000,20000,50000 [--profile]

Steps: (1) remember where the payments topic ends, (2) insert --rows rows into the payments database with one SQL
statement and time how long Debezium takes to get them all into Kafka, (3) for each batch size, point a fresh consumer
group at the remembered offset and drain the backlog in batch mode, timing it and counting files, (4) optionally profile
one drain to see where the time goes.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

import psycopg
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lake_writer.writer import drain  # noqa: E402

TOPIC = "cdc.payments.public.payments"
PG = "host=localhost port=5433 user=postgres password=postgres dbname=payments"


def end_offset(consumer: KafkaConsumer) -> int:
    tp = TopicPartition(TOPIC, 0)
    return consumer.end_offsets([tp])[tp]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=300000)
    parser.add_argument("--batch-sizes", default="1000,5000,20000,50000")
    parser.add_argument("--clients", default="confluent,kafka-python")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    probe = KafkaConsumer(bootstrap_servers="localhost:9092")
    start = end_offset(probe)
    tag = "bw-" + uuid.uuid4().hex[:6]

    conn = psycopg.connect(PG, autocommit=True)
    inserted_at = time.time()
    conn.execute("insert into payments (id, order_id, amount, currency, status, failure_code, payment_method)"
                 " select gen_random_uuid(), gen_random_uuid(), 1000, 'USD', 'CAPTURED', %s, 'pm_card_visa'"
                 " from generate_series(1, %s)", (tag, args.rows))
    sql_seconds = time.time() - inserted_at
    while end_offset(probe) < start + args.rows:
        time.sleep(0.5)
    debezium_seconds = time.time() - inserted_at
    result = {"rows": args.rows, "sql_insert_seconds": round(sql_seconds, 1),
              "debezium_to_kafka_seconds": round(debezium_seconds, 1),
              "debezium_rows_per_s": round(args.rows / debezium_seconds), "drains": []}

    for client, batch in [(c, int(b)) for c in args.clients.split(",") for b in args.batch_sizes.split(",")]:
        group = f"{tag}-{client}-{batch}"
        committer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id=group, enable_auto_commit=False)
        tp = TopicPartition(TOPIC, 0)
        committer.assign([tp])
        committer.commit({tp: OffsetAndMetadata(start, "", -1)})
        committer.close()
        lake = Path(tempfile.mkdtemp(prefix="writer-", dir=ROOT / "perf"))
        profiler = cProfile.Profile() if args.profile and batch == 5000 and client == "confluent" else None
        begun = time.time()
        if profiler:
            profiler.enable()
        # idle_seconds is short: all the data is already there, so the first idle poll means the backlog is cleared
        written = drain("localhost:9092", lake, group=group, batch_size=batch, idle_seconds=2.0, client=client)
        if profiler:
            profiler.disable()
        elapsed = time.time() - begun - 2.0          # minus the idle wait that ends the run
        rows = written.get("payments.payments", 0)
        files = list(lake.rglob("*.parquet"))
        entry = {"client": client, "batch_size": batch, "rows": rows, "seconds": round(elapsed, 1), "rows_per_s": round(rows / elapsed),
                 "files": len(files), "mb": round(sum(f.stat().st_size for f in files) / 1e6, 1)}
        if profiler:
            out = io.StringIO()
            pstats.Stats(profiler, stream=out).sort_stats("cumulative").print_stats(14)
            entry["profile_top"] = [l.strip() for l in out.getvalue().splitlines() if "{" in l or ".py" in l][:14]
        result["drains"].append(entry)
        shutil.rmtree(lake, ignore_errors=True)
        print(json.dumps({k: v for k, v in entry.items() if k != "profile_top"}), flush=True)
    print(json.dumps(result))
    with open(ROOT / "perf" / "results-writer.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
