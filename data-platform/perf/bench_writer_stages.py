"""Where does lake_writer spend its time? Drains the last --rows records of the payments topic with every stage timed.

    python data-platform/perf/bench_writer_stages.py --rows 300000 --batch-size 20000

Stages: poll (the Kafka client fetching and decoding records), parse (my code: JSON in, row dict out), flush (building
Arrow tables and writing Parquet), commit (offset commit to Kafka). Run after bench_writer.py, which leaves the backlog.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import lake_writer.writer as writer  # noqa: E402

TOPIC = "cdc.payments.public.payments"
spent = {"poll": 0.0, "parse": 0.0, "flush": 0.0, "commit": 0.0}


def timed(stage, function):
    def wrapper(*args, **kwargs):
        started = time.perf_counter()
        try:
            return function(*args, **kwargs)
        finally:
            spent[stage] += time.perf_counter() - started
    return wrapper


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=300000)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--client", default="auto")
    args = parser.parse_args()
    tp = TopicPartition(TOPIC, 0)
    probe = KafkaConsumer(bootstrap_servers="localhost:9092")
    start = probe.end_offsets([tp])[tp] - args.rows
    group = "stages-" + uuid.uuid4().hex[:6]
    committer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id=group, enable_auto_commit=False)
    committer.assign([tp])
    committer.commit({tp: OffsetAndMetadata(start, "", -1)})
    committer.close()

    from lake_writer import sources
    writer.parse = timed("parse", writer.parse)
    writer.flush = timed("flush", writer.flush)
    for cls in (sources.ConfluentSource, sources.KafkaPythonSource):
        cls.poll = timed("poll", cls.poll)
        cls.commit = timed("commit", cls.commit)

    lake = Path(tempfile.mkdtemp(prefix="stages-", dir=ROOT / "perf"))
    begun = time.perf_counter()
    written = writer.drain("localhost:9092", lake, group=group, batch_size=args.batch_size, idle_seconds=2.0,
                           client=args.client)
    total = time.perf_counter() - begun - 2.0
    rows = written.get("payments.payments", 0)
    shutil.rmtree(lake, ignore_errors=True)
    out = {"client": args.client, "rows": rows, "batch_size": args.batch_size, "total_seconds": round(total, 1), "rows_per_s": round(rows / total),
           "seconds": {k: round(v - (2.0 if k == "poll" else 0), 1) for k, v in spent.items()},
           "microseconds_per_row": {k: round((v - (2.0 if k == "poll" else 0)) / max(rows, 1) * 1e6) for k, v in spent.items()}}
    print(json.dumps(out))


if __name__ == "__main__":
    main()
