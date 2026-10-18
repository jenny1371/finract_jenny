"""Drains the Debezium change topics from Kafka into Parquet files: the raw layer of the lake.

One file set per source table, partitioned by the day of the change:

    <lake>/raw/<database>__<table>/dt=2026-10-18/part-<partition>-<first offset>-<last offset>-<id>.parquet

A row is one change event, kept as Debezium sent it (op, before, after, source position). Nothing is interpreted here;
turning change events into current state is the job of the dbt staging models.

Delivery is at-least-once: files are written first and offsets are committed afterwards, so a crash between the two
re-delivers events. That is harmless because staging de-duplicates on (topic, partition, offset).

    python -m lake_writer.writer --bootstrap localhost:9092 --lake ./lake            # drain what is there, then stop
    python -m lake_writer.writer --follow --flush-seconds 2                          # keep running
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

try:                                   # 3 to 10 times faster than the standard library; same results
    import orjson

    def _loads(data):
        return orjson.loads(data)

    def _dumps(value) -> str:
        return orjson.dumps(value).decode()
except ImportError:                    # pragma: no cover
    def _loads(data):
        return json.loads(data)

    def _dumps(value) -> str:
        return json.dumps(value, separators=(",", ":"))

TOPIC_PATTERN = r"^cdc\..+"          # cdc.<database>.<schema>.<table>; heartbeat topics start with __debezium and do not match
TOPIC_RE = re.compile(r"^cdc\.(?P<db>[^.]+)\.(?P<schema>[^.]+)\.(?P<table>.+)$")

SCHEMA = pa.schema([
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("offset", pa.int64()),
    ("op", pa.string()),                # r = snapshot read, c = insert, u = update, d = delete
    ("ts_ms", pa.int64()),              # when the change was committed in the source database
    ("kafka_ts_ms", pa.int64()),        # when Debezium produced the record (Kafka CreateTime)
    ("lsn", pa.int64()),
    ("pk", pa.string()),                # the Kafka key: the primary key as JSON
    ("before", pa.string()),            # row image as JSON, null when it does not exist
    ("after", pa.string()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),   # when the file holding this row was written
])


@dataclass(frozen=True)
class Change:
    db: str
    table: str
    day: str
    row: dict

    @property
    def directory(self) -> str:
        return f"{self.db}__{self.table}"


def parse(topic: str, partition: int, offset: int, key: bytes | None, value: bytes | None, now: datetime,
          kafka_ts_ms: int | None = None) -> Change | None:
    """One Kafka record -> one raw row, or None for records that are not change events (tombstones, other topics)."""
    match = TOPIC_RE.match(topic)
    if match is None or value is None:
        return None
    event = _loads(value)
    if isinstance(event, dict) and isinstance(event.get("payload"), dict):
        event = event["payload"]               # a converter with schemas enabled wraps the envelope: {"schema":..,"payload":..}
    if not isinstance(event, dict) or "op" not in event:
        return None
    source = event.get("source") or {}
    ts_ms = int(source.get("ts_ms") or event.get("ts_ms") or 0)
    row = {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "op": event["op"],
        "ts_ms": ts_ms,
        "kafka_ts_ms": kafka_ts_ms,
        "lsn": source.get("lsn"),
        "pk": primary_key(key),
        "before": _dumps(event["before"]) if event.get("before") is not None else None,
        "after": _dumps(event["after"]) if event.get("after") is not None else None,
        "ingested_at": now,
    }
    day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return Change(match["db"], match["table"], day, row)


def primary_key(key: bytes | None) -> str | None:
    """The Kafka key as canonical JSON of the primary-key columns, with or without the schema wrapper."""
    if not key:
        return None
    parsed = _loads(key)
    if isinstance(parsed, dict) and isinstance(parsed.get("payload"), dict):
        parsed = parsed["payload"]
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))   # canonical: sorted keys


def flush(lake: Path, changes: list[Change]) -> int:
    """Writes the buffered changes, one Parquet file per (table, day, Kafka partition). Returns the number of files."""
    groups: dict[tuple[str, str, int], list[dict]] = defaultdict(list)
    for change in changes:
        groups[(change.directory, change.day, change.row["partition"])].append(change.row)
    for (directory, day, partition), rows in groups.items():
        target = lake / "raw" / directory / f"dt={day}"
        target.mkdir(parents=True, exist_ok=True)
        written_at = datetime.now(timezone.utc)
        for r in rows:
            r["ingested_at"] = written_at
        first, last = min(r["offset"] for r in rows), max(r["offset"] for r in rows)
        name = f"part-{partition}-{first}-{last}-{uuid.uuid4().hex[:8]}.parquet"
        temporary = target / (name + ".tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), temporary, compression="zstd")
        os.replace(temporary, target / name)             # readers never see a half-written file
    return len(groups)


def drain(bootstrap: str, lake: Path, group: str = "lake-writer", idle_seconds: float = 8.0, batch_size: int = 5000,
          assignment_timeout: float = 60.0, follow: bool = False, flush_seconds: float = 2.0,
          stop=None, start: str = "earliest", client: str = "auto") -> dict[str, int]:
    """Reads Kafka, writes Parquet, commits offsets, and returns rows written per table.

    batch mode (default): reads what is available now and returns once nothing new arrived for idle_seconds.
    follow mode: runs until stop() is true, writing files at least every flush_seconds (or every batch_size rows).
    A smaller flush_seconds means lower latency and more small files; lake_writer.compact merges them.
    """
    from lake_writer.sources import open_source

    source = open_source(client, bootstrap, group, start, batch_size, TOPIC_PATTERN)
    written: dict[str, int] = defaultdict(int)
    buffer: list[Change] = []
    pending: dict[tuple[str, int], int] = {}      # next offset to commit per (topic, partition), for records already buffered
    first_buffered_at = None

    def commit_buffer():
        # Offsets are committed only for records whose rows are on disk, never for the consumer's read position.
        nonlocal first_buffered_at
        if buffer:
            flush(lake, buffer)
            for change in buffer:
                written[f"{change.db}.{change.table}"] += 1
            buffer.clear()
        if pending:
            source.commit(pending)
            pending.clear()
        first_buffered_at = None

    try:
        # Group join and partition assignment take a few polls; records can already arrive in the poll that brings the
        # assignment, so every poll is processed. The idle timer only starts once partitions are assigned.
        started = time.time()
        idle_since = None
        while True:
            if stop is not None and stop():
                break
            records = source.poll(0.2 if follow else 1.0)
            if records:
                idle_since = time.time()
                now = datetime.now(timezone.utc)
                for r in records:
                    key = (r.topic, r.partition)
                    if r.offset + 1 > pending.get(key, 0):
                        pending[key] = r.offset + 1
                    change = parse(r.topic, r.partition, r.offset, r.key, r.value, now, r.timestamp_ms)
                    if change is not None:
                        buffer.append(change)
                        first_buffered_at = first_buffered_at or time.time()
                if len(buffer) >= batch_size:
                    commit_buffer()
            elif source.assigned():
                idle_since = idle_since or time.time()
                if not follow and time.time() - idle_since >= idle_seconds:
                    break
            elif time.time() - started > assignment_timeout:
                raise TimeoutError("no partitions assigned: is Kafka reachable and are there cdc.* topics?")
            if follow and pending and (first_buffered_at is None or time.time() - first_buffered_at >= flush_seconds):
                commit_buffer()        # flush interval reached; also commits records that produced no row (tombstones)
        commit_buffer()
    finally:
        source.close()
    return dict(written)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--lake", default="./lake")
    parser.add_argument("--idle-seconds", type=float, default=8.0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--group", default="lake-writer", help="Kafka consumer group (own offsets)")
    parser.add_argument("--client", choices=["auto", "confluent", "kafka-python"], default="auto",
                        help="Kafka client; auto = confluent-kafka when installed")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest",
                        help="where a consumer group with no committed offsets begins")
    parser.add_argument("--follow", action="store_true", help="keep running instead of stopping when idle")
    parser.add_argument("--flush-seconds", type=float, default=2.0, help="follow mode: write files at least this often")
    args = parser.parse_args()
    result = drain(args.bootstrap, Path(args.lake), group=args.group, idle_seconds=args.idle_seconds, batch_size=args.batch_size,
                   follow=args.follow, flush_seconds=args.flush_seconds, start=args.start, client=args.client)
    print(json.dumps(result, indent=2) if result else "no new change events")


if __name__ == "__main__":
    main()
