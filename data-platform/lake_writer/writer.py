"""Drains the Debezium change topics from Kafka into Parquet files: the raw layer of the lake.

One file set per source table, partitioned by the day of the change:

    <lake>/raw/<database>__<table>/dt=2026-10-18/part-<partition>-<first offset>-<last offset>-<id>.parquet

A row is one change event, kept as Debezium sent it (op, before, after, source position). Nothing is interpreted here;
turning change events into current state is the job of the dbt staging models.

Delivery is at-least-once: files are written first and offsets are committed afterwards, so a crash between the two
re-delivers events. That is harmless because staging de-duplicates on (topic, partition, offset).

    python -m lake_writer.writer --bootstrap localhost:9092 --lake ./lake
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

TOPIC_PATTERN = r"^cdc\..+"          # cdc.<database>.<schema>.<table>; heartbeat topics start with __debezium and do not match
TOPIC_RE = re.compile(r"^cdc\.(?P<db>[^.]+)\.(?P<schema>[^.]+)\.(?P<table>.+)$")

SCHEMA = pa.schema([
    ("topic", pa.string()),
    ("partition", pa.int32()),
    ("offset", pa.int64()),
    ("op", pa.string()),                # r = snapshot read, c = insert, u = update, d = delete
    ("ts_ms", pa.int64()),              # when the change was committed in the source database
    ("lsn", pa.int64()),
    ("pk", pa.string()),                # the Kafka key: the primary key as JSON
    ("before", pa.string()),            # row image as JSON, null when it does not exist
    ("after", pa.string()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
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


def parse(topic: str, partition: int, offset: int, key: bytes | None, value: bytes | None, now: datetime) -> Change | None:
    """One Kafka record -> one raw row, or None for records that are not change events (tombstones, other topics)."""
    match = TOPIC_RE.match(topic)
    if match is None or value is None:
        return None
    event = json.loads(value)
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
        "lsn": source.get("lsn"),
        "pk": primary_key(key),
        "before": json.dumps(event["before"], separators=(",", ":")) if event.get("before") is not None else None,
        "after": json.dumps(event["after"], separators=(",", ":")) if event.get("after") is not None else None,
        "ingested_at": now,
    }
    day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return Change(match["db"], match["table"], day, row)


def primary_key(key: bytes | None) -> str | None:
    """The Kafka key as canonical JSON of the primary-key columns, with or without the schema wrapper."""
    if not key:
        return None
    parsed = json.loads(key)
    if isinstance(parsed, dict) and isinstance(parsed.get("payload"), dict):
        parsed = parsed["payload"]
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


def flush(lake: Path, changes: list[Change]) -> int:
    """Writes the buffered changes, one Parquet file per (table, day, Kafka partition). Returns the number of files."""
    groups: dict[tuple[str, str, int], list[dict]] = defaultdict(list)
    for change in changes:
        groups[(change.directory, change.day, change.row["partition"])].append(change.row)
    for (directory, day, partition), rows in groups.items():
        target = lake / "raw" / directory / f"dt={day}"
        target.mkdir(parents=True, exist_ok=True)
        first, last = min(r["offset"] for r in rows), max(r["offset"] for r in rows)
        name = f"part-{partition}-{first}-{last}-{uuid.uuid4().hex[:8]}.parquet"
        temporary = target / (name + ".tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), temporary, compression="zstd")
        os.replace(temporary, target / name)             # readers never see a half-written file
    return len(groups)


def drain(bootstrap: str, lake: Path, group: str = "lake-writer", idle_seconds: float = 8.0, batch_size: int = 5000,
          assignment_timeout: float = 60.0) -> dict[str, int]:
    """Reads everything that is available now, writes it, commits the offsets, and returns rows written per table."""
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False,
                             auto_offset_reset="earliest", metadata_max_age_ms=5000, max_poll_records=batch_size)
    consumer.subscribe(pattern=TOPIC_PATTERN)
    written: dict[str, int] = defaultdict(int)
    buffer: list[Change] = []
    pending: dict[TopicPartition, int] = {}       # next offset to commit per partition, for records already buffered

    def commit_buffer():
        # Offsets are committed only for records whose rows are on disk, never for the consumer's read position.
        if buffer:
            flush(lake, buffer)
            for change in buffer:
                written[f"{change.db}.{change.table}"] += 1
            buffer.clear()
        if pending:
            consumer.commit({tp: OffsetAndMetadata(offset, "", -1) for tp, offset in pending.items()})
            pending.clear()

    try:
        # Group join and partition assignment take a few polls; records can already arrive in the poll that brings the
        # assignment, so every poll is processed. The idle timer only starts once partitions are assigned.
        started = time.time()
        idle_since = None
        while True:
            batches = consumer.poll(timeout_ms=1000, max_records=batch_size)
            records = [r for batch in batches.values() for r in batch]
            if records:
                idle_since = time.time()
                now = datetime.now(timezone.utc)
                for r in records:
                    key = TopicPartition(r.topic, r.partition)
                    pending[key] = max(pending.get(key, 0), r.offset + 1)
                    change = parse(r.topic, r.partition, r.offset, r.key, r.value, now)
                    if change is not None:
                        buffer.append(change)
                if len(buffer) >= batch_size:
                    commit_buffer()
            elif consumer.assignment():
                idle_since = idle_since or time.time()
                if time.time() - idle_since >= idle_seconds:
                    break
            elif time.time() - started > assignment_timeout:
                raise TimeoutError("no partitions assigned: is Kafka reachable and are there cdc.* topics?")
        commit_buffer()
    finally:
        consumer.close()
    return dict(written)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--lake", default="./lake")
    parser.add_argument("--idle-seconds", type=float, default=8.0)
    args = parser.parse_args()
    result = drain(args.bootstrap, Path(args.lake), idle_seconds=args.idle_seconds)
    print(json.dumps(result, indent=2) if result else "no new change events")


if __name__ == "__main__":
    main()
