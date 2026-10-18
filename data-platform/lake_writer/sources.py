"""Kafka clients behind one small interface, so the writer does not care which one reads.

confluent-kafka (librdkafka, C) is the default: measured 12 microseconds per record to fetch against 204 for
kafka-python (pure Python), which made the Python client 81 % of the writer's time. kafka-python stays as the fallback
for environments where the C library cannot be installed.
"""
from __future__ import annotations

from typing import NamedTuple


class Record(NamedTuple):
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None


class ConfluentSource:
    name = "confluent-kafka"

    def __init__(self, bootstrap: str, group: str, start: str, batch_size: int, pattern: str):
        from confluent_kafka import Consumer
        self._batch = batch_size
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": start, "topic.metadata.refresh.interval.ms": 5000})
        self._consumer.subscribe([pattern])

    def poll(self, timeout_s: float) -> list[Record]:
        from confluent_kafka import KafkaError
        out = []
        for m in self._consumer.consume(num_messages=self._batch, timeout=timeout_s):
            if m.error():
                if m.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise RuntimeError(f"Kafka error: {m.error()}")
            out.append(Record(m.topic(), m.partition(), m.offset(), m.key(), m.value(), m.timestamp()[1]))
        return out

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        from confluent_kafka import TopicPartition
        self._consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False)

    def assigned(self) -> bool:
        return bool(self._consumer.assignment())

    def close(self) -> None:
        self._consumer.close()


class KafkaPythonSource:
    name = "kafka-python"

    def __init__(self, bootstrap: str, group: str, start: str, batch_size: int, pattern: str):
        from kafka import KafkaConsumer
        self._batch = batch_size
        self._consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False,
                                       auto_offset_reset=start, metadata_max_age_ms=5000, max_poll_records=batch_size)
        self._consumer.subscribe(pattern=pattern)

    def poll(self, timeout_s: float) -> list[Record]:
        batches = self._consumer.poll(timeout_ms=int(timeout_s * 1000), max_records=self._batch)
        return [Record(r.topic, r.partition, r.offset, r.key, r.value, r.timestamp)
                for batch in batches.values() for r in batch]

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        self._consumer.commit({TopicPartition(t, p): OffsetAndMetadata(o, "", -1) for (t, p), o in offsets.items()})

    def assigned(self) -> bool:
        return bool(self._consumer.assignment())

    def close(self) -> None:
        self._consumer.close()


def open_source(client: str, bootstrap: str, group: str, start: str, batch_size: int, pattern: str):
    if client in ("auto", "confluent"):
        try:
            return ConfluentSource(bootstrap, group, start, batch_size, pattern)
        except ImportError:
            if client == "confluent":
                raise
    return KafkaPythonSource(bootstrap, group, start, batch_size, pattern)
