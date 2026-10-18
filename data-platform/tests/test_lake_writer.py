"""Unit tests for the parts of lake_writer that need no Kafka. The Kafka path is covered by the end-to-end run in
data-platform/README.md (real Debezium, real Kafka, real Postgres and Fineract)."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lake_writer.writer import SCHEMA, flush, parse, primary_key  # noqa: E402

NOW = datetime(2026, 10, 18, 3, 0, tzinfo=timezone.utc)
TS = int(datetime(2026, 10, 11, 9, 9, tzinfo=timezone.utc).timestamp() * 1000)
TOPIC = "cdc.payments.public.payments"


def event(op="c", before=None, after=None, lsn=100, wrapped=False):
    envelope = {"before": before, "after": after, "source": {"ts_ms": TS, "lsn": lsn}, "op": op, "ts_ms": TS + 5}
    return json.dumps({"schema": {}, "payload": envelope} if wrapped else envelope).encode()


def test_insert_is_kept_as_sent_and_dated_by_the_source_commit_time():
    change = parse(TOPIC, 0, 7, b'{"id":"a"}', event(after={"id": "a", "amount": 100}), NOW)
    assert (change.db, change.table, change.day) == ("payments", "payments", "2026-10-11")
    assert change.row["op"] == "c" and change.row["offset"] == 7 and change.row["lsn"] == 100
    assert json.loads(change.row["after"]) == {"id": "a", "amount": 100}
    assert change.row["before"] is None


def test_envelope_with_schema_wrapper_gives_the_same_row():
    plain = parse(TOPIC, 0, 1, b'{"id":"a"}', event(after={"id": "a"}), NOW)
    wrapped = parse(TOPIC, 0, 1, b'{"schema":{},"payload":{"id":"a"}}', event(after={"id": "a"}, wrapped=True), NOW)
    assert plain.row == wrapped.row


def test_primary_key_is_canonical_json():
    assert primary_key(b'{"b":1,"a":2}') == '{"a":2,"b":1}'
    assert primary_key(b'{"schema":{},"payload":{"id":"x"}}') == '{"id":"x"}'
    assert primary_key(None) is None


def test_delete_keeps_the_before_image_and_has_no_after():
    change = parse(TOPIC, 0, 9, b'{"id":"a"}', event(op="d", before={"id": "a"}), NOW)
    assert change.row["op"] == "d" and change.row["after"] is None
    assert json.loads(change.row["before"]) == {"id": "a"}


@pytest.mark.parametrize("topic,value", [
    (TOPIC, None),                                              # tombstone
    ("__debezium-heartbeat.cdc.payments", event(after={})),     # not a change topic
    ("other.topic", event(after={})),
    (TOPIC, b'{"ts_ms": 1}'),                                   # no op: not a change event
])
def test_records_that_are_not_change_events_are_skipped(topic, value):
    assert parse(topic, 0, 1, b"{}", value, NOW) is None


def test_flush_writes_one_file_per_table_day_and_partition_and_leaves_no_temporary_files(tmp_path):
    changes = [parse(TOPIC, 0, i, b'{"id":"%d"}' % i, event(after={"id": str(i)}), NOW) for i in range(3)]
    changes.append(parse("cdc.orders.public.orders", 0, 0, b'{"id":"o"}', event(after={"id": "o"}), NOW))
    assert flush(tmp_path, changes) == 2
    files = sorted(tmp_path.rglob("*.parquet"))
    assert [f.parent.parent.name for f in files] == ["orders__orders", "payments__payments"]
    assert all(f.parent.name == "dt=2026-10-11" for f in files)
    assert not list(tmp_path.rglob("*.tmp"))
    table = pq.read_table(files[1])
    assert table.schema.equals(SCHEMA) and table.num_rows == 3
    assert sorted(table.column("offset").to_pylist()) == [0, 1, 2]
