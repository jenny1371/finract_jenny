"""Unit tests for lake_writer.compact: many small files become one, no row is lost or duplicated, young files are left alone."""
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lake_writer.compact import compact  # noqa: E402
from lake_writer.writer import SCHEMA  # noqa: E402


def write_file(directory: Path, name: str, offsets: list[int], age_seconds: float) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    rows = [{"topic": "cdc.payments.public.payments", "partition": 0, "offset": o, "op": "c", "ts_ms": 1, "kafka_ts_ms": 2,
             "lsn": o, "pk": f'{{"id":"{o}"}}', "before": None, "after": "{}",
             "ingested_at": datetime(2026, 10, 18, tzinfo=timezone.utc)} for o in offsets]
    path = directory / name
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
    old = time.time() - age_seconds
    os.utime(path, (old, old))
    return path


def offsets_in(directory: Path) -> list[int]:
    return sorted(o for f in directory.glob("*.parquet") for o in pq.read_table(f)["offset"].to_pylist())


def test_small_files_are_merged_into_one_without_losing_or_duplicating_rows(tmp_path):
    day = tmp_path / "raw" / "payments__payments" / "dt=2026-10-18"
    write_file(day, "a.parquet", [5, 6], 600)
    write_file(day, "b.parquet", [0, 1, 2], 600)
    write_file(day, "c.parquet", [3, 4], 600)

    result = compact(tmp_path, min_age_seconds=60)

    assert result == {"partitions": 1, "files_removed": 3, "rows": 7}
    assert len(list(day.glob("*.parquet"))) == 1
    assert offsets_in(day) == list(range(7))
    merged = pq.read_table(next(day.glob("*.parquet")))
    assert merged["offset"].to_pylist() == list(range(7))          # sorted by (partition, offset)
    assert not list(day.glob("*.tmp"))


def test_a_file_that_is_still_being_written_is_left_alone(tmp_path):
    day = tmp_path / "raw" / "payments__payments" / "dt=2026-10-18"
    write_file(day, "old1.parquet", [0, 1], 600)
    write_file(day, "old2.parquet", [2, 3], 600)
    young = write_file(day, "young.parquet", [4], 1)

    result = compact(tmp_path, min_age_seconds=60)

    assert result["files_removed"] == 2
    assert young.exists()
    assert offsets_in(day) == [0, 1, 2, 3, 4]


def test_a_partition_with_a_single_file_is_not_rewritten(tmp_path):
    day = tmp_path / "raw" / "payments__payments" / "dt=2026-10-18"
    only = write_file(day, "only.parquet", [0, 1], 600)

    assert compact(tmp_path, min_age_seconds=60) == {"partitions": 0, "files_removed": 0, "rows": 0}
    assert only.exists()
