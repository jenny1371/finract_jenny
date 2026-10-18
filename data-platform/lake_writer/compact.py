"""Merges the many small Parquet files a low-latency writer leaves into one file per table and day.

    python -m lake_writer.compact --lake ./lake [--min-age-seconds 60]

Why: every flush writes one file per (table, day, Kafka partition). Flushing every 2 s for a day is 43,200 files per
table, and an engine that opens every file pays for that on every query. Compaction trades them for a few large files.

Safe to run while the writer is running and while dbt is reading:
- only files older than --min-age-seconds are touched, so a file that is still being written is left alone;
- the merged file is written first (atomically) and the originals are deleted afterwards, so a reader never misses rows;
  for a moment it may see both, which is harmless because staging de-duplicates on (topic, partition, offset).
"""
from __future__ import annotations

import argparse
import os
import time
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lake_writer.writer import SCHEMA


def compact(lake: Path, min_age_seconds: float = 60.0, min_files: int = 2) -> dict[str, int]:
    """Returns {"partitions": merged partition directories, "files_removed": n, "rows": n}."""
    merged = {"partitions": 0, "files_removed": 0, "rows": 0}
    cutoff = time.time() - min_age_seconds
    for directory in sorted((lake / "raw").glob("*/dt=*")):
        files = [f for f in directory.glob("*.parquet") if f.stat().st_mtime < cutoff]
        if len(files) < min_files:
            continue
        tables = []
        for f in files:
            table = pq.read_table(f)
            for field in SCHEMA:                       # files written before a column existed get it as nulls
                if field.name not in table.column_names:
                    table = table.append_column(field, pa.nulls(table.num_rows, field.type))
            tables.append(table.select(SCHEMA.names).cast(SCHEMA))
        table = pa.concat_tables(tables).sort_by([("partition", "ascending"), ("offset", "ascending")])
        name = f"compact-{table['offset'][0]}-{table['offset'][-1]}-{uuid.uuid4().hex[:8]}.parquet"
        temporary = directory / (name + ".tmp")
        pq.write_table(table, temporary, compression="zstd")
        os.replace(temporary, directory / name)
        for f in files:
            f.unlink()
        merged["partitions"] += 1
        merged["files_removed"] += len(files)
        merged["rows"] += table.num_rows
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--lake", default="./lake")
    parser.add_argument("--min-age-seconds", type=float, default=60.0)
    args = parser.parse_args()
    print(compact(Path(args.lake), args.min_age_seconds))


if __name__ == "__main__":
    main()
