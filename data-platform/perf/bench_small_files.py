"""The small-file problem: what many tiny Parquet files cost the reader, and what compaction buys back.

    python data-platform/perf/bench_small_files.py --payments 100000 --rows-per-file 100,500,5000

A low-latency writer flushes often, so it leaves many small files (flush every 0.5 s = 172,800 files per table per day).
For each --rows-per-file the lake of --payments payments (gen_lake.py) is rewritten as files of about that size, then:
    1. time one scan of the payments events (count + group by), and one `dbt run`,
    2. compact the lake (lake_writer.compact) and time the same two things again.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "perf"))
from gen_lake import build  # noqa: E402
from lake_writer.compact import compact  # noqa: E402


def shatter(source: Path, target: Path, rows_per_file: int) -> int:
    """Rewrites every partition of `source` as files of about rows_per_file rows, like a writer that flushes often."""
    if target.exists():
        shutil.rmtree(target)
    count = 0
    for f in sorted(source.glob("raw/*/dt=*/*.parquet")):
        table = pq.read_table(f)
        out = target / f.relative_to(source).parent
        out.mkdir(parents=True, exist_ok=True)
        for start in range(0, table.num_rows, rows_per_file):
            pq.write_table(table.slice(start, rows_per_file), out / f"part-{count}.parquet", compression="zstd")
            count += 1
    (target / "expected.json").write_text((source / "expected.json").read_text())
    return count


def scan_seconds(lake: Path) -> float:
    pattern = (lake / "raw" / "payments__payments" / "*" / "*.parquet").as_posix()
    started = time.time()
    duckdb.connect().execute(f"select op, count(*), count(distinct pk) from read_parquet('{pattern}', union_by_name = true) group by 1").fetchall()
    return time.time() - started


def dbt_run_seconds(lake: Path, warehouse: Path) -> tuple[float, int]:
    expected = json.loads((lake / "expected.json").read_text())
    for leftover in warehouse.parent.glob(warehouse.name + "*"):
        leftover.unlink()
    env = {**os.environ, "WAREHOUSE_PATH": str(warehouse)}
    started = time.time()
    result = subprocess.run([sys.executable, "-m", "dbt.cli.main", "run", "--profiles-dir", ".", "--no-use-colors", "--vars",
                             json.dumps({"lake_path": str(lake), "as_of": expected["as_of"]})],
                            cwd=ROOT / "dbt", env=env, capture_output=True, text=True)
    return time.time() - started, result.returncode


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payments", type=int, default=100000)
    parser.add_argument("--rows-per-file", default="100,500,5000")
    args = parser.parse_args()
    base = ROOT / "perf" / f"lake-{args.payments}"
    if not (base / "expected.json").exists():
        build(args.payments, base)
    results = []
    for rows in (int(r) for r in args.rows_per_file.split(",")):
        lake = ROOT / "perf" / f"lake-small-{rows}"
        files = shatter(base, lake, rows)
        before = {"scan_seconds": round(scan_seconds(lake), 2)}
        before["dbt_run_seconds"], code = (round(v, 1) for v in dbt_run_seconds(lake, ROOT / "perf" / "wh-small.duckdb"))
        started = time.time()
        merged = compact(lake, min_age_seconds=0)
        compact_seconds = time.time() - started
        after_files = len(list(lake.rglob("*.parquet")))
        after = {"scan_seconds": round(scan_seconds(lake), 2)}
        after["dbt_run_seconds"], code2 = (round(v, 1) for v in dbt_run_seconds(lake, ROOT / "perf" / "wh-small.duckdb"))
        entry = {"payments": args.payments, "rows_per_file": rows, "files_before": files, "files_after": after_files,
                 "before_compaction": before, "after_compaction": after, "compact_seconds": round(compact_seconds, 1),
                 "dbt_exit": [int(code), int(code2)]}
        print(json.dumps(entry), flush=True)
        results.append(entry)
        shutil.rmtree(lake, ignore_errors=True)
    for leftover in (ROOT / "perf").glob("wh-small.duckdb*"):
        leftover.unlink()
    (ROOT / "perf" / "results-small-files.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
