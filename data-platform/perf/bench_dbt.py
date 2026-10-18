"""How long the dbt part of the pipeline takes as the lake grows, and how much memory it needs.

    python data-platform/perf/bench_dbt.py --sizes 10000,100000,1000000 [--memory-limit 12GB] [--threads 4]

For each size: generate a synthetic lake (gen_lake.py), run `dbt run` (models) and `dbt test` (35 tests) against a fresh
DuckDB file, record wall time, peak resident memory of the dbt process tree, the slowest models, and check that the
planted differences were found exactly. Results are appended to data-platform/perf/results-dbt.jsonl.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "perf"))
from gen_lake import BASE, build  # noqa: E402


class PeakMemory:
    """Samples the resident memory of a process and all its children, every 100 ms."""

    def __init__(self, pid: int):
        self.peak, self._stop, self._pid = 0, threading.Event(), pid
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            root = psutil.Process(self._pid)
        except psutil.NoSuchProcess:
            return
        while not self._stop.is_set():
            try:
                total = root.memory_info().rss + sum(c.memory_info().rss for c in root.children(recursive=True))
                self.peak = max(self.peak, total)
            except psutil.Error:
                pass
            time.sleep(0.1)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def dbt(command: list[str], env: dict, variables: dict) -> tuple[float, int, int, str]:
    started = time.time()
    process = subprocess.Popen([sys.executable, "-m", "dbt.cli.main", *command, "--profiles-dir", ".", "--no-use-colors",
                                "--vars", json.dumps(variables)], cwd=ROOT / "dbt", env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    with PeakMemory(process.pid) as memory:
        output = process.communicate()[0]
    return time.time() - started, process.returncode, memory.peak, output


def slowest_models(count: int = 4) -> list[dict]:
    results = json.loads((ROOT / "dbt" / "target" / "run_results.json").read_text())["results"]
    models = [(r["unique_id"].split(".")[-1], r["execution_time"]) for r in results if r["unique_id"].startswith("model.")]
    return [{"model": name, "seconds": round(seconds, 2)} for name, seconds in sorted(models, key=lambda m: -m[1])[:count]]


def bench(size: int, memory_limit: str, threads: int, keep_lake: bool, incremental_step: bool = True) -> dict:
    lake = ROOT / "perf" / f"lake-{size}"
    if not (lake / "expected.json").exists():
        build(size, lake)
    expected = json.loads((lake / "expected.json").read_text())
    warehouse = ROOT / "perf" / f"wh-{size}.duckdb"
    for leftover in ROOT.joinpath("perf").glob(f"wh-{size}.duckdb*"):
        leftover.unlink()
    env = {**os.environ, "WAREHOUSE_PATH": str(warehouse), "DUCKDB_MEMORY_LIMIT": memory_limit, "DBT_THREADS": str(threads)}
    variables = {"lake_path": str(lake), "as_of": expected["as_of"]}
    run_seconds, run_code, run_peak, run_out = dbt(["run"], env, variables)
    models = slowest_models() if run_code == 0 else []
    result = {"payments": size, "events": expected["events"], "lake_files": expected["files"], "lake_mb": expected["lake_mb"],
              "memory_limit": memory_limit, "threads": threads, "dbt_run_seconds": round(run_seconds, 1),
              "dbt_run_exit": run_code, "dbt_run_peak_gb": round(run_peak / 1e9, 2), "slowest_models": models}
    if run_code != 0:
        result["error"] = [line for line in run_out.splitlines() if "rror" in line][:3]
    else:
        test_seconds, test_code, test_peak, test_out = dbt(["test"], env, variables)
        con = duckdb.connect(str(warehouse), read_only=True)
        found = dict(con.execute("select type, count(*) from recon_discrepancies group by 1").fetchall())
        con.close()                                  # dbt needs the file for its next run
        correct = all(found.get(k, 0) == expected[k] for k in
                      ("MISSING_IN_LEDGER", "AMOUNT_MISMATCH", "DUPLICATE_BOOKING", "MISSING_IN_STRIPE"))
        result |= {"dbt_test_seconds": round(test_seconds, 1), "dbt_test_exit": test_code,
                   "dbt_test_peak_gb": round(test_peak / 1e9, 2),
                   "failed_tests": [l.split("FAIL")[0].split()[-1] + " (" + l.split("FAIL")[-1].split()[0] + " rows)"
                                    for l in test_out.splitlines() if " FAIL " in l],
                   "differences_found": found,
                   "differences_exact": correct, "warehouse_mb": round(warehouse.stat().st_size / 1e6, 1)}
    if run_code == 0 and incremental_step:
        grow = max(100, size // 100)
        grown = build(grow, lake, first=size + 1, base=BASE.replace("2026-09-20", "2026-10-18"), seed_days=1,
                      offset_base=10**10, tag="append", append=True)
        incr_seconds, incr_code, incr_peak, _ = dbt(["run"], env, variables)
        incr_test_seconds, incr_test_code, _, _ = dbt(["test"], env, variables)
        con = duckdb.connect(str(warehouse), read_only=True)
        snapshot = {t: con.execute(f"select * from {t} order by all").fetchall()
                    for t in ("stg_payments", "recon_discrepancies", "fct_daily_reconciliation")}
        con.close()
        full_seconds, full_code, full_peak, _ = dbt(["run", "--full-refresh"], env, variables)
        con = duckdb.connect(str(warehouse), read_only=True)
        same = all(snapshot[t] == con.execute(f"select * from {t} order by all").fetchall() for t in snapshot)             if not any(c for c in (incr_code, full_code)) else False
        # stg_payments carries the position of the change (_ingested_at etc.), which differs by design: compare content
        result["incremental"] = {"new_payments": grow, "run_seconds": round(incr_seconds, 1), "run_exit": incr_code,
                                 "run_peak_gb": round(incr_peak / 1e9, 2), "test_seconds": round(incr_test_seconds, 1),
                                 "test_exit": incr_test_code, "full_refresh_seconds": round(full_seconds, 1),
                                 "full_refresh_peak_gb": round(full_peak / 1e9, 2), "same_result_as_full_refresh": same}
    if not keep_lake:
        shutil.rmtree(lake, ignore_errors=True)
    for leftover in ROOT.joinpath("perf").glob(f"wh-{size}.duckdb*"):
        leftover.unlink()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", default="10000,100000,1000000")
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--keep-lake", action="store_true")
    parser.add_argument("--no-incremental-step", action="store_true", help="skip the append-1%-and-rerun measurement")
    args = parser.parse_args()
    for size in (int(s) for s in args.sizes.split(",")):
        outcome = bench(size, args.memory_limit, args.threads, args.keep_lake, not args.no_incremental_step)
        print(json.dumps(outcome))
        with open(ROOT / "perf" / "results-dbt.jsonl", "a") as f:
            f.write(json.dumps(outcome) + "\n")
