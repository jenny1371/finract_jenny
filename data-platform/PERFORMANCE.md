# Performance of the data platform

**Status: checkpoint, not finished.** The measurements below were run; the small-file test, the limit of this laptop and
the write-up in the main README are still to do (list at the end). Everything is one laptop (15.6 GB RAM), one Postgres,
one Kafka broker, one Debezium, DuckDB with a 12 GB memory limit, and synthetic data. These are relative results that
show where the bottleneck is and why, not production numbers.

Rows are written straight into the source database (not through the API), so the backend is not what is measured.

## 1. CDC latency: commit in Postgres to Parquet file on disk

[`perf/bench_cdc_latency.py`](perf/bench_cdc_latency.py): a steady write rate for 45 s, three timestamps per row (commit
time from the WAL, Debezium's Kafka record time, the time the file was written). All rows landed in every run.

After the writer optimization of section 2 (flush every 2 s unless stated):

| rows/s (achieved) | end-to-end p50 / p99 | Debezium p50 / p99 | writer p50 / p99 | files in 45 s |
|---|---|---|---|---|
| 1,000 (990) | 1.7 s / 3.1 s | 0.51 s / 0.91 s | 1.2 s / 2.2 s | 18 |
| 5,000 (4,952) | 1.1 s / 2.0 s | 0.55 s / 0.97 s | 0.36 s / 1.5 s | 39 |
| 10,000 (9,639) | 0.9 s / 1.5 s | 0.48 s / 0.86 s | 0.37 s / 0.96 s | 68 |
| 20,000 (17,017) | 0.9 s / 1.5 s | 0.41 s / 0.80 s | 0.40 s / 1.2 s | 117 |
| 1,000, flush every 0.5 s | 1.0 s / 1.6 s | 0.47 s / 0.90 s | 0.64 s / 0.85 s | 45 |

Findings:
- Debezium is not the bottleneck: about 0.5 s at every rate (its default poll interval is 500 ms), p99 under 1 s.
- At low rates the writer's share is the flush interval: waiting for the next flush. A 0.5 s flush gives p99 1.6 s at
  1,000 rows/s, at the price of 2.5 times more files (45 against 18 in 45 s; 172,800 files per table per day).
- Before the optimization of section 2 the writer was the bottleneck: at 5,000 rows/s p50 6.0 s / p99 8.0 s, and at 10,000
  rows/s it fell behind (p50 30.8 s, p99 53.9 s). Raw: `results-cdc-latency-before-optimization.jsonl`.
- A first attempt gave 25 to 85 s at 1,000 rows/s. That was my benchmark's mistake, not the pipeline: each run's new
  consumer group started from the beginning of the topic and first read the previous runs' rows. The runs were thrown away
  and repeated with the group starting at the end of the topic
  (`results-cdc-latency-DISCARDED-contaminated-by-backlog.jsonl` is kept to show it).
- One flush interval of 10 s was measured only before the optimization (p50 3.2 s, p99 10.6 s at 1,000 rows/s, 9 files).
  The writer also flushes when 5,000 rows are buffered, which is why p50 is below half of 10 s.

## 2. Writer throughput: a backlog in Kafka, then how fast it clears

[`perf/bench_writer.py`](perf/bench_writer.py): 300,000 rows inserted with one SQL statement, which Debezium delivered to
Kafka in 31.7 s (a transaction is only streamed once it is committed, so this is a property of one huge transaction, not
a ceiling: section 1 shows 17,000 rows/s of small transactions landing in 1.4 s). Then lake_writer drains the backlog.

| Kafka client | batch size | rows/s | files for 300,000 rows |
|---|---|---|---|
| kafka-python (pure Python) | 1,000 / 5,000 / 20,000 / 50,000 | 3,600 / 5,200 / 5,100 / 5,200 | 247 / 65 / 20 / 11 |
| confluent-kafka (librdkafka, C) | 1,000 / 5,000 / 20,000 / 50,000 | 16,800 / 21,000 / 20,200 / 20,300 | 308 / 66 / 21 / 12 |

Where the time goes ([`perf/bench_writer_stages.py`](perf/bench_writer_stages.py), microseconds per row):

| stage | kafka-python | confluent-kafka | confluent-kafka + orjson |
|---|---|---|---|
| fetch (Kafka client) | 204 | 16 | 16 |
| parse (my code) | 37 | 29 | 14 |
| write Parquet | 6 | 4 | 4 |
| commit offsets | about 0 | about 0 | about 0 |
| **rows/s** | **about 4,000** | **about 19,500** | **about 28,000** |

The writer was limited by the Kafka client library (81 % of its time), not by file writing or batch size. The fix is the
C client (`lake_writer/sources.py`, default when installed) and orjson. Batch size only changes the number of files
(about 12 to 300 files for 300,000 rows), not the speed.

An earlier bug the measurements exposed: the first writer committed Kafka offsets for records it had not written yet. It
now commits only offsets of rows that are on disk.

## 3. dbt reconciliation time and memory as the lake grows

[`perf/bench_dbt.py`](perf/bench_dbt.py) over [`perf/gen_lake.py`](perf/gen_lake.py): a synthetic lake with about 8 events
per payment (payments, orders, ledger entries, Fineract deposit and note) and 350 planted differences per 10,000 payments;
every run checks that the differences found are exactly the planted ones.

**Before: staging models were views (full recompute), 4 dbt threads** (`results-dbt-baseline-views.jsonl`):

| payments | events | `dbt run` | peak memory | `dbt test` (35 tests) | peak memory |
|---|---|---|---|---|---|
| 10,000 | 80 thousand | 4.3 s | 0.36 GB | 5.0 s | 0.38 GB |
| 100,000 | 0.8 million | 8.2 s | 0.96 GB | 9.1 s | 1.03 GB |
| 1,000,000 | 8 million | 23.4 s | 4.31 GB | **160.7 s** | 6.55 GB |

The models grow about linearly; the tests do not. Staging was a view, so each of the 35 tests recomputed the current
state from all raw events.

**After: staging models are incremental tables** (`results-dbt.jsonl`, 4 threads):

| payments | `dbt run` (first build) | peak | `dbt test` | peak | then +1 % new data: incremental run vs full refresh |
|---|---|---|---|---|---|
| 10,000 | 4.9 s | 0.28 GB | 5.2 s | 0.20 GB | 5.1 s vs 5.1 s |
| 100,000 | 8.3 s | 0.76 GB | 5.1 s | 0.21 GB | 5.4 s (0.30 GB) vs 6.6 s (0.77 GB) |
| 1,000,000 | 21.6 s | 3.7 GB | **5.9 s** | 0.39 GB | **6.4 s (1.1 GB) vs 22.5 s (4.1 GB)** |

- Tests at 1,000,000 payments: 160.7 s to 5.9 s.
- About 4 to 5 s of every run is dbt starting up (10,000 payments takes as long as 100,000).
- The incremental result equals a full refresh at every size (compared table by table).
- How it works: a run reads only files written since the previous run (minus a 15-minute lookback), and a row is replaced
  only by a change with a higher log position, so a late or repeated file cannot move a row backwards. Tested offline
  (`tests/test_incremental.py`); removing that guard makes two of the four tests fail.

**Threads.** DuckDB already uses every core, so dbt threads multiply them. First build, 12 GB limit:

| payments (events) | dbt threads | `dbt run` | peak memory |
|---|---|---|---|
| 3,000,000 (24 million) | 4 | 204 s | 6.84 GB |
| 3,000,000 | 2 | 69 s | 6.64 GB |
| 3,000,000 | **1** | **61 s** | 5.53 GB |
| 6,000,000 (48 million, 2.1 GB of Parquet) | 1 | 292 s (tests 11 s) | 6.52 GB |

Four threads made 3,000,000 payments three times slower than one; the staging models took 90 to 190 s each instead of
10 to 17 s. The default is now one thread. At 1,000,000 payments the difference is small (20 s with 4, 23.5 s with 1).

## 4. DuckDB memory

First build of 1,000,000 payments (8 million events):

| memory limit | 4 threads | 1 thread |
|---|---|---|
| 12 GB | 19.4 s, peak 4.16 GB | |
| 4 GB | 20.7 s, peak 3.8 GB | |
| 2 GB | **fails** (out of memory in `stg_orders`) | 23.5 s, peak 2.24 GB |
| 1 GB | fails | fails |
| 512 MB | fails | fails |

- The first build (or a full refresh, or a backfill) holds the history in memory: about 2 GB per 8 million events with one
  thread. Ordinary incremental runs do not: 1.1 GB at 1,000,000 payments with 1 % new data.
- The upper limit of this laptop has **not been found**: 6,000,000 payments (48 million events) still fit, at 6.5 GB.

## To do (next session)

1. Small files: [`perf/bench_small_files.py`](perf/bench_small_files.py) is written and not run. It shatters a lake into
   files of 100, 500 and 5,000 rows, times a scan and `dbt run`, compacts (`lake_writer/compact.py`, written and not yet
   tested) and times them again.
2. Test `compact` (a unit test and one run on the lake the CDC benchmark leaves behind).
3. Find where one laptop stops: 12 million payments, or lower the memory limit at 6 million.
4. Check the marts (`recon_discrepancies`, `fct_daily_transaction_volume`) as separate costs at 3 and 6 million.
5. Re-run the end-to-end pipeline (Debezium to Dagster) once with the new defaults (C Kafka client, one dbt thread).
6. Update `README.md` here and the main README: the writer's follow mode, client choice and compaction, incremental
   staging, a short performance section with these tables, and the questions an interviewer will ask ("what if the data is
   100 times bigger": the first build and backfills need memory in proportion to history, incremental runs do not; the
   next steps are partitioned backfills, or a warehouse).
7. Add psutil (`perf/requirements.txt`) and the perf commands to the README.
