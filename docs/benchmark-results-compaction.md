First run, with the default 30-second trigger. Its compaction numbers are the ones
reported in [benchmarks.md](benchmarks.md). This run predates the replay's time squeeze, so
silver's watermark dropped all but 255 rows: its silver throughput and latency don't
describe the pipeline.

Run at 2026-11-22T07:04:27+00:00 on Linux-7.0.0-43-generic-x86_64-with-glibc2.43, 6 CPUs, 15.0 GB RAM. Recording `data/recordings/live.jsonl`, replayed once at max speed.

| Metric | Value |
|---|---|
| Events published | 30,854 |
| Publish rate (events/s) | 11,272.1 |
| Rows in bronze | 30,854 |
| Rows in silver (after dedup) | 255 |
| Bronze throughput (events/s) | 973.8 |
| Silver throughput (events/s) | 4.3 |
| Latency, Kafka to bronze: p50 / p95 / p99 / max (s) | 1.37 / 29.87 / 29.98 / 30.02 |
| Latency, Kafka to silver: p50 / p95 / p99 / max (s) | 59.04 / 59.05 / 59.05 / 59.05 |
| Compaction run time (s) | 138.8 |

| Table | Data files before compaction | After | Change |
|---|---|---|---|
| `lakehouse.bronze.flight_positions` | 415 | 4 | -99% |
| `lakehouse.silver.flight_positions` | 415 | 4 | -99% |

Trino query time, median of 5 runs after a warm-up:

| Query | Before compaction (ms) | After (ms) | Change |
|---|---|---|---|
| `silver_count` | 584.9 | 54.2 | -91% |
| `silver_hourly_positions` | 594.2 | 79.7 | -87% |
| `silver_busiest_aircraft` | 538.2 | 81.9 | -85% |
| `silver_last_hour` | 218.8 | 70.2 | -68% |
