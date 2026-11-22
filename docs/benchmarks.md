# Benchmarks

How fast the pipeline moves positions from Kafka to silver, and how much compaction
speeds up Trino queries. `make benchmark` produces every number here; nothing is
estimated by hand.

## Results

Two runs on 2026-11-22, one after the other, on the home server that runs the deployed
stack. Airflow, Grafana, Prometheus and the web UIs were stopped for both, so the
benchmark had the machine's memory to itself (about 9.5 GB available, no swapping).

- **Pipeline numbers** come from the second run, with the 5-second trigger
  (`TRIGGER_SECONDS=5 make benchmark`), so they measure the Spark jobs rather than
  the 30-second schedule.
- **Compaction numbers** come from the first run. It found the tables as the live
  pipeline's small micro-batch writes had left them, 415 data files each; the second
  run, minutes after that compaction, started from 11 files and shows little.

### Pipeline: max-speed replay of 31,090 positions

| Metric | Value |
|---|---|
| Publish rate into Kafka (events/s) | 10,875 |
| Bronze throughput (events/s) | 7,319 |
| Silver throughput (events/s) | 3,344 |
| Rows: bronze / silver | 31,090 / 31,088 |
| Latency, Kafka to bronze: p50 / p95 / p99 / max (s) | 2.99 / 4.12 / 4.22 / 4.25 |
| Latency, Kafka to silver: p50 / p95 / p99 / max (s) | 8.04 / 9.17 / 9.27 / 9.30 |

Throughput is rows over the time from the first event entering Kafka to the last row
written, so it includes the wait for the first trigger and is a lower bound on what
the jobs can process. Silver reads bronze, so its latency covers both jobs. With the
default 30-second trigger the first run's bronze latency was 1.4 s at p50 and 30 s at
p95: a row waits for the next trigger.

### Compaction: Trino query time before and after

| Table | Data files before | After | Change |
|---|---|---|---|
| `lakehouse.bronze.flight_positions` | 415 | 4 | -99% |
| `lakehouse.silver.flight_positions` | 415 | 4 | -99% |

| Query (median of 5 runs after a warm-up) | Before (ms) | After (ms) | Change |
|---|---|---|---|
| `silver_count` | 584.9 | 54.2 | -91% |
| `silver_hourly_positions` | 594.2 | 79.7 | -87% |
| `silver_busiest_aircraft` | 538.2 | 81.9 | -85% |
| `silver_last_hour` | 218.8 | 70.2 | -68% |

The maintenance run (retention, compaction and snapshot expiry on every table) took
139 s. The raw results of the pipeline run are in
[`benchmark-results.json`](benchmark-results.json) and
[`benchmark-results.md`](benchmark-results.md); the first run's are in
[`benchmark-results-compaction.md`](benchmark-results-compaction.md).

### Hardware

| | |
|---|---|
| Machine | Lenovo ThinkCentre M720q, single node, everything in Docker Compose |
| CPU | Intel Core i5-9500T @ 2.20 GHz, 6 cores |
| RAM | 16 GB (15 GB usable) |
| Disk | 512 GB NVMe SSD (Samsung PM9A1, MZVL2512HCJQ) |
| OS and Docker | Ubuntu, Linux 7.0.0-43, Docker 29.10.0 |
| Recording | `data/recordings/live.jsonl`: live adsb.lol polls around Barcelona, 31,090 positions, squeezed into 5 minutes |

## Method

`make benchmark` stops the live producer, makes sure the Spark bronze and silver
jobs are running, and starts Trino. Then `flight-lakehouse benchmark`
([src/flight_lakehouse/benchmark/](../src/flight_lakehouse/benchmark/)) runs these steps:

1. **Replay at max speed.** It notes every partition's end offset on the topic,
   replays the recording with the replay tool's max-speed mode (no sleeps between
   responses) and notes the end offsets again. The offsets in between are this
   run's events; every measurement below is filtered to them, so older data in the
   tables doesn't count. The recording's times are squeezed into 5 minutes (order
   and relative gaps kept) and shifted so its last response happens now: silver's
   15-minute watermark would otherwise drop all but the last few minutes of a long
   recording as late data. Each position's own time is squeezed the same way, so a
   position repeated in two responses keeps one `event_time` and silver's
   deduplication still drops the copy.
2. **Wait for the pipeline.** It polls Trino until bronze holds every event and
   silver's row count has stayed the same for four polls 15 seconds apart (two
   silver triggers). Silver holds fewer rows than bronze: duplicates of the same
   (icao24, event_time) and rejected rows don't reach it.
3. **Throughput and latency**, from the tables:
   - *Publish rate*: events over the replay's wall-clock time, flush included.
   - *Bronze and silver throughput*: rows over the time from the first event's
     Kafka timestamp to the last row's `ingested_at` (bronze) or `processed_at`
     (silver).
   - *End-to-end latency*, per row: from the Kafka timestamp to bronze's
     `ingested_at`, and to silver's `processed_at` (joined back to bronze by topic,
     partition and offset), as p50, p95, p99 and max (Trino's `approx_percentile`).
     A max-speed replay squeezes hours of recorded `event_time` into seconds, so
     `event_time` can't start the clock here; the Kafka timestamp is when the
     event entered the pipeline. For live data, the Pipeline freshness dashboard
     shows latency from `event_time` (to bronze, and to gold at each dbt run).
4. **Query time before and after compaction.** It counts the data files of the
   bronze and silver position tables (Trino's `$files` metadata tables), times four
   queries on silver (a count, an hourly aggregate over every row, a top-10
   group-by and a last-hour scan; the SQL is in the results file), runs the
   project's maintenance job (`make maintain`'s command: retention, compaction,
   snapshot expiry), then counts and times again. Each query runs once to warm up
   and then five times; the report shows the median.

The results go to `docs/benchmark-results.json` (every raw number, the SQL, the
offsets and the machine) and `docs/benchmark-results.md` (the tables).

### Limits

- Spark's micro-batch trigger (30 s, `TRIGGER_SECONDS`) sets a floor on latency:
  a row waits for the next trigger. Lower it to measure the jobs rather than the
  schedule.
- The stack shares one machine, so the replay, Kafka, Spark and Trino compete for
  CPU. The numbers describe this single-node setup, not a cluster.
- Compaction rewrites more than this run's files (it maintains every table), so
  its run time depends on what the tables already hold.
- Squeezing puts a day of positions into 5 minutes, so `silver.flights` merges each
  aircraft's replayed positions into one flight, and the live dashboards show the
  replay as a burst of traffic until retention removes it. The benchmark doesn't
  measure flights.
- Silver's watermark can still drop a few events as late; the report shows bronze
  and silver row counts side by side, so that would be visible.

## How to run

On a machine with at least 16 GB of RAM:

```sh
make install
make up                # Kafka, schema registry, SeaweedFS, Polaris
make trino && make dbt-seed   # silver's flights need reference.airports
# A recording with real traffic: an hour or more of live polls (the test fixture
# has three responses, too few to measure anything).
uv run --env-file .env flight-lakehouse produce --no-publish --record data/recordings/live.jsonl --polls 30
make benchmark         # or: make benchmark BENCH_RECORDING=path/to/recording.jsonl
```

`TRIGGER_SECONDS=5 make benchmark` runs both Spark jobs with a 5-second trigger, so
throughput and latency measure the jobs rather than the 30-second schedule; `make
stream` afterwards puts them back on 30 seconds.

`make grafana` while it runs shows the replay on the Streaming dashboard: *Rows per
second, 1-minute rate* and *Peak bronze rows per second* (set the time range to the
run).

Options (`uv run flight-lakehouse benchmark --help`): `--repeats` (timed runs per
query, default 5), `--timeout` (seconds to wait for the pipeline, default 1800),
`--compact-command` and `--output-dir` (default `docs`).
