# Benchmarks

How fast the pipeline moves positions from Kafka to silver, and how much compaction
speeds up Trino queries. `make benchmark` produces every number here; nothing is
estimated by hand.

## Results

**Pending: run `make benchmark` on a machine with ≥16 GB RAM.**

The development laptop has 8 GB of RAM, which can't hold Kafka, Polaris, SeaweedFS,
both Spark jobs and Trino at once without swapping, and swapping numbers would
mislead. When the run is done, paste `docs/benchmark-results.md` here, fill in the
hardware table, and commit `docs/benchmark-results.json` next to it.

### Hardware

| | |
|---|---|
| Machine | _pending_ |
| CPU | _pending_ |
| RAM | _pending_ |
| Disk | _pending_ |
| OS and Docker | _pending_ |
| Recording | _pending: file, responses, positions, time span_ |

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
