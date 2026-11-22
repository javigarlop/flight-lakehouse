# 0004: Stream processor

- **Status:** Accepted
- **Date:** 2026-11-22 (written up after Phase 4; the choice was made in Phase 2)

## Context

Positions arrive in Kafka and have to reach Iceberg as bronze (raw, exactly once) and silver
(validated, deduplicated, sessionised into flights). The same engine should also run the
Iceberg maintenance: compaction, retention deletes, snapshot expiry and orphan cleanup. It
has to fit a laptop next to Kafka, Trino and the rest of the stack, and be written in Python
like the rest of the project.

The live feed is slow: one poll every 4 minutes, about 25,000 positions a day. Latency of a
few seconds is far below anything the data can show.

## Options

**Spark Structured Streaming.** Micro-batches on a trigger. Iceberg's Spark integration is
its most complete one: the streaming sink, schema evolution on write, and the maintenance
procedures (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`) are all Spark
SQL. PySpark covers the whole API, including watermarks, `dropDuplicates` and session
windows.

**Apache Flink.** Processes events one at a time, so its latency is lower and its state
handling is more flexible. Iceberg has a Flink sink. But the Python API (PyFlink) is
narrower than the Java and SQL APIs, and Iceberg's table maintenance is written mainly for
Spark, so a Flink pipeline would still need Spark, or Java code, for maintenance.

**Kafka Streams or a plain Python consumer.** Light, but neither writes Iceberg with
exactly-once commits out of the box; that would mean building the sink by hand.

## Decision

Use **Spark Structured Streaming** (PySpark 4.1, Iceberg 1.12) for bronze and silver, and
Spark batch jobs for maintenance.

- Each query keeps its own checkpoint; with Iceberg's sink skipping a batch it already
  committed, a restart neither loses nor duplicates rows. The kill-and-restart tests check
  this for bronze and silver.
- Silver deduplicates on (`icao24`, `event_time`) with a 15-minute watermark and builds
  flights with session windows (20-minute gap).
- The trigger is 30 seconds by default (`TRIGGER_SECONDS`).

## Consequences

- One engine, one image and one set of configs for streaming and maintenance.
- Latency has a floor set by the trigger. The benchmark measures 3.0 s p50 from Kafka to
  bronze with a 5-second trigger, and 8.0 s to silver ([benchmarks](../benchmarks.md));
  with the default 30 seconds a row waits up to one trigger. That's ample for 4-minute polls.
- Each micro-batch writes small files, so compaction is required, not optional. It runs daily
  from Airflow and took query times down 68–91% in the benchmark.
- Spark's JVM costs memory: each streaming job runs a 1 GB driver, more than a plain Python
  consumer would need.
- A Flink version stays on the plan's ideas list, to compare the two on the same recording.

## References

- [Iceberg: Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
- [Iceberg: Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Iceberg: Flink](https://iceberg.apache.org/docs/latest/flink/)
