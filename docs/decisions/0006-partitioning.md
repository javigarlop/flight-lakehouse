# 0006: Partitioning strategy

- **Status:** Accepted
- **Date:** 2026-11-22 (written up after Phase 4; the layout was set in Phase 2)

## Context

The tables are small. The live feed brings about 25,000 positions a day, around 1 MB of
Parquet, and bronze and silver keep 7 days. Gold holds hourly and daily aggregates.

Partitioning has to serve three things:
- **Retention:** a daily job deletes rows older than 7 days.
- **Queries:** dashboards filter on recent time ranges.
- **File count:** a 30-second trigger commits up to 2,880 times a day, so finer partitions
  multiply the number of tiny files.

## Options

- **Hourly** (`hours(...)`): 24 times the partitions, each with a few kilobytes. More small
  files, no real gain in pruning at this size.
- **Daily** (`days(...)`): a few hundred kilobytes per partition, one partition per day of
  retention.
- **Daily plus `bucket(icao24)`**: speeds up per-aircraft lookups on large tables, but splits
  tiny partitions further.
- **No partitioning**: simplest, but retention would have to rewrite files instead of
  dropping partitions.

## Decision

Partition every bronze and silver table **by day**, using Iceberg's hidden partitioning on the
column that decides its retention:

| Table | Partition |
|---|---|
| `bronze.flight_positions` | `days(ingested_at)` |
| `silver.flight_positions` | `days(event_time)` |
| `silver.rejected_positions` | `days(ingested_at)` |
| `silver.flights` | `days(first_seen)` |

Gold is not partitioned. Its tables are tiny and kept for good, and dbt merges into them on
their unique keys.

## Consequences

- Retention cuts at the start of a UTC day, so each delete drops whole partitions and Iceberg
  only rewrites metadata. Between 7 and 8 days of rows are kept.
- Queries filter on the timestamps themselves, such as `event_time > now() - 1 hour`, and
  Iceberg prunes to the matching days. No query has to know the layout.
- Bronze partitions by arrival time, not event time, so late events land in today's
  partition and are never deleted early.
- Within a day, the streaming writes still leave many small files. Daily compaction rewrites
  each table into a few larger files: from 415 to 4 in the benchmark.
- If the data grows a lot, for example a much larger area, Iceberg's partition evolution can
  add `bucket(icao24)` or switch to hourly partitions without rewriting old data.

## References

- [Iceberg: Partitioning](https://iceberg.apache.org/docs/latest/partitioning/)
- [Iceberg: Partition evolution](https://iceberg.apache.org/docs/latest/evolution/#partition-evolution)
