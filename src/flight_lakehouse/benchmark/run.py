"""The benchmark: max-speed replay through the running pipeline, then Trino before and
after compaction. `make benchmark` runs it (see docs/benchmarks.md).

1. Note each Kafka partition's end offset, replay a recording at max speed and note
   them again. The offsets in between are this run's events, so the measurements
   below ignore anything else in the tables.
2. Wait until bronze holds every one of them and silver's count stops changing.
3. Measure from the tables, through Trino: throughput (rows over the time from the
   first publish to the last row written) and latency per row, from the Kafka
   timestamp (when the replay published it) to bronze's ingested_at and silver's
   processed_at. A max-speed replay compresses hours of recorded event_time into
   seconds, so event_time can't be the start of the clock here; the Kafka
   timestamp is the moment the event entered the pipeline.
4. Time a fixed set of Trino queries, run the compaction, time them again, and
   count the data files on each side.

The replay shifts the recording's times so its last response is "now": otherwise
silver's watermark would discard old recorded events as late data.

Every live service is passed in, so the logic is unit tested with fakes.
"""
# The SQL is built from constants, integers and the topic name, which is quoted.
# ruff: noqa: S608

import logging
import os
import platform
import shlex
import subprocess  # noqa: S404 - runs the configured compaction command
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flight_lakehouse.benchmark.report import (
    OffsetRange,
    offset_filter,
    offset_ranges,
    rate,
    time_queries,
)
from flight_lakehouse.producer.run import Publisher
from flight_lakehouse.replay.replay import replay, shift_to_end_at

log = logging.getLogger(__name__)

BRONZE = "lakehouse.bronze.flight_positions"
SILVER = "lakehouse.silver.flight_positions"
DEFAULT_COMPACT_COMMAND = "make --no-print-directory maintain"

# Queries timed before and after compaction: a full count, an aggregation over every
# row, a group-by like the dashboards' and a selective, recent-only scan.
QUERIES = {
    "silver_count": f"SELECT count(*) FROM {SILVER}",
    "silver_hourly_positions": (
        f"SELECT date_trunc('hour', event_time) AS hour, count(*) AS positions, "
        f"avg(velocity) AS avg_velocity FROM {SILVER} GROUP BY 1 ORDER BY 1"
    ),
    "silver_busiest_aircraft": (
        f"SELECT icao24, count(*) AS positions FROM {SILVER} "
        f"GROUP BY icao24 ORDER BY positions DESC LIMIT 10"
    ),
    "silver_last_hour": (
        f"SELECT icao24, max(event_time) AS last_seen FROM {SILVER} "
        f"WHERE event_time > current_timestamp - INTERVAL '1' HOUR GROUP BY icao24"
    ),
}
# Iceberg metadata tables Trino exposes: one row per live data file.
FILE_TABLES = {
    BRONZE: 'lakehouse.bronze."flight_positions$files"',
    SILVER: 'lakehouse.silver."flight_positions$files"',
}

Execute = Callable[[str], list[tuple]]


@dataclass(frozen=True)
class Services:
    """The live pieces the benchmark drives; fakes stand in for them in tests."""

    publisher: Publisher
    end_offsets: Callable[[], dict[int, int]]  # partition -> next offset
    execute: Execute  # runs SQL on Trino and fetches every row
    compact: Callable[[], None]


@dataclass(frozen=True)
class Settings:
    recording: Path
    topic: str
    repeats: int = 5
    timeout_s: float = 1800
    poll_s: float = 15
    # Silver triggers every 30 s, so four unchanged polls 15 s apart span two triggers.
    stable_polls: int = 4


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def run_filter(ranges: list[OffsetRange], topic: str, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}kafka_topic = {_quote(topic)} AND {offset_filter(ranges, alias)}"


def count_sql(ranges: list[OffsetRange], topic: str) -> str:
    where = run_filter(ranges, topic)
    return (
        f"SELECT (SELECT count(*) FROM {BRONZE} WHERE {where}), "
        f"(SELECT count(*) FROM {SILVER} WHERE {where})"
    )


def _stats(latency: str) -> str:
    return (
        f"approx_percentile({latency}, 0.5), approx_percentile({latency}, 0.95), "
        f"approx_percentile({latency}, 0.99), max({latency})"
    )


def bronze_sql(ranges: list[OffsetRange], topic: str) -> str:
    latency = "to_milliseconds(ingested_at - kafka_timestamp)"
    return (
        f"SELECT count(*), to_milliseconds(max(ingested_at) - min(kafka_timestamp)), "
        f"{_stats(latency)} FROM {BRONZE} WHERE {run_filter(ranges, topic)}"
    )


def silver_sql(ranges: list[OffsetRange], topic: str) -> str:
    # Silver keeps the Kafka offset but not its timestamp, so join back to bronze.
    latency = "to_milliseconds(s.processed_at - b.kafka_timestamp)"
    return (
        f"SELECT count(*), to_milliseconds(max(s.processed_at) - min(b.kafka_timestamp)), "
        f"{_stats(latency)} FROM {SILVER} s JOIN {BRONZE} b "
        f"ON s.kafka_topic = b.kafka_topic AND s.kafka_partition = b.kafka_partition "
        f"AND s.kafka_offset = b.kafka_offset "
        f"WHERE {run_filter(ranges, topic, 's')} AND {run_filter(ranges, topic, 'b')}"
    )


def wait_for_rows(
    count: Callable[[], tuple[int, int]],
    expected: int,
    settings: Settings,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[int, int, float]:
    """Poll until bronze has `expected` rows and silver's count has stopped changing.

    Silver holds fewer rows than bronze (duplicates and rejects don't reach it), so
    "done" there is a count that stays the same for `stable_polls` polls in a row.
    Returns the bronze and silver counts and the seconds waited.
    """
    start = clock()
    previous, unchanged = None, 0
    while True:
        bronze, silver = count()
        if bronze >= expected:
            unchanged = unchanged + 1 if silver == previous else 0
            if unchanged >= settings.stable_polls:
                return bronze, silver, clock() - start
        previous = silver
        if clock() - start > settings.timeout_s:
            raise TimeoutError(
                f"after {settings.timeout_s:.0f} s bronze has {bronze} of {expected} rows "
                f"and silver {silver}; is the pipeline running (make stream)?"
            )
        log.info("Waiting: bronze %d of %d rows, silver %d", bronze, expected, silver)
        sleep(settings.poll_s)


def _ms_to_s(ms: float | None) -> float | None:
    return None if ms is None else ms / 1000


def _latency(row: tuple) -> dict[str, float | None]:
    return dict(zip(("p50", "p95", "p99", "max"), row[2:6], strict=True))


def pipeline_stats(execute: Execute, ranges: list[OffsetRange], topic: str) -> dict[str, Any]:
    (bronze,) = execute(bronze_sql(ranges, topic))
    (silver,) = execute(silver_sql(ranges, topic))
    return {
        "bronze_rows": bronze[0],
        "silver_rows": silver[0],
        "bronze_throughput_eps": rate(bronze[0], _ms_to_s(bronze[1])),
        "silver_throughput_eps": rate(silver[0], _ms_to_s(silver[1])),
        "bronze_latency_ms": _latency(bronze),
        "silver_latency_ms": _latency(silver),
    }


def data_files(execute: Execute) -> dict[str, int]:
    return {
        table: execute(f"SELECT count(*) FROM {files}")[0][0]
        for table, files in FILE_TABLES.items()
    }


def environment() -> dict[str, Any]:
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    except (AttributeError, OSError, ValueError):
        memory = None
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpus": os.cpu_count(),
        "memory_gb": memory and round(memory, 1),
        "python": platform.python_version(),
    }


def run_benchmark(
    services: Services,
    settings: Settings,
    *,
    clock: Callable[[], float] = time.perf_counter,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    env: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run every step and return the results (see report.write_results)."""
    started = now()
    shift_ms = shift_to_end_at(settings.recording, int(started.timestamp() * 1000))

    before = services.end_offsets()
    log.info("Replaying %s at max speed", settings.recording)
    publish_start = clock()
    published = replay(settings.recording, services.publisher, speed=None, shift_ms=shift_ms)
    publish_s = clock() - publish_start
    ranges = offset_ranges(before, services.end_offsets())
    in_kafka = sum(r.count for r in ranges)
    if in_kafka != published:
        # Another producer wrote to the topic meanwhile, so the ranges hold its events too.
        log.warning("Published %d events but the topic grew by %d", published, in_kafka)

    def count() -> tuple[int, int]:
        (row,) = services.execute(count_sql(ranges, settings.topic))
        return row[0], row[1]

    _bronze, _silver, waited_s = wait_for_rows(count, in_kafka, settings, clock, sleep)
    pipeline = pipeline_stats(services.execute, ranges, settings.topic)

    files_before = data_files(services.execute)
    queries_before = time_queries(services.execute, QUERIES, settings.repeats, clock)
    log.info("Compacting")
    compact_start = clock()
    services.compact()
    compaction_s = clock() - compact_start
    files_after = data_files(services.execute)
    queries_after = time_queries(services.execute, QUERIES, settings.repeats, clock)

    return {
        "started_at": started.isoformat(timespec="seconds"),
        "environment": dict(env) if env is not None else environment(),
        "run": {
            "recording": str(settings.recording),
            "topic": settings.topic,
            "events_published": published,
            "events_in_kafka": in_kafka,
            "publish_s": publish_s,
            "publish_rate_eps": rate(published, publish_s),
            "time_shift_ms": shift_ms,
            "offsets": [vars(r) for r in ranges],
            "waited_s": waited_s,
        },
        "pipeline": pipeline,
        "compaction": {
            "duration_s": compaction_s,
            "files_before": files_before,
            "files_after": files_after,
        },
        "queries": {
            "repeats": settings.repeats,
            "sql": QUERIES,
            "before": queries_before,
            "after": queries_after,
        },
    }


# The live services, used by `flight-lakehouse benchmark`.


def kafka_end_offsets(bootstrap: str, topic: str) -> Callable[[], dict[int, int]]:
    def end_offsets() -> dict[int, int]:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "flight-lakehouse-benchmark",
                "enable.auto.commit": False,
            }
        )
        try:
            partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
            return {
                p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in partitions
            }
        finally:
            consumer.close()

    return end_offsets


def trino_execute(host: str, port: int) -> Execute:
    import trino

    connection = trino.dbapi.connect(host=host, port=port, user="benchmark", catalog="lakehouse")

    def execute(sql: str) -> list[tuple]:
        cursor = connection.cursor()
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]

    return execute


def shell_command(command: str) -> Callable[[], None]:
    def run() -> None:
        subprocess.run(shlex.split(command), check=True)  # noqa: S603 - the user's own command

    return run
