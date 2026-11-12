"""Silver end to end: duplicates collapse, bad rows are rejected, positions become flights.

Kafka -> bronze -> silver, on a fresh topic and fresh tables. Both jobs run with --once,
twice: the second round resumes from the checkpoints and closes the flights.
Needs the stack running and the Spark image built (make up && make spark-build).
Run with: make test-stream
"""

import subprocess
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime

import pytest

from flight_lakehouse.config import Settings
from flight_lakehouse.producer.kafka import (
    PositionPublisher,
    ensure_topic,
    register_schema,
    registry_client,
    value_subject,
)
from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import read_recording, replay
from tests.integration.helpers import (
    COMPOSE,
    host_catalog,
    purge_tables,
    spark_submit,
    table_rows,
)

pytestmark = [pytest.mark.integration, pytest.mark.spark]

REPLAYS = 2  # every position arrives twice


@pytest.fixture
def catalog():
    return host_catalog()


@pytest.fixture
def pipeline(catalog):
    """A fresh topic plus names for fresh bronze and silver tables and checkpoints."""
    from confluent_kafka.admin import AdminClient

    settings = Settings.from_env()
    suffix = uuid.uuid4().hex[:8]
    topic = f"test.silver.{suffix}"
    ensure_topic(settings.kafka_bootstrap, topic, partitions=3, retention_hours=1)
    register_schema(settings.schema_registry_url, topic)
    tables = {
        "bronze": f"test_silver.bronze_{suffix}",
        "positions": f"test_silver.positions_{suffix}",
        "rejected": f"test_silver.rejected_{suffix}",
        "flights": f"test_silver.flights_{suffix}",
    }
    yield settings, topic, tables, f"/checkpoints/test-{suffix}"

    purge_tables(catalog, *tables.values())
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    admin.delete_topics([topic])[topic].result(timeout=30)
    registry = registry_client(settings.schema_registry_url)
    registry.delete_subject(value_subject(topic), permanent=False)
    registry.delete_subject(value_subject(topic), permanent=True)


def publish_garbage(settings: Settings, topic: str) -> None:
    """A message with a valid wire header but a body that isn't Avro."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    producer.produce(topic, key=b"garbage", value=b"\x00\x00\x00\x00\x01not avro")
    assert producer.flush(30) == 0


def run_job(*command: str) -> None:
    result = subprocess.run(
        [*COMPOSE, "run", "--rm", "spark-bronze", *command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-3000:]


def run_pipeline(topic: str, tables: dict[str, str], checkpoint: str) -> None:
    spark = {name: f"lakehouse.{table}" for name, table in tables.items()}
    run_job(
        *spark_submit(
            "bronze",
            f"--topic={topic}",
            f"--table={spark['bronze']}",
            f"--checkpoint={checkpoint}/bronze",
            "--once",
        )
    )
    run_job(
        *spark_submit(
            "silver",
            f"--source={spark['bronze']}",
            f"--positions-table={spark['positions']}",
            f"--rejected-table={spark['rejected']}",
            f"--flights-table={spark['flights']}",
            f"--checkpoint-dir={checkpoint}",
            "--once",
            "--max-files=2",  # several micro-batches, so state has to carry across them
        )
    )


def flight_id(icao24: str, event_time_ms: int) -> str:
    started = datetime.fromtimestamp(event_time_ms / 1000, UTC)
    return f"{icao24}-{started:%Y%m%dT%H%M%S}"


def test_silver_deduplicates_rejects_and_sessionises(pipeline, catalog, recording):
    settings, topic, tables, checkpoint = pipeline
    publisher = PositionPublisher(settings.kafka_bootstrap, settings.schema_registry_url, topic)
    for _ in range(REPLAYS):
        replay(recording, publisher, speed=None)
    fixture = [p for response in read_recording(recording) for p in to_positions(response)]
    expected = {(p["icao24"], p["event_time"]) for p in fixture}
    # One position off the globe; the Avro schema accepts it, silver must not.
    bad = {**to_positions(next(iter(read_recording(recording))))[0], "latitude": 95.0}
    publisher.publish([bad])
    publisher.flush()
    publish_garbage(settings, topic)

    run_pipeline(topic, tables, checkpoint)

    positions = table_rows(catalog, tables["positions"])
    keys = [
        (icao24, int(event_time.timestamp() * 1000))
        for icao24, event_time in zip(
            positions["icao24"].to_pylist(), positions["event_time"].to_pylist(), strict=True
        )
    ]
    duplicated = [key for key, count in Counter(keys).items() if count > 1]
    assert not duplicated, f"{len(duplicated)} positions written more than once"
    assert set(keys) == expected

    rejected = table_rows(catalog, tables["rejected"])
    reasons = sorted(tuple(r) for r in rejected["reject_reasons"].to_pylist())
    assert reasons == [("decode_failed",), ("latitude_out_of_range",)]

    # Every flight is still open: nothing has moved event time past the session gap.
    assert table_rows(catalog, tables["flights"]).num_rows == 0

    # Round 2: one aircraft seen two hours later moves the watermark past every
    # fixture flight, so they close. Its own flight stays open.
    later = {**fixture[0], "icao24": "ffffff", "callsign": "CLOCK"}
    later["event_time"] = later["received_at"] = max(p["event_time"] for p in fixture) + 7_200_000
    publisher.publish([later])
    publisher.flush()
    run_pipeline(topic, tables, checkpoint)

    by_aircraft = defaultdict(list)
    for p in fixture:
        by_aircraft[p["icao24"]].append(p)
    expected_flights = {
        flight_id(icao24, min(p["event_time"] for p in ps)): len({p["event_time"] for p in ps})
        for icao24, ps in by_aircraft.items()
    }
    rows = table_rows(catalog, tables["flights"]).to_pylist()
    assert {f["flight_id"]: f["positions"] for f in rows} == expected_flights
    assert len(rows) == len(expected_flights), "a flight was written twice"

    # Aircraft on the ground at Barcelona for the whole recording.
    parked = {f["icao24"] for f in rows if f["departure_airport"] == f["arrival_airport"] == "LEBL"}
    assert {"344045", "344156", "3c6655"} <= parked
