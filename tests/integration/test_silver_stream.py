"""Silver end to end: replayed duplicates collapse to one row each, bad rows are rejected.

Kafka -> bronze -> silver, on a fresh topic and fresh tables. Both jobs run with --once.
Needs the stack running and the Spark image built (make up && make spark-build).
Run with: make test-stream
"""

import subprocess
import uuid
from collections import Counter

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


def test_silver_deduplicates_and_rejects(pipeline, catalog, recording):
    settings, topic, tables, checkpoint = pipeline
    publisher = PositionPublisher(settings.kafka_bootstrap, settings.schema_registry_url, topic)
    for _ in range(REPLAYS):
        replay(recording, publisher, speed=None)
    expected = {
        (p["icao24"], p["event_time"])
        for response in read_recording(recording)
        for p in to_positions(response)
    }
    # One position off the globe; the Avro schema accepts it, silver must not.
    bad = {**to_positions(next(iter(read_recording(recording))))[0], "latitude": 95.0}
    publisher.publish([bad])
    publisher.flush()
    publish_garbage(settings, topic)

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
            f"--checkpoint-dir={checkpoint}",
            "--once",
            "--max-files=2",  # several micro-batches, so dedup has to work across them
        )
    )

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
