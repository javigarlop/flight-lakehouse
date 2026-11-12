"""Bronze exactly-once: kill the Spark job mid-stream, restart it, lose and duplicate nothing.

Needs the stack running and the Spark image built (make up && make spark-build).
Slow (Spark starts twice), so it has its own marker. Run with: make test-stream
"""

import subprocess
import time
import uuid

import pytest

from flight_lakehouse.config import Settings
from flight_lakehouse.producer.kafka import (
    PositionPublisher,
    ensure_topic,
    register_schema,
    value_subject,
)
from flight_lakehouse.replay.replay import replay
from tests.integration.helpers import (
    COMPOSE,
    host_catalog,
    purge_tables,
    spark_submit,
    table_rows,
)

pytestmark = [pytest.mark.integration, pytest.mark.spark]

REPLAYS = 5  # 5 x 138 positions from the fixture


def bronze_cmd(topic: str, table: str, checkpoint: str, *extra: str) -> list[str]:
    return spark_submit(
        "bronze", f"--topic={topic}", f"--table={table}", f"--checkpoint={checkpoint}", *extra
    )


@pytest.fixture
def catalog():
    return host_catalog()


@pytest.fixture
def stream_target(catalog):
    """A fresh topic filled with the fixture, plus names for a fresh table and checkpoint."""
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient

    settings = Settings.from_env()
    suffix = uuid.uuid4().hex[:8]
    topic = f"test.bronze.{suffix}"
    ensure_topic(settings.kafka_bootstrap, topic, partitions=3, retention_hours=1)
    register_schema(settings.schema_registry_url, topic)
    yield settings, topic, f"test_bronze.positions_{suffix}", f"/checkpoints/test-{suffix}"

    purge_tables(catalog, f"test_bronze.positions_{suffix}")
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    admin.delete_topics([topic])[topic].result(timeout=30)
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    registry.delete_subject(value_subject(topic), permanent=False)
    registry.delete_subject(value_subject(topic), permanent=True)


def test_kill_and_restart_loses_and_duplicates_nothing(stream_target, catalog, recording):
    settings, topic, table, checkpoint = stream_target
    publisher = PositionPublisher(settings.kafka_bootstrap, settings.schema_registry_url, topic)
    expected = sum(replay(recording, publisher, speed=None) for _ in range(REPLAYS))
    spark_table = f"lakehouse.{table}"

    # 1. Continuous job in small batches, so it is still mid-stream when killed.
    name = f"bronze-test-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [*COMPOSE, "run", "-d", "--name", name, "spark-bronze"]
        + bronze_cmd(topic, spark_table, checkpoint, "--trigger-seconds=2", "--max-offsets=60"),
        check=True,
        capture_output=True,
    )
    try:
        deadline = time.time() + 180
        while True:
            rows = table_rows(catalog, table)
            if rows is not None and rows.num_rows > 0:
                break
            assert time.time() < deadline, "bronze job wrote nothing within 3 minutes"
            time.sleep(1)
        # 2. Kill without warning: no graceful shutdown, like a crash.
        subprocess.run(["docker", "kill", name], check=True, capture_output=True)
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)

    written_before_kill = table_rows(catalog, table).num_rows
    assert 0 < written_before_kill < expected, "kill should land mid-stream"

    # 3. Restart from the same checkpoint and drain the rest.
    subprocess.run(
        [*COMPOSE, "run", "--rm", "spark-bronze"]
        + bronze_cmd(topic, spark_table, checkpoint, "--once", "--max-offsets=60"),
        check=True,
        capture_output=True,
    )

    rows = table_rows(catalog, table)
    offsets = list(
        zip(rows["kafka_partition"].to_pylist(), rows["kafka_offset"].to_pylist(), strict=True)
    )
    assert len(offsets) == expected, f"expected {expected} rows, got {len(offsets)}"
    assert len(set(offsets)) == expected, "some Kafka messages were written twice"
