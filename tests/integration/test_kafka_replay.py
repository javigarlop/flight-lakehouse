"""Phase 1 done criterion: two replays of the same recording give identical topic contents.

Needs the stack running (make up). Run with: make smoke
"""

import uuid

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

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture
def fresh_topic(settings: Settings):
    """Factory for throwaway topics with the schema registered; deletes them afterwards."""
    from confluent_kafka.admin import AdminClient

    created: list[str] = []

    def make() -> str:
        topic = f"test.positions.{uuid.uuid4().hex[:8]}"
        assert ensure_topic(settings.kafka_bootstrap, topic, partitions=3, retention_hours=1)
        register_schema(settings.schema_registry_url, topic)
        created.append(topic)
        return topic

    yield make

    if created:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        for future in admin.delete_topics(created).values():
            future.result(timeout=30)
        registry = registry_client(settings.schema_registry_url)
        for topic in created:
            registry.delete_subject(value_subject(topic), permanent=False)
            registry.delete_subject(value_subject(topic), permanent=True)


def read_topic(settings: Settings, topic: str) -> dict[int, list[tuple[bytes, bytes]]]:
    """Every message, as (key, value) bytes, grouped by partition in offset order."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions}
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
        messages: dict[int, list[tuple[bytes, bytes]]] = {p: [] for p in partitions}
        while sum(len(m) for m in messages.values()) < sum(ends.values()):
            msg = consumer.poll(5)
            assert msg is not None, "timed out reading the topic"
            assert msg.error() is None, msg.error()
            messages[msg.partition()].append((msg.key(), msg.value()))
        return messages
    finally:
        consumer.close()


def test_two_replays_produce_identical_topics(settings, recording, fresh_topic):
    topics = [fresh_topic(), fresh_topic()]
    for topic in topics:
        publisher = PositionPublisher(settings.kafka_bootstrap, settings.schema_registry_url, topic)
        replay(recording, publisher, speed=None)

    first, second = (read_topic(settings, t) for t in topics)
    expected = sum(len(to_positions(r)) for r in read_recording(recording))
    assert sum(len(m) for m in first.values()) == expected
    assert first == second
    # Keyed by aircraft: each aircraft lands on exactly one partition.
    keys_per_partition = [{k for k, _ in msgs} for msgs in first.values()]
    assert sum(len(k) for k in keys_per_partition) == len(set().union(*keys_per_partition))


def test_values_decode_with_the_registered_schema(settings, recording, fresh_topic):
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    topic = fresh_topic()
    replay(
        recording,
        PositionPublisher(settings.kafka_bootstrap, settings.schema_registry_url, topic),
        speed=None,
    )
    registry = registry_client(settings.schema_registry_url)
    decode = AvroDeserializer(registry)
    ctx = SerializationContext(topic, MessageField.VALUE)

    decoded = [decode(v, ctx) for msgs in read_topic(settings, topic).values() for _, v in msgs]
    # timestamp-millis fields decode as datetimes; compare them as epoch milliseconds.
    for p in decoded:
        for field in ("event_time", "received_at"):
            p[field] = round(p[field].timestamp() * 1000)
    expected = [p for r in read_recording(recording) for p in to_positions(r)]
    key = lambda p: (p["received_at"], p["icao24"])  # noqa: E731
    assert sorted(decoded, key=key) == sorted(expected, key=key)


def test_compatibility_rule_is_backward(settings):

    registry = registry_client(settings.schema_registry_url)
    assert registry.get_compatibility(value_subject(settings.topic)) == "BACKWARD"
