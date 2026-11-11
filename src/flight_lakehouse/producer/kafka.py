"""Kafka topic setup, schema registration and the Avro position publisher."""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from flight_lakehouse.config import POSITIONS_SCHEMA


def value_subject(topic: str) -> str:
    return f"{topic}-value"


def load_schema(path: Path = POSITIONS_SCHEMA) -> str:
    # Round-trip through json so the registered text is compact and stable.
    return json.dumps(json.loads(path.read_text()), separators=(",", ":"))


def ensure_topic(bootstrap: str, topic: str, partitions: int, retention_hours: int) -> bool:
    """Create the topic if missing. Returns True if it was created."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return False
    new_topic = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=1,
        config={"retention.ms": str(retention_hours * 3600 * 1000)},
    )
    try:
        admin.create_topics([new_topic])[topic].result(timeout=30)
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
        return False
    return True


def register_schema(registry_url: str, topic: str, compatibility: str = "BACKWARD") -> int:
    """Register the value schema and set its compatibility rule. Returns the schema id."""
    client = SchemaRegistryClient({"url": registry_url})
    subject = value_subject(topic)
    schema_id = client.register_schema(subject, Schema(load_schema(), "AVRO"))
    client.set_compatibility(subject, compatibility)
    return schema_id


class PositionPublisher:
    """Publishes position records as Avro, keyed by icao24.

    The serializer uses the schema already registered for the topic rather than
    registering its own, so `flight-lakehouse setup` must run first.
    """

    def __init__(self, bootstrap: str, registry_url: str, topic: str):
        self.topic = topic
        registry = SchemaRegistryClient({"url": registry_url})
        self._value = AvroSerializer(
            registry,
            load_schema(),
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        self._key = StringSerializer("utf_8")
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 50,
                "compression.type": "zstd",
            }
        )
        self._errors: list[str] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def publish(self, positions: Iterable[dict[str, Any]]) -> int:
        count = 0
        for position in positions:
            value = self._value(position, SerializationContext(self.topic, MessageField.VALUE))
            key = self._key(position["icao24"], SerializationContext(self.topic, MessageField.KEY))
            while True:
                try:
                    self._producer.produce(
                        self.topic, key=key, value=value, on_delivery=self._on_delivery
                    )
                    break
                except BufferError:
                    # Local queue full (max-speed replay): wait for deliveries, then retry.
                    self._producer.poll(0.5)
            self._producer.poll(0)
            count += 1
        return count

    def flush(self, timeout_s: float = 30.0) -> None:
        remaining = self._producer.flush(timeout_s)
        if remaining:
            raise RuntimeError(f"{remaining} messages not delivered within {timeout_s} s")
        if self._errors:
            errors, self._errors = self._errors, []
            raise RuntimeError(f"{len(errors)} messages failed: {errors[0]}")
