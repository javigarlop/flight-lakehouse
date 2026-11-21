"""Kafka topic setup, schema registration and the Avro position publisher."""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from flight_lakehouse.config import POSITIONS_SCHEMA, schema_versions

# Seconds before a schema registry request gives up and is retried. The client's
# default is no timeout at all, so one unanswered request blocks the producer forever.
REGISTRY_TIMEOUT_S = 10
# How long the publisher trusts its copy of the subject's latest schema. The client
# caches it forever by default; with a TTL, a running producer moves to a newly
# registered version (such as v2) within a minute, without a restart.
LATEST_SCHEMA_TTL_S = 60


class IncompatibleSchemaError(RuntimeError):
    """The registry says the schema breaks the subject's compatibility rule."""


def registry_client(registry_url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient(
        {
            "url": registry_url,
            "timeout": REGISTRY_TIMEOUT_S,
            "cache.latest.ttl.sec": LATEST_SCHEMA_TTL_S,
        }
    )


def value_subject(topic: str) -> str:
    return f"{topic}-value"


def load_schema(path: Path = POSITIONS_SCHEMA) -> str:
    # Round-trip through json so the registered text is compact and stable.
    return json.dumps(json.loads(path.read_text()), separators=(",", ":"))


def schema_path(version: int | None) -> Path:
    """The schema file for a version number; None means the latest."""
    if version is None:
        return POSITIONS_SCHEMA
    versions = schema_versions()
    if version not in versions:
        raise ValueError(f"no schema v{version}; have {', '.join(f'v{v}' for v in versions)}")
    return versions[version]


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


def register_schema(
    registry_url: str,
    topic: str,
    compatibility: str = "BACKWARD",
    path: Path = POSITIONS_SCHEMA,
) -> int:
    """Register the value schema and set its compatibility rule. Returns the schema id.

    When the subject already has versions, the registry first checks the schema
    against the latest one, so an incompatible change fails here with a clear
    error and nothing is registered. Registering a version already there is a no-op
    that returns its id.
    """
    client = registry_client(registry_url)
    subject = value_subject(topic)
    schema = Schema(load_schema(path), "AVRO")
    if subject in client.get_subjects() and not client.test_compatibility(subject, schema):
        raise IncompatibleSchemaError(
            f"{path.name} is not {compatibility} compatible with the latest {subject} version"
        )
    schema_id = client.register_schema(subject, schema)
    client.set_compatibility(subject, compatibility)
    return schema_id


def serializer_conf(schema_version: int | None) -> dict[str, Any]:
    """AvroSerializer settings: follow the latest registered version, or pin one."""
    return {
        "auto.register.schemas": False,
        # Unpinned, records are written with the subject's latest version, re-read every
        # LATEST_SCHEMA_TTL_S. Pinned, the serializer looks the given schema up instead,
        # which fails if that version isn't registered.
        "use.latest.version": schema_version is None,
        # The subject is always <topic>-value. Without this, the serializer first
        # asks the registry for "associations", an API Apicurio doesn't implement.
        "subject.name.strategy": topic_subject_name_strategy,
    }


class PositionPublisher:
    """Publishes position records as Avro, keyed by icao24.

    The serializer uses a schema already registered for the topic rather than
    registering its own, so `flight-lakehouse setup` must run first. By default it
    writes with the subject's latest version; `schema_version` pins one, as an
    older producer would (the schema evolution demo publishes v1 and v2 this way).
    Records carry every field of the newest schema; Avro leaves out those the
    writer version doesn't have.
    """

    def __init__(
        self, bootstrap: str, registry_url: str, topic: str, schema_version: int | None = None
    ):
        self.topic = topic
        self._value = AvroSerializer(
            registry_client(registry_url),
            load_schema(schema_path(schema_version)),
            conf=serializer_conf(schema_version),
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
