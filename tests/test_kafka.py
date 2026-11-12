import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from flight_lakehouse.producer import kafka
from flight_lakehouse.producer.kafka import (
    PositionPublisher,
    ensure_topic,
    load_schema,
    register_schema,
    value_subject,
)

POSITION = {"icao24": "348609", "callsign": "AEA25LD"}


def test_value_subject():
    assert value_subject("positions") == "positions-value"


def test_load_schema_is_compact_json(tmp_path):
    path = tmp_path / "schema.avsc"
    path.write_text('{\n  "type": "record",\n  "name": "P",\n  "fields": []\n}\n')
    text = load_schema(path)
    assert text == '{"type":"record","name":"P","fields":[]}'
    assert json.loads(text)["name"] == "P"


@pytest.fixture
def admin():
    with patch.object(kafka, "AdminClient") as cls:
        yield cls.return_value


def test_ensure_topic_skips_existing_topic(admin):
    admin.list_topics.return_value.topics = {"positions": object()}
    assert ensure_topic("broker:9092", "positions", 3, 24) is False
    admin.create_topics.assert_not_called()


def test_ensure_topic_creates_missing_topic(admin):
    admin.list_topics.return_value.topics = {}
    assert ensure_topic("broker:9092", "positions", 3, 24) is True
    (new_topic,) = admin.create_topics.call_args.args[0]
    assert new_topic.topic == "positions"
    assert new_topic.num_partitions == 3
    assert new_topic.config == {"retention.ms": str(24 * 3600 * 1000)}


def test_ensure_topic_tolerates_creation_race(admin):
    admin.list_topics.return_value.topics = {}
    future = admin.create_topics.return_value["positions"]
    future.result.side_effect = KafkaException("TOPIC_ALREADY_EXISTS")
    assert ensure_topic("broker:9092", "positions", 3, 24) is False


def test_ensure_topic_reraises_other_errors(admin):
    admin.list_topics.return_value.topics = {}
    future = admin.create_topics.return_value["positions"]
    future.result.side_effect = KafkaException("CLUSTER_AUTHORIZATION_FAILED")
    with pytest.raises(KafkaException, match="CLUSTER_AUTHORIZATION_FAILED"):
        ensure_topic("broker:9092", "positions", 3, 24)


def test_register_schema_sets_compatibility():
    with patch.object(kafka, "SchemaRegistryClient") as cls:
        client = cls.return_value
        client.register_schema.return_value = 7
        assert register_schema("http://registry", "positions") == 7
    subject, schema = client.register_schema.call_args.args
    assert subject == "positions-value"
    assert schema.schema_type == "AVRO"
    assert schema.schema_str == load_schema()
    client.set_compatibility.assert_called_once_with("positions-value", "BACKWARD")


@pytest.fixture
def producer():
    """The confluent Producer behind a PositionPublisher, with serializers stubbed out."""
    with (
        patch.object(kafka, "SchemaRegistryClient"),
        patch.object(kafka, "AvroSerializer") as avro,
        patch.object(kafka, "Producer") as cls,
    ):
        avro.return_value = MagicMock(side_effect=lambda record, _ctx: f"avro:{record['icao24']}")
        cls.return_value.flush.return_value = 0
        yield cls


def test_publisher_never_registers_schemas(producer):
    PositionPublisher("broker:9092", "http://registry", "positions")
    conf = kafka.AvroSerializer.call_args.kwargs["conf"]
    assert conf["auto.register.schemas"] is False
    assert producer.call_args.args[0]["enable.idempotence"] is True


def test_publish_sends_keyed_avro_records(producer):
    publisher = PositionPublisher("broker:9092", "http://registry", "positions")
    assert publisher.publish([POSITION, {**POSITION, "icao24": "abc123"}]) == 2
    sent = [
        (c.args[0], c.kwargs["key"], c.kwargs["value"])
        for c in producer.return_value.produce.mock_calls
    ]
    assert sent == [
        ("positions", b"348609", "avro:348609"),
        ("positions", b"abc123", "avro:abc123"),
    ]


def test_publish_waits_and_retries_when_queue_is_full(producer):
    producer.return_value.produce.side_effect = [BufferError, BufferError, None]
    publisher = PositionPublisher("broker:9092", "http://registry", "positions")
    assert publisher.publish([POSITION]) == 1
    assert producer.return_value.produce.call_count == 3
    assert producer.return_value.poll.call_args_list[:2] == [((0.5,),), ((0.5,),)]


def test_flush_raises_when_messages_are_left(producer):
    producer.return_value.flush.return_value = 4
    publisher = PositionPublisher("broker:9092", "http://registry", "positions")
    with pytest.raises(RuntimeError, match="4 messages not delivered"):
        publisher.flush(timeout_s=1)


def test_flush_reports_delivery_errors_once(producer):
    publisher = PositionPublisher("broker:9092", "http://registry", "positions")
    publisher._on_delivery(None, object())
    publisher._on_delivery("MSG_TIMED_OUT", object())
    with pytest.raises(RuntimeError, match="1 messages failed: MSG_TIMED_OUT"):
        publisher.flush()
    publisher.flush()  # errors were cleared by the first flush
