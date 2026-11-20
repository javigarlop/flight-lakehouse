"""Streaming metrics: progress events in, Prometheus samples out."""

import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from prometheus_client import CollectorRegistry  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from flight_lakehouse.streaming import metrics  # noqa: E402

QUERY = "lakehouse.bronze.flight_positions"
T0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC).timestamp()


def progress(**overrides):
    fields = {
        "id": "q1",
        "name": QUERY,
        "numInputRows": 120,
        "batchDuration": 2500,
        "timestamp": "2026-11-20T12:00:00.000Z",
        "eventTime": {},
        "sources": [SimpleNamespace(metrics={})],
    }
    return SimpleNamespace(progress=SimpleNamespace(**(fields | overrides)))


@pytest.fixture
def listener():
    registry = CollectorRegistry()
    listener = metrics.ProgressMetrics(registry)

    def sample(name, query=QUERY):
        return registry.get_sample_value(name, {"query": query})

    return listener, sample


def test_progress_counts_rows_and_batches(listener):
    listener, sample = listener
    listener.onQueryProgress(progress())
    listener.onQueryProgress(progress(numInputRows=30))

    assert sample("flight_stream_input_rows_total") == 150
    assert sample("flight_stream_batches_total") == 2
    assert sample("flight_stream_batch_duration_seconds") == 2.5
    assert sample("flight_stream_last_active_timestamp_seconds") == T0


def test_kafka_lag_is_the_furthest_behind_source(listener):
    listener, sample = listener
    sources = [
        SimpleNamespace(metrics={"maxOffsetsBehindLatest": "7", "avgOffsetsBehindLatest": "3"}),
        SimpleNamespace(metrics={"maxOffsetsBehindLatest": "42"}),
    ]
    listener.onQueryProgress(progress(sources=sources))

    assert sample("flight_stream_kafka_offsets_behind") == 42


def test_non_kafka_sources_report_no_lag(listener):
    listener, sample = listener
    listener.onQueryProgress(progress(sources=[SimpleNamespace(metrics=None)]))

    assert sample("flight_stream_kafka_offsets_behind") is None


def test_event_time_and_watermark(listener):
    listener, sample = listener
    event_time = {"max": "2026-11-20T12:00:00.000Z", "watermark": "2026-11-20T11:45:00.000Z"}
    listener.onQueryProgress(progress(eventTime=event_time))

    assert sample("flight_stream_event_time_max_timestamp_seconds") == T0
    assert sample("flight_stream_watermark_timestamp_seconds") == T0 - 15 * 60


def test_epoch_watermark_before_the_first_data_is_skipped(listener):
    listener, sample = listener
    listener.onQueryProgress(progress(eventTime={"watermark": metrics.EPOCH}))

    assert sample("flight_stream_watermark_timestamp_seconds") is None
    assert sample("flight_stream_event_time_max_timestamp_seconds") is None


def test_idle_events_keep_the_query_alive_under_its_name(listener):
    listener, sample = listener
    listener.onQueryStarted(SimpleNamespace(id="q1", name=QUERY))
    listener.onQueryIdle(SimpleNamespace(id="q1", timestamp="2026-11-20T12:00:00.000Z"))
    listener.onQueryTerminated(SimpleNamespace(id="q1", exception=None))

    assert sample("flight_stream_last_active_timestamp_seconds") == T0


def test_unnamed_queries_are_labelled_by_id(listener):
    listener, sample = listener
    listener.onQueryStarted(SimpleNamespace(id="q2", name=None))
    listener.onQueryProgress(progress(id="q2", name=None))

    assert sample("flight_stream_batches_total", query="q2") == 1


def test_serve_metrics_starts_the_server_and_listens(monkeypatch):
    ports = []
    monkeypatch.setattr(metrics, "start_http_server", ports.append)
    monkeypatch.setenv("METRICS_PORT", "9200")
    spark = MagicMock()

    metrics.serve_metrics(spark)

    assert ports == [9200]
    (listener,) = spark.streams.addListener.call_args.args
    assert isinstance(listener, metrics.ProgressMetrics)


def test_listener_reads_real_spark_progress(spark):
    """A real query's events have the fields the listener reads."""
    registry = CollectorRegistry()
    listener = metrics.ProgressMetrics(registry)
    spark.streams.addListener(listener)
    rows = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 10)
        .load()
        .withWatermark("timestamp", "1 second")
        .groupBy(F.window("timestamp", "1 second"))
        .count()
    )
    query = rows.writeStream.queryName("rate").format("noop").outputMode("append").start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            watermark = registry.get_sample_value(
                "flight_stream_watermark_timestamp_seconds", {"query": "rate"}
            )
            if watermark:
                break
            time.sleep(0.5)
    finally:
        query.stop()
        spark.streams.removeListener(listener)  # the session is shared with other tests

    assert watermark is not None
    assert registry.get_sample_value("flight_stream_input_rows_total", {"query": "rate"}) > 0
