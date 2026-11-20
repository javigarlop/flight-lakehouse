"""Prometheus metrics for the streaming jobs, from each micro-batch's progress.

Every job serves them on METRICS_PORT (9108) for Prometheus (infra/prometheus/), labelled
by query name, which is the table the query writes. Runs inside the Spark container
(Python 3.10), so prometheus-client is installed there too (infra/spark/Dockerfile).
"""

import os
from datetime import datetime
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, start_http_server
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

PORT = 9108
LABELS = ["query"]
# Spark reports a watermark of the epoch until the first batch with data moves it.
EPOCH = "1970-01-01T00:00:00.000Z"


def _seconds(timestamp: str) -> float:
    # Spark writes UTC with a Z, which Python 3.10's fromisoformat doesn't accept.
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


class ProgressMetrics(StreamingQueryListener):
    """Turns query progress events into Prometheus series."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.input_rows = Counter(
            "flight_stream_input_rows",
            "Rows read by the query's micro-batches",
            labelnames=LABELS,
            registry=registry,
        )
        self.batches = Counter(
            "flight_stream_batches", "Micro-batches completed", labelnames=LABELS, registry=registry
        )
        self.batch_duration = Gauge(
            "flight_stream_batch_duration_seconds",
            "Duration of the last micro-batch",
            labelnames=LABELS,
            registry=registry,
        )
        self.last_active = Gauge(
            "flight_stream_last_active_timestamp_seconds",
            "When the query last reported progress, including idle checks for new data",
            labelnames=LABELS,
            registry=registry,
        )
        self.offsets_behind = Gauge(
            "flight_stream_kafka_offsets_behind",
            "Kafka messages not yet read after the last batch, on the furthest-behind partition",
            labelnames=LABELS,
            registry=registry,
        )
        self.event_time_max = Gauge(
            "flight_stream_event_time_max_timestamp_seconds",
            "Newest event_time the query has read",
            labelnames=LABELS,
            registry=registry,
        )
        self.watermark = Gauge(
            "flight_stream_watermark_timestamp_seconds",
            "The query's event-time watermark",
            labelnames=LABELS,
            registry=registry,
        )
        # Idle events carry only the query id, so names are remembered from the start event.
        self._names: dict[str, str] = {}

    def _name(self, query_id: Any) -> str:
        return self._names.get(str(query_id), str(query_id))

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802 - Spark's listener API
        self._names[str(event.id)] = event.name or str(event.id)

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        progress = event.progress
        query = progress.name or self._name(progress.id)
        self.input_rows.labels(query).inc(progress.numInputRows)
        self.batches.labels(query).inc()
        self.batch_duration.labels(query).set(progress.batchDuration / 1000)
        self.last_active.labels(query).set(_seconds(progress.timestamp))

        behind = [
            float(source.metrics["maxOffsetsBehindLatest"])
            for source in progress.sources
            if "maxOffsetsBehindLatest" in (source.metrics or {})
        ]
        if behind:
            self.offsets_behind.labels(query).set(max(behind))

        event_time = progress.eventTime or {}
        if "max" in event_time:
            self.event_time_max.labels(query).set(_seconds(event_time["max"]))
        if event_time.get("watermark", EPOCH) != EPOCH:
            self.watermark.labels(query).set(_seconds(event_time["watermark"]))

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        self.last_active.labels(self._name(event.id)).set(_seconds(event.timestamp))

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        # A failed query stops the job and the container restarts it; Prometheus sees the
        # restart as the target going down, so there's nothing to record here.
        pass


def serve_metrics(spark: SparkSession) -> None:
    """Report every streaming query of this session to Prometheus, on METRICS_PORT."""
    start_http_server(int(os.environ.get("METRICS_PORT", PORT)))
    spark.streams.addListener(ProgressMetrics())
