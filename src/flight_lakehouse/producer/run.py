"""Polling loop: fetch from adsb.lol, optionally record, publish to Kafka."""

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from flight_lakehouse.producer.adsblol import RateLimit, RateLimited
from flight_lakehouse.producer.positions import to_positions

log = logging.getLogger(__name__)

# Below this many requests left, wait for the daily reset instead of polling.
RESERVE_REQUESTS = 10


class Source(Protocol):
    def point(
        self, latitude: float, longitude: float, radius_nm: int
    ) -> tuple[dict[str, Any], RateLimit]: ...


class Publisher(Protocol):
    def publish(self, positions: Any) -> int: ...
    def flush(self) -> None: ...


def append_recording(path: Path, response: dict[str, Any]) -> None:
    """Append one raw response as a JSON line; the format the replay tool reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(response, separators=(",", ":")) + "\n")


def next_wait(interval_s: float, rate_limit: RateLimit) -> float:
    """Seconds until the next poll: the interval, or the reset if the budget is nearly spent."""
    if rate_limit.remaining is not None and rate_limit.remaining <= RESERVE_REQUESTS:
        return max(interval_s, float(rate_limit.reset_s or interval_s))
    return interval_s


def run(
    source: Source,
    publisher: Publisher | None,
    *,
    latitude: float,
    longitude: float,
    radius_nm: int,
    interval_s: float,
    record_to: Path | None = None,
    polls: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Poll until `polls` is reached (forever if None). Returns positions published."""
    total = 0
    done = 0
    while polls is None or done < polls:
        try:
            response, rate_limit = source.point(latitude, longitude, radius_nm)
        except RateLimited as exc:
            log.warning("%s", exc)
            sleep(max(exc.retry_after_s, interval_s))
            continue

        done += 1
        if record_to is not None:
            append_recording(record_to, response)
        positions = to_positions(response)
        if publisher is not None:
            total += publisher.publish(positions)
            publisher.flush()
        log.info(
            "poll %d: %d positions, %s requests left today",
            done,
            len(positions),
            rate_limit.remaining,
        )
        if polls is None or done < polls:
            sleep(next_wait(interval_s, rate_limit))
    return total
