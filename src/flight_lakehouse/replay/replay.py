"""Re-emit a recording of adsb.lol responses to Kafka.

Each line of the recording is one raw API response. Records come only from the
recording (never from the clock), so two replays publish identical messages.
The exceptions are explicit: `shift_ms` moves every response's time by the same
amount, and `squeeze_ms` packs the responses' times into a shorter span. The
benchmark uses both so a long recording looks like current traffic rather than
late data silver's watermark would discard.
"""

import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.producer.run import Publisher


def read_recording(path: Path) -> Iterator[dict[str, Any]]:
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def time_span(path: Path) -> tuple[int, int]:
    """The `now` of the recording's first and last responses, in epoch milliseconds."""
    first = last = None
    for response in read_recording(path):
        last = int(response["now"])
        first = last if first is None else first
    if first is None or last is None:
        raise ValueError(f"{path} has no responses")
    return first, last


def last_response_time(path: Path) -> int:
    """The `now` of the recording's last response, in epoch milliseconds."""
    return time_span(path)[1]


def squeezer(path: Path, span_ms: int) -> Callable[[int], int]:
    """Map response times so the recording spans at most `span_ms`, ending where it did.

    Times keep their order and relative gaps; a recording already that short is unchanged.
    """
    if span_ms <= 0:
        raise ValueError("span must be positive")
    first, last = time_span(path)
    if last - first <= span_ms:
        return lambda now: now
    scale = span_ms / (last - first)
    return lambda now: last - round((last - now) * scale)


def shift_to_end_at(path: Path, end_ms: int) -> int:
    """The shift that makes the recording's last response happen at `end_ms`."""
    return end_ms - last_response_time(path)


def replay(
    path: Path,
    publisher: Publisher,
    *,
    speed: float | None = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    shift_ms: int = 0,
    squeeze_ms: int | None = None,
) -> int:
    """Publish every response in order. `speed=None` means as fast as possible.

    With a speed, the gap between responses is their gap divided by it.
    `squeeze_ms` packs the recording's times (responses' and positions') into that
    span, ending at its last response (see `squeezer`); then `shift_ms` is added to
    every time. Returns the number of positions published.
    """
    if speed is not None and speed <= 0:
        raise ValueError("speed must be positive")
    squeeze = squeezer(path, squeeze_ms) if squeeze_ms is not None else None

    total = 0
    previous_now: int | None = None
    for response in read_recording(path):
        original = int(response["now"])
        now = (squeeze(original) if squeeze else original) + shift_ms
        if speed is not None and previous_now is not None and now > previous_now:
            sleep((now - previous_now) / 1000 / speed)
        previous_now = now
        positions = to_positions(response)
        if squeeze:
            # Squeeze each position's own time too, so a position repeated in two
            # responses (the aircraft sent nothing new) keeps one event_time and
            # silver's deduplication still drops the copy.
            positions = [
                {**p, "event_time": squeeze(p["event_time"]) + shift_ms, "received_at": now}
                for p in positions
            ]
        elif shift_ms:
            positions = to_positions({**response, "now": now})
        total += publisher.publish(positions)
    publisher.flush()
    return total
