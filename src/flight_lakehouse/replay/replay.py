"""Re-emit a recording of adsb.lol responses to Kafka.

Each line of the recording is one raw API response. Records come only from the
recording (never from the clock), so two replays publish identical messages.
The one exception is an explicit `shift_ms`, which moves every response's time
by the same amount: the benchmark uses it so a replay looks like current
traffic rather than late data silver's watermark would discard.
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


def last_response_time(path: Path) -> int:
    """The `now` of the recording's last response, in epoch milliseconds."""
    last = None
    for response in read_recording(path):
        last = int(response["now"])
    if last is None:
        raise ValueError(f"{path} has no responses")
    return last


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
) -> int:
    """Publish every response in order. `speed=None` means as fast as possible.

    With a speed, the gap between responses is their original gap divided by it.
    `shift_ms` is added to every response's time (and so to every position's).
    Returns the number of positions published.
    """
    if speed is not None and speed <= 0:
        raise ValueError("speed must be positive")

    total = 0
    previous_now: int | None = None
    for response in read_recording(path):
        now = int(response["now"])
        if speed is not None and previous_now is not None and now > previous_now:
            sleep((now - previous_now) / 1000 / speed)
        previous_now = now
        shifted = {**response, "now": now + shift_ms} if shift_ms else response
        total += publisher.publish(to_positions(shifted))
    publisher.flush()
    return total
