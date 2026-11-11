"""Re-emit a recording of adsb.lol responses to Kafka.

Each line of the recording is one raw API response. Records come only from the
recording (never from the clock), so two replays publish identical messages.
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


def replay(
    path: Path,
    publisher: Publisher,
    *,
    speed: float | None = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Publish every response in order. `speed=None` means as fast as possible.

    With a speed, the gap between responses is their original gap divided by it.
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
        total += publisher.publish(to_positions(response))
    publisher.flush()
    return total
