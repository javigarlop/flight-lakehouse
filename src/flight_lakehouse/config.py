"""Settings read from the environment, with defaults for the local Compose stack."""

import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = ROOT / "schemas"
SCHEMA_FILE = re.compile(r"^flight_position\.v(\d+)\.avsc$")


def schema_versions(directory: Path = SCHEMAS_DIR) -> dict[int, Path]:
    """The position schema files in `directory`, by version number, oldest first."""
    found = {}
    for path in directory.iterdir():
        match = SCHEMA_FILE.match(path.name)
        if match:
            found[int(match.group(1))] = path
    if not found:
        raise FileNotFoundError(f"no flight_position.v<N>.avsc schema in {directory}")
    return dict(sorted(found.items()))


# Each version only appends optional fields with defaults, so it stays BACKWARD
# compatible with the one before (tests/test_schemas.py checks it).
LATEST_SCHEMA_VERSION = max(schema_versions())
POSITIONS_SCHEMA = schema_versions()[LATEST_SCHEMA_VERSION]
# Reference data lives as dbt seeds; dbt loads them as tables in `reference`.
AIRPORTS_CSV = ROOT / "dbt" / "seeds" / "airports.csv"


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    schema_registry_url: str
    topic: str
    topic_partitions: int
    topic_retention_hours: int
    adsblol_url: str
    # Area polled: a radius in nautical miles around Barcelona airport (LEBL).
    latitude: float
    longitude: float
    radius_nm: int
    poll_interval_s: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", f"127.0.0.1:{_env('KAFKA_HOST_PORT', '9094')}"),
            schema_registry_url=_env(
                "SCHEMA_REGISTRY_URL",
                f"http://localhost:{_env('REGISTRY_HOST_PORT', '8081')}/apis/ccompat/v7",
            ),
            topic=_env("POSITIONS_TOPIC", "flight.positions.v1"),
            topic_partitions=int(_env("POSITIONS_TOPIC_PARTITIONS", "3")),
            topic_retention_hours=int(_env("POSITIONS_TOPIC_RETENTION_HOURS", "168")),
            adsblol_url=_env("ADSBLOL_URL", "https://api.adsb.lol"),
            latitude=float(_env("AREA_LATITUDE", "41.297")),
            longitude=float(_env("AREA_LONGITUDE", "2.078")),
            radius_nm=int(_env("AREA_RADIUS_NM", "100")),
            poll_interval_s=float(_env("POLL_INTERVAL_SECONDS", "240")),
        )
