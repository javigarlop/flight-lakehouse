"""Settings read from the environment, with defaults for the local Compose stack."""

import os
from dataclasses import dataclass
from pathlib import Path

SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "schemas"
POSITIONS_SCHEMA = SCHEMAS_DIR / "flight_position.v1.avsc"


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
            topic_retention_hours=int(_env("POSITIONS_TOPIC_RETENTION_HOURS", "72")),
            adsblol_url=_env("ADSBLOL_URL", "https://api.adsb.lol"),
            latitude=float(_env("AREA_LATITUDE", "41.297")),
            longitude=float(_env("AREA_LONGITUDE", "2.078")),
            radius_nm=int(_env("AREA_RADIUS_NM", "100")),
            poll_interval_s=float(_env("POLL_INTERVAL_SECONDS", "240")),
        )
