"""Turn an adsb.lol API response into position records matching flight_position.v2.

This is a pure function of the response, so replaying a recording always yields
the same records. A v1 publisher serialises the same records; Avro then leaves
out the fields v1 doesn't have.
"""

from typing import Any

FEET_TO_M = 0.3048
KNOTS_TO_MS = 0.514444
FEET_PER_MIN_TO_MS = FEET_TO_M / 60


def _scaled(value: Any, factor: float) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return round(value * factor, 3)
    return None


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def to_position(aircraft: dict[str, Any], received_at: int) -> dict[str, Any] | None:
    """Map one aircraft entry; return None when it has no usable position."""
    lat, lon = aircraft.get("lat"), aircraft.get("lon")
    if lat is None or lon is None or "hex" not in aircraft:
        return None

    alt_baro = aircraft.get("alt_baro")
    on_ground = alt_baro == "ground"
    # seen_pos is how many seconds before the response the position was received.
    seen_pos = aircraft.get("seen_pos") or 0
    return {
        "icao24": aircraft["hex"].lower(),
        "callsign": _text(aircraft.get("flight")),
        "registration": _text(aircraft.get("r")),
        "aircraft_type": _text(aircraft.get("t")),
        "category": _text(aircraft.get("category")),
        "source": aircraft.get("type") or "unknown",
        "event_time": received_at - round(seen_pos * 1000),
        "received_at": received_at,
        "latitude": float(lat),
        "longitude": float(lon),
        "baro_altitude": None if on_ground else _scaled(alt_baro, FEET_TO_M),
        "geo_altitude": _scaled(aircraft.get("alt_geom"), FEET_TO_M),
        "velocity": _scaled(aircraft.get("gs"), KNOTS_TO_MS),
        "true_track": _scaled(aircraft.get("track"), 1.0),
        "vertical_rate": _scaled(aircraft.get("baro_rate"), FEET_PER_MIN_TO_MS),
        "on_ground": on_ground,
        "squawk": _text(aircraft.get("squawk")),
        "emergency": _text(aircraft.get("emergency")),
    }


def to_positions(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Map every aircraft with a position, sorted by icao24 for a stable order."""
    received_at = int(response["now"])
    positions = (to_position(a, received_at) for a in response.get("ac") or [])
    return sorted((p for p in positions if p is not None), key=lambda p: p["icao24"])
