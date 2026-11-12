import io
import json

import fastavro
import pytest

from flight_lakehouse.config import POSITIONS_SCHEMA
from flight_lakehouse.producer.positions import to_position, to_positions
from flight_lakehouse.replay.replay import read_recording

NOW = 1_794_398_441_501

AIRBORNE = {
    "hex": "348609",
    "type": "adsb_icao",
    "flight": "AEA25LD ",
    "r": "EC-OTA",
    "t": "B38M",
    "category": "A3",
    "alt_baro": 35350,
    "alt_geom": 36525,
    "gs": 370.1,
    "track": 256.56,
    "baro_rate": -896,
    "squawk": "1000",
    "lat": 41.006195,
    "lon": -0.060425,
    "seen_pos": 0.058,
}


@pytest.fixture(scope="module")
def schema():
    return fastavro.parse_schema(json.loads(POSITIONS_SCHEMA.read_text()))


def test_airborne_aircraft_is_mapped_to_si_units():
    p = to_position(AIRBORNE, NOW)
    assert p["icao24"] == "348609"
    assert p["callsign"] == "AEA25LD"
    assert p["registration"] == "EC-OTA"
    assert p["aircraft_type"] == "B38M"
    assert p["source"] == "adsb_icao"
    assert p["received_at"] == NOW
    assert p["event_time"] == NOW - 58
    assert p["baro_altitude"] == pytest.approx(10774.68, abs=0.01)
    assert p["geo_altitude"] == pytest.approx(11132.82, abs=0.01)
    assert p["velocity"] == pytest.approx(190.396, abs=0.001)
    assert p["true_track"] == 256.56
    assert p["vertical_rate"] == pytest.approx(-4.552, abs=0.001)
    assert p["on_ground"] is False


def test_ground_aircraft_has_no_baro_altitude():
    p = to_position({**AIRBORNE, "alt_baro": "ground", "gs": 12.0}, NOW)
    assert p["on_ground"] is True
    assert p["baro_altitude"] is None


def test_missing_optional_fields_become_null():
    p = to_position({"hex": "ABC123", "lat": 41.3, "lon": 2.1}, NOW)
    assert p["icao24"] == "abc123"
    assert p["source"] == "unknown"
    assert p["event_time"] == NOW
    for field in ("callsign", "registration", "velocity", "true_track", "squawk"):
        assert p[field] is None


def test_blank_callsign_is_null():
    assert to_position({**AIRBORNE, "flight": "        "}, NOW)["callsign"] is None


def test_aircraft_without_position_is_skipped():
    assert to_position({"hex": "abc123", "alt_baro": 1000}, NOW) is None
    response = {"now": NOW, "ac": [AIRBORNE, {"hex": "abc123"}]}
    assert [p["icao24"] for p in to_positions(response)] == ["348609"]


def test_empty_response():
    assert to_positions({"now": NOW, "ac": []}) == []
    assert to_positions({"now": NOW}) == []


def test_recording_maps_and_matches_the_schema(recording, schema):
    responses = list(read_recording(recording))
    assert len(responses) == 3
    for response in responses:
        positions = to_positions(response)
        assert positions, "each recorded response should have aircraft"
        assert [p["icao24"] for p in positions] == sorted(p["icao24"] for p in positions)
        for p in positions:
            assert p["event_time"] <= p["received_at"]
            assert -90 <= p["latitude"] <= 90
            assert -180 <= p["longitude"] <= 180
        # Raises if any record doesn't fit the Avro schema.
        fastavro.writer(io.BytesIO(), schema, positions, validator=True)


def test_mapping_is_deterministic(recording):
    first = [to_positions(r) for r in read_recording(recording)]
    second = [to_positions(r) for r in read_recording(recording)]
    assert first == second
