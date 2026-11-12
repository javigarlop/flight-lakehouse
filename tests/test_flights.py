"""Flight sessionisation and airport matching, run on a local Spark in batch mode."""

import math
from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flight_lakehouse.streaming import flights  # noqa: E402
from flight_lakehouse.streaming.flights import Airport  # noqa: E402

# The silver.flight_positions columns sessionise reads.
POSITIONS_SCHEMA = StructType(
    [
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("registration", StringType()),
        StructField("aircraft_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("baro_altitude", DoubleType()),
        StructField("geo_altitude", DoubleType()),
        StructField("on_ground", BooleanType()),
    ]
)
T0 = datetime(2026, 11, 1, 12, 0)  # naive means UTC (see the spark fixture)
AIRPORTS = flights.load_airports()
BY_CODE = {a.code: a for a in AIRPORTS}
LEBL, LELL = BY_CODE["LEBL"], BY_CODE["LELL"]


def position(minutes: float = 0, **overrides):
    row = {
        "icao24": "4d2260",
        "callsign": "RYR3LL",
        "registration": "9H-QDS",
        "aircraft_type": "B738",
        "event_time": T0 + timedelta(minutes=minutes),
        "latitude": 41.6,
        "longitude": 2.6,
        "baro_altitude": 11_000.0,
        "geo_altitude": 11_300.0,
        "on_ground": False,
    }
    row.update(overrides)
    return row


def at(airport: Airport, north_km: float = 0.0) -> dict:
    """Coordinates north of an airport by the given distance."""
    return {"latitude": airport.latitude + north_km / 111.195, "longitude": airport.longitude}


def sessionise(spark, *rows, gap="20 minutes"):
    frame = spark.createDataFrame(
        [tuple(r[f.name] for f in POSITIONS_SCHEMA) for r in rows], POSITIONS_SCHEMA
    )
    result = flights.sessionise(frame, gap, "15 minutes", AIRPORTS)
    return sorted(result.collect(), key=lambda f: (f["icao24"], f["first_seen"]))


def test_airports_extract_covers_the_polling_area():
    assert {"LEBL", "LEGE", "LERS", "LELL", "LEDA"} <= BY_CODE.keys()
    assert LEBL.elevation_m == pytest.approx(12 * 0.3048)


def test_distance_matches_the_spherical_law_of_cosines(spark):
    lat, lon = 41.904639, 2.761774  # Girona airport
    rad = math.radians
    expected = flights.EARTH_RADIUS_KM * math.acos(
        math.sin(rad(lat)) * math.sin(rad(LEBL.latitude))
        + math.cos(rad(lat)) * math.cos(rad(LEBL.latitude)) * math.cos(rad(lon - LEBL.longitude))
    )
    frame = spark.createDataFrame([(lat, lon)], "latitude double, longitude double")
    result = frame.select(flights.distance_km(frame.latitude, frame.longitude, LEBL)).first()[0]
    assert result == pytest.approx(expected, rel=1e-9)
    assert 80 < result < 90


def test_gap_longer_than_the_session_gap_starts_a_new_flight(spark):
    result = sessionise(spark, position(0), position(4), position(8), position(60))

    assert [(f["first_seen"], f["last_seen"], f["positions"]) for f in result] == [
        (T0, T0 + timedelta(minutes=8), 3),
        (T0 + timedelta(minutes=60), T0 + timedelta(minutes=60), 1),
    ]
    assert result[0]["duration_s"] == 480


def test_aircraft_are_sessionised_separately(spark):
    result = sessionise(spark, position(0), position(2, icao24="3c6444"), position(4))
    assert [(f["icao24"], f["positions"]) for f in result] == [("3c6444", 1), ("4d2260", 2)]


def test_flight_id_is_aircraft_and_first_seen(spark):
    (flight,) = sessionise(spark, position(0.5), position(4))
    assert flight["flight_id"] == "4d2260-20261101T120030"


def test_last_reported_callsign_wins(spark):
    (flight,) = sessionise(
        spark,
        position(0, callsign="RYR3L"),
        position(4, callsign="RYR3LL"),
        position(8, callsign=None),
    )
    assert flight["callsign"] == "RYR3LL"


def test_on_ground_at_both_ends_matches_departure_and_arrival(spark):
    (flight,) = sessionise(
        spark,
        position(0, on_ground=True, baro_altitude=None, geo_altitude=None, **at(LEBL)),
        position(4),
        position(8, on_ground=True, baro_altitude=None, geo_altitude=None, **at(LELL)),
    )
    assert (flight["departure_airport"], flight["arrival_airport"]) == ("LEBL", "LELL")
    assert flight["first_altitude"] == 0.0


def test_low_approach_counts_as_arrival(spark):
    (flight,) = sessionise(
        spark, position(0), position(4, baro_altitude=800.0, **at(LEBL, north_km=-15))
    )
    assert flight["departure_airport"] is None  # first seen in cruise
    assert flight["arrival_airport"] == "LEBL"


def test_overflight_and_distant_positions_match_no_airport(spark):
    (flight,) = sessionise(
        spark,
        position(0, **at(LEBL)),  # right above the airport, but at 11 km
        position(4, baro_altitude=500.0, latitude=40.0, longitude=4.5),  # low, over the sea
    )
    assert (flight["departure_airport"], flight["arrival_airport"]) == (None, None)


def test_geo_altitude_is_used_when_baro_is_missing(spark):
    (flight,) = sessionise(spark, position(0, baro_altitude=None, geo_altitude=600.0, **at(LEBL)))
    assert flight["departure_airport"] == "LEBL"
    assert flight["first_altitude"] == 600.0


def test_nearest_airport_wins(spark):
    # Sabadell and Barcelona are about 25 km apart; 5 km from Sabadell is nearer it.
    (flight,) = sessionise(spark, position(0, baro_altitude=700.0, **at(LELL, north_km=-5)))
    assert flight["departure_airport"] == "LELL"
