"""Flights: silver.flight_positions sessionised into silver.flights, one row per flight.

A flight is an aircraft's positions with no gap longer than the session gap (default
20 minutes, five missed polls). Spark's session windows keep each aircraft's open
flight in state and emit it once the event_time watermark passes its end, so a flight
appears about gap + watermark after its last position.

Departure and arrival airports come from the first and last positions. A flight is
matched to the nearest airport in the airports seed (dbt/seeds/airports.csv) when
that position is close to it and low above it. Otherwise the airport is null. That
covers flights that enter or leave the polling area in cruise, and take-offs the
4-minute polling only caught higher up.

Known limit: an aircraft that lands and leaves again within the gap, with its
transponder on, merges both legs into one flight.
"""

import csv
import math
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from flight_lakehouse.config import AIRPORTS_CSV

FEET_TO_M = 0.3048
EARTH_RADIUS_KM = 6371.0088

# A position counts as at an airport within this distance and height above it. Loose on
# purpose: with 4-minute polls, the last position before landing can be 20 km out.
MAX_AIRPORT_DISTANCE_KM = 25.0
MAX_HEIGHT_ABOVE_AIRPORT_M = 1500.0


@dataclass(frozen=True)
class Airport:
    code: str
    latitude: float
    longitude: float
    elevation_m: float


def load_airports(path: Path = AIRPORTS_CSV) -> list[Airport]:
    """Airports from the OurAirports extract (public domain), around the polling area."""
    with path.open(newline="") as f:
        return [
            Airport(
                code=row["airport_code"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                elevation_m=float(row["elevation_ft"] or 0) * FEET_TO_M,
            )
            for row in csv.DictReader(f)
        ]


def distance_km(latitude: Column, longitude: Column, airport: Airport) -> Column:
    """Great-circle (haversine) distance from a position to an airport."""
    lat0 = math.radians(airport.latitude)
    dlat = F.radians(latitude) - F.lit(lat0)
    dlon = F.radians(longitude) - F.lit(math.radians(airport.longitude))
    h = F.pow(F.sin(dlat / 2), 2) + F.cos(F.lit(lat0)) * F.cos(F.radians(latitude)) * F.pow(
        F.sin(dlon / 2), 2
    )
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(h))


def nearest_airport(point: Column, airports: list[Airport]) -> Column:
    """Code of the nearest airport the point is at, or null.

    A plain expression rather than a join, so it can follow the streaming aggregation.
    """
    candidates = F.array(
        *(
            F.struct(
                distance_km(point["latitude"], point["longitude"], a).alias("distance_km"),
                (point["altitude"] - F.lit(a.elevation_m)).alias("height_m"),
                F.lit(a.code).alias("code"),
            )
            for a in airports
        )
    )
    close = F.filter(
        candidates,
        lambda c: (
            (c["distance_km"] <= MAX_AIRPORT_DISTANCE_KM)
            & (c["height_m"] <= MAX_HEIGHT_ABOVE_AIRPORT_M)
        ),
    )
    # Structs compare field by field, so the minimum is the closest airport.
    return F.array_min(close)["code"]


def _latest(column: str) -> Column:
    """The column's last non-null value in the session."""
    return F.max_by(column, F.when(F.col(column).isNotNull(), F.col("event_time")))


def sessionise(
    positions: DataFrame, gap: str, watermark: str, airports: list[Airport]
) -> DataFrame:
    """One row per flight from deduplicated silver positions."""
    altitude = F.when(F.col("on_ground"), F.lit(0.0)).otherwise(
        F.coalesce("baro_altitude", "geo_altitude")
    )
    point = F.struct("latitude", "longitude", altitude.alias("altitude"), "on_ground")
    flights = (
        positions.withWatermark("event_time", watermark)
        .groupBy(F.session_window("event_time", gap), "icao24")
        .agg(
            F.min("event_time").alias("first_seen"),
            F.max("event_time").alias("last_seen"),
            F.count("*").alias("positions"),
            _latest("callsign").alias("callsign"),
            _latest("registration").alias("registration"),
            _latest("aircraft_type").alias("aircraft_type"),
            F.min_by(point, "event_time").alias("first"),
            F.max_by(point, "event_time").alias("last"),
        )
    )
    return flights.select(
        # Deterministic, so replaying the same positions gives the same ids.
        F.concat_ws("-", "icao24", F.date_format("first_seen", "yyyyMMdd'T'HHmmss")).alias(
            "flight_id"
        ),
        "icao24",
        "callsign",
        "registration",
        "aircraft_type",
        "first_seen",
        "last_seen",
        (F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")).alias("duration_s"),
        "positions",
        nearest_airport(F.col("first"), airports).alias("departure_airport"),
        nearest_airport(F.col("last"), airports).alias("arrival_airport"),
        F.col("first.latitude").alias("first_latitude"),
        F.col("first.longitude").alias("first_longitude"),
        F.col("first.altitude").alias("first_altitude"),
        F.col("last.latitude").alias("last_latitude"),
        F.col("last.longitude").alias("last_longitude"),
        F.col("last.altitude").alias("last_altitude"),
        F.current_timestamp().alias("processed_at"),
    )
