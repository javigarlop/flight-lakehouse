"""Build the airports reference extract from OurAirports.

OurAirports publishes every airport in the world (public domain). The pipeline only
needs those around the polling area, so this keeps the airports within the area's
radius plus a margin: every large and medium airport, and small ones that have a
4-letter ICAO code (Sabadell, for example, but not farm strips).

Run with `flight-lakehouse airports` (or `make airports`) after changing the polling
area, then commit the updated CSV. Git history records when the extract changed.
"""

import csv
import io
import math
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import httpx

SOURCE_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
# Flights can be first seen at the edge of the area, so look a little beyond it.
MARGIN_NM = 20
KEPT_TYPES = {"large_airport", "medium_airport", "small_airport"}
COLUMNS = [
    "airport_code",
    "icao_code",
    "iata_code",
    "name",
    "type",
    "latitude",
    "longitude",
    "elevation_ft",
    "municipality",
    "iso_country",
]
EARTH_RADIUS_NM = 6371.0088 / 1.852
_ICAO = re.compile(r"[A-Z]{4}")


def distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle (haversine) distance."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = (
        math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_NM * math.asin(math.sqrt(h))


def icao_code(row: dict[str, str]) -> str:
    """The airport's ICAO code. Some only have it in gps_code, such as Sabadell (LELL)."""
    for value in (row.get("icao_code"), row.get("gps_code")):
        if value and _ICAO.fullmatch(value):
            return value
    return ""


def select(
    rows: Iterable[dict[str, str]], latitude: float, longitude: float, radius_nm: float
) -> list[dict[str, Any]]:
    """Airports within the radius, as extract rows sorted by code."""
    kept = []
    for row in rows:
        if row["type"] not in KEPT_TYPES:
            continue
        code = icao_code(row)
        if row["type"] == "small_airport" and not code:
            continue
        lat, lon = float(row["latitude_deg"]), float(row["longitude_deg"])
        if distance_nm(latitude, longitude, lat, lon) > radius_nm:
            continue
        kept.append(
            {
                "airport_code": code or row["ident"],
                "icao_code": code,
                "iata_code": row["iata_code"],
                "name": row["name"],
                "type": row["type"],
                "latitude": row["latitude_deg"],
                "longitude": row["longitude_deg"],
                "elevation_ft": row["elevation_ft"],
                "municipality": row["municipality"],
                "iso_country": row["iso_country"],
            }
        )
    return sorted(kept, key=lambda r: r["airport_code"])


def write(airports: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(airports)


def fetch(url: str = SOURCE_URL) -> list[dict[str, str]]:
    response = httpx.get(url, timeout=60, follow_redirects=True)
    response.raise_for_status()
    return list(csv.DictReader(io.StringIO(response.text)))


def refresh(path: Path, latitude: float, longitude: float, area_radius_nm: float) -> int:
    """Download OurAirports and write the extract for the polling area. Returns the count."""
    airports = select(fetch(), latitude, longitude, area_radius_nm + MARGIN_NM)
    write(airports, path)
    return len(airports)
