import csv
import math

import httpx
import pytest

from flight_lakehouse.cli import main
from flight_lakehouse.config import AIRPORTS_CSV
from flight_lakehouse.reference import airports

BCN = (41.297, 2.078)


def ourairports_row(ident, type_="large_airport", lat=41.2971, lon=2.07846, **fields):
    """One row as OurAirports publishes it (only the columns the extract reads)."""
    row = {
        "ident": ident,
        "type": type_,
        "name": f"{ident} Airport",
        "latitude_deg": str(lat),
        "longitude_deg": str(lon),
        "elevation_ft": "12",
        "iso_country": "ES",
        "municipality": "Somewhere",
        "icao_code": "",
        "gps_code": "",
        "iata_code": "",
    }
    row.update(fields)
    return row


def test_distance_matches_the_spherical_law_of_cosines():
    (lat1, lon1), (lat2, lon2) = (41.2971, 2.07846), (41.904639, 2.761774)  # LEBL, LEGE
    p1, p2, dlon = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    expected = airports.EARTH_RADIUS_NM * math.acos(
        math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(dlon)
    )
    assert airports.distance_nm(lat1, lon1, lat2, lon2) == pytest.approx(expected, rel=1e-9)


def test_icao_code_falls_back_to_gps_code():
    assert airports.icao_code({"icao_code": "LEBL", "gps_code": ""}) == "LEBL"
    assert airports.icao_code({"icao_code": "", "gps_code": "LELL"}) == "LELL"
    assert airports.icao_code({"icao_code": "", "gps_code": "ES-0109"}) == ""


def test_select_keeps_airports_in_range_with_a_usable_code():
    rows = [
        ourairports_row("LEBL", icao_code="LEBL", iata_code="BCN"),
        ourairports_row("ES-0109", "medium_airport", lat=41.95, lon=2.31),  # no ICAO: kept
        ourairports_row("LELL", "small_airport", lat=41.52, lon=2.105, gps_code="LELL"),
        ourairports_row("ES-0999", "small_airport", lat=41.4, lon=2.0),  # strip, no code
        ourairports_row("ES-H01", "heliport", lat=41.3, lon=2.1),
        ourairports_row("LEMD", icao_code="LEMD", lat=40.47, lon=-3.56),  # Madrid, too far
    ]
    selected = airports.select(rows, *BCN, radius_nm=120)

    assert [a["airport_code"] for a in selected] == ["ES-0109", "LEBL", "LELL"]
    assert set(selected[0]) == set(airports.COLUMNS)
    lebl = selected[1]
    assert (lebl["icao_code"], lebl["iata_code"], lebl["latitude"]) == ("LEBL", "BCN", "41.2971")


def test_write_produces_the_seed_format(tmp_path):
    path = tmp_path / "seeds" / "airports.csv"
    rows = airports.select([ourairports_row("LEBL", icao_code="LEBL")], *BCN, radius_nm=120)
    airports.write(rows, path)

    with path.open() as f:
        assert next(csv.reader(f)) == airports.COLUMNS
    assert path.read_text().count("\n") == 2


def test_refresh_adds_the_margin_to_the_area_radius(tmp_path, monkeypatch):
    # 110 nm from Barcelona: outside the 100 nm area, inside the 20 nm margin.
    palma = ourairports_row("LEPA", icao_code="LEPA", lat=39.5517, lon=2.73881)
    monkeypatch.setattr(airports, "fetch", lambda: [palma])

    assert airports.refresh(tmp_path / "a.csv", *BCN, area_radius_nm=100) == 1
    assert airports.refresh(tmp_path / "b.csv", *BCN, area_radius_nm=80) == 0


def test_fetch_parses_the_csv(monkeypatch):
    body = "ident,type,latitude_deg\nLEBL,large_airport,41.2971\n"
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return httpx.Response(200, text=body, request=httpx.Request("GET", url))

    monkeypatch.setattr(airports.httpx, "get", get)
    assert airports.fetch() == [
        {"ident": "LEBL", "type": "large_airport", "latitude_deg": "41.2971"}
    ]
    assert calls[0][0] == airports.SOURCE_URL
    assert calls[0][1]["timeout"] == 60


def test_committed_seed_is_in_the_extract_format():
    with AIRPORTS_CSV.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0]) == airports.COLUMNS
    codes = [r["airport_code"] for r in rows]
    assert codes == sorted(codes)
    assert len(codes) == len(set(codes))
    assert "LEBL" in codes


def test_cli_writes_to_the_given_path(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(airports, "refresh", lambda *args: calls.append(args) or 30)
    for name in ("AREA_LATITUDE", "AREA_LONGITUDE", "AREA_RADIUS_NM"):
        monkeypatch.delenv(name, raising=False)

    assert main(["airports", "--output", str(tmp_path / "x.csv")]) == 0

    path, lat, lon, radius = calls[0]
    assert path == tmp_path / "x.csv"
    assert (lat, lon, radius) == (41.297, 2.078, 100)
    assert "Wrote 30 airports" in capsys.readouterr().out
