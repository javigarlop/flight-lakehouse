"""Schema evolution: every flight_position version stays BACKWARD compatible with the last.

BACKWARD means a consumer on the new schema reads data written with the old one,
which is the rule the registry enforces (flight-lakehouse setup sets it). Bronze
relies on something stricter: each version only appends optional fields with
defaults, so it can tell versions apart by trying the newest writer schema first.
"""

import io
import json
from datetime import datetime

import fastavro
import pytest

from flight_lakehouse.config import LATEST_SCHEMA_VERSION, POSITIONS_SCHEMA, schema_versions
from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import read_recording
from tests.conftest import FIXTURES

VERSIONS = schema_versions()
PAIRS = list(zip(list(VERSIONS)[:-1], list(VERSIONS)[1:], strict=True))


def raw(version: int) -> dict:
    return json.loads(VERSIONS[version].read_text())


def parsed(version: int):
    return fastavro.parse_schema(raw(version))


def write(version: int, record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed(version), record)
    return buf.getvalue()


def read(data: bytes, writer: int, reader: int) -> dict:
    """Decode with Avro schema resolution; timestamps back to epoch millis, as written."""
    record = fastavro.schemaless_reader(io.BytesIO(data), parsed(writer), parsed(reader))
    return {
        k: round(v.timestamp() * 1000) if isinstance(v, datetime) else v for k, v in record.items()
    }


@pytest.fixture(scope="module")
def positions() -> list[dict]:
    """Every position in the test recording, with v2's emergency field."""
    recording = FIXTURES / "adsblol_bcn_100nm.jsonl"
    return [p for response in read_recording(recording) for p in to_positions(response)]


def test_versions_are_numbered_from_one_without_gaps():
    assert list(VERSIONS) == list(range(1, LATEST_SCHEMA_VERSION + 1))
    assert LATEST_SCHEMA_VERSION >= 2
    assert VERSIONS[LATEST_SCHEMA_VERSION] == POSITIONS_SCHEMA


@pytest.mark.parametrize(("old", "new"), PAIRS)
def test_new_version_only_appends_optional_fields(old, new):
    old_schema, new_schema = raw(old), raw(new)
    # Readers match records by full name, so it must not change between versions.
    assert (new_schema["namespace"], new_schema["name"]) == (
        old_schema["namespace"],
        old_schema["name"],
    )
    old_fields, new_fields = old_schema["fields"], new_schema["fields"]
    assert new_fields[: len(old_fields)] == old_fields, "existing fields must not change"
    added = new_fields[len(old_fields) :]
    assert added, "a new version should add a field"
    for field in added:
        assert "default" in field, f"{field['name']} needs a default for BACKWARD compatibility"
        assert field["type"][0] == "null"
        assert field["default"] is None


@pytest.mark.parametrize(("old", "new"), PAIRS)
def test_new_reader_reads_old_data(old, new, positions):
    """The BACKWARD guarantee, checked with Avro schema resolution on real records."""
    added = [f["name"] for f in raw(new)["fields"][len(raw(old)["fields"]) :]]
    for position in positions[:50]:
        decoded = read(write(old, position), writer=old, reader=new)
        assert {k: v for k, v in decoded.items() if k not in added} == {
            k: position[k] for k in decoded if k not in added
        }
        assert all(decoded[name] is None for name in added)


def test_v1_and_v2_events_decode_into_the_same_rows(positions):
    emergencies = {p["emergency"] for p in positions}
    assert "none" in emergencies  # the recording has the field, so v2 carries it

    for position in positions:
        as_v1 = read(write(1, position), writer=1, reader=2)
        as_v2 = read(write(2, position), writer=2, reader=2)
        assert as_v2 == {**as_v1, "emergency": position["emergency"]}
        assert as_v1["emergency"] is None
        assert as_v2 == position  # the row bronze gets is the record the producer sent


def test_old_writer_bytes_run_out_under_a_newer_writer_schema(positions):
    """What bronze's newest-first decoding relies on to tell versions apart."""
    data = write(1, positions[0])
    with pytest.raises(EOFError):
        read(data, writer=2, reader=2)


def test_schema_versions_needs_a_schema(tmp_path):
    with pytest.raises(FileNotFoundError, match="no flight_position"):
        schema_versions(tmp_path)
