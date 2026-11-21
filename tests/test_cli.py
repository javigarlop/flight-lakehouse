from pathlib import Path
from unittest.mock import MagicMock

import pytest

from flight_lakehouse import __version__
from flight_lakehouse.cli import COMMANDS, build_parser, main
from flight_lakehouse.config import Settings
from flight_lakehouse.producer import adsblol, kafka, run
from flight_lakehouse.reference import airports
from flight_lakehouse.replay import replay


@pytest.fixture(autouse=True)
def settings(monkeypatch) -> Settings:
    """Defaults only, whatever the shell or .env sets."""
    for name in ("KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL", "POSITIONS_TOPIC", "AREA_RADIUS_NM"):
        monkeypatch.delenv(name, raising=False)
    return Settings.from_env()


@pytest.fixture
def publishers(monkeypatch) -> list[tuple]:
    created = []
    monkeypatch.setattr(kafka, "PositionPublisher", lambda *args: created.append(args) or args)
    return created


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


REQUIRED_ARGS = {"replay": ["recording.jsonl"]}


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_known_commands_parse(command):
    args = build_parser().parse_args([command, *REQUIRED_ARGS.get(command, [])])
    assert args.command == command


def test_replay_speed_and_max_are_exclusive():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["replay", "r.jsonl", "--speed", "2", "--max"])


def test_produce_no_publish_needs_record(capsys):
    assert main(["produce", "--no-publish"]) == 2
    assert "--record" in capsys.readouterr().err


def test_command_is_required():
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([])
    assert exc.value.code == 2


def test_unimplemented_command_fails(capsys):
    assert main(["stream"]) == 1
    assert "not implemented" in capsys.readouterr().err


def test_setup_creates_topic_and_registers_schema(monkeypatch, settings, capsys):
    topics, schemas = [], []
    monkeypatch.setattr(kafka, "ensure_topic", lambda *args: topics.append(args) or True)
    monkeypatch.setattr(kafka, "register_schema", lambda *args: schemas.append(args) or 42)

    assert main(["setup"]) == 0

    assert topics == [(settings.kafka_bootstrap, settings.topic, 3, 168)]
    assert schemas == [(settings.schema_registry_url, settings.topic)]
    out = capsys.readouterr().out
    assert "created" in out
    assert "id 42" in out


def test_setup_reports_an_existing_topic(monkeypatch, capsys):
    monkeypatch.setattr(kafka, "ensure_topic", lambda *_args: False)
    monkeypatch.setattr(kafka, "register_schema", lambda *_args: 1)

    assert main(["setup"]) == 0
    assert "already exists" in capsys.readouterr().out


@pytest.fixture
def produce(monkeypatch):
    """Runs `produce` with the API client and poll loop replaced by fakes."""
    client, calls = MagicMock(), []
    monkeypatch.setattr(adsblol, "AdsbLolClient", lambda _url: client)

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return 7

    monkeypatch.setattr(run, "run", fake_run)
    return client, calls


def test_produce_publishes_and_closes_the_client(produce, publishers, settings, capsys):
    client, calls = produce

    assert main(["produce", "--polls", "2", "--record", "r.jsonl"]) == 0

    [(args, kwargs)] = calls
    assert args == (client, publishers[0])
    assert kwargs["polls"] == 2
    assert kwargs["record_to"] == Path("r.jsonl")
    assert kwargs["interval_s"] == settings.poll_interval_s
    client.close.assert_called_once()
    assert "Published 7 positions" in capsys.readouterr().out


def test_produce_record_only_has_no_publisher(produce, publishers):
    _client, calls = produce

    assert main(["produce", "--no-publish", "--record", "r.jsonl", "--interval", "5"]) == 0

    [(args, kwargs)] = calls
    assert args[1] is None
    assert kwargs["interval_s"] == 5
    assert publishers == []


def test_produce_closes_the_client_on_error(produce, monkeypatch):
    client, _calls = produce
    monkeypatch.setattr(run, "run", MagicMock(side_effect=RuntimeError("boom")))

    with pytest.raises(RuntimeError):
        main(["produce"])
    client.close.assert_called_once()


@pytest.mark.parametrize(
    ("argv", "speed", "topic"),
    [
        (["--speed", "4"], 4.0, "flight.positions.v1"),
        (["--max", "--topic", "t2"], None, "t2"),
    ],
)
def test_replay_publishes_to_the_topic(monkeypatch, publishers, capsys, argv, speed, topic):
    calls = []
    monkeypatch.setattr(
        replay, "replay", lambda path, pub, speed: calls.append((path, pub, speed)) or 3
    )

    assert main(["replay", "r.jsonl", *argv]) == 0

    assert calls == [(Path("r.jsonl"), publishers[0], speed)]
    assert publishers[0][2] == topic
    assert f"Replayed 3 positions to {topic}" in capsys.readouterr().out


def test_airports_rebuilds_the_extract(monkeypatch, settings, capsys):
    calls = []
    monkeypatch.setattr(airports, "refresh", lambda *args: calls.append(args) or 30)

    assert main(["airports", "--output", "out.csv"]) == 0

    assert calls == [(Path("out.csv"), settings.latitude, settings.longitude, 100)]
    assert "Wrote 30 airports to out.csv" in capsys.readouterr().out
