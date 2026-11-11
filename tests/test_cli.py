import pytest

from flight_lakehouse import __version__
from flight_lakehouse.cli import COMMANDS, build_parser, main


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
