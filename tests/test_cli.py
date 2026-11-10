import pytest

from flight_lakehouse import __version__
from flight_lakehouse.cli import COMMANDS, build_parser, main


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_known_commands_parse(command):
    assert build_parser().parse_args([command]).command == command


def test_command_is_required():
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([])
    assert exc.value.code == 2


def test_unimplemented_command_fails(capsys):
    assert main(["produce"]) == 1
    assert "not implemented" in capsys.readouterr().err
