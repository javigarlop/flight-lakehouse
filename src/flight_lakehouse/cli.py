"""Command-line entry point: flight-lakehouse produce | replay | stream | maintain."""

import argparse
import sys

from flight_lakehouse import __version__

COMMANDS = {
    "produce": "Poll the ADS-B API and publish positions to Kafka",
    "replay": "Re-emit recorded data to Kafka at 1x to 100x speed",
    "stream": "Run the Spark streaming jobs (bronze and silver)",
    "maintain": "Run Iceberg maintenance: compaction, snapshot expiry, backfill",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flight-lakehouse", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in COMMANDS.items():
        subparsers.add_parser(name, help=help_text)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(f"'{args.command}' is not implemented yet.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
