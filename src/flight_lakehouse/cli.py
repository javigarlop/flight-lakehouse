"""Command-line entry point.

flight-lakehouse setup | produce | replay | stream | maintain | airports
"""

import argparse
import logging
import sys
from pathlib import Path

from flight_lakehouse import __version__
from flight_lakehouse.config import AIRPORTS_CSV, Settings

COMMANDS = {
    "setup": "Create the Kafka topic and register the Avro schema",
    "produce": "Poll the ADS-B API and publish positions to Kafka",
    "replay": "Re-emit recorded data to Kafka at 1x up to max speed",
    "stream": "Run the Spark streaming jobs (bronze and silver)",
    "maintain": "Run Iceberg maintenance: compaction, snapshot expiry, backfill",
    "airports": "Rebuild the airports reference extract for the polling area",
}
NOT_IMPLEMENTED = {"stream", "maintain"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flight-lakehouse", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    commands = {name: subparsers.add_parser(name, help=text) for name, text in COMMANDS.items()}

    produce = commands["produce"]
    produce.add_argument(
        "--record", type=Path, metavar="PATH", help="also append raw responses to a JSONL file"
    )
    produce.add_argument(
        "--no-publish", action="store_true", help="only record, don't publish to Kafka"
    )
    produce.add_argument(
        "--polls", type=int, help="stop after this many polls (default: run forever)"
    )
    produce.add_argument("--interval", type=float, help="seconds between polls (default: 240)")

    replay = commands["replay"]
    replay.add_argument("recording", type=Path, help="JSONL recording made with produce --record")
    speed = replay.add_mutually_exclusive_group()
    speed.add_argument("--speed", type=float, default=1.0, help="speed multiplier (default: 1)")
    speed.add_argument("--max", action="store_true", help="publish as fast as possible")
    replay.add_argument("--topic", help="publish to this topic instead of the default")

    airports = commands["airports"]
    airports.add_argument("--output", type=Path, default=AIRPORTS_CSV, help="default: the dbt seed")
    return parser


def _setup(settings: Settings) -> int:
    from flight_lakehouse.producer.kafka import ensure_topic, register_schema

    created = ensure_topic(
        settings.kafka_bootstrap,
        settings.topic,
        settings.topic_partitions,
        settings.topic_retention_hours,
    )
    print(f"Topic {settings.topic}: {'created' if created else 'already exists'}")
    schema_id = register_schema(settings.schema_registry_url, settings.topic)
    print(f"Schema registered for {settings.topic}-value (id {schema_id}, BACKWARD)")
    return 0


def _publisher(settings: Settings, topic: str | None = None):
    from flight_lakehouse.producer.kafka import PositionPublisher

    return PositionPublisher(
        settings.kafka_bootstrap, settings.schema_registry_url, topic or settings.topic
    )


def _produce(settings: Settings, args: argparse.Namespace) -> int:
    from flight_lakehouse.producer.adsblol import AdsbLolClient
    from flight_lakehouse.producer.run import run

    if args.no_publish and args.record is None:
        print("--no-publish needs --record", file=sys.stderr)
        return 2
    client = AdsbLolClient(settings.adsblol_url)
    try:
        total = run(
            client,
            None if args.no_publish else _publisher(settings),
            latitude=settings.latitude,
            longitude=settings.longitude,
            radius_nm=settings.radius_nm,
            interval_s=args.interval or settings.poll_interval_s,
            record_to=args.record,
            polls=args.polls,
        )
    finally:
        client.close()
    print(f"Published {total} positions")
    return 0


def _replay(settings: Settings, args: argparse.Namespace) -> int:
    from flight_lakehouse.replay.replay import replay

    total = replay(
        args.recording,
        _publisher(settings, args.topic),
        speed=None if args.max else args.speed,
    )
    print(f"Replayed {total} positions to {args.topic or settings.topic}")
    return 0


def _airports(settings: Settings, args: argparse.Namespace) -> int:
    from flight_lakehouse.reference.airports import SOURCE_URL, refresh

    count = refresh(args.output, settings.latitude, settings.longitude, settings.radius_nm)
    print(f"Wrote {count} airports to {args.output} (from {SOURCE_URL})")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in NOT_IMPLEMENTED:
        print(f"'{args.command}' is not implemented yet.", file=sys.stderr)
        return 1

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The schema registry client bundles its own copy of httpx, logging as "httpx2".
    for name in ("httpx", "httpx2"):
        logging.getLogger(name).setLevel(logging.WARNING)
    settings = Settings.from_env()
    if args.command == "setup":
        return _setup(settings)
    if args.command == "produce":
        return _produce(settings, args)
    if args.command == "airports":
        return _airports(settings, args)
    return _replay(settings, args)


if __name__ == "__main__":
    raise SystemExit(main())
