"""Benchmark measurement helpers and the results report, free of any live service.

The benchmark run (benchmark/run.py) collects raw numbers; this module turns them
into rates and summaries and writes them as JSON (machine-readable, the source of
truth) and as a Markdown table for docs/benchmarks.md and the README.
"""

import json
import statistics
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OffsetRange:
    """The Kafka offsets one run published to a partition: start inclusive, end exclusive."""

    partition: int
    start: int
    end: int

    @property
    def count(self) -> int:
        return self.end - self.start


def offset_ranges(before: Mapping[int, int], after: Mapping[int, int]) -> list[OffsetRange]:
    """The offsets added between two snapshots of each partition's end offset."""
    ranges = []
    for partition in sorted(after):
        start = before.get(partition, 0)
        if after[partition] < start:
            raise ValueError(f"partition {partition} went back from {start} to {after[partition]}")
        if after[partition] > start:
            ranges.append(OffsetRange(partition, start, after[partition]))
    return ranges


def offset_filter(ranges: Iterable[OffsetRange], alias: str = "") -> str:
    """A SQL condition matching rows whose Kafka offset is in one of the ranges."""
    prefix = f"{alias}." if alias else ""
    terms = [
        f"({prefix}kafka_partition = {r.partition:d}"
        f" AND {prefix}kafka_offset >= {r.start:d} AND {prefix}kafka_offset < {r.end:d})"
        for r in ranges
    ]
    return "(" + " OR ".join(terms) + ")" if terms else "FALSE"


def rate(count: float, seconds: float | None) -> float | None:
    """Events per second, or None when there is no duration to divide by."""
    if seconds is None or seconds <= 0:
        return None
    return count / seconds


def summarise(samples_ms: list[float]) -> dict[str, float | int]:
    """Median, min and max of repeated timings, in milliseconds."""
    if not samples_ms:
        raise ValueError("no samples")
    return {
        "runs": len(samples_ms),
        "median_ms": statistics.median(samples_ms),
        "min_ms": min(samples_ms),
        "max_ms": max(samples_ms),
    }


def time_queries(
    execute: Callable[[str], Any],
    queries: Mapping[str, str],
    repeats: int,
    clock: Callable[[], float],
) -> dict[str, dict[str, float | int]]:
    """Run each query once to warm up, then `repeats` times, timing each run.

    `execute` must fetch every row, so the time covers the whole query and not
    only its first page.
    """
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    results = {}
    for name, sql in queries.items():
        execute(sql)  # warm-up: metadata and file listings get cached
        samples = []
        for _ in range(repeats):
            start = clock()
            execute(sql)
            samples.append((clock() - start) * 1000)
        results[name] = summarise(samples)
    return results


def change_pct(before: float | None, after: float | None) -> float | None:
    """Relative change from before to after, in percent; negative is faster or fewer."""
    if before is None or after is None or before == 0:
        return None
    return (after - before) / before * 100


def _number(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.{digits}f}"


def _seconds(ms: float | None) -> str:
    return "n/a" if ms is None else f"{ms / 1000:,.2f}"


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.0f}%"


def render_markdown(results: Mapping[str, Any]) -> str:
    """The results as Markdown tables, for docs/benchmarks.md and the README."""
    env, run, pipe = results["environment"], results["run"], results["pipeline"]
    lines = [
        f"Run at {results['started_at']} on {env['platform']}, {env['cpus']} CPUs, "
        f"{_number(env['memory_gb'])} GB RAM. Recording `{run['recording']}`, "
        f"replayed once at max speed.",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Events published | {_number(run['events_published'])} |",
        f"| Publish rate (events/s) | {_number(run['publish_rate_eps'])} |",
        f"| Rows in bronze | {_number(pipe['bronze_rows'])} |",
        f"| Rows in silver (after dedup) | {_number(pipe['silver_rows'])} |",
        f"| Bronze throughput (events/s) | {_number(pipe['bronze_throughput_eps'])} |",
        f"| Silver throughput (events/s) | {_number(pipe['silver_throughput_eps'])} |",
    ]
    for stage in ("bronze", "silver"):
        latency = pipe[f"{stage}_latency_ms"]
        lines.append(
            f"| Latency, Kafka to {stage}: p50 / p95 / p99 / max (s) | "
            + " / ".join(_seconds(latency[k]) for k in ("p50", "p95", "p99", "max"))
            + " |"
        )
    compaction = results["compaction"]
    lines += [
        f"| Compaction run time (s) | {_number(compaction['duration_s'])} |",
        "",
        "| Table | Data files before compaction | After | Change |",
        "|---|---|---|---|",
    ]
    for table, before in compaction["files_before"].items():
        after = compaction["files_after"].get(table)
        lines.append(
            f"| `{table}` | {_number(before)} | {_number(after)} "
            f"| {_percent(change_pct(before, after))} |"
        )
    lines += [
        "",
        f"Trino query time, median of {results['queries']['repeats']} runs after a warm-up:",
        "",
        "| Query | Before compaction (ms) | After (ms) | Change |",
        "|---|---|---|---|",
    ]
    before_q, after_q = results["queries"]["before"], results["queries"]["after"]
    for name, timing in before_q.items():
        after_ms = after_q.get(name, {}).get("median_ms")
        lines.append(
            f"| `{name}` | {_number(timing['median_ms'])} | {_number(after_ms)} "
            f"| {_percent(change_pct(timing['median_ms'], after_ms))} |"
        )
    return "\n".join(lines) + "\n"


def write_results(results: Mapping[str, Any], output_dir: Path) -> tuple[Path, Path]:
    """Write benchmark-results.json and benchmark-results.md; returns their paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "benchmark-results.json"
    md_path = output_dir / "benchmark-results.md"
    json_path.write_text(json.dumps(results, indent=2, sort_keys=True, default=str) + "\n")
    md_path.write_text(render_markdown(results))
    return json_path, md_path
