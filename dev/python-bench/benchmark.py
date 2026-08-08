"""Manual end-to-end benchmarks for mcapdecode and pure-Python rosbags."""

from __future__ import annotations

import argparse
import dataclasses
import gc
import importlib.metadata
import json
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import mcapdecode
import numpy as np
import pyarrow as pa
from rosbags.highlevel import AnyReader

CASES = ("flat", "nested", "bytes", "numeric-array", "strings")
TOPIC = "/bench"
BATCH_SIZE = 1024


def generate_fixture(case: str, fixture_dir: Path) -> Path:
    fixture_dir.mkdir(parents=True, exist_ok=True)
    path = fixture_dir / f"{case}.mcap"
    subprocess.run(
        [
            "cargo",
            "run",
            "--release",
            "-p",
            "mcapbench",
            "--",
            "--case",
            case,
            "--encoding",
            "ros2msg",
            "--output",
            str(path),
        ],
        check=True,
    )
    return path


def to_arrow_value(value: Any) -> Any:
    """Convert a rosbags-generated message field into a PyArrow-compatible value."""
    if dataclasses.is_dataclass(value):
        return {
            field.name: to_arrow_value(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, np.ndarray):
        if value.dtype == np.uint8 and value.ndim == 1:
            return value.tobytes()
        return [to_arrow_value(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        return {key: to_arrow_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_arrow_value(item) for item in value]
    return value


def read_with_rosbags(path: Path, schema: pa.Schema) -> pa.Table:
    batches: list[pa.RecordBatch] = []
    rows: list[dict[str, Any]] = []
    with AnyReader([path]) as reader:
        connections = [connection for connection in reader.connections if connection.topic == TOPIC]
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            message = reader.deserialize(rawdata, connection.msgtype)
            row = {
                "@log_time": timestamp,
                "@publish_time": timestamp,
            }
            row.update(to_arrow_value(message))
            rows.append(row)
            if len(rows) == BATCH_SIZE:
                batches.append(pa.RecordBatch.from_pylist(rows, schema=schema))
                rows.clear()
    if rows:
        batches.append(pa.RecordBatch.from_pylist(rows, schema=schema))
    return pa.Table.from_batches(batches, schema=schema)


def validate_equivalence(path: Path) -> tuple[pa.Table, pa.Table]:
    native = mcapdecode.read(path, TOPIC)
    pure_python = read_with_rosbags(path, native.schema)
    if not native.equals(pure_python):
        raise AssertionError(f"mcapdecode and rosbags returned different tables for {path}")
    return native, pure_python


def measure(function: Callable[[], pa.Table], iterations: int, warmup: int) -> list[int]:
    for _ in range(warmup):
        function()

    samples = []
    for _ in range(iterations):
        gc.collect()
        start = time.perf_counter_ns()
        table = function()
        samples.append(time.perf_counter_ns() - start)
        if table.num_rows == 0:
            raise AssertionError("benchmark fixture unexpectedly has no rows")
    return samples


def percentile(samples: Sequence[int], ratio: float) -> float:
    if not samples:
        raise ValueError("cannot calculate a percentile of an empty sample")
    if not 0 <= ratio <= 1:
        raise ValueError("percentile ratio must be between 0 and 1")

    ordered = sorted(samples)
    position = (len(ordered) - 1) * ratio
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize(samples_ns: Sequence[int], file_bytes: int, rows: int) -> dict[str, float | int]:
    median_ns = int(statistics.median(samples_ns))
    seconds = median_ns / 1_000_000_000
    return {
        "min_ms": min(samples_ns) / 1_000_000,
        "median_ms": median_ns / 1_000_000,
        "p95_ms": percentile(samples_ns, 0.95) / 1_000_000,
        "file_mib_per_s": file_bytes / (1024 * 1024) / seconds,
        "messages_per_s": rows / seconds,
    }


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def benchmark_case(path: Path, iterations: int, warmup: int) -> dict[str, Any]:
    native, _ = validate_equivalence(path)
    schema = native.schema
    rows = native.num_rows
    native_samples = measure(lambda: mcapdecode.read(path, TOPIC), iterations, warmup)
    rosbags_samples = measure(
        lambda: read_with_rosbags(path, schema), iterations, warmup
    )
    file_bytes = path.stat().st_size
    native = summarize(native_samples, file_bytes, rows)
    pure_python = summarize(rosbags_samples, file_bytes, rows)
    return {
        "fixture": str(path),
        "file_bytes": file_bytes,
        "rows": rows,
        "mcapdecode": native,
        "rosbags": pure_python,
        "median_speedup": pure_python["median_ms"] / native["median_ms"],
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Python Decode Benchmark",
        "",
        "| Case | Rows | mcapdecode median | rosbags median | Speedup |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for case, result in report["cases"].items():
        lines.append(
            "| {case} | {rows} | {native:.2f} ms | {rosbags:.2f} ms | {speedup:.2f}x |".format(
                case=case,
                rows=result["rows"],
                native=result["mcapdecode"]["median_ms"],
                rosbags=result["rosbags"]["median_ms"],
                speedup=result["median_speedup"],
            )
        )
    lines.extend(
        [
            "",
            "## Environment",
            "",
            *[f"- {key}: {value}" for key, value in report["environment"].items()],
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, action="append", dest="cases")
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--fixture-dir", type=Path, default=Path("/tmp/mcapdecode-python-bench"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.iterations < 2:
        parser.error("--iterations must be at least 2")
    if args.warmup < 0:
        parser.error("--warmup must not be negative")
    return args


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cases = args.cases or CASES
    report = {
        "settings": {"iterations": args.iterations, "warmup": args.warmup},
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "machine": platform.machine(),
            "mcapdecode": package_version("mcapdecode"),
            "pyarrow": pa.__version__,
            "rosbags": package_version("rosbags"),
        },
        "cases": {
            case: benchmark_case(
                generate_fixture(case, args.fixture_dir), args.iterations, args.warmup
            )
            for case in cases
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
