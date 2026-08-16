#!/usr/bin/env python3
"""Reproducible GridEdge-T replay and read-path performance gate.

The harness intentionally uses only the Python standard library.  It executes
the release-facing CLI and HTTP API exactly as an operator would, writes all
measurements as JSON, and keeps nightly thresholds warn-only until explicitly
promoted with --enforce.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import csv
import hashlib
import json
import math
import os
import platform
import resource
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


API_TOKEN = "gridedge-perf-local-token"
ROOT = Path(__file__).resolve().parents[1]
INPUT_MANIFEST = ROOT / "configs/performance-inputs-v1.json"
BASELINE = {
    "dataset_bars": 34_944,
    "event_count": 110_532,
    "replay_wall_seconds": 30.345818,
    "replay_bars_per_second": 1151.526047,
    "replay_peak_rss_mib": 25.140625,
    "database_mib": 87.609375,
    "cold_status_p95_seconds": 0.509206,
    "rebuild_wall_seconds": 4.017797,
    "rebuild_peak_rss_mib": 159.203125,
    "snapshot_sequential_p95_seconds": 0.533831,
    "snapshot_sequential_response_bytes": 138_420,
    "bars_1000_p95_seconds": 0.004206,
    "bars_1000_response_bytes": 164_652,
    "events_1000_p95_seconds": 0.004441,
    "events_1000_response_bytes": 630_117,
    "snapshot_concurrency_4_p95_seconds": 0.232526,
}

REPLAY_HARD_TIMEOUT_SECONDS = 240.0

THRESHOLDS = {
    "short": {
        "replay_wall_seconds": 1.0,
        "replay_peak_rss_mib": 64.0,
        "database_mib": 2.0,
    },
    "nightly": {
        "replay_wall_seconds": 180.0,
        "replay_peak_rss_mib": 64.0,
        "database_mib": 100.0,
        "cold_status_p95_seconds": 0.75,
        "rebuild_wall_seconds": 8.0,
        "rebuild_peak_rss_mib": 256.0,
        "snapshot_sequential_p95_seconds": 0.75,
        "bars_1000_p95_seconds": 0.025,
        "events_1000_p95_seconds": 0.025,
        "snapshot_concurrency_4_p95_seconds": 1.0,
        "snapshot_sequential_response_bytes": 256 * 1024,
        "bars_1000_response_bytes": 256 * 1024,
        "events_1000_response_bytes": 1024 * 1024,
        "web_peak_rss_mib": 96.0,
    },
}

MINIMUM_THRESHOLDS = {
    "short": {},
    "nightly": {"replay_bars_per_second": 190.0},
}

CORRECTNESS_EXPECTATIONS = {
    "short": {
        "dataset_bars": 21,
        "event_count": 233,
        "replay_mode": "STOPPED",
        "duplicate_events": 0,
        "rebuild_matched": True,
        "rebuild_full_sequence": 233,
        "rebuild_snapshot_sequence": 233,
        "redundant_event_index_count": 0,
    },
    "nightly": {
        "dataset_bars": 34_944,
        "event_count": 110_532,
        "replay_mode": "STOPPED",
        "duplicate_events": 0,
        "rebuild_matched": True,
        "rebuild_full_sequence": 110_532,
        "rebuild_snapshot_sequence": 110_532,
        "redundant_event_index_count": 0,
    },
}


@dataclass(frozen=True)
class MeasuredProcess:
    wall_seconds: float
    peak_rss_mib: float
    stdout: str
    stderr: str
    returncode: int


@dataclass
class WebMeasurement:
    base_url: str
    peak_rss_mib: float | None = None


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def canonical_json_sha256(value: Any) -> str:
    return sha256_bytes(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def canonical_decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise RuntimeError("business result contains a non-finite Decimal")
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        whole, fraction = text.split(".", 1)
        fraction = fraction.rstrip("0")
        text = whole + (f".{fraction}" if fraction else "")
    return text


def is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def load_input_manifest(mode: str, content: bytes | None = None) -> dict[str, Any]:
    payload = json.loads(
        (content if content is not None else INPUT_MANIFEST.read_bytes())
    )
    if payload.get("schema_version") != 1 or set(payload.get("modes", {})) != {
        "short",
        "nightly",
    }:
        raise ValueError("performance input manifest shape is invalid")
    entry = payload["modes"].get(mode)
    if not isinstance(entry, dict):
        raise ValueError(f"performance input manifest has no {mode} entry")
    for field in (
        "source_config_sha256",
        "canonical_config_sha256",
        "data_sha256",
        "business_result_sha256",
    ):
        if not is_sha256(entry.get(field)):
            raise ValueError(f"performance input manifest {mode}.{field} is invalid")
    if type(entry.get("dataset_bars")) is not int or entry["dataset_bars"] < 1:
        raise ValueError(f"performance input manifest {mode}.dataset_bars is invalid")
    if not isinstance(entry.get("run_id"), str) or not entry["run_id"]:
        raise ValueError(f"performance input manifest {mode}.run_id is invalid")
    if type(entry.get("iterations")) is not int or entry["iterations"] < 1:
        raise ValueError(f"performance input manifest {mode}.iterations is invalid")
    algorithm = entry.get("algorithm")
    if not isinstance(algorithm, dict):
        raise ValueError(f"performance input manifest {mode}.algorithm is invalid")
    if not all(
        isinstance(algorithm.get(field), str) and algorithm[field]
        for field in ("algorithm_name", "algorithm_version")
    ) or not all(
        is_sha256(algorithm.get(field))
        for field in ("artifact_sha256", "environment_sha256")
    ):
        raise ValueError(f"performance input manifest {mode}.algorithm is invalid")
    if (
        algorithm.get("canonical_arguments") != []
        or type(algorithm.get("deterministic")) is not bool
        or not isinstance(algorithm.get("supported_contract_versions"), list)
        or not algorithm["supported_contract_versions"]
        or not all(
            type(version) is int and version > 0
            for version in algorithm["supported_contract_versions"]
        )
        or type(algorithm.get("supports_checkpoint")) is not bool
    ):
        raise ValueError(f"performance input manifest {mode}.algorithm is invalid")
    return dict(entry)


def input_identity(
    mode: str,
    binary: Path,
    source_config: Path,
    data: Path,
    run_id: str,
    iterations: int,
    expected: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bytes, bytes, bytes]:
    expected = dict(expected) if expected is not None else load_input_manifest(mode)
    binary_bytes = binary.read_bytes()
    config_bytes = source_config.read_bytes()
    data_bytes = data.read_bytes()
    inputs = {
        "source_config": {
            "path": str(source_config),
            "bytes": len(config_bytes),
            "expected_sha256": expected["source_config_sha256"],
            "actual_sha256": sha256_bytes(config_bytes),
        },
        "data": {
            "path": str(data),
            "bytes": len(data_bytes),
            "expected_sha256": expected["data_sha256"],
            "actual_sha256": sha256_bytes(data_bytes),
        },
        "binary": {
            "path": str(binary),
            "bytes": len(binary_bytes),
            "expected_sha256": None,
            "actual_sha256": sha256_bytes(binary_bytes),
        },
    }
    checks = []
    for name in ("source_config", "data"):
        item = inputs[name]
        passed = item["actual_sha256"] == item["expected_sha256"]
        item["passed"] = passed
        checks.append(
            {
                "metric": f"{name}_sha256",
                "actual": item["actual_sha256"],
                "expected": item["expected_sha256"],
                "passed": passed,
                "blocking": True,
                "kind": "input_identity",
            }
        )
    inputs["binary"]["passed"] = True
    for name, actual, expected_value in (
        ("run_id", run_id, expected["run_id"]),
        ("iterations", iterations, expected["iterations"]),
    ):
        checks.append(
            {
                "metric": name,
                "actual": actual,
                "expected": expected_value,
                "passed": actual == expected_value,
                "blocking": True,
                "kind": "workload_identity",
            }
        )
    provenance: dict[str, Any] = {}
    for name, spec in expected.get("provenance", {}).items():
        path = ROOT / spec["path"]
        content = path.read_bytes()
        actual = sha256_bytes(content)
        passed = actual == spec["sha256"]
        provenance[name] = {
            "path": spec["path"],
            "bytes": len(content),
            "expected_sha256": spec["sha256"],
            "actual_sha256": actual,
            "passed": passed,
        }
        checks.append(
            {
                "metric": f"provenance_{name}_sha256",
                "actual": actual,
                "expected": spec["sha256"],
                "passed": passed,
                "blocking": True,
                "kind": "input_identity",
            }
        )
    return (
        {
            "schema_version": 1,
            "mode": mode,
            "manifest": str(INPUT_MANIFEST.relative_to(ROOT)),
            "passed": all(check["passed"] for check in checks),
            "inputs": inputs,
            "provenance": provenance,
            "workload": {
                "run_id": run_id,
                "iterations": iterations,
            },
            "checks": checks,
            "expected_run_identity": {
                "canonical_config_sha256": expected["canonical_config_sha256"],
                "data_sha256": expected["data_sha256"],
                "dataset_bars": expected["dataset_bars"],
                "symbol": expected["symbol"],
                "first_timestamp": expected["first_timestamp"],
                "last_timestamp": expected["last_timestamp"],
                "algorithm": expected["algorithm"],
            },
        },
        binary_bytes,
        config_bytes,
        data_bytes,
    )


def write_identity_outputs(output_dir: Path, identity: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "input-identity.json").write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if identity["passed"]:
        return
    failed_kinds = {
        check["kind"] for check in identity["checks"] if not check["passed"]
    }
    if "stage_identity" in failed_kinds:
        failure_explanation = (
            "A frozen input changed between measurement stages; certification stopped."
        )
    elif "business_result" in failed_kinds:
        failure_explanation = "The deterministic business-result fingerprint changed."
    elif "run_identity" in failed_kinds:
        failure_explanation = "Durable ledger identity differs from the frozen inputs."
    else:
        failure_explanation = (
            "No child process was started and no replay database was created."
        )
    summary = [
        f"# GridEdge-T performance ({identity['mode']})",
        "",
        "Input identity: **FAIL (blocking)**",
        "",
        failure_explanation,
        "",
        "| Identity field | Actual | Expected | Result |",
        "|---|---|---|---|",
    ]
    for check in identity["checks"]:
        summary.append(
            f"| `{check['metric']}` | `{check['actual']}` | `{check['expected']}` | "
            f"{'PASS' if check['passed'] else 'FAIL'} |"
        )
    (output_dir / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")


def freeze_inputs(
    output_dir: Path,
    original_binary: Path,
    original_config: Path,
    original_data: Path,
    binary_bytes: bytes,
    config_bytes: bytes,
    data_bytes: bytes,
    manifest_bytes: bytes,
) -> tuple[Path, Path, Path, Path]:
    frozen = output_dir / "frozen-inputs"
    frozen.mkdir(parents=True, exist_ok=False)
    binary = frozen / original_binary.name
    source_config = frozen / original_config.name
    data = frozen / original_data.name
    manifest = frozen / INPUT_MANIFEST.name
    binary.write_bytes(binary_bytes)
    binary.chmod(original_binary.stat().st_mode | 0o111)
    source_config.write_bytes(config_bytes)
    data.write_bytes(data_bytes)
    manifest.write_bytes(manifest_bytes)
    return binary, source_config, data, manifest


def initialize_frozen_identity(
    identity: dict[str, Any],
    binary: Path,
    source_config: Path,
    data: Path,
    effective: Path,
    manifest: Path,
) -> None:
    identity["frozen_inputs"] = {
        "binary": {"path": str(binary), "sha256": sha256_bytes(binary.read_bytes())},
        "source_config": {
            "path": str(source_config),
            "sha256": sha256_bytes(source_config.read_bytes()),
        },
        "data": {"path": str(data), "sha256": sha256_bytes(data.read_bytes())},
        "effective_config": {
            "path": str(effective),
            "sha256": sha256_bytes(effective.read_bytes()),
        },
        "input_manifest": {
            "path": str(manifest),
            "sha256": sha256_bytes(manifest.read_bytes()),
        },
    }
    identity["stages"] = []


def verify_frozen_stage(identity: dict[str, Any], output_dir: Path, stage: str) -> None:
    checks = []
    for name, expected in identity["frozen_inputs"].items():
        path = Path(expected["path"])
        actual = sha256_bytes(path.read_bytes()) if path.is_file() else None
        checks.append(
            {
                "input": name,
                "actual_sha256": actual,
                "expected_sha256": expected["sha256"],
                "passed": actual == expected["sha256"],
            }
        )
    passed = all(check["passed"] for check in checks)
    identity["stages"].append({"stage": stage, "passed": passed, "checks": checks})
    if not passed:
        identity["passed"] = False
        identity["checks"].extend(
            {
                "metric": f"{stage}_{check['input']}_sha256",
                "actual": check["actual_sha256"],
                "expected": check["expected_sha256"],
                "passed": check["passed"],
                "blocking": True,
                "kind": "stage_identity",
            }
            for check in checks
            if not check["passed"]
        )
    write_identity_outputs(output_dir, identity)
    if not passed:
        raise RuntimeError(f"frozen input identity changed at stage {stage}")


def checked_frozen_process(
    command: list[str],
    *,
    identity: dict[str, Any],
    output_dir: Path,
    stage: str,
    timeout: float | None = None,
) -> MeasuredProcess:
    verify_frozen_stage(identity, output_dir, f"before_{stage}")
    result = (
        checked_process(command)
        if timeout is None
        else checked_process(command, timeout=timeout)
    )
    verify_frozen_stage(identity, output_dir, f"after_{stage}")
    return result


def percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[rank - 1]


def linux_rss_mib(pid: int) -> float | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return None


def run_measured(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> MeasuredProcess:
    before_children_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )
    peak_rss = 0.0
    stop = threading.Event()

    def sample() -> None:
        nonlocal peak_rss
        while not stop.wait(0.01):
            current = linux_rss_mib(process.pid)
            if current is not None:
                peak_rss = max(peak_rss, current)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise TimeoutError(
            f"command exceeded the {timeout:g}s deadline: {' '.join(command)}"
        ) from error
    finally:
        stop.set()
        sampler.join(timeout=1)
    wall = time.perf_counter() - started
    if peak_rss == 0.0:
        after = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        delta = max(before_children_rss, after)
        divisor = 1024.0 if platform.system() != "Darwin" else 1024.0 * 1024.0
        peak_rss = delta / divisor
    return MeasuredProcess(wall, peak_rss, stdout, stderr, process.returncode)


def checked_process(
    command: list[str], *, timeout: float | None = None
) -> MeasuredProcess:
    result = run_measured(command, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n{result.stderr}"
        )
    return result


def reserve_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def api_get(url: str, *, timeout: float = 30.0) -> tuple[float, int, Any]:
    request = urllib.request.Request(url, headers={"X-GridEdge-Api-Token": API_TOKEN})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            if response.status != 200:
                raise RuntimeError(f"GET {url} returned HTTP {response.status}")
    except TimeoutError as error:
        error.add_note(f"GET {url} exceeded the {timeout:g}s HTTP deadline")
        raise
    elapsed = time.perf_counter() - started
    return elapsed, len(body), json.loads(body)


def wait_for_health(base_url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20.0
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"web process exited before readiness: {process.returncode}"
            )
        try:
            with urllib.request.urlopen(f"{base_url}/ready", timeout=0.5) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(0.05)
    raise RuntimeError(f"web readiness deadline exceeded: {last_error}")


@contextlib.contextmanager
def web_server(binary: Path, config: Path, data: Path):
    port = reserve_port()
    env = os.environ.copy()
    env["GRIDEDGE_API_TOKEN"] = API_TOKEN
    process = subprocess.Popen(
        [
            str(binary),
            "web",
            "--config",
            str(config),
            "--data",
            str(data),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    base_url = f"http://127.0.0.1:{port}"
    measurement = WebMeasurement(base_url)
    stop_sampler = threading.Event()

    def sample_rss() -> None:
        while not stop_sampler.wait(0.01):
            current = linux_rss_mib(process.pid)
            if current is not None:
                measurement.peak_rss_mib = max(measurement.peak_rss_mib or 0.0, current)

    sampler: threading.Thread | None = None
    try:
        wait_for_health(base_url, process)
        initial_rss = linux_rss_mib(process.pid)
        if initial_rss is not None:
            measurement.peak_rss_mib = initial_rss
            sampler = threading.Thread(target=sample_rss, daemon=True)
            sampler.start()
        yield measurement
    finally:
        stop_sampler.set()
        if sampler is not None:
            sampler.join(timeout=1)
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.returncode not in (0, 130, -signal.SIGINT):
            stderr = process.stderr.read() if process.stderr else ""
            cleanup_error = RuntimeError(
                f"web process failed ({process.returncode}): {stderr}"
            )
            active_error = sys.exception()
            if active_error is None:
                raise cleanup_error
            active_error.add_note(str(cleanup_error))


def replace_database(source: Path, destination: Path, database: Path) -> None:
    lines = source.read_text(encoding="utf-8").splitlines()
    matches = [
        index for index, line in enumerate(lines) if line.startswith("database:")
    ]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one database key in {source}")
    lines[matches[0]] = f'database: "{database.as_posix()}"'
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def exact_event_payload(database: Path, run_id: str, event_type: str) -> dict[str, Any]:
    with contextlib.closing(sqlite3.connect(database)) as connection:
        rows = connection.execute(
            "SELECT payload FROM events WHERE run_id=? AND event_type=? ORDER BY sequence_number",
            (run_id, event_type),
        ).fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"expected exactly one {event_type} for {run_id}, found {len(rows)}"
        )
    payload = json.loads(rows[0][0])
    if not isinstance(payload, dict):
        raise RuntimeError(f"{event_type} payload is not an object")
    return payload


def verify_durable_run_identity(
    database: Path, run_id: str, identity: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    expected = identity["expected_run_identity"]
    config = exact_event_payload(database, run_id, "CONFIG_SNAPSHOTTED")
    algorithm = exact_event_payload(database, run_id, "ALGORITHM_REGISTERED")
    replay = exact_event_payload(database, run_id, "REPLAY_INITIALIZED")
    observed = {
        "run_id": run_id,
        "canonical_config_sha256": config.get("_content_sha256"),
        "data_sha256": replay.get("data_sha256"),
        "dataset_bars": replay.get("total_bars"),
        "symbol": replay.get("symbol"),
        "first_timestamp": replay.get("first_timestamp"),
        "last_timestamp": replay.get("last_timestamp"),
        "algorithm_name": algorithm.get("algorithm_name"),
        "algorithm_version": algorithm.get("algorithm_version"),
        "artifact_sha256": algorithm.get("artifact_sha256"),
        "environment_sha256": algorithm.get("environment_sha256"),
        "platform_sha256": algorithm.get("platform_sha256"),
        "algorithm_manifest": algorithm,
        "algorithm_manifest_sha256": canonical_json_sha256(algorithm),
        "canonical_arguments": algorithm.get("canonical_arguments"),
        "deterministic": algorithm.get("deterministic"),
        "supported_contract_versions": algorithm.get("supported_contract_versions"),
        "supports_checkpoint": algorithm.get("supports_checkpoint"),
    }
    expected_values = {
        **{key: value for key, value in expected.items() if key != "algorithm"},
        "platform_sha256": identity["inputs"]["binary"]["actual_sha256"],
        **expected["algorithm"],
    }
    checks = []
    for field, expected_value in expected_values.items():
        actual = observed.get(field)
        checks.append(
            {
                "metric": f"durable_{field}",
                "actual": actual,
                "expected": expected_value,
                "passed": actual == expected_value,
                "blocking": True,
                "kind": "run_identity",
            }
        )
    return observed, checks


def csv_bar_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]
    if not rows:
        raise ValueError(f"CSV has no header: {path}")
    return len(rows) - 1


def response_measurements(
    base_url: str, run_id: str, iterations: int
) -> dict[str, float | int]:
    endpoints = {
        "snapshot_sequential": f"{base_url}/api/v1/runs/{run_id}",
        "bars_1000": f"{base_url}/api/v1/runs/{run_id}/bars?max_points=1000",
        "events_1000": f"{base_url}/api/v1/runs/{run_id}/events?after=0&limit=1000",
    }
    measurements: dict[str, float | int] = {}
    for name, endpoint in endpoints.items():
        samples = [api_get(endpoint) for _ in range(iterations)]
        measurements[f"{name}_p95_seconds"] = percentile(
            [sample[0] for sample in samples], 0.95
        )
        measurements[f"{name}_response_bytes"] = max(sample[1] for sample in samples)

    concurrent_latencies: list[float] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for _ in range(iterations):
            futures = [
                executor.submit(api_get, endpoints["snapshot_sequential"])
                for _ in range(4)
            ]
            concurrent_latencies.extend(future.result()[0] for future in futures)
    measurements["snapshot_concurrency_4_p95_seconds"] = percentile(
        concurrent_latencies, 0.95
    )
    return measurements


def database_measurements(database: Path) -> dict[str, Any]:
    size = database.stat().st_size
    with contextlib.closing(sqlite3.connect(database)) as connection:
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        redundant_event_index_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN "
                "('idx_events_run_sequence','idx_events_market_run',"
                "'idx_events_processed_bar_run')"
            ).fetchone()[0]
        )
    live_bytes = (page_count - freelist_count) * page_size
    return {
        "path": database.name,
        "bytes": size,
        "mib": size / (1024 * 1024),
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "live_bytes": live_bytes,
        "live_mib": live_bytes / (1024 * 1024),
        "redundant_event_index_count": redundant_event_index_count,
    }


BUSINESS_RESULT_FIELDS = (
    "ambiguous_bars",
    "available_cash",
    "closed_lots",
    "conservative_exit_adjustment",
    "conservative_exit_unrealized_grid_pnl",
    "current_deferred_quantity",
    "deployed_grid_quantity",
    "duplicate_events",
    "event_count",
    "executed_levels",
    "last_price",
    "mark_to_market_unrealized_grid_pnl",
    "mode",
    "open_lots",
    "open_orders",
    "realized_grid_pnl",
    "sellable_position",
    "skipped_levels",
    "standard_quantity",
    "total_conservative_exit_grid_pnl",
    "total_fees",
    "total_mark_to_market_grid_pnl",
    "total_position",
    "touched_levels",
    "unpriced_open_quantity",
    "valuation_policy_version",
)


def business_result_v1(
    database: Path, run_id: str, status_payload: dict[str, Any]
) -> dict[str, Any]:
    missing = [field for field in BUSINESS_RESULT_FIELDS if field not in status_payload]
    if missing:
        raise RuntimeError(f"status is missing business result fields: {missing}")
    with contextlib.closing(sqlite3.connect(database)) as connection:
        histogram = {
            str(event_type): int(count)
            for event_type, count in connection.execute(
                "SELECT event_type, COUNT(*) FROM events WHERE run_id=? "
                "GROUP BY event_type ORDER BY event_type",
                (run_id,),
            )
        }
        quantity_payloads = connection.execute(
            "SELECT event_type, payload FROM events WHERE run_id=? AND event_type IN "
            "('GRID_RIGHT_GRANTED','GATE_DECISION_MADE','GRID_RIGHT_DEFERRED',"
            "'GRID_RIGHT_RESIDUAL_HELD',"
            "'GRID_RIGHT_BLOCKED','GRID_RIGHT_RESERVED','ORDER_INTENT_CREATED',"
            "'ORDER_PARTIALLY_FILLED','ORDER_FILLED') "
            "ORDER BY sequence_number",
            (run_id,),
        ).fetchall()
    quantities = {
        "t_plus_one_blocked": 0,
        "risk_blocked_before_decision": 0,
        "no_profit_blocked": 0,
        "gross_available": 0,
        "platform_residual": 0,
        "algorithm_authorized": 0,
        "decision_exercise": 0,
        "decision_defer": 0,
        "platform_blocked": 0,
        "order_intent": 0,
        "remaining_decision": 0,
        "intent_buy": 0,
        "intent_sell": 0,
        "fill_buy": 0,
        "fill_sell": 0,
    }
    dispositions: dict[str, tuple[int, int, int, int, int, int]] = {}
    for event_type, raw_payload in quantity_payloads:
        payload = json.loads(raw_payload)
        if event_type == "GRID_RIGHT_GRANTED":
            capacity = payload["capacity"]
            quantities["t_plus_one_blocked"] += int(
                capacity["t_plus_one_blocked_quantity"]
            )
            quantities["risk_blocked_before_decision"] += int(
                capacity["risk_blocked_quantity"]
            )
            quantities["no_profit_blocked"] += int(
                capacity["no_profit_blocked_quantity"]
            )
        elif event_type == "GATE_DECISION_MADE":
            outcome = payload["response"]["outcome"]
            quantities["decision_exercise"] += int(outcome.get("exercise_quantity", 0))
            quantities["decision_defer"] += int(outcome.get("defer_quantity", 0))
        elif event_type in {
            "GRID_RIGHT_DEFERRED",
            "GRID_RIGHT_RESIDUAL_HELD",
            "GRID_RIGHT_BLOCKED",
            "GRID_RIGHT_RESERVED",
        }:
            if event_type == "GRID_RIGHT_BLOCKED" and payload.get("partial") is True:
                if any(
                    int(payload[field]) < 0
                    for field in (
                        "t_plus_one_blocked_quantity",
                        "risk_blocked_quantity",
                        "no_profit_blocked_quantity",
                    )
                ):
                    raise RuntimeError("partial block contains a negative quantity")
                continue
            right_id = str(payload["right_id"])
            partition = (
                int(payload["gross_available_quantity"]),
                int(payload["platform_blocked_quantity"]),
                int(payload["order_intent_quantity"]),
                int(payload["remaining_decision_quantity"]),
                int(payload["algorithm_authorized_quantity"]),
                int(payload["platform_residual_quantity"]),
            )
            previous = dispositions.setdefault(right_id, partition)
            if previous != partition:
                raise RuntimeError(f"right {right_id} has inconsistent dispositions")
        elif event_type == "ORDER_INTENT_CREATED":
            intent = payload["intent"]
            quantities[f"intent_{intent['direction'].lower()}"] += int(
                intent["quantity"]
            )
        else:
            fill = payload["fill"]
            quantities[f"fill_{fill['direction'].lower()}"] += int(fill["quantity"])
    for (
        gross,
        blocked,
        intent,
        remaining,
        authorized,
        residual,
    ) in dispositions.values():
        quantities["gross_available"] += gross
        quantities["platform_residual"] += residual
        quantities["algorithm_authorized"] += authorized
        quantities["platform_blocked"] += blocked
        quantities["order_intent"] += intent
        quantities["remaining_decision"] += remaining
    if quantities["gross_available"] != (
        quantities["algorithm_authorized"] + quantities["platform_residual"]
    ):
        raise RuntimeError("business result violates C=A+P")
    if quantities["algorithm_authorized"] != (
        quantities["decision_exercise"] + quantities["decision_defer"]
    ):
        raise RuntimeError("business result violates A=E+D")
    if quantities["order_intent"] != (
        quantities["decision_exercise"] - quantities["platform_blocked"]
    ):
        raise RuntimeError("business result violates I=E-B")
    if quantities["remaining_decision"] != (
        quantities["decision_defer"] + quantities["platform_blocked"]
    ):
        raise RuntimeError("business result violates R=D+B")
    if (
        quantities["intent_buy"] + quantities["intent_sell"]
        != quantities["order_intent"]
    ):
        raise RuntimeError("business result intent direction totals differ from I")
    standard_quantity = int(status_payload["standard_quantity"])
    for name in (
        "algorithm_authorized",
        "decision_exercise",
        "decision_defer",
        "platform_blocked",
        "order_intent",
        "remaining_decision",
    ):
        if quantities[name] < 0 or quantities[name] % standard_quantity != 0:
            raise RuntimeError(f"business result {name} is not whole-Q")
    result = {
        "schema_version": 1,
        "event_type_histogram": histogram,
        "quantities": quantities,
        "terminal": {field: status_payload[field] for field in BUSINESS_RESULT_FIELDS},
    }
    result["sha256"] = canonical_json_sha256(result)
    return result


def business_result(
    database: Path, run_id: str, status_payload: dict[str, Any]
) -> dict[str, Any]:
    """Build the fail-closed v2 business certification fingerprint.

    V1 remains available above solely to make the previous global fingerprint
    reproducible. V2 adds direction-local rights equations and independent lot,
    tranche and Paper-ledger safety facts before hashing the result.
    """
    legacy = business_result_v1(database, run_id, status_payload)
    global_quantities = legacy["quantities"]
    quantity_fields = tuple(global_quantities)

    def empty_quantities() -> dict[str, int]:
        return {field: 0 for field in quantity_fields}

    with contextlib.closing(sqlite3.connect(database)) as connection:
        payload_rows = connection.execute(
            "SELECT event_type,payload FROM events WHERE run_id=? AND event_type IN "
            "('GRID_RIGHT_GRANTED','GATE_DECISION_MADE','GRID_RIGHT_DEFERRED',"
            "'GRID_RIGHT_RESIDUAL_HELD','GRID_RIGHT_BLOCKED','GRID_RIGHT_RESERVED',"
            "'ORDER_INTENT_CREATED','ORDER_PARTIALLY_FILLED','ORDER_FILLED') "
            "ORDER BY sequence_number",
            (run_id,),
        ).fetchall()
        snapshot_row = connection.execute(
            "SELECT sequence_number,state_json,checksum FROM snapshots WHERE run_id=? "
            "ORDER BY sequence_number DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        paper_account_row = connection.execute(
            "SELECT snapshot_json FROM paper_accounts WHERE run_id=?",
            (run_id,),
        ).fetchone()
        paper_order_rows = connection.execute(
            "SELECT intent_id,order_id,report_json FROM paper_orders "
            "WHERE run_id=? ORDER BY intent_id",
            (run_id,),
        ).fetchall()
    if snapshot_row is None or paper_account_row is None:
        raise RuntimeError(
            "business result requires final strategy and Paper snapshots"
        )
    snapshot_sequence, raw_state, snapshot_checksum = snapshot_row
    if sha256_bytes(raw_state.encode("utf-8")) != snapshot_checksum:
        raise RuntimeError("latest strategy snapshot checksum is invalid")
    if int(snapshot_sequence) != int(status_payload["event_count"]):
        raise RuntimeError("latest strategy snapshot is not at the journal head")
    state = json.loads(raw_state)
    if state.get("run_id") != run_id:
        raise RuntimeError("strategy snapshot run id differs from certification run")
    if int(state["event_count"]) != int(status_payload["event_count"]):
        raise RuntimeError("strategy snapshot event count differs from status")
    for lot_id, lot in state["lots"].items():
        if str(lot_id) != str(lot["lot_id"]):
            raise RuntimeError("strategy lot map key differs from lot id")
    state_lot_ids = {str(lot_id) for lot_id in state["lots"]}

    by_direction = {"BUY": empty_quantities(), "SELL": empty_quantities()}
    direction_by_right: dict[str, str] = {}
    decision_by_right: dict[str, tuple[str, int, int]] = {}
    intent_by_right: dict[str, tuple[str, int]] = {}
    dispositions: dict[str, tuple[str, tuple[int, int, int, int, int, int]]] = {}
    ledger_fill_ids: list[str] = []
    sell_allocation_profits: list[Decimal] = []
    actual_sell_profits: list[Decimal] = []
    actual_profit_by_lot: dict[str, Decimal] = {}
    sold_cost_by_lot: dict[str, Decimal] = {}
    for event_type, raw_payload in payload_rows:
        payload = json.loads(raw_payload)
        if event_type == "GRID_RIGHT_GRANTED":
            right_id = str(payload["right_id"])
            direction = str(payload["direction"])
            if direction not in by_direction:
                raise RuntimeError(f"unsupported right direction {direction}")
            if right_id in direction_by_right:
                raise RuntimeError(f"right {right_id} has duplicate grants")
            direction_by_right[right_id] = direction
            capacity = payload["capacity"]
            if any(
                int(capacity[field]) < 0
                for field in (
                    "t_plus_one_blocked_quantity",
                    "risk_blocked_quantity",
                    "no_profit_blocked_quantity",
                )
            ):
                raise RuntimeError(f"right {right_id} has negative blocked capacity")
            by_direction[direction]["t_plus_one_blocked"] += int(
                capacity["t_plus_one_blocked_quantity"]
            )
            by_direction[direction]["risk_blocked_before_decision"] += int(
                capacity["risk_blocked_quantity"]
            )
            by_direction[direction]["no_profit_blocked"] += int(
                capacity["no_profit_blocked_quantity"]
            )
        elif event_type == "GATE_DECISION_MADE":
            context = payload["request"]["context"]
            request_right = payload["request"]["right"]
            response = payload["response"]
            right_id = str(context["right_id"])
            if (
                str(response["right_id"]) != right_id
                or str(request_right["right_id"]) != right_id
            ):
                raise RuntimeError("decision response changes right id")
            direction = str(context["direction"])
            if direction not in by_direction:
                raise RuntimeError(f"unsupported decision direction {direction}")
            if direction_by_right.get(right_id) != direction:
                raise RuntimeError(f"decision direction differs for right {right_id}")
            if str(request_right["direction"]) != direction:
                raise RuntimeError(f"decision request right differs for {right_id}")
            outcome = response["outcome"]
            exercise = int(outcome.get("exercise_quantity", 0))
            defer = int(outcome.get("defer_quantity", 0))
            if right_id in decision_by_right:
                raise RuntimeError(f"right {right_id} has duplicate decisions")
            decision_by_right[right_id] = (direction, exercise, defer)
            by_direction[direction]["decision_exercise"] += exercise
            by_direction[direction]["decision_defer"] += defer
        elif event_type in {
            "GRID_RIGHT_DEFERRED",
            "GRID_RIGHT_RESIDUAL_HELD",
            "GRID_RIGHT_BLOCKED",
            "GRID_RIGHT_RESERVED",
        }:
            right_id = str(payload["right_id"])
            direction = direction_by_right.get(right_id)
            if direction is None:
                raise RuntimeError(
                    f"right {right_id} disposition has no grant direction"
                )
            if event_type == "GRID_RIGHT_BLOCKED" and payload.get("partial") is True:
                if any(
                    int(payload[field]) < 0
                    for field in (
                        "t_plus_one_blocked_quantity",
                        "risk_blocked_quantity",
                        "no_profit_blocked_quantity",
                    )
                ):
                    raise RuntimeError("partial block contains a negative quantity")
                continue
            partition = (
                int(payload["gross_available_quantity"]),
                int(payload["platform_blocked_quantity"]),
                int(payload["order_intent_quantity"]),
                int(payload["remaining_decision_quantity"]),
                int(payload["algorithm_authorized_quantity"]),
                int(payload["platform_residual_quantity"]),
            )
            if right_id in dispositions:
                raise RuntimeError(f"right {right_id} has duplicate dispositions")
            dispositions[right_id] = (direction, partition)
        elif event_type == "ORDER_INTENT_CREATED":
            intent = payload["intent"]
            direction = str(intent["direction"])
            right_id = str(intent["right_id"])
            if direction_by_right.get(right_id) != direction:
                raise RuntimeError(f"intent direction differs for right {right_id}")
            if right_id in intent_by_right:
                raise RuntimeError(f"right {right_id} has duplicate intents")
            intent_by_right[right_id] = (direction, int(intent["quantity"]))
            by_direction[direction][f"intent_{direction.lower()}"] += int(
                intent["quantity"]
            )
        else:
            fill = payload["fill"]
            direction = str(fill["direction"])
            by_direction[direction][f"fill_{direction.lower()}"] += int(
                fill["quantity"]
            )
            ledger_fill_ids.append(str(fill["fill_id"]))
            if direction == "SELL":
                allocations = fill["target_lot_allocations"]
                if not allocations or sum(
                    int(allocation["quantity"]) for allocation in allocations
                ) != int(fill["quantity"]):
                    raise RuntimeError(
                        "SELL fill allocations do not cover the fill quantity"
                    )
                remaining_fees = Decimal(str(fill["commission"])) + Decimal(
                    str(fill["tax"])
                )
                if remaining_fees < 0:
                    raise RuntimeError("SELL fill has negative total fees")
                for allocation in allocations:
                    quantity = int(allocation["quantity"])
                    cost_basis = Decimal(str(allocation["cost_basis"]))
                    worst_profit = Decimal(str(allocation["worst_case_profit"]))
                    maximum_fees = Decimal(str(allocation["maximum_sell_fees"]))
                    worst_fill_price = Decimal(str(allocation["worst_fill_price"]))
                    lot_id = str(allocation["lot_id"])
                    if lot_id not in state_lot_ids:
                        raise RuntimeError("SELL allocation names an unknown lot")
                    if quantity <= 0 or cost_basis < 0 or maximum_fees < 0:
                        raise RuntimeError(
                            "SELL allocation has invalid quantity, cost, or fees"
                        )
                    if worst_profit != (
                        worst_fill_price * Decimal(quantity) - maximum_fees - cost_basis
                    ):
                        raise RuntimeError("SELL allocation worst-case proof differs")
                    assigned_fees = min(remaining_fees, maximum_fees)
                    remaining_fees -= assigned_fees
                    actual_profit = (
                        Decimal(str(fill["price"])) * Decimal(quantity)
                        - cost_basis
                        - assigned_fees
                    )
                    actual_profit_by_lot[lot_id] = (
                        actual_profit_by_lot.get(lot_id, Decimal(0)) + actual_profit
                    )
                    sold_cost_by_lot[lot_id] = (
                        sold_cost_by_lot.get(lot_id, Decimal(0)) + cost_basis
                    )
                    sell_allocation_profits.append(worst_profit)
                    actual_sell_profits.append(actual_profit)
                if remaining_fees != 0:
                    raise RuntimeError(
                        "SELL fill fees exceed canonical allocation ceilings"
                    )
    right_partitions: list[dict[str, Any]] = []
    for right_id, (direction, partition) in dispositions.items():
        gross, blocked, intent, remaining, authorized, residual = partition
        decision = decision_by_right.get(right_id)
        exercise = 0 if decision is None else decision[1]
        defer = 0 if decision is None else decision[2]
        if decision is not None and decision[0] != direction:
            raise RuntimeError(f"right {right_id} decision direction differs")
        if authorized > 0 and decision is None:
            raise RuntimeError(f"right {right_id} has authorization without a decision")
        if (
            gross < 0
            or residual < 0
            or residual >= int(status_payload["standard_quantity"])
        ):
            raise RuntimeError(f"right {right_id} has an invalid C/P partition")
        if gross != authorized + residual or authorized != exercise + defer:
            raise RuntimeError(f"right {right_id} violates C=A+P or A=E+D")
        if intent != exercise - blocked or remaining != defer + blocked:
            raise RuntimeError(f"right {right_id} violates I=E-B or R=D+B")
        for field, value in {
            "A": authorized,
            "E": exercise,
            "D": defer,
            "B": blocked,
            "I": intent,
            "R": remaining,
        }.items():
            if value < 0 or value % int(status_payload["standard_quantity"]) != 0:
                raise RuntimeError(f"right {right_id} {field} is not whole-Q")
        recorded_intent = intent_by_right.get(right_id)
        if intent == 0 and recorded_intent is not None:
            raise RuntimeError(f"right {right_id} has an unexpected intent")
        if intent > 0 and recorded_intent != (direction, intent):
            raise RuntimeError(f"right {right_id} intent differs from I")
        right_partitions.append(
            {
                "right_id": right_id,
                "direction": direction,
                "gross_available": gross,
                "platform_residual": residual,
                "algorithm_authorized": authorized,
                "decision_exercise": exercise,
                "decision_defer": defer,
                "platform_blocked": blocked,
                "order_intent": intent,
                "remaining_decision": remaining,
            }
        )
        values = by_direction[direction]
        values["gross_available"] += gross
        values["platform_residual"] += residual
        values["algorithm_authorized"] += authorized
        values["platform_blocked"] += blocked
        values["order_intent"] += intent
        values["remaining_decision"] += remaining
    if not (set(direction_by_right) == set(decision_by_right) == set(dispositions)):
        raise RuntimeError("grant, decision, and disposition right sets differ")
    if set(intent_by_right) - set(dispositions):
        raise RuntimeError("intent has no terminal right disposition")
    right_partitions.sort(key=lambda value: value["right_id"])

    standard_quantity = int(status_payload["standard_quantity"])

    def validate_partition(label: str, values: dict[str, int]) -> None:
        if values["gross_available"] != (
            values["algorithm_authorized"] + values["platform_residual"]
        ):
            raise RuntimeError(f"business result {label} violates C=A+P")
        if values["algorithm_authorized"] != (
            values["decision_exercise"] + values["decision_defer"]
        ):
            raise RuntimeError(f"business result {label} violates A=E+D")
        if values["order_intent"] != (
            values["decision_exercise"] - values["platform_blocked"]
        ):
            raise RuntimeError(f"business result {label} violates I=E-B")
        if values["remaining_decision"] != (
            values["decision_defer"] + values["platform_blocked"]
        ):
            raise RuntimeError(f"business result {label} violates R=D+B")
        if values["intent_buy"] + values["intent_sell"] != values["order_intent"]:
            raise RuntimeError(f"business result {label} intent totals differ from I")
        for field in (
            "algorithm_authorized",
            "decision_exercise",
            "decision_defer",
            "platform_blocked",
            "order_intent",
            "remaining_decision",
        ):
            if values[field] < 0 or values[field] % standard_quantity != 0:
                raise RuntimeError(f"business result {label} {field} is not whole-Q")

    for direction, values in by_direction.items():
        validate_partition(direction, values)
    for field in quantity_fields:
        if global_quantities[field] != sum(
            values[field] for values in by_direction.values()
        ):
            raise RuntimeError(f"business result direction totals differ for {field}")

    def decimal_text(value: Decimal) -> str:
        return canonical_decimal_text(value)

    lots = list(state["lots"].values())
    realized_values = [Decimal(str(lot["realized_pnl"])) for lot in lots]
    negative_realized = sum(value < 0 for value in realized_values)
    negative_remaining_cost = sum(
        Decimal(str(lot["remaining_cost"])) < 0 for lot in lots
    )
    negative_sell_allocations = sum(profit < 0 for profit in sell_allocation_profits)
    negative_actual_sell_allocations = sum(profit < 0 for profit in actual_sell_profits)
    allocated_cost_sum = Decimal(0)
    sold_cost_sum = Decimal(0)
    remaining_cost_sum = Decimal(0)
    cost_conservation_violations = 0
    for lot in lots:
        lot_id = str(lot["lot_id"])
        actual_profit = actual_profit_by_lot.get(lot_id, Decimal(0))
        if actual_profit != Decimal(str(lot["realized_pnl"])):
            raise RuntimeError(
                f"actual SELL slice PnL differs from lot {lot['lot_id']}"
            )
        allocated_cost = Decimal(str(lot["allocated_cost"]))
        sold_cost = sold_cost_by_lot.get(lot_id, Decimal(0))
        remaining_cost = Decimal(str(lot["remaining_cost"]))
        allocated_cost_sum += allocated_cost
        sold_cost_sum += sold_cost
        remaining_cost_sum += remaining_cost
        cost_conservation_violations += sold_cost + remaining_cost != allocated_cost
    if cost_conservation_violations:
        raise RuntimeError("business result violates lot cost conservation")
    if (
        negative_realized
        or negative_remaining_cost
        or negative_sell_allocations
        or negative_actual_sell_allocations
    ):
        raise RuntimeError(
            "business result contains a negative-profit sell slice or lot"
        )
    open_lots = sum(int(lot["remaining_quantity"]) > 0 for lot in lots)
    if open_lots != int(status_payload["open_lots"]):
        raise RuntimeError("strategy lot count differs from status")
    if len(lots) - open_lots != int(status_payload["closed_lots"]):
        raise RuntimeError("strategy closed-lot count differs from status")
    realized_sum = sum(realized_values, Decimal(0))
    if realized_sum != Decimal(str(status_payload["realized_grid_pnl"])):
        raise RuntimeError("lot realized PnL does not equal terminal realized PnL")
    lot_safety = {
        "lot_count": len(lots),
        "open_lot_count": open_lots,
        "negative_realized_lot_count": negative_realized,
        "negative_remaining_cost_lot_count": negative_remaining_cost,
        "cost_conservation_violation_count": cost_conservation_violations,
        "allocated_cost_sum": decimal_text(allocated_cost_sum),
        "sold_cost_basis_sum": decimal_text(sold_cost_sum),
        "remaining_cost_sum": decimal_text(remaining_cost_sum),
        "realized_pnl_sum": decimal_text(realized_sum),
        "minimum_realized_pnl": (
            decimal_text(min(realized_values)) if realized_values else None
        ),
        "sell_allocation_count": len(sell_allocation_profits),
        "negative_sell_allocation_count": negative_sell_allocations,
        "negative_actual_sell_allocation_count": negative_actual_sell_allocations,
        "minimum_worst_case_sell_profit": (
            decimal_text(min(sell_allocation_profits))
            if sell_allocation_profits
            else None
        ),
        "worst_case_sell_profit_sum": decimal_text(
            sum(sell_allocation_profits, Decimal(0))
        ),
        "minimum_actual_sell_profit": (
            decimal_text(min(actual_sell_profits)) if actual_sell_profits else None
        ),
        "actual_sell_profit_sum": decimal_text(sum(actual_sell_profits, Decimal(0))),
    }

    quantity_buckets = (
        "minted_quantity",
        "available_quantity",
        "reserved_quantity",
        "consumed_quantity",
        "revoked_quantity",
        "expired_quantity",
    )
    budget_buckets = tuple(
        field.replace("quantity", "budget") for field in quantity_buckets
    )
    tranche_totals: dict[str, dict[str, int | Decimal]] = {
        direction: {
            "tranche_count": 0,
            **{field: 0 for field in quantity_buckets},
            **{field: Decimal(0) for field in budget_buckets},
        }
        for direction in ("BUY", "SELL")
    }
    tranches = list(state["right_tranches"].values())
    conservation_violations = 0
    for tranche in tranches:
        direction = str(tranche["direction"])
        totals = tranche_totals.get(direction)
        if totals is None:
            raise RuntimeError(f"unsupported tranche direction {direction}")
        if tranche.get("unit") != "QUANTITY":
            raise RuntimeError("business result contains a non-quantity tranche")
        totals["tranche_count"] = int(totals["tranche_count"]) + 1
        for field in quantity_buckets:
            value = int(tranche[field])
            if value < 0:
                raise RuntimeError("business result contains a negative tranche bucket")
            totals[field] = int(totals[field]) + value
        for field in budget_buckets:
            value = Decimal(str(tranche[field]))
            if value < 0:
                raise RuntimeError("business result contains a negative tranche budget")
            totals[field] = Decimal(totals[field]) + value
        quantity_delta = int(tranche["minted_quantity"]) - sum(
            int(tranche[field]) for field in quantity_buckets[1:]
        )
        budget_delta = Decimal(str(tranche["minted_budget"])) - sum(
            (Decimal(str(tranche[field])) for field in budget_buckets[1:]),
            Decimal(0),
        )
        conservation_violations += quantity_delta != 0 or budget_delta != 0
    if conservation_violations:
        raise RuntimeError("business result violates tranche conservation")
    tranche_safety = {
        "tranche_count": len(tranches),
        "conservation_violation_count": conservation_violations,
        "by_direction": {
            direction: {
                field: decimal_text(value) if isinstance(value, Decimal) else value
                for field, value in totals.items()
            }
            for direction, totals in tranche_totals.items()
        },
    }

    paper = json.loads(paper_account_row[0])
    paper_order_ids: list[str] = []
    paper_intent_ids: list[str] = []
    paper_fill_ids: list[str] = []
    paper_fill_quantity = 0
    normalized_reports: list[dict[str, Any]] = []
    report_counts = {"accepted": 0, "rejected": 0, "cancelled": 0}
    for stored_intent_id, stored_order_id, raw_report in paper_order_rows:
        report = json.loads(raw_report)
        if len(report) != 1:
            raise RuntimeError("Paper report must have exactly one variant")
        variant, payload = next(iter(report.items()))
        order_id = str(payload["order_id"])
        if order_id != str(stored_order_id):
            raise RuntimeError("Paper report order id differs from its database key")
        intent_id = str(stored_intent_id)
        paper_intent_ids.append(intent_id)
        paper_order_ids.append(order_id)
        normalized_variant = variant.lower()
        if normalized_variant not in report_counts:
            raise RuntimeError(f"unsupported Paper report variant {variant}")
        fills = payload.get("fills", [])
        if normalized_variant in {"rejected", "cancelled"} and fills:
            raise RuntimeError(f"Paper {variant} report must not contain fills")
        report_counts[normalized_variant] += 1
        normalized_reports.append(
            {"intent_id": intent_id, "order_id": order_id, "report": report}
        )
        for fill in fills:
            paper_fill_ids.append(str(fill["fill_id"]))
            paper_fill_quantity += int(fill["quantity"])
    unique_order_ids = len(paper_order_ids) == len(set(paper_order_ids))
    unique_intent_ids = len(paper_intent_ids) == len(set(paper_intent_ids))
    unique_fill_ids = len(paper_fill_ids) == len(set(paper_fill_ids))
    state_order_ids = sorted(str(order_id) for order_id in state["orders"])
    open_statuses = {
        "CREATED",
        "SUBMITTED",
        "ACCEPTED",
        "PARTIALLY_FILLED",
        "CANCEL_PENDING",
    }
    strategy_open_order_ids = sorted(
        str(order_id)
        for order_id, order in state["orders"].items()
        if str(order["status"]) in open_statuses
    )
    state_intent_order_pairs = sorted(
        (str(order["intent"]["intent_id"]), str(order_id))
        for order_id, order in state["orders"].items()
    )
    paper_intent_order_pairs = sorted(zip(paper_intent_ids, paper_order_ids))
    strategy_filled_quantity = sum(
        int(order["filled_quantity"]) for order in state["orders"].values()
    )
    paper_open_order_ids = sorted(str(value) for value in paper["open_order_ids"])
    ledger_fill_ids.sort()
    paper_fill_ids.sort()
    comparisons = {
        "cash_available": (paper["cash"]["available"], state["cash"]["available"]),
        "cash_frozen": (paper["cash"]["frozen"], state["cash"]["frozen"]),
        "total_fees": (paper["cash"]["total_fees"], state["cash"]["total_fees"]),
        "position_total": (paper["position"]["total"], state["position"]["total"]),
        "position_sellable": (
            paper["position"]["sellable"],
            state["position"]["sellable"],
        ),
        "position_today_bought": (
            paper["position"]["today_bought"],
            state["position"]["today_bought"],
        ),
        "position_frozen_sell": (
            paper["position"]["frozen_sell"],
            state["position"]["frozen_sell"],
        ),
        "open_order_ids": (paper_open_order_ids, strategy_open_order_ids),
        "order_ids": (sorted(paper_order_ids), state_order_ids),
        "intent_order_pairs": (paper_intent_order_pairs, state_intent_order_pairs),
        "fill_ids": (paper_fill_ids, ledger_fill_ids),
        "report_fill_quantity": (
            paper_fill_quantity,
            int(paper["cumulative_filled_quantity"]),
        ),
        "strategy_filled_quantity": (
            strategy_filled_quantity,
            int(paper["cumulative_filled_quantity"]),
        ),
        "cumulative_filled_quantity": (
            int(paper["cumulative_filled_quantity"]),
            global_quantities["fill_buy"] + global_quantities["fill_sell"],
        ),
    }
    differences = [
        name
        for name, (paper_value, strategy_value) in comparisons.items()
        if paper_value != strategy_value
    ]
    if not unique_order_ids:
        differences.append("duplicate_paper_order_id")
    if not unique_intent_ids:
        differences.append("duplicate_paper_intent_id")
    if not unique_fill_ids:
        differences.append("duplicate_paper_fill_id")
    if differences:
        raise RuntimeError(f"Paper reconciliation differs: {sorted(differences)}")
    paper_reconciliation = {
        "matched": True,
        "differences": [],
        "available_cash": str(paper["cash"]["available"]),
        "frozen_cash": str(paper["cash"]["frozen"]),
        "total_fees": str(paper["cash"]["total_fees"]),
        "total_position": int(paper["position"]["total"]),
        "sellable_position": int(paper["position"]["sellable"]),
        "today_bought": int(paper["position"]["today_bought"]),
        "frozen_sell": int(paper["position"]["frozen_sell"]),
        "open_order_ids": paper_open_order_ids,
        "cumulative_filled_quantity": int(paper["cumulative_filled_quantity"]),
        "report_counts": report_counts,
        "fill_count": len(paper_fill_ids),
        "report_filled_quantity": paper_fill_quantity,
        "unique_intent_ids": unique_intent_ids,
        "unique_order_ids": unique_order_ids,
        "unique_fill_ids": unique_fill_ids,
        "reports_sha256": canonical_json_sha256(normalized_reports),
        "order_ids_sha256": canonical_json_sha256(sorted(paper_order_ids)),
        "fill_ids_sha256": canonical_json_sha256(paper_fill_ids),
    }

    result = {
        "schema_version": 2,
        "event_type_histogram": legacy["event_type_histogram"],
        "quantities": global_quantities,
        "quantities_by_direction": by_direction,
        "right_partitions": right_partitions,
        "lot_safety": lot_safety,
        "tranche_safety": tranche_safety,
        "paper_reconciliation": paper_reconciliation,
        "terminal": legacy["terminal"],
    }
    result["sha256"] = canonical_json_sha256(result)
    return result


def evaluate(metrics: dict[str, Any], mode: str, enforce: bool) -> list[dict[str, Any]]:
    results = []
    for name, expected in CORRECTNESS_EXPECTATIONS[mode].items():
        actual = metrics[name]
        results.append(
            {
                "metric": name,
                "actual": actual,
                "expected": expected,
                "passed": actual == expected,
                "blocking": True,
                "kind": "correctness",
            }
        )
    for name, limit in THRESHOLDS[mode].items():
        raw_actual = metrics.get(name)
        unsupported = raw_actual is None
        actual = None if unsupported else float(raw_actual)
        required_instrumentation = (
            unsupported
            and mode == "nightly"
            and name == "web_peak_rss_mib"
            and platform.system() == "Linux"
        )
        passed = not required_instrumentation if unsupported else actual <= limit
        results.append(
            {
                "metric": name,
                "actual": actual,
                "maximum": limit,
                "passed": passed,
                "blocking": required_instrumentation
                if unsupported
                else mode == "short" or enforce,
                "unsupported": unsupported,
                "kind": "instrumentation"
                if required_instrumentation
                else "performance",
            }
        )
    for name, limit in MINIMUM_THRESHOLDS[mode].items():
        actual = float(metrics[name])
        results.append(
            {
                "metric": name,
                "actual": actual,
                "minimum": limit,
                "passed": actual >= limit,
                "blocking": mode == "short" or enforce,
                "kind": "performance",
            }
        )
    return results


def write_outputs(
    output_dir: Path,
    metrics: dict[str, Any],
    checks: list[dict[str, Any]],
    database: dict[str, Any],
    mode: str,
    enforce: bool,
    identity: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 2,
        "mode": mode,
        "enforced": mode == "short" or enforce,
        "warn_only": mode == "nightly" and not enforce,
        "baseline": BASELINE,
        "metrics": metrics,
        "checks": checks,
        "input_identity": identity,
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
    }
    (output_dir / "metrics.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "database-size.json").write_text(
        json.dumps(database, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary = [
        f"# GridEdge-T performance ({mode})",
        "",
        f"Enforcement: **{'blocking' if payload['enforced'] else 'warn-only'}**",
        "",
        (
            "Input, durable run, and business-result identity: "
            f"**{'PASS' if identity['passed'] else 'FAIL'} (always blocking)**"
        ),
        "",
        "| Metric | Actual | Budget / Expected | Result |",
        "|---|---:|---:|---|",
    ]

    def display(value: Any) -> str:
        return (
            "N/A"
            if value is None
            else f"{value:.6f}"
            if isinstance(value, float)
            else str(value)
        )

    for check in checks:
        result = (
            "N/A"
            if check.get("unsupported")
            else "PASS"
            if check["passed"]
            else "FAIL"
            if check["blocking"]
            else "WARN"
        )
        if "expected" in check:
            budget = f"= {display(check['expected'])}"
        elif "minimum" in check:
            budget = f">= {display(check['minimum'])}"
        else:
            budget = f"<= {display(check['maximum'])}"
        summary.append(
            f"| `{check['metric']}` | {display(check['actual'])} | {budget} | {result} |"
        )
    (output_dir / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("short", "nightly"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--iterations", type=int)
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="make nightly threshold violations blocking (short is always blocking)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    binary = args.binary.resolve()
    source_config = args.config.resolve()
    data = args.data.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    database = output_dir / "gridedge-perf.db"
    generated_config = output_dir / "config.yaml"
    if database.exists():
        raise RuntimeError(f"performance database already exists: {database}")
    for required in (binary, source_config, data):
        if not required.is_file():
            raise FileNotFoundError(required)
    manifest_bytes = INPUT_MANIFEST.read_bytes()
    manifest_sha256 = sha256_bytes(manifest_bytes)
    expected_workload = load_input_manifest(args.mode, manifest_bytes)
    run_id = args.run_id or expected_workload["run_id"]
    iterations = args.iterations or expected_workload["iterations"]
    if iterations < 1:
        raise ValueError("iterations must be positive")
    identity, binary_bytes, config_bytes, data_bytes = input_identity(
        args.mode,
        binary,
        source_config,
        data,
        run_id,
        iterations,
        expected_workload,
    )
    identity["harness_sha256"] = sha256_bytes(Path(__file__).read_bytes())
    identity["manifest_sha256"] = manifest_sha256
    identity["threshold_contract_sha256"] = canonical_json_sha256(
        {
            "baseline": BASELINE,
            "thresholds": THRESHOLDS,
            "minimum_thresholds": MINIMUM_THRESHOLDS,
            "correctness": CORRECTNESS_EXPECTATIONS,
            "response_profile": {
                "snapshot": "sequential",
                "bars_max_points": 1000,
                "events_after": 0,
                "events_limit": 1000,
                "concurrency": 4,
                "percentile": "nearest-rank-p95",
            },
        }
    )
    write_identity_outputs(output_dir, identity)
    if not identity["passed"]:
        print((output_dir / "summary.md").read_text(encoding="utf-8"), end="")
        return 1

    identity["environment"] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "logical_cpus": os.cpu_count(),
        "github_sha": os.environ.get("GITHUB_SHA"),
    }
    identity["environment_identity_sha256"] = canonical_json_sha256(
        identity["environment"]
    )

    binary, source_config, data, frozen_manifest = freeze_inputs(
        output_dir,
        binary,
        source_config,
        data,
        binary_bytes,
        config_bytes,
        data_bytes,
        manifest_bytes,
    )
    replace_database(source_config, generated_config, database)
    initialize_frozen_identity(
        identity,
        binary,
        source_config,
        data,
        generated_config,
        frozen_manifest,
    )
    verify_frozen_stage(identity, output_dir, "inputs_frozen")

    # Warm the executable and dynamic loader without touching the replay database.
    checked_frozen_process(
        [str(binary), "validate-config", "--config", str(generated_config)],
        identity=identity,
        output_dir=output_dir,
        stage="validate_config",
    )

    replay = checked_frozen_process(
        [
            str(binary),
            "replay",
            "--config",
            str(generated_config),
            "--data",
            str(data),
            "--run-id",
            run_id,
            "--json",
        ],
        identity=identity,
        output_dir=output_dir,
        stage="replay",
        timeout=REPLAY_HARD_TIMEOUT_SECONDS,
    )
    replay_payload = json.loads(replay.stdout)
    metrics: dict[str, Any] = {
        "dataset_bars": csv_bar_count(data),
        "event_count": int(replay_payload["event_count"]),
        "replay_mode": replay_payload["mode"],
        "duplicate_events": int(replay_payload["duplicate_events"]),
        "replay_wall_seconds": replay.wall_seconds,
        "replay_peak_rss_mib": replay.peak_rss_mib,
    }
    metrics["replay_bars_per_second"] = metrics["dataset_bars"] / replay.wall_seconds
    observed_identity, run_identity_checks = verify_durable_run_identity(
        database, run_id, identity
    )
    identity["observed_run_identity"] = observed_identity
    identity["checks"].extend(run_identity_checks)
    identity["passed"] = all(check["passed"] for check in identity["checks"])
    identity["workload_identity_sha256"] = canonical_json_sha256(
        {
            "schema_version": 1,
            "mode": args.mode,
            "run_id": run_id,
            "iterations": iterations,
            "source_config_sha256": identity["inputs"]["source_config"][
                "actual_sha256"
            ],
            "canonical_config_sha256": observed_identity["canonical_config_sha256"],
            "data_sha256": identity["inputs"]["data"]["actual_sha256"],
            "binary_sha256": identity["inputs"]["binary"]["actual_sha256"],
            "algorithm_manifest_sha256": observed_identity["algorithm_manifest_sha256"],
            "harness_sha256": identity["harness_sha256"],
            "manifest_sha256": identity["manifest_sha256"],
            "threshold_contract_sha256": identity["threshold_contract_sha256"],
            "dataset": identity["expected_run_identity"],
        }
    )
    write_identity_outputs(output_dir, identity)
    database_info = database_measurements(database)
    database_info.update(
        {
            "schema_version": 2,
            "run_id": run_id,
            "input_manifest_sha256": identity["manifest_sha256"],
            "workload_identity_sha256": identity["workload_identity_sha256"],
        }
    )
    metrics["database_mib"] = database_info["mib"]
    metrics["redundant_event_index_count"] = database_info[
        "redundant_event_index_count"
    ]

    status_samples = [
        checked_frozen_process(
            [
                str(binary),
                "status",
                "--config",
                str(generated_config),
                "--run-id",
                run_id,
                "--json",
            ],
            identity=identity,
            output_dir=output_dir,
            stage=f"status_{index + 1}",
        )
        for index in range(iterations)
    ]
    metrics["cold_status_p95_seconds"] = percentile(
        [sample.wall_seconds for sample in status_samples], 0.95
    )
    status_payloads = [json.loads(sample.stdout) for sample in status_samples]
    rebuild = checked_frozen_process(
        [
            str(binary),
            "rebuild-state",
            "--config",
            str(generated_config),
            "--run-id",
            run_id,
            "--json",
        ],
        identity=identity,
        output_dir=output_dir,
        stage="rebuild",
    )
    metrics["rebuild_wall_seconds"] = rebuild.wall_seconds
    metrics["rebuild_peak_rss_mib"] = rebuild.peak_rss_mib
    rebuild_payload = json.loads(rebuild.stdout)
    metrics["rebuild_matched"] = rebuild_payload.get("matched") is True
    metrics["rebuild_full_sequence"] = int(rebuild_payload["full_sequence"])
    metrics["rebuild_snapshot_sequence"] = int(rebuild_payload["snapshot_sequence"])
    expected_business_sha256 = expected_workload["business_result_sha256"]
    if metrics["rebuild_matched"]:
        result_identity = business_result(database, run_id, status_payloads[0])
        if any(
            business_result(database, run_id, payload)["sha256"]
            != result_identity["sha256"]
            for payload in status_payloads[1:]
        ):
            raise RuntimeError("business result changed across cold status samples")
        actual_business_sha256: str | None = result_identity["sha256"]
    else:
        result_identity = {
            "schema_version": 2,
            "unavailable": "full journal rebuild differs from the final snapshot",
        }
        actual_business_sha256 = None
    business_check = {
        "metric": "business_result_sha256",
        "actual": actual_business_sha256,
        "expected": expected_business_sha256,
        "passed": actual_business_sha256 == expected_business_sha256,
        "blocking": True,
        "kind": "business_result",
    }
    identity["business_result"] = result_identity
    identity["checks"].append(business_check)
    identity["passed"] = identity["passed"] and business_check["passed"]
    write_identity_outputs(output_dir, identity)

    verify_frozen_stage(identity, output_dir, "before_web")
    with web_server(binary, generated_config, data) as web:
        metrics.update(response_measurements(web.base_url, run_id, iterations))
    verify_frozen_stage(identity, output_dir, "after_web")
    metrics["web_peak_rss_mib"] = web.peak_rss_mib

    checks = [*identity["checks"], *evaluate(metrics, args.mode, args.enforce)]
    write_outputs(
        output_dir,
        metrics,
        checks,
        database_info,
        args.mode,
        args.enforce,
        identity,
    )
    print((output_dir / "summary.md").read_text(encoding="utf-8"), end="")
    return (
        1 if any(not check["passed"] and check["blocking"] for check in checks) else 0
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"performance harness failed: {error}", file=sys.stderr)
        raise
