"""Contract tests for the dependency-free performance gate."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "perf_gate", ROOT / "scripts/perf_gate.py"
)
assert SPEC is not None and SPEC.loader is not None
PERF_GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PERF_GATE
SPEC.loader.exec_module(PERF_GATE)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def create_business_result_v2_fixture(database: Path) -> dict[str, object]:
    zero_capacity = {
        "t_plus_one_blocked_quantity": 0,
        "risk_blocked_quantity": 0,
        "no_profit_blocked_quantity": 0,
    }
    events: list[tuple[str, dict[str, object]]] = [
        (
            "GRID_RIGHT_GRANTED",
            {
                "right_id": "buy-right",
                "direction": "BUY",
                "capacity": {
                    "t_plus_one_blocked_quantity": 600,
                    "risk_blocked_quantity": 300,
                    "no_profit_blocked_quantity": 200,
                },
            },
        ),
        (
            "GRID_RIGHT_GRANTED",
            {
                "right_id": "sell-right",
                "direction": "SELL",
                "capacity": zero_capacity,
            },
        ),
        (
            "GRID_RIGHT_GRANTED",
            {
                "right_id": "deferred-right",
                "direction": "BUY",
                "capacity": zero_capacity,
            },
        ),
    ]
    for right_id, direction, decision, exercise, defer in (
        ("buy-right", "BUY", "EXERCISE", 1500, 0),
        ("sell-right", "SELL", "EXERCISE", 1500, 0),
        ("deferred-right", "BUY", "DEFER", 0, 1500),
    ):
        outcome: dict[str, object] = {
            "decision": decision,
            "defer_quantity": defer,
        }
        if decision == "EXERCISE":
            outcome["exercise_quantity"] = exercise
        events.append(
            (
                "GATE_DECISION_MADE",
                {
                    "request": {
                        "context": {"right_id": right_id, "direction": direction},
                        "right": {"right_id": right_id, "direction": direction},
                    },
                    "response": {"right_id": right_id, "outcome": outcome},
                },
            )
        )
    events.append(
        (
            "GRID_RIGHT_BLOCKED",
            {
                "right_id": "sell-right",
                "partial": True,
                "t_plus_one_blocked_quantity": 100,
                "risk_blocked_quantity": 200,
                "no_profit_blocked_quantity": 300,
            },
        )
    )
    for right_id, event_type, intent, remaining in (
        ("buy-right", "GRID_RIGHT_RESERVED", 1500, 0),
        ("sell-right", "GRID_RIGHT_RESERVED", 1500, 0),
        ("deferred-right", "GRID_RIGHT_DEFERRED", 0, 1500),
    ):
        events.append(
            (
                event_type,
                {
                    "right_id": right_id,
                    "gross_available_quantity": 1700,
                    "platform_residual_quantity": 200,
                    "algorithm_authorized_quantity": 1500,
                    "platform_blocked_quantity": 0,
                    "order_intent_quantity": intent,
                    "remaining_decision_quantity": remaining,
                },
            )
        )
    events.extend(
        [
            (
                "ORDER_INTENT_CREATED",
                {
                    "intent": {
                        "intent_id": "buy-intent",
                        "right_id": "buy-right",
                        "direction": "BUY",
                        "quantity": 1500,
                    }
                },
            ),
            (
                "ORDER_INTENT_CREATED",
                {
                    "intent": {
                        "intent_id": "sell-intent",
                        "right_id": "sell-right",
                        "direction": "SELL",
                        "quantity": 1500,
                    }
                },
            ),
            (
                "ORDER_PARTIALLY_FILLED",
                {
                    "fill": {
                        "fill_id": "buy-partial",
                        "direction": "BUY",
                        "quantity": 500,
                    }
                },
            ),
            (
                "ORDER_FILLED",
                {
                    "fill": {
                        "fill_id": "buy-final",
                        "direction": "BUY",
                        "quantity": 1000,
                    }
                },
            ),
            (
                "ORDER_PARTIALLY_FILLED",
                {
                    "fill": {
                        "fill_id": "sell-partial",
                        "direction": "SELL",
                        "quantity": 700,
                        "price": "2",
                        "commission": "5",
                        "tax": "2",
                        "target_lot_allocations": [
                            {
                                "lot_id": "lot-sell-a",
                                "quantity": 700,
                                "cost_basis": "560",
                                "worst_fill_price": "2",
                                "maximum_sell_fees": "7",
                                "worst_case_profit": "833",
                            }
                        ],
                    }
                },
            ),
            (
                "ORDER_FILLED",
                {
                    "fill": {
                        "fill_id": "sell-final",
                        "direction": "SELL",
                        "quantity": 800,
                        "price": "2",
                        "commission": "5",
                        "tax": "3",
                        "target_lot_allocations": [
                            {
                                "lot_id": "lot-sell-b",
                                "quantity": 800,
                                "cost_basis": "640",
                                "worst_fill_price": "2",
                                "maximum_sell_fees": "8",
                                "worst_case_profit": "952",
                            }
                        ],
                    }
                },
            ),
        ]
    )
    tranche = {
        "unit": "QUANTITY",
        "minted_budget": "0",
        "available_budget": "0",
        "reserved_budget": "0",
        "consumed_budget": "0",
        "revoked_budget": "0",
        "expired_budget": "0",
        "minted_quantity": 1500,
        "available_quantity": 0,
        "reserved_quantity": 0,
        "consumed_quantity": 1500,
        "revoked_quantity": 0,
        "expired_quantity": 0,
    }
    state = {
        "run_id": "fixture",
        "event_count": len(events),
        "cash": {"available": "1000", "frozen": "0", "total_fees": "15"},
        "position": {
            "total": 1500,
            "sellable": 1500,
            "today_bought": 0,
            "frozen_sell": 0,
        },
        "lots": {
            "lot-sell-a": {
                "lot_id": "lot-sell-a",
                "remaining_quantity": 0,
                "remaining_cost": "0",
                "allocated_cost": "560",
                "realized_pnl": "833",
            },
            "lot-sell-b": {
                "lot_id": "lot-sell-b",
                "remaining_quantity": 0,
                "remaining_cost": "0",
                "allocated_cost": "640",
                "realized_pnl": "952",
            },
        },
        "right_tranches": {
            "buy-tranche": {"direction": "BUY", **tranche},
            "sell-tranche": {"direction": "SELL", **tranche},
        },
        "orders": {
            "buy-order": {
                "status": "FILLED",
                "intent": {"intent_id": "buy-intent"},
                "filled_quantity": 1500,
            },
            "sell-order": {
                "status": "FILLED",
                "intent": {"intent_id": "sell-intent"},
                "filled_quantity": 1500,
            },
            "rejected-order": {
                "status": "REJECTED",
                "intent": {"intent_id": "rejected-intent"},
                "filled_quantity": 0,
            },
            "cancelled-order": {
                "status": "CANCELLED",
                "intent": {"intent_id": "cancelled-intent"},
                "filled_quantity": 0,
            },
        },
    }
    paper = {
        "cash": state["cash"],
        "position": state["position"],
        "open_order_ids": [],
        "cumulative_filled_quantity": 3000,
    }
    reports = (
        (
            "buy-intent",
            "buy-order",
            {
                "Accepted": {
                    "order_id": "buy-order",
                    "fills": [
                        {"fill_id": "buy-partial", "quantity": 500},
                        {"fill_id": "buy-final", "quantity": 1000},
                    ],
                }
            },
        ),
        (
            "sell-intent",
            "sell-order",
            {
                "Accepted": {
                    "order_id": "sell-order",
                    "fills": [
                        {"fill_id": "sell-partial", "quantity": 700},
                        {"fill_id": "sell-final", "quantity": 800},
                    ],
                }
            },
        ),
        (
            "rejected-intent",
            "rejected-order",
            {"Rejected": {"order_id": "rejected-order", "fills": []}},
        ),
        (
            "cancelled-intent",
            "cancelled-order",
            {"Cancelled": {"order_id": "cancelled-order", "fills": []}},
        ),
    )
    raw_state = json.dumps(state, sort_keys=True, separators=(",", ":"))
    snapshot_checksum = hashlib.sha256(raw_state.encode("utf-8")).hexdigest()
    with contextlib.closing(sqlite3.connect(database)) as connection:
        connection.executescript(
            "CREATE TABLE events (run_id TEXT,event_type TEXT,sequence_number INTEGER,payload TEXT);"
            "CREATE TABLE snapshots (run_id TEXT,sequence_number INTEGER,state_json TEXT,checksum TEXT);"
            "CREATE TABLE paper_accounts (run_id TEXT,snapshot_json TEXT);"
            "CREATE TABLE paper_orders (run_id TEXT,intent_id TEXT,order_id TEXT,report_json TEXT);"
        )
        connection.executemany(
            "INSERT INTO events(run_id,event_type,sequence_number,payload) "
            "VALUES ('fixture',?,?,?)",
            [
                (event_type, sequence, json.dumps(payload))
                for sequence, (event_type, payload) in enumerate(events, 1)
            ],
        )
        connection.execute(
            "INSERT INTO snapshots VALUES ('fixture',?,?,?)",
            (len(events), raw_state, snapshot_checksum),
        )
        connection.execute(
            "INSERT INTO paper_accounts VALUES ('fixture',?)", (json.dumps(paper),)
        )
        connection.executemany(
            "INSERT INTO paper_orders VALUES ('fixture',?,?,?)",
            [
                (intent_id, order_id, json.dumps(report))
                for intent_id, order_id, report in reports
            ],
        )
        connection.commit()
    status = {field: None for field in PERF_GATE.BUSINESS_RESULT_FIELDS}
    status.update(
        {
            "mode": "STOPPED",
            "event_count": len(events),
            "duplicate_events": 0,
            "standard_quantity": 1500,
            "open_lots": 0,
            "closed_lots": 2,
            "realized_grid_pnl": "1785",
            "available_cash": "1000",
            "total_fees": "15",
            "total_position": 1500,
            "sellable_position": 1500,
            "open_orders": 0,
        }
    )
    return status


class PerformanceGateContract(unittest.TestCase):
    def test_canonical_decimal_text_is_global_stable_and_hash_distinguishes_ten(
        self,
    ) -> None:
        cases = (
            ("0", "0"),
            ("-0.000", "0"),
            ("1", "1"),
            ("1.0000", "1"),
            ("10", "10"),
            ("10.0000", "10"),
            ("0.0100", "0.01"),
            ("-12.3400", "-12.34"),
        )
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(
                    PERF_GATE.canonical_decimal_text(Decimal(raw)), expected
                )
        for nonfinite in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(nonfinite=nonfinite):
                with self.assertRaisesRegex(RuntimeError, "non-finite Decimal"):
                    PERF_GATE.canonical_decimal_text(Decimal(nonfinite))
        one = {"value": PERF_GATE.canonical_decimal_text(Decimal("1"))}
        ten = {"value": PERF_GATE.canonical_decimal_text(Decimal("10"))}
        self.assertNotEqual(one, ten)
        self.assertNotEqual(
            PERF_GATE.canonical_json_sha256(one),
            PERF_GATE.canonical_json_sha256(ten),
        )

    def test_measured_process_timeout_kills_the_entire_child_process_group(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            identities = Path(directory) / "process-identities"
            program = """
import os
import subprocess
import sys
import time

child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    handle.write(f"{os.getpid()} {os.getpgrp()} {child.pid}")
time.sleep(60)
"""
            started = time.monotonic()
            with self.assertRaises(TimeoutError) as raised:
                PERF_GATE.run_measured(
                    [sys.executable, "-c", program, str(identities)],
                    timeout=0.5,
                )
            self.assertLess(time.monotonic() - started, 3.0)
            self.assertIsInstance(
                raised.exception.__cause__, PERF_GATE.subprocess.TimeoutExpired
            )
            self.assertIn("exceeded the 0.5s deadline", str(raised.exception))
            parent_pid, process_group, child_pid = map(
                int, identities.read_text(encoding="utf-8").split()
            )
            self.assertEqual(parent_pid, process_group)
            try:
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    try:
                        os.killpg(process_group, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.02)
                else:
                    self.fail(
                        "timed-out parent/child process group remained alive: "
                        f"parent={parent_pid} child={child_pid} pgid={process_group}"
                    )
            finally:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process_group, PERF_GATE.signal.SIGKILL)

    def test_api_timeout_preserves_timeout_type_and_identifies_endpoint(self) -> None:
        endpoint = "http://127.0.0.1:18765/api/v1/runs/perf-three-year"
        with (
            mock.patch.object(
                PERF_GATE.urllib.request,
                "urlopen",
                side_effect=TimeoutError("timed out"),
            ),
            self.assertRaises(TimeoutError) as raised,
        ):
            PERF_GATE.api_get(endpoint, timeout=0.01)
        diagnostic = "\n".join(
            [str(raised.exception), *getattr(raised.exception, "__notes__", [])]
        )
        self.assertIn(endpoint, diagnostic)

    def test_web_cleanup_failure_does_not_replace_original_snapshot_timeout(
        self,
    ) -> None:
        endpoint = "http://127.0.0.1:18765/api/v1/runs/perf-three-year"
        process = mock.Mock()
        process.pid = 12345
        process.poll.return_value = None
        process.returncode = -9
        process.wait.side_effect = [
            PERF_GATE.subprocess.TimeoutExpired("gridedge web", 10),
            -9,
        ]
        process.stderr.read.return_value = "forced kill after snapshot timeout"
        with (
            mock.patch.object(PERF_GATE, "reserve_port", return_value=18765),
            mock.patch.object(PERF_GATE.subprocess, "Popen", return_value=process),
            mock.patch.object(PERF_GATE, "wait_for_health"),
            mock.patch.object(PERF_GATE, "linux_rss_mib", return_value=None),
            self.assertRaises(TimeoutError) as raised,
        ):
            with PERF_GATE.web_server(Path("gridedge"), Path("config"), Path("data")):
                raise TimeoutError(f"GET {endpoint} timed out")
        diagnostic = "\n".join(
            [str(raised.exception), *getattr(raised.exception, "__notes__", [])]
        )
        self.assertIn(endpoint, diagnostic)
        process.send_signal.assert_called_once_with(PERF_GATE.signal.SIGINT)
        process.kill.assert_called_once()

    def test_nearest_rank_percentile_is_deterministic(self) -> None:
        self.assertEqual(PERF_GATE.percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)
        self.assertEqual(PERF_GATE.percentile([4.0, 1.0, 3.0, 2.0], 0.50), 2.0)

    def test_short_thresholds_are_always_blocking(self) -> None:
        metrics = dict(PERF_GATE.CORRECTNESS_EXPECTATIONS["short"])
        metrics.update(
            {
                "replay_wall_seconds": 1.01,
                "replay_peak_rss_mib": 63.0,
                "database_mib": 1.9,
            }
        )
        checks = PERF_GATE.evaluate(metrics, "short", enforce=False)
        failed = [check for check in checks if not check["passed"]]
        self.assertEqual([check["metric"] for check in failed], ["replay_wall_seconds"])
        self.assertTrue(failed[0]["blocking"])

    def test_nightly_is_warn_only_until_explicitly_enforced(self) -> None:
        metrics = dict(PERF_GATE.CORRECTNESS_EXPECTATIONS["nightly"])
        metrics.update(
            {key: limit + 1.0 for key, limit in PERF_GATE.THRESHOLDS["nightly"].items()}
        )
        metrics.update(
            {
                key: limit - 1.0
                for key, limit in PERF_GATE.MINIMUM_THRESHOLDS["nightly"].items()
            }
        )
        warning_checks = PERF_GATE.evaluate(metrics, "nightly", enforce=False)
        enforced_checks = PERF_GATE.evaluate(metrics, "nightly", enforce=True)
        warning_performance = [c for c in warning_checks if c["kind"] == "performance"]
        enforced_performance = [
            c for c in enforced_checks if c["kind"] == "performance"
        ]
        self.assertTrue(all(not check["passed"] for check in warning_performance))
        self.assertTrue(all(not check["blocking"] for check in warning_performance))
        self.assertTrue(all(check["blocking"] for check in enforced_performance))

    def test_unsupported_web_rss_is_explicit_na_on_non_linux(self) -> None:
        metrics = dict(PERF_GATE.CORRECTNESS_EXPECTATIONS["nightly"])
        metrics.update(PERF_GATE.THRESHOLDS["nightly"])
        metrics.update(PERF_GATE.MINIMUM_THRESHOLDS["nightly"])
        metrics["web_peak_rss_mib"] = None
        check = next(
            item
            for item in PERF_GATE.evaluate(metrics, "nightly", enforce=True)
            if item["metric"] == "web_peak_rss_mib"
        )
        self.assertTrue(check["unsupported"])
        self.assertTrue(check["passed"])
        self.assertFalse(check["blocking"])

    def test_linux_nightly_missing_web_rss_is_a_blocking_instrumentation_failure(
        self,
    ) -> None:
        metrics = dict(PERF_GATE.CORRECTNESS_EXPECTATIONS["nightly"])
        metrics.update(PERF_GATE.THRESHOLDS["nightly"])
        metrics.update(PERF_GATE.MINIMUM_THRESHOLDS["nightly"])
        metrics["web_peak_rss_mib"] = None
        with mock.patch.object(PERF_GATE.platform, "system", return_value="Linux"):
            check = next(
                item
                for item in PERF_GATE.evaluate(metrics, "nightly", enforce=False)
                if item["metric"] == "web_peak_rss_mib"
            )
        self.assertTrue(check["unsupported"])
        self.assertFalse(check["passed"])
        self.assertTrue(check["blocking"])
        self.assertEqual(check["kind"], "instrumentation")

    def test_correctness_mismatch_is_blocking_even_when_nightly_warn_only(self) -> None:
        metrics = dict(PERF_GATE.CORRECTNESS_EXPECTATIONS["nightly"])
        metrics.update(PERF_GATE.THRESHOLDS["nightly"])
        metrics.update(PERF_GATE.MINIMUM_THRESHOLDS["nightly"])
        metrics["event_count"] -= 1
        metrics["rebuild_matched"] = False
        checks = PERF_GATE.evaluate(metrics, "nightly", enforce=False)
        failures = [check for check in checks if not check["passed"]]
        self.assertEqual(
            {check["metric"] for check in failures}, {"event_count", "rebuild_matched"}
        )
        self.assertTrue(all(check["blocking"] for check in failures))

    def test_csv_count_ignores_trailing_and_interior_blank_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "bars.csv"
            fixture.write_text("timestamp,close\n1,10\n\n2,11\n   \n", encoding="utf-8")
            self.assertEqual(PERF_GATE.csv_bar_count(fixture), 2)

    def test_published_baseline_and_threshold_schema_is_complete(self) -> None:
        self.assertEqual(
            PERF_GATE.BASELINE,
            {
                "dataset_bars": 34_944,
                "event_count": 110_532,
                "replay_wall_seconds": 30.345818,
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
                "replay_bars_per_second": 1151.526047,
            },
        )
        self.assertEqual(
            PERF_GATE.CORRECTNESS_EXPECTATIONS["short"]["dataset_bars"], 21
        )
        self.assertEqual(
            PERF_GATE.CORRECTNESS_EXPECTATIONS["short"]["event_count"], 233
        )
        expected = {
            "replay_wall_seconds",
            "replay_peak_rss_mib",
            "database_mib",
            "cold_status_p95_seconds",
            "rebuild_wall_seconds",
            "rebuild_peak_rss_mib",
            "snapshot_sequential_p95_seconds",
            "bars_1000_p95_seconds",
            "events_1000_p95_seconds",
            "snapshot_concurrency_4_p95_seconds",
            "snapshot_sequential_response_bytes",
            "bars_1000_response_bytes",
            "events_1000_response_bytes",
            "web_peak_rss_mib",
        }
        self.assertEqual(set(PERF_GATE.THRESHOLDS["nightly"]), expected)
        self.assertEqual(
            PERF_GATE.MINIMUM_THRESHOLDS["nightly"],
            {"replay_bars_per_second": 190.0},
        )
        self.assertEqual(PERF_GATE.THRESHOLDS["nightly"]["replay_wall_seconds"], 180.0)
        self.assertEqual(
            PERF_GATE.THRESHOLDS["nightly"]["snapshot_concurrency_4_p95_seconds"],
            1.0,
        )
        self.assertEqual(PERF_GATE.REPLAY_HARD_TIMEOUT_SECONDS, 240.0)
        self.assertEqual(
            PERF_GATE.THRESHOLDS["nightly"]["snapshot_sequential_response_bytes"],
            256 * 1024,
        )
        self.assertEqual(
            PERF_GATE.THRESHOLDS["nightly"]["bars_1000_response_bytes"], 256 * 1024
        )
        self.assertEqual(
            PERF_GATE.THRESHOLDS["nightly"]["events_1000_response_bytes"], 1024 * 1024
        )

    def test_versioned_manifest_freezes_short_and_nightly_input_bytes(self) -> None:
        manifest_path = ROOT / "configs/performance-inputs-v1.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(set(manifest["modes"]), {"short", "nightly"})
        expected = {
            "short": {
                "run_id": "perf-short",
                "iterations": 3,
                "source_config_sha256": (
                    "268340b73416db5984aa656430b15cf054884e2ae92338c698999a6ff0d94c26"
                ),
                "canonical_config_sha256": (
                    "6f5d8e72ba49540aea45a2527adf49fdc42a61e2aae116fa96b2d2b30a825d71"
                ),
                "data_sha256": (
                    "0daa2cf5b5b170125318e526c53b8af246b5f67bda4a301b7910c833e65af97b"
                ),
                "dataset_bars": 21,
                "symbol": "600000.SH",
                "first_timestamp": "2026-01-05T09:30:00",
                "last_timestamp": "2026-01-06T09:40:00",
                "algorithm": {
                    "algorithm_name": "gate-policy-adapter",
                    "algorithm_version": "1",
                    "artifact_sha256": (
                        "62a3c64dd2dd2d37dc25f289e6975f6851a57044555777e113ab2a45e36f2576"
                    ),
                    "environment_sha256": (
                        "e7ed148fef8281e4c0e83d26878e5df2bad8ab0c5bed2b941ee681046a2e4a60"
                    ),
                    "canonical_arguments": [],
                    "deterministic": True,
                    "supported_contract_versions": [3],
                    "supports_checkpoint": False,
                },
                "business_result_sha256": (
                    "d7e3644ba66965104998770b9ebf795f4cec5838f2f101adf9076423c7da9e1d"
                ),
            },
            "nightly": {
                "run_id": "perf-three-year",
                "iterations": 7,
                "source_config_sha256": (
                    "5eef06e3de3e475105069ad947329a88c42949bd897bb291532f2c3eb2c27a3f"
                ),
                "canonical_config_sha256": (
                    "64d38a587043d47fcd05e4cfa8f69a166f2b956c65df8eec77c4d7150e3b6a1f"
                ),
                "data_sha256": (
                    "f68e60309c7af91a3805f7144f6b7fec07fab4274bd5eb6102c051151ce19e7b"
                ),
                "dataset_bars": 34_944,
                "symbol": "002256.SZ",
                "first_timestamp": "2023-08-14T09:35:00",
                "last_timestamp": "2026-08-14T15:00:00",
                "algorithm": {
                    "algorithm_name": "gate-policy-adapter",
                    "algorithm_version": "1",
                    "artifact_sha256": (
                        "62a3c64dd2dd2d37dc25f289e6975f6851a57044555777e113ab2a45e36f2576"
                    ),
                    "environment_sha256": (
                        "e7ed148fef8281e4c0e83d26878e5df2bad8ab0c5bed2b941ee681046a2e4a60"
                    ),
                    "canonical_arguments": [],
                    "deterministic": True,
                    "supported_contract_versions": [3],
                    "supports_checkpoint": False,
                },
                "business_result_sha256": (
                    "1b624dd87114ffe3270c66e91ac31a62d6e94ff00f692bd1a34616001a3c082b"
                ),
                "provenance": {
                    "raw": {
                        "path": (
                            "data/raw/baostock_002256.SZ_5m_raw_20230814_20260814.csv"
                        ),
                        "sha256": (
                            "83b53b76c2888b481a98d55125e115c2f61d8f7055014d8f633dc02c6a51ba0a"
                        ),
                    },
                    "quality": {
                        "path": (
                            "data/reports/002256.SZ_5m_raw_20230814_20260814.quality.json"
                        ),
                        "sha256": (
                            "feee377541ebe35bdacc22ff17530785bcb550d537f893fb76a666294903fe50"
                        ),
                    },
                },
            },
        }
        for mode, frozen in expected.items():
            configured = manifest["modes"][mode]
            for field, value in frozen.items():
                self.assertEqual(configured[field], value, f"{mode}.{field}")
        self.assertEqual(
            file_sha256(ROOT / "configs/default.yaml"),
            manifest["modes"]["short"]["source_config_sha256"],
        )
        self.assertEqual(
            file_sha256(ROOT / "tests/fixtures/sample.csv"),
            manifest["modes"]["short"]["data_sha256"],
        )
        self.assertEqual(
            file_sha256(ROOT / "configs/zhaoxin_5m_quantity_v10.yaml"),
            manifest["modes"]["nightly"]["source_config_sha256"],
        )
        self.assertEqual(
            file_sha256(ROOT / "data/processed/002256.SZ_5m_raw_20230814_20260814.csv"),
            manifest["modes"]["nightly"]["data_sha256"],
        )
        for provenance in manifest["modes"]["nightly"]["provenance"].values():
            self.assertEqual(
                file_sha256(ROOT / provenance["path"]), provenance["sha256"]
            )

    def assert_identity_failure_before_execution(
        self,
        *,
        mode: str,
        source_config: Path,
        data: Path,
        expected_failed_input: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            binary = temporary / "gridedge"
            binary.write_bytes(b"identity-preflight-must-not-execute")
            output_dir = temporary / "output"
            arguments = [
                "perf_gate.py",
                "--mode",
                mode,
                "--binary",
                str(binary),
                "--config",
                str(source_config),
                "--data",
                str(data),
                "--output-dir",
                str(output_dir),
                "--run-id",
                "perf-short" if mode == "short" else "perf-three-year",
            ]
            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(
                    PERF_GATE.subprocess,
                    "Popen",
                    side_effect=AssertionError("identity failure started a child"),
                ) as popen,
            ):
                self.assertEqual(PERF_GATE.main(), 1)
            popen.assert_not_called()
            self.assertFalse((output_dir / "config.yaml").exists())
            self.assertFalse((output_dir / "gridedge-perf.db").exists())

            identity = json.loads(
                (output_dir / "input-identity.json").read_text(encoding="utf-8")
            )
            self.assertEqual(identity["schema_version"], 1)
            self.assertEqual(identity["mode"], mode)
            self.assertFalse(identity["passed"])
            failed = identity["inputs"][expected_failed_input]
            self.assertFalse(failed["passed"])
            failed_check = next(
                check
                for check in identity["checks"]
                if check["metric"] == f"{expected_failed_input}_sha256"
            )
            self.assertTrue(failed_check["blocking"])
            self.assertEqual(failed_check["kind"], "input_identity")
            expected_actual_path = (
                data if expected_failed_input == "data" else source_config
            )
            self.assertEqual(failed["actual_sha256"], file_sha256(expected_actual_path))
            self.assertNotEqual(failed["actual_sha256"], failed["expected_sha256"])
            summary = (output_dir / "summary.md").read_text(encoding="utf-8")
            self.assertIn(failed["expected_sha256"], summary)
            self.assertIn(failed["actual_sha256"], summary)
            self.assertIn("FAIL", summary)
            self.assertNotIn("WARN", summary)

    def test_same_bar_count_market_or_behavior_config_mutation_fails_preflight(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            changed_data = temporary / "same-count.csv"
            original_data = ROOT / "tests/fixtures/sample.csv"
            data_text = original_data.read_text(encoding="utf-8")
            changed_data.write_text(
                data_text.replace(",100000,1000000", ",100001,1000001", 1),
                encoding="utf-8",
            )
            self.assertEqual(
                PERF_GATE.csv_bar_count(changed_data),
                PERF_GATE.csv_bar_count(original_data),
            )
            self.assert_identity_failure_before_execution(
                mode="short",
                source_config=ROOT / "configs/default.yaml",
                data=changed_data,
                expected_failed_input="data",
            )

            changed_config = temporary / "behavior-change.yaml"
            original_config = ROOT / "configs/default.yaml"
            config_text = original_config.read_text(encoding="utf-8")
            self.assertEqual(config_text.count('grid_ratio: "0.02"'), 1)
            changed_config.write_text(
                config_text.replace('grid_ratio: "0.02"', 'grid_ratio: "0.021"', 1),
                encoding="utf-8",
            )
            self.assert_identity_failure_before_execution(
                mode="short",
                source_config=changed_config,
                data=original_data,
                expected_failed_input="source_config",
            )

    def test_nightly_warn_only_never_downgrades_input_identity_failure(self) -> None:
        self.assert_identity_failure_before_execution(
            mode="nightly",
            source_config=ROOT / "configs/default.yaml",
            data=ROOT / "tests/fixtures/sample.csv",
            expected_failed_input="source_config",
        )

    def test_nightly_provenance_mismatch_is_blocking_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            raw = temporary / "raw.csv"
            raw.write_bytes(b"tampered-raw-source")
            quality = temporary / "quality.json"
            quality.write_bytes(b"exact-quality-source")
            manifest = {
                "schema_version": 1,
                "modes": {
                    mode: {
                        "run_id": "perf-short"
                        if mode == "short"
                        else "perf-three-year",
                        "iterations": 3 if mode == "short" else 7,
                        "source_config_sha256": file_sha256(
                            ROOT
                            / (
                                "configs/default.yaml"
                                if mode == "short"
                                else "configs/zhaoxin_5m_quantity_v10.yaml"
                            )
                        ),
                        "canonical_config_sha256": "a" * 64,
                        "data_sha256": file_sha256(
                            ROOT
                            / (
                                "tests/fixtures/sample.csv"
                                if mode == "short"
                                else "data/processed/002256.SZ_5m_raw_20230814_20260814.csv"
                            )
                        ),
                        "dataset_bars": 21 if mode == "short" else 34_944,
                        "symbol": "600000.SH" if mode == "short" else "002256.SZ",
                        "first_timestamp": "2026-01-05T09:30:00",
                        "last_timestamp": "2026-01-06T09:40:00",
                        "algorithm": {
                            "algorithm_name": "gate-policy-adapter",
                            "algorithm_version": "1",
                            "artifact_sha256": "b" * 64,
                            "environment_sha256": "c" * 64,
                            "canonical_arguments": [],
                            "deterministic": True,
                            "supported_contract_versions": [3],
                            "supports_checkpoint": False,
                        },
                        "business_result_sha256": "0" * 64,
                    }
                    for mode in ("short", "nightly")
                },
            }
            manifest["modes"]["nightly"]["provenance"] = {
                "raw": {
                    "path": raw.name,
                    "sha256": hashlib.sha256(b"expected-raw-source").hexdigest(),
                },
                "quality": {
                    "path": quality.name,
                    "sha256": file_sha256(quality),
                },
            }
            manifest_path = temporary / "performance-inputs-v1.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            binary = temporary / "gridedge"
            binary.write_bytes(b"provenance-preflight-must-not-execute")
            output_dir = temporary / "output"
            arguments = [
                "perf_gate.py",
                "--mode",
                "nightly",
                "--binary",
                str(binary),
                "--config",
                str(ROOT / "configs/zhaoxin_5m_quantity_v10.yaml"),
                "--data",
                str(ROOT / "data/processed/002256.SZ_5m_raw_20230814_20260814.csv"),
                "--output-dir",
                str(output_dir),
                "--run-id",
                "perf-three-year",
                "--iterations",
                "7",
            ]
            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(PERF_GATE, "ROOT", temporary),
                mock.patch.object(PERF_GATE, "INPUT_MANIFEST", manifest_path),
                mock.patch.object(
                    PERF_GATE.subprocess,
                    "Popen",
                    side_effect=AssertionError("provenance mismatch started a child"),
                ) as popen,
            ):
                self.assertEqual(PERF_GATE.main(), 1)
            popen.assert_not_called()
            self.assertFalse((output_dir / "config.yaml").exists())
            self.assertFalse((output_dir / "gridedge-perf.db").exists())
            identity = json.loads(
                (output_dir / "input-identity.json").read_text(encoding="utf-8")
            )
            self.assertFalse(identity["provenance"]["raw"]["passed"])
            self.assertTrue(identity["provenance"]["quality"]["passed"])
            check = next(
                item
                for item in identity["checks"]
                if item["metric"] == "provenance_raw_sha256"
            )
            self.assertFalse(check["passed"])
            self.assertTrue(check["blocking"])
            self.assertEqual(check["kind"], "input_identity")
            summary = (output_dir / "summary.md").read_text(encoding="utf-8")
            self.assertIn("provenance_raw_sha256", summary)
            self.assertIn(check["actual"], summary)
            self.assertIn(check["expected"], summary)

    def test_run_id_and_iterations_drift_fail_before_any_child_or_database(
        self,
    ) -> None:
        cases = [
            ("run_id", "not-perf-short", 3),
            ("iterations", "perf-short", 4),
        ]
        for failed_metric, run_id, iterations in cases:
            with (
                self.subTest(metric=failed_metric),
                tempfile.TemporaryDirectory() as directory,
            ):
                temporary = Path(directory)
                binary = temporary / "gridedge"
                binary.write_bytes(b"workload-identity-must-not-execute")
                output_dir = temporary / "output"
                arguments = [
                    "perf_gate.py",
                    "--mode",
                    "short",
                    "--binary",
                    str(binary),
                    "--config",
                    str(ROOT / "configs/default.yaml"),
                    "--data",
                    str(ROOT / "tests/fixtures/sample.csv"),
                    "--output-dir",
                    str(output_dir),
                    "--run-id",
                    run_id,
                    "--iterations",
                    str(iterations),
                ]
                with (
                    mock.patch.object(sys, "argv", arguments),
                    mock.patch.object(
                        PERF_GATE.subprocess,
                        "Popen",
                        side_effect=AssertionError("workload drift started a child"),
                    ) as popen,
                ):
                    self.assertEqual(PERF_GATE.main(), 1)
                popen.assert_not_called()
                self.assertFalse((output_dir / "config.yaml").exists())
                self.assertFalse((output_dir / "gridedge-perf.db").exists())
                identity = json.loads(
                    (output_dir / "input-identity.json").read_text(encoding="utf-8")
                )
                check = next(
                    item
                    for item in identity["checks"]
                    if item["metric"] == failed_metric
                )
                self.assertFalse(check["passed"])
                self.assertTrue(check["blocking"])
                self.assertEqual(check["kind"], "workload_identity")
                summary = (output_dir / "summary.md").read_text(encoding="utf-8")
                self.assertIn(failed_metric, summary)
                self.assertIn(str(check["actual"]), summary)
                self.assertIn(str(check["expected"]), summary)
                self.assertIn("FAIL", summary)

    def test_same_bytes_from_other_paths_are_accepted_and_binary_bytes_are_frozen(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            source_config = temporary / "config-link.yaml"
            source_config.symlink_to((ROOT / "configs/default.yaml").resolve())
            data = temporary / "sample-copy.csv"
            data.write_bytes((ROOT / "tests/fixtures/sample.csv").read_bytes())
            binary = temporary / "gridedge"
            original_binary = b"release-binary-before-replacement"
            binary.write_bytes(original_binary)
            binary_sha256 = hashlib.sha256(original_binary).hexdigest()
            output_dir = temporary / "output"
            observed_executables: list[Path] = []

            def assert_frozen(executable: Path | str) -> None:
                path = Path(executable)
                observed_executables.append(path)
                self.assertEqual(file_sha256(path), binary_sha256)

            def checked_process(
                command: list[str], *, timeout: float | None = None
            ) -> object:
                if command[1] == "replay":
                    self.assertEqual(timeout, PERF_GATE.REPLAY_HARD_TIMEOUT_SECONDS)
                assert_frozen(command[0])
                if len(observed_executables) == 1:
                    binary.write_bytes(b"replacement-after-first-child")
                action = command[1]
                stdout = ""
                if action == "replay":
                    stdout = json.dumps(
                        {
                            "event_count": 233,
                            "mode": "STOPPED",
                            "duplicate_events": 0,
                        }
                    )
                elif action == "rebuild-state":
                    stdout = json.dumps(
                        {
                            "matched": True,
                            "full_sequence": 233,
                            "snapshot_sequence": 233,
                        }
                    )
                elif action == "status":
                    stdout = "{}"
                return PERF_GATE.MeasuredProcess(0.01, 1.0, stdout, "", 0)

            def verified_run_identity(
                _database: Path, run_id: str, identity: dict[str, object]
            ) -> tuple[dict[str, object], list[dict[str, object]]]:
                self.assertEqual(run_id, "perf-short")
                expected = identity["expected_run_identity"]
                self.assertIsInstance(expected, dict)
                return {
                    "canonical_config_sha256": expected["canonical_config_sha256"],
                    "algorithm_manifest_sha256": "d" * 64,
                }, []

            @contextlib.contextmanager
            def fake_web_server(executable: Path, _config: Path, _data: Path):
                assert_frozen(executable)
                yield PERF_GATE.WebMeasurement("http://127.0.0.1:1", 1.0)

            response_metrics = {
                "snapshot_sequential_p95_seconds": 0.001,
                "snapshot_sequential_response_bytes": 100,
                "bars_1000_p95_seconds": 0.001,
                "bars_1000_response_bytes": 100,
                "events_1000_p95_seconds": 0.001,
                "events_1000_response_bytes": 100,
                "snapshot_concurrency_4_p95_seconds": 0.001,
            }
            database_metrics = {
                "path": "gridedge-perf.db",
                "bytes": 1024,
                "mib": 1024 / (1024 * 1024),
                "page_size": 4096,
                "page_count": 1,
                "freelist_count": 0,
                "live_bytes": 4096,
                "live_mib": 4096 / (1024 * 1024),
                "redundant_event_index_count": 0,
            }
            arguments = [
                "perf_gate.py",
                "--mode",
                "short",
                "--binary",
                str(binary),
                "--config",
                str(source_config),
                "--data",
                str(data),
                "--output-dir",
                str(output_dir),
                "--run-id",
                "perf-short",
                "--iterations",
                "3",
            ]
            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(PERF_GATE, "checked_process", checked_process),
                mock.patch.object(PERF_GATE, "web_server", fake_web_server),
                mock.patch.object(
                    PERF_GATE,
                    "response_measurements",
                    return_value=response_metrics,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "database_measurements",
                    return_value=database_metrics,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "verify_durable_run_identity",
                    side_effect=verified_run_identity,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "business_result",
                    return_value={
                        "sha256": json.loads(
                            (ROOT / "configs/performance-inputs-v1.json").read_text(
                                encoding="utf-8"
                            )
                        )["modes"]["short"]["business_result_sha256"]
                    },
                ),
            ):
                self.assertEqual(PERF_GATE.main(), 0)
            self.assertGreaterEqual(len(observed_executables), 5)
            self.assertNotEqual(file_sha256(binary), binary_sha256)

            identity = json.loads(
                (output_dir / "input-identity.json").read_text(encoding="utf-8")
            )
            self.assertTrue(identity["passed"])
            self.assertEqual(
                identity["workload"], {"run_id": "perf-short", "iterations": 3}
            )
            self.assertEqual(
                identity["expected_run_identity"]["algorithm"],
                json.loads(
                    (ROOT / "configs/performance-inputs-v1.json").read_text(
                        encoding="utf-8"
                    )
                )["modes"]["short"]["algorithm"],
            )
            self.assertEqual(
                identity["inputs"]["source_config"]["actual_sha256"],
                file_sha256(ROOT / "configs/default.yaml"),
            )
            self.assertEqual(
                identity["inputs"]["data"]["actual_sha256"],
                file_sha256(ROOT / "tests/fixtures/sample.csv"),
            )
            self.assertEqual(
                identity["inputs"]["binary"]["actual_sha256"], binary_sha256
            )
            self.assertTrue(identity["inputs"]["binary"]["passed"])
            metrics = json.loads(
                (output_dir / "metrics.json").read_text(encoding="utf-8")
            )
            self.assertEqual(metrics["input_identity"], identity)
            summary = (output_dir / "summary.md").read_text(encoding="utf-8")
            for input_name in ("source_config", "data"):
                self.assertIn(input_name, summary)
                self.assertIn(identity["inputs"][input_name]["actual_sha256"], summary)

    def test_frozen_binary_or_effective_config_tamper_blocks_before_next_child(
        self,
    ) -> None:
        for tampered_input in ("binary", "effective_config"):
            with (
                self.subTest(input=tampered_input),
                tempfile.TemporaryDirectory() as directory,
            ):
                temporary = Path(directory)
                paths = {
                    "binary": temporary / "gridedge",
                    "source_config": temporary / "source.yaml",
                    "data": temporary / "bars.csv",
                    "effective_config": temporary / "effective.yaml",
                    "input_manifest": temporary / "manifest.json",
                }
                for name, path in paths.items():
                    path.write_bytes(f"frozen-{name}".encode())
                output_dir = temporary / "output"
                identity: dict[str, object] = {
                    "schema_version": 1,
                    "mode": "short",
                    "passed": True,
                    "checks": [],
                }
                PERF_GATE.initialize_frozen_identity(
                    identity,
                    paths["binary"],
                    paths["source_config"],
                    paths["data"],
                    paths["effective_config"],
                    paths["input_manifest"],
                )
                measured = PERF_GATE.MeasuredProcess(0.01, 1.0, "", "", 0)
                with mock.patch.object(
                    PERF_GATE, "checked_process", return_value=measured
                ) as child:
                    PERF_GATE.checked_frozen_process(
                        [str(paths["binary"]), "validate-config"],
                        identity=identity,
                        output_dir=output_dir,
                        stage="validate_config",
                    )
                    paths[tampered_input].write_bytes(b"tampered-after-validation")
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "frozen input identity changed at stage before_replay",
                    ):
                        PERF_GATE.checked_frozen_process(
                            [str(paths["binary"]), "replay"],
                            identity=identity,
                            output_dir=output_dir,
                            stage="replay",
                        )
                child.assert_called_once()
                failed = [check for check in identity["checks"] if not check["passed"]]
                self.assertEqual(len(failed), 1)
                self.assertEqual(failed[0]["kind"], "stage_identity")
                self.assertEqual(
                    failed[0]["metric"],
                    f"before_replay_{tampered_input}_sha256",
                )
                summary = (output_dir / "summary.md").read_text(encoding="utf-8")
                self.assertIn(
                    "A frozen input changed between measurement stages", summary
                )
                self.assertNotIn("business-result fingerprint changed", summary)
                self.assertNotIn("Durable ledger identity differs", summary)
                self.assertIn("Input identity: **FAIL (blocking)**", summary)

    def test_business_result_closes_quantities_and_sums_partial_and_final_fills(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "business.db"
            status = create_business_result_v2_fixture(database)
            result = PERF_GATE.business_result(database, "fixture", status)
            self.assertEqual(result["schema_version"], 2)
            quantities = result["quantities"]
            self.assertEqual(
                quantities,
                {
                    "t_plus_one_blocked": 600,
                    "risk_blocked_before_decision": 300,
                    "no_profit_blocked": 200,
                    "gross_available": 5100,
                    "platform_residual": 600,
                    "algorithm_authorized": 4500,
                    "decision_exercise": 3000,
                    "decision_defer": 1500,
                    "platform_blocked": 0,
                    "order_intent": 3000,
                    "remaining_decision": 1500,
                    "intent_buy": 1500,
                    "intent_sell": 1500,
                    "fill_buy": 1500,
                    "fill_sell": 1500,
                },
            )
            self.assertEqual(
                quantities["gross_available"],
                quantities["algorithm_authorized"] + quantities["platform_residual"],
            )
            self.assertEqual(
                quantities["algorithm_authorized"],
                quantities["decision_exercise"] + quantities["decision_defer"],
            )
            self.assertEqual(
                quantities["order_intent"],
                quantities["decision_exercise"] - quantities["platform_blocked"],
            )
            self.assertEqual(
                quantities["remaining_decision"],
                quantities["decision_defer"] + quantities["platform_blocked"],
            )
            self.assertEqual(
                result["event_type_histogram"]["ORDER_PARTIALLY_FILLED"], 2
            )
            self.assertEqual(result["event_type_histogram"]["ORDER_FILLED"], 2)
            self.assertEqual(result["event_type_histogram"]["GRID_RIGHT_BLOCKED"], 1)
            self.assertEqual(
                result["quantities_by_direction"]["BUY"]["order_intent"], 1500
            )
            self.assertEqual(
                result["quantities_by_direction"]["SELL"]["order_intent"], 1500
            )
            self.assertEqual(
                result["tranche_safety"]["conservation_violation_count"], 0
            )
            self.assertTrue(result["paper_reconciliation"]["matched"])
            self.assertEqual(
                result["paper_reconciliation"]["report_counts"],
                {"accepted": 2, "rejected": 1, "cancelled": 1},
            )
            self.assertEqual(result["paper_reconciliation"]["fill_count"], 4)
            self.assertEqual(result["lot_safety"]["actual_sell_profit_sum"], "1785")
            self.assertEqual(result["lot_safety"]["worst_case_sell_profit_sum"], "1785")
            self.assertEqual(result["lot_safety"]["allocated_cost_sum"], "1200")
            payload_without_sha256 = dict(result)
            returned_sha256 = payload_without_sha256.pop("sha256")
            self.assertNotIn("sha256", payload_without_sha256)
            self.assertEqual(
                returned_sha256,
                PERF_GATE.canonical_json_sha256(payload_without_sha256),
            )

    def test_business_result_v2_rejects_independent_safety_fact_attacks(self) -> None:
        cases = (
            ("bad_checksum", "snapshot checksum is invalid"),
            ("direction", "decision direction differs"),
            ("negative_actual_slice", "negative-profit sell slice or lot"),
            ("unassigned_fees", "fees exceed canonical allocation ceilings"),
            ("lot_cost_conservation", "lot cost conservation"),
            ("right_residual_offset", "right buy-right has an invalid C/P partition"),
            ("missing_terminal", "right sets differ"),
            ("duplicate_terminal", "duplicate dispositions"),
            ("negative_tranche", "negative tranche bucket"),
            ("rejected_fill", "Paper Rejected report must not contain fills"),
            ("cancelled_fill", "Paper Cancelled report must not contain fills"),
            ("paper", "Paper reconciliation differs"),
        )
        for attack, expected_error in cases:
            with (
                self.subTest(attack=attack),
                tempfile.TemporaryDirectory() as directory,
            ):
                database = Path(directory) / "business.db"
                status = create_business_result_v2_fixture(database)
                with contextlib.closing(sqlite3.connect(database)) as connection:

                    def load_state() -> dict[str, object]:
                        raw = connection.execute(
                            "SELECT state_json FROM snapshots WHERE run_id='fixture'"
                        ).fetchone()[0]
                        return json.loads(raw)

                    def store_state(state: dict[str, object]) -> None:
                        raw = json.dumps(state, sort_keys=True, separators=(",", ":"))
                        checksum = hashlib.sha256(raw.encode("utf-8")).hexdigest()
                        connection.execute(
                            "UPDATE snapshots SET state_json=?,checksum=? "
                            "WHERE run_id='fixture'",
                            (raw, checksum),
                        )

                    if attack == "bad_checksum":
                        connection.execute(
                            "UPDATE snapshots SET checksum=? WHERE run_id='fixture'",
                            ("0" * 64,),
                        )
                    elif attack == "direction":
                        sequence, raw = connection.execute(
                            "SELECT sequence_number,payload FROM events "
                            "WHERE event_type='GATE_DECISION_MADE' "
                            "ORDER BY sequence_number LIMIT 1"
                        ).fetchone()
                        payload = json.loads(raw)
                        payload["request"]["context"]["direction"] = "SELL"
                        connection.execute(
                            "UPDATE events SET payload=? WHERE sequence_number=?",
                            (json.dumps(payload), sequence),
                        )
                    elif attack in {"negative_actual_slice", "unassigned_fees"}:
                        sequence, raw = connection.execute(
                            "SELECT sequence_number,payload FROM events "
                            "WHERE event_type='ORDER_PARTIALLY_FILLED' "
                            "AND json_extract(payload,'$.fill.direction')='SELL'"
                        ).fetchone()
                        payload = json.loads(raw)
                        if attack == "negative_actual_slice":
                            payload["fill"]["price"] = "0.5"
                            state = load_state()
                            state["lots"]["lot-sell-a"]["realized_pnl"] = "-217"
                            store_state(state)
                            status["realized_grid_pnl"] = "735"
                        else:
                            payload["fill"]["commission"] = "8"
                            payload["fill"]["tax"] = "0"
                        connection.execute(
                            "UPDATE events SET payload=? WHERE sequence_number=?",
                            (json.dumps(payload), sequence),
                        )
                    elif attack == "lot_cost_conservation":
                        state = load_state()
                        state["lots"]["lot-sell-a"]["allocated_cost"] = "561"
                        store_state(state)
                    elif attack == "right_residual_offset":
                        terminals = connection.execute(
                            "SELECT sequence_number,payload FROM events "
                            "WHERE event_type='GRID_RIGHT_RESERVED' "
                            "ORDER BY sequence_number"
                        ).fetchall()
                        for (sequence, raw), residual in zip(
                            terminals, (-100, 500), strict=True
                        ):
                            payload = json.loads(raw)
                            payload["platform_residual_quantity"] = residual
                            payload["gross_available_quantity"] = 1500 + residual
                            connection.execute(
                                "UPDATE events SET payload=? WHERE sequence_number=?",
                                (json.dumps(payload), sequence),
                            )
                    elif attack == "missing_terminal":
                        connection.execute(
                            "DELETE FROM events WHERE event_type='GRID_RIGHT_DEFERRED'"
                        )
                        sequence, raw = connection.execute(
                            "SELECT sequence_number,payload FROM events "
                            "WHERE event_type='GATE_DECISION_MADE' "
                            "AND json_extract(payload,'$.response.right_id')="
                            "'deferred-right'"
                        ).fetchone()
                        payload = json.loads(raw)
                        payload["response"]["outcome"]["defer_quantity"] = 0
                        connection.execute(
                            "UPDATE events SET payload=? WHERE sequence_number=?",
                            (json.dumps(payload), sequence),
                        )
                    elif attack == "duplicate_terminal":
                        raw = connection.execute(
                            "SELECT payload FROM events "
                            "WHERE event_type='GRID_RIGHT_RESERVED' "
                            "ORDER BY sequence_number LIMIT 1"
                        ).fetchone()[0]
                        next_sequence = int(status["event_count"]) + 1
                        connection.execute(
                            "INSERT INTO events VALUES ('fixture',?,?,?)",
                            ("GRID_RIGHT_RESERVED", next_sequence, raw),
                        )
                        state = load_state()
                        state["event_count"] = next_sequence
                        store_state(state)
                        connection.execute(
                            "UPDATE snapshots SET sequence_number=? "
                            "WHERE run_id='fixture'",
                            (next_sequence,),
                        )
                        status["event_count"] = next_sequence
                    elif attack == "negative_tranche":
                        state = load_state()
                        state["right_tranches"]["buy-tranche"][
                            "available_quantity"
                        ] = -1
                        state["right_tranches"]["buy-tranche"]["consumed_quantity"] = (
                            1501
                        )
                        store_state(state)
                    elif attack in {"rejected_fill", "cancelled_fill"}:
                        accepted_raw = connection.execute(
                            "SELECT report_json FROM paper_orders "
                            "WHERE intent_id='buy-intent'"
                        ).fetchone()[0]
                        target = attack.removesuffix("_fill")
                        target_intent = f"{target}-intent"
                        target_variant = target.title()
                        target_raw = connection.execute(
                            "SELECT report_json FROM paper_orders " "WHERE intent_id=?",
                            (target_intent,),
                        ).fetchone()[0]
                        accepted = json.loads(accepted_raw)
                        target_report = json.loads(target_raw)
                        moved_fill = accepted["Accepted"]["fills"].pop(0)
                        target_report[target_variant]["fills"].append(moved_fill)
                        connection.execute(
                            "UPDATE paper_orders SET report_json=? "
                            "WHERE intent_id='buy-intent'",
                            (json.dumps(accepted),),
                        )
                        connection.execute(
                            "UPDATE paper_orders SET report_json=? "
                            "WHERE intent_id=?",
                            (json.dumps(target_report), target_intent),
                        )
                    else:
                        raw = connection.execute(
                            "SELECT snapshot_json FROM paper_accounts "
                            "WHERE run_id='fixture'"
                        ).fetchone()[0]
                        paper = json.loads(raw)
                        paper["cash"]["available"] = "999"
                        connection.execute(
                            "UPDATE paper_accounts SET snapshot_json=? "
                            "WHERE run_id='fixture'",
                            (json.dumps(paper),),
                        )
                    connection.commit()
                with self.assertRaisesRegex(RuntimeError, expected_error):
                    PERF_GATE.business_result(database, "fixture", status)

    def test_identity_summary_attributes_business_and_durable_mismatches_precisely(
        self,
    ) -> None:
        cases = (
            (
                "business_result",
                "The deterministic business-result fingerprint changed.",
                "Durable ledger identity differs",
            ),
            (
                "run_identity",
                "Durable ledger identity differs from the frozen inputs.",
                "business-result fingerprint changed",
            ),
        )
        for kind, expected_message, excluded_message in cases:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                identity = {
                    "schema_version": 1,
                    "mode": "short",
                    "passed": False,
                    "checks": [
                        {
                            "metric": f"{kind}_sha256",
                            "actual": "a" * 64,
                            "expected": "b" * 64,
                            "passed": False,
                            "blocking": True,
                            "kind": kind,
                        }
                    ],
                }
                output_dir = Path(directory)
                PERF_GATE.write_identity_outputs(output_dir, identity)
                summary = (output_dir / "summary.md").read_text(encoding="utf-8")
                self.assertIn(expected_message, summary)
                self.assertNotIn(excluded_message, summary)
                self.assertNotIn(
                    "A frozen input changed between measurement stages", summary
                )
                self.assertIn("Input identity: **FAIL (blocking)**", summary)
                self.assertNotIn("Input identity: **PASS", summary)

    def test_manifest_is_read_once_and_one_byte_snapshot_drives_every_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            manifest_path = temporary / "performance-inputs-v1.json"
            manifest_a = (ROOT / "configs/performance-inputs-v1.json").read_bytes()
            expected_a = json.loads(manifest_a)["modes"]["short"]
            manifest_b = b'{"changed":"B","run_id":"mixed-B"}'
            manifest_c = b'{"changed":"C","run_id":"mixed-C"}'
            manifest_path.write_bytes(manifest_a)
            binary = temporary / "gridedge"
            binary.write_bytes(b"single-manifest-read-binary")
            output_dir = temporary / "output"
            observed_loads: list[bytes] = []
            real_load = PERF_GATE.load_input_manifest

            def load_once(
                mode: str, manifest_bytes: bytes | None = None
            ) -> dict[str, object]:
                self.assertEqual(mode, "short")
                self.assertEqual(manifest_bytes, manifest_a)
                observed_loads.append(manifest_bytes)
                manifest_path.write_bytes(manifest_b)
                return real_load(mode, manifest_bytes)

            def checked_process(
                command: list[str], *, timeout: float | None = None
            ) -> object:
                if command[1] == "replay":
                    self.assertEqual(timeout, PERF_GATE.REPLAY_HARD_TIMEOUT_SECONDS)
                action = command[1]
                if action == "validate-config":
                    manifest_path.write_bytes(manifest_c)
                    stdout = ""
                elif action == "replay":
                    stdout = json.dumps(
                        {
                            "event_count": 233,
                            "mode": "STOPPED",
                            "duplicate_events": 0,
                        }
                    )
                elif action == "rebuild-state":
                    stdout = json.dumps(
                        {
                            "matched": True,
                            "full_sequence": 233,
                            "snapshot_sequence": 233,
                        }
                    )
                else:
                    stdout = "{}"
                return PERF_GATE.MeasuredProcess(0.01, 1.0, stdout, "", 0)

            def durable_identity(
                _database: Path, run_id: str, identity: dict[str, object]
            ) -> tuple[dict[str, object], list[dict[str, object]]]:
                self.assertEqual(run_id, expected_a["run_id"])
                self.assertEqual(
                    identity["expected_run_identity"],
                    {
                        "canonical_config_sha256": expected_a[
                            "canonical_config_sha256"
                        ],
                        "data_sha256": expected_a["data_sha256"],
                        "dataset_bars": expected_a["dataset_bars"],
                        "symbol": expected_a["symbol"],
                        "first_timestamp": expected_a["first_timestamp"],
                        "last_timestamp": expected_a["last_timestamp"],
                        "algorithm": expected_a["algorithm"],
                    },
                )
                return {
                    "canonical_config_sha256": expected_a["canonical_config_sha256"],
                    "algorithm_manifest_sha256": "d" * 64,
                }, []

            @contextlib.contextmanager
            def fake_web_server(_binary: Path, _config: Path, _data: Path):
                yield PERF_GATE.WebMeasurement("http://127.0.0.1:1", 1.0)

            response_metrics = {
                "snapshot_sequential_p95_seconds": 0.001,
                "snapshot_sequential_response_bytes": 100,
                "bars_1000_p95_seconds": 0.001,
                "bars_1000_response_bytes": 100,
                "events_1000_p95_seconds": 0.001,
                "events_1000_response_bytes": 100,
                "snapshot_concurrency_4_p95_seconds": 0.001,
            }
            database_metrics = {
                "path": "gridedge-perf.db",
                "bytes": 1024,
                "mib": 1024 / (1024 * 1024),
                "page_size": 4096,
                "page_count": 1,
                "freelist_count": 0,
                "live_bytes": 4096,
                "live_mib": 4096 / (1024 * 1024),
                "redundant_event_index_count": 0,
            }
            arguments = [
                "perf_gate.py",
                "--mode",
                "short",
                "--binary",
                str(binary),
                "--config",
                str(ROOT / "configs/default.yaml"),
                "--data",
                str(ROOT / "tests/fixtures/sample.csv"),
                "--output-dir",
                str(output_dir),
            ]
            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(PERF_GATE, "ROOT", temporary),
                mock.patch.object(PERF_GATE, "INPUT_MANIFEST", manifest_path),
                mock.patch.object(
                    PERF_GATE, "load_input_manifest", side_effect=load_once
                ) as load,
                mock.patch.object(PERF_GATE, "checked_process", checked_process),
                mock.patch.object(PERF_GATE, "web_server", fake_web_server),
                mock.patch.object(
                    PERF_GATE,
                    "response_measurements",
                    return_value=response_metrics,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "database_measurements",
                    return_value=database_metrics,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "verify_durable_run_identity",
                    side_effect=durable_identity,
                ),
                mock.patch.object(
                    PERF_GATE,
                    "business_result",
                    return_value={"sha256": expected_a["business_result_sha256"]},
                ),
            ):
                self.assertEqual(PERF_GATE.main(), 0)
            load.assert_called_once()
            self.assertEqual(observed_loads, [manifest_a])
            self.assertEqual(manifest_path.read_bytes(), manifest_c)
            identity = json.loads(
                (output_dir / "input-identity.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                identity["manifest_sha256"], hashlib.sha256(manifest_a).hexdigest()
            )
            self.assertEqual(
                identity["workload"],
                {
                    "run_id": expected_a["run_id"],
                    "iterations": expected_a["iterations"],
                },
            )
            self.assertEqual(
                identity["inputs"]["source_config"]["expected_sha256"],
                expected_a["source_config_sha256"],
            )
            self.assertEqual(
                identity["inputs"]["data"]["expected_sha256"],
                expected_a["data_sha256"],
            )
            self.assertEqual(
                identity["business_result"]["sha256"],
                expected_a["business_result_sha256"],
            )
            frozen_manifest = Path(identity["frozen_inputs"]["input_manifest"]["path"])
            self.assertEqual(frozen_manifest.read_bytes(), manifest_a)
            artifact = (output_dir / "input-identity.json").read_bytes()
            self.assertNotIn(b"mixed-B", artifact)
            self.assertNotIn(b"mixed-C", artifact)

    def test_expected_hashes_are_not_cli_or_workflow_inputs(self) -> None:
        required = [
            "perf_gate.py",
            "--mode",
            "short",
            "--binary",
            "gridedge",
            "--config",
            "config.yaml",
            "--data",
            "data.csv",
            "--output-dir",
            "output",
        ]
        for option in ("--expected-config-sha256", "--expected-data-sha256"):
            with (
                self.subTest(option=option),
                mock.patch.object(sys, "argv", [*required, option, "0" * 64]),
                self.assertRaises(SystemExit),
            ):
                PERF_GATE.parse_arguments()

    def test_ci_workflows_bind_short_and_three_year_inputs_and_artifacts(self) -> None:
        pull_request_ci = (ROOT / ".github/workflows/ci.yml").read_text(
            encoding="utf-8"
        )
        performance_ci = (ROOT / ".github/workflows/performance.yml").read_text(
            encoding="utf-8"
        )
        for required in (
            "--mode short",
            "configs/default.yaml",
            "tests/fixtures/sample.csv",
            "target/release/gridedge",
            "database-size.json",
            "input-identity.json",
            "actions/upload-artifact@v4",
        ):
            self.assertIn(required, pull_request_ci)
        for required in (
            "schedule:",
            "release:",
            "workflow_dispatch:",
            "enforce_thresholds:",
            "--mode nightly",
            "configs/zhaoxin_5m_quantity_v10.yaml",
            "data/processed/002256.SZ_5m_raw_20230814_20260814.csv",
            "metrics.json",
            "database-size.json",
            "summary.md",
            "input-identity.json",
            "actions/upload-artifact@v4",
        ):
            self.assertIn(required, performance_ci)
        for workflow in (pull_request_ci, performance_ci):
            self.assertNotIn("--expected-config-sha256", workflow)
            self.assertNotIn("--expected-data-sha256", workflow)
            self.assertNotIn("expected_config_sha256:", workflow)
            self.assertNotIn("expected_data_sha256:", workflow)
        self.assertIn('tags:\n      - "v*"', performance_ci)
        self.assertIn("release:\n    types: [published]", performance_ci)
        enforce_expression = (
            "PERF_ENFORCE: ${{ github.event_name == 'release' || "
            "startsWith(github.ref, 'refs/tags/') || "
            "(github.event_name == 'workflow_dispatch' && inputs.enforce_thresholds) }}"
        )
        self.assertIn(enforce_expression, performance_ci)
        self.assertNotIn("github.event_name == 'schedule'", performance_ci)
        self.assertIn(
            'if [[ "${PERF_ENFORCE}" == "true" ]]; then\n'
            "            arguments+=(--enforce)",
            performance_ci,
        )


if __name__ == "__main__":
    unittest.main()
