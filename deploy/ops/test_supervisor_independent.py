"""Independent supervisor red tests.

These cases use only temporary files, temporary SQLite databases and fake
process APIs.  They must never touch the installed worker, browser, emulator,
broker or production ledger.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sqlite3
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
import uuid
from zoneinfo import ZoneInfo


SPEC = importlib.util.spec_from_file_location(
    "session_supervisor_independent_subject",
    Path(__file__).with_name("session_supervisor.py"),
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
STAGE_SPEC = importlib.util.spec_from_file_location(
    "stage_session_supervisor_independent_subject",
    Path(__file__).with_name("stage_session_supervisor.py"),
)
STAGE_MODULE = importlib.util.module_from_spec(STAGE_SPEC)
STAGE_SPEC.loader.exec_module(STAGE_MODULE)
TZ = ZoneInfo("Asia/Shanghai")


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class SupervisorStageIndependentContract(unittest.TestCase):
    def test_scoped_stage_cannot_promote_dirty_companion_artifacts(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-stage-fixture-") as temporary:
            fixture = Path(temporary)
            home = fixture / "home"
            root = fixture / "installed"
            repo = fixture / "repo"
            (root / "bin").mkdir(parents=True)
            (root / "config").mkdir()
            (repo / "deploy/ops").mkdir(parents=True)

            installed_companions = {
                "start_ths_trusted_session.sh": b"installed reviewed starter",
                "market_raw_repair.py": b"installed reviewed raw repair",
                "market_ingestor.py": b"installed reviewed ingestor",
                "gridedge_market_replay": b"installed reviewed replay",
            }
            for name, value in installed_companions.items():
                (root / "bin" / name).write_bytes(value)
            candidate = b"reviewed supervisor candidate"
            (repo / "deploy/ops/session_supervisor.py").write_bytes(candidate)

            # These deliberately different bytes model unrelated repository
            # development that must never enter a scoped supervisor stage.
            dirty = {
                repo / "deploy/start_ths_trusted_session.sh": b"dirty starter",
                repo / "deploy/ops/market_raw_repair.py": b"dirty raw repair",
                repo / "deploy/market_data/ingestor/market_ingestor.py": b"dirty ingestor",
                repo / "target/release/gridedge_market_replay": b"dirty replay",
            }
            for path, value in dirty.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(value)

            installed_inputs = [
                root / "bin/gridedge_ths_live",
                root / "bin/run_ths_android_sim.sh",
                root / "bin/run_ths_trusted_session_guard.sh",
                root / "config/ths_002256_sim.yaml",
            ]
            launch_plist = home / "Library/LaunchAgents/com.gridedge.ths-sim.plist"
            adb = home / "Library/Android/sdk/platform-tools/adb"
            emulator = home / "Library/Android/sdk/emulator/emulator"
            for path in installed_inputs + [launch_plist, adb, emulator]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(("reviewed " + path.name).encode())
            old_supervisor = root / "bin/session_supervisor.py"
            old_supervisor.write_bytes(b"reviewed old supervisor")
            baseline_files = installed_inputs + [launch_plist, adb, emulator, old_supervisor]
            baseline_files += [root / "bin" / name for name in installed_companions]
            baseline = dict(
                schema="gridedge.session-supervisor.v1", root=str(root),
                run_id="ths-002256-20260819-grid15-opening-v1",
                sdk=str(home / "Library/Android/sdk"), market_host="192.168.1.201",
                starter=str(root / "bin/start_ths_trusted_session.sh"),
                chrome_profile="Default", reviewed_url=MODULE.URL, calendar="SSE_2026_NOTICE_45",
                files={str(path): sha_bytes(path.read_bytes()) for path in baseline_files},
            )
            manifest = root / "config/session-supervisor-manifest.json"
            manifest.write_text(json.dumps(baseline, sort_keys=True))
            baseline_sha = sha_bytes(manifest.read_bytes())

            stage = Path("/tmp") / ("gridedge-supervisor-stage-independent-" + str(uuid.uuid4()))
            try:
                with patch.object(STAGE_MODULE, "ROOT", root), patch.object(
                    STAGE_MODULE, "REPO", repo
                ), patch.object(Path, "home", return_value=home), patch.object(
                    STAGE_MODULE.sys, "argv", ["stage_session_supervisor.py", str(stage),
                                               "--baseline-manifest-sha256", baseline_sha]
                ), patch("sys.stdout", new=io.StringIO()):
                    STAGE_MODULE.main()
                self.assertEqual((stage / "session_supervisor.py").read_bytes(), candidate)
                for name, value in installed_companions.items():
                    self.assertEqual((stage / name).read_bytes(), value)
                    self.assertNotEqual((stage / name).read_bytes(), next(
                        dirty_value for dirty_path, dirty_value in dirty.items()
                        if dirty_path.name == name
                    ))
            finally:
                if stage.exists():
                    shutil.rmtree(stage)


class ManifestIndependentContract(unittest.TestCase):
    def test_reviewed_root_cannot_be_redirected_through_parent_symlink(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-root-") as temporary:
            home = Path(temporary) / "home"
            actual = Path(temporary) / "redirected-installation"
            root = home / "Library/Application Support/GridEdge-T"
            sdk = home / "Library/Android/sdk"
            actual.mkdir(parents=True)
            root.parent.mkdir(parents=True)
            root.symlink_to(actual, target_is_directory=True)

            files = [
                root / "bin/gridedge_ths_live",
                root / "bin/run_ths_android_sim.sh",
                root / "bin/run_ths_trusted_session_guard.sh",
                root / "bin/start_ths_trusted_session.sh",
                root / "bin/session_supervisor.py",
                root / "config/ths_002256_sim.yaml",
                home / "Library/LaunchAgents/com.gridedge.ths-sim.plist",
                sdk / "platform-tools/adb",
                sdk / "emulator/emulator",
            ]
            for item in files:
                item.parent.mkdir(parents=True, exist_ok=True)
                item.write_bytes(b"isolated fixture")
            manifest = {
                "schema": "gridedge.session-supervisor.v1",
                "root": str(root),
                "run_id": MODULE.RUN,
                "sdk": str(sdk),
                "market_host": "192.168.1.201",
                "starter": str(root / "bin/start_ths_trusted_session.sh"),
                "chrome_profile": "Default",
                "reviewed_url": MODULE.URL,
                "calendar": "SSE_2026_NOTICE_45",
                "files": {str(item): MODULE.sha(item) for item in files},
            }
            manifest_path = home / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            with patch.object(Path, "home", return_value=home), patch.object(
                MODULE, "__file__", str(root / "bin/session_supervisor.py")
            ):
                with self.assertRaises(ValueError):
                    MODULE.load_manifest(manifest_path, MODULE.sha(manifest_path))


class BindingHistoryIndependentContract(unittest.TestCase):
    def make_fixture(self, root: Path):
        runtime = root / "runtime"
        runtime.mkdir(parents=True)
        ledger = sqlite3.connect(runtime / "002256-grid.db")
        ledger.executescript(
            """
            CREATE TABLE events(run_id TEXT,sequence_number INTEGER,event_type TEXT,
                                event_time TEXT,payload TEXT);
            CREATE TABLE paper_accounts(run_id TEXT,snapshot_json TEXT);
            CREATE TABLE database_identity(singleton INTEGER,instance_id TEXT);
            """
        )
        ledger.execute(
            "INSERT INTO events VALUES(?,?,?,?,?)",
            (MODULE.RUN, 1, "ALGORITHM_REGISTERED", "2026-09-07 09:00:00",
             json.dumps({"platform_sha256": "a" * 64})),
        )
        ledger.execute(
            "INSERT INTO paper_accounts VALUES(?,?)",
            (MODULE.RUN, json.dumps({"cash": {"available": "1"}, "open_order_ids": []})),
        )
        ledger.execute("INSERT INTO database_identity VALUES(1,'db-instance')")
        ledger.commit()
        ledger.close()

        outbox = sqlite3.connect(runtime / "002256-outbox-opening-v1.db")
        outbox.executescript(
            """
            CREATE TABLE outbox_metadata(source_database_instance_id TEXT,run_id TEXT,cursor INTEGER);
            CREATE TABLE remote_adapter_binding_history(
              revision INTEGER,identity_sha256 TEXT,identity_json TEXT,
              previous_identity_sha256 TEXT,binding_kind TEXT,
              source_database_instance_id TEXT,run_id TEXT);
            CREATE TABLE remote_adapter_binding(
              identity_sha256 TEXT,identity_json TEXT,source_database_instance_id TEXT,run_id TEXT);
            CREATE TABLE staged_intents(
              intent_id TEXT,state TEXT,cancel_state TEXT,
              remote_contract_id TEXT,request_sha256 TEXT);
            CREATE TABLE remote_execution_facts(
              intent_id TEXT,remote_contract_id TEXT,request_sha256 TEXT,terminal_kind TEXT);
            """
        )
        outbox.execute("INSERT INTO outbox_metadata VALUES('db-instance',?,1)", (MODULE.RUN,))
        genesis = json.dumps({
            "platform_sha256": "a" * 64,
            "runner_sha256": "b" * 64,
            "guard_sha256": "c" * 64,
            "launch_plist_sha256": "d" * 64,
        }, sort_keys=True, separators=(",", ":"))
        genesis_sha = sha_bytes(genesis.encode())
        latest = json.dumps({
            "platform_sha256": "a" * 64,
            "runner_sha256": "e" * 64,
            "guard_sha256": "c" * 64,
            "launch_plist_sha256": "d" * 64,
        }, sort_keys=True, separators=(",", ":"))
        latest_sha = sha_bytes(latest.encode())
        outbox.execute(
            "INSERT INTO remote_adapter_binding_history VALUES(1,?,?,NULL,'GENESIS','db-instance',?)",
            (genesis_sha, genesis, MODULE.RUN),
        )
        outbox.execute(
            "INSERT INTO remote_adapter_binding_history VALUES(2,?,?,?,'PLATFORM_UPGRADE','db-instance',?)",
            (latest_sha, latest, genesis_sha, MODULE.RUN),
        )
        outbox.execute(
            "INSERT INTO remote_adapter_binding VALUES(?,?, 'db-instance',?)",
            (genesis_sha, genesis, MODULE.RUN),
        )
        outbox.commit()
        outbox.close()
        return runtime / "002256-outbox-opening-v1.db", genesis_sha

    def test_frozen_terminal_outbox_accepts_cancelled_and_exact_filled_only(self):
        cases = [
            ("SUBMITTED", "CANCELLED", None, 0),
            ("SUBMITTED", "CANCELLING", None, 1),
            ("SUBMITTED", "AMBIGUOUS", None, 1),
            ("PREPARED", "CANCELLED", None, 1),
            ("SUBMITTED", "NOT_REQUESTED", ("intent", "contract", "request", "FILLED"), 0),
            ("SUBMITTED", "NOT_REQUESTED", ("intent", "other", "request", "FILLED"), 1),
            ("SUBMITTED", "NOT_REQUESTED", ("intent", "contract", "other", "FILLED"), 1),
        ]
        for state, cancel_state, fact, expected in cases:
            with self.subTest(
                state=state, cancel_state=cancel_state, fact=fact, expected=expected
            ), tempfile.TemporaryDirectory(
                prefix="gridedge-supervisor-terminal-"
            ) as temporary:
                root = Path(temporary)
                outbox_path, _ = self.make_fixture(root)
                db = sqlite3.connect(outbox_path)
                db.execute(
                    "INSERT INTO staged_intents VALUES(?,?,?,?,?)",
                    ("intent", state, cancel_state, "contract", "request"),
                )
                if fact is not None:
                    db.execute(
                        "INSERT INTO remote_execution_facts VALUES(?,?,?,?)", fact
                    )
                db.commit()
                db.close()
                health = MODULE.ledger_health(root)
                self.assertEqual(health["unresolved"], expected)

    def append_latest_bar(self, root: Path, stage_modes, final_mode=None):
        ledger_path = root / "runtime/002256-grid.db"
        db = sqlite3.connect(ledger_path)
        sequence = db.execute("SELECT max(sequence_number) FROM events").fetchone()[0]
        timestamp = "2026-09-07T11:25:00+08:00"
        for mode, event_type in zip(
            stage_modes,
            (
                "MARKET_DATA_RECEIVED",
                "MARKET_BAR_DECISIONS_COMMITTED",
                "MARKET_BAR_PROCESSED",
            ),
        ):
            sequence += 1
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?)",
                (
                    MODULE.RUN,
                    sequence,
                    "SERVICE_MODE_CHANGED",
                    timestamp,
                    json.dumps({"mode": mode}),
                ),
            )
            sequence += 1
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?)",
                (
                    MODULE.RUN,
                    sequence,
                    event_type,
                    timestamp,
                    json.dumps({"timestamp": timestamp}),
                ),
            )
        if final_mode is not None:
            sequence += 1
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?)",
                (
                    MODULE.RUN,
                    sequence,
                    "SERVICE_MODE_CHANGED",
                    timestamp,
                    json.dumps({"mode": final_mode}),
                ),
            )
        db.commit()
        db.close()

    def test_latest_bar_strategy_evaluation_is_bound_to_each_stage_mode(self):
        cases = [
            (("RUNNING", "RUNNING", "RUNNING"), "RUNNING", True),
            (("READ_ONLY", "READ_ONLY", "READ_ONLY"), "RUNNING", False),
            (("RUNNING", "READ_ONLY", "RUNNING"), "RUNNING", None),
        ]
        for stage_modes, final_mode, evaluated in cases:
            with self.subTest(stage_modes=stage_modes), tempfile.TemporaryDirectory(
                prefix="gridedge-supervisor-bar-mode-"
            ) as temporary:
                root = Path(temporary)
                self.make_fixture(root)
                self.append_latest_bar(root, stage_modes, final_mode)
                health = MODULE.ledger_health(root)
                self.assertEqual(health["mode"], final_mode)
                self.assertEqual(
                    [stage["mode"] for stage in health["latest_bar_stage_modes"]],
                    list(stage_modes),
                )
                self.assertIs(health["latest_bar_strategy_evaluated"], evaluated)

    def test_history_gap_prior_digest_kind_source_and_genesis_projection_are_rejected(self):
        mutations = [
            "UPDATE remote_adapter_binding_history SET revision=3 WHERE revision=2",
            "UPDATE remote_adapter_binding_history SET previous_identity_sha256='bad' WHERE revision=2",
            "UPDATE remote_adapter_binding_history SET identity_sha256='bad' WHERE revision=2",
            "UPDATE remote_adapter_binding_history SET binding_kind='GENESIS' WHERE revision=2",
            "UPDATE remote_adapter_binding_history SET source_database_instance_id='other' WHERE revision=2",
            "UPDATE remote_adapter_binding SET identity_sha256='bad'",
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory(
                prefix="gridedge-supervisor-binding-"
            ) as temporary:
                root = Path(temporary)
                outbox_path, _ = self.make_fixture(root)
                db = sqlite3.connect(outbox_path)
                db.execute(mutation)
                db.commit()
                db.close()
                with self.assertRaises(ValueError):
                    MODULE.ledger_health(root)


class MarketHealthIndependentContract(unittest.TestCase):
    def fixture_event(self, sequence, now, age=5):
        return {
            "event_id": f"{sequence:064x}",
            "event_type": "SOURCE_STATUS",
            "source_sequence": sequence,
            "source": {
                "source_id": "eastmoney-web-time-sales",
                "source_instance_id": "8101d65c-bdba-4de3-83e0-8983506f159e",
                "provider_version": "eastmoney-time-sales-dom-v6",
            },
            "payload": {
                "status": "SOURCE_OBSERVED_CURRENT",
                "observed_at_us": int((now - timedelta(seconds=age)).timestamp() * 1_000_000),
            },
        }

    def check_stream(self, events, now, expected_sequence):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-retransmit-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            payloads = [json.dumps(event, separators=(",", ":")).encode() for event in events]
            raw = b"".join(json.dumps({"topic": "isolated", "payload": list(payload)}).encode()
                           + b"\n" for payload in payloads)
            path = root / "runtime/002256-opening-v1-market-mqtt.jsonl"
            path.write_bytes(raw)
            selected = next(payload for event, payload in zip(events, payloads)
                            if event["source_sequence"] == expected_sequence)
            with patch.object(MODULE, "command", return_value=SimpleNamespace(
                returncode=0, stdout=f"{sha_bytes(selected)}|{expected_sequence}\n".encode(),
                stderr=b"",
            )) as remote:
                health = MODULE.market_health(root, now)
                self.assertEqual(health["source_sequence"], expected_sequence)
                self.assertTrue(health["committed"])
                self.assertIn(f"{expected_sequence:064x}", remote.call_args.args[0][-1])
                self.assertEqual(path.read_bytes(), raw)
                return health

    def test_old_retransmissions_never_regress_sequence_or_observation(self):
        now = datetime(2026, 9, 8, 9, 30, 20, tzinfo=TZ)
        old = self.fixture_event(42, now, age=86400)
        current = self.fixture_event(43, now)
        for repeats in (1, 1200):
            with self.subTest(repeats=repeats):
                health = self.check_stream([old, current] + [old] * repeats, now, 43)
                self.assertTrue(health["source_fresh"])
                self.assertEqual(health["observed_at_us"], current["payload"]["observed_at_us"])

    def test_conflict_identity_gap_and_unseen_reordering_are_not_duplicates(self):
        now = datetime(2026, 9, 8, 9, 30, 20, tzinfo=TZ)
        original = self.fixture_event(42, now)
        changed = self.fixture_event(42, now, age=10)
        reused_id = self.fixture_event(43, now)
        reused_id["event_id"] = original["event_id"]
        wrong_source = self.fixture_event(43, now)
        wrong_source["source"]["source_id"] = "unreviewed"
        for events in ([original, changed], [original, reused_id],
                       [original, wrong_source], [original, self.fixture_event(44, now)],
                       [original, self.fixture_event(41, now)]):
            with self.subTest(events=events), self.assertRaises(ValueError):
                self.check_stream(events, now, 42)

    def test_exact_payload_hash_event_and_sequence_are_all_required(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-market-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            now = datetime(2026, 9, 7, 10, 0, 20, tzinfo=TZ)
            event = {
                "event_id": "1" * 64,
                "event_type": "SOURCE_STATUS",
                "source_sequence": 42,
                "source": {
                    "source_id": "eastmoney-web-time-sales",
                    "source_instance_id": "8101d65c-bdba-4de3-83e0-8983506f159e",
                    "provider_version": "eastmoney-time-sales-dom-v6",
                },
                "payload": {
                    "status": "SOURCE_OBSERVED_CURRENT",
                    "observed_at_us": int((now - timedelta(seconds=5)).timestamp() * 1_000_000),
                },
            }
            payload = json.dumps(event, separators=(",", ":")).encode()
            envelope = json.dumps({"topic": "isolated", "payload": list(payload)}).encode() + b"\n"
            (root / "runtime/002256-opening-v1-market-mqtt.jsonl").write_bytes(envelope)
            expected = f"{sha_bytes(payload)}|42\n".encode()
            with patch.object(
                MODULE,
                "command",
                return_value=SimpleNamespace(returncode=0, stdout=expected, stderr=b""),
            ):
                healthy = MODULE.market_health(root, now)
                self.assertTrue(healthy["committed"])
                self.assertTrue(healthy["source_fresh"])
            for remote in [b"0" * 64 + b"|42\n", sha_bytes(payload).encode() + b"|43\n", b""]:
                with self.subTest(remote=remote), patch.object(
                    MODULE,
                    "command",
                    return_value=SimpleNamespace(returncode=0, stdout=remote, stderr=b""),
                ):
                    self.assertFalse(MODULE.market_health(root, now)["committed"])


class PowerAndClosedWindowIndependentContract(unittest.TestCase):
    def test_power_reports_ac_battery_and_unknown_without_mutation(self):
        cases = [
            (
                SimpleNamespace(
                    returncode=0,
                    stdout=(
                        b"Now drawing from 'AC Power'\n"
                        b" -InternalBattery-0\t100%; charged; 0:00 remaining present: true\n"
                    ),
                    stderr=b"",
                ),
                {"known": True, "ac_connected": True, "battery_percent": 100},
            ),
            (
                SimpleNamespace(
                    returncode=0,
                    stdout=(
                        b"Now drawing from 'Battery Power'\n"
                        b" -InternalBattery-0\t47%; discharging; 2:10 remaining present: true\n"
                    ),
                    stderr=b"",
                ),
                {"known": True, "ac_connected": False, "battery_percent": 47},
            ),
            (
                SimpleNamespace(returncode=1, stdout=b"", stderr=b"failed"),
                {"known": False, "ac_connected": False, "battery_percent": None},
            ),
        ]
        for result, expected in cases:
            with self.subTest(expected=expected), patch.object(
                MODULE, "command", return_value=result
            ) as invoked:
                self.assertEqual(MODULE.power(), expected)
                invoked.assert_called_once_with(["/usr/bin/pmset", "-g", "batt"])

    def test_closed_once_preserves_safety_block_instead_of_observe_closed(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-closed-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            state = {
                "identity": False,
                "unlocked": True,
                "maintenance": False,
                "guards": 0,
                "workers": 0,
                "chrome": True,
                "devices": 1,
                "reviewed_device": True,
                "emulator_processes": 1,
                "booted": True,
                "mqtt": True,
                "breaker_open": False,
                "power": {
                    "known": True,
                    "ac_connected": True,
                    "battery_percent": 100,
                },
            }
            reports = []
            args = SimpleNamespace(
                manifest="ignored",
                manifest_sha256="frozen",
                verify_manifest_only=False,
                once=True,
            )
            with patch.object(
                MODULE.argparse.ArgumentParser, "parse_args", return_value=args
            ), patch.object(
                MODULE, "load_manifest", return_value={"root": str(root)}
            ), patch.object(
                MODULE, "snapshot", return_value=(state, {"binding": {}})
            ), patch.object(
                MODULE, "window", return_value="CLOSED"
            ), patch.object(
                MODULE,
                "market_health",
                return_value={"committed": False, "source_fresh": False},
            ), patch.object(
                MODULE, "write_record", side_effect=lambda _path, row: reports.append(row)
            ), patch.object(MODULE.fcntl, "flock"), patch.object(
                MODULE.signal, "signal"
            ), patch("builtins.print"):
                with self.assertRaisesRegex(ValueError, "BLOCKED_IDENTITY"):
                    MODULE.main()
            self.assertEqual(reports[0]["window"], "CLOSED")
            self.assertEqual(reports[0]["action"], "BLOCKED_IDENTITY")
            self.assertFalse(reports[0]["executed"])


class ColdBootOrchestrationIndependentContract(unittest.TestCase):
    @staticmethod
    def booting_state():
        return {
            "identity": True,
            "unlocked": True,
            "maintenance": False,
            "guards": 0,
            "workers": 0,
            "chrome": True,
            "devices": 1,
            "reviewed_device": False,
            "reviewed_booting": True,
            "emulator_processes": 1,
            "booted": False,
            "mqtt": True,
            "breaker_open": False,
        }

    def test_cold_boot_launch_receives_its_own_full_boot_deadline(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-loop-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            reports = []
            applied = []
            virtual = {"seconds": 0, "sleeps": 0}

            def monotonic():
                return virtual["seconds"]

            def sleep(_seconds):
                virtual["seconds"] += 1
                virtual["sleeps"] += 1
                if applied and reports and reports[-1].get("action") == "WAIT_ANDROID_BOOT":
                    raise KeyboardInterrupt

            args = SimpleNamespace(
                manifest="ignored",
                manifest_sha256="frozen",
                verify_manifest_only=False,
                once=False,
            )
            manifest = {"root": str(root)}
            health = {"binding": {}}

            def apply(action, _manifest, _manifest_path, _manifest_sha256):
                applied.append(action)
                if action == "COLD_BOOT_EMULATOR":
                    attempt = {
                        "date": MODULE.datetime.now(MODULE.TZ).date().isoformat(),
                        "phase": "LAUNCHED",
                        "launched_at": MODULE.datetime.now(MODULE.TZ).isoformat(),
                    }
                    MODULE.atomic_json(root / "runtime/supervisor-cold-boot-attempt.json", attempt)

            with patch.object(MODULE.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(MODULE, "load_manifest", return_value=manifest), \
                 patch.object(MODULE, "snapshot", return_value=(self.booting_state(), health)), \
                 patch.object(MODULE, "market_health", return_value={"committed": True, "source_fresh": True}), \
                 patch.object(MODULE, "window", return_value="TRADING"), \
                 patch.object(MODULE, "write_record", side_effect=lambda _path, row: reports.append(row)), \
                 patch.object(MODULE, "apply", side_effect=apply), \
                 patch.object(MODULE.fcntl, "flock"), patch.object(MODULE.signal, "signal"), \
                 patch.object(MODULE.time, "monotonic", side_effect=monotonic), \
                 patch.object(MODULE.time, "sleep", side_effect=sleep), \
                 patch("builtins.print"):
                with self.assertRaises(KeyboardInterrupt):
                    MODULE.main()
            actions = [row["action"] for row in reports if "window" in row]
            transitions = [action for index, action in enumerate(actions)
                           if index == 0 or action != actions[index - 1]]
            self.assertEqual(transitions[:3], [
                "WAIT_ANDROID_BOOT", "COLD_BOOT_EMULATOR", "WAIT_ANDROID_BOOT"
            ])
            self.assertEqual(applied, ["COLD_BOOT_EMULATOR"])

    def test_offline_adb_kill_failure_cannot_consume_attempt_without_stopping_unique_qemu(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-coldboot-") as temporary:
            root = Path(temporary)
            (root / "logs").mkdir()
            (root / "runtime").mkdir()
            sdk = root / "sdk"
            (sdk / "platform-tools").mkdir(parents=True)
            (sdk / "emulator").mkdir()
            manifest = {"root": str(root), "sdk": str(sdk)}
            events = []
            qemu_inventory = iter([[4242], [4242], [4242], [], []])

            def command(argv, **_kwargs):
                if argv[-2:] == ["emu", "kill"]:
                    events.append("adb-kill-failed")
                    return SimpleNamespace(returncode=1, stdout=b"", stderr=b"device offline")
                if argv[0] == "/bin/ps":
                    events.append("identity-rechecked")
                    executable = sdk / "emulator/qemu/darwin-aarch64/qemu-system-aarch64"
                    return SimpleNamespace(
                        returncode=0,
                        stdout=(
                            "Sun Sep  7 10:00:00 2026 " + str(executable) +
                            " -avd THSP_API_32 -memory 4096 -no-snapshot-load "
                            "-no-snapshot-save -gpu swiftshader_indirect\n"
                        ).encode(),
                        stderr=b"",
                    )
                raise AssertionError(f"unexpected command: {argv!r}")

            def fake_kill(pid, sig):
                self.assertEqual(pid, 4242)
                self.assertEqual(sig, MODULE.signal.SIGTERM)
                events.append("sigterm")

            def fake_popen(*_args, **_kwargs):
                events.append("popen")
                return SimpleNamespace(pid=5252)

            def pids(pattern, exact=False):
                del exact
                if "gridedge_ths_live" in pattern:
                    return []
                return next(qemu_inventory)

            with patch.object(MODULE, "pids", side_effect=pids), \
                 patch.object(MODULE, "command", side_effect=command), \
                 patch.object(MODULE.os, "kill", side_effect=fake_kill) as kill, \
                 patch.object(MODULE.time, "sleep"), \
                 patch.object(MODULE.subprocess, "Popen", side_effect=fake_popen) as popen:
                MODULE.apply("COLD_BOOT_EMULATOR", manifest)
            kill.assert_called_once_with(4242, MODULE.signal.SIGTERM)
            popen.assert_called_once()
            self.assertLess(events.index("identity-rechecked"), events.index("sigterm"))
            self.assertLess(events.index("sigterm"), events.index("popen"))

    def test_reserved_stopped_launching_and_launched_phases_resume_without_second_cold_boot(self):
        today = MODULE.datetime.now(MODULE.TZ).date().isoformat()
        for phase in ("RESERVED", "STOPPED"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory(
                prefix="gridedge-supervisor-phase-"
            ) as temporary:
                root = Path(temporary)
                (root / "runtime").mkdir()
                (root / "logs").mkdir()
                sdk = root / "sdk"
                marker = root / "runtime/supervisor-cold-boot-attempt.json"
                MODULE.atomic_json(marker, {
                    "date": today,
                    "phase": phase,
                    "pid": 4242,
                    "identity": "reviewed-old-owner",
                })
                manifest = {"root": str(root), "sdk": str(sdk)}

                def pids(pattern, exact=False):
                    del exact
                    return []

                with patch.object(MODULE, "pids", side_effect=pids), \
                     patch.object(MODULE, "launch_emulator") as launch, \
                     patch.object(MODULE.os, "kill") as kill:
                    MODULE.cold_boot(manifest)
                launch.assert_called_once_with(manifest)
                kill.assert_not_called()
                self.assertEqual(json.loads(marker.read_text())["phase"], "LAUNCHED")

        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-phase-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            marker = root / "runtime/supervisor-cold-boot-attempt.json"
            MODULE.atomic_json(marker, {
                "date": today,
                "phase": "LAUNCHING",
                "pid": 4242,
                "identity": "reviewed-old-owner",
                "launch_requested_at": MODULE.datetime.now(MODULE.TZ).isoformat(),
            })
            manifest = {"root": str(root), "sdk": str(root / "sdk")}

            def launching_pids(pattern, exact=False):
                del exact
                return [] if "gridedge_ths_live" in pattern else [5252]

            replacement = (
                "reviewed-new-owner -no-snapshot-load -no-snapshot-save "
                "-gpu swiftshader_indirect"
            )
            with patch.object(MODULE, "pids", side_effect=launching_pids), \
                 patch.object(MODULE, "emulator_identity", return_value=replacement), \
                 patch.object(MODULE, "launch_emulator") as launch, \
                 patch.object(MODULE.os, "kill") as kill:
                MODULE.cold_boot(manifest)
            launch.assert_not_called()
            kill.assert_not_called()
            self.assertEqual(json.loads(marker.read_text())["phase"], "LAUNCHED")

        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-phase-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            marker = root / "runtime/supervisor-cold-boot-attempt.json"
            MODULE.atomic_json(marker, {
                "date": today,
                "phase": "LAUNCHED",
                "pid": 4242,
                "identity": "reviewed-old-owner",
                "launched_at": MODULE.datetime.now(MODULE.TZ).isoformat(),
            })
            manifest = {"root": str(root), "sdk": str(root / "sdk")}
            with patch.object(MODULE, "pids", return_value=[]), \
                 patch.object(MODULE, "launch_emulator") as launch, \
                 patch.object(MODULE.os, "kill") as kill:
                with self.assertRaisesRegex(ValueError, "already launched today"):
                    MODULE.cold_boot(manifest)
            launch.assert_not_called()
            kill.assert_not_called()

    def test_reserved_owner_identity_change_and_never_exit_both_block_before_restart(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-owner-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            manifest = {"root": str(root), "sdk": str(root / "sdk")}
            identities = iter(["owner-at-reservation", "changed-owner-same-pid"])

            def pids(pattern, exact=False):
                del exact
                return [] if "gridedge_ths_live" in pattern else [4242]

            with patch.object(MODULE, "pids", side_effect=pids), \
                 patch.object(MODULE, "emulator_identity", side_effect=lambda *_: next(identities)), \
                 patch.object(MODULE, "launch_emulator") as launch, \
                 patch.object(MODULE.os, "kill") as kill:
                with self.assertRaisesRegex(ValueError, "reserved cold boot owner changed"):
                    MODULE.cold_boot(manifest)
            launch.assert_not_called()
            kill.assert_not_called()

        with tempfile.TemporaryDirectory(prefix="gridedge-supervisor-owner-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            (root / "logs").mkdir()
            manifest = {"root": str(root), "sdk": str(root / "sdk")}

            def pids(pattern, exact=False):
                del exact
                return [] if "gridedge_ths_live" in pattern else [4242]

            clock = iter([0, 9])
            with patch.object(MODULE, "pids", side_effect=pids), \
                 patch.object(MODULE, "emulator_identity", return_value="same-reviewed-owner"), \
                 patch.object(MODULE, "command", return_value=SimpleNamespace(
                     returncode=1, stdout=b"", stderr=b"device offline"
                 )), \
                 patch.object(MODULE.time, "monotonic", side_effect=lambda: next(clock)), \
                 patch.object(MODULE, "launch_emulator") as launch, \
                 patch.object(MODULE.os, "kill") as kill:
                with self.assertRaisesRegex(ValueError, "did not stop cleanly"):
                    MODULE.cold_boot(manifest)
            kill.assert_called_once_with(4242, MODULE.signal.SIGTERM)
            launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
