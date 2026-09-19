"""Independent backend integration: real isolated SQLite/files, simulated OS/Rust.

No production Host, tmux, signatures, Rust rebuild, UI or release certification.
The fake Rust boundary appends durable facts; it never supplies in-memory phase.
"""
import copy
from datetime import datetime
import importlib
import json
import os
from pathlib import Path
import plistlib
import stat
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import test_core_identity_migration as stage_fixture
import test_postclose_core_migration as db_fixture


class ReceiptLost(RuntimeError):
    pass


class FixtureHost:
    def __init__(self, case):
        self.case = case
        self.owner = dict(pid=45001, birth="fixture-cli-birth-1",
                          executable_sha256=db_fixture.sha(b"reviewed python"), executable_inode=99)
        self.old_supervisor = dict(pid=45002, birth="fixture-supervisor-birth",
            executable_sha256=self.owner["executable_sha256"], executable_inode=99,
            manifest_sha256=case.stage.trust["manifest_sha256"])
        self.processes = dict(supervisors=[self.old_supervisor], guards=[], workers=[],
            launchd_present=False, supervisor_socket="default", supervisor_session="gridedge_supervisor")
        self.current_time = datetime(2026, 9, 14, 19, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
        self.calls = []
        self.dead_owners = []
        self.failure = None
        self.sticky_supervisor = False

    def now(self):
        return self.current_time

    def owner_alive(self, owner):
        return "ABSENT" if owner in self.dead_owners else "MATCH"

    def inventory(self):
        return copy.deepcopy(self.processes)

    def verify_maintenance(self, entry, boundary):
        for name in ('ths-deployment-maintenance', 'ths-deployment-coordination.lock'):
            self.case.assertTrue((self.case.root / 'runtime' / name).is_dir())
        self.case.assertEqual(self.processes['supervisors'], [entry])
        self.case.assertIsNotNone(boundary.tzinfo)
        self.calls.append('verify:maintenance')
        return True

    def verify_released(self, entry, boundary):
        self.case.assertEqual(self.processes['supervisors'], [entry])
        self.case.assertIsNotNone(boundary.tzinfo)
        for name in ('ths-deployment-maintenance', 'ths-deployment-coordination.lock'):
            self.case.assertFalse((self.case.root / 'runtime' / name).exists())
        self.calls.append('verify:released')
        return True

    def quiesce_supervisor(self, owner):
        self.case.assertEqual(owner, self.old_supervisor)
        self.case.assertEqual(self.processes["supervisors"], [self.old_supervisor])
        self.calls.append("quiesce")
        if not self.sticky_supervisor:
            self.processes["supervisors"] = []

    def verify_signature(self, path, digest):
        self.case.assertEqual(db_fixture.sha(Path(path).read_bytes()), digest)
        self.calls.append("verify:" + digest)
        return True

    def next_cli_process(self):
        self.dead_owners.append(dict(self.owner))
        self.owner = dict(self.owner, pid=self.owner["pid"] + 10,
                          birth=self.owner["birth"] + ":next")

    def assert_fenced(self):
        for name in ("ths-deployment-coordination.lock", "ths-deployment-maintenance"):
            path = self.case.root / "runtime" / name / "owner.json"
            self.case.assertTrue(path.is_file(), "command without durable " + name)
        self.case.assertEqual(self.processes["supervisors"], [])
        self.case.assertEqual(self.processes["guards"], [])
        self.case.assertEqual(self.processes["workers"], [])
        self.case.assertFalse(self.processes["launchd_present"])

    def run(self, argv):
        argv = [str(value) for value in argv]
        self.assert_fenced()
        case = self.case
        trust = case.stage.trust
        state = case.reader.read_state(case.root)
        if len(argv) > 1 and argv[1] == "authorize-platform-upgrade":
            self.case.assertIn("--json", argv)
            self.case.assertEqual(db_fixture.sha(Path(argv[0]).read_bytes()), trust["validator_sha256"])
            self.case.assertEqual(stat.S_IMODE(Path(argv[0]).stat().st_mode), 0o500)
            self.case.assertIn("verify:" + trust["validator_sha256"], self.calls)
            # Check flags by position independently of their ordering.
            for flag, expected in (("--run-id", db_fixture.RUN),
                                   ("--outbox", str(case.db.outbox)),
                                   ("--config", str(case.root / "config/ths_002256_sim.yaml"))):
                self.case.assertEqual(argv[argv.index(flag) + 1], expected)
            for flag, key in (("--target-binary", "candidate_sha256"),
                              ("--certification-report", "certificate_sha256")):
                self.case.assertEqual(db_fixture.sha(Path(argv[argv.index(flag)+1]).read_bytes()), trust[key])
            reservation = json.loads(case.reserve_path.read_bytes())
            case.assertEqual(reservation["index"], case.plan.index_sha256)
            case.assertEqual(reservation["baseline"], case.baseline)
            case.assertIn(case.reserve_path.parent.stat().st_ino, case.synced_dirs,
                          "reserve directory not fsynced before authorization")
            case.assertIn((case.root / "runtime").stat().st_ino, case.synced_dirs,
                          "reserve parent not fsynced before authorization")
            case.assertIsNone(state["pending"])
            case.assertEqual(state["head"], case.baseline["head"])
            if self.failure == "before_authorize":
                self.failure = None
                raise ReceiptLost("before_authorize")
            payload = dict(upgrade_id="fixture-rust-generated-upgrade", from_platform_sha256=trust["core_sha256"],
                to_platform_sha256=trust["candidate_sha256"], target_binary_sha256=trust["candidate_sha256"],
                expected_head_sequence=state["head"], algorithm_contract_sha256=db_fixture.sha(b"algorithm"),
                config_content_sha256=db_fixture.sha(b"config"), reason_code="COMPLETION_GATE_FIX",
                certification_evidence_sha256=trust["certificate_sha256"],
                operator="fixture", authorization_kind="LOCAL_OPERATOR_EXPLICIT")
            case.db.append("PLATFORM_UPGRADE_AUTHORIZED", payload)
            action = "authorize"
        elif argv[-1] in ("--activate-platform-upgrade-only", "--bind-execution-identity-only"):
            case.assertEqual(argv[:-1], [str(case.root / "bin/gridedge_ths_live"), *case.worker_args])
            case.assertEqual(db_fixture.sha(Path(argv[0]).read_bytes()), trust["candidate_sha256"])
            case.assertIn("verify:" + trust["candidate_sha256"], self.calls)
            if argv[-1] == "--activate-platform-upgrade-only":
                case.assertEqual(state["pending"], trust["candidate_sha256"])
                authorization = state["authorization"]
                payload = dict(upgrade_id=authorization["upgrade_id"], authorization_event_id=state["authorization_event_id"],
                    authorization_sequence=state["authorization_sequence"], validated_through_sequence=state["head"],
                    from_platform_sha256=trust["core_sha256"], to_platform_sha256=trust["candidate_sha256"],
                    observed_platform_sha256=trust["candidate_sha256"], paper_reconciled=True)
                case.db.append("PLATFORM_UPGRADE_ACTIVATED", payload)
                action = "activate"
            else:
                case.assertEqual(state["effective"], trust["candidate_sha256"])
                case.assertEqual(state["binding_revision"], 1)
                identity = db_fixture.encoded(dict(case.binding_identity, platform_sha256=trust["candidate_sha256"]))
                payload = {}
                with db_fixture.fixture_database(case.db.outbox) as db:
                    db.execute("INSERT INTO remote_adapter_binding_history VALUES(2,?,?,?,?,?,?,?)", (
                        db_fixture.INSTANCE, db_fixture.RUN, db_fixture.sha(identity.encode()), identity,
                        trust["binding_sha256"], "2026-09-14T19:35:01", "PLATFORM_UPGRADE"))
                    db.execute("UPDATE outbox_metadata SET cursor=?", (state["head"],))
                action = "bind"
        else:
            raise AssertionError("unreviewed command path: " + repr(argv))
        self.calls.append(action)
        if self.failure == action:
            self.failure = None
            raise ReceiptLost(action)
        return payload

    def start_supervisor(self, path):
        self.assert_fenced()
        case = self.case
        case.assertEqual(Path(path), case.root / "bin/run_session_supervisor.sh")
        state = case.reader.read_state(case.root)
        case.assertEqual(state["effective"], case.stage.trust["candidate_sha256"])
        case.assertEqual(state["binding_platform"], state["effective"])
        case.assertEqual(state["head"], state["cursor"])
        case.assertIsNone(state["pending"])
        case.assertTrue(state["terminal_ok"] and state["integrity_ok"])
        case.assertEqual(case.breaker.read_bytes(), case.breaker_bytes)
        for name, value in case.plan.targets.items():
            target = case.root / ("config" if name.endswith(".json") else "bin") / name
            case.assertEqual(target.read_bytes(), value)
        self.processes["supervisors"] = [dict(self.old_supervisor, pid=45100,
            birth="fixture-new-supervisor", manifest_sha256=case.plan.target_shas["session-supervisor-manifest.json"])]
        self.calls.append("start")
        if self.failure == "start":
            self.failure = None
            raise ReceiptLost("start")


class PostcloseBackendTest(unittest.TestCase):
    def setUp(self):
        self.stage = stage_fixture.CoreIdentityMigrationTest()
        self.stage.setUp()
        self.addCleanup(self.stage.doCleanups)
        self.db = db_fixture.PostcloseReaderTest()
        self.db.setUp()
        self.addCleanup(self.db.doCleanups)
        self.root = self.stage.root
        (self.root / "runtime").mkdir()
        for field in ("ledger", "outbox"):
            source = getattr(self.db, field)
            destination = self.root / "runtime" / source.name
            source.rename(destination)
            setattr(self.db, field, destination)
        self.db.root = self.root
        self.binding_identity = dict(schema="gridedge.ths-remote-execution-identity.v1", adapter="ANDROID",
                                     platform_sha256=self.stage.trust["core_sha256"])
        identity = db_fixture.encoded(self.binding_identity)
        identity_sha = db_fixture.sha(identity.encode())
        with db_fixture.fixture_database(self.db.ledger) as db:
            db.execute("UPDATE events SET payload=? WHERE sequence_number=2", (
                db_fixture.encoded({"platform_sha256": self.stage.trust["core_sha256"]}),))
        with db_fixture.fixture_database(self.db.outbox) as db:
            for table in ("remote_adapter_binding", "remote_adapter_binding_history"):
                db.execute(f"UPDATE {table} SET identity_sha256=?,identity_json=?", (identity_sha, identity))
        self.stage.trust.update(binding_revision=1, binding_sha256=identity_sha)
        self.worker_args = ["--config", str(self.root / "config/ths_002256_sim.yaml"),
                            "--run-id", db_fixture.RUN, "--outbox", str(self.db.outbox),
                            "--simulation-adapter", "android"]
        plist = self.stage.home / "Library/LaunchAgents/com.gridedge.ths-sim.plist"
        plist.write_bytes(plistlib.dumps({"Label": "com.gridedge.ths-sim",
            "ProgramArguments": [str(self.root / "bin/run_ths_android_sim.sh"), *self.worker_args]}))
        self.stage.baseline[str(plist)] = db_fixture.sha(plist.read_bytes())
        self.stage.manifest.write_text(json.dumps(self.stage.document, sort_keys=True))
        old_anchor = self.stage.trust["manifest_sha256"]
        new_anchor = db_fixture.sha(self.stage.manifest.read_bytes())
        self.stage.launcher.write_bytes(self.stage.launcher.read_bytes().replace(old_anchor.encode(), new_anchor.encode()))
        self.stage.trust.update(manifest_sha256=new_anchor, launcher_sha256=db_fixture.sha(self.stage.launcher.read_bytes()))
        self.stage.installed_bytes.update({"session-supervisor-manifest.json": self.stage.manifest.read_bytes(),
                                          "run_session_supervisor.sh": self.stage.launcher.read_bytes()})
        self.plan = self.stage.prepare()
        self.reader = importlib.import_module("postclose_core_migration")
        self.backend_type = self.reader.PostCloseBackend
        self.breaker = self.root / "runtime/android-runner-failures"
        self.breaker_bytes = b"2026-09-14 2\n"
        self.breaker.write_bytes(self.breaker_bytes)
        self.reserve_path = self.root / "runtime" / ("core-migration-" + self.plan.index_sha256) / "reserve.json"
        self.baseline = dict(head=3, source_database_instance_id=db_fixture.INSTANCE,
            effective=self.stage.trust["core_sha256"], binding_revision=1,
            binding_sha256=identity_sha, breaker=db_fixture.sha(self.breaker_bytes))
        self.host = FixtureHost(self)
        self.synced_dirs = []
        original_fsync = os.fsync
        def fsync(fd):
            info = os.fstat(fd)
            if stat.S_ISDIR(info.st_mode):
                self.synced_dirs.append(info.st_ino)
            return original_fsync(fd)
        patcher = patch("os.fsync", side_effect=fsync)
        patcher.start()
        self.addCleanup(patcher.stop)

    def execute(self):
        backend = self.backend_type(self.plan, self.host)
        return self.stage.subject.execute_migration(self.plan, backend)

    def mutations(self):
        return [call for call in self.host.calls if not call.startswith("verify:")]

    def assert_no_authorization(self):
        self.assertFalse(any(call in self.host.calls for call in ("authorize", "activate", "bind", "start")))
        self.assertEqual(self.reader.read_state(self.root)["head"], 3)
        self.assertEqual(self.breaker.read_bytes(), self.breaker_bytes)

    def test_real_files_sqlite_and_reserve_precede_one_complete_start(self):
        self.assertEqual(self.execute(), "COMPLETE")
        self.assertEqual([x for x in self.host.calls if x in ("authorize", "activate", "bind", "start")],
                         ["authorize", "activate", "bind", "start"])
        self.assertEqual(json.loads(self.reserve_path.read_bytes())["baseline"], self.baseline)
        self.assertFalse((self.root / "runtime/ths-deployment-maintenance").exists())

    def test_each_committed_command_response_loss_recovers_without_duplicate_fact(self):
        # Separate test fixture per boundary to avoid carrying completed phase.
        for action in ("authorize", "activate", "bind", "start"):
            with self.subTest(action=action):
                case = PostcloseBackendTest()
                case.setUp()
                try:
                    case.host.failure = action
                    with case.assertRaises(ReceiptLost):
                        case.execute()
                    reserved = case.reserve_path.read_bytes()
                    case.assertTrue((case.root / "runtime/ths-deployment-maintenance").exists())
                    case.host.next_cli_process()
                    case.execute()
                    case.assertEqual(case.reserve_path.read_bytes(), reserved)
                    for name in ("authorize", "activate", "bind", "start"):
                        case.assertEqual(case.host.calls.count(name), 1, "duplicate " + name)
                    case.assertEqual(case.reader.read_state(case.root)["head"], 5)
                finally:
                    case.doCleanups()

    def test_fresh_window_boundaries_reject_before_mutation(self):
        for hour, minute in ((9, 25), (15, 59), (22, 0), (23, 59)):
            with self.subTest(hour=hour, minute=minute):
                self.host.current_time = self.host.current_time.replace(hour=hour, minute=minute)
                with self.assertRaises(ValueError):
                    self.execute()
                self.assertNotIn("quiesce", self.host.calls)
                self.assert_no_authorization()

    def test_naive_clock_is_rejected(self):
        self.host.current_time = self.host.current_time.replace(tzinfo=None)
        with self.assertRaises(ValueError):
            self.execute()
        self.assert_no_authorization()

    def test_false_signature_result_before_quiesce_is_not_success(self):
        for filename in ("gridedge_ths_live", "validator"):
            with self.subTest(filename=filename):
                original = self.host.verify_signature
                target = self.plan.stage / filename
                def verify(path, digest):
                    result = original(path, digest)
                    return False if Path(path) == target else result
                with patch.object(self.host, "verify_signature", side_effect=verify):
                    with self.assertRaises(ValueError):
                        self.execute()
                self.assert_no_authorization()
                self.assertNotIn("quiesce", self.host.calls)
                self.host.next_cli_process()

    def test_false_installed_signature_preserves_pending_and_never_activates(self):
        original = self.host.verify_signature
        target = self.root / "bin/gridedge_ths_live"
        def verify(path, digest):
            result = original(path, digest)
            return False if Path(path) == target else result
        with patch.object(self.host, "verify_signature", side_effect=verify):
            with self.assertRaises(ValueError):
                self.execute()
        state = self.reader.read_state(self.root)
        self.assertEqual(state["pending"], self.stage.trust["candidate_sha256"])
        self.assertEqual(state["effective"], self.stage.trust["core_sha256"])
        self.assertEqual(self.host.calls.count("authorize"), 1)
        self.assertNotIn("activate", self.host.calls)
        self.assertNotIn("start", self.host.calls)
        self.assertTrue((self.root / "runtime/ths-deployment-maintenance").is_dir())

    def test_surviving_supervisor_blocks_authorize_and_publication(self):
        self.host.sticky_supervisor = True
        with self.assertRaises(ValueError):
            self.execute()
        self.assert_no_authorization()

    def test_guard_worker_launchd_wrong_socket_session_or_competing_supervisor_reject(self):
        original = copy.deepcopy(self.host.processes)
        mutations = [("guards", [self.host.owner]), ("workers", [self.host.owner]),
            ("launchd_present", True), ("supervisor_socket", "gridedge_codex"),
            ("supervisor_session", "0"), ("supervisors", [self.host.old_supervisor] * 2)]
        for key, value in mutations:
            with self.subTest(key=key):
                self.host.processes = dict(copy.deepcopy(original), **{key: value})
                with self.assertRaises(ValueError):
                    self.execute()
                self.assert_no_authorization()
                self.assertNotIn("quiesce", self.host.calls)
                self.host.next_cli_process()

    def test_fresh_wrong_supervisor_manifest_is_not_quiesced(self):
        self.host.processes["supervisors"][0]["manifest_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.execute()
        self.assert_no_authorization()
        self.assertNotIn("quiesce", self.host.calls)

    def test_bad_owner_identity_is_rejected(self):
        self.host.owner["birth"] = ""
        with self.assertRaises(ValueError):
            self.execute()
        self.assert_no_authorization()

    def test_pending_rollforward_outside_fresh_window(self):
        self.host.failure = "authorize"
        with self.assertRaises(ReceiptLost):
            self.execute()
        self.host.next_cli_process()
        self.host.current_time = self.host.current_time.replace(hour=22, minute=1)
        self.assertEqual(self.execute(), "COMPLETE")
        self.assertEqual(self.host.calls.count("authorize"), 1)

    def test_reservation_without_authorization_cannot_open_new_transaction_outside_window(self):
        self.host.failure = "before_authorize"
        with self.assertRaises(ReceiptLost):
            self.execute()
        self.assertTrue(self.reserve_path.is_file())
        self.assertIsNone(self.reader.read_state(self.root)["pending"])
        self.host.next_cli_process()
        self.host.current_time = self.host.current_time.replace(day=15, hour=9, minute=25)
        with self.assertRaises(ValueError):
            self.execute()
        self.assert_no_authorization()

    def test_pending_retry_rejects_rebaselined_breaker(self):
        self.host.failure = "authorize"
        with self.assertRaises(ReceiptLost):
            self.execute()
        self.host.next_cli_process()
        self.breaker.write_bytes(b"2026-09-14 0\n")
        before = self.mutations()
        with self.assertRaises(ValueError):
            self.execute()
        self.assertEqual(self.mutations(), before)
        self.assertEqual(self.reader.read_state(self.root)["effective"], self.stage.trust["core_sha256"])

    def test_completed_retry_requires_new_supervisor_manifest_not_merely_count_one(self):
        self.execute()
        self.host.next_cli_process()
        self.host.processes["supervisors"][0]["manifest_sha256"] = self.stage.trust["manifest_sha256"]
        before = self.mutations()
        with self.assertRaises(ValueError):
            self.execute()
        self.assertEqual(self.mutations(), before)

    def test_completed_retry_is_true_noop(self):
        self.execute()
        self.host.next_cli_process()
        before = self.mutations()
        self.assertEqual(self.execute(), "ALREADY_COMPLETE")
        self.assertEqual(self.mutations(), before)


if __name__ == "__main__":
    unittest.main()
