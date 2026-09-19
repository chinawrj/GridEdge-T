"""Independent migration contract tests. All files and owners are isolated fakes.

This suite is not production deployment authorization or a native-adapter test.
"""
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import sys
import tempfile
import unittest


def sha(value):
    return hashlib.sha256(value).hexdigest()


class InjectedFailure(RuntimeError):
    pass


class FakeBackend:
    """Persistent facts survive failures; assertions independently guard start."""
    def __init__(self, case, fail=None):
        self.case = case
        self.fail = fail
        self.failed = False
        self.locked = False
        self.calls = []
        self.facts = []
        self.files = dict(case.installed_bytes)
        self.state = {
            "effective": case.trust["core_sha256"],
            "binding_revision": case.trust["binding_revision"],
            "binding_sha256": case.trust["binding_sha256"],
            "binding_platform": case.trust["core_sha256"],
            "binding_previous_sha256": None, "migration_index": None,
            "migration_breaker": None,
            "integrity_ok": True, "terminal_ok": True, "maintenance": False,
            "pending": None, "core": case.trust["core_sha256"],
            "manifest": case.trust["manifest_sha256"],
            "launcher": case.trust["launcher_sha256"],
            "breaker": sha(b"unchanged daily circuit breaker"),
            "supervisors": 1, "guards": 0, "workers": 0,
            "launchd_present": False, "head": 2448, "cursor": 2448,
        }
        self.breaker = self.state["breaker"]

    @contextmanager
    def exclusive(self):
        assert not self.locked, "nested/competing ownership"
        self.locked = True
        self.state["maintenance"] = True
        try:
            yield
        except BaseException:
            # A native adapter must durably fence pending/mixed identities.
            raise
        else:
            self.state["maintenance"] = False
        finally:
            self.locked = False

    def snapshot(self):
        assert self.locked, "snapshot must be inside exclusive ownership"
        return copy.deepcopy(self.state)

    def checkpoint(self, name, when):
        assert self.locked, "mutation outside exclusive ownership"
        if self.fail == (name, when) and not self.failed:
            self.failed = True
            raise InjectedFailure(f"{name}:{when}")

    def mutation(self, name, operation):
        self.checkpoint(name, "before")
        operation()
        self.calls.append(name)
        self.checkpoint(name, "after")

    def quiesce(self):
        def action():
            self.state.update(supervisors=0, guards=0, workers=0, launchd_present=False)
        self.mutation("quiesce", action)

    def publish(self, name, value):
        def action():
            assert name in self.case.target_bytes, "unreviewed publication target"
            assert value == self.case.target_bytes[name], "non-frozen published bytes"
            assert not any(self.state[k] for k in ("supervisors", "guards", "workers", "launchd_present"))
            self.files[name] = value
            self.state[{"gridedge_ths_live": "core", "session-supervisor-manifest.json": "manifest",
                        "run_session_supervisor.sh": "launcher"}[name]] = sha(value)
        self.mutation("publish:" + name, action)

    def authorize(self):
        def action():
            assert self.state["pending"] is None
            assert self.state["effective"] == self.case.trust["core_sha256"]
            assert "authorize" not in self.facts, "duplicate authorization"
            self.state["pending"] = self.case.trust["candidate_sha256"]
            self.state["migration_index"] = self.case.index
            self.state["migration_breaker"] = self.state["breaker"]
            self.state["head"] += 1
            self.facts.append("authorize")
        self.mutation("authorize", action)

    def activate(self):
        def action():
            assert self.state["pending"] == self.case.trust["candidate_sha256"]
            assert self.state["core"] == self.case.trust["candidate_sha256"]
            assert "activate" not in self.facts, "duplicate activation"
            self.state.update(effective=self.case.trust["candidate_sha256"], pending=None)
            self.state["head"] += 1
            self.facts.append("activate")
        self.mutation("activate", action)

    def bind(self):
        def action():
            assert self.state["effective"] == self.case.trust["candidate_sha256"]
            assert "bind" not in self.facts, "duplicate binding"
            self.state.update(binding_platform=self.case.trust["candidate_sha256"],
                              binding_revision=self.case.trust["binding_revision"] + 1,
                              binding_previous_sha256=self.case.trust["binding_sha256"],
                              binding_sha256=sha(b"fixture new append-only binding"))
            self.facts.append("bind")
        self.mutation("bind", action)

    def sync(self):
        self.mutation("sync", lambda: self.state.update(cursor=self.state["head"]))

    def start(self):
        def action():
            target = self.case.trust["candidate_sha256"]
            assert self.state["pending"] is None
            assert self.state["effective"] == self.state["core"] == self.state["binding_platform"] == target
            assert self.state["manifest"] == sha(self.case.target_bytes["session-supervisor-manifest.json"])
            assert self.state["launcher"] == sha(self.case.target_bytes["run_session_supervisor.sh"])
            assert self.state["cursor"] == self.state["head"]
            assert not any(self.state[k] for k in ("supervisors", "guards", "workers", "launchd_present"))
            assert self.state["breaker"] == self.breaker
            self.state["supervisors"] = 1
        self.mutation("start", action)


class CoreIdentityMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gridedge-core-migration-test-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.fixture = Path(self.tmp.name)
        self.home = self.fixture / "home"
        self.root = self.home / "Library/Application Support/GridEdge-T"
        self.sdk = self.home / "Library/Android/sdk"
        self.stage = self.fixture / "stage"
        paths = [self.root / "bin" / name for name in (
            "gridedge_ths_live", "run_ths_android_sim.sh", "run_ths_trusted_session_guard.sh",
            "start_ths_trusted_session.sh", "session_supervisor.py", "market_raw_repair.py",
            "market_ingestor.py", "gridedge_market_replay",
        )] + [self.root / "config/ths_002256_sim.yaml",
              self.home / "Library/LaunchAgents/com.gridedge.ths-sim.plist",
              self.sdk / "platform-tools/adb", self.sdk / "emulator/emulator"]
        self.baseline = {}
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            value = ("reviewed fixture " + path.name).encode()
            path.write_bytes(value)
            self.baseline[str(path)] = sha(value)
        self.manifest = self.root / "config/session-supervisor-manifest.json"
        self.document = dict(schema="gridedge.session-supervisor.v1", root=str(self.root),
            run_id="ths-002256-20260819-grid15-opening-v1", sdk=str(self.sdk), market_host="192.168.1.201",
            starter=str(self.root / "bin/start_ths_trusted_session.sh"), chrome_profile="Default",
            reviewed_url="https://quote.eastmoney.com/f1.html?newcode=0.002256", calendar="SSE_2026_NOTICE_45",
            files=self.baseline)
        self.manifest.write_text(json.dumps(self.document, sort_keys=True))
        self.launcher = self.root / "bin/run_session_supervisor.sh"
        self.launcher.write_text("#!/bin/sh\nset -eu\nexec " + shlex.join([
            "/opt/homebrew/bin/python3", str(self.root / "bin/session_supervisor.py"),
            "--manifest", str(self.manifest), "--manifest-sha256", sha(self.manifest.read_bytes())]) + "\n")
        self.candidate = self.fixture / "candidate"
        self.validator = self.fixture / "validator"
        self.certificate = self.fixture / "certificate.json"
        self.candidate.write_bytes(b"reviewed signed candidate fixture")
        self.validator.write_bytes(b"reviewed validator fixture")
        self.certificate.write_text(json.dumps({
            "certification_profile_version": "GRIDEDGE_PLATFORM_UPGRADE_CERTIFICATION_V1",
            "run_id": "ths-002256-20260819-grid15-opening-v1",
            "target_binary_sha256": sha(self.candidate.read_bytes()),
            "full_rebuild_passed": True, "paper_reconciliation_passed": True,
            "outbox_v3_to_v4_passed": True, "ambiguous_fill_recovery_passed": True,
            "full_gate_passed": True, "duplicate_money_action_count": 0,
            "generated_at": "2026-09-14T19:10:00",
        }))
        self.trust = dict(manifest_sha256=sha(self.manifest.read_bytes()),
            launcher_sha256=sha(self.launcher.read_bytes()), core_sha256=self.baseline[str(paths[0])],
            binding_revision=15, binding_sha256=sha(b"reviewed baseline binding"),
            candidate_sha256=sha(self.candidate.read_bytes()), validator_sha256=sha(self.validator.read_bytes()),
            certificate_sha256=sha(self.certificate.read_bytes()))
        self.installed_bytes = {"gridedge_ths_live": paths[0].read_bytes(),
            "session-supervisor-manifest.json": self.manifest.read_bytes(),
            "run_session_supervisor.sh": self.launcher.read_bytes()}
        spec = importlib.util.spec_from_file_location("core_migration_under_test", Path(__file__).with_name("core_identity_migration.py"))
        self.subject = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.subject
        spec.loader.exec_module(self.subject)

    def freeze(self):
        return self.subject.freeze_stage(self.root, self.home, self.stage, self.trust,
                                         self.candidate, self.validator, self.certificate)

    def prepare(self):
        index = self.freeze()
        self.index = index
        self.target_bytes = {name: (self.stage / name).read_bytes() for name in self.installed_bytes}
        return self.subject.validate_stage(self.root, self.home, self.stage, self.trust, index)

    def test_freeze_changes_only_core_manifest_entry_and_launcher_anchor(self):
        self.prepare()
        candidate_manifest = json.loads(self.target_bytes["session-supervisor-manifest.json"])
        expected = copy.deepcopy(self.document)
        expected["files"][str(self.root / "bin/gridedge_ths_live")] = self.trust["candidate_sha256"]
        self.assertEqual(candidate_manifest, expected)
        self.assertEqual(self.target_bytes["gridedge_ths_live"], self.candidate.read_bytes())
        self.assertEqual(self.target_bytes["run_session_supervisor.sh"], self.installed_bytes["run_session_supervisor.sh"].replace(
            self.trust["manifest_sha256"].encode(), sha(self.target_bytes["session-supervisor-manifest.json"]).encode()))
        for name, value in self.installed_bytes.items():
            target = self.manifest if name == self.manifest.name else self.root / "bin" / name
            self.assertEqual(target.read_bytes(), value, "freeze mutated installation")

    def test_wrong_trust_hashes_reject_before_stage(self):
        for key in ("manifest_sha256", "launcher_sha256", "core_sha256", "candidate_sha256", "validator_sha256", "certificate_sha256"):
            with self.subTest(key=key):
                original = self.trust[key]
                self.trust[key] = "0" * 64
                try:
                    with self.assertRaises((ValueError, OSError)):
                        self.freeze()
                    self.assertFalse(self.stage.exists())
                finally:
                    self.trust[key] = original

    def test_rehashed_wrong_metadata_or_allowlist_still_rejects(self):
        for change in ("metadata", "extra_file", "missing_file"):
            with self.subTest(change=change):
                changed = copy.deepcopy(self.document)
                if change == "metadata":
                    changed["chrome_profile"] = "Profile 9"
                elif change == "extra_file":
                    changed["files"][str(self.root / "unreviewed")] = "0" * 64
                else:
                    del changed["files"][str(self.root / "bin/market_raw_repair.py")]
                self.manifest.write_text(json.dumps(changed, sort_keys=True))
                self.trust["manifest_sha256"] = sha(self.manifest.read_bytes())
                self.launcher.write_bytes(self.installed_bytes["run_session_supervisor.sh"].replace(
                    sha(self.installed_bytes["session-supervisor-manifest.json"]).encode(), self.trust["manifest_sha256"].encode()))
                self.trust["launcher_sha256"] = sha(self.launcher.read_bytes())
                with self.assertRaises((ValueError, OSError)):
                    self.freeze()
                self.assertFalse(self.stage.exists())

    def test_every_companion_drift_rejects(self):
        for name in self.baseline:
            path = Path(name)
            before = path.read_bytes()
            with self.subTest(path=path.name):
                path.write_bytes(b"unreviewed drift")
                try:
                    with self.assertRaises((ValueError, OSError)):
                        self.freeze()
                    self.assertFalse(self.stage.exists())
                finally:
                    path.write_bytes(before)

    def test_stage_index_and_published_bytes_cannot_be_replaced(self):
        index = self.freeze()
        with self.assertRaises((ValueError, OSError)):
            self.subject.validate_stage(self.root, self.home, self.stage, self.trust, "0" * 64)
        for name in self.installed_bytes:
            with self.subTest(name=name):
                path = self.stage / name
                value = path.read_bytes()
                path.chmod(0o600)
                path.write_bytes(b"substitution")
                try:
                    with self.assertRaises((ValueError, OSError)):
                        self.subject.validate_stage(self.root, self.home, self.stage, self.trust, index)
                finally:
                    path.write_bytes(value)

    def test_success_has_one_owner_exact_targets_and_unique_facts(self):
        plan = self.prepare()
        backend = FakeBackend(self)
        self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.facts, ["authorize", "activate", "bind"])
        self.assertEqual(backend.state["supervisors"], 1)
        self.assertEqual(backend.state["breaker"], backend.breaker)
        self.assertEqual(backend.files, self.target_bytes)
        self.assertFalse(backend.locked)

    def test_each_transition_failure_is_resumable_without_duplicate_facts(self):
        plan = self.prepare()
        actions = ["quiesce", "authorize", "publish:gridedge_ths_live",
                   "publish:session-supervisor-manifest.json", "publish:run_session_supervisor.sh",
                   "activate", "bind", "sync", "start"]
        for action in actions:
            for when in ("before", "after"):
                with self.subTest(action=action, when=when):
                    backend = FakeBackend(self, fail=(action, when))
                    with self.assertRaises(InjectedFailure):
                        self.subject.execute_migration(plan, backend)
                    self.assertTrue(backend.failed, "injection must reach its actual operation")
                    self.assertTrue(backend.state["maintenance"], "failure must retain maintenance fence")
                    if backend.state["supervisors"]:
                        self.assertTrue((not backend.facts and backend.files == self.installed_bytes) or
                                        (backend.facts == ["authorize", "activate", "bind"] and backend.files == self.target_bytes))
                    self.subject.execute_migration(plan, backend)
                    self.assertEqual(backend.facts, ["authorize", "activate", "bind"])
                    self.assertEqual(backend.state["supervisors"], 1)
                    self.assertEqual(backend.state["breaker"], backend.breaker)
                    self.assertEqual(backend.files, self.target_bytes)
                    self.assertFalse(backend.locked)

    def test_drift_before_exclusive_precheck_causes_zero_mutations(self):
        plan = self.prepare()
        for field, value in (("effective", "0" * 64), ("binding_sha256", "0" * 64),
                             ("binding_revision", 99), ("core", "0" * 64),
                             ("manifest", "0" * 64), ("launcher", "0" * 64)):
            with self.subTest(field=field):
                backend = FakeBackend(self)
                backend.state[field] = value
                with self.assertRaises((ValueError, RuntimeError)):
                    self.subject.execute_migration(plan, backend)
                self.assertEqual(backend.calls, [])
                self.assertEqual(backend.facts, [])

    def test_finished_same_migration_is_noop_without_quiescing_healthy_owner(self):
        plan = self.prepare()
        backend = FakeBackend(self)
        self.subject.execute_migration(plan, backend)
        before = list(backend.calls)
        self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.calls, before, "completed retry restarted a healthy owner")
        self.assertEqual(backend.state["supervisors"], 1)

    def test_owners_or_launchd_surviving_quiescence_prevent_all_publication(self):
        plan = self.prepare()
        for field in ("supervisors", "guards", "workers", "launchd_present"):
            with self.subTest(field=field):
                backend = FakeBackend(self)
                original = backend.quiesce
                def incomplete_quiesce():
                    original()
                    backend.state[field] = 1
                backend.quiesce = incomplete_quiesce
                with self.assertRaises((ValueError, RuntimeError)):
                    self.subject.execute_migration(plan, backend)
                self.assertEqual(backend.calls, ["quiesce"])
                self.assertEqual(backend.facts, [])
                self.assertEqual(backend.files, self.installed_bytes)

    def test_integrity_terminal_or_foreign_pending_refuse_before_mutation(self):
        plan = self.prepare()
        variations = [dict(integrity_ok=False), dict(terminal_ok=False),
                      dict(pending="0" * 64), dict(binding_platform="0" * 64),
                      dict(pending=self.trust["candidate_sha256"], migration_index="0" * 64)]
        for changed in variations:
            with self.subTest(changed=changed):
                backend = FakeBackend(self)
                backend.state.update(changed)
                with self.assertRaises((ValueError, RuntimeError)):
                    self.subject.execute_migration(plan, backend)
                self.assertEqual(backend.calls, [])
                self.assertEqual(backend.facts, [])

    def test_symlink_candidate_and_companion_are_not_accepted(self):
        for path in (self.candidate, self.manifest, self.root / "bin/market_raw_repair.py"):
            with self.subTest(path=path.name):
                backup = path.with_name(path.name + ".regular")
                path.rename(backup)
                path.symlink_to(backup)
                try:
                    with self.assertRaises((ValueError, OSError)):
                        self.freeze()
                    self.assertFalse(self.stage.exists())
                finally:
                    path.unlink()
                    backup.rename(path)

    def test_matching_certificate_hash_does_not_bless_failed_or_foreign_proof(self):
        original = json.loads(self.certificate.read_bytes())
        for field, value in (("run_id", "another-run"), ("target_binary_sha256", "0" * 64),
                             ("full_gate_passed", False), ("duplicate_money_action_count", 1),
                             ("certification_profile_version", "unreviewed")):
            with self.subTest(field=field):
                self.certificate.write_text(json.dumps(dict(original, **{field: value})))
                self.trust["certificate_sha256"] = sha(self.certificate.read_bytes())
                with self.assertRaises((ValueError, OSError)):
                    self.freeze()
                self.assertFalse(self.stage.exists())

    def test_companion_change_after_freeze_invalidates_plan_before_mutation(self):
        index = self.freeze()
        companion = self.root / "bin/market_raw_repair.py"
        companion.write_bytes(b"changed after frozen stage")
        with self.assertRaises((ValueError, OSError)):
            self.subject.validate_stage(self.root, self.home, self.stage, self.trust, index)

    def test_breaker_change_during_quiesce_prevents_authorization(self):
        plan = self.prepare()
        backend = FakeBackend(self)
        original = backend.quiesce
        def changed_breaker():
            original()
            backend.state["breaker"] = sha(b"different safety state")
        backend.quiesce = changed_breaker
        with self.assertRaises((ValueError, RuntimeError)):
            self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.calls, ["quiesce"])
        self.assertEqual(backend.facts, [])

    def test_target_files_without_matching_durable_migration_are_not_adopted(self):
        plan = self.prepare()
        for name, field in (("gridedge_ths_live", "core"),
                            ("session-supervisor-manifest.json", "manifest"),
                            ("run_session_supervisor.sh", "launcher")):
            with self.subTest(field=field):
                backend = FakeBackend(self)
                backend.files[name] = self.target_bytes[name]
                backend.state[field] = sha(self.target_bytes[name])
                with self.assertRaises((ValueError, RuntimeError)):
                    self.subject.execute_migration(plan, backend)
                self.assertEqual(backend.calls, [])
                self.assertEqual(backend.facts, [])

    def test_breaker_cannot_be_rebaselined_after_authorized_attempt_failure(self):
        plan = self.prepare()
        backend = FakeBackend(self, fail=("authorize", "after"))
        with self.assertRaises(InjectedFailure):
            self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.facts, ["authorize"])
        before = list(backend.calls)
        backend.state["breaker"] = sha(b"externally reset after authorization")
        with self.assertRaises((ValueError, RuntimeError)):
            self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.calls, before, "changed breaker was adopted on retry")
        self.assertEqual(backend.facts, ["authorize"])
        self.assertEqual(backend.state["supervisors"], 0)

    def test_launcher_ahead_of_manifest_is_not_a_reachable_resume_phase(self):
        plan = self.prepare()
        backend = FakeBackend(self, fail=("activate", "after"))
        with self.assertRaises(InjectedFailure):
            self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.facts, ["authorize", "activate"])
        # Valid phases publish manifest before launcher. This combination could
        # not be produced by any single before/after interruption of that path.
        name = "run_session_supervisor.sh"
        backend.files[name] = self.target_bytes[name]
        backend.state["launcher"] = sha(self.target_bytes[name])
        before = list(backend.calls)
        with self.assertRaises((ValueError, RuntimeError)):
            self.subject.execute_migration(plan, backend)
        self.assertEqual(backend.calls, before, "unreachable mixed phase was silently repaired")
        self.assertEqual(backend.state["supervisors"], 0)


if __name__ == "__main__":
    unittest.main()
