"""Independent stage identity regressions; every installation is a /tmp fixture."""

import builtins
from contextlib import ExitStack
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import uuid


SPEC = importlib.util.spec_from_file_location(
    "stage_identity_regression_subject", Path(__file__).with_name("stage_session_supervisor.py")
)
STAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STAGE)


def sha(value):
    return hashlib.sha256(value).hexdigest()


class StageIdentityRegression(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/tmp", prefix="gridedge-identity-fixture-")
        self.addCleanup(self.temporary.cleanup)
        fixture = Path(self.temporary.name)
        self.home = fixture / "home"
        self.root = self.home / "Library/Application Support/GridEdge-T"
        self.sdk = self.home / "Library/Android/sdk"
        self.repo = fixture / "repo"
        self.stage = Path("/tmp") / ("gridedge-supervisor-stage-identity-" + str(uuid.uuid4()))
        self.addCleanup(lambda: shutil.rmtree(self.stage) if self.stage.exists() else None)
        self.companions = [self.root / "bin" / name for name in (
            "start_ths_trusted_session.sh", "market_raw_repair.py",
            "market_ingestor.py", "gridedge_market_replay",
        )]
        self.baseline = [self.root / "bin" / name for name in (
            "gridedge_ths_live", "run_ths_android_sim.sh", "run_ths_trusted_session_guard.sh",
        )] + [self.root / "config/ths_002256_sim.yaml",
              self.home / "Library/LaunchAgents/com.gridedge.ths-sim.plist",
              self.sdk / "platform-tools/adb", self.sdk / "emulator/emulator"]
        self.supervisor = self.root / "bin/session_supervisor.py"
        self.reviewed = {}
        for path in self.companions + self.baseline + [self.supervisor]:
            path.parent.mkdir(parents=True, exist_ok=True)
            value = ("reviewed " + path.name).encode()
            path.write_bytes(value)
            self.reviewed[str(path)] = sha(value)
        self.candidate = self.repo / "deploy/ops/session_supervisor.py"
        self.candidate.parent.mkdir(parents=True)
        self.candidate.write_bytes(b"independently reviewed new supervisor")
        self.manifest = self.root / "config/session-supervisor-manifest.json"
        self.old_manifest = dict(
            schema="gridedge.session-supervisor.v1", root=str(self.root),
            run_id="ths-002256-20260819-grid15-opening-v1", sdk=str(self.sdk),
            market_host="192.168.1.201", starter=str(self.companions[0]),
            chrome_profile="Default", reviewed_url="https://quote.eastmoney.com/f1.html?newcode=0.002256",
            calendar="SSE_2026_NOTICE_45", files=self.reviewed,
        )
        self.manifest.write_text(json.dumps(self.old_manifest, sort_keys=True))
        self.anchor = sha(self.manifest.read_bytes())
        (self.root / "bin/run_session_supervisor.sh").write_text(
            "#!/bin/sh\nexec python3 '" + str(self.supervisor) + "' --manifest '" +
            str(self.manifest) + "' --manifest-sha256 " + self.anchor + "\n"
        )

    def run_stage(self, anchor=None):
        with patch.object(STAGE, "ROOT", self.root), patch.object(STAGE, "REPO", self.repo), \
             patch.object(Path, "home", return_value=self.home), patch.object(STAGE.sys, "argv", [
                 "stage_session_supervisor.py", str(self.stage),
                 "--baseline-manifest-sha256", anchor if anchor is not None else self.anchor,
             ]), patch("sys.stdout", new=io.StringIO()):
            STAGE.main()

    def test_wrong_reviewed_manifest_sha_is_rejected(self):
        with self.assertRaises(ValueError):
            self.run_stage("0" * 64)

    def test_manifest_contents_cannot_change_under_the_reviewed_sha(self):
        changed = dict(self.old_manifest, chrome_profile="unreviewed profile")
        self.manifest.write_text(json.dumps(changed, sort_keys=True))
        with self.assertRaises(ValueError):
            self.run_stage()
        self.assertFalse(self.stage.exists())

    def test_manifest_metadata_is_fixed_even_with_matching_sha(self):
        for key, value in (("root", str(self.root.parent)), ("chrome_profile", "Profile 9"),
                           ("reviewed_url", "https://example.invalid/"), ("extra", "metadata")):
            with self.subTest(field=key):
                changed = dict(self.old_manifest, **{key: value})
                self.manifest.write_text(json.dumps(changed, sort_keys=True))
                with self.assertRaises(ValueError):
                    self.run_stage(sha(self.manifest.read_bytes()))
                self.assertFalse(self.stage.exists())

    def test_manifest_file_allowlist_cannot_shrink_or_expand(self):
        for operation in ("remove", "add"):
            with self.subTest(operation=operation):
                files = dict(self.reviewed)
                if operation == "remove":
                    del files[str(self.companions[0])]
                else:
                    files[str(self.root / "bin/unreviewed-helper")] = "0" * 64
                self.manifest.write_text(json.dumps(dict(self.old_manifest, files=files), sort_keys=True))
                with self.assertRaises(ValueError):
                    self.run_stage(sha(self.manifest.read_bytes()))
                self.assertFalse(self.stage.exists())

    def test_symlink_manifest_baseline_companion_and_candidate_are_rejected(self):
        for path in [self.manifest, self.baseline[0], self.companions[0], self.candidate]:
            with self.subTest(path=path.name):
                backup = path.with_suffix(path.suffix + ".regular")
                path.rename(backup)
                path.symlink_to(backup)
                try:
                    with self.assertRaises((ValueError, OSError)):
                        self.run_stage()
                    self.assertFalse(self.stage.exists())
                finally:
                    path.unlink()
                    backup.rename(path)

    def test_missing_explicit_anchor_is_rejected_before_staging(self):
        with patch.object(STAGE, "ROOT", self.root), patch.object(STAGE, "REPO", self.repo), \
             patch.object(Path, "home", return_value=self.home), patch.object(STAGE.sys, "argv", [
                 "stage_session_supervisor.py", str(self.stage),
             ]), patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit) as rejected:
                STAGE.main()
        self.assertEqual(rejected.exception.code, 2)
        self.assertFalse(self.stage.exists())

    def test_companion_drift_is_not_reauthorized(self):
        for path in self.companions:
            with self.subTest(companion=path.name):
                reviewed_bytes = path.read_bytes()
                path.write_bytes(b"unreviewed companion drift")
                try:
                    with self.assertRaises(ValueError):
                        self.run_stage()
                finally:
                    path.write_bytes(reviewed_bytes)
                    if self.stage.exists():
                        shutil.rmtree(self.stage)

    def test_baseline_and_old_supervisor_drift_are_rejected(self):
        for path in self.baseline + [self.supervisor]:
            with self.subTest(baseline=path.name):
                reviewed_bytes = path.read_bytes()
                path.write_bytes(b"unreviewed baseline drift")
                try:
                    with self.assertRaises(ValueError):
                        self.run_stage()
                finally:
                    path.write_bytes(reviewed_bytes)
                    if self.stage.exists():
                        shutil.rmtree(self.stage)

    def test_replaced_companion_during_read_cannot_enter_stage(self):
        # Atomically substitute a different inode only while the source is
        # opened, then restore the reviewed pathname before hashing can occur.
        # Intercept standard file-open boundaries, not an implementation helper.
        target = self.companions[1]
        backup = target.with_suffix(".reviewed-backup")
        original_io_open = io.open
        injected = []

        def intercept(original):
            def opened(path, *args, **kwargs):
                if not injected and isinstance(path, (str, bytes, os.PathLike)) and Path(path) == target:
                    injected.append(True)
                    target.rename(backup)
                    with original_io_open(target, "wb") as output:
                        output.write(b"transient unreviewed replacement")
                    try:
                        return original(path, *args, **kwargs)
                    finally:
                        target.unlink()
                        backup.rename(target)
                return original(path, *args, **kwargs)
            return opened

        with ExitStack() as stack:
            stack.enter_context(patch("builtins.open", side_effect=intercept(builtins.open)))
            stack.enter_context(patch("io.open", side_effect=intercept(io.open)))
            stack.enter_context(patch("os.open", side_effect=intercept(os.open)))
            try:
                self.run_stage()
            except ValueError:
                pass
            else:
                staged = self.stage / target.name
                self.assertEqual(sha(staged.read_bytes()), self.reviewed[str(target)],
                                 "stage blessed bytes absent from the reviewed baseline")
        self.assertTrue(injected, "test must exercise a real source-read boundary")
        self.assertEqual(sha(target.read_bytes()), self.reviewed[str(target)])

    def test_valid_supervisor_only_upgrade_retains_all_other_identities(self):
        self.run_stage()
        result = json.loads((self.stage / "session-supervisor-manifest.json").read_bytes())
        expected_files = dict(self.reviewed)
        expected_files[str(self.supervisor)] = sha(self.candidate.read_bytes())
        self.assertEqual(result["files"], expected_files)
        self.assertEqual({key: value for key, value in result.items() if key != "files"},
                         {key: value for key, value in self.old_manifest.items() if key != "files"})
        for path in self.companions:
            self.assertEqual(sha((self.stage / path.name).read_bytes()), self.reviewed[str(path)])
        self.assertEqual(sha(self.manifest.read_bytes()), self.anchor)


if __name__ == "__main__":
    unittest.main()
