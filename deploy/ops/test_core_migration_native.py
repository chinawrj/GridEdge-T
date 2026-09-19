"""Independent native-evidence tests; all processes/commands are simulated.

Real files are temporary fixtures. No signals, tmux changes, production reads,
code signing operations or native migration authorization occur in this suite.
"""
import copy
import errno
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shlex
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import core_migration_native as subject


def sha(value):
    return hashlib.sha256(value).hexdigest()


def reply(stdout=b"", returncode=0, stderr=b""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class NativeEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gridedge-native-evidence-test-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "installation with spaces"
        self.root.mkdir()
        self.python = self.root / "reviewed python"
        self.python.write_bytes(b"fixture interpreter bytes")
        self.anchor = sha(b"reviewed manifest")
        self.argv = [str(self.python), str(self.root / "bin/session_supervisor.py"), "--manifest",
                     str(self.root / "config/session-supervisor-manifest.json"), "--manifest-sha256", self.anchor]
        self.launcher = ("#!/bin/sh\nset -eu\nexec " + shlex.join(self.argv) + "\n").encode()
        self.observed = dict(pid=42001, birth="Mon Sep 14 08:02:12 2026", executable_sha256=sha(self.python.read_bytes()),
                             executable_inode=self.python.stat().st_ino, executable_path=str(self.python), argv=self.argv)
        self.owner = {key: self.observed[key] for key in ("pid", "birth", "executable_sha256", "executable_inode")}
        # Never initialize native ctypes or use real process discovery.
        self.probe = object.__new__(subject.MacProcessProbe)

    def inspect_with(self, **overrides):
        evidence = dict(alive=Mock(return_value=True), _birth=Mock(return_value=self.observed["birth"]),
                        _path=Mock(return_value=self.python), _argv=Mock(return_value=self.argv))
        evidence.update(overrides)
        with patch.multiple(self.probe, **evidence):
            return self.probe.inspect(self.owner["pid"])

    def test_procargs_preserves_spaces_empty_arguments_and_ignores_environment(self):
        argv = ["/Applications/Reviewed Python/python", "script with spaces.py", "", "--literal=$not-a-shell"]
        raw = struct.pack("=i", len(argv)) + b"/actual executable\0\0\0" + b"\0".join(x.encode() for x in argv) + b"\0SECRET=not-an-arg\0"
        self.assertEqual(subject.parse_procargs(raw), argv)

    def test_procargs_rejects_invalid_count_missing_executable_truncation_and_encoding(self):
        fixtures = [b"", b"\0" * 4, struct.pack("=i", 0) + b"/x\0x\0",
                    struct.pack("=i", -1) + b"/x\0x\0", struct.pack("=i", 65537) + b"/x\0x\0",
                    struct.pack("=i", 1) + b"\0x\0", struct.pack("=i", 2) + b"/x\0x\0",
                    struct.pack("=i", 1) + b"/x\0unterminated", struct.pack("=i", 1) + b"/x\0\xff\0"]
        for raw in fixtures:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_procargs(raw)

    def test_alive_permission_denial_is_unknown_not_absence(self):
        with patch.object(subject.os, "kill", side_effect=PermissionError):
            with self.assertRaises(ValueError):
                self.probe.alive(42001)
        with patch.object(subject.os, "kill", side_effect=ProcessLookupError):
            self.assertFalse(self.probe.alive(42001))

    def test_inspect_stable_double_observation_returns_exact_owner(self):
        self.assertEqual(self.inspect_with(), self.observed)

    def test_inspect_absent_requires_initial_positive_absence(self):
        with patch.object(self.probe, "alive", return_value=False), patch.object(self.probe, "_birth") as birth:
            self.assertIsNone(self.probe.inspect(42001))
            birth.assert_not_called()

    def test_birth_argv_or_path_change_is_identity_mismatch_not_absence(self):
        changed = [dict(_birth=Mock(side_effect=["old", "new"])),
                   dict(_argv=Mock(side_effect=[self.argv, self.argv + ["--foreign"]])),
                   dict(_path=Mock(side_effect=[self.python, self.root / "other python"]))]
        for values in changed:
            with self.subTest(fields=list(values)), self.assertRaises(ValueError):
                self.inspect_with(**values)

    def test_inspect_exit_after_initial_liveness_is_explicit_esrch(self):
        with self.assertRaises(ProcessLookupError) as failure:
            self.inspect_with(alive=Mock(side_effect=[True, False]))
        self.assertEqual(failure.exception.errno, errno.ESRCH)

    def test_birth_missing_ps_row_and_proven_absence_is_explicit_esrch(self):
        with patch.object(subject, "command", return_value=reply(returncode=1)), \
                patch.object(self.probe, "alive", return_value=False) as alive:
            with self.assertRaises(ProcessLookupError) as failure:
                self.probe._birth(42001)
            self.assertEqual(failure.exception.errno, errno.ESRCH)
            alive.assert_called_once_with(42001)

    def test_birth_missing_ps_row_live_or_unknown_pid_is_not_esrch(self):
        with patch.object(subject, "command", return_value=reply(returncode=1)):
            with patch.object(self.probe, "alive", return_value=True), self.assertRaises(ValueError):
                self.probe._birth(42001)
            with patch.object(self.probe, "alive", side_effect=ValueError("permission unknown")), self.assertRaises(ValueError):
                self.probe._birth(42001)

    def test_birth_other_ps_failures_remain_unknown_even_if_pid_now_absent(self):
        for result in (reply(returncode=1, stderr=b"Operation not permitted"), reply(returncode=127),
                       reply(returncode=0), reply(stdout=b"invalid", returncode=1)):
            with self.subTest(result=result), patch.object(subject, "command", return_value=result), \
                    patch.object(self.probe, "alive", return_value=False), self.assertRaises(ValueError):
                self.probe._birth(42001)

    def test_same_inode_byte_change_during_observation_is_not_stable_identity(self):
        calls = []
        def argv(_pid):
            calls.append(1)
            if len(calls) == 2:
                self.python.write_bytes(b"in-place changed interpreter")
            return self.argv
        with self.assertRaises(ValueError):
            self.inspect_with(_argv=Mock(side_effect=argv))

    def test_owner_alive_distinguishes_pid_reuse_unknown_and_absence(self):
        for observed, expected in ((None, "ABSENT"), (self.observed, "MATCH"),
            (dict(self.observed, birth="reused pid birth"), "MISMATCH"),
            (dict(self.observed, executable_inode=self.owner["executable_inode"] + 1), "MISMATCH")):
            with self.subTest(expected=expected), patch.object(self.probe, "inspect", return_value=observed):
                self.assertEqual(self.probe.owner_alive(self.owner), expected)
        for failure in (ValueError("changing"), PermissionError(), ProcessLookupError(errno.ESRCH, "exited during sample"),
                        subprocess.TimeoutExpired("fixture", 5)):
            with self.subTest(error=type(failure).__name__), patch.object(self.probe, "inspect", side_effect=failure):
                self.assertEqual(self.probe.owner_alive(self.owner), "UNKNOWN")

    def test_supervisor_exact_kernel_argv_and_interpreter(self):
        self.assertEqual(subject.supervisor_identity(self.observed, self.root, self.launcher, {self.anchor}),
                         dict(self.owner, manifest_sha256=self.anchor))
        for index, value in ((1, "foreign/script.py"), (2, "--wrong"), (3, "/foreign/manifest"),
                             (4, "--wrong-sha"), (5, "0" * 64)):
            altered = copy.deepcopy(self.observed)
            altered["argv"][index] = value
            with self.subTest(index=index), self.assertRaises(ValueError):
                subject.supervisor_identity(altered, self.root, self.launcher, {self.anchor})
        altered = dict(self.observed, argv=self.argv + ["--once"])
        with self.assertRaises(ValueError):
            subject.supervisor_identity(altered, self.root, self.launcher, {self.anchor})
        with self.assertRaises(ValueError):
            subject.supervisor_identity(dict(self.observed, executable_inode=1), self.root, self.launcher, {self.anchor})

    def test_pane_parser_preserves_live_and_terminal_evidence(self):
        self.assertEqual(subject.parse_panes(b"0|%0|42|0|\ngridedge_supervisor|%7|42001|1|0\n"), [
            dict(session="0", pane="%0", pid=42, dead=False, exit_status=""),
            dict(session="gridedge_supervisor", pane="%7", pid=42001, dead=True, exit_status="0")])
        for raw in (b"truncated", b"s|7|42001|0|", b"s|%x|42001|0|", b"s|%7|unknown|0|", b"s|%7|42001|2|"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_panes(raw)

    def inventory(self, ps=None, panes=None, label_code=113, tmux_code=0, observed=None, probe_override=None):
        uid = os.getuid()
        ps = ps if ps is not None else f"42001 {uid} {shlex.join(self.argv)}\n".encode()
        panes = panes if panes is not None else b"0|%0|42|0|\ngridedge_supervisor|%7|42001|0|\n"
        def command(argv):
            if argv[0] == "/bin/ps":
                return reply(ps)
            if argv[0] == "/bin/launchctl":
                return reply(returncode=label_code)
            if argv[0] == "/opt/homebrew/bin/tmux":
                return reply(panes, tmux_code)
            raise AssertionError("unreviewed native command: " + repr(argv))
        probe = Mock()
        probe.inspect.return_value = self.observed if observed is None else observed
        probe.arguments.return_value = probe.inspect.return_value["argv"]
        probe.alive.return_value = False
        if probe_override is not None:
            probe = probe_override
        with patch.object(subject, "command", side_effect=command):
            return subject.NativeInventory(self.root, self.launcher, {self.anchor}, probe).read()

    def test_inventory_matches_unique_default_pane_and_preserves_session_zero(self):
        value = self.inventory()
        self.assertEqual(value["supervisors"], [dict(self.owner, manifest_sha256=self.anchor)])
        self.assertEqual(value["supervisor_socket"], "default")
        self.assertFalse(value["launchd_present"])

    def test_inventory_rejects_pane_pid_dead_or_multiplicity_conflicts(self):
        for panes in (b"gridedge_supervisor|%7|42002|0|\n", b"gridedge_supervisor|%7|42001|1|0\n",
                      b"", b"gridedge_supervisor|%7|42001|0|\ngridedge_supervisor|%8|42001|0|\n"):
            with self.subTest(panes=panes), self.assertRaises(ValueError):
                self.inventory(panes=panes)

    def test_inventory_errors_and_non_113_launchctl_failures_never_mean_absent(self):
        for code in (1, 127, -15):
            with self.subTest(code=code), self.assertRaises(ValueError):
                self.inventory(label_code=code)
        with self.assertRaises(ValueError):
            self.inventory(tmux_code=1)
        with self.assertRaises(ValueError):
            self.inventory(ps=b"42001\n")
        self.assertTrue(self.inventory(label_code=0)["launchd_present"])

    def test_lossy_ps_rendering_cannot_hide_kernel_argv_worker(self):
        worker = dict(self.observed, argv=[str(self.root / "bin/gridedge_ths_live"), "--run-id", "fixture"])
        value = self.inventory(ps=f"42001 {os.getuid()} /a/long/path/truncated-before-program\n".encode(),
                               panes=b"0|%0|42|0|\n", observed=worker)
        self.assertEqual(len(value["workers"]), 1, "lossy ps screen silently certified zero worker owners")

    def test_inventory_unknown_arguments_never_become_zero_owners(self):
        probe = Mock()
        probe.arguments.side_effect = PermissionError("kernel argv unavailable")
        with self.assertRaises((ValueError, OSError)):
            self.inventory(probe_override=probe)

    def test_inventory_changed_candidate_between_arguments_and_inspect_rejects(self):
        probe = Mock()
        probe.arguments.return_value = self.argv
        for observed in (None, dict(self.observed, argv=self.argv + ["--once"])):
            probe.inspect.return_value = observed
            with self.subTest(observed=observed), self.assertRaises(ValueError):
                self.inventory(probe_override=probe)

    def test_retained_dead_pane_needs_clean_status_and_absent_pid(self):
        probe = Mock()
        probe.arguments.return_value = ["/usr/bin/true"]
        probe.alive.return_value = False
        clean = b"0|%0|42|0|\ngridedge_supervisor|%7|42001|1|0\n"
        self.assertEqual(self.inventory(panes=clean, probe_override=probe)["supervisors"], [])
        for status in (b"", b"1", b"-15", b"unknown"):
            pane = b"gridedge_supervisor|%7|42001|1|" + status + b"\n"
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.inventory(panes=pane, probe_override=probe)
        probe.alive.return_value = True
        with self.assertRaises(ValueError):
            self.inventory(panes=clean, probe_override=probe)

    def test_einval_inventory_can_record_proven_zombie_without_claiming_absence(self):
        probe = Mock()
        probe.arguments.side_effect = OSError(errno.EINVAL, "zombie argv unavailable")
        evidence = dict(pid=42001, birth=self.owner["birth"], kernel_state=5, uid=os.getuid())
        probe.zombie_evidence.return_value = evidence
        value = self.inventory(panes=b"0|%0|42|0|\n", probe_override=probe)
        self.assertEqual(value["nonexecuting_zombies"], [evidence])
        self.assertEqual(value["supervisors"], [])
        self.assertEqual(value["guards"], [])
        self.assertEqual(value["workers"], [])
        probe.alive.assert_not_called()
        probe.inspect.assert_not_called()

    def test_einval_inventory_unknown_or_malformed_zombie_evidence_is_rejected(self):
        original = dict(pid=42001, birth=self.owner["birth"], kernel_state=5, uid=os.getuid())
        invalid = [None, {}, dict(original, pid=42002), dict(original, uid=os.getuid() + 1),
                   dict(original, kernel_state=2), dict(original, birth=""), dict(original, extra="unreviewed")]
        for evidence in invalid:
            probe = Mock()
            probe.arguments.side_effect = OSError(errno.EINVAL, "not proof of zombie")
            probe.zombie_evidence.return_value = evidence
            with self.subTest(evidence=evidence), self.assertRaises((ValueError, OSError)):
                self.inventory(panes=b"0|%0|42|0|\n", probe_override=probe)

    def test_other_inventory_errors_do_not_use_zombie_exception(self):
        for code in (errno.EPERM, errno.EACCES, errno.ENOENT, errno.ESRCH):
            probe = Mock()
            probe.arguments.side_effect = OSError(code, "unproven state")
            probe.zombie_evidence.return_value = dict(pid=42001, birth=self.owner["birth"], kernel_state=5, uid=os.getuid())
            with self.subTest(errno=code), self.assertRaises((ValueError, OSError)):
                self.inventory(panes=b"0|%0|42|0|\n", probe_override=probe)
            probe.zombie_evidence.assert_not_called()

    def test_zombie_evidence_never_changes_owner_alive_to_absent(self):
        evidence = dict(pid=42001, birth=self.owner["birth"], kernel_state=5, uid=os.getuid())
        with patch.object(self.probe, "inspect", side_effect=OSError(errno.EINVAL, "zombie")), \
                patch.object(self.probe, "zombie_evidence", return_value=evidence) as zombie:
            self.assertEqual(self.probe.owner_alive(self.owner), "UNKNOWN")
            zombie.assert_not_called()

    def zombie_samples(self):
        start = 1789374000
        kernel = dict(pid=42001, uid=os.getuid(), status=5, start_sec=start, start_usec=123456)
        ps = dict(pid=42001, uid=os.getuid(), birth=datetime.fromtimestamp(start).strftime("%a %b %d %H:%M:%S %Y"), state="Z+")
        return kernel, ps

    def test_zombie_evidence_requires_two_matching_kernel_and_ps_samples(self):
        kernel, ps = self.zombie_samples()
        with patch.object(self.probe, "_bsd", side_effect=[kernel, dict(kernel)]) as bsd, \
                patch.object(self.probe, "_zombie_ps", side_effect=[ps, dict(ps)]) as corroboration:
            self.assertEqual(self.probe.zombie_evidence(42001),
                dict(pid=42001, uid=os.getuid(), kernel_state=5, birth=ps["birth"]))
            self.assertEqual(bsd.call_count, 2)
            self.assertEqual(corroboration.call_count, 2)

    def test_zombie_evidence_initial_live_foreign_uid_or_pid_is_not_proof(self):
        kernel, _ = self.zombie_samples()
        for changed in (dict(kernel, status=2), dict(kernel, uid=os.getuid()+1), dict(kernel, pid=42002)):
            with self.subTest(kernel=changed), patch.object(self.probe, "_bsd", return_value=changed), \
                    patch.object(self.probe, "_zombie_ps") as ps:
                self.assertIsNone(self.probe.zombie_evidence(42001))
                ps.assert_not_called()

    def test_zombie_evidence_microsecond_birth_pid_uid_or_state_drift_is_rejected(self):
        kernel, ps = self.zombie_samples()
        for field, value in (("pid", 42002), ("uid", os.getuid()+1), ("status", 2),
                             ("start_sec", kernel["start_sec"]+1), ("start_usec", kernel["start_usec"]+1)):
            with self.subTest(field=field), patch.object(self.probe, "_bsd", side_effect=[kernel, dict(kernel, **{field:value})]), \
                    patch.object(self.probe, "_zombie_ps", return_value=ps), self.assertRaises(ValueError):
                self.probe.zombie_evidence(42001)

    def test_zombie_evidence_ps_change_live_state_or_kernel_birth_disagreement_rejects(self):
        kernel, ps = self.zombie_samples()
        cases = [(ps, dict(ps, state="R")), (dict(ps, state="S"), dict(ps, state="S")),
                 (dict(ps, pid=42002), dict(ps, pid=42002)),
                 (dict(ps, uid=os.getuid()+1), dict(ps, uid=os.getuid()+1)),
                 (dict(ps, birth="Mon Sep 14 01:00:00 2026"), dict(ps, birth="Mon Sep 14 01:00:00 2026"))]
        for first, second in cases:
            with self.subTest(first=first, second=second), patch.object(self.probe, "_bsd", return_value=kernel), \
                    patch.object(self.probe, "_zombie_ps", side_effect=[first, second]), self.assertRaises(ValueError):
                self.probe.zombie_evidence(42001)

    def test_zombie_evidence_kernel_or_ps_read_failure_is_unknown(self):
        kernel, _ = self.zombie_samples()
        with patch.object(self.probe, "_bsd", side_effect=OSError(errno.ESRCH, "no BSD info")), \
                self.assertRaises(OSError):
            self.probe.zombie_evidence(42001)
        with patch.object(self.probe, "_bsd", return_value=kernel), \
                patch.object(self.probe, "_zombie_ps", side_effect=ValueError("truncated ps")), \
                self.assertRaises(ValueError):
            self.probe.zombie_evidence(42001)

    def test_native_signature_requires_exact_bytes_team_and_success_codes(self):
        with patch.object(subject, "command", side_effect=[reply(), reply(stderr=b"TeamIdentifier=WM4JXVE5GV\n")]):
            self.assertIs(subject.verify_signature(self.python, sha(self.python.read_bytes())), True)
        for verify_code, details_code, team in ((1, 0, b"TeamIdentifier=WM4JXVE5GV\n"),
            (0, 1, b"TeamIdentifier=WM4JXVE5GV\n"), (0, 0, b"TeamIdentifier=OTHER\n"),
            (0, 0, b"TeamIdentifier=WM4JXVE5GV\nTeamIdentifier=OTHER\n")):
            with self.subTest(verify=verify_code, detail=details_code, team=team), patch.object(subject, "command",
                    side_effect=[reply(returncode=verify_code), reply(returncode=details_code, stderr=team)]):
                with self.assertRaises(ValueError):
                    subject.verify_signature(self.python, sha(self.python.read_bytes()))
        with patch.object(subject, "command") as command, self.assertRaises(ValueError):
            subject.verify_signature(self.python, "0" * 64)
        command.assert_not_called()

    def test_native_signature_rejects_same_inode_mutation_during_codesign(self):
        expected = sha(self.python.read_bytes())
        def command(argv):
            if "-d" in argv:
                self.python.write_bytes(b"changed after verify")
                return reply(stderr=b"TeamIdentifier=WM4JXVE5GV\n")
            return reply()
        with patch.object(subject, "command", side_effect=command), self.assertRaises(ValueError):
            subject.verify_signature(self.python, expected)

    def test_default_supervisor_argv_zero_cannot_be_spoofed(self):
        altered = copy.deepcopy(self.observed)
        altered["argv"][0] = "/unreviewed/python"
        with self.assertRaises(ValueError):
            subject.supervisor_identity(altered, self.root, self.launcher, {self.anchor})

    def mapped_interpreter(self):
        native = self.root / "Resources/Python.app/Contents/MacOS/Python"
        native.parent.mkdir(parents=True, exist_ok=True)
        native.write_bytes(b"reviewed explicit native wrapper target")
        trust = dict(launcher_interpreter_sha256=sha(self.python.read_bytes()), native_executable_path=str(native),
                     native_executable_sha256=sha(native.read_bytes()), expected_argv0=str(native))
        observed = dict(self.observed, executable_path=str(native), executable_inode=native.stat().st_ino,
                        executable_sha256=sha(native.read_bytes()), argv=[str(native), *self.argv[1:]])
        return native, trust, observed

    def test_explicit_wrapper_mapping_binds_both_hashes_path_and_argv_zero(self):
        native, trust, observed = self.mapped_interpreter()
        result = subject.supervisor_identity(observed, self.root, self.launcher, {self.anchor}, interpreter_trust=trust)
        self.assertEqual(result["executable_sha256"], sha(native.read_bytes()))
        self.assertEqual(result["executable_inode"], native.stat().st_ino)
        self.assertEqual(result["manifest_sha256"], self.anchor)
        with self.assertRaises(ValueError):
            subject.supervisor_identity(observed, self.root, self.launcher, {self.anchor})

    def test_wrapper_mapping_rejects_missing_extra_and_wrong_trust_fields(self):
        _, trust, observed = self.mapped_interpreter()
        bad = [dict(trust, unexpected="not-reviewed"),
               {key: value for key, value in trust.items() if key != "expected_argv0"},
               dict(trust, launcher_interpreter_sha256="0" * 64),
               dict(trust, native_executable_sha256="0" * 64),
               dict(trust, native_executable_path=str(self.python)),
               dict(trust, expected_argv0="/arbitrary/alias/python")]
        for item in bad:
            with self.subTest(trust=item), self.assertRaises(ValueError):
                subject.supervisor_identity(observed, self.root, self.launcher, {self.anchor}, interpreter_trust=item)

    def test_wrapper_mapping_cannot_accept_forged_observation_hash_or_inode(self):
        _, trust, observed = self.mapped_interpreter()
        for changed in (dict(observed, executable_sha256="0" * 64), dict(observed, executable_inode=1),
                        dict(observed, argv=[str(self.python), *self.argv[1:]])):
            with self.subTest(observation=changed), self.assertRaises(ValueError):
                subject.supervisor_identity(changed, self.root, self.launcher, {self.anchor}, interpreter_trust=trust)

    def test_wrapper_mapping_rejects_current_launcher_and_native_byte_drift(self):
        native, trust, observed = self.mapped_interpreter()
        for path in (self.python, native):
            before = path.read_bytes()
            try:
                path.write_bytes(b"unreviewed in-place drift")
                with self.subTest(path=path.name), self.assertRaises(ValueError):
                    subject.supervisor_identity(observed, self.root, self.launcher, {self.anchor}, interpreter_trust=trust)
            finally:
                path.write_bytes(before)

    def test_lightweight_arguments_absence_and_unknown_remain_distinct(self):
        with patch.object(self.probe, "alive", return_value=False), \
                patch.object(self.probe, "_argv", side_effect=OSError(errno.ESRCH, "no process")):
            self.assertIsNone(self.probe.arguments(42001))
        for failure in (PermissionError(), OSError("kernel read failed"), ValueError("malformed")):
            with self.subTest(error=type(failure).__name__), patch.object(self.probe, "alive", return_value=True), \
                    patch.object(self.probe, "_argv", side_effect=failure), self.assertRaises((OSError, ValueError)):
                self.probe.arguments(42001)

    def test_readable_protected_process_arguments_do_not_require_signal_permission(self):
        with patch.object(self.probe, "alive", side_effect=ValueError("kill zero EPERM")) as alive, \
                patch.object(self.probe, "_argv", return_value=self.argv):
            self.assertEqual(self.probe.arguments(42001), self.argv)
            alive.assert_not_called()

    def test_arguments_invalid_pid_rejects_before_any_kernel_read(self):
        for pid in (0, 1, -1, True, 42001.0, "42001"):
            with self.subTest(pid=pid), patch.object(self.probe, "_argv") as argv, self.assertRaises(ValueError):
                self.probe.arguments(pid)
            argv.assert_not_called()

    def test_argument_esrch_without_second_absence_proof_is_not_absent(self):
        with patch.object(self.probe, "_argv", side_effect=OSError(errno.ESRCH, "no process")):
            with patch.object(self.probe, "alive", return_value=True), self.assertRaises((ValueError, OSError)):
                self.probe.arguments(42001)
            with patch.object(self.probe, "alive", side_effect=ValueError("unknown")), self.assertRaises((ValueError, OSError)):
                self.probe.arguments(42001)

    def test_other_argument_errors_cannot_use_kill_absence_to_hide_unknown(self):
        for code in (errno.EPERM, errno.EACCES, errno.ENOENT, errno.EINVAL):
            with self.subTest(errno=code), patch.object(self.probe, "alive", return_value=False), \
                    patch.object(self.probe, "_argv", side_effect=OSError(code, "not ESRCH")), \
                    self.assertRaises((OSError, ValueError)):
                self.probe.arguments(42001)

    def helper_fixture(self):
        path = self.root / "reviewed kernel helper"
        path.write_bytes(b"frozen read-only helper fixture")
        self.probe.kernel_probe = (path, sha(path.read_bytes()))
        return path, self.zombie_samples()[0]

    def test_kernel_helper_requires_explicit_frozen_hash_and_exact_argv(self):
        path, value = self.helper_fixture()
        with patch.object(subject, "command", return_value=reply(json.dumps(value).encode())) as command:
            self.assertEqual(self.probe._bsd(42001), value)
            command.assert_called_once_with([str(path), "42001"])
        self.probe.kernel_probe = (path, "0" * 64)
        with patch.object(subject, "command") as command, self.assertRaises(ValueError):
            self.probe._bsd(42001)
        command.assert_not_called()

    def test_kernel_helper_rejects_truncated_wrong_shape_types_and_identity(self):
        _, value = self.helper_fixture()
        invalid = [b"{", b"[]", b"null", json.dumps(dict(value, extra="unknown")).encode(),
                   json.dumps({k:v for k,v in value.items() if k != "start_usec"}).encode()]
        invalid += [json.dumps(dict(value, **{field: bad})).encode() for field, bad in (
            ("pid", 42002), ("pid", True), ("uid", -1), ("status", 5.0), ("start_sec", 0),
            ("start_sec", "1"), ("start_usec", -1), ("start_usec", 1000000))]
        for raw in invalid:
            with self.subTest(raw=raw), patch.object(subject, "command", return_value=reply(raw)), self.assertRaises(ValueError):
                self.probe._bsd(42001)

    def test_kernel_helper_error_rc_never_yields_terminal_evidence(self):
        _, value = self.helper_fixture()
        for code in (2, 3, 4, -15):
            with self.subTest(code=code), patch.object(subject, "command", return_value=reply(json.dumps(value).encode(), code)), \
                    self.assertRaises(ValueError):
                self.probe._bsd(42001)

    def test_kernel_helper_same_inode_byte_drift_is_rejected(self):
        path, value = self.helper_fixture()
        def command(_argv):
            path.write_bytes(b"changed during helper execution")
            return reply(json.dumps(value).encode())
        with patch.object(subject, "command", side_effect=command), self.assertRaises(ValueError):
            self.probe._bsd(42001)


if __name__ == "__main__":
    unittest.main()
