"""Real isolated tmux/daemon lifecycle tests, never the default socket/runtime.

All processes are test-created under a UUID socket and a temporary root.
The daemon has no market, broker, Android, trading, or account functionality.
"""
from datetime import datetime, timedelta, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

import core_migration_lifecycle as subject
import core_migration_native as native

DAEMON = '''import argparse, datetime, json, os, pathlib, signal, time
p=argparse.ArgumentParser()
p.add_argument('--manifest',required=True)
p.add_argument('--manifest-sha256',required=True)
a=p.parse_args()
root=pathlib.Path(a.manifest).parent.parent
running=True
def stop(*_):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
while running:
    maintenance=(root/'runtime/ths-deployment-maintenance').is_dir()
    record=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),pid=os.getpid(),
        manifest_sha256=a.manifest_sha256,window='CLOSED',executed=False,
        state=dict(maintenance=maintenance),action='BLOCKED_MAINTENANCE' if maintenance else 'OBSERVE_CLOSED')
    temporary=root/'runtime/fixture-heartbeat.tmp'
    temporary.write_text(json.dumps(record))
    temporary.replace(root/'runtime/session-supervisor-heartbeat.json')
    time.sleep(.05)
exit_code=root/'runtime/fixture-exit-code'
raise SystemExit(int(exit_code.read_text()) if exit_code.exists() else 0)
'''


def sha(value):
    return hashlib.sha256(value).hexdigest()


class ControllerCrash(RuntimeError):
    pass


class RealLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gridedge-real-lifecycle-test-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "isolated runtime with spaces"
        for folder in ("bin", "config", "runtime"):
            (self.root / folder).mkdir(parents=True, exist_ok=True)
        self.socket = "gridedge_migration_e2e_" + uuid.uuid4().hex
        self.prefix = ["/opt/homebrew/bin/tmux", "-L", self.socket]
        self.script = self.root / "bin/session_supervisor.py"
        self.script.write_text(DAEMON)
        self.manifest = self.root / "config/session-supervisor-manifest.json"
        self.manifest.write_text(json.dumps({"fixture_only": True, "script_sha256": sha(self.script.read_bytes())}))
        self.anchor = sha(self.manifest.read_bytes())
        self.launcher = self.root / "bin/run_session_supervisor.sh"
        self.launcher_bytes = ("#!/bin/sh\nset -eu\nexec " + shlex.join([sys.executable, str(self.script),
            "--manifest", str(self.manifest), "--manifest-sha256", self.anchor]) + "\n").encode()
        self.launcher.write_bytes(self.launcher_bytes)
        self.launcher.chmod(0o500)
        self.maintenance = self.root / "runtime/ths-deployment-maintenance"
        self.maintenance.mkdir()
        self.probe = native.MacProcessProbe()
        # This is explicitly self-owned fixture interpreter trust, not a formal
        # reviewed identity generated from arbitrary installed production files.
        self.interpreter = self.probe.inspect(os.getpid())
        self.trust = dict(launcher_interpreter_sha256=sha(Path(sys.executable).resolve().read_bytes()),
            native_executable_path=self.interpreter["executable_path"],
            native_executable_sha256=self.interpreter["executable_sha256"], expected_argv0=self.interpreter["argv"][0])
        self.migration_index = sha(b"reviewed fixture migration stage 1")
        self.owner = self.new_controller()
        self.addCleanup(self.cleanup_fixture_sessions)

    def tmux(self, *args):
        return subprocess.run([*self.prefix, *args], capture_output=True, timeout=5, check=False)

    def panes(self):
        result = self.tmux("list-panes", "-a", "-F", "#{session_name}|#{pane_id}|#{pane_pid}|#{pane_dead}|#{pane_dead_status}")
        return [] if result.returncode != 0 else native.parse_panes(result.stdout)

    def cleanup_fixture_sessions(self):
        # Only sessions created on this test's unique socket. Formal/default
        # routing never appears in a command and no kill-server is issued.
        for pane in self.panes():
            if pane["session"] == "gridedge_supervisor" and not pane["dead"]:
                observed = self.probe.inspect(pane["pid"])
                if observed is not None and str(self.script) in observed["argv"]:
                    os.kill(pane["pid"], signal.SIGTERM)
                    deadline = time.monotonic() + 3
                    while self.probe.alive(pane["pid"]) and time.monotonic() < deadline:
                        time.sleep(.05)
            self.assertIn(pane["session"], ("gridedge_supervisor", "0", "gridedge_supervisor_archive"))
            self.tmux("kill-session", "-t", "=" + pane["session"])

    def start(self):
        return self.owner.start(self.launcher, self.anchor)

    def new_controller(self):
        return subject.TmuxSupervisorOwner(self.root, self.launcher_bytes, {self.anchor},
            self.probe, self.trust, self.socket, self.migration_index)

    @property
    def stop_receipt(self):
        return self.root / "runtime" / ("core-quiesce-" + self.migration_index) / "receipt.json"

    @property
    def terminal_receipt(self):
        return self.stop_receipt.parent / "terminal/terminal.json"

    def interrupted_stop(self, boundary):
        entry = self.start()
        real_kill, real_tmux = os.kill, self.owner._tmux
        reached = []
        def kill(pid, sig):
            if pid == entry["pid"] and sig == signal.SIGTERM:
                if boundary == "before_signal":
                    reached.append(boundary)
                    raise ControllerCrash(boundary)
                result = real_kill(pid, sig)
                if boundary == "after_signal":
                    reached.append(boundary)
                    raise ControllerCrash(boundary)
                return result
            return real_kill(pid, sig)
        def tmux(*args):
            if args[0] == "kill-session":
                if boundary == "before_remove":
                    reached.append(boundary)
                    raise ControllerCrash(boundary)
                result = real_tmux(*args)
                if boundary == "after_remove":
                    reached.append(boundary)
                    raise ControllerCrash(boundary)
                return result
            return real_tmux(*args)
        with patch.object(subject.os, "kill", side_effect=kill), patch.object(self.owner, "_tmux", side_effect=tmux):
            with self.assertRaises(ControllerCrash):
                self.owner.quiesce(entry)
        self.assertEqual(reached, [boundary])
        self.assertTrue(self.stop_receipt.is_file())
        return entry

    def wait_for_fixture_exit(self, entry):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            panes = self.panes()
            if not self.probe.alive(entry["pid"]) and len(panes) == 1 and panes[0]["dead"]:
                return
            time.sleep(.02)
        self.fail("test daemon failed to reach a retained terminal pane")

    def test_stop_receipt_is_exact_and_fsynced_before_first_signal(self):
        entry = self.start()
        pane = self.owner._panes()[0]
        real_sync, real_kill = os.fsync, os.kill
        synced_files, synced_dirs = set(), set()
        def fsync(fd):
            info = os.fstat(fd)
            (synced_dirs if stat.S_ISDIR(info.st_mode) else synced_files).add(info.st_ino)
            return real_sync(fd)
        def kill(pid, sig):
            if sig == signal.SIGTERM:
                record = json.loads(self.stop_receipt.read_bytes())
                self.assertEqual(record, dict(schema="gridedge.supervisor-quiesce.v1", index=self.migration_index,
                    root=str(self.root), socket=self.socket, session="gridedge_supervisor", pane=pane,
                    entry=entry, launcher_sha256=sha(self.launcher_bytes)))
                self.assertIn(self.stop_receipt.stat().st_ino, synced_files)
                self.assertIn(self.stop_receipt.parent.stat().st_ino, synced_dirs)
                self.assertIn((self.root / "runtime").stat().st_ino, synced_dirs)
            return real_kill(pid, sig)
        with patch.object(subject.os, "fsync", side_effect=fsync), patch.object(subject.os, "kill", side_effect=kill):
            self.owner.quiesce(entry)

    def test_retry_refsyncs_visible_but_unfinished_receipt_before_signaling(self):
        entry = self.start()
        real_directory_sync = subject.files.directory_sync
        def directory_sync(path):
            if Path(path) == self.stop_receipt.parent:
                raise ControllerCrash("receipt renamed before directory durability")
            return real_directory_sync(path)
        with patch.object(subject.files, "directory_sync", side_effect=directory_sync):
            with self.assertRaises(ControllerCrash):
                self.owner.quiesce(entry)
        original = self.stop_receipt.read_bytes()
        self.assertTrue(self.probe.alive(entry["pid"]))
        real_sync, real_kill = os.fsync, os.kill
        synced_dirs = set()
        def fsync(fd):
            info = os.fstat(fd)
            if stat.S_ISDIR(info.st_mode):
                synced_dirs.add(info.st_ino)
            return real_sync(fd)
        def kill(pid, sig):
            if sig == signal.SIGTERM:
                self.assertIn(self.stop_receipt.parent.stat().st_ino, synced_dirs,
                              "retry treated a visible receipt as durably committed")
                self.assertIn((self.root / "runtime").stat().st_ino, synced_dirs)
            return real_kill(pid, sig)
        with patch.object(subject.os, "fsync", side_effect=fsync), patch.object(subject.os, "kill", side_effect=kill):
            self.new_controller().resume_quiescence()
        self.assertEqual(self.stop_receipt.read_bytes(), original)

    def test_terminal_receipt_is_exact_and_durable_before_removing_session(self):
        entry = self.start()
        pane = self.owner._panes()[0]
        real_sync, real_tmux = os.fsync, self.owner._tmux
        events = []
        def fsync(fd):
            info = os.fstat(fd)
            events.append((stat.S_ISDIR(info.st_mode), info.st_ino, self.terminal_receipt.exists()))
            return real_sync(fd)
        def tmux(*args):
            if args[0] == "kill-session":
                expected = dict(schema="gridedge.supervisor-quiesce-terminal.v1", receipt_sha256=sha(self.stop_receipt.read_bytes()),
                    pane=dict(pane, dead=True, exit_status="0"), observed_pid_absent=True)
                self.assertEqual(json.loads(self.terminal_receipt.read_bytes()), expected)
                self.assertTrue(any(not is_dir and inode == self.terminal_receipt.stat().st_ino for is_dir,inode,_ in events))
                for path in (self.terminal_receipt.parent, self.stop_receipt.parent):
                    self.assertIn((True, path.stat().st_ino, True), events, "terminal directory not durable before deletion")
                self.assertFalse(self.probe.alive(entry["pid"]))
                self.assertEqual(args, ("kill-session", "-t", "=gridedge_supervisor"))
            return real_tmux(*args)
        with patch.object(subject.os, "fsync", side_effect=fsync), patch.object(self.owner, "_tmux", side_effect=tmux):
            self.owner.quiesce(entry)

    def test_existing_receipt_directory_replacement_before_fsync_cannot_authorize_signal(self):
        self.receipt_directory_race(replace_runtime=False)

    def test_receipt_runtime_parent_replacement_cannot_authorize_signal(self):
        self.receipt_directory_race(replace_runtime=True)

    def receipt_directory_race(self, replace_runtime):
        entry = self.interrupted_stop("before_signal")
        original = self.stop_receipt.read_bytes()
        real_sync, real_kill = subject.files.directory_sync, os.kill
        substituted, signals = [], []
        def directory_sync(path):
            if Path(path) == self.stop_receipt.parent and not substituted:
                substituted.append(True)
                target = self.root / "runtime" if replace_runtime else self.stop_receipt.parent
                backup = self.root / "preserved-original-receipt-tree"
                target.rename(backup)
                self.stop_receipt.parent.mkdir(parents=True)
                self.stop_receipt.write_bytes(original if replace_runtime else b'{"untrusted":true}')
            return real_sync(path)
        def kill(pid, sig):
            if sig == signal.SIGTERM:
                signals.append(pid)
                raise ControllerCrash("signal attempted after receipt directory substitution")
            return real_kill(pid, sig)
        with patch.object(subject.files, "directory_sync", side_effect=directory_sync), \
                patch.object(subject.os, "kill", side_effect=kill):
            with self.assertRaises((ValueError, OSError, ControllerCrash)):
                self.new_controller().resume_quiescence()
        self.assertEqual(substituted, [True], "directory substitution was not exercised")
        self.assertEqual(signals, [], "receipt inode replacement was trusted before signaling")
        self.assertTrue(self.probe.alive(entry["pid"]))

    def test_controller_restart_at_each_stop_boundary_uses_immutable_receipts(self):
        for boundary in ("before_signal", "after_signal", "before_remove", "after_remove"):
            with self.subTest(boundary=boundary):
                case = RealLifecycleTest()
                case.setUp()
                try:
                    entry = case.interrupted_stop(boundary)
                    original = case.stop_receipt.read_bytes()
                    terminal = case.terminal_receipt.read_bytes() if case.terminal_receipt.exists() else None
                    resumed = case.new_controller()
                    resumed.resume_quiescence()
                    case.assertEqual(case.stop_receipt.read_bytes(), original)
                    case.assertTrue(case.terminal_receipt.is_file())
                    if terminal is not None:
                        case.assertEqual(case.terminal_receipt.read_bytes(), terminal)
                    case.assertFalse(case.probe.alive(entry["pid"]))
                    case.assertEqual(case.panes(), [])
                    resumed.resume_quiescence()
                    case.assertEqual(case.stop_receipt.read_bytes(), original)
                finally:
                    case.doCleanups()

    def test_start_after_controller_crash_finishes_exact_old_quiescence(self):
        old = self.interrupted_stop("after_signal")
        self.wait_for_fixture_exit(old)
        original = self.stop_receipt.read_bytes()
        self.owner = self.new_controller()
        new = self.start()
        self.assertNotEqual(new["pid"], old["pid"])
        self.assertFalse(self.probe.alive(old["pid"]))
        self.assertEqual(self.stop_receipt.read_bytes(), original)
        self.assertTrue(self.terminal_receipt.exists())

    def test_resume_same_pane_becoming_dead_before_signal_does_not_signal_again(self):
        entry = self.interrupted_stop("before_signal")
        resumed = self.new_controller()
        original_tmux, original_kill = resumed._tmux, os.kill
        transitioned = []
        def tmux(*args):
            result = original_tmux(*args)
            if args[0] == "set-window-option":
                original_kill(entry["pid"], signal.SIGTERM)
                self.wait_for_fixture_exit(entry)
                transitioned.append(True)
            return result
        with patch.object(resumed, "_tmux", side_effect=tmux), patch.object(subject.os, "kill", wraps=original_kill) as kill:
            resumed.resume_quiescence()
            self.assertFalse(any(call.args[1] == signal.SIGTERM for call in kill.call_args_list))
        self.assertEqual(transitioned, [True])
        self.assertFalse(self.probe.alive(entry["pid"]))
        self.assertEqual(self.panes(), [])

    def test_resume_changed_pane_at_presignal_recheck_is_not_treated_as_exit(self):
        entry = self.interrupted_stop("before_signal")
        resumed = self.new_controller()
        original_tmux, original_panes, original_kill = resumed._tmux, resumed._panes, os.kill
        state = {"changed": False}
        def tmux(*args):
            result = original_tmux(*args)
            if args[0] == "set-window-option":
                state["changed"] = True
            return result
        def panes():
            values = original_panes()
            return [dict(values[0], pane="%999999")] if state["changed"] else values
        with patch.object(resumed, "_tmux", side_effect=tmux) as tmux_mock, patch.object(resumed, "_panes", side_effect=panes), \
                patch.object(subject.os, "kill", wraps=original_kill) as kill:
            with self.assertRaises(ValueError):
                resumed.resume_quiescence()
            self.assertFalse(any(call.args[1] == signal.SIGTERM for call in kill.call_args_list))
            self.assertFalse(any(call.args[0] == "kill-session" for call in tmux_mock.call_args_list))
        self.assertTrue(state["changed"])
        self.assertTrue(self.probe.alive(entry["pid"]))
        self.new_controller().resume_quiescence()

    def test_orphan_clean_dead_pane_without_stop_receipt_cannot_be_cleared(self):
        entry = self.start()
        self.assertEqual(self.tmux("set-window-option", "-t", "=gridedge_supervisor", "remain-on-exit", "on").returncode, 0)
        os.kill(entry["pid"], signal.SIGTERM)
        self.wait_for_fixture_exit(entry)
        before = self.panes()
        self.assertFalse(self.stop_receipt.exists())
        with self.assertRaises((ValueError, OSError)):
            self.new_controller().resume_quiescence()
        self.assertEqual(self.panes(), before)

    def test_missing_session_without_prior_terminal_receipt_is_not_clean_completion(self):
        entry = self.interrupted_stop("after_signal")
        self.wait_for_fixture_exit(entry)
        self.assertFalse(self.terminal_receipt.exists())
        self.assertEqual(self.tmux("kill-session", "-t", "=gridedge_supervisor").returncode, 0)
        with self.assertRaises((ValueError, OSError)):
            self.new_controller().resume_quiescence()
        self.assertFalse(self.terminal_receipt.exists())

    def test_tampered_stop_or_terminal_receipt_cannot_authorize_dead_pane_removal(self):
        self.interrupted_stop("before_remove")
        before = self.panes()
        for path in (self.stop_receipt, self.terminal_receipt):
            original = path.read_bytes()
            try:
                path.chmod(0o600)
                record = json.loads(original)
                record["unexpected"] = "not reviewed"
                path.write_text(json.dumps(record))
                with self.subTest(path=path.name), self.assertRaises(ValueError):
                    self.new_controller().resume_quiescence()
                self.assertEqual(self.panes(), before)
            finally:
                path.write_bytes(original)

    def test_resume_with_foreign_transaction_cannot_adopt_existing_dead_pane(self):
        self.interrupted_stop("before_remove")
        before = self.panes()
        self.migration_index = sha(b"different unbound fixture migration")
        with self.assertRaises((ValueError, OSError)):
            self.new_controller().resume_quiescence()
        self.assertEqual(self.panes(), before)

    def test_new_launcher_restart_needs_explicit_old_anchor_without_rebaselining_receipt(self):
        self.interrupted_stop("before_remove")
        before_panes = self.panes()
        original_receipt = self.stop_receipt.read_bytes()
        old_launcher_sha = sha(self.launcher_bytes)
        self.manifest.write_text(json.dumps({"fixture_only": True, "version": 2, "script_sha256": sha(self.script.read_bytes())}))
        new_anchor = sha(self.manifest.read_bytes())
        new_launcher = self.launcher_bytes.replace(self.anchor.encode(), new_anchor.encode())
        self.launcher.chmod(0o600)
        self.launcher.write_bytes(new_launcher)
        self.launcher.chmod(0o500)
        wrong_baseline = subject.TmuxSupervisorOwner(self.root, new_launcher, {self.anchor, new_anchor},
            self.probe, self.trust, self.socket, self.migration_index)
        with self.assertRaises(ValueError):
            wrong_baseline.resume_quiescence()
        self.assertEqual(self.panes(), before_panes)
        exact_baseline = subject.TmuxSupervisorOwner(self.root, new_launcher, {self.anchor, new_anchor},
            self.probe, self.trust, self.socket, self.migration_index, baseline_launcher_sha256=old_launcher_sha)
        exact_baseline.resume_quiescence()
        entry = exact_baseline.start(self.launcher, new_anchor)
        self.assertEqual(entry["manifest_sha256"], new_anchor)
        self.assertTrue(self.probe.alive(entry["pid"]))
        self.assertEqual(self.stop_receipt.read_bytes(), original_receipt)

    def test_real_start_release_clean_stop_and_second_start(self):
        for cycle in range(2):
            with self.subTest(cycle=cycle):
                if cycle:
                    self.migration_index = sha(b"reviewed fixture migration stage 2")
                    self.owner = self.new_controller()
                if not self.maintenance.exists():
                    self.maintenance.mkdir()
                entry = self.start()
                self.assertTrue(self.probe.alive(entry["pid"]))
                initial = json.loads(self.owner.heartbeat.read_bytes())
                self.assertEqual(initial["pid"], entry["pid"])
                self.assertEqual(initial["action"], "BLOCKED_MAINTENANCE")
                self.assertFalse(initial["executed"])
                boundary = datetime.now(timezone.utc)
                self.maintenance.rmdir()
                released = self.owner.verify_released(entry, boundary)
                self.assertEqual(released["pid"], entry["pid"])
                self.assertEqual(released["action"], "OBSERVE_CLOSED")
                self.assertGreater(datetime.fromisoformat(released["at"]), boundary)
                self.owner.quiesce(entry)
                self.assertFalse(self.probe.alive(entry["pid"]))
                self.assertEqual(self.panes(), [])

    def test_second_start_does_not_duplicate_or_replace_live_owner(self):
        entry = self.start()
        with self.assertRaises(ValueError):
            self.start()
        self.assertEqual(self.panes()[0]["pid"], entry["pid"])
        self.assertTrue(self.probe.alive(entry["pid"]))
        self.owner.quiesce(entry)

    def test_wrong_birth_does_not_send_signal_to_same_pid(self):
        entry = self.start()
        with patch.object(subject.os, "kill") as kill, self.assertRaises(ValueError):
            self.owner.quiesce(dict(entry, birth="unreviewed replacement birth"))
        # kill(pid,0) is a read-only existence probe; SIGTERM must not occur.
        self.assertFalse(any(call.args[1] != 0 for call in kill.call_args_list))
        self.assertTrue(self.probe.alive(entry["pid"]))
        self.owner.quiesce(entry)

    def test_nonzero_exit_retains_dead_pane_for_evidence(self):
        (self.root / "runtime/fixture-exit-code").write_text("42")
        entry = self.start()
        with self.assertRaises(ValueError):
            self.owner.quiesce(entry)
        panes = self.panes()
        self.assertEqual(len(panes), 1)
        self.assertEqual(panes[0]["pid"], entry["pid"])
        self.assertTrue(panes[0]["dead"])
        self.assertEqual(panes[0]["exit_status"], "42")

    def test_foreign_session_zero_is_never_removed_or_signaled(self):
        self.assertEqual(self.tmux("new-session", "-d", "-s", "0", "/bin/sleep", "60").returncode, 0)
        before = self.panes()
        entry = self.start()
        self.assertEqual([pane for pane in self.panes() if pane["session"] == "0"], before)
        self.owner.quiesce(entry)
        self.assertEqual(self.panes(), before)

    def test_same_prefix_foreign_session_survives_owned_lifecycle(self):
        self.assertEqual(self.tmux("new-session", "-d", "-s", "gridedge_supervisor_archive", "/bin/sleep", "60").returncode, 0)
        before = self.panes()
        entry = self.start()
        self.owner.quiesce(entry)
        self.assertEqual(self.panes(), before)

    def test_exit_between_pane_snapshot_and_identity_waits_for_clean_terminal_proof(self):
        self.exit_race(brief_alive=False)

    def test_exit_esrch_with_brief_live_pid_waits_without_clearing_early(self):
        self.exit_race(brief_alive=True)

    def test_post_signal_eperm_is_not_swallowed_even_if_pid_later_exits(self):
        self.exit_race(brief_alive=False, error_code=errno.EPERM)

    def exit_race(self, brief_alive, error_code=errno.ESRCH):
        entry = self.start()
        prior = self.owner._panes()
        actual_panes = self.owner._panes
        actual_inspect = self.probe.inspect
        actual_alive = self.probe.alive
        actual_kill = os.kill
        state = dict(stopped=False, stale_sent=False, stale_active=False, transient=False, live_seen=False)
        def panes():
            if state["stopped"] and not state["stale_sent"]:
                state.update(stale_sent=True, stale_active=True)
                return prior
            return actual_panes()
        def inspect(pid):
            if state["stale_active"]:
                state["stale_active"] = False
                state["transient"] = brief_alive
                raise OSError(error_code, "fixture native identity sample unavailable")
            return actual_inspect(pid)
        def alive(pid):
            if state["transient"]:
                state.update(transient=False, live_seen=True)
                return True
            return actual_alive(pid)
        def kill(pid, sig):
            result = actual_kill(pid, sig)
            if pid == entry["pid"] and sig == signal.SIGTERM:
                state["stopped"] = True
                deadline = time.monotonic() + 3
                while self.probe.alive(pid) and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertFalse(self.probe.alive(pid), "test daemon did not really exit")
            return result
        with patch.object(self.owner, "_panes", side_effect=panes), patch.object(self.probe, "inspect", side_effect=inspect), \
                patch.object(subject.os, "kill", side_effect=kill), patch.object(self.probe, "alive", side_effect=alive):
            if error_code == errno.ESRCH:
                self.owner.quiesce(entry)
            else:
                with self.assertRaises(OSError) as failure:
                    self.owner.quiesce(entry)
                self.assertEqual(failure.exception.errno, error_code)
        self.assertTrue(state["stale_sent"], "exit race injection was not exercised")
        self.assertEqual(state["live_seen"], brief_alive)
        self.assertFalse(self.probe.alive(entry["pid"]))
        if error_code == errno.ESRCH:
            self.assertEqual(self.panes(), [])
        else:
            self.assertEqual(len(self.panes()), 1, "unknown identity error must retain the owned pane")

    def test_post_signal_live_pid_identity_mismatch_does_not_remove_session(self):
        entry = self.start()
        original_kill = os.kill
        original_inspect = self.probe.inspect
        original_tmux = self.owner._tmux
        state = {"stopping": False}
        def kill(pid, sig):
            if pid == entry["pid"] and sig == signal.SIGTERM:
                # Keep only this test daemon alive so the unknown/reused-identity
                # branch is exercised without signaling an unrelated process.
                state["stopping"] = True
                return None
            return original_kill(pid, sig)
        def inspect(pid):
            observed = original_inspect(pid)
            return dict(observed, birth="different process birth") if state["stopping"] else observed
        with patch.object(subject.os, "kill", side_effect=kill), patch.object(self.probe, "inspect", side_effect=inspect), \
                patch.object(self.owner, "_tmux", side_effect=original_tmux) as tmux:
            with self.assertRaises(ValueError):
                self.owner.quiesce(entry)
            self.assertFalse(any(call.args[0] == "kill-session" for call in tmux.call_args_list))
        self.assertTrue(state["stopping"])
        self.assertTrue(self.probe.alive(entry["pid"]))
        self.assertEqual(self.panes()[0]["pid"], entry["pid"])
        self.owner.quiesce(entry)

    def test_formal_root_or_default_socket_is_rejected_before_process_work(self):
        for root, socket in ((Path("/Users/fixture/Library/Application Support/GridEdge-T"), self.socket),
                             (self.root, "default"), (self.root, "gridedge_codex")):
            with self.subTest(root=str(root), socket=socket), self.assertRaises(ValueError):
                subject.TmuxSupervisorOwner(root, self.launcher_bytes, {self.anchor}, self.probe, self.trust, socket, self.migration_index)
        self.assertEqual(self.panes(), [])

    def test_absent_maintenance_or_drifted_manifest_never_starts(self):
        self.maintenance.rmdir()
        with self.assertRaises(ValueError):
            self.start()
        self.maintenance.mkdir()
        self.manifest.write_bytes(b"unreviewed manifest")
        with self.assertRaises(ValueError):
            self.start()
        self.assertEqual(self.panes(), [])

    def test_receipt_rejects_old_future_wrong_pid_anchor_or_executing_heartbeat(self):
        boundary = datetime.now(timezone.utc) - timedelta(seconds=1)
        entry = dict(pid=42001, manifest_sha256=self.anchor)
        receipt = dict(pid=42001, manifest_sha256=self.anchor, at=datetime.now(timezone.utc).isoformat(),
                       executed=False, window="CLOSED", state={"maintenance": True}, action="BLOCKED_MAINTENANCE")
        with patch.object(subject.files, "regular_read", return_value=json.dumps(receipt).encode()):
            self.assertEqual(self.owner._receipt(entry, boundary, True), receipt)
        invalid = [dict(receipt, pid=42002), dict(receipt, manifest_sha256="0"*64), dict(receipt, executed=True),
            dict(receipt, window="TRADING"), dict(receipt, state={"maintenance": False}), dict(receipt, action="OBSERVE_CLOSED"),
            dict(receipt, at=(boundary-timedelta(seconds=1)).isoformat()),
            dict(receipt, at=(boundary+timedelta(days=1)).isoformat()),
            dict(receipt, at=datetime.now().isoformat())]
        for value in invalid:
            with self.subTest(receipt=value), patch.object(subject.files, "regular_read", return_value=json.dumps(value).encode()), \
                    self.assertRaises(ValueError):
                self.owner._receipt(entry, boundary, True)


if __name__ == "__main__":
    unittest.main()
