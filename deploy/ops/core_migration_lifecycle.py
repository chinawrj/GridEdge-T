#!/usr/bin/env python3
"""Isolated real tmux supervisor lifecycle. Production routing is disabled."""
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import time

import core_migration_files as files
from core_migration_native import command, parse_panes, require, supervisor_identity

SESSION = 'gridedge_supervisor'


class TmuxSupervisorOwner:
    def __init__(self, root, launcher_bytes, allowed_anchors, probe, interpreter_trust, socket_name,
                 migration_index, baseline_launcher_sha256=None):
        self.root = Path(root)
        require(self.root.resolve().is_relative_to(Path('/tmp').resolve()),
                'production lifecycle has not been reviewed or enabled')
        require(re.fullmatch(r'gridedge_migration_e2e_[0-9a-f]{32}', socket_name) is not None,
                'only a fresh isolated lifecycle socket is allowed')
        self.prefix = ['/opt/homebrew/bin/tmux', '-L', socket_name]
        require(files.is_sha(migration_index), 'explicit frozen migration index required')
        self.index, self.socket_name = migration_index, socket_name
        self.launcher_bytes, self.anchors = launcher_bytes, allowed_anchors
        self.baseline_launcher_sha256 = baseline_launcher_sha256 or hashlib.sha256(launcher_bytes).hexdigest()
        require(files.is_sha(self.baseline_launcher_sha256), 'invalid baseline launcher identity')
        self.probe, self.interpreter_trust = probe, interpreter_trust
        self.heartbeat = self.root / 'runtime/session-supervisor-heartbeat.json'
        self.receipt_dir = self.root / 'runtime' / ('core-quiesce-' + self.index)
        self.receipt_path = self.receipt_dir / 'receipt.json'

    def _store_record(self, parent, name, filename, value):
        """Publish a complete immutable directory with NO_REPLACE + parent fsync."""
        parent = files.plain_path(parent)
        fd = os.open(parent, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            target, raw = parent / name, files.canonical(value)
            if not target.exists() and not target.is_symlink():
                temporary = files.prepared_directory(parent, filename, value)
                identity = files.inode(temporary.stat())
                require(files.inode(parent.stat()) == files.inode(os.fstat(fd)), 'record parent replaced')
                require(files.regular_read(temporary / filename) == raw, 'prepared record changed')
                try:
                    files.rename_exclusive(temporary, target, fd)
                except OSError as error:
                    if error.errno != errno.EEXIST:
                        raise
                else:
                    require(files.inode(target.stat()) == identity, 'published record inode changed')
            require(files.inode(parent.stat()) == files.inode(os.fstat(fd)), 'record parent changed')
            directory = os.open(files.plain_path(target), os.O_RDONLY | os.O_NOFOLLOW)
            try:
                require(stat.S_ISDIR(os.fstat(directory).st_mode), 'record target is not a directory')
                document = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
                with os.fdopen(document, 'rb') as source:
                    require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), 'record is not a regular file')
                    identity = files.inode(os.fstat(source.fileno()))
                    require(source.read() == raw, 'immutable record cannot be replaced')
                    os.fsync(source.fileno())
                    files.directory_sync(target)
                    os.fsync(directory)
                    os.fsync(fd)
                    files.plain_path(target / filename)
                    require(files.inode(parent.stat()) == files.inode(os.fstat(fd)), 'record parent changed after fsync')
                    require(files.inode(target.stat()) == files.inode(os.fstat(directory)), 'record directory changed after fsync')
                    require(files.inode((target / filename).stat()) == identity, 'record file changed after fsync')
                    source.seek(0)
                    require(source.read() == raw and files.regular_read(target / filename) == raw,
                            'record bytes changed after fsync')
            finally:
                os.close(directory)
        finally:
            os.close(fd)

    def _read_quiesce(self):
        raw = files.regular_read(self.receipt_path)
        value = json.loads(raw)
        require(isinstance(value, dict) and set(value) == {'schema', 'index', 'root', 'socket', 'session',
                'pane', 'entry', 'launcher_sha256'} and raw == files.canonical(value), 'invalid quiesce record')
        require(value['schema'] == 'gridedge.supervisor-quiesce.v1' and value['index'] == self.index
                and value['root'] == str(self.root) and value['socket'] == self.socket_name
                and value['session'] == SESSION and value['launcher_sha256'] == self.baseline_launcher_sha256,
                'foreign quiesce transaction')
        entry, pane = value['entry'], value['pane']
        require(isinstance(entry, dict) and set(entry) == {'pid', 'birth', 'executable_sha256',
                'executable_inode', 'manifest_sha256'}, 'quiesce owner fields differ')
        files.check_owner({k: v for k, v in entry.items() if k != 'manifest_sha256'})
        require(entry['manifest_sha256'] in self.anchors and isinstance(pane, dict)
                and set(pane) == {'session', 'pane', 'pid', 'dead', 'exit_status'}
                and pane['session'] == SESSION and pane['pid'] == entry['pid']
                and re.fullmatch(r'%[0-9]+', pane['pane']) is not None
                and pane['dead'] is False and pane['exit_status'] == '', 'quiesce pane identity differs')
        return value

    def _terminal_record(self, receipt):
        return dict(schema='gridedge.supervisor-quiesce-terminal.v1',
                    receipt_sha256=hashlib.sha256(files.canonical(receipt)).hexdigest(),
                    pane=dict(receipt['pane'], dead=True, exit_status='0'), observed_pid_absent=True)

    def _validate_terminal(self, receipt):
        expected = self._terminal_record(receipt)
        raw = files.regular_read(self.receipt_dir / 'terminal/terminal.json')
        require(raw == files.canonical(expected), 'clean termination receipt missing or differs')
        self._store_record(self.receipt_dir, 'terminal', 'terminal.json', expected)

    def _tmux(self, *args):
        result = command([*self.prefix, *args])
        require(result.returncode == 0, 'isolated tmux operation failed: ' + args[0])
        return result

    def _panes(self):
        result = command([*self.prefix, 'list-panes', '-a', '-F',
                          '#{session_name}|#{pane_id}|#{pane_pid}|#{pane_dead}|#{pane_dead_status}'])
        if result.returncode != 0:
            require(result.returncode == 1 and (b'No such file or directory' in result.stderr
                    or b'no server running on' in result.stderr), 'isolated tmux absence unknown')
            return []
        panes = [p for p in parse_panes(result.stdout) if p['session'] == SESSION]
        require(len(panes) <= 1, 'supervisor has multiple panes')
        return panes

    def _identity(self, pane):
        require(not pane['dead'], 'supervisor pane is dead')
        observed = self.probe.inspect(pane['pid'])
        if observed is None:
            raise ProcessLookupError(errno.ESRCH, 'supervisor exited during identity verification')
        return supervisor_identity(observed, self.root, self.launcher_bytes, self.anchors,
                                   self.interpreter_trust)

    def quiesce(self, entry):
        if self.receipt_dir.exists() or self.receipt_dir.is_symlink():
            require(self._read_quiesce()['entry'] == entry, 'cannot rebaseline quiesce owner')
            return self.resume_quiescence()
        panes = self._panes()
        require(len(panes) == 1 and self._identity(panes[0]) == entry, 'unreviewed supervisor owner')
        require(hashlib.sha256(files.regular_read(self.root / 'bin/run_session_supervisor.sh')).hexdigest()
                == self.baseline_launcher_sha256, 'baseline launcher changed before quiescence')
        receipt = dict(schema='gridedge.supervisor-quiesce.v1', index=self.index, root=str(self.root),
                       socket=self.socket_name, session=SESSION, pane=dict(panes[0]), entry=dict(entry),
                       launcher_sha256=self.baseline_launcher_sha256)
        self._store_record(self.root / 'runtime', self.receipt_dir.name, 'receipt.json', receipt)
        return self.resume_quiescence()

    def resume_quiescence(self):
        receipt = self._read_quiesce()  # No record is never permission to clean a dead pane.
        # A previous controller can die between directory rename and parent
        # fsync. Revalidate the same immutable bytes and finish durability
        # before any process/session action; existence is not a commit receipt.
        self._store_record(self.root / 'runtime', self.receipt_dir.name, 'receipt.json', receipt)
        entry, original_pane = receipt['entry'], receipt['pane']
        panes = self._panes()
        if not panes:
            self._validate_terminal(receipt)
            require(not self.probe.alive(entry['pid']), 'original process still exists after pane removal')
            return
        require(panes[0]['pane'] == original_pane['pane'] and panes[0]['pid'] == entry['pid'],
                'quiesce retry found another pane')
        if not panes[0]['dead']:
            try:
                require(self._identity(panes[0]) == entry, 'quiesce retry found another owner')
                self._signal(entry, panes[0])
            except OSError as error:
                if error.errno != errno.ESRCH:
                    raise
                # The previous controller may already have sent SIGTERM.
                # Only wait for the same retained pane's terminal proof.
        return self._wait_for_terminal(receipt)

    def _signal(self, entry, pane):
        self._tmux('set-window-option', '-t', '=' + SESSION, 'remain-on-exit', 'on')
        current = self._panes()
        require(len(current) == 1 and current[0]['pane'] == pane['pane'] and current[0]['pid'] == entry['pid'],
                'supervisor pane changed before termination')
        if current[0]['dead']:
            return  # No signal. The caller still requires clean status and PID absence.
        require(self._identity(current[0]) == entry, 'supervisor changed before termination')
        os.kill(entry['pid'], signal.SIGTERM)

    def _wait_for_terminal(self, receipt):
        entry, pane = receipt['entry'], receipt['pane']
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            current = self._panes()
            require(len(current) == 1 and current[0]['pane'] == pane['pane']
                    and current[0]['pid'] == entry['pid'], 'supervisor pane replaced during termination')
            if current[0]['dead']:
                require(current[0]['exit_status'] == '0', 'supervisor did not exit cleanly')
                if not self.probe.alive(entry['pid']):
                    require(self._read_quiesce() == receipt, 'quiesce receipt changed before terminal publication')
                    self._store_record(self.receipt_dir, 'terminal', 'terminal.json', self._terminal_record(receipt))
                    # Recheck the exact retained pane and kernel absence before
                    # removing only this session. Never kill a tmux server.
                    require(self._panes() == current and not self.probe.alive(entry['pid']),
                            'dead pane identity changed before removal')
                    self._tmux('kill-session', '-t', '=' + SESSION)
                    require(self._panes() == [], 'supervisor session survived removal')
                    return
            else:
                try:
                    require(self._identity(current[0]) == entry, 'PID reused before clean exit')
                except OSError as error:
                    if error.errno != errno.ESRCH:
                        raise
                    # After SIGTERM, native executable lookup can be ESRCH
                    # while kill(0) still sees a short-lived unreaped PID.
                    # This permits only bounded observation, never removal.
                    # Actual identity mismatch/EPERM/unknown errors propagate.
            time.sleep(0.1)
        raise TimeoutError('supervisor clean termination exceeded 45 seconds')

    def _receipt(self, entry, after, maintenance):
        value = json.loads(files.regular_read(self.heartbeat))
        at = datetime.fromisoformat(value['at'])
        require(at.tzinfo is not None and after < at <= datetime.now(timezone.utc), 'stale/future heartbeat')
        require(value.get('pid') == entry['pid'] and value.get('manifest_sha256') == entry['manifest_sha256']
                and value.get('executed') is False and value.get('window') == 'CLOSED'
                and value.get('state', {}).get('maintenance') is maintenance,
                'supervisor heartbeat is not a closed non-executing receipt')
        require(value.get('action') == ('BLOCKED_MAINTENANCE' if maintenance else 'OBSERVE_CLOSED'),
                'supervisor is not in expected maintenance state')
        return value

    def start(self, launcher, expected_manifest):
        launcher = Path(launcher)
        require(launcher == self.root / 'bin/run_session_supervisor.sh'
                and files.regular_read(launcher) == self.launcher_bytes
                and expected_manifest in self.anchors, 'start launcher identity differs')
        require(hashlib.sha256(files.regular_read(self.root / 'config/session-supervisor-manifest.json')).hexdigest()
                == expected_manifest, 'start manifest identity differs')
        require((self.root / 'runtime/ths-deployment-maintenance').is_dir(), 'start without maintenance')
        if self.receipt_dir.exists() or self.receipt_dir.is_symlink():
            self.resume_quiescence()
        require(self._panes() == [], 'supervisor session already exists')
        boundary = datetime.now(timezone.utc)
        self._tmux('new-session', '-d', '-s', SESSION, '/bin/sh', str(launcher))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            panes = self._panes()
            require(len(panes) == 1 and not panes[0]['dead'], 'new supervisor disappeared')
            try:
                entry = self._identity(panes[0])
                require(entry['manifest_sha256'] == expected_manifest, 'new supervisor uses wrong anchor')
                self._receipt(entry, boundary, True)
                return entry
            except (ValueError, FileNotFoundError, json.JSONDecodeError):
                # During sh→Python exec, or before its first durable heartbeat,
                # no identity/health claim is made. A bounded wait cannot admit
                # an incorrect owner; it ultimately retains the failed session.
                time.sleep(0.1)
        raise TimeoutError('new supervisor has no exact maintenance heartbeat')

    def verify_released(self, entry, boundary):
        require(not (self.root / 'runtime/ths-deployment-maintenance').exists(), 'maintenance not released')
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            panes = self._panes()
            require(len(panes) == 1 and self._identity(panes[0]) == entry, 'released supervisor identity changed')
            try:
                return self._receipt(entry, boundary, False)
            except (ValueError, FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.1)
        raise TimeoutError('released supervisor has no new CLOSED heartbeat')


if __name__ == '__main__':
    raise SystemExit('Isolated lifecycle library only; no formal process operation is enabled.')
