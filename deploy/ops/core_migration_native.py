#!/usr/bin/env python3
"""macOS read-only process evidence. No native migration mutation is enabled.

Actual argv comes from KERN_PROCARGS2, not shell-splitting ps's lossy rendering.
An unreadable or changing process is unknown, never evidence of absence.
"""
import ctypes
from datetime import datetime
import errno
import hashlib
import json
import os
from pathlib import Path
import shlex
import struct
import subprocess
import sys

import core_migration_files as files


def require(value, message):
    if not value:
        raise ValueError(message)


def command(argv):
    return subprocess.run(argv, capture_output=True, timeout=5, check=False,
                          env={**os.environ, 'LC_ALL': 'C'})


def parse_procargs(raw):
    require(len(raw) >= 5, 'short process argv')
    count = struct.unpack_from('=i', raw)[0]
    require(0 < count <= 65536, 'invalid process argc')
    position = raw.find(b'\0', 4)
    require(position > 4, 'process executable path missing')
    position += 1
    while position < len(raw) and raw[position] == 0:
        position += 1
    args = []
    for _ in range(count):
        end = raw.find(b'\0', position)
        require(end >= position, 'truncated process argv')
        args.append(raw[position:end].decode('utf-8', errors='strict'))
        position = end + 1
    require(args[0] != '', 'process argv0 missing')
    return args


class ProcBsdInfo(ctypes.Structure):
    # macOS SDK sys/proc_info.h; MAXCOMLEN=16 from sys/param.h.
    _fields_ = [(name, ctypes.c_uint32) for name in (
        'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid', 'ruid', 'rgid',
        'svuid', 'svgid', 'reserved')]
    _fields_ += [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
    _fields_ += [(name, ctypes.c_uint32) for name in ('nfiles', 'pgid', 'pjobc', 'tdev', 'tpgid')]
    _fields_ += [('nice', ctypes.c_int32), ('start_sec', ctypes.c_uint64), ('start_usec', ctypes.c_uint64)]


class MacProcessProbe:
    def __init__(self, kernel_probe=None):
        require(sys.platform == 'darwin', 'macOS native process probe required')
        self.kernel_probe = kernel_probe
        self.libc = ctypes.CDLL('/usr/lib/libSystem.B.dylib', use_errno=True)
        self.libproc = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        self.libc.sysctl.argtypes = [ctypes.POINTER(ctypes.c_int), ctypes.c_uint,
                                    ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t),
                                    ctypes.c_void_p, ctypes.c_size_t]
        self.libc.sysctl.restype = ctypes.c_int
        self.libproc.proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
        self.libproc.proc_pidpath.restype = ctypes.c_int
        self.libproc.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                             ctypes.c_void_p, ctypes.c_int]
        self.libproc.proc_pidinfo.restype = ctypes.c_int

    def alive(self, pid):
        require(type(pid) is int and pid > 1, 'invalid process id')
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError as error:
            raise ValueError('process existence unknown') from error

    def _argv(self, pid):
        # CTL_KERN=1, KERN_PROCARGS2=49; size bounded by the kernel's own reply.
        mib = (ctypes.c_int * 3)(1, 49, pid)
        size = ctypes.c_size_t()
        if self.libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0:
            raise OSError(ctypes.get_errno(), 'cannot read native process arguments')
        require(4 < size.value <= 16 * 1024 * 1024, 'unbounded native process arguments')
        buffer = ctypes.create_string_buffer(size.value)
        if self.libc.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) != 0:
            raise OSError(ctypes.get_errno(), 'native process arguments changed')
        return parse_procargs(buffer.raw[:size.value])

    def _path(self, pid):
        buffer = ctypes.create_string_buffer(4096)
        if self.libproc.proc_pidpath(pid, buffer, len(buffer)) <= 0:
            raise OSError(ctypes.get_errno(), 'cannot read native executable')
        return Path(buffer.value.decode())

    def _birth(self, pid):
        result = command(['/bin/ps', '-p', str(pid), '-o', 'lstart='])
        if result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip() and self.alive(pid) is False:
            raise ProcessLookupError(errno.ESRCH, 'process exited before birth observation')
        require(result.returncode == 0 and result.stdout.strip(), 'process birth unavailable')
        return result.stdout.decode().strip()

    def arguments(self, pid):
        require(type(pid) is int and pid > 1, 'invalid process id')
        try:
            return self._argv(pid)
        except OSError as error:
            if error.errno == errno.ESRCH and self.alive(pid) is False:
                return None
            raise

    def _bsd(self, pid):
        helper = getattr(self, 'kernel_probe', None)
        if helper is not None:
            path, expected_sha = helper
            path = Path(path)
            before = files.regular_read(path)
            require(hashlib.sha256(before).hexdigest() == expected_sha, 'kernel state helper differs')
            inode = path.stat().st_ino
            result = command([str(path), str(pid)])
            require(result.returncode == 0 and path.stat().st_ino == inode
                    and files.regular_read(path) == before, 'kernel state helper failed or changed')
            value = json.loads(result.stdout)
            require(isinstance(value, dict) and set(value) == {'pid', 'uid', 'status', 'start_sec', 'start_usec'}
                    and all(type(v) is int for v in value.values()) and value['pid'] == pid
                    and value['uid'] >= 0 and value['start_sec'] > 0 and 0 <= value['start_usec'] < 1000000,
                    'kernel state helper returned invalid identity')
            return value
        info = ProcBsdInfo()
        require(ctypes.sizeof(info) == 136, 'unexpected SDK process structure layout')
        size = self.libproc.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
        require(size == ctypes.sizeof(info), 'kernel process terminal state unavailable')
        return {key: getattr(info, key) for key in ('pid', 'uid', 'status', 'start_sec', 'start_usec')}

    def _zombie_ps(self, pid):
        result = command(['/bin/ps', '-p', str(pid), '-o', 'pid=,uid=,lstart=,stat='])
        parts = result.stdout.decode().split()
        require(result.returncode == 0 and len(parts) == 8 and parts[0].isdigit()
                and parts[1].isdigit(), 'terminal process corroboration unavailable')
        return dict(pid=int(parts[0]), uid=int(parts[1]), birth=' '.join(parts[2:7]), state=parts[7])

    def zombie_evidence(self, pid):
        """Prove non-execution, never PID absence or permission to remove a pane."""
        require(type(pid) is int and pid > 1, 'invalid process id')
        before = self._bsd(pid)
        if before['pid'] != pid or before['uid'] != os.getuid() or before['status'] != 5:
            return None
        first, second = self._zombie_ps(pid), self._zombie_ps(pid)
        after = self._bsd(pid)
        require(before == after and first == second and first['pid'] == pid
                and first['uid'] == os.getuid() and first['state'].startswith('Z'),
                'terminal process identity changed')
        birth = ' '.join(datetime.fromtimestamp(before['start_sec']).strftime('%a %b %d %H:%M:%S %Y').split())
        # ps formats a one-digit day without a leading zero.
        require(datetime.strptime(first['birth'], '%a %b %d %H:%M:%S %Y')
                == datetime.strptime(birth, '%a %b %d %H:%M:%S %Y'), 'kernel and ps birth differ')
        return dict(pid=pid, birth=first['birth'], kernel_state=5, uid=os.getuid())

    def inspect(self, pid):
        if not self.alive(pid):
            return None
        birth = self._birth(pid)
        path, args = self._path(pid), self._argv(pid)
        info = path.stat()
        digest = hashlib.sha256(files.regular_read(path)).hexdigest()
        if self.alive(pid) is False:
            raise ProcessLookupError(errno.ESRCH, 'process exited during identity observation')
        require(self._birth(pid) == birth and self._path(pid) == path
                and self._argv(pid) == args and path.stat().st_ino == info.st_ino
                and hashlib.sha256(files.regular_read(path)).hexdigest() == digest,
                'process identity changed during observation')
        return dict(pid=pid, birth=birth, executable_sha256=digest,
                    executable_inode=info.st_ino, executable_path=str(path), argv=args)

    def owner_alive(self, owner):
        files.check_owner(owner)
        try:
            observed = self.inspect(owner['pid'])
            if observed is None:
                return 'ABSENT'
            return 'MATCH' if all(observed[key] == value for key, value in owner.items()) else 'MISMATCH'
        except (OSError, ValueError, subprocess.TimeoutExpired):
            return 'UNKNOWN'


def supervisor_identity(observed, root, launcher, allowed_anchors, interpreter_trust=None):
    """Bind interpreter inode and exact argv to the previously frozen launcher."""
    args = shlex.split(launcher.decode().splitlines()[2])
    require(args[0] == 'exec' and len(args) == 7, 'unreviewed supervisor launcher')
    interpreter = Path(args[1]).resolve(strict=True)
    expected_argv0 = args[1]
    if interpreter_trust is not None:
        require(set(interpreter_trust) == {'launcher_interpreter_sha256', 'native_executable_path',
                'native_executable_sha256', 'expected_argv0'}, 'native interpreter trust allowlist differs')
        require(hashlib.sha256(files.regular_read(interpreter)).hexdigest()
                == interpreter_trust['launcher_interpreter_sha256'], 'launcher interpreter bytes differ')
        interpreter = Path(interpreter_trust['native_executable_path'])
        require(str(interpreter) == observed['executable_path'] and interpreter.is_absolute()
                and hashlib.sha256(files.regular_read(interpreter)).hexdigest()
                == observed['executable_sha256'] == interpreter_trust['native_executable_sha256'],
                'native interpreter bytes or exact path differ')
        expected_argv0 = interpreter_trust['expected_argv0']
        require(expected_argv0 in (args[1], str(interpreter)), 'unreviewed native argv0 alias')
    require(Path(observed['executable_path']).resolve(strict=True) == interpreter.resolve(strict=True)
            and observed['executable_inode'] == interpreter.stat().st_ino
            and hashlib.sha256(files.regular_read(interpreter)).hexdigest() == observed['executable_sha256'],
            'supervisor interpreter differs')
    native = observed['argv']
    require(len(native) == 6 and native[0] == expected_argv0 and native[1:5] == [str(root / 'bin/session_supervisor.py'),
                '--manifest', str(root / 'config/session-supervisor-manifest.json'), '--manifest-sha256']
            and native[5] in allowed_anchors, 'supervisor argv differs')
    return {key: observed[key] for key in ('pid', 'birth', 'executable_sha256', 'executable_inode')} | {
        'manifest_sha256': native[5]}


def parse_panes(raw):
    panes = []
    for line in raw.decode().splitlines():
        fields = line.split('|')
        require(len(fields) == 5 and fields[1].startswith('%') and fields[1][1:].isdigit()
                and fields[2].isdigit() and fields[3] in ('0', '1'), 'invalid tmux pane evidence')
        panes.append(dict(session=fields[0], pane=fields[1], pid=int(fields[2]),
                          dead=fields[3] == '1', exit_status=fields[4]))
    return panes


class NativeInventory:
    """Read-only formal owner inventory; never interprets a scan failure as zero."""
    def __init__(self, root, launcher, allowed_anchors, probe=None, interpreter_trust=None):
        self.root = Path(root)
        self.launcher, self.allowed_anchors = launcher, allowed_anchors
        self.probe = probe or MacProcessProbe()
        self.interpreter_trust = interpreter_trust

    def read(self):
        # ps provides PID/uid only for this user's processes. Its rendered
        # command must never be used to exclude a process from kernel argv reads.
        result = command(['/bin/ps', '-axo', 'pid=,uid=,command='])
        require(result.returncode == 0 and result.stdout.strip(), 'process inventory unavailable')
        supervisors, guards, workers, zombies = [], [], [], []
        names = ('session_supervisor.py', 'gridedge_ths_live', 'run_ths_android_sim.sh',
                 'run_ths_trusted_session_guard.sh')
        for line in result.stdout.decode().splitlines():
            parts = line.strip().split(None, 2)
            require(len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit(), 'truncated ps inventory')
            if int(parts[1]) != os.getuid():
                require(not any(name in parts[2] for name in names), 'foreign runtime owner requires review')
                continue
            pid = int(parts[0])
            try:
                native_args = self.probe.arguments(pid)
            except OSError as error:
                if error.errno != errno.EINVAL:
                    raise
                evidence = self.probe.zombie_evidence(pid)
                require(isinstance(evidence, dict) and set(evidence) == {'pid', 'birth', 'kernel_state', 'uid'}
                        and evidence['pid'] == pid and evidence['uid'] == os.getuid()
                        and type(evidence['kernel_state']) is int and evidence['kernel_state'] == 5
                        and isinstance(evidence['birth'], str) and bool(evidence['birth']),
                        'argv unavailable without positive terminal process evidence')
                zombies.append(evidence)
                continue
            if native_args is None:
                continue  # Kernel existence check, not a missing/failed argv read.
            matches = [name for name in names if any(Path(arg).name == name for arg in native_args)]
            if not matches:
                continue
            observed = self.probe.inspect(int(parts[0]))
            require(observed is not None and observed['argv'] == native_args, 'runtime changed during inventory')
            require(len(matches) == 1, 'ambiguous native runtime process')
            name = matches[0]
            require(str(self.root / 'bin' / name) in observed['argv'], 'runtime path outside reviewed installation')
            if name == 'session_supervisor.py':
                supervisors.append(supervisor_identity(observed, self.root, self.launcher,
                                                       self.allowed_anchors, self.interpreter_trust))
            elif name == 'run_ths_trusted_session_guard.sh':
                guards.append(observed)
            else:
                workers.append(observed)
        label = command(['/bin/launchctl', 'print', f'gui/{os.getuid()}/com.gridedge.ths-sim'])
        require(label.returncode in (0, 113), 'launchd absence is unknown')
        panes_result = command(['/opt/homebrew/bin/tmux', 'list-panes', '-a', '-F',
                               '#{session_name}|#{pane_id}|#{pane_pid}|#{pane_dead}|#{pane_dead_status}'])
        require(panes_result.returncode == 0, 'default tmux inventory unavailable')
        panes = [pane for pane in parse_panes(panes_result.stdout) if pane['session'] == 'gridedge_supervisor']
        require(len(supervisors) <= 1 and len(panes) <= 1, 'multiple supervisor owners or panes')
        if supervisors:
            require(len(panes) == 1 and not panes[0]['dead'] and panes[0]['pid'] == supervisors[0]['pid'],
                    'native supervisor and tmux pane differ')
        elif panes:
            require(panes[0]['dead'] and panes[0]['exit_status'] == '0'
                    and not self.probe.alive(panes[0]['pid']), 'unproven supervisor dead pane')
        return dict(supervisors=supervisors, guards=guards, workers=workers,
                    launchd_present=label.returncode == 0, supervisor_socket='default',
                    supervisor_session='gridedge_supervisor', nonexecuting_zombies=zombies)


def verify_signature(path, expected_sha, team='WM4JXVE5GV'):
    """Exact bytes + strict native signature; bool True is the sole success."""
    path = Path(path)
    before = files.regular_read(path)
    require(hashlib.sha256(before).hexdigest() == expected_sha, 'native signed bytes differ')
    info = path.stat()
    verified = command(['/usr/bin/codesign', '--verify', '--strict', str(path)])
    details = command(['/usr/bin/codesign', '-d', '--verbose=4', str(path)])
    require(verified.returncode == 0 and details.returncode == 0, 'native code signature invalid')
    teams = [line for line in details.stderr.decode().splitlines() if line.startswith('TeamIdentifier=')]
    require(teams == ['TeamIdentifier=' + team], 'native signing team differs')
    require(path.stat().st_ino == info.st_ino and files.regular_read(path) == before,
            'signed executable changed during verification')
    return True


if __name__ == '__main__':
    raise SystemExit('Read-only native identity library; no production apply command.')
