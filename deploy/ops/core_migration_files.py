#!/usr/bin/env python3
"""Durable file/reservation/fence primitives; no broker, UI, journal or CLI writes.

Callers must supply independently verified process identity/death evidence.
These primitives alone are not an authorized native production publisher.
"""
import ctypes
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def is_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def plain_path(path):
    path = Path(path)
    require(path.is_absolute(), 'absolute path required')
    for parent in (path, *path.parents):
        require(parent == Path('/tmp') or not parent.is_symlink(), 'symlink path rejected')
    return path


def regular_read(path):
    path = plain_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as source:
        require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), 'not a regular file')
        return source.read()


def directory_sync(path):
    fd = os.open(plain_path(path), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        require(stat.S_ISDIR(os.fstat(fd).st_mode), 'not a directory')
        os.fsync(fd)
    finally:
        os.close(fd)


def inode(value):
    return value.st_dev, value.st_ino


def rename_exclusive(source, target, directory_fd=None):
    """Atomic NO_REPLACE, including existing empty directories. Never emulate
    with exists()+rename(), which could overwrite another owner's empty lock.
    Darwin flag is from the installed SDK sys/stdio.h (RENAME_EXCL=0x4).
    """
    source, target = plain_path(source), plain_path(target)
    if directory_fd is not None:
        require(source.parent == target.parent, 'anchored rename requires one parent')
        require(inode(source.parent.stat()) == inode(os.fstat(directory_fd)), 'rename parent replaced')
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin':
        if directory_fd is None:
            operation = libc.renamex_np
            operation.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
            operation.restype = ctypes.c_int
            result = operation(os.fsencode(source), os.fsencode(target), 4)
        else:
            operation = libc.renameatx_np
            operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            operation.restype = ctypes.c_int
            result = operation(directory_fd, os.fsencode(source.name), directory_fd, os.fsencode(target.name), 4)
    elif sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
        operation = libc.renameat2
        operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        fd = -100 if directory_fd is None else directory_fd
        result = operation(fd, os.fsencode(source if directory_fd is None else source.name),
                           fd, os.fsencode(target if directory_fd is None else target.name), 1)
    else:
        raise RuntimeError('atomic exclusive rename unavailable; no unsafe fallback')
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def atomic_write(path, value, mode=0o400, *, checkpoint=lambda _: None):
    path = plain_path(path)
    require(isinstance(value, bytes), 'only frozen bytes may be published')
    require(mode in (0o400, 0o444, 0o500, 0o555, 0o600), 'unreviewed file mode')
    if path.exists():
        require(path.is_file(), 'publication target is not a regular file')
    parent = os.open(plain_path(path.parent), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        parent_identity = inode(os.fstat(parent))
        original = inode(path.stat()) if path.exists() else None
        temporary = '.' + path.name + '.migration-' + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        with os.fdopen(fd, 'wb') as target:
            temporary_identity = inode(os.fstat(target.fileno()))
            target.write(value)
            target.flush()
            os.fchmod(target.fileno(), mode)
            checkpoint('before_file_fsync')
            os.fsync(target.fileno())
            checkpoint('after_file_fsync')
        checkpoint('before_rename')
        plain_path(path)
        require(inode(path.parent.stat()) == parent_identity, 'publication parent replaced')
        current = inode(path.stat()) if path.exists() else None
        require(current == original, 'publication target replaced')
        descriptor = os.open(temporary, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
        with os.fdopen(descriptor, 'rb') as source:
            require(inode(os.fstat(source.fileno())) == temporary_identity and source.read() == value,
                    'staged inode or frozen bytes replaced')
        os.replace(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent)
        checkpoint('after_rename')
        checkpoint('before_dir_fsync')
        os.fsync(parent)
        checkpoint('after_dir_fsync')
        require(inode(plain_path(path.parent).stat()) == parent_identity, 'published parent identity lost')
    finally:
        os.close(parent)


def check_owner(owner):
    require(set(owner) == {'pid', 'birth', 'executable_sha256', 'executable_inode'}, 'owner fields differ')
    require(type(owner['pid']) is int and owner['pid'] > 1, 'invalid owner PID')
    require(type(owner['executable_inode']) is int and owner['executable_inode'] > 0, 'invalid executable inode')
    require(is_sha(owner['executable_sha256']) and isinstance(owner['birth'], str)
            and bool(owner['birth']), 'missing owner identity')


def check_baseline(baseline):
    require(set(baseline) == {'head', 'source_database_instance_id', 'effective',
                             'binding_revision', 'binding_sha256', 'breaker'}, 'baseline fields differ')
    require(type(baseline['head']) is int and baseline['head'] > 0, 'invalid original head')
    require(type(baseline['binding_revision']) is int and baseline['binding_revision'] > 0,
            'invalid original binding revision')
    require(isinstance(baseline['source_database_instance_id'], str)
            and bool(baseline['source_database_instance_id']), 'missing source database identity')
    require(all(is_sha(baseline[key]) for key in ('effective', 'binding_sha256', 'breaker')),
            'invalid baseline digest')


def prepared_directory(runtime, filename, document):
    temporary = Path(tempfile.mkdtemp(prefix='.core-migration-prepared-', dir=plain_path(runtime)))
    atomic_write(temporary / filename, canonical(document))
    directory_sync(temporary)
    return temporary


def reserve(root, index, baseline, owner, *, checkpoint=lambda _: None):
    runtime = plain_path(Path(root) / 'runtime')
    fd = os.open(runtime, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        require(stat.S_ISDIR(os.fstat(fd).st_mode), 'reservation parent is not a directory')
        return _reserve(root, index, baseline, owner, fd, checkpoint=checkpoint)
    finally:
        os.close(fd)


def _reserve(root, index, baseline, owner, runtime_fd, *, checkpoint):
    require(is_sha(index), 'invalid frozen stage index')
    check_baseline(baseline)
    check_owner(owner)
    runtime = plain_path(Path(root) / 'runtime')
    target = runtime / ('core-migration-' + index)
    path = target / 'reserve.json'

    def existing():
        require(inode(runtime.stat()) == inode(os.fstat(runtime_fd)), 'reservation parent replaced')
        raw = regular_read(path)
        prior = json.loads(raw)
        require(set(prior) == {'schema', 'index', 'baseline', 'original_owner'}, 'reserve fields differ')
        require(prior['schema'] == 'gridedge.core-migration-reserve.v1'
                and prior['index'] == index and prior['baseline'] == baseline, 'cannot rebaseline transaction')
        check_owner(prior['original_owner'])
        require(raw == canonical(prior), 'noncanonical or corrupted reservation')
        # The preceding caller may have crashed immediately after rename. An
        # existing pathname is not yet a durable reservation receipt.
        directory_sync(target)
        os.fsync(runtime_fd)
        require(inode(runtime.stat()) == inode(os.fstat(runtime_fd)), 'reservation parent changed after sync')
        return path

    if target.exists() or target.is_symlink():
        return existing()
    document = dict(schema='gridedge.core-migration-reserve.v1', index=index,
                    baseline=dict(baseline), original_owner=dict(owner))
    temporary = prepared_directory(runtime, 'reserve.json', document)
    prepared_identity = inode(temporary.stat())
    checkpoint('reserve.before_publish')
    require(inode(runtime.stat()) == inode(os.fstat(runtime_fd)), 'reservation parent replaced')
    require(inode(temporary.stat()) == prepared_identity
            and regular_read(temporary / 'reserve.json') == canonical(document), 'prepared reservation replaced')
    try:
        rename_exclusive(temporary, target, runtime_fd)
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
        return existing()
    checkpoint('reserve.after_publish')
    checkpoint('reserve.before_dir_fsync')
    os.fsync(runtime_fd)
    checkpoint('reserve.after_dir_fsync')
    require(inode(plain_path(runtime).stat()) == inode(os.fstat(runtime_fd)), 'reservation publication parent lost')
    require(inode(target.stat()) == prepared_identity and regular_read(path) == canonical(document),
            'published reservation identity lost')
    return path


class OwnedFence:
    """Nonblocking dual deployment fence with durable exact owner identities.

    An exited context retains both directories unless finish() was explicit.
    Reclamation requires the same stage and verified ABSENT owner; kill-0
    failure or a reused PID is not accepted as an absence proof.
    """
    def __init__(self, root, index, owner, owner_alive, *, checkpoint=lambda _: None):
        require(is_sha(index), 'invalid stage index')
        check_owner(owner)
        self.runtime = plain_path(Path(root) / 'runtime')
        self.index, self.owner = index, dict(owner)
        self.owner_alive, self.checkpoint = owner_alive, checkpoint
        self.fd = None
        self.runtime_fd = None
        self.identities = {}
        self.finished = False
        self.paths = {'coordination': self.runtime / 'ths-deployment-coordination.lock',
                      'maintenance': self.runtime / 'ths-deployment-maintenance'}
        self.document = dict(schema='gridedge.core-migration-lock.v1', index=index, owner=dict(owner))

    def lock_document(self, path):
        plain_path(path)
        require(path.is_dir(), 'lock is not a directory')
        require({p.name for p in path.iterdir()} == {'owner.json'}, 'unexpected lock contents')
        raw = regular_read(path / 'owner.json')
        value = json.loads(raw)
        require(set(value) == {'schema', 'index', 'owner'}
                and value['schema'] == self.document['schema'] and value['index'] == self.index,
                'foreign lock must be retained')
        check_owner(value['owner'])
        require(raw == canonical(value), 'noncanonical lock owner')
        return value

    def archive(self, path, label, expected, expected_inode):
        require(inode(plain_path(self.runtime).stat()) == inode(os.fstat(self.runtime_fd)),
                'fence parent replaced')
        require(self.lock_document(path) == expected and path.stat().st_ino == expected_inode,
                'lock changed at archive boundary')
        destination = self.runtime / ('.core-migration-' + label + '-' + uuid.uuid4().hex)
        rename_exclusive(path, destination, self.runtime_fd)
        require(self.lock_document(destination) == expected and destination.stat().st_ino == expected_inode,
                'archived lock evidence changed')
        os.fsync(self.runtime_fd)
        return destination

    def install(self, label, path):
        if path.exists() or path.is_symlink():
            value = self.lock_document(path)
            require(self.owner_alive(value['owner']) == 'ABSENT', 'owner not proven absent')
            old_inode = path.stat().st_ino
            require(self.lock_document(path) == value and path.stat().st_ino == old_inode,
                    'lock changed during owner check')
            # All conforming migration reclaimers hold the private flock;
            # ordinary starters cannot pass the still-present deployment lock.
            self.archive(path, 'reclaimed-' + label, value, old_inode)
        temporary = prepared_directory(self.runtime, 'owner.json', self.document)
        prepared_identity = inode(temporary.stat())
        self.checkpoint(label + '.before_publish')
        require(inode(plain_path(self.runtime).stat()) == inode(os.fstat(self.runtime_fd)),
                'fence parent replaced before publication')
        require(inode(temporary.stat()) == prepared_identity
                and self.lock_document(temporary) == self.document, 'prepared lock replaced')
        rename_exclusive(temporary, path, self.runtime_fd)
        self.identities[label] = path.stat().st_ino
        self.checkpoint(label + '.after_publish')
        self.checkpoint(label + '.before_dir_fsync')
        os.fsync(self.runtime_fd)
        self.checkpoint(label + '.after_dir_fsync')
        require(inode(plain_path(self.runtime).stat()) == inode(os.fstat(self.runtime_fd)),
                'fence publication parent lost')
        require(inode(path.stat()) == prepared_identity and self.lock_document(path) == self.document,
                'published lock identity lost')

    def __enter__(self):
        require(self.fd is None and not self.finished, 'fence object cannot be reentered')
        lock = plain_path(self.runtime / 'core-migration-exclusive.lock')
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            require(stat.S_ISREG(os.fstat(descriptor).st_mode), 'invalid migration flock')
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(descriptor)
            raise
        self.fd = descriptor
        try:
            self.runtime_fd = os.open(plain_path(self.runtime), os.O_RDONLY | os.O_NOFOLLOW)
            for label, path in self.paths.items():
                self.install(label, path)
        except BaseException:
            self.release_flock()
            raise
        return self

    def finish(self):
        require(self.fd is not None and not self.finished, 'finish requires live exclusive ownership')
        # Check BOTH before touching either; never remove a foreign successor.
        for label, path in self.paths.items():
            require(self.lock_document(path) == self.document
                    and path.stat().st_ino == self.identities[label], 'lock ownership changed')
        for label in ('maintenance', 'coordination'):
            self.checkpoint('finish.' + label + '.before_rename')
            self.archive(self.paths[label], 'finished-' + label, self.document, self.identities[label])
            self.checkpoint('finish.' + label + '.after_rename')
        self.finished = True

    def release_flock(self):
        if self.runtime_fd is not None:
            os.close(self.runtime_fd)
            self.runtime_fd = None
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None

    def __exit__(self, *_):
        self.release_flock()


if __name__ == '__main__':
    raise SystemExit('Filesystem primitives only; no native production publication CLI.')
