#!/usr/bin/env python3
"""Explicit core/manifest/launcher migration: frozen stage and fixture coordinator.

There is deliberately no native apply backend or production execution CLI.
The existing supervisor-only publisher's permissions are unchanged.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
from types import MappingProxyType
from datetime import datetime

RUN = 'ths-002256-20260819-grid15-opening-v1'
URL = 'https://quote.eastmoney.com/f1.html?newcode=0.002256'
CORE = 'gridedge_ths_live'
MANIFEST = 'session-supervisor-manifest.json'
LAUNCHER = 'run_session_supervisor.sh'
TARGETS = (CORE, MANIFEST, LAUNCHER)
TRUST_KEYS = {'manifest_sha256', 'launcher_sha256', 'core_sha256',
              'binding_revision', 'binding_sha256', 'candidate_sha256',
              'validator_sha256', 'certificate_sha256'}


def sha(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest_valid(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def read(path):
    """Hash and consume the same regular inode; reject parent redirection too."""
    path = Path(path)
    for parent in (path, *path.parents):
        # macOS /tmp is a system alias, not a deployment-controlled redirect.
        require(parent == Path('/tmp') or not parent.is_symlink(), 'symlink in input path')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as source:
        require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), 'input is not regular')
        return source.read()


def check_trust(trust):
    require(set(trust) == TRUST_KEYS, 'explicit trust allowlist differs')
    for key in TRUST_KEYS - {'binding_revision'}:
        require(digest_valid(trust[key]), 'noncanonical trust SHA: ' + key)
    require(type(trust['binding_revision']) is int and trust['binding_revision'] > 0,
            'invalid binding revision')
    require(trust['core_sha256'] != trust['candidate_sha256'], 'core migration must change core')


def layout(root, home):
    require(root.is_absolute() and home.is_absolute(), 'absolute installation paths required')
    require(root == home / 'Library/Application Support/GridEdge-T', 'wrong installation root')
    sdk = home / 'Library/Android/sdk'
    metadata = dict(schema='gridedge.session-supervisor.v1', root=str(root), run_id=RUN,
                    sdk=str(sdk), market_host='192.168.1.201',
                    starter=str(root / 'bin/start_ths_trusted_session.sh'),
                    chrome_profile='Default', reviewed_url=URL, calendar='SSE_2026_NOTICE_45')
    paths = [root / 'bin' / name for name in (
        CORE, 'run_ths_android_sim.sh', 'run_ths_trusted_session_guard.sh',
        'start_ths_trusted_session.sh', 'session_supervisor.py', 'market_raw_repair.py',
        'market_ingestor.py', 'gridedge_market_replay')]
    paths += [root / 'config/ths_002256_sim.yaml',
              home / 'Library/LaunchAgents/com.gridedge.ths-sim.plist',
              sdk / 'platform-tools/adb', sdk / 'emulator/emulator']
    return metadata, paths


def check_manifest(value, root, home, core_sha):
    document = json.loads(value)
    metadata, paths = layout(root, home)
    require({k: v for k, v in document.items() if k != 'files'} == metadata,
            'reviewed metadata differs')
    require(set(document['files']) == {str(p) for p in paths}, 'file allowlist differs')
    require(all(digest_valid(v) for v in document['files'].values()), 'invalid manifest digest')
    require(document['files'][str(root / 'bin' / CORE)] == core_sha, 'baseline core differs')
    return document


def check_launcher(value, root, manifest_sha):
    # Preserve exact reviewed bytes, including interpreter; never regenerate a
    # command from the development environment's Python or caller arguments.
    lines = value.decode().splitlines()
    require(len(lines) == 3 and lines[:2] == ['#!/bin/sh', 'set -eu'],
            'unreviewed launcher structure')
    args = shlex.split(lines[2])
    require(len(args) == 7 and args[0] == 'exec' and Path(args[1]).is_absolute(),
            'unreviewed launcher interpreter')
    require(args[2:] == [str(root / 'bin/session_supervisor.py'), '--manifest',
                        str(root / 'config' / MANIFEST), '--manifest-sha256', manifest_sha],
            'unreviewed launcher command')
    require(value.count(manifest_sha.encode()) == 1, 'ambiguous launcher anchor')


def check_certificate(value, target):
    cert = json.loads(value)
    require(cert.get('certification_profile_version') == 'GRIDEDGE_PLATFORM_UPGRADE_CERTIFICATION_V1'
            and cert.get('run_id') == RUN and cert.get('target_binary_sha256') == target,
            'certification target differs')
    for key in ('full_rebuild_passed', 'paper_reconciliation_passed', 'outbox_v3_to_v4_passed',
                'ambiguous_fill_recovery_passed', 'full_gate_passed'):
        require(cert.get(key) is True, 'certification gate failed: ' + key)
    require(type(cert.get('duplicate_money_action_count')) is int
            and cert['duplicate_money_action_count'] == 0, 'money action duplication')
    require(datetime.fromisoformat(cert['generated_at']).tzinfo is None,
            'certification time must be NaiveDateTime')


def check_stage_location(stage, root, home):
    require(stage.is_absolute() and stage.resolve().is_relative_to(Path('/tmp').resolve()),
            'stage must be isolated under /tmp')
    require(not stage.resolve().is_relative_to(home.resolve())
            and not root.resolve().is_relative_to(stage.resolve()), 'stage overlaps installation')
    for parent in (stage, *stage.parents):
        require(parent == Path('/tmp') or not parent.is_symlink(), 'redirected staging root')


def freeze_stage(root, home, stage, trust, candidate, validator, certificate):
    root, home, stage = Path(root), Path(home), Path(stage)
    check_trust(trust)
    check_stage_location(stage, root, home)
    baseline = read(root / 'config' / MANIFEST)
    require(sha(baseline) == trust['manifest_sha256'], 'reviewed baseline anchor mismatch')
    document = check_manifest(baseline, root, home, trust['core_sha256'])
    launcher = read(root / 'bin' / LAUNCHER)
    require(sha(launcher) == trust['launcher_sha256'], 'reviewed launcher mismatch')
    check_launcher(launcher, root, trust['manifest_sha256'])
    frozen = {}
    for name, expected in document['files'].items():
        value = read(name)
        require(sha(value) == expected, 'installed baseline drift: ' + Path(name).name)
        frozen['baseline-' + Path(name).name] = value
    for name, path, key in ((CORE, candidate, 'candidate_sha256'),
                            ('validator', validator, 'validator_sha256'),
                            ('certificate.json', certificate, 'certificate_sha256')):
        frozen[name] = read(path)
        require(sha(frozen[name]) == trust[key], 'frozen target identity differs: ' + name)
    check_certificate(frozen['certificate.json'], trust['candidate_sha256'])
    frozen['baseline-' + MANIFEST] = baseline
    frozen['baseline-' + LAUNCHER] = launcher
    new_document = dict(document, files=dict(document['files']))
    new_document['files'][str(root / 'bin' / CORE)] = trust['candidate_sha256']
    frozen[MANIFEST] = encoded(new_document)
    frozen[LAUNCHER] = launcher.replace(trust['manifest_sha256'].encode(), sha(frozen[MANIFEST]).encode())
    frozen['trust.json'] = encoded(dict(trust))
    # No installed writes and no owner quiescence. Retain a partial stage on I/O
    # failure as evidence; it has no complete trusted index and cannot apply.
    stage.mkdir(mode=0o700, exist_ok=False)
    for name, value in frozen.items():
        with (stage / name).open('xb') as target:
            target.write(value)
            target.flush()
            os.fsync(target.fileno())
        (stage / name).chmod(0o400)
    index = encoded({name: sha(value) for name, value in frozen.items()})
    with (stage / 'stage-sha256.json').open('xb') as target:
        target.write(index)
        target.flush()
        os.fsync(target.fileno())
    (stage / 'stage-sha256.json').chmod(0o400)
    fd = os.open(stage, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return sha(index)


class MigrationPlan:
    __slots__ = ('root', 'home', 'stage', 'index_sha256', 'trust', 'targets', 'target_shas', '_sealed')

    def __init__(self, root, home, stage, index_sha256, trust, frozen):
        self.root, self.home, self.stage, self.index_sha256 = root, home, stage, index_sha256
        self.trust = MappingProxyType(dict(trust))
        self.targets = MappingProxyType({name: frozen[name] for name in TARGETS})
        self.target_shas = MappingProxyType({name: sha(frozen[name]) for name in TARGETS})
        self._sealed = True

    def __setattr__(self, key, value):
        require(not getattr(self, '_sealed', False), 'immutable migration plan')
        object.__setattr__(self, key, value)


def validate_stage(root, home, stage, trust, index_sha):
    root, home, stage = Path(root), Path(home), Path(stage)
    check_trust(trust)
    check_stage_location(stage, root, home)
    raw = read(stage / 'stage-sha256.json')
    require(digest_valid(index_sha) and sha(raw) == index_sha, 'stage index differs')
    index = json.loads(raw)
    _, paths = layout(root, home)
    names = {'baseline-' + p.name for p in paths}
    names |= {'baseline-' + MANIFEST, 'baseline-' + LAUNCHER, 'trust.json',
              'validator', 'certificate.json', *TARGETS}
    require(set(index) == names and {p.name for p in stage.iterdir()} == names | {'stage-sha256.json'},
            'stage allowlist differs')
    frozen = {name: read(stage / name) for name in names}
    require(all(digest_valid(index[name]) and sha(value) == index[name] for name, value in frozen.items()),
            'staged bytes differ')
    require(json.loads(frozen['trust.json']) == dict(trust), 'stage belongs to another explicit trust')
    old = frozen['baseline-' + MANIFEST]
    require(sha(old) == trust['manifest_sha256'], 'frozen old anchor differs')
    document = check_manifest(old, root, home, trust['core_sha256'])
    old_launcher = frozen['baseline-' + LAUNCHER]
    require(sha(old_launcher) == trust['launcher_sha256'], 'frozen old launcher differs')
    check_launcher(old_launcher, root, trust['manifest_sha256'])
    expected = dict(document, files=dict(document['files']))
    expected['files'][str(root / 'bin' / CORE)] = trust['candidate_sha256']
    require(frozen[MANIFEST] == encoded(expected), 'migration changed companion or metadata')
    require(frozen[LAUNCHER] == old_launcher.replace(trust['manifest_sha256'].encode(),
                                                   sha(frozen[MANIFEST]).encode()), 'launcher changed beyond anchor')
    for name, key in ((CORE, 'candidate_sha256'), ('validator', 'validator_sha256'),
                      ('certificate.json', 'certificate_sha256')):
        require(sha(frozen[name]) == trust[key], 'candidate evidence changed')
    check_certificate(frozen['certificate.json'], trust['candidate_sha256'])
    for filename, expected_sha in document['files'].items():
        require(sha(frozen['baseline-' + Path(filename).name]) == expected_sha, 'frozen baseline differs')
        # Mutable trio is checked against the exact durable transaction phase
        # by the coordinator, not accepted solely because a hash is in this set.
        allowed = {expected_sha}
        if Path(filename).name == CORE:
            allowed.add(trust['candidate_sha256'])
        require(sha(read(filename)) in allowed, 'installed companion drift')
    for name, path, old_sha in ((MANIFEST, root / 'config' / MANIFEST, trust['manifest_sha256']),
                                (LAUNCHER, root / 'bin' / LAUNCHER, trust['launcher_sha256'])):
        require(sha(read(path)) in {old_sha, sha(frozen[name])}, 'installed migration target drift')
    return MigrationPlan(root, home, stage, index_sha, trust, frozen)


def execute_migration(plan, backend):
    """Exercise a fixture backend; native deployment is explicitly disabled.

    backend.exclusive must retain a durable maintenance fence on exceptions.
    A future native backend must additionally prove PID birth/inode absence,
    fsync receipts and independently parsed Rust journal/identity facts.
    """
    require(plan.home.resolve().is_relative_to(Path('/tmp').resolve()),
            'native production migration adapter is not reviewed or enabled')

    def frozen_check():
        checked = validate_stage(plan.root, plan.home, plan.stage, plan.trust, plan.index_sha256)
        require(dict(checked.targets) == dict(plan.targets), 'plan differs from frozen stage')

    frozen_check()  # Bad static inputs never stop a healthy owner.
    t = plan.trust
    old, new = t['core_sha256'], t['candidate_sha256']
    baseline = {'core': old, 'manifest': t['manifest_sha256'], 'launcher': t['launcher_sha256']}
    targets = {'core': new, 'manifest': plan.target_shas[MANIFEST], 'launcher': plan.target_shas[LAUNCHER]}

    def inspect(breaker=None, absent=False):
        frozen_check()
        s = backend.snapshot()
        require(s['maintenance'] is True, 'exclusive maintenance fence is absent')
        require(s['integrity_ok'] is True and s['terminal_ok'] is True, 'ledger/account gates failed')
        require(type(s['head']) is int and type(s['cursor']) is int and 0 <= s['cursor'] <= s['head'],
                'invalid outbox cursor')
        require(all(type(s[k]) is int and s[k] >= 0 for k in ('supervisors', 'guards', 'workers')),
                'unknown owner counts')
        require(type(s['launchd_present']) is bool, 'unknown launchd state')
        require(s['supervisors'] <= 1 and s['guards'] <= 1 and s['workers'] <= 1, 'competing owners')
        if breaker is not None:
            require(s['breaker'] == breaker, 'daily breaker changed')
        require(s['effective'] in (old, new) and s['pending'] in (None, new), 'foreign platform state')
        begun = s['pending'] == new or s['effective'] == new
        require(s['migration_index'] == (plan.index_sha256 if begun else None), 'foreign migration transaction')
        if begun:
            require(s['migration_breaker'] == s['breaker'], 'durable migration breaker changed')
        require(not (s['effective'] == new and s['pending'] is not None), 'pending after activation')
        if not begun:
            require(all(s[k] == v for k, v in baseline.items()), 'unapproved publication before authorization')
            require(s['head'] == s['cursor'], 'preauthorization outbox not synchronized')
        else:
            require(all(s[k] in (baseline[k], targets[k]) for k in baseline), 'unknown published identity')
            if s['effective'] == old:
                require(s['manifest'] == baseline['manifest'] and s['launcher'] == baseline['launcher'],
                        'new supervisor anchor before activation')
            else:
                require(s['core'] == new, 'activated target core missing; rollback forbidden')
                require(s['launcher'] != targets['launcher'] or s['manifest'] == targets['manifest'],
                        'launcher precedes manifest in an unreachable publication phase')
        bound = s['binding_platform'] == new
        if bound:
            require(s['effective'] == new and s['binding_revision'] == t['binding_revision'] + 1
                    and s['binding_previous_sha256'] == t['binding_sha256']
                    and digest_valid(s['binding_sha256']) and s['binding_sha256'] != t['binding_sha256'],
                    'invalid append-only target binding')
            require(s['manifest'] == targets['manifest'] and s['launcher'] == targets['launcher'],
                    'target binding precedes supervisor anchor publication')
        else:
            require(s['binding_platform'] == old and s['binding_revision'] == t['binding_revision']
                    and s['binding_sha256'] == t['binding_sha256'], 'baseline binding drift')
        complete = (s['effective'] == new and bound and all(s[k] == v for k, v in targets.items())
                    and s['head'] == s['cursor'] and not s['launchd_present'])
        if absent or (begun and not complete):
            require(not any(s[k] for k in ('supervisors', 'guards', 'workers', 'launchd_present')),
                    'old or mixed-identity owner remains')
        return s, complete

    with backend.exclusive():
        state, complete = inspect()
        breaker = state['breaker']
        if complete and state['supervisors'] == 1:
            return 'ALREADY_COMPLETE'
        if any(state[k] for k in ('supervisors', 'guards', 'workers', 'launchd_present')):
            backend.quiesce()
        state, _ = inspect(breaker, absent=True)
        if state['effective'] == old and state['pending'] is None:
            backend.authorize()
            state, _ = inspect(breaker, absent=True)
            require(state['pending'] == new, 'authorization receipt missing')
        if state['core'] != new:
            backend.publish(CORE, plan.targets[CORE])
            state, _ = inspect(breaker, absent=True)
            require(state['core'] == new, 'core publication receipt missing')
        if state['effective'] != new:
            backend.activate()
            state, _ = inspect(breaker, absent=True)
            require(state['effective'] == new and state['pending'] is None, 'activation receipt missing')
        for name, field in ((MANIFEST, 'manifest'), (LAUNCHER, 'launcher')):
            if state[field] != targets[field]:
                backend.publish(name, plan.targets[name])
                state, _ = inspect(breaker, absent=True)
                require(state[field] == targets[field], 'publication receipt missing')
        if state['binding_platform'] != new:
            backend.bind()
            state, _ = inspect(breaker, absent=True)
            require(state['binding_platform'] == new, 'execution binding receipt missing')
        if state['head'] != state['cursor']:
            backend.sync()
        state, complete = inspect(breaker, absent=True)
        require(complete, 'migration not coherent after synchronization')
        backend.start()
        state, complete = inspect(breaker)
        require(complete and state['supervisors'] == 1, 'unique supervisor start not verified')
    return 'COMPLETE'


if __name__ == '__main__':
    raise SystemExit('No native production apply adapter is enabled; isolated contract tests only.')
