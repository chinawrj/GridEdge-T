#!/usr/bin/env python3
"""Read-only native ledger evidence for a post-close core migration.

This reader does not authorize, activate, reconcile, launch, or submit orders.
Rust remains responsible for complete bootstrap/account semantic validation.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import plistlib
from zoneinfo import ZoneInfo

import core_identity_migration as migration
import core_migration_files as files

RUN = 'ths-002256-20260819-grid15-opening-v1'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def valid_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


@contextmanager
def read_database(path):
    path = Path(path)
    require(path.is_absolute() and path.is_file(), 'required database missing')
    for parent in (path, *path.parents):
        require(parent == Path('/tmp') or not parent.is_symlink(), 'redirected database path')
    connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        require(connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)],
                'database integrity check failed')
        yield connection
    except sqlite3.Error as error:
        raise ValueError('required ledger/outbox schema or query failed') from error
    finally:
        connection.close()


def read_state(root, run_id=RUN):
    require(run_id == RUN, 'unreviewed migration run')
    root = Path(root)
    with read_database(root / 'runtime/002256-grid.db') as source:
        identity = source.execute('SELECT singleton,instance_id FROM database_identity').fetchall()
        require(len(identity) == 1 and identity[0][0] == 1
                and isinstance(identity[0][1], str) and identity[0][1], 'source database identity invalid')
        source_id = identity[0][1]
        events = source.execute('SELECT sequence_number,event_id,event_type,payload FROM events '
                                'WHERE run_id=? ORDER BY sequence_number', (run_id,)).fetchall()
        require(events and [row[0] for row in events] == list(range(1, len(events) + 1)),
                'journal sequence gap or duplicate')
        require(all(isinstance(row[1], str) and row[1] for row in events)
                and len({row[1] for row in events}) == len(events), 'journal event identity duplicate')
        effective = pending = authorization = activation = None
        auth_id = auth_sequence = None
        used_platforms, used_upgrades = set(), set()
        for sequence, event_id, kind, payload in events:
            data = json.loads(payload)
            require(isinstance(data, dict), 'event payload is not an object')
            if pending is not None and kind != 'PLATFORM_UPGRADE_ACTIVATED':
                raise ValueError('non-activation fact follows pending authorization')
            if kind == 'ALGORITHM_REGISTERED':
                require(effective is None and valid_sha(data.get('platform_sha256')),
                        'invalid or duplicate initial platform')
                effective = data['platform_sha256']
                used_platforms.add(effective)
            elif kind == 'PLATFORM_UPGRADE_AUTHORIZED':
                target = data.get('to_platform_sha256')
                upgrade = data.get('upgrade_id')
                require(effective is not None and data.get('from_platform_sha256') == effective
                        and valid_sha(target) and target not in used_platforms
                        and data.get('target_binary_sha256') == target
                        and type(data.get('expected_head_sequence')) is int
                        and data['expected_head_sequence'] == sequence - 1
                        and isinstance(upgrade, str) and upgrade and upgrade not in used_upgrades,
                        'invalid platform authorization chain')
                pending, authorization, auth_id, auth_sequence = target, data, event_id, sequence
                activation = None
                used_upgrades.add(upgrade)
            elif kind == 'PLATFORM_UPGRADE_ACTIVATED':
                require(pending is not None and sequence == auth_sequence + 1
                        and data.get('authorization_event_id') == auth_id
                        and data.get('authorization_sequence') == auth_sequence
                        and data.get('validated_through_sequence') == auth_sequence
                        and data.get('upgrade_id') == authorization['upgrade_id']
                        and data.get('from_platform_sha256') == effective
                        and data.get('to_platform_sha256') == pending
                        and data.get('observed_platform_sha256') == pending
                        and data.get('paper_reconciled') is True,
                        'activation is not adjacent or does not bind exact authorization')
                effective, pending, activation = pending, None, data
                used_platforms.add(effective)
        require(effective is not None, 'no effective platform fact')
        head = events[-1][0]
        paper = source.execute('SELECT snapshot_json FROM paper_accounts WHERE run_id=?', (run_id,)).fetchall()
        require(len(paper) == 1, 'missing or duplicate independent Paper account')
        account = json.loads(paper[0][0])
        require(isinstance(account, dict) and isinstance(account.get('open_order_ids'), list),
                'independent Paper account shape invalid')

    with read_database(root / 'runtime/002256-outbox-opening-v1.db') as outbox:
        metadata = outbox.execute('SELECT source_database_instance_id,run_id,cursor FROM outbox_metadata').fetchall()
        require(len(metadata) == 1 and metadata[0][:2] == (source_id, run_id), 'foreign outbox source/run')
        cursor = metadata[0][2]
        require(type(cursor) is int and 0 <= cursor <= head, 'outbox cursor exceeds journal')
        history = outbox.execute('SELECT revision,source_database_instance_id,run_id,identity_sha256,'
                                 'identity_json,previous_identity_sha256,binding_kind '
                                 'FROM remote_adapter_binding_history ORDER BY revision').fetchall()
        require(bool(history), 'missing execution binding history')
        previous_sha = None
        latest_binding = None
        for expected_revision, row in enumerate(history, 1):
            revision, bound_source, bound_run, sha, value, previous, kind = row
            require(type(revision) is int and revision == expected_revision and bound_source == source_id
                    and bound_run == run_id and valid_sha(sha) and digest(value) == sha
                    and previous == previous_sha
                    and kind == ('GENESIS' if expected_revision == 1 else 'PLATFORM_UPGRADE'),
                    'execution identity append-only chain invalid')
            latest_binding = json.loads(value)
            require(isinstance(latest_binding, dict) and valid_sha(latest_binding.get('platform_sha256')),
                    'execution identity platform invalid')
            previous_sha = sha
        genesis = outbox.execute('SELECT source_database_instance_id,run_id,identity_sha256,identity_json '
                                 'FROM remote_adapter_binding').fetchall()
        require(genesis == [(source_id, run_id, history[0][3], history[0][4])],
                'immutable genesis projection differs from identity history')
        unresolved = outbox.execute("SELECT count(*) FROM staged_intents s WHERE NOT COALESCE("
            "s.state='SUBMITTED' AND typeof(s.remote_contract_id)='text' "
            "AND length(trim(s.remote_contract_id))>0 "
            "AND typeof(s.source_submitted_sequence)='integer' AND s.source_submitted_sequence>0 "
            "AND (s.cancel_state='CANCELLED' OR EXISTS (SELECT 1 FROM remote_execution_facts f "
            "WHERE f.intent_id=s.intent_id AND f.remote_contract_id=s.remote_contract_id "
            "AND f.request_sha256=s.request_sha256 AND f.terminal_kind='FILLED')), 0)").fetchone()[0]
        latest = history[-1]
    return dict(effective=effective, pending=pending, authorization=authorization, activation=activation,
                authorization_event_id=auth_id, authorization_sequence=auth_sequence,
                binding_revision=latest[0], binding_sha256=latest[3],
                binding_platform=latest_binding['platform_sha256'], binding_previous_sha256=latest[5],
                head=head, cursor=cursor, source_database_instance_id=source_id, integrity_ok=True,
                terminal_ok=unresolved == 0 and not account['open_order_ids'])


class PostCloseBackend:
    """Real filesystem/SQLite backend with an injected, separately reviewed Host.

    Until the OS process/CLI Host is reviewed, this implementation accepts only
    isolated /tmp homes. No default subprocess or production apply path exists.
    """
    def __init__(self, plan, host):
        require(plan.home.resolve().is_relative_to(Path('/tmp').resolve()),
                'production OS Host has not been reviewed or enabled')
        self.plan, self.host, self.locked = plan, host, False
        self.root = plan.root
        self.reserve_path = self.root / 'runtime' / ('core-migration-' + plan.index_sha256) / 'reserve.json'
        self.fence = None

    def _validate(self):
        return migration.validate_stage(self.plan.root, self.plan.home, self.plan.stage,
                                        self.plan.trust, self.plan.index_sha256)

    def _breaker(self):
        return hashlib.sha256(files.regular_read(self.root / 'runtime/android-runner-failures')).hexdigest()

    def _signature(self, path, digest):
        require(self.host.verify_signature(path, digest) is True, 'native signature verification failed')

    def _reservation(self):
        if not self.reserve_path.exists():
            require(not self.reserve_path.is_symlink(), 'redirected reservation')
            return None
        raw = files.regular_read(self.reserve_path)
        value = json.loads(raw)
        require(set(value) == {'schema', 'index', 'baseline', 'original_owner'}
                and value['schema'] == 'gridedge.core-migration-reserve.v1'
                and value['index'] == self.plan.index_sha256 and raw == files.canonical(value),
                'invalid durable migration reservation')
        files.check_baseline(value['baseline'])
        files.check_owner(value['original_owner'])
        baseline, trust = value['baseline'], self.plan.trust
        require(baseline['effective'] == trust['core_sha256']
                and baseline['binding_revision'] == trust['binding_revision']
                and baseline['binding_sha256'] == trust['binding_sha256'], 'reservation trust drift')
        return value

    def _inventory(self):
        inventory = self.host.inventory()
        require(inventory['supervisor_socket'] == 'default'
                and inventory['supervisor_session'] == 'gridedge_supervisor', 'unreviewed tmux owner routing')
        require(isinstance(inventory['supervisors'], list) and len(inventory['supervisors']) <= 1
                and inventory['guards'] == [] and inventory['workers'] == []
                and inventory['launchd_present'] is False, 'post-close worker/launcher not quiescent')
        for supervisor in inventory['supervisors']:
            require(set(supervisor) == {'pid', 'birth', 'executable_sha256', 'executable_inode', 'manifest_sha256'},
                    'incomplete native supervisor identity')
            files.check_owner({k: v for k, v in supervisor.items() if k != 'manifest_sha256'})
            require(supervisor['manifest_sha256'] in (self.plan.trust['manifest_sha256'],
                    self.plan.target_shas[migration.MANIFEST]), 'foreign supervisor command identity')
        return inventory

    @contextmanager
    def exclusive(self):
        require(not self.locked, 'nested migration owner')
        self._validate()
        existing = self._reservation()
        now = self.host.now()
        require(now.tzinfo is not None, 'native clock must be timezone-aware')
        local = now.astimezone(ZoneInfo('Asia/Shanghai'))
        if not 16 <= local.hour < 22:
            state = read_state(self.root)
            target = self.plan.trust['candidate_sha256']
            require(existing is not None and (state['pending'] == target or state['effective'] == target),
                    'new maintenance requires a bounded post-close window')
        # A reserved interrupted migration rolls forward even across a date
        # boundary. It never reauthorizes from the current head or resets gates.
        self._inventory()
        self._signature(self.plan.stage / migration.CORE, self.plan.trust['candidate_sha256'])
        self._signature(self.plan.stage / 'validator', self.plan.trust['validator_sha256'])
        self.fence = files.OwnedFence(self.root, self.plan.index_sha256, self.host.owner, self.host.owner_alive)
        with self.fence:
            self.locked = True
            try:
                maintenance_boundary = self.host.now()
                require(maintenance_boundary.tzinfo is not None, 'native clock must be timezone-aware')
                yield
                state = self.snapshot()
                require(state['effective'] == state['binding_platform'] == state['core']
                        == self.plan.trust['candidate_sha256'] and state['pending'] is None
                        and state['head'] == state['cursor'] and state['supervisors'] == 1
                        and state['manifest'] == self.plan.target_shas[migration.MANIFEST]
                        and state['launcher'] == self.plan.target_shas[migration.LAUNCHER],
                        'cannot release maintenance before exact final identities')
                # Process existence is not acceptance, including a retry whose
                # identities were already published. Require this owner to
                # acknowledge the maintenance acquired by this invocation.
                entry = dict(self._inventory()['supervisors'][0])
                require(self.host.verify_maintenance(dict(entry), maintenance_boundary) is True,
                        'fresh maintenance heartbeat not verified')
                require(self.snapshot() == state
                        and self._inventory()['supervisors'] == [entry],
                        'migration state or supervisor changed during heartbeat verification')
                self.fence.finish()
                # Sample after both lock removals have been durably completed.
                # A failed release receipt is NOT COMPLETE. Do not recreate a
                # released lock or repeat authorization/startup; a retry will
                # revalidate the already-published identities and both receipts.
                try:
                    release_boundary = self.host.now()
                    require(release_boundary.tzinfo is not None
                            and release_boundary >= maintenance_boundary, 'native clock moved backwards')
                    require(self.host.verify_released(dict(entry), release_boundary) is True,
                            'fresh released heartbeat not verified')
                    require(self.snapshot() == dict(state, maintenance=False)
                            and self._inventory()['supervisors'] == [entry],
                            'released migration state or supervisor identity changed')
                except Exception as error:
                    error.add_note('Identities published and maintenance released; runtime acceptance failed. '
                                   'Do not report migration complete or recreate the finished fence.')
                    raise
            finally:
                self.locked = False

    def snapshot(self):
        require(self.locked, 'native snapshot outside exclusive owner')
        self._validate()
        state = read_state(self.root)
        inventory = self._inventory()
        reservation = self._reservation()
        breaker = self._breaker()
        begun = state['pending'] == self.plan.trust['candidate_sha256'] or state['effective'] == self.plan.trust['candidate_sha256']
        if reservation:
            baseline = reservation['baseline']
            require(state['source_database_instance_id'] == baseline['source_database_instance_id']
                    and breaker == baseline['breaker'], 'reserved source or breaker changed')
            if not begun:
                require(state['head'] == baseline['head'], 'journal moved outside reserved migration')
            else:
                authorization = state['authorization']
                require(authorization is not None
                        and authorization['from_platform_sha256'] == baseline['effective']
                        and authorization['to_platform_sha256'] == self.plan.trust['candidate_sha256']
                        and authorization['expected_head_sequence'] == baseline['head']
                        and state['authorization_sequence'] == baseline['head'] + 1
                        and authorization.get('certification_evidence_sha256') == self.plan.trust['certificate_sha256'],
                        'journal authorization does not bind exact reserved stage evidence')
        else:
            require(not begun, 'pending or active target has no durable reservation')
        current_manifest = hashlib.sha256(files.regular_read(self.root / 'config' / migration.MANIFEST)).hexdigest()
        for supervisor in inventory['supervisors']:
            require(supervisor['manifest_sha256'] == current_manifest, 'old supervisor inode owns new manifest')
        state.update(core=hashlib.sha256(files.regular_read(self.root / 'bin' / migration.CORE)).hexdigest(),
                     manifest=current_manifest,
                     launcher=hashlib.sha256(files.regular_read(self.root / 'bin' / migration.LAUNCHER)).hexdigest(),
                     breaker=breaker, migration_breaker=reservation['baseline']['breaker'] if reservation else None,
                     migration_index=self.plan.index_sha256 if begun else None,
                     supervisors=len(inventory['supervisors']), guards=0, workers=0, launchd_present=False,
                     maintenance=(self.root / 'runtime/ths-deployment-maintenance').is_dir())
        return state

    def quiesce(self):
        require(self.locked, 'quiesce outside exclusive owner')
        inventory = self._inventory()
        for supervisor in inventory['supervisors']:
            require(supervisor['manifest_sha256'] == self.plan.trust['manifest_sha256'], 'cannot quiesce foreign supervisor')
            self.host.quiesce_supervisor(dict(supervisor))
        require(self._inventory()['supervisors'] == [], 'old supervisor did not exit cleanly')

    def _reserve_before_authorize(self):
        state = self.snapshot()
        require(state['supervisors'] == 0 and state['pending'] is None
                and state['effective'] == self.plan.trust['core_sha256']
                and state['binding_revision'] == self.plan.trust['binding_revision']
                and state['binding_sha256'] == self.plan.trust['binding_sha256']
                and state['head'] == state['cursor'] and state['terminal_ok'], 'preauthorization gates failed')
        baseline = {key: state[key] for key in ('head', 'source_database_instance_id', 'effective',
                                               'binding_revision', 'binding_sha256', 'breaker')}
        return files.reserve(self.root, self.plan.index_sha256, baseline, self.host.owner)

    def authorize(self):
        self._reserve_before_authorize()
        validator = self.reserve_path.parent / 'validator'
        value = files.regular_read(self.plan.stage / 'validator')
        require(hashlib.sha256(value).hexdigest() == self.plan.trust['validator_sha256'], 'validator drift')
        files.atomic_write(validator, value, mode=0o500)
        self._signature(validator, self.plan.trust['validator_sha256'])
        self.host.run([str(validator), 'authorize-platform-upgrade', '--config',
                       str(self.root / 'config/ths_002256_sim.yaml'), '--run-id', RUN,
                       '--target-binary', str(self.plan.stage / migration.CORE),
                       '--certification-report', str(self.plan.stage / 'certificate.json'),
                       '--outbox', str(self.root / 'runtime/002256-outbox-opening-v1.db'),
                       '--reason-code', 'CORE_COMPLETION_GATE_SAFETY_FIX',
                       '--operator', 'codex-reviewed-core-migration', '--json'])

    def publish(self, name, value):
        state = self.snapshot()
        require(state['supervisors'] == 0 and name in migration.TARGETS
                and value == self.plan.targets[name], 'publication not frozen or not quiescent')
        target = self.root / ('config' if name == migration.MANIFEST else 'bin') / name
        files.atomic_write(target, value, mode=0o400 if name == migration.MANIFEST else 0o555)

    def _worker_command(self, mode):
        require(mode in ('--activate-platform-upgrade-only', '--bind-execution-identity-only'),
                'native migration forbids all other worker modes')
        self.snapshot()
        raw = files.regular_read(self.plan.home / 'Library/LaunchAgents/com.gridedge.ths-sim.plist')
        document = plistlib.loads(raw)
        args = document['ProgramArguments']
        require(isinstance(args, list) and all(isinstance(v, str) for v in args)
                and args[0] == str(self.root / 'bin/run_ths_android_sim.sh'), 'unreviewed worker descriptor')
        require(not any(key in document for key in ('RunAtLoad', 'KeepAlive', 'StartCalendarInterval',
                    'StartInterval', 'WatchPaths', 'QueueDirectories', 'Sockets', 'MachServices')),
                'launchd descriptor must be trigger-free')
        for flag, value in (('--config', str(self.root / 'config/ths_002256_sim.yaml')),
                            ('--run-id', RUN), ('--outbox', str(self.root / 'runtime/002256-outbox-opening-v1.db')),
                            ('--simulation-adapter', 'android')):
            require(args.count(flag) == 1 and args[args.index(flag) + 1] == value, 'worker path or adapter drift')
        require('--activate-platform-upgrade-only' not in args and '--bind-execution-identity-only' not in args,
                'mode already present in launcher descriptor')
        program = self.root / 'bin' / migration.CORE
        self._signature(program, self.plan.trust['candidate_sha256'])
        return [str(program), *args[1:], mode]

    def activate(self):
        self.host.run(self._worker_command('--activate-platform-upgrade-only'))

    def bind(self):
        self.host.run(self._worker_command('--bind-execution-identity-only'))

    def sync(self):
        # The reviewed bind-only command already stages the source before
        # appending its identity. Never add an unreviewed money/UI-capable sync.
        state = self.snapshot()
        require(state['head'] == state['cursor'], 'bind-only did not synchronize outbox')

    def start(self):
        state = self.snapshot()
        target = self.plan.trust['candidate_sha256']
        require(state['supervisors'] == 0 and state['pending'] is None
                and state['effective'] == state['core'] == state['binding_platform'] == target
                and state['manifest'] == self.plan.target_shas[migration.MANIFEST]
                and state['launcher'] == self.plan.target_shas[migration.LAUNCHER]
                and state['head'] == state['cursor'] and state['terminal_ok'], 'not coherent before supervisor start')
        self.host.start_supervisor(self.root / 'bin' / migration.LAUNCHER)


if __name__ == '__main__':
    raise SystemExit('No production OS Host is enabled; read-only evidence/isolated backend only.')
