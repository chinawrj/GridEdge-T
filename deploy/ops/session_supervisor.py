#!/usr/bin/env python3
"""App-independent dependency recovery and reviewed guard bootstrap.

No order API, ledger writer, breaker writer or broker publisher. Raw gap repair
is separately validated and only publishes under exclusive quiescence.
The immutable installation manifest is an allowlist, not permission to promote code.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import re
import signal
import shlex
import stat
import socket
import sqlite3
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
RUN = 'ths-002256-20260819-grid15-opening-v1'
URL = 'https://quote.eastmoney.com/f1.html?newcode=0.002256'
CLOSED = {(1,1),(1,2),(2,16),(2,17),(2,18),(2,19),(2,20),(2,23),(4,6),
          (5,1),(5,4),(5,5),(6,19),(9,25),(10,1),(10,2),(10,5),(10,6),(10,7)}


def window(now):
    now = now.astimezone(TZ)
    if now.year != 2026 or now.weekday() >= 5 or (now.month, now.day) in CLOSED:
        return 'CLOSED'
    minute = now.hour * 60 + now.minute
    if 540 <= minute < 570 or 775 <= minute < 780:
        return 'PREFLIGHT'
    if 570 <= minute < 690 or 780 <= minute < 905:
        return 'TRADING' if minute < 900 else 'CLOSING'
    return 'CLOSED'


def plan(s):
    for key in ['identity', 'unlocked']:
        if not s[key]:
            return 'BLOCKED_' + key.upper()
    if s['maintenance']:
        return 'BLOCKED_MAINTENANCE'
    if s['guards'] > 1 or s['workers'] > 1 or s['devices'] > 1 or s['emulator_processes'] > 1:
        return 'BLOCKED_MULTIPLE_OWNERS'
    if s['devices'] == 1 and not s['reviewed_device'] and not s.get('reviewed_booting', False):
        return 'BLOCKED_ANDROID_IDENTITY'
    if s['breaker_open']:
        return 'BLOCKED_BREAKER'
    if not s['chrome']:
        return 'START_CHROME'
    if s['devices'] == 0 and s['emulator_processes'] == 0:
        return 'START_EMULATOR'
    if not s['booted']:
        return 'WAIT_ANDROID_BOOT'
    if not s['mqtt']:
        return 'WAIT_MQTT'
    if s['guards'] == 0:
        return 'START_GUARD'
    return 'OBSERVE'


def boot_recovery(s, elapsed, attempted):
    if s['booted']:
        return 'READY'
    if not s['identity'] or not s['unlocked'] or s['maintenance'] or s['breaker_open']:
        return 'BLOCKED_BOOT_SAFETY'
    if s['devices'] > 1 or s['emulator_processes'] != 1:
        return 'BLOCKED_BOOT_IDENTITY'
    if s['devices'] == 1 and not (s['reviewed_device'] or s.get('reviewed_booting', False)):
        return 'BLOCKED_BOOT_IDENTITY'
    if elapsed < 120:
        return 'WAIT_ANDROID_BOOT'
    if attempted:
        return 'BLOCKED_ANDROID_BOOT_TIMEOUT'
    if s['workers']:
        return 'WAIT_WORKER_EXIT'
    return 'COLD_BOOT_EMULATOR'


def sha(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError('identity file is missing or not regular')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(argv, timeout=8, check=False, env=None):
    return subprocess.run(argv, timeout=timeout, check=check, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def pids(pattern, exact=False):
    result = command(['/usr/bin/pgrep', '-x' if exact else '-f', pattern])
    if result.returncode not in (0, 1):
        raise RuntimeError('process inventory failed')
    return [int(value) for value in result.stdout.split()]


@contextmanager
def read_db(path):
    if Path(path).is_symlink() or not Path(path).is_file():
        raise ValueError('required ledger/outbox is missing or not regular')
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        yield db
    finally:
        db.close()


def ledger_health(root):
    with read_db(root / 'runtime/002256-grid.db') as db:
        db.execute('BEGIN')
        rows = db.execute('SELECT sequence_number,event_type,event_time,payload FROM events '
                          'WHERE run_id=? ORDER BY sequence_number', (RUN,)).fetchall()
        if not rows or [r[0] for r in rows] != list(range(1, len(rows)+1)):
            raise ValueError('journal sequence integrity failed')
        effective = None
        pending = None
        mode = 'UNKNOWN'
        stages = {}
        latest_bar = None
        for seq, typ, timestamp, payload in rows:
            data = json.loads(payload)
            if typ == 'ALGORITHM_REGISTERED':
                if effective is not None:
                    raise ValueError('duplicate initial platform identity')
                effective = data['platform_sha256']
            elif typ == 'PLATFORM_UPGRADE_AUTHORIZED':
                if pending or data['from_platform_sha256'] != effective:
                    raise ValueError('invalid platform authorization chain')
                pending = data['to_platform_sha256']
            elif typ == 'PLATFORM_UPGRADE_ACTIVATED':
                if data['to_platform_sha256'] != pending or data['from_platform_sha256'] != effective:
                    raise ValueError('invalid platform activation chain')
                effective, pending = pending, None
            elif pending:
                raise ValueError('business event between platform authorization and activation')
            if typ == 'SERVICE_MODE_CHANGED':
                mode = data['mode']
            if typ in ('MARKET_DATA_RECEIVED','MARKET_BAR_DECISIONS_COMMITTED','MARKET_BAR_PROCESSED'):
                key = data['timestamp'].replace('T', ' ')
                stages.setdefault(key, []).append((seq, typ, mode))
                if typ == 'MARKET_BAR_PROCESSED':
                    latest_bar = key
        if pending:
            raise ValueError('pending platform upgrade')
        latest_bar_stages = stages.get(latest_bar, [])
        latest_chain = [(seq, typ) for seq, typ, _ in latest_bar_stages]
        if latest_bar and [x[1] for x in latest_chain] != [
                'MARKET_DATA_RECEIVED','MARKET_BAR_DECISIONS_COMMITTED','MARKET_BAR_PROCESSED']:
            raise ValueError('latest bar lacks one ordered three-stage chain')
        latest_bar_stage_modes = [
            dict(sequence_number=seq, event_type=typ, mode=stage_mode)
            for seq, typ, stage_mode in latest_bar_stages
        ]
        stage_mode_values = [row['mode'] for row in latest_bar_stage_modes]
        if stage_mode_values and all(value == 'RUNNING' for value in stage_mode_values):
            latest_bar_strategy_evaluated = True
        elif stage_mode_values and all(value == 'READ_ONLY' for value in stage_mode_values):
            latest_bar_strategy_evaluated = False
        else:
            latest_bar_strategy_evaluated = None
        account = json.loads(db.execute('SELECT snapshot_json FROM paper_accounts WHERE run_id=?',
                                       (RUN,)).fetchone()[0])
    with read_db(root / 'runtime/002256-outbox-opening-v1.db') as db:
        db.execute('BEGIN')
        cursor = db.execute('SELECT cursor FROM outbox_metadata WHERE run_id=?', (RUN,)).fetchone()[0]
        metadata = db.execute('SELECT source_database_instance_id,run_id,cursor FROM outbox_metadata').fetchall()
        if len(metadata) != 1 or metadata[0][1] != RUN:
            raise ValueError('outbox source/run binding invalid')
        with read_db(root/'runtime/002256-grid.db') as source:
            instance = source.execute('SELECT instance_id FROM database_identity WHERE singleton=1').fetchone()[0]
        if metadata[0][0] != instance:
            raise ValueError('outbox database instance mismatch')
        history = db.execute('SELECT revision,identity_sha256,identity_json,previous_identity_sha256,'
                             'binding_kind,source_database_instance_id,run_id '
                             'FROM remote_adapter_binding_history ORDER BY revision').fetchall()
        previous = None
        for revision, digest, encoded, prior, kind, bound_instance, bound_run in history:
            if (revision != (1 if previous is None else last_revision+1) or prior != previous or
                    hashlib.sha256(encoded.encode()).hexdigest() != digest or
                    bound_instance != instance or bound_run != RUN or
                    kind != ('GENESIS' if previous is None else 'PLATFORM_UPGRADE')):
                raise ValueError('execution identity history integrity failed')
            json.loads(encoded)
            previous, last_revision = digest, revision
        if not history:
            raise ValueError('missing execution identity history')
        binding_row = history[-1]
        binding = json.loads(binding_row[2])
        current = db.execute('SELECT identity_sha256,identity_json,source_database_instance_id,run_id '
                             'FROM remote_adapter_binding').fetchall()
        if current != [(history[0][1], history[0][2], instance, RUN)]:
            raise ValueError('execution identity genesis projection mismatch')
        unresolved = db.execute("SELECT count(*) FROM staged_intents s WHERE s.state != 'SUBMITTED' "
          "OR NOT (s.cancel_state='CANCELLED' OR EXISTS (SELECT 1 FROM remote_execution_facts f WHERE f.intent_id=s.intent_id "
          "AND f.remote_contract_id=s.remote_contract_id AND f.request_sha256=s.request_sha256 "
          "AND f.terminal_kind='FILLED'))").fetchone()[0]
    return dict(head=rows[-1][0], cursor=cursor, mode=mode, latest_bar=latest_bar,
                latest_chain=latest_chain, latest_bar_stage_modes=latest_bar_stage_modes,
                latest_bar_strategy_evaluated=latest_bar_strategy_evaluated,
                paper=account, unresolved=unresolved,
                effective=effective, binding=binding, revision=binding_row[0], binding_sha=binding_row[1])


def load_manifest(path, expected):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError('supervisor manifest is not regular')
        payload = source.read(65537)
    if len(payload) > 65536 or hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError('supervisor manifest identity mismatch')
    data = json.loads(payload)
    if data['schema'] != 'gridedge.session-supervisor.v1' or data['run_id'] != RUN:
        raise ValueError('unreviewed supervisor contract')
    root = Path(data['root'])
    if root != Path.home() / 'Library/Application Support/GridEdge-T':
        raise ValueError('not the reviewed installation root')
    if data['reviewed_url'] != URL or data['calendar'] != 'SSE_2026_NOTICE_45':
        raise ValueError('unreviewed source/calendar')
    if data['chrome_profile'] != 'Default':
        raise ValueError('unreviewed production Chrome profile')
    if data['sdk'] != str(Path.home()/'Library/Android/sdk') or data['market_host'] != '192.168.1.201':
        raise ValueError('unreviewed SDK or broker')
    if data['starter'] != str(root/'bin/start_ths_trusted_session.sh'):
        raise ValueError('unreviewed guard starter')
    required = {str(root/'bin/gridedge_ths_live'), str(root/'bin/run_ths_android_sim.sh'),
                str(root/'bin/run_ths_trusted_session_guard.sh'), str(root/'bin/start_ths_trusted_session.sh'),
                str(root/'bin/session_supervisor.py'), str(root/'config/ths_002256_sim.yaml'),
                str(Path.home()/'Library/LaunchAgents/com.gridedge.ths-sim.plist'),
                str(Path(data['sdk'])/'platform-tools/adb'), str(Path(data['sdk'])/'emulator/emulator')}
    required.update(str(root/'bin'/name) for name in
                    ['market_raw_repair.py','market_ingestor.py','gridedge_market_replay'])
    if set(data['files']) != required:
        raise ValueError('supervisor manifest file allowlist differs')
    for name, value in data['files'].items():
        if sha(name) != value:
            raise ValueError('installed file identity mismatch: ' + Path(name).name)
    if str(Path(__file__).resolve()) not in data['files']:
        raise ValueError('supervisor code is not frozen in manifest')
    return data


def unlocked():
    result = command(['/usr/sbin/ioreg', '-n', 'Root', '-d1', '-a'])
    if result.returncode:
        return False
    root = plistlib.loads(result.stdout)
    if not isinstance(root, dict) or root.get('IOConsoleLocked') is not False:
        return False
    users = root.get('IOConsoleUsers', [])
    active = [u for u in users if u.get('kCGSSessionOnConsoleKey') is True]
    return len(active) == 1 and active[0].get('kCGSessionLoginDoneKey') is True and active[0].get('kCGSSessionUserIDKey') == os.getuid() and \
        active[0].get('CGSSessionScreenIsLocked', False) is False


def power():
    result = command(['/usr/bin/pmset', '-g', 'batt'])
    value = result.stdout.decode()
    level = re.search(r'\b(\d{1,3})%;', value)
    percent = int(level[1]) if level else None
    return dict(known=result.returncode==0 and percent is not None and 0<=percent<=100,
                ac_connected="Now drawing from 'AC Power'" in value,
                battery_percent=percent)


def snapshot(m, now):
    root = Path(m['root'])
    h = ledger_health(root)
    binding = h['binding']
    identity = h['effective'] == m['files'][str(root/'bin/gridedge_ths_live')]
    for key, file in [('platform_sha256', root/'bin/gridedge_ths_live'),
                      ('runner_sha256', root/'bin/run_ths_android_sim.sh'),
                      ('guard_sha256', root/'bin/run_ths_trusted_session_guard.sh'),
                      ('launch_plist_sha256', Path.home()/'Library/LaunchAgents/com.gridedge.ths-sim.plist')]:
        identity = identity and binding[key] == m['files'][str(file)]
    label = command(['/bin/launchctl', 'print', f'gui/{os.getuid()}/com.gridedge.ths-sim'])
    identity = identity and label.returncode == 113
    identity = identity and h['cursor'] <= h['head'] and h['unresolved'] == 0
    identity = identity and not h['paper']['open_order_ids']
    adb = str(Path(m['sdk'])/'platform-tools/adb')
    inventory = command([adb, 'devices']).stdout.decode().splitlines()[1:]
    devices = [line.split() for line in inventory if line.strip()]
    reviewed = len(devices) == 1 and devices[0] == ['emulator-5554', 'device']
    booted = False
    if reviewed:
        avd = command([adb, '-s', 'emulator-5554', 'shell', 'getprop', 'ro.boot.qemu.avd_name']).stdout.strip()
        reviewed = avd == b'THSP_API_32'
        booted = command([adb, '-s', 'emulator-5554', 'shell', 'getprop', 'sys.boot_completed']).stdout.strip() == b'1'
    try:
        with socket.create_connection((m['market_host'], 8883), timeout=3):
            mqtt = True
    except OSError:
        mqtt = False
    breaker = root/'runtime/android-runner-failures'
    breaker_bytes = breaker.read_bytes() if breaker.exists() else b''
    breaker_open = False
    if breaker_bytes:
        match = re.fullmatch(rb'(\d{4}-\d{2}-\d{2}) ([0-9]+)\n', breaker_bytes)
        if not match:
            identity = False
        elif match[1].decode() == now.date().isoformat():
            breaker_open = int(match[2]) >= 3
    guard_path = re.escape(str(root/'bin/run_ths_trusted_session_guard.sh'))
    guards = pids(r'^(/bin/)?(sh|bash|zsh) ' + guard_path + r'( |$)')
    workers = pids('^' + re.escape(str(root/'bin/gridedge_ths_live')) + r'( |$)')
    emulators = pids(r'qemu-system-aarch64 .* -avd THSP_API_32( |$)')
    # qemu commonly prints -avd immediately after argv[0].
    if not emulators:
        emulators = pids(r'qemu-system-aarch64 -avd THSP_API_32( |$)')
    s = dict(identity=identity, unlocked=unlocked(), maintenance=(root/'runtime/ths-deployment-maintenance').exists(),
             guards=len(guards), workers=len(workers), chrome=bool(pids('Google Chrome', exact=True)),
             devices=len(devices), reviewed_device=reviewed, emulator_processes=len(emulators),
             booted=booted, mqtt=mqtt, breaker_open=breaker_open,
             reviewed_booting=(devices == [['emulator-5554', 'offline']] and len(emulators) == 1),
             breaker_sha256=hashlib.sha256(breaker_bytes).hexdigest(), power=power())
    return s, h


def market_health(root, now):
    path = root/'runtime/002256-opening-v1-market-mqtt.jsonl'
    latest = observed = None
    payload = None
    seen_sequences = {}
    seen_ids = {}
    with path.open('rb') as stream:
        # Bound the snapshot at open: a concurrent append belongs to the next
        # check. Scan the complete prefix, since old QoS-1 retries can occupy
        # arbitrarily more than a 1 MiB tail without advancing the source.
        remaining = os.fstat(stream.fileno()).st_size
        while remaining:
            line = stream.readline(remaining)
            if not line:
                raise ValueError('formal market raw truncated during observation')
            remaining -= len(line)
            if not line.endswith(b'\n'):
                break
            envelope = json.loads(line)
            current_payload = bytes(envelope['payload'])
            event = json.loads(current_payload)
            if (event['source']['source_instance_id'] != '8101d65c-bdba-4de3-83e0-8983506f159e' or
                event['source']['source_id'] != 'eastmoney-web-time-sales'):
                raise ValueError('formal market source identity changed')
            sequence = event['source_sequence']
            event_id = event['event_id']
            if type(sequence) is not int or not 0 <= sequence < 2**64:
                raise ValueError('invalid source sequence')
            if not isinstance(event_id, str) or re.fullmatch('[0-9a-f]{64}', event_id) is None:
                raise ValueError('invalid source event identity')
            identity = (envelope['topic'], hashlib.sha256(current_payload).digest())
            if sequence in seen_sequences:
                if seen_sequences[sequence] != identity:
                    raise ValueError('conflicting source sequence retransmission')
                continue
            if event_id in seen_ids:
                raise ValueError('source event identity reused')
            if latest is not None and sequence != latest['source_sequence'] + 1:
                raise ValueError('formal market source gap or unseen reorder')
            seen_sequences[sequence] = identity
            seen_ids[event_id] = sequence
            latest, payload = event, current_payload
            if (event['event_type'] == 'SOURCE_STATUS' and
                event['payload'].get('status') == 'SOURCE_OBSERVED_CURRENT'):
                observed = event
    if latest is None:
        return dict(committed=False, source_fresh=False, reason='NO_SOURCE_RECEIPT')
    event_id = latest['event_id']
    if re.fullmatch('[0-9a-f]{64}', event_id) is None:
        raise ValueError('invalid source event identity')
    query = ("SELECT encode(payload_sha256,'hex'),source_sequence::text FROM market_events "
             "WHERE event_id=decode('"+event_id+"','hex') AND source_instance_id="
             "'8101d65c-bdba-4de3-83e0-8983506f159e' AND source_id='eastmoney-web-time-sales'")
    remote = shlex.join(['/usr/local/bin/docker','exec','gridedge-market-postgres','psql',
                         '-U','gridedge_market','-d','gridedge_market','-Atqc',query])
    result = command(['/usr/bin/ssh','-o','BatchMode=yes','-o','ConnectTimeout=3',
                      '-o','LogLevel=ERROR','192.168.1.201',remote],timeout=8)
    expected = hashlib.sha256(payload).hexdigest()+'|'+str(latest['source_sequence'])
    committed = result.returncode==0 and result.stdout.decode().strip()==expected
    observed_us = observed['payload']['observed_at_us'] if observed else None
    age_us = int(now.timestamp()*1000000)-observed_us if observed_us else None
    return dict(committed=committed, source_fresh=age_us is not None and 0<=age_us<=60000000,
                event_id=event_id, payload_sha256=expected.split('|')[0],
                source_sequence=latest['source_sequence'], observed_at_us=observed_us,
                age_us=age_us, provider=latest['source'].get('provider_version'))


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(f'.tmp.{os.getpid()}')
    with temporary.open('w') as output:
        json.dump(value, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def boot_attempt(path, today):
    attempt = json.loads(path.read_text()) if path.exists() else {}
    if attempt.get('date') != today:
        return {}
    if attempt.get('phase') not in ('RESERVED', 'STOPPED', 'LAUNCHING', 'LAUNCHED'):
        raise ValueError('invalid cold boot recovery phase')
    return attempt


def emulator_identity(m, pid):
    result = command(['/bin/ps', '-p', str(pid), '-o', 'lstart=', '-o', 'command='])
    text = result.stdout.decode().strip()
    executable = str(Path(m['sdk'])/'emulator/qemu/darwin-aarch64/qemu-system-aarch64')
    # Preserve the birth time in the fingerprint; a recycled PID is not the owner.
    if result.returncode or not re.fullmatch(
            r'.{24} +' + re.escape(executable) + r' -avd THSP_API_32(?: .*)?', text):
        raise ValueError('cold boot qemu command identity differs')
    return text


def cold_boot(m):
    root = Path(m['root'])
    path = root/'runtime/supervisor-cold-boot-attempt.json'
    today = datetime.now(TZ).date().isoformat()
    attempt = boot_attempt(path, today)
    pattern = r'qemu-system-aarch64 (.* )?-avd THSP_API_32( |$)'
    workers = '^' + re.escape(str(root/'bin/gridedge_ths_live')) + r'( |$)'
    if pids(workers):
        raise ValueError('cold boot requires worker exit')
    owners = pids(pattern)
    if not attempt:
        if len(owners) != 1:
            raise ValueError('cold boot lost unique reviewed emulator')
        attempt = dict(date=today, phase='RESERVED', pid=owners[0],
                       identity=emulator_identity(m, owners[0]))
        atomic_json(path, attempt)
    if attempt['phase'] == 'LAUNCHED':
        raise ValueError('cold boot already launched today')
    if attempt['phase'] == 'RESERVED':
        if owners:
            if owners != [attempt['pid']] or emulator_identity(m, owners[0]) != attempt['identity']:
                raise ValueError('reserved cold boot owner changed')
            try:
                command([str(Path(m['sdk'])/'platform-tools/adb'), '-s', 'emulator-5554',
                         'emu', 'kill'], timeout=4)
            except subprocess.TimeoutExpired:
                pass
            if pids(pattern):
                if (pids(pattern) != owners or pids(workers) or
                        emulator_identity(m, owners[0]) != attempt['identity']):
                    raise ValueError('cold boot termination identity changed')
                os.kill(owners[0], signal.SIGTERM)
            deadline = time.monotonic()+8
            while pids(pattern):
                if time.monotonic() >= deadline:
                    raise ValueError('reviewed emulator did not stop cleanly')
                time.sleep(0.25)
        attempt['phase'] = 'STOPPED'
        atomic_json(path, attempt)
    if attempt['phase'] == 'LAUNCHING':
        # A crash between spawn and durable receipt requires adoption before retry.
        if len(owners) == 1 and owners != [attempt['pid']]:
            identity = emulator_identity(m, owners[0])
            if not all(flag in identity for flag in ['-no-snapshot-load', '-no-snapshot-save',
                                                     '-gpu swiftshader_indirect']):
                raise ValueError('cold boot replacement flags differ')
            attempt.update(phase='LAUNCHED', launched_at=datetime.now(TZ).isoformat())
            atomic_json(path, attempt)
            return
        requested = datetime.fromisoformat(attempt['launch_requested_at'])
        emulator_parent = pids('^'+re.escape(str(Path(m['sdk'])/'emulator/emulator'))+r'( |$)')
        if owners or emulator_parent or (datetime.now(TZ)-requested).total_seconds() < 30:
            raise ValueError('cold boot launch is awaiting process reconciliation')
        attempt['phase'] = 'STOPPED'
        atomic_json(path, attempt)
    if attempt['phase'] != 'STOPPED' or pids(pattern) or pids(workers):
        raise ValueError('cold boot restart requires stopped exclusive owner')
    attempt.update(phase='LAUNCHING', launch_requested_at=datetime.now(TZ).isoformat())
    atomic_json(path, attempt)
    try:
        launch_emulator(m)
    except OSError:
        attempt['phase'] = 'STOPPED'
        atomic_json(path, attempt)
        raise
    attempt.update(phase='LAUNCHED', launched_at=datetime.now(TZ).isoformat())
    atomic_json(path, attempt)


def launch_emulator(m):
    # No snapshot or user-data deletion. Quick Boot loading/saving is disabled.
    log = Path(m['root'])/'logs/supervised-emulator.log'
    with log.open('ab') as out:
        subprocess.Popen([str(Path(m['sdk'])/'emulator/emulator'), '-avd', 'THSP_API_32',
          '-memory', '4096', '-no-snapshot-load', '-no-snapshot-save', '-no-boot-anim',
          '-gpu', 'swiftshader_indirect'], stdin=subprocess.DEVNULL, stdout=out, stderr=out,
          start_new_session=True)


def raw_module():
    path = Path(__file__).with_name('market_raw_repair.py')
    spec = importlib.util.spec_from_file_location('supervised_raw_repair', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def raw_has_gap(root, now):
    module = raw_module()
    return bool(module.missing_intervals((root/'runtime/002256-opening-v1-market-mqtt.jsonl').read_bytes(),
                                         int(now.timestamp()*1000000)))


def repair_raw(m, manifest_path, manifest_sha256):
    root = Path(m['root'])
    coordination = root/'runtime/ths-deployment-coordination.lock'
    maintenance = root/'runtime/ths-deployment-maintenance'
    workers = '^'+re.escape(str(root/'bin/gridedge_ths_live'))+r'( |$)'
    guards = r'^(/bin/)?(sh|bash|zsh) '+re.escape(str(root/'bin/run_ths_trusted_session_guard.sh'))+r'( |$)'
    coordination.mkdir()
    owns_maintenance = False
    try:
        maintenance.mkdir()
        owns_maintenance = True
        load_manifest(manifest_path, manifest_sha256)
        if pids(workers):
            raise ValueError('raw repair cannot displace a running worker')
        owners = pids(guards)
        if len(owners)>1:
            raise ValueError('raw repair found multiple guards')
        if owners:
            fingerprint = command(['/bin/ps','-p',str(owners[0]),'-o','lstart=','-o','command=']).stdout
            if not fingerprint or pids(guards)!=owners or pids(workers):
                raise ValueError('raw repair guard identity changed')
            if command(['/bin/ps','-p',str(owners[0]),'-o','lstart=','-o','command=']).stdout!=fingerprint:
                raise ValueError('raw repair guard PID was reused')
            os.kill(owners[0], signal.SIGTERM)
        deadline = time.monotonic()+8
        while pids(guards):
            if time.monotonic()>=deadline:
                raise ValueError('raw repair guard did not exit')
            time.sleep(.25)
        def verify():
            load_manifest(manifest_path, manifest_sha256)
            if not coordination.is_dir() or not maintenance.is_dir() or pids(workers) or pids(guards):
                raise ValueError('raw repair lost exclusive quiescence')
            state, _ = snapshot(m, datetime.now(TZ))
            if not state['identity'] or not state['unlocked'] or state['breaker_open']:
                raise ValueError('raw repair safety preflight differs')
        verify()
        module = raw_module()
        path = root/'runtime/002256-opening-v1-market-mqtt.jsonl'
        original = path.read_bytes()
        cutoff = int(datetime.now(TZ).timestamp()*1000000)
        records = module.local_records(original, cutoff)
        if not module.missing_intervals(original, cutoff):
            return
        rows = module.committed_rows(records[0][0],max(record[0] for record in records))
        candidate, audit = module.prepare(original,rows,cutoff)
        evidence = root/'runtime/raw-repair-evidence'/f'{audit["original_sha256"]}-{os.getpid()}-{time.time_ns()}'
        evidence.mkdir(parents=True)
        audit['semantic_replays'] = module.semantic_replay(candidate,evidence,root/'bin/gridedge_market_replay')
        verify()
        approved_replay = m['files'][str(root/'bin/gridedge_market_replay')]
        if any(row['replay_binary_sha256'] != approved_replay for row in audit['semantic_replays']):
            raise ValueError('raw repair replay binary was not the frozen approved artifact')
        atomic_json(evidence/'audit.json', audit)
        module.fsync_directory(evidence)
        backup = module.publish(path,original,candidate,audit,verify)
        atomic_json(evidence/'published.json',dict(candidate_sha256=audit['candidate_sha256'],backup=str(backup),
                                                  at=datetime.now(TZ).isoformat()))
    finally:
        if owns_maintenance:
            maintenance.rmdir()
        coordination.rmdir()


def apply(action, m, manifest_path=None, manifest_sha256=None):
    # Each action starts only a reviewed dependency or the original serialized
    # guard starter. There is intentionally no worker/reconciliation/order call.
    if action == 'START_CHROME':
        command(['/usr/bin/open', '-a', 'Google Chrome', '--args',
                 '--profile-directory=Default', URL], check=True)
    elif action in ('START_EMULATOR', 'COLD_BOOT_EMULATOR'):
        if action == 'COLD_BOOT_EMULATOR':
            cold_boot(m)
        else:
            launch_emulator(m)
    elif action == 'START_GUARD':
        plist = plistlib.loads((Path.home()/'Library/LaunchAgents/com.gridedge.ths-sim.plist').read_bytes())
        args = plist['ProgramArguments']
        masked = args[args.index('--android-masked-account')+1]
        env = dict(os.environ, GRIDEDGE_DEPLOYMENT_ROOT=m['root'], GRIDEDGE_ANDROID_SDK_ROOT=m['sdk'],
                   GRIDEDGE_MARKET_HOST=m['market_host'], GRIDEDGE_ANDROID_MASKED_ACCOUNT=masked,
                   GRIDEDGE_REVIEWED_SESSION_DATE=datetime.now(TZ).date().isoformat(),
                   GRIDEDGE_USER_HOME=str(Path.home()), GRIDEDGE_TMUX_SESSION='ths_guard_supervised', TZ='Asia/Shanghai',
                   GRIDEDGE_SUPERVISOR_MANIFEST=str(manifest_path), GRIDEDGE_SUPERVISOR_MANIFEST_SHA256=str(manifest_sha256))
        # Remove test knobs and non-reviewed tmux routing inherited by accident.
        for key in list(env):
            if key.startswith('GRIDEDGE_') and key not in {
                'GRIDEDGE_DEPLOYMENT_ROOT','GRIDEDGE_ANDROID_SDK_ROOT','GRIDEDGE_MARKET_HOST',
                'GRIDEDGE_ANDROID_MASKED_ACCOUNT','GRIDEDGE_REVIEWED_SESSION_DATE',
                'GRIDEDGE_USER_HOME','GRIDEDGE_TMUX_SESSION','GRIDEDGE_SUPERVISOR_MANIFEST','GRIDEDGE_SUPERVISOR_MANIFEST_SHA256'}:
                del env[key]
        command(['/bin/sh', m['starter']], timeout=12, check=True, env=env)
    elif action == 'REPAIR_RAW':
        repair_raw(m, manifest_path, manifest_sha256)


def write_record(path, data):
    with Path(path).open('a', encoding='utf-8') as out:
        out.write(json.dumps(data, ensure_ascii=False, sort_keys=True) + '\n')
        out.flush()
        os.fsync(out.fileno())


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--verify-manifest-only', action='store_true')
    parser.add_argument('--once', action='store_true', help='read-only preflight; never launch a process')
    args = parser.parse_args()
    m = load_manifest(args.manifest, args.manifest_sha256)
    if args.verify_manifest_only:
        return
    root = Path(m['root'])
    lock = (root/'runtime/session-supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    running = True
    def stop(_signal, _frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous_action = None
    last_attempt = {}
    boot_started = None
    boot_attempted_path = root/'runtime/supervisor-cold-boot-attempt.json'
    while running:
        now = datetime.now(TZ)
        try:
            m = load_manifest(args.manifest, args.manifest_sha256)
            current_window = window(now)
            s, h = snapshot(m, now)
            action = plan(s)
            if current_window == 'CLOSED' and not action.startswith('BLOCKED_'):
                action = 'OBSERVE_CLOSED'
            attempt = boot_attempt(boot_attempted_path, now.date().isoformat())
            resuming = attempt.get('phase') in ('RESERVED','STOPPED','LAUNCHING')
            if action == 'WAIT_ANDROID_BOOT':
                if boot_started is None:
                    age = max(0, (now-datetime.fromisoformat(attempt['launched_at'])).total_seconds()) if attempt.get('phase') == 'LAUNCHED' else 0
                    boot_started = time.monotonic()-age
                attempted = attempt.get('phase') == 'LAUNCHED'
                action = boot_recovery(s, time.monotonic()-boot_started, attempted)
            elif s['booted']:
                boot_started = None
            if resuming and action in ('START_EMULATOR','WAIT_ANDROID_BOOT','OBSERVE','START_GUARD') and not s['workers']:
                action = 'COLD_BOOT_EMULATOR'
            if (current_window != 'CLOSED' and s['workers']==0 and
                    action in ('START_GUARD','OBSERVE') and raw_has_gap(root,now)):
                action = 'REPAIR_RAW'
            try:
                market = market_health(root, now)
            except Exception as error:
                market = dict(committed=False, source_fresh=False, error=type(error).__name__)
            if current_window == 'TRADING' and action == 'OBSERVE' and not (market['committed'] and market['source_fresh']):
                action = 'SOURCE_UNAVAILABLE_WAITING_COLLECTOR_WATCHDOG'
            report = dict(at=now.isoformat(), pid=os.getpid(), manifest_sha256=args.manifest_sha256,
                          window=current_window, action=action, state=s, market=market,
                          evidence_kind='DEPENDENCY_SUPERVISION_NOT_TRADING_PERMIT',
                          health={key: value for key,value in h.items() if key != 'binding'},
                          executed=False)
            if not args.once and action in ('START_CHROME','START_EMULATOR','START_GUARD','COLD_BOOT_EMULATOR','REPAIR_RAW'):
                elapsed = time.monotonic() - last_attempt.get(action, -1000)
                if elapsed >= (150 if action == 'START_EMULATOR' else 30):
                    # Recheck frozen files, identity and presence immediately before action.
                    m = load_manifest(args.manifest, args.manifest_sha256)
                    fresh, _ = snapshot(m, datetime.now(TZ))
                    fresh_action = plan(fresh)
                    if (action == 'REPAIR_RAW' and fresh['workers']==0 and
                            fresh_action in ('START_GUARD','OBSERVE') and raw_has_gap(root,datetime.now(TZ))):
                        fresh_action = action
                    attempt = boot_attempt(boot_attempted_path, now.date().isoformat())
                    if action == 'COLD_BOOT_EMULATOR':
                        if attempt.get('phase') in ('RESERVED','STOPPED','LAUNCHING') and fresh_action in ('START_EMULATOR','WAIT_ANDROID_BOOT','OBSERVE','START_GUARD') and not fresh['workers']:
                            fresh_action = action
                        elif fresh_action == 'WAIT_ANDROID_BOOT':
                            fresh_action = boot_recovery(fresh, time.monotonic()-boot_started, attempt.get('phase') == 'LAUNCHED')
                    if fresh_action == action and window(datetime.now(TZ)) != 'CLOSED':
                        last_attempt[action] = time.monotonic()
                        apply(action, m, args.manifest, args.manifest_sha256)
                        if action in ('COLD_BOOT_EMULATOR', 'START_EMULATOR'):
                            boot_started = time.monotonic()
                        report['executed'] = True
            write_record(root/'logs/session-supervisor.jsonl', report)
            ready = root/'runtime/session-supervisor-heartbeat.json'
            temporary = ready.with_suffix(f'.tmp.{os.getpid()}')
            temporary.write_text(json.dumps(report, ensure_ascii=False))
            os.replace(temporary, ready)
            if action != previous_action or args.once:
                print(json.dumps({k:report[k] for k in ['at','pid','window','action','executed']}, ensure_ascii=False), flush=True)
            previous_action = action
            if args.once and action.startswith('BLOCKED_'):
                raise ValueError('read-only supervisor preflight blocked: '+action)
        except Exception as error:
            # The exception class is safe to report; subprocess args can contain
            # a masked account, and arbitrary driver stdout is never copied here.
            write_record(root/'logs/session-supervisor.jsonl',
                         dict(at=now.isoformat(), pid=os.getpid(), action='BLOCKED_PROBE', error=type(error).__name__,
                              reason=str(error) if isinstance(error, ValueError) else type(error).__name__))
            print('BLOCKED_PROBE ' + type(error).__name__, flush=True)
            if args.once:
                raise
        if args.once:
            break
        for _ in range(15):
            if not running:
                break
            time.sleep(1)

if __name__ == '__main__':
    main()
