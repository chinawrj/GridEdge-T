#!/usr/bin/env python3
"""Freeze an additive supervisor release; no activation or production mutation."""
import hashlib
import argparse
import json
from pathlib import Path
import os
import stat
import sys

ROOT=Path.home()/'Library/Application Support/GridEdge-T'
REPO=Path(__file__).resolve().parents[2]

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def frozen_read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError('stage source is not a regular file')
        return source.read()

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser()
    parser.add_argument('stage')
    parser.add_argument('--baseline-manifest-sha256',required=True)
    args=parser.parse_args()
    stage=Path(args.stage).resolve()
    if stage.parent!=Path('/tmp').resolve() or not stage.name.startswith('gridedge-supervisor-stage-'):
        raise ValueError('staging requires an isolated /tmp/gridedge-supervisor-stage-* directory')
    # A scoped supervisor release may change only the supervisor.  Every
    # companion executable is copied from the currently installed, reviewed
    # identity so an unrelated dirty worktree or freshly built Rust binary can
    # never hitchhike into this sidecar upgrade.
    sources={ROOT/'bin/session_supervisor.py':REPO/'deploy/ops/session_supervisor.py',
             ROOT/'bin/start_ths_trusted_session.sh':ROOT/'bin/start_ths_trusted_session.sh',
             ROOT/'bin/market_raw_repair.py':ROOT/'bin/market_raw_repair.py',
             ROOT/'bin/market_ingestor.py':ROOT/'bin/market_ingestor.py',
             ROOT/'bin/gridedge_market_replay':ROOT/'bin/gridedge_market_replay'}
    sdk=Path.home()/'Library/Android/sdk'
    files=[ROOT/'bin/gridedge_ths_live',ROOT/'bin/run_ths_android_sim.sh',
           ROOT/'bin/run_ths_trusted_session_guard.sh',ROOT/'config/ths_002256_sim.yaml',
           Path.home()/'Library/LaunchAgents/com.gridedge.ths-sim.plist',
           sdk/'platform-tools/adb',sdk/'emulator/emulator']
    baseline_bytes=frozen_read(ROOT/'config/session-supervisor-manifest.json')
    if hashlib.sha256(baseline_bytes).hexdigest()!=args.baseline_manifest_sha256:
        raise ValueError('reviewed baseline manifest identity mismatch')
    baseline=json.loads(baseline_bytes)
    metadata=dict(schema='gridedge.session-supervisor.v1',root=str(ROOT),run_id='ths-002256-20260819-grid15-opening-v1',
                  sdk=str(sdk),market_host='192.168.1.201',starter=str(ROOT/'bin/start_ths_trusted_session.sh'),
                  chrome_profile='Default',reviewed_url='https://quote.eastmoney.com/f1.html?newcode=0.002256',
                  calendar='SSE_2026_NOTICE_45')
    if {key:value for key,value in baseline.items() if key!='files'}!=metadata:
        raise ValueError('reviewed baseline metadata differs')
    if set(baseline['files'])!={str(p) for p in files+list(sources)}:
        raise ValueError('reviewed baseline file allowlist differs')
    # Hash and stage the same frozen bytes. A second read could bless a
    # transient unreviewed inode even when the pathname is later restored.
    frozen={}
    for name,expected_sha in baseline['files'].items():
        value=frozen_read(Path(name))
        if hashlib.sha256(value).hexdigest()!=expected_sha:
            raise ValueError('reviewed baseline file identity mismatch: '+Path(name).name)
        if Path(name) in sources:
            frozen[Path(name).name]=value
    frozen['session_supervisor.py']=frozen_read(REPO/'deploy/ops/session_supervisor.py')
    expected=dict(baseline['files'])
    expected[str(ROOT/'bin/session_supervisor.py')]=hashlib.sha256(frozen['session_supervisor.py']).hexdigest()
    manifest=dict(metadata,files=expected)
    stage.mkdir(exist_ok=False)
    for name,value in frozen.items():
        target=stage/name
        target.write_bytes(value)
        if digest(target)!=expected[str(ROOT/'bin'/name)]:
            raise ValueError('staged bytes differ from reviewed identity')
    path=stage/'session-supervisor-manifest.json';path.write_text(json.dumps(manifest,sort_keys=True))
    expected_sha=digest(path)
    import shlex
    command=[str(Path(sys.executable).resolve()),str(ROOT/'bin/session_supervisor.py'),
             '--manifest',str(ROOT/'config/session-supervisor-manifest.json'),'--manifest-sha256',expected_sha]
    (stage/'run_session_supervisor.sh').write_text('#!/bin/sh\nset -eu\nexec '+shlex.join(command)+'\n')
    hashes={p.name:digest(p) for p in stage.iterdir() if p.is_file()}
    (stage/'stage-sha256.json').write_text(json.dumps(hashes,sort_keys=True))
    for p in stage.iterdir():p.chmod(0o444)
    print(json.dumps(dict(stage=str(stage),manifest_sha256=expected_sha,files=hashes),sort_keys=True))

if __name__=='__main__':main()
