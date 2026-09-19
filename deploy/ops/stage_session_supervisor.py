#!/usr/bin/env python3
"""Freeze an additive supervisor release; no activation or production mutation."""
import hashlib
import json
from pathlib import Path
import os
import shutil
import sys

ROOT=Path.home()/'Library/Application Support/GridEdge-T'
REPO=Path(__file__).resolve().parents[2]

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    os.umask(0o077)
    stage=Path(sys.argv[1]).resolve()
    if stage.parent!=Path('/tmp').resolve() or not stage.name.startswith('gridedge-supervisor-stage-'):
        raise ValueError('staging requires an isolated /tmp/gridedge-supervisor-stage-* directory')
    stage.mkdir(exist_ok=False)
    sources={ROOT/'bin/session_supervisor.py':REPO/'deploy/ops/session_supervisor.py',
             ROOT/'bin/start_ths_trusted_session.sh':REPO/'deploy/start_ths_trusted_session.sh',
             ROOT/'bin/market_raw_repair.py':REPO/'deploy/ops/market_raw_repair.py',
             ROOT/'bin/market_ingestor.py':REPO/'deploy/market_data/ingestor/market_ingestor.py',
             ROOT/'bin/gridedge_market_replay':REPO/'target/release/gridedge_market_replay'}
    for target,source in sources.items():
        shutil.copyfile(source,stage/target.name)
    sdk=Path.home()/'Library/Android/sdk'
    files=[ROOT/'bin/gridedge_ths_live',ROOT/'bin/run_ths_android_sim.sh',
           ROOT/'bin/run_ths_trusted_session_guard.sh',ROOT/'config/ths_002256_sim.yaml',
           Path.home()/'Library/LaunchAgents/com.gridedge.ths-sim.plist',
           sdk/'platform-tools/adb',sdk/'emulator/emulator']
    expected={str(p):digest(p) for p in files}
    expected.update({str(p):digest(stage/p.name) for p in sources})
    manifest=dict(schema='gridedge.session-supervisor.v1',root=str(ROOT),run_id='ths-002256-20260819-grid15-opening-v1',
                  sdk=str(sdk),market_host='192.168.1.201',starter=str(ROOT/'bin/start_ths_trusted_session.sh'),
                  chrome_profile='Default',reviewed_url='https://quote.eastmoney.com/f1.html?newcode=0.002256',
                  calendar='SSE_2026_NOTICE_45',files=expected)
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
