#!/usr/bin/env python3
"""Publish only the frozen additive dependency supervisor, retaining core identity."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT=Path.home()/'Library/Application Support/GridEdge-T'
NAMES={'session_supervisor.py','start_ths_trusted_session.sh','market_raw_repair.py',
       'market_ingestor.py','gridedge_market_replay','session-supervisor-manifest.json',
       'run_session_supervisor.sh'}

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def sync_directory(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)

def sync_file(path):
    with path.open('rb') as stream:os.fsync(stream.fileno())

def validate(stage,expected):
    if stage.parent!=Path('/tmp').resolve() or not stage.name.startswith('gridedge-supervisor-stage-'):
        raise ValueError('unreviewed staging root')
    index=stage/'stage-sha256.json'
    if index.is_symlink():raise ValueError('staging index is a symlink')
    raw=index.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('staging identity mismatch')
    payload=json.loads(raw)
    if set(payload)!=NAMES or {p.name for p in stage.iterdir()}!=NAMES|{'stage-sha256.json'}:
        raise ValueError('staging file allowlist differs')
    for name,digest in payload.items():
        if (stage/name).is_symlink() or sha(stage/name)!=digest:raise ValueError('staged file identity differs')
    manifest=json.loads((stage/'session-supervisor-manifest.json').read_bytes())
    sources={str(ROOT/'bin'/name):stage/name for name in NAMES if name not in
             {'session-supervisor-manifest.json','run_session_supervisor.sh'}}
    for filename,digest in manifest['files'].items():
        path=Path(filename)
        if sha(sources.get(filename,path))!=digest:raise ValueError('installed baseline changed')
    return payload

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser();parser.add_argument('--stage',required=True)
    parser.add_argument('--stage-sha256',required=True);args=parser.parse_args()
    stage=Path(args.stage).resolve();expected=validate(stage,args.stage_sha256)
    runtime=ROOT/'runtime';lock=runtime/'ths-deployment-coordination.lock'
    if (runtime/'ths-deployment-maintenance').exists():raise ValueError('another deployment owns maintenance')
    lock.mkdir()
    changed=[];backup=runtime/('supervisor-install-'+args.stage_sha256)
    try:
        validate(stage,args.stage_sha256)
        # Additive installation never quiesces, restarts or replaces the core worker.
        if subprocess.run(['/usr/bin/pgrep','-f','^.*python[^ ]* .*'+str(ROOT/'bin/session_supervisor.py')+'( |$)'],
                          capture_output=True).returncode==0:
            raise ValueError('existing supervisor requires an explicit reviewed upgrade')
        backup.mkdir(exist_ok=False)
        sync_directory(runtime)
        for name in sorted(NAMES):
            target=ROOT/('config' if name=='session-supervisor-manifest.json' else 'bin')/name
            if target.is_symlink():raise ValueError('installed additive target is a symlink')
            if target.exists():
                shutil.copy2(target,backup/name)
                sync_file(backup/name)
                sync_directory(backup)
            temporary=target.with_name(target.name+'.stage-'+str(os.getpid()))
            shutil.copyfile(stage/name,temporary)
            if sha(temporary)!=expected[name]:raise ValueError('staged bytes changed while copying')
            temporary.chmod(0o555 if name.endswith('.sh') or name=='gridedge_market_replay' else 0o444)
            with temporary.open('rb') as stream:os.fsync(stream.fileno())
            os.replace(temporary,target);changed.append(target)
            sync_directory(target.parent)
        manifest=ROOT/'config/session-supervisor-manifest.json'
        if sha(manifest)!=expected['session-supervisor-manifest.json']:
            raise ValueError('installed manifest differs from frozen stage index')
        command=[str(Path(sys.executable).resolve()),str(ROOT/'bin/session_supervisor.py'),
                 '--manifest',str(manifest),'--manifest-sha256',sha(manifest)]
        subprocess.run(command+['--verify-manifest-only'],check=True,timeout=15,capture_output=True)
        subprocess.run(command+['--once'],check=True,timeout=45,capture_output=True)
        record=dict(schema='gridedge.supervisor-install.v1',stage_sha256=args.stage_sha256,
                    files={str(p):sha(p) for p in changed},core_worker_sha256=sha(ROOT/'bin/gridedge_ths_live'))
        (backup/'installation.json').write_text(json.dumps(record,sort_keys=True))
        sync_file(backup/'installation.json')
        sync_directory(backup)
        print(json.dumps(record,sort_keys=True))
    except Exception:
        for target in reversed(changed):
            previous=backup/target.name
            if previous.exists():os.replace(previous,target)
            else:target.unlink()
            sync_directory(target.parent)
        raise
    finally:lock.rmdir()
    # The caller starts the exact installed immutable launcher after review.

if __name__=='__main__':main()
