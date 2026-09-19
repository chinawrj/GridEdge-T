#!/usr/bin/env python3
"""Own two prepared, nonce-isolated live E2E rounds outside Codex's lifetime."""
import argparse
from datetime import datetime, timedelta
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')

def fits(now):
    if now.date().isoformat() not in ('2026-09-07','2026-09-08'):
        return False
    minute=now.hour*60+now.minute+now.second/60
    return 570<=minute<=674 or 780<=minute<=884

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--plan',required=True)
    parser.add_argument('--sha256',required=True);args=parser.parse_args()
    payload=Path(args.plan).read_bytes()
    if hashlib.sha256(payload).hexdigest()!=args.sha256:raise RuntimeError('plan changed')
    plan=json.loads(payload)
    evidence=Path(plan['evidence']);repo=Path(plan['repo'])
    manager=repo/'deploy/market_data/local_e2e/manage.py'
    frozen=manager.read_bytes()
    if hashlib.sha256(frozen).hexdigest()!=plan['manager_sha256']:raise RuntimeError('manager changed')
    spec=importlib.util.spec_from_file_location('isolated_manage',manager)
    manage=importlib.util.module_from_spec(spec)
    exec(compile(frozen,str(manager),'exec'),manage.__dict__)
    lock=(evidence/'acceptance-owner.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    manage.validate_candidate_bundle(Path(plan['bundle']))
    for value in plan['roots']+[plan['predecessor']]:manage.validated_root(value)
    progress=dict(pid=os.getpid(),plan_sha256=args.sha256,completed=[])
    def save(status):
        progress['status']=status;progress['at']=datetime.now(TZ).isoformat()
        tmp=evidence/'acceptance-progress.tmp';tmp.write_text(json.dumps(progress))
        os.replace(tmp,evidence/'acceptance-progress.json')
        print(json.dumps(progress),flush=True)
    def archive(root,label):
        dest=evidence/label;dest.mkdir(exist_ok=True)
        for source in ['contract.json','bundle-manifest.json','state/shadow-result.json',
                       'state/market-events.ndjson','state/bars.ndjson','logs/shadow.stdout.log',
                       'logs/shadow.stderr.log']:
            path=root/source
            if path.exists():
                target=dest/path.name
                if target.exists() and target.read_bytes()!=path.read_bytes():
                    raise RuntimeError('archived evidence changed')
                if not target.exists():shutil.copy2(path,target)
        hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in dest.iterdir() if p.is_file() and p.name!='evidence-sha256.json'}
        (dest/'evidence-sha256.json').write_text(json.dumps(hashes,sort_keys=True))
        manage.stop(root)
        # Keep stopped candidate roots for review; never delete forensic inputs.
        progress['completed'].append(label)
    predecessor=manage.validated_root(plan['predecessor'])
    while not (predecessor/'state/shadow-result.json').exists():
        save('WAITING_FOR_0652_INTERMEDIATE_RESULT');time.sleep(15)
    archive(predecessor,'0652-intermediate')
    for index,value in enumerate(plan['roots'],1):
        root=manage.validated_root(value);label=f'0653-r{index}'
        if (evidence/label).exists():raise RuntimeError('round already archived; explicit resume required')
        while not fits(datetime.now(TZ)):
            if datetime.now(TZ).date().isoformat()>'2026-09-08':raise RuntimeError('reviewed E2E dates expired')
            save('WAITING_FOR_FULL_REVIEWED_WINDOW_'+label);time.sleep(15)
        if manager.read_bytes()!=frozen:raise RuntimeError('manager drift before start')
        manage.validate_candidate_bundle(Path(plan['bundle']))
        started=time.monotonic();injected=False
        try:
            manage.start(root);manage.start_shadow(root);manage.start_browser(root)
            save('RUNNING_'+label)
            while not (root/'state/shadow-result.json').exists():
                if time.monotonic()-started>980:raise RuntimeError('shadow result deadline missed')
                if index==2 and not injected and time.monotonic()-started>=120:
                    targets=manage.browser_targets(root)
                    pages=[t for t in targets if t.get('type')=='page' and t.get('url')==manage.EASTMONEY_REVIEWED_URL]
                    if len(pages)!=1:raise RuntimeError('missing unique candidate page before recovery injection')
                    before=dict(at=datetime.now(TZ).isoformat(),target_id=pages[0]['targetId'])
                    manage.browser_cdp_command(root,'Target.closeTarget',{'targetId':pages[0]['targetId']})
                    (evidence/'0653-r2-page-loss-injection.json').write_text(json.dumps(before))
                    injected=True;save('INJECTED_ISOLATED_PAGE_LOSS_'+label)
                time.sleep(10)
            result=json.loads((root/'state/shadow-result.json').read_text())
            passed=manage.shadow_result_succeeded(root)
            archive(root,label)
            if not passed:raise RuntimeError('complete shadow acceptance failed: '+label)
        except Exception:
            manage.stop(root);save('FAILED_'+label);raise
    save('TWO_ROUNDS_PASSED_REVIEW_REQUIRED')
    lock.close()

if __name__=='__main__':main()
