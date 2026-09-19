"""Exercise the queue against a fake manager that refuses noncanonical roots."""
import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

PATH=Path(__file__).with_name('run_isolated_acceptance.py')

class CanonicalRootRecovery(unittest.TestCase):
    def test_alias_roots_complete_both_rounds_without_leaking_to_formal_services(self):
        with tempfile.TemporaryDirectory(prefix='gridedge-queue-contract-') as temporary:
            base=Path(temporary).resolve();repo=base/'repo';evidence=base/'evidence'
            evidence.mkdir();(base/'alias').mkdir()
            manager=repo/'deploy/market_data/local_e2e/manage.py';manager.parent.mkdir(parents=True)
            manager.write_text('''from pathlib import Path
import json
def validated_root(value):return Path(value).resolve()
def validate_candidate_bundle(path):assert path.name=='frozen-fixture'
def start(root):assert root==root.resolve()
def start_shadow(root):
    assert root==root.resolve()
    (root/'state/shadow-result.json').write_text(json.dumps({'exit_code':0}))
def start_browser(root):assert root==root.resolve()
def stop(root):assert root==root.resolve(), 'canonical root required'
def shadow_result_succeeded(root):return json.loads((root/'state/shadow-result.json').read_text())['exit_code']==0
''')
            roots=[]
            for name in ['predecessor','r1','r2']:
                root=base/name;(root/'state').mkdir(parents=True)
                roots.append(str(base/'alias'/'..'/name))
            (base/'predecessor/state/shadow-result.json').write_text('{"exit_code":0}')
            plan=dict(evidence=str(evidence),repo=str(repo),bundle=str(base/'frozen-fixture'),
                manager_sha256=hashlib.sha256(manager.read_bytes()).hexdigest(),
                predecessor=roots[0],roots=roots[1:])
            path=base/'plan.json';payload=json.dumps(plan).encode();path.write_bytes(payload)
            spec=importlib.util.spec_from_file_location('queue_under_test',PATH)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            args=argparse.Namespace(plan=str(path),sha256=hashlib.sha256(payload).hexdigest())
            with patch.object(module.argparse.ArgumentParser,'parse_args',return_value=args), \
                 patch.object(module,'fits',return_value=True),contextlib.redirect_stdout(io.StringIO()):
                module.main()
            progress=json.loads((evidence/'acceptance-progress.json').read_text())
            self.assertEqual(progress['status'],'TWO_ROUNDS_PASSED_REVIEW_REQUIRED')
            self.assertEqual(progress['completed'],['0652-intermediate','0653-r1','0653-r2'])
            for name in progress['completed']:
                self.assertTrue((evidence/name/'evidence-sha256.json').is_file())

if __name__=='__main__':unittest.main()
