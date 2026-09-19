"""Independent operating-contract cases: no broker, browser, ADB or formal state."""
import importlib.util
from pathlib import Path
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

SPEC = importlib.util.spec_from_file_location('session_supervisor', Path(__file__).with_name('session_supervisor.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

class OperatingContract(unittest.TestCase):
    def at(self, value):
        return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('Asia/Shanghai'))

    def test_app_exit_does_not_remove_next_open_action(self):
        for stamp in ['2026-09-07T09:00:00', '2026-09-08T09:00:00', '2026-09-08T12:55:00']:
            self.assertEqual(MODULE.window(self.at(stamp)), 'PREFLIGHT')
        self.assertEqual(MODULE.window(self.at('2026-09-08T09:35:00')), 'TRADING')

    def test_no_weekend_holiday_year_or_postclose_launch(self):
        for stamp in ['2026-09-06T09:00:00', '2026-09-25T09:00:00',
                      '2027-09-07T09:00:00', '2026-09-07T15:05:00',
                      '2026-09-07T08:59:59', '2026-09-07T12:54:59']:
            self.assertEqual(MODULE.window(self.at(stamp)), 'CLOSED')

    def test_missing_dependency_gets_recovery_without_worker_or_breaker_action(self):
        healthy = dict(identity=True, unlocked=True, maintenance=False, guards=0,
                       workers=0, chrome=True, devices=1, reviewed_device=True,
                       emulator_processes=1, booted=True, mqtt=True, breaker_open=False)
        for key, expected in [('chrome', 'START_CHROME'), ('mqtt', 'WAIT_MQTT')]:
            state = dict(healthy, **{key: False})
            self.assertEqual(MODULE.plan(state), expected)
        self.assertEqual(MODULE.plan(dict(healthy, devices=0, emulator_processes=0,
                                         reviewed_device=False, booted=False)), 'START_EMULATOR')
        self.assertEqual(MODULE.plan(dict(healthy, booted=False)), 'WAIT_ANDROID_BOOT')
        self.assertEqual(MODULE.plan(healthy), 'START_GUARD')

    def test_every_safety_boundary_blocks_recovery(self):
        state = dict(identity=True, unlocked=True, maintenance=False, guards=0,
                     workers=0, chrome=False, devices=0, reviewed_device=False,
                     emulator_processes=0, booted=False, mqtt=True, breaker_open=False)
        for change in [dict(identity=False), dict(unlocked=False), dict(maintenance=True),
                       dict(guards=2), dict(workers=2), dict(devices=2),
                       dict(devices=1, reviewed_device=False), dict(breaker_open=True)]:
            self.assertTrue(MODULE.plan(dict(state, **change)).startswith('BLOCKED_'), change)

    def test_healthy_guard_and_worker_are_never_displaced(self):
        state = dict(identity=True, unlocked=True, maintenance=False, guards=1,
                     workers=1, chrome=True, devices=1, reviewed_device=True,
                     emulator_processes=1, booted=True, mqtt=True, breaker_open=False)
        self.assertEqual(MODULE.plan(state), 'OBSERVE')
        self.assertEqual(MODULE.plan(dict(state, guards=0)), 'START_GUARD')
        self.assertEqual(MODULE.plan(dict(state, workers=0)), 'OBSERVE')

class BootRecoveryContract(unittest.TestCase):
    def state(self):
        return dict(identity=True,unlocked=True,maintenance=False,breaker_open=False,
                    devices=1,emulator_processes=1,reviewed_device=False,
                    reviewed_booting=True,booted=False,workers=0)

    def test_offline_is_booting_but_wrong_device_is_not(self):
        state=self.state()
        self.assertEqual(MODULE.boot_recovery(state,60,False),'WAIT_ANDROID_BOOT')
        self.assertEqual(MODULE.boot_recovery(dict(state,reviewed_booting=False),60,False),'BLOCKED_BOOT_IDENTITY')

    def test_only_one_data_preserving_cold_boot_after_deadline(self):
        state=self.state()
        self.assertEqual(MODULE.boot_recovery(state,121,False),'COLD_BOOT_EMULATOR')
        self.assertEqual(MODULE.boot_recovery(state,121,True),'BLOCKED_ANDROID_BOOT_TIMEOUT')
        self.assertEqual(MODULE.boot_recovery(dict(state,workers=1),121,False),'WAIT_WORKER_EXIT')
        self.assertEqual(MODULE.boot_recovery(dict(state,booted=True),121,True),'READY')

class ManifestContract(unittest.TestCase):
    def fixture(self, temporary):
        import json,hashlib
        home=Path(temporary).resolve()
        root=home/'Library/Application Support/GridEdge-T'
        sdk=home/'Library/Android/sdk'
        files=[root/('bin/'+n) for n in ['gridedge_ths_live','run_ths_android_sim.sh',
               'run_ths_trusted_session_guard.sh','start_ths_trusted_session.sh','session_supervisor.py',
               'market_raw_repair.py','market_ingestor.py','gridedge_market_replay']]
        files.extend([root/'config/ths_002256_sim.yaml',home/'Library/LaunchAgents/com.gridedge.ths-sim.plist',
                      sdk/'platform-tools/adb',sdk/'emulator/emulator'])
        for p in files:
            p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'isolated fixture only')
        manifest=dict(schema='gridedge.session-supervisor.v1',root=str(root),run_id=MODULE.RUN,
            sdk=str(sdk),market_host='192.168.1.201',starter=str(root/'bin/start_ths_trusted_session.sh'),
            chrome_profile='Default',reviewed_url=MODULE.URL,calendar='SSE_2026_NOTICE_45',
            files={str(p):MODULE.sha(p) for p in files})
        path=home/'manifest.json';path.write_text(json.dumps(manifest))
        return home,root,path,manifest

    def test_single_fd_payload_is_the_only_hash_and_parse_input(self):
        import tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='gridedge-supervisor-test-') as tmp:
            home,root,path,manifest=self.fixture(tmp)
            digest=MODULE.sha(path)
            with patch.object(Path,'home',return_value=home),patch.object(MODULE,'__file__',str(root/'bin/session_supervisor.py')):
                with patch.object(Path,'read_text',side_effect=AssertionError('second path read is forbidden')):
                    self.assertEqual(MODULE.load_manifest(path,digest),manifest)

    def test_rehashed_unreviewed_paths_host_and_missing_allowlist_are_rejected(self):
        import tempfile,json
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='gridedge-supervisor-test-') as tmp:
            home,root,path,manifest=self.fixture(tmp)
            for change in [dict(starter='/tmp/unreviewed-starter'),dict(market_host='203.0.113.7'),
                           dict(sdk='/tmp/unreviewed-sdk'),dict(files={}),dict(chrome_profile='Profile 2')]:
                path.write_text(json.dumps(dict(manifest,**change)))
                with patch.object(Path,'home',return_value=home),patch.object(MODULE,'__file__',str(root/'bin/session_supervisor.py')):
                    with self.assertRaises(ValueError):MODULE.load_manifest(path,MODULE.sha(path))

    def test_manifest_symlink_and_wrong_hash_are_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory(prefix='gridedge-supervisor-test-') as tmp:
            _home,_root,path,_manifest=self.fixture(tmp)
            link=Path(tmp)/'link';link.symlink_to(path)
            with self.assertRaises(OSError):MODULE.load_manifest(link,MODULE.sha(path))
            with self.assertRaises(ValueError):MODULE.load_manifest(path,'0'*64)

if __name__ == '__main__':
    unittest.main()
