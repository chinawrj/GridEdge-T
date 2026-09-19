"""Independent missing-receipt regressions; isolated files/SQLite, no native UI.

The OS double deliberately separates process existence from fresh maintenance
and release acknowledgements. A live PID must never substitute for either.
"""
import copy
import unittest

import test_postclose_backend as fixtures


class PostcloseHeartbeatIndependentTest(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.PostcloseBackendTest()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.seen = []
        self.maintenance_result = True
        self.release_result = True
        self.maintenance_effect = None
        self.case.host.verify_maintenance = self.verify_maintenance
        self.case.host.verify_released = self.verify_released

    def verify_maintenance(self, entry, boundary):
        self.assertIsNotNone(boundary.tzinfo)
        self.assertEqual(entry, self.case.host.processes['supervisors'][0])
        for name in ('ths-deployment-maintenance', 'ths-deployment-coordination.lock'):
            self.assertTrue((self.case.root / 'runtime' / name).is_dir())
        self.seen.append(('maintenance', copy.deepcopy(entry), boundary))
        if self.maintenance_effect:
            self.maintenance_effect()
        if isinstance(self.maintenance_result, Exception):
            raise self.maintenance_result
        return self.maintenance_result

    def verify_released(self, entry, boundary):
        self.assertIsNotNone(boundary.tzinfo)
        self.assertEqual(entry, self.seen[-1][1])
        self.assertGreaterEqual(boundary, self.seen[-1][2])
        for name in ('ths-deployment-maintenance', 'ths-deployment-coordination.lock'):
            self.assertFalse((self.case.root / 'runtime' / name).exists())
        self.seen.append(('released', copy.deepcopy(entry), boundary))
        if isinstance(self.release_result, Exception):
            raise self.release_result
        return self.release_result

    def complete_then_retry(self):
        self.assertEqual(self.case.execute(), 'COMPLETE')
        self.case.host.next_cli_process()
        self.seen.clear()

    def test_fresh_complete_requires_both_acknowledgements(self):
        self.assertEqual(self.case.execute(), 'COMPLETE')
        self.assertEqual([value[0] for value in self.seen], ['maintenance', 'released'])

    def test_already_complete_still_requires_both_acknowledgements_without_money_or_restart(self):
        self.complete_then_retry()
        before = self.case.mutations()
        self.assertEqual(self.case.execute(), 'ALREADY_COMPLETE')
        self.assertEqual([value[0] for value in self.seen], ['maintenance', 'released'])
        self.assertEqual(self.case.mutations(), before)

    def test_stale_maintenance_receipt_retains_fence_and_cannot_complete(self):
        self.maintenance_result = ValueError('stale heartbeat')
        with self.assertRaisesRegex(ValueError, 'stale heartbeat'):
            self.case.execute()
        self.assertEqual([value[0] for value in self.seen], ['maintenance'])
        self.assertTrue((self.case.root / 'runtime/ths-deployment-maintenance').is_dir())

    def test_already_complete_stale_maintenance_cannot_succeed_or_restart(self):
        self.complete_then_retry()
        before = self.case.mutations()
        self.maintenance_result = ValueError('stale heartbeat')
        with self.assertRaisesRegex(ValueError, 'stale heartbeat'):
            self.case.execute()
        self.assertEqual(self.case.mutations(), before)
        self.assertTrue((self.case.root / 'runtime/ths-deployment-maintenance').is_dir())

    def test_false_or_non_boolean_maintenance_receipt_is_not_success(self):
        self.maintenance_result = 1
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertTrue((self.case.root / 'runtime/ths-deployment-maintenance').is_dir())

    def test_release_timeout_is_not_complete_and_does_not_recreate_finished_fence(self):
        self.release_result = TimeoutError('released heartbeat absent')
        with self.assertRaisesRegex(TimeoutError, 'released heartbeat absent'):
            self.case.execute()
        self.assertFalse((self.case.root / 'runtime/ths-deployment-maintenance').exists())
        self.case.host.next_cli_process()
        before = self.case.mutations()
        self.release_result = True
        self.seen.clear()
        self.assertEqual(self.case.execute(), 'ALREADY_COMPLETE')
        self.assertEqual(self.case.mutations(), before)
        self.assertEqual([value[0] for value in self.seen], ['maintenance', 'released'])

    def test_false_release_receipt_is_not_complete(self):
        self.release_result = False
        with self.assertRaises(ValueError):
            self.case.execute()

    def test_owner_changed_during_maintenance_ack_keeps_fence(self):
        def replace_owner():
            owner = self.case.host.processes['supervisors'][0]
            owner['pid'] += 100
            owner['birth'] += ':replacement'
        self.maintenance_effect = replace_owner
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertTrue((self.case.root / 'runtime/ths-deployment-maintenance').is_dir())
        self.assertEqual([value[0] for value in self.seen], ['maintenance'])

    def test_release_wait_breaker_drift_cannot_complete(self):
        original = self.case.host.verify_released
        def drift(entry, boundary):
            result = original(entry, boundary)
            self.case.breaker.write_bytes(b'changed during release verification\n')
            return result
        self.case.host.verify_released = drift
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertFalse((self.case.root / 'runtime/ths-deployment-maintenance').exists())

    def test_release_wait_installed_artifact_drift_cannot_complete(self):
        original = self.case.host.verify_released
        def drift(entry, boundary):
            result = original(entry, boundary)
            core = self.case.root / 'bin/gridedge_ths_live'
            core.chmod(0o600)
            core.write_bytes(b'unreviewed replacement during release verification')
            return result
        self.case.host.verify_released = drift
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertFalse((self.case.root / 'runtime/ths-deployment-maintenance').exists())

    def test_release_wait_ledger_drift_cannot_complete(self):
        original = self.case.host.verify_released
        def drift(entry, boundary):
            result = original(entry, boundary)
            self.case.db.append('RECOVERY_COMPLETED', {})
            return result
        self.case.host.verify_released = drift
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertFalse((self.case.root / 'runtime/ths-deployment-maintenance').exists())

    def test_maintenance_callback_cannot_rewrite_expected_owner(self):
        original = self.case.host.verify_maintenance
        def replacement(entry, boundary):
            result = original(entry, boundary)
            owner = self.case.host.processes['supervisors'][0]
            owner['pid'] += 100
            owner['birth'] += ':replacement'
            entry.update(owner)
            return result
        self.case.host.verify_maintenance = replacement
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertTrue((self.case.root / 'runtime/ths-deployment-maintenance').is_dir())

    def test_release_callback_cannot_rewrite_expected_owner(self):
        original = self.case.host.verify_released
        def replacement(entry, boundary):
            result = original(entry, boundary)
            owner = self.case.host.processes['supervisors'][0]
            owner['pid'] += 100
            owner['birth'] += ':replacement'
            entry.update(owner)
            return result
        self.case.host.verify_released = replacement
        with self.assertRaises(ValueError):
            self.case.execute()
        self.assertFalse((self.case.root / 'runtime/ths-deployment-maintenance').exists())


if __name__ == '__main__':
    unittest.main()
