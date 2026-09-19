from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock
import uuid

import manage


class LocalE2EContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.nonce = f"e2e-0629-{uuid.uuid4()}"
        self.root = Path("/tmp") / f"gridedge-market-e2e-{self.nonce}"
        self.bundle = Path("/tmp") / f"gridedge-market-e2e-bundle-{self.nonce}"

    def make_bundle(self) -> Path:
        self.bundle.mkdir(mode=0o700)
        extension = self.bundle / "extension"
        extension.mkdir()
        (extension / "manifest.json").write_text('{"version":"0.6.30"}\n')
        for name in ("gridedge_market_e2e_shadow", "gridedge_ths_live",
                     "market_ingestor.py", "shadow_runner.py"):
            (self.bundle / name).write_text(name + "\n")
        manifest = {
            "schema_version": 1,
            "source_revision": "a" * 40,
            "source_snapshot_sha256": "b" * 64,
            "extension_tree_sha256": manage.tree_sha256(extension),
            "shadow_binary_sha256": manage.sha256_file(
                self.bundle / "gridedge_market_e2e_shadow"),
            "release_binary_sha256": manage.sha256_file(self.bundle / "gridedge_ths_live"),
            "ingestor_sha256": manage.sha256_file(self.bundle / "market_ingestor.py"),
            "shadow_runner_sha256": manage.sha256_file(self.bundle / "shadow_runner.py"),
        }
        (self.bundle / "bundle-manifest.json").write_text(
            json.dumps(manifest, sort_keys=True) + "\n")
        self.addCleanup(lambda: __import__("shutil").rmtree(self.bundle, ignore_errors=True))
        return self.bundle

    def test_root_and_nonce_refuse_broad_or_nonreviewed_targets(self) -> None:
        self.assertEqual(manage.validated_root(str(self.root)), self.root.resolve())
        self.assertEqual(manage.validated_nonce(self.nonce), self.nonce)
        for value in ("/tmp", "/", str(Path.home()), "/tmp/gridedge-market-e2e-bad"):
            with self.assertRaises(ValueError):
                manage.validated_root(value)

    def test_contract_is_loopback_nonce_scoped_and_never_matches_formal_topic(self) -> None:
        bundle = self.make_bundle()
        contract = manage.generated_contract(self.root, self.nonce, bundle)
        self.assertEqual(contract["mqtt_host"], "127.0.0.1")
        self.assertEqual(contract["postgres_host"], "127.0.0.1")
        self.assertTrue(str(contract["mqtt_input"]).startswith(f"gridedge-e2e/{self.nonce}/"))
        self.assertFalse(str(contract["mqtt_input"]).startswith("gridedge/market/v1/"))
        self.assertEqual(contract["browser_extension_installation"], "UNPACKED_REVIEWED")
        self.assertNotIn("192.168.1.201", str(contract))
        self.assertEqual(contract["worker_mqtt_username"], "gridedge-e2e-worker")
        self.assertEqual(contract["worker_mqtt_client_id"], f"{self.nonce}-worker")
        self.assertFalse(contract["money_actions_enabled"])
        self.assertEqual(contract["candidate_extension_tree_sha256"],
                         manage.tree_sha256(bundle / "extension"))
        self.assertEqual(contract["candidate_shadow_binary_sha256"],
                         manage.sha256_file(bundle / "gridedge_market_e2e_shadow"))
        self.assertEqual(contract["candidate_release_binary_sha256"],
                         manage.sha256_file(bundle / "gridedge_ths_live"))
        for key in ("market_event_log", "bar_log", "quote_log", "ledger", "outbox",
                    "chrome_profile", "browser_extension"):
            self.assertTrue(Path(str(contract[key])).is_relative_to(self.root.resolve()))

    def test_contract_rejects_any_mutated_topic_or_state_path(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp", prefix="unused-"):
            pass
        root = self.root.resolve()
        root.mkdir(mode=0o700)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        bundle = self.make_bundle()
        contract = manage.generated_contract(root, self.nonce, bundle)
        (root / "contract.json").write_text(__import__("json").dumps(contract))
        self.assertEqual(manage.load_contract(root), contract)
        contract["outbox"] = "/tmp/formal-outbox.sqlite3"
        (root / "contract.json").write_text(__import__("json").dumps(contract))
        with self.assertRaises(RuntimeError):
            manage.load_contract(root)

    def test_frozen_bundle_rejects_any_extension_or_executor_mutation(self) -> None:
        bundle = self.make_bundle()
        identity = manage.validate_candidate_bundle(bundle)
        self.assertEqual(identity["extension_tree_sha256"],
                         manage.tree_sha256(bundle / "extension"))
        (bundle / "extension/manifest.json").write_text('{"version":"changed"}\n')
        with self.assertRaisesRegex(RuntimeError, "identity or bytes changed"):
            manage.validate_candidate_bundle(bundle)

    def test_prepare_uses_only_one_frozen_bundle_for_candidate_artifacts(self) -> None:
        source = __import__("inspect").getsource(manage.prepare)
        self.assertIn('bundle_root / "extension"', source)
        self.assertIn('bundle_root / "gridedge_market_e2e_shadow"', source)
        self.assertIn("copy_function=shutil.copyfile", source)
        self.assertNotIn('REPO / "build/gridedge-web-market-extension"', source)
        self.assertNotIn('REPO / "target/debug/gridedge_market_e2e_shadow"', source)

    def test_teardown_guard_is_independent_of_current_working_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                manage.validated_root(directory)

    def test_partial_start_failure_always_attempts_bounded_owned_rollback(self) -> None:
        failure = RuntimeError("injected broker startup failure")
        with mock.patch.object(manage, "start_services", side_effect=failure), \
                mock.patch.object(manage, "stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "injected broker startup failure"):
                manage.start(self.root)
        stop.assert_called_once_with(self.root)

    def test_partial_start_preserves_root_when_owned_rollback_is_incomplete(self) -> None:
        with mock.patch.object(manage, "start_services", side_effect=RuntimeError("start")), \
                mock.patch.object(manage, "stop", side_effect=RuntimeError("listener remains")):
            with self.assertRaisesRegex(RuntimeError, "bounded rollback was incomplete"):
                manage.start(self.root)

    def test_shadow_terminal_result_distinguishes_success_crash_and_missing(self) -> None:
        root = self.root.resolve()
        (root / "state").mkdir(parents=True)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        self.assertFalse(manage.shadow_result_succeeded(root))
        failed = {"exit_code": 1, "output": None}
        (root / "state/shadow-result.json").write_text(json.dumps(failed))
        self.assertFalse(manage.shadow_result_succeeded(root))
        passed = {
            "exit_code": 0,
            "output": {
                "ledger_head_sha256": "a" * 64,
                "money_actions_enabled": False,
                "shadow_market_path_only": True,
                "strategy_evaluated": False,
            },
        }
        (root / "state/shadow-result.json").write_text(json.dumps(passed))
        self.assertTrue(manage.shadow_result_succeeded(root))

    def test_browser_extension_is_loaded_through_cdp_with_nonce_root(self) -> None:
        root = self.root.resolve()
        (root / "browser-extension").mkdir(parents=True)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        with mock.patch.object(
                manage,
                "browser_cdp_command",
                return_value={"id": "a" * 32},
        ) as command:
            extension_id = manage.load_browser_extension(root)
        self.assertEqual(extension_id, "a" * 32)
        command.assert_called_once_with(
            root,
            "Extensions.loadUnpacked", {"path": str((root / "browser-extension").resolve())})

    def test_browser_extension_refuses_symlink_escape(self) -> None:
        root = self.root.resolve()
        root.mkdir(mode=0o700)
        (root / "browser-extension").symlink_to("/tmp")
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        with self.assertRaisesRegex(RuntimeError, "escaped"):
            manage.load_browser_extension(root)

    def test_isolated_browser_never_uses_the_user_chrome_binary(self) -> None:
        self.assertIn("Google Chrome for Testing.app", str(manage.CHROME))
        self.assertNotEqual(
            manage.CHROME,
            Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        )

    def test_browser_start_failure_rolls_back_owned_chrome_immediately(self) -> None:
        failure = RuntimeError("injected extension startup failure")
        with mock.patch.object(manage, "start_browser_owned", side_effect=failure), \
                mock.patch.object(manage, "stop_browser") as stop:
            with self.assertRaisesRegex(RuntimeError, "injected extension startup failure"):
                manage.start_browser(self.root)
        stop.assert_called_once_with(self.root)

    def test_browser_bootstraps_blank_before_creating_one_reviewed_page(self) -> None:
        source = __import__("inspect").getsource(manage.start_browser_owned)
        self.assertIn('"about:blank"', source)
        self.assertIn("create_and_activate_reviewed_target(root)", source)
        self.assertIn("len(reviewed_pages) == 1", source)
        self.assertNotIn("--load-extension", source)
        self.assertNotIn("--disable-extensions-except", source)

    def test_reviewed_target_is_activated_immediately_after_creation(self) -> None:
        root = self.root.resolve()
        with mock.patch.object(manage, "browser_cdp_command", side_effect=[
            {"targetId": "A" * 32},
            {},
        ]) as command:
            target_id = manage.create_and_activate_reviewed_target(root)
        self.assertEqual(target_id, "A" * 32)
        self.assertEqual(command.call_args_list, [
            mock.call(root, "Target.createTarget", {"url": manage.EASTMONEY_REVIEWED_URL}),
            mock.call(root, "Target.activateTarget", {"targetId": "A" * 32}),
        ])

    def test_reviewed_target_creation_rejects_missing_or_malformed_identity_before_activation(self) -> None:
        for response in ({}, {"targetId": "not-a-target"}):
            with self.subTest(response=response), \
                    mock.patch.object(manage, "browser_cdp_command", return_value=response) as command:
                with self.assertRaisesRegex(RuntimeError, "target identity"):
                    manage.create_and_activate_reviewed_target(self.root.resolve())
            command.assert_called_once()

    def test_browser_refuses_a_preoccupied_debug_port_before_launch(self) -> None:
        root = self.root.resolve()
        root.mkdir(mode=0o700)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        with mock.patch.object(manage, "load_contract", return_value={}), \
                mock.patch.object(manage, "listening_pids", return_value={991}), \
                mock.patch.object(manage.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "already owned"):
                manage.start_browser_owned(root)
        popen.assert_not_called()

    def test_browser_endpoint_rejects_wrong_listener_product_and_websocket(self) -> None:
        root = self.root.resolve()
        (root / "run").mkdir(parents=True)
        (root / "run/chrome.pid").write_text("515\n")
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        contract = {"browser_version": "Google Chrome for Testing 151.0.7922.34"}
        valid = {
            "Browser": "Chrome/151.0.7922.34",
            "webSocketDebuggerUrl":
                "ws://127.0.0.1:19222/devtools/browser/01234567-89ab-cdef-0123-456789abcdef",
        }
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with mock.patch.object(manage, "load_contract", return_value=contract), \
                mock.patch.object(manage, "require_owned_process"), \
                mock.patch.object(manage, "listening_pids", return_value={999}):
            with self.assertRaisesRegex(RuntimeError, "not owned"):
                manage.validated_browser_endpoint(root)
        for broken, message in (({**valid, "Browser": "Chrome/150.0.0.0"}, "product"),
                                ({**valid, "webSocketDebuggerUrl": "ws://evil/"}, "endpoint")):
            response.read.return_value = json.dumps(broken).encode()
            with mock.patch.object(manage, "load_contract", return_value=contract), \
                    mock.patch.object(manage, "require_owned_process"), \
                    mock.patch.object(manage, "listening_pids", return_value={515}), \
                    mock.patch.object(manage.urllib.request, "urlopen", return_value=response):
                with self.assertRaisesRegex(RuntimeError, message):
                    manage.validated_browser_endpoint(root)

    def test_shadow_refuses_to_start_after_browser_or_committed_events(self) -> None:
        root = self.root.resolve()
        (root / "run").mkdir(parents=True)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        with mock.patch.object(manage, "load_contract", return_value={}):
            (root / "run/chrome.pid").write_text("10\n")
            with self.assertRaisesRegex(RuntimeError, "subscribe before"):
                manage.start_shadow(root)
            (root / "run/chrome.pid").unlink()
        (root / "secrets").mkdir()
        (root / "secrets/postgres.password").write_text("secret\n")
        completed = mock.MagicMock(stdout="1\n")
        with mock.patch.object(manage, "load_contract", return_value={}), \
                mock.patch.object(manage.subprocess, "run", return_value=completed), \
                mock.patch.object(manage.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "empty committed-event"):
                manage.start_shadow(root)
        popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
