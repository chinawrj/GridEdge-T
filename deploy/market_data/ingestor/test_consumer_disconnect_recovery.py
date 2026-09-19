"""Isolated main-loop regression: no sockets, secrets, DB, or production state.

Run: python3 -m unittest discover -s deploy/market_data/ingestor
     -p test_consumer_disconnect_recovery.py -v
Optional GRIDEDGE_INGESTOR_TEST_TARGET selects an archived implementation file;
GRIDEDGE_INGESTOR_TEST_SHA256 must then bind its exact bytes.
"""
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch


TARGET = Path(os.environ.get("GRIDEDGE_INGESTOR_TEST_TARGET", Path(__file__).with_name("market_ingestor.py")))
if "GRIDEDGE_INGESTOR_TEST_TARGET" in os.environ:
    assert hashlib.sha256(TARGET.read_bytes()).hexdigest() == os.environ["GRIDEDGE_INGESTOR_TEST_SHA256"]
SPEC = importlib.util.spec_from_file_location("disconnect_regression_target", TARGET)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LoopBudgetExceeded(BaseException):
    """Stop the broken loop deterministically without wall-clock waiting."""


class ConsumerRecoveryTest(unittest.TestCase):
    def exercise(self, disconnected, *, stop_signal=None, loop_exception=False):
        stopping = threading.Event()
        consumer, publisher, store = MagicMock(), MagicMock(), MagicMock()
        calls = []
        restored = False
        handlers = {}
        injected_error = RuntimeError("injected consumer loop failure")
        self.loop_error = None
        self.clients = (consumer, publisher)

        def reconnect():
            nonlocal restored
            restored = True
            consumer.on_connect(consumer, None, None, SimpleNamespace(is_failure=False), None)
            return 0

        def loop(timeout):
            self.assertLessEqual(timeout, 1.0)
            calls.append(timeout)
            if len(calls) > 8:
                raise LoopBudgetExceeded()
            if stop_signal is not None:
                handlers[stop_signal](stop_signal, None)
            if loop_exception:
                raise injected_error
            if restored or (not disconnected and len(calls) == 3):
                stopping.set()
            return 4 if disconnected and not restored else 0  # MQTT_ERR_NO_CONN

        consumer.loop.side_effect = loop
        consumer.reconnect.side_effect = reconnect
        mqtt = SimpleNamespace(
            Client=MagicMock(side_effect=[consumer, publisher]),
            CallbackAPIVersion=SimpleNamespace(VERSION2=2), MQTTv5=5,
            MQTT_CLEAN_START_FIRST_ONLY=3, MQTT_ERR_SUCCESS=0, MQTT_ERR_NO_CONN=4,
        )
        namespace = "gridedge-e2e/e2e-0629-123e4567-e89b-42d3-a456-426614174000"
        environment = {
            "MQTT_TOPIC_NAMESPACE": namespace, "MQTT_TOPIC": namespace + "/market/v1/#",
            "MQTT_CLIENT_ID": "isolated-disconnect-regression", "MQTT_USERNAME": "fixture",
            "MQTT_HOST": "invalid.invalid", "MQTT_PORT": "18883", "MQTT_CA_FILE": "unused",
            "MQTT_PASSWORD_FILE": "unused", "POSTGRES_PASSWORD_FILE": "unused",
            "POSTGRES_HOST": "invalid.invalid", "POSTGRES_PORT": "15432",
            "POSTGRES_DB": "fixture", "POSTGRES_USER": "fixture",
        }
        result, exhausted = None, False
        with patch.dict(os.environ, environment, clear=True), \
             patch.object(MODULE, "mqtt", mqtt), \
             patch.object(MODULE, "psycopg", object()), \
             patch.object(MODULE, "Jsonb", object()), \
             patch.object(MODULE, "read_secret", return_value="fixture-not-a-secret"), \
             patch.object(MODULE, "EventStore", return_value=store), \
             patch.object(MODULE, "threading", SimpleNamespace(Event=lambda: stopping)), \
             patch.object(MODULE, "signal", SimpleNamespace(
                 SIGTERM=15, SIGINT=2, signal=lambda number, handler: handlers.update({number: handler}))), \
             patch.object(MODULE, "time", SimpleNamespace(sleep=lambda _: None)):
            try:
                result = MODULE.main()
            except SystemExit as error:
                result = error.code
            except LoopBudgetExceeded:
                exhausted = True
            except RuntimeError as error:
                if error is not injected_error:
                    raise
                self.loop_error = error
        # Recovery must not fabricate a delivery, commit, or acknowledgement.
        store.ingest.assert_not_called()
        store.reject.assert_not_called()
        consumer.ack.assert_not_called()
        consumer.publish.assert_not_called()
        publisher.publish.assert_not_called()
        return result, exhausted, restored, consumer, calls

    def assert_clients_cleaned_up(self):
        consumer, publisher = self.clients
        consumer.disconnect.assert_called()
        publisher.disconnect.assert_called()
        publisher.loop_stop.assert_called_once()

    def test_healthy_loop_shuts_down_without_reconnect_or_synthetic_events(self):
        result, exhausted, restored, consumer, calls = self.exercise(False)
        self.assertEqual(result, 0)
        self.assertFalse(exhausted)
        self.assertFalse(restored)
        consumer.reconnect.assert_not_called()
        self.assertEqual(len(calls), 3)

    def test_normal_exit_cleans_both_clients_and_publisher_thread(self):
        result, exhausted, *_ = self.exercise(False)
        self.assertEqual(result, 0)
        self.assertFalse(exhausted)
        self.assert_clients_cleaned_up()

    def test_signal_and_disconnect_same_iteration_is_graceful(self):
        for number in (15, 2):
            with self.subTest(signal=number):
                result, exhausted, restored, consumer, calls = self.exercise(
                    True, stop_signal=number
                )
                self.assertEqual(result, 0)
                self.assertFalse(exhausted)
                self.assertFalse(restored)
                consumer.reconnect.assert_not_called()
                self.assertEqual(len(calls), 1)
                self.assert_clients_cleaned_up()

    def test_loop_exception_propagates_after_cleaning_both_clients(self):
        result, exhausted, *_ = self.exercise(False, loop_exception=True)
        self.assertIsNone(result)
        self.assertFalse(exhausted)
        self.assertIsNotNone(self.loop_error)
        self.assert_clients_cleaned_up()

    def test_disconnect_nonzero_exit_cleans_both_clients(self):
        result, exhausted, *_ = self.exercise(True)
        self.assertFalse(exhausted)
        self.assertEqual(result, 1)
        self.assert_clients_cleaned_up()

    def test_disconnected_consumer_reconnects_or_exits_for_restart_owner(self):
        result, exhausted, restored, consumer, calls = self.exercise(True)
        self.assertFalse(
            exhausted,
            f"consumer ignored MQTT_ERR_NO_CONN for {len(calls) - 1} iterations; "
            f"reconnects={consumer.reconnect.call_count}; no nonzero exit for restart owner",
        )
        if restored:
            consumer.subscribe.assert_called_once()
            self.assertEqual(consumer.subscribe.call_args.kwargs.get("qos"), 1)
        else:
            self.assertIsInstance(result, int)
            self.assertNotEqual(result, 0, "disconnect must not report graceful success")


if __name__ == "__main__":
    unittest.main()
