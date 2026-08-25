import hashlib
import importlib.util
import json
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("market_ingestor.py")
SPEC = importlib.util.spec_from_file_location("market_ingestor", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
market_ingestor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = market_ingestor
SPEC.loader.exec_module(market_ingestor)


def event(source_id="ths-web-tick", source_sequence=1, price=3530):
    document = {
        "spec": "gridedge.market",
        "schema_version": 1,
        "event_type": "TRADE_TICK",
        "source": {
            "source_id": source_id,
            "source_instance_id": "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
            "source_type": "WEB_UI",
            "environment": "SIMULATION",
        },
        "instrument": {
            "venue": "XSHE",
            "symbol": "002256",
            "asset_class": "EQUITY",
            "currency": "CNY",
        },
        "source_sequence": source_sequence,
        "ts_us": 1787103000123456,
        "recv_us": 1787103002987654,
        "payload": {
            "price": {"mantissa": price, "scale": 3},
            "quantity": 1200,
            "unit": "SHARE",
            "side": "BUY",
        },
        "evidence_sha256": "a" * 64,
    }
    document["event_id"] = market_ingestor.canonical_event_identity(document).hex()
    return market_ingestor.canonical_json(document)


class FakeProperties:
    def __init__(self, content_type="application/json"):
        self.ContentType = content_type


class FakeMessage:
    def __init__(self, payload, content_type="application/json"):
        self.payload = payload
        self.topic = "gridedge/market/v1/XSHE/002256/trade"
        self.qos = 1
        self.retain = False
        self.dup = False
        self.mid = 42
        self.properties = FakeProperties(content_type)


class MarketEventValidationTest(unittest.TestCase):
    def test_message_handler_publishes_commit_receipt_before_acknowledging_insert_or_duplicate(self):
        validated = market_ingestor.validate_document(
            event(), "gridedge/market/v1/XSHE/002256/trade"
        )
        for result in ("inserted", "duplicate"):
            actions = []

            class Store:
                def ingest(self, received, topic, qos, duplicate_flag):
                    self.assertions = (received, topic, qos, duplicate_flag)
                    actions.append("database-committed")
                    return result

                def reject(self, *_args):
                    raise AssertionError("valid event must not be rejected")

            message = FakeMessage(event())

            def publish(topic, payload):
                self.assertEqual(
                    (topic, payload), market_ingestor.application_ack(validated, result)
                )
                actions.append("application-ack-published")

            market_ingestor.process_market_message(
                Store(), message, publish, lambda mid, qos: actions.append(("mqtt-ack", mid, qos))
            )
            self.assertEqual(
                actions,
                [
                    "database-committed",
                    "application-ack-published",
                    ("mqtt-ack", message.mid, message.qos),
                ],
            )

    def test_message_handler_never_publishes_committed_for_conflict_or_rejection(self):
        published = []
        acknowledged = []

        class ConflictStore:
            def ingest(self, *_args):
                return "conflict"

            def reject(self, *_args):
                raise AssertionError("validated conflict must not enter rejection storage")

        message = FakeMessage(event())
        result = market_ingestor.process_market_message(
            ConflictStore(),
            message,
            lambda *args: published.append(args),
            lambda mid, qos: acknowledged.append((mid, qos)),
        )
        self.assertEqual(result, "conflict")
        self.assertEqual(published, [])
        self.assertEqual(acknowledged, [(message.mid, message.qos)])

        class RejectStore:
            def ingest(self, *_args):
                raise AssertionError("invalid delivery must be rejected before ingest")

            def reject(self, topic, raw, reason):
                self.rejected = (topic, raw, reason)

        rejected_message = FakeMessage(event(), content_type="application/octet-stream")
        result = market_ingestor.process_market_message(
            RejectStore(),
            rejected_message,
            lambda *args: published.append(args),
            lambda mid, qos: acknowledged.append((mid, qos)),
        )
        self.assertEqual(result, "rejected")
        self.assertEqual(published, [])
        self.assertEqual(acknowledged[-1], (rejected_message.mid, rejected_message.qos))

    def test_message_handler_does_not_ack_original_when_database_or_commit_receipt_publish_fails(self):
        acknowledged = []

        class FailingStore:
            def ingest(self, *_args):
                raise RuntimeError("database unavailable")

            def reject(self, *_args):
                raise AssertionError("valid event must not be rejected")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            market_ingestor.process_market_message(
                FailingStore(),
                FakeMessage(event()),
                lambda *_args: None,
                lambda mid, qos: acknowledged.append((mid, qos)),
            )
        self.assertEqual(acknowledged, [])

        class InsertStore:
            def ingest(self, *_args):
                return "inserted"

            def reject(self, *_args):
                raise AssertionError("valid event must not be rejected")

        with self.assertRaisesRegex(RuntimeError, "ACK broker unavailable"):
            market_ingestor.process_market_message(
                InsertStore(),
                FakeMessage(event()),
                lambda *_args: (_ for _ in ()).throw(RuntimeError("ACK broker unavailable")),
                lambda mid, qos: acknowledged.append((mid, qos)),
            )
        self.assertEqual(acknowledged, [])

    def test_application_ack_is_canonical_and_binds_the_committed_event(self):
        validated = market_ingestor.validate_document(
            event(), "gridedge/market/v1/XSHE/002256/trade"
        )
        topic, payload = market_ingestor.application_ack(validated, "inserted")
        self.assertEqual(
            topic,
            f"gridedge/market-ack/v1/{validated.event_id.hex()}",
        )
        self.assertEqual(market_ingestor.canonical_json(json.loads(payload)), payload)
        self.assertEqual(
            json.loads(payload),
            {
                "event_id": validated.event_id.hex(),
                "result": "COMMITTED",
                "schema_version": 1,
                "source_id": validated.source_id,
                "source_instance_id": str(validated.source_instance_id),
                "source_sequence": validated.source_sequence,
                "spec": "gridedge.market.ack",
            },
        )
        duplicate_topic, duplicate_payload = market_ingestor.application_ack(
            validated, "duplicate"
        )
        self.assertEqual(duplicate_topic, topic)
        self.assertEqual(duplicate_payload, payload)
        with self.assertRaisesRegex(ValueError, "committed or duplicate"):
            market_ingestor.application_ack(validated, "conflict")

    def test_mqtt5_delivery_contract_is_qos1_nonretained_json(self):
        market_ingestor.validate_delivery("application/json", 1, False)
        for content_type, qos, retained in (
            (None, 1, False),
            ("application/octet-stream", 1, False),
            ("application/json", 0, False),
            ("application/json", 1, True),
        ):
            with self.assertRaises(market_ingestor.RejectedEvent):
                market_ingestor.validate_delivery(content_type, qos, retained)

    def test_canonical_tick_validates(self):
        validated = market_ingestor.validate_document(
            event(), "gridedge/market/v1/XSHE/002256/trade"
        )
        self.assertEqual(validated.source_id, "ths-web-tick")
        self.assertEqual(validated.source_sequence, 1)
        self.assertEqual(validated.event_type, "TRADE_TICK")

    def test_recv_time_is_not_part_of_event_identity(self):
        raw = event()
        first = json.loads(raw)
        second = dict(first)
        second["recv_us"] += 3_000_000
        self.assertEqual(
            market_ingestor.canonical_event_identity(first),
            market_ingestor.canonical_event_identity(second),
        )

    def test_source_clock_may_be_ahead_of_receive_clock(self):
        document = json.loads(event())
        document["recv_us"] = document["ts_us"] - 1_000
        document["event_id"] = market_ingestor.canonical_event_identity(document).hex()
        validated = market_ingestor.validate_document(
            market_ingestor.canonical_json(document),
            "gridedge/market/v1/XSHE/002256/trade",
        )
        self.assertLess(validated.recv_us, validated.ts_us)

    def test_same_stock_from_another_source_has_another_identity(self):
        first = json.loads(event(source_id="ths-web-tick"))
        second = json.loads(event(source_id="ths-sim-quote"))
        self.assertNotEqual(first["event_id"], second["event_id"])

    def test_noncanonical_json_is_rejected(self):
        raw = event()
        document = json.loads(raw)
        pretty = json.dumps(document, indent=2).encode()
        with self.assertRaisesRegex(market_ingestor.RejectedEvent, "not canonical"):
            market_ingestor.validate_document(
                pretty, "gridedge/market/v1/XSHE/002256/trade"
            )

    def test_topic_and_identity_must_agree(self):
        with self.assertRaisesRegex(market_ingestor.RejectedEvent, "disagrees"):
            market_ingestor.validate_document(
                event(), "gridedge/market/v1/XSHG/002256/trade"
            )

    def test_payload_change_without_event_id_change_is_rejected(self):
        document = json.loads(event())
        document["payload"]["price"]["mantissa"] = 3540
        raw = market_ingestor.canonical_json(document)
        with self.assertRaisesRegex(market_ingestor.RejectedEvent, "does not match"):
            market_ingestor.validate_document(
                raw, "gridedge/market/v1/XSHE/002256/trade"
            )

    def test_u64_boundaries_are_explicit(self):
        maximum = market_ingestor.MAX_U64
        document = json.loads(event(source_sequence=maximum))
        document["ts_us"] = maximum
        document["recv_us"] = maximum
        document["event_id"] = market_ingestor.canonical_event_identity(document).hex()
        validated = market_ingestor.validate_document(
            market_ingestor.canonical_json(document),
            "gridedge/market/v1/XSHE/002256/trade",
        )
        self.assertEqual(validated.source_sequence, maximum)

        document["source_sequence"] = maximum + 1
        document["event_id"] = market_ingestor.canonical_event_identity(document).hex()
        with self.assertRaisesRegex(market_ingestor.RejectedEvent, "must be u64"):
            market_ingestor.validate_document(
                market_ingestor.canonical_json(document),
                "gridedge/market/v1/XSHE/002256/trade",
            )


if __name__ == "__main__":
    unittest.main()
