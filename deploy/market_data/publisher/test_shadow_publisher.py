from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from deploy.market_data.publisher.shadow_publisher import ShadowSpool, build_quote_event


def quote(price: str = "3.530") -> dict[str, object]:
    return {
        "observed_from": "2026-08-19T09:30:01",
        "observed_at": "2026-08-19T09:30:02",
        "symbol": "002256",
        "last_price": price,
        "source_sha256": "a" * 64,
        "application_bundle": "cn.com.10jqka.macstockPro",
        "application_version": "5.3.2",
        "account_marker": "模拟练习",
    }


class ShadowPublisherTest(unittest.TestCase):
    def test_market_payload_omits_account_execution_marker(self) -> None:
        _topic, payload = build_quote_event(
            quote(),
            source_id="ths-sim-quote",
            source_instance_id="0198a1c0-0000-7000-8000-000000000003",
            source_sequence=1,
            venue="XSHE",
            symbol="002256",
            recv_us=1787103003000000,
        )
        document = json.loads(payload)
        self.assertNotIn("account_marker", payload.decode())
        self.assertEqual(document["source"]["source_type"], "THS_SIM_UI_QUOTE")
        self.assertEqual(document["payload"]["price"], {"mantissa": 353, "scale": 2})

    def test_stages_complete_lines_and_retries_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "quotes.jsonl"
            log.write_text(json.dumps(quote(), ensure_ascii=False) + "\n", encoding="utf-8")
            spool = ShadowSpool(
                root / "state.db",
                log,
                source_id="ths-sim-quote",
                venue="XSHE",
                symbol="002256",
            )
            self.assertEqual(spool.stage_available(lambda: 1787103003000000), 1)
            first: list[tuple[str, bytes]] = []

            def fail_after_capture(topic: str, payload: bytes) -> None:
                first.append((topic, payload))
                raise RuntimeError("lost acknowledgement")

            with self.assertRaisesRegex(RuntimeError, "lost acknowledgement"):
                spool.drain(fail_after_capture)
            spool.close()

            recovered = ShadowSpool(
                root / "state.db",
                log,
                source_id="ths-sim-quote",
                venue="XSHE",
                symbol="002256",
            )
            second: list[tuple[str, bytes]] = []
            self.assertEqual(recovered.stage_available(), 0)
            self.assertEqual(recovered.drain(lambda topic, payload: second.append((topic, payload))), 1)
            self.assertEqual(first, second)
            self.assertEqual(recovered.drain(lambda _topic, _payload: None), 0)
            recovered.close()

    def test_incomplete_line_is_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "quotes.jsonl"
            log.write_text(json.dumps(quote(), ensure_ascii=False), encoding="utf-8")
            spool = ShadowSpool(
                root / "state.db",
                log,
                source_id="ths-sim-quote",
                venue="XSHE",
                symbol="002256",
            )
            self.assertEqual(spool.stage_available(), 0)
            with log.open("a", encoding="utf-8") as output:
                output.write("\n")
            self.assertEqual(spool.stage_available(lambda: 1787103003000000), 1)
            spool.close()

    def test_truncation_or_rotation_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "quotes.jsonl"
            log.write_text(json.dumps(quote(), ensure_ascii=False) + "\n", encoding="utf-8")
            spool = ShadowSpool(
                root / "state.db",
                log,
                source_id="ths-sim-quote",
                venue="XSHE",
                symbol="002256",
            )
            spool.stage_available(lambda: 1787103003000000)
            log.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "changed or was truncated"):
                spool.stage_available()
            spool.close()

    def test_state_cannot_be_reused_for_another_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "quotes.jsonl"
            first = ShadowSpool(
                root / "state.db",
                log,
                source_id="ths-sim-quote",
                venue="XSHE",
                symbol="002256",
            )
            first.close()
            with self.assertRaisesRegex(RuntimeError, "bound to another"):
                ShadowSpool(
                    root / "state.db",
                    log,
                    source_id="ths-sim-quote",
                    venue="XSHG",
                    symbol="600000",
                )


if __name__ == "__main__":
    unittest.main()
