#!/usr/bin/env python3
"""Emit one canonical GridEdge market event for deployment verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from decimal import Decimal


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def price_parts(text: str) -> dict[str, int]:
    value = Decimal(text)
    if not value.is_finite() or value <= 0:
        raise ValueError("price must be finite and positive")
    normalized = value.normalize()
    sign, digits, exponent = normalized.as_tuple()
    mantissa = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        mantissa = -mantissa
    if exponent > 0:
        mantissa *= 10**exponent
        exponent = 0
    return {"mantissa": mantissa, "scale": -exponent}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-instance-id", required=True, type=uuid.UUID)
    parser.add_argument("--source-sequence", required=True, type=int)
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--event-type", choices=("TRADE_TICK", "QUOTE_SNAPSHOT"), required=True)
    parser.add_argument("--venue", default="XSHE")
    parser.add_argument("--symbol", default="002256")
    parser.add_argument("--price", required=True)
    parser.add_argument("--quantity", type=int)
    parser.add_argument("--side", choices=("BUY", "SELL", "UNKNOWN"), default="UNKNOWN")
    parser.add_argument("--ts-us", type=int)
    parser.add_argument("--recv-us", type=int)
    parser.add_argument("--evidence-sha256", default="0" * 64)
    args = parser.parse_args()

    now_us = time.time_ns() // 1_000
    ts_us = args.ts_us if args.ts_us is not None else now_us
    recv_us = args.recv_us if args.recv_us is not None else max(now_us, ts_us)
    payload: dict[str, object] = {"price": price_parts(args.price), "unit": "SHARE"}
    if args.event_type == "TRADE_TICK":
        if args.quantity is None or args.quantity <= 0:
            parser.error("TRADE_TICK requires positive --quantity")
        payload.update(quantity=args.quantity, side=args.side)
    document: dict[str, object] = {
        "spec": "gridedge.market",
        "schema_version": 1,
        "event_type": args.event_type,
        "source": {
            "source_id": args.source_id,
            "source_instance_id": str(args.source_instance_id),
            "source_type": args.source_type,
        },
        "instrument": {
            "venue": args.venue,
            "symbol": args.symbol,
            "asset_class": "EQUITY",
            "currency": "CNY",
        },
        "source_sequence": args.source_sequence,
        "ts_us": ts_us,
        "recv_us": recv_us,
        "payload": payload,
        "evidence_sha256": args.evidence_sha256,
    }
    identity = {key: value for key, value in document.items() if key != "recv_us"}
    document["event_id"] = hashlib.sha256(canonical_json(identity)).hexdigest()
    print(canonical_json(document).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
