#!/usr/bin/env python3
"""Publish one nonce-scoped canonical event and prove exact local commit."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import ssl
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import psycopg


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def event(source_instance: uuid.UUID) -> tuple[str, bytes]:
    now_us = time.time_ns() // 1_000
    document: dict[str, object] = {
        "spec": "gridedge.market", "schema_version": 1, "event_type": "TRADE_TICK",
        "source": {"source_id": "eastmoney-web-time-sales-e2e",
                   "source_instance_id": str(source_instance), "source_type": "WEB_UI",
                   "provider": "eastmoney", "provider_version": "eastmoney-time-sales-dom-v6"},
        "instrument": {"venue": "XSHE", "symbol": "002256",
                       "asset_class": "EQUITY", "currency": "CNY"},
        "source_sequence": 1, "ts_us": now_us, "recv_us": now_us,
        "payload": {"price": {"mantissa": 344, "scale": 2}, "quantity": 100,
                    "unit": "SHARE", "side": "UNKNOWN", "source_row_key": "isolated-probe",
                    "source_page": 1, "source_captured_at_us": now_us},
        "evidence_sha256": "0" * 64,
    }
    identity = {key: value for key, value in document.items() if key not in {"event_id", "recv_us"}}
    event_id = hashlib.sha256(canonical_json(identity)).hexdigest()
    document["event_id"] = event_id
    return event_id, canonical_json(document)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    contract = json.loads((root / "contract.json").read_text())
    namespace = contract["mqtt_namespace"]
    if not namespace.startswith("gridedge-e2e/e2e-0629-") or "192.168.1.201" in str(contract):
        raise RuntimeError("probe contract is not physically isolated")
    password = (root / "secrets/publisher.password").read_text().strip()
    event_id, payload = event(uuid.uuid4())
    input_topic = f"{namespace}/market/v1/XSHE/002256/trade"
    ack_topic = f"{namespace}/market-ack/v1/{event_id}"
    committed_topic = f"{namespace}/market-committed/v1/XSHE/002256/trade"
    received: dict[str, bytes] = {}
    publication_results: dict[int, bool] = {}
    ready = threading.Event()
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"{contract['nonce']}-probe", protocol=mqtt.MQTTv5)
    client.username_pw_set("gridedge-e2e-publisher", password)
    client.tls_set(ca_certs=str(root / "certs/ca.crt"), tls_version=ssl.PROTOCOL_TLS_CLIENT)

    def on_connect(current: mqtt.Client, _userdata: object, _flags: object,
                   reason_code: object, _properties: object) -> None:
        if getattr(reason_code, "is_failure", True):
            return
        current.subscribe([(ack_topic, 1), (committed_topic, 1)])
        ready.set()

    def on_message(_client: mqtt.Client, _userdata: object, message: mqtt.MQTTMessage) -> None:
        received[message.topic] = bytes(message.payload)

    def on_publish(_client: mqtt.Client, _userdata: object, mid: int,
                   reason_code: object, _properties: object) -> None:
        publication_results[mid] = bool(getattr(reason_code, "is_failure", False))

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.connect("127.0.0.1", 18883, keepalive=20)
    client.loop_start()
    if not ready.wait(5):
        raise RuntimeError("isolated MQTT probe did not connect")
    properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    properties.ContentType = "application/json"
    publication = client.publish(input_topic, payload, qos=1, retain=False, properties=properties)
    publication.wait_for_publish(5)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not {ack_topic, committed_topic}.issubset(received):
        time.sleep(0.05)
    forbidden = client.publish("gridedge/market/v1/XSHE/002256/trade", payload,
                               qos=1, retain=False, properties=properties)
    forbidden.wait_for_publish(5)
    deny_deadline = time.monotonic() + 2
    while time.monotonic() < deny_deadline and forbidden.mid not in publication_results:
        time.sleep(0.02)
    client.disconnect()
    client.loop_stop()
    if set(received) != {ack_topic, committed_topic} or received[committed_topic] != payload:
        raise RuntimeError("isolated exact COMMITTED publication proof is incomplete")
    if publication_results.get(publication.mid, True) or not publication_results.get(forbidden.mid, False):
        raise RuntimeError("local broker ACL did not accept E2E and reject formal publication")
    ack = json.loads(received[ack_topic])
    if ack.get("event_id") != event_id or ack.get("result") != "COMMITTED":
        raise RuntimeError("isolated ACK does not bind the exact event")

    pg_password = (root / "secrets/postgres.password").read_text().strip()
    with psycopg.connect(host="127.0.0.1", port=15432, dbname="gridedge_market_e2e",
                         user="gridedge_e2e", password=pg_password) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT mqtt_topic, payload_bytes, delivery_count FROM market_events")
            rows = cursor.fetchall()
            cursor.execute("SELECT count(*) FROM market_conflicts")
            conflicts = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM market_rejections")
            rejections = cursor.fetchone()[0]
    if rows != [(input_topic, payload, 1)] or conflicts != 0 or rejections != 0:
        raise RuntimeError("isolated PostgreSQL fact differs from the exact probe")
    if any(row[0].startswith("gridedge/market/") for row in rows):
        raise RuntimeError("formal topic tripwire fired inside isolated PostgreSQL")
    print(json.dumps({"ack": "COMMITTED", "committed_payload_exact": True,
                      "conflicts": conflicts, "formal_publish_denied": True,
                      "formal_topic_tripwire": 0,
                      "rejections": rejections, "rows": len(rows),
                      "topic": input_topic}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
