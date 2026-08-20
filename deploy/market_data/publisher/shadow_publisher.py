#!/usr/bin/env python3
"""Durably mirror reviewed Tonghuashun quote evidence to MQTT 5."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, time as wall_time
from decimal import Decimal
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

try:
    import paho.mqtt.client as mqtt
except ModuleNotFoundError:  # Unit tests exercise the durable spool without MQTT.
    mqtt = None  # type: ignore[assignment]


THS_BUNDLE = "cn.com.10jqka.macstockPro"
THS_VERSION = "5.3.2"
THS_SIMULATION_MARKER = "模拟练习"
SHANGHAI = ZoneInfo("Asia/Shanghai")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def unix_us(text: str) -> int:
    observed = datetime.fromisoformat(text)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=SHANGHAI)
    return int(observed.timestamp() * 1_000_000)


def price_parts(text: str) -> dict[str, int]:
    value = Decimal(text)
    if not value.is_finite() or value <= 0 or -value.as_tuple().exponent > 3:
        raise ValueError("reviewed quote price is outside the A-share envelope")
    normalized = value.normalize()
    sign, digits, exponent = normalized.as_tuple()
    mantissa = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        mantissa = -mantissa
    if exponent > 0:
        mantissa *= 10**exponent
        exponent = 0
    return {"mantissa": mantissa, "scale": -exponent}


def build_quote_event(
    quote: dict[str, object],
    *,
    source_id: str,
    source_instance_id: str,
    source_sequence: int,
    venue: str,
    symbol: str,
    recv_us: int,
) -> tuple[str, bytes]:
    required = {
        "observed_from",
        "observed_at",
        "symbol",
        "last_price",
        "source_sha256",
        "application_bundle",
        "application_version",
        "account_marker",
    }
    if set(quote) != required:
        raise ValueError("Tonghuashun quote JSON shape is not reviewed")
    if quote["symbol"] != symbol:
        raise ValueError("Tonghuashun quote symbol changed")
    if quote["application_bundle"] != THS_BUNDLE or quote["application_version"] != THS_VERSION:
        raise ValueError("Tonghuashun quote application identity changed")
    if quote["account_marker"] != THS_SIMULATION_MARKER:
        raise ValueError("Tonghuashun quote is not simulation evidence")
    evidence = quote["source_sha256"]
    if not isinstance(evidence, str) or len(evidence) != 64 or any(
        character not in "0123456789abcdef" for character in evidence
    ):
        raise ValueError("Tonghuashun quote evidence hash is invalid")
    observed_from_us = unix_us(str(quote["observed_from"]))
    observed_at_us = unix_us(str(quote["observed_at"]))
    if observed_from_us > observed_at_us or recv_us < observed_at_us:
        raise ValueError("Tonghuashun quote observation times are invalid")

    document: dict[str, object] = {
        "spec": "gridedge.market",
        "schema_version": 1,
        "event_type": "QUOTE_SNAPSHOT",
        "source": {
            "source_id": source_id,
            "source_instance_id": source_instance_id,
            "source_type": "THS_SIM_UI_QUOTE",
            "application_bundle": THS_BUNDLE,
            "application_version": THS_VERSION,
        },
        "instrument": {
            "venue": venue,
            "symbol": symbol,
            "asset_class": "EQUITY",
            "currency": "CNY",
        },
        "source_sequence": source_sequence,
        "ts_us": observed_at_us,
        "recv_us": recv_us,
        "payload": {
            "price": price_parts(str(quote["last_price"])),
            "observed_from_us": observed_from_us,
            "unit": "SHARE",
        },
        "evidence_sha256": evidence,
    }
    identity = {key: value for key, value in document.items() if key != "recv_us"}
    document["event_id"] = hashlib.sha256(canonical_json(identity)).hexdigest()
    topic = f"gridedge/market/v1/{venue}/{symbol}/quote"
    return topic, canonical_json(document)


class ShadowSpool:
    def __init__(
        self,
        state_db: Path,
        quote_log: Path,
        *,
        source_id: str,
        venue: str,
        symbol: str,
    ) -> None:
        self.state_db = state_db
        self.quote_log = quote_log
        self.source_id = source_id
        self.venue = venue
        self.symbol = symbol
        state_db.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(state_db)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS publisher_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                source_instance_id TEXT NOT NULL,
                source_sequence INTEGER NOT NULL CHECK (source_sequence >= 0),
                source_id TEXT,
                venue TEXT,
                symbol TEXT,
                file_device INTEGER,
                file_inode INTEGER,
                byte_offset INTEGER NOT NULL CHECK (byte_offset >= 0)
            );
            CREATE TABLE IF NOT EXISTS publisher_outbox (
                source_sequence INTEGER PRIMARY KEY,
                mqtt_topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
                sent INTEGER NOT NULL DEFAULT 0 CHECK (sent IN (0, 1))
            );
            """
        )
        columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(publisher_state)").fetchall()
        }
        for column in ("source_id", "venue", "symbol"):
            if column not in columns:
                self.db.execute(f"ALTER TABLE publisher_state ADD COLUMN {column} TEXT")
        self.db.execute(
            "INSERT OR IGNORE INTO publisher_state "
            "(singleton,source_instance_id,source_sequence,source_id,venue,symbol,byte_offset) "
            "VALUES (1,?,?,?,?,?,0)",
            (str(uuid.uuid4()), 0, source_id, venue, symbol),
        )
        self.db.execute(
            "UPDATE publisher_state SET source_id=COALESCE(source_id,?),"
            "venue=COALESCE(venue,?),symbol=COALESCE(symbol,?) WHERE singleton=1",
            (source_id, venue, symbol),
        )
        binding = self.db.execute(
            "SELECT source_id,venue,symbol FROM publisher_state WHERE singleton=1"
        ).fetchone()
        if binding != (source_id, venue, symbol):
            self.db.close()
            raise RuntimeError("publisher state is bound to another market source")
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def stage_available(self, recv_us: Callable[[], int] | None = None) -> int:
        if not self.quote_log.exists():
            return 0
        stat = self.quote_log.stat()
        row = self.db.execute(
            "SELECT source_instance_id,source_sequence,file_device,file_inode,byte_offset "
            "FROM publisher_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise RuntimeError("publisher state singleton is absent")
        instance_id, sequence, device, inode, offset = row
        if device is None:
            self.db.execute(
                "UPDATE publisher_state SET file_device=?,file_inode=? WHERE singleton=1",
                (stat.st_dev, stat.st_ino),
            )
            self.db.commit()
        elif (device, inode) != (stat.st_dev, stat.st_ino) or stat.st_size < offset:
            raise RuntimeError("quote log identity changed or was truncated")

        staged = 0
        with self.quote_log.open("rb") as quote_file:
            quote_file.seek(offset)
            while True:
                start = quote_file.tell()
                line = quote_file.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    quote_file.seek(start)
                    break
                quote = json.loads(line)
                next_sequence = sequence + 1
                received = recv_us() if recv_us is not None else time.time_ns() // 1_000
                topic, payload = build_quote_event(
                    quote,
                    source_id=self.source_id,
                    source_instance_id=instance_id,
                    source_sequence=next_sequence,
                    venue=self.venue,
                    symbol=self.symbol,
                    recv_us=received,
                )
                next_offset = quote_file.tell()
                with self.db:
                    self.db.execute(
                        "INSERT INTO publisher_outbox "
                        "(source_sequence,mqtt_topic,payload,payload_sha256) VALUES (?,?,?,?)",
                        (next_sequence, topic, payload, hashlib.sha256(payload).digest()),
                    )
                    self.db.execute(
                        "UPDATE publisher_state SET source_sequence=?,byte_offset=? WHERE singleton=1",
                        (next_sequence, next_offset),
                    )
                sequence = next_sequence
                offset = next_offset
                staged += 1
        return staged

    def drain(self, publish: Callable[[str, bytes], None]) -> int:
        sent = 0
        rows = self.db.execute(
            "SELECT source_sequence,mqtt_topic,payload FROM publisher_outbox "
            "WHERE sent=0 ORDER BY source_sequence"
        ).fetchall()
        for sequence, topic, payload in rows:
            publish(topic, bytes(payload))
            with self.db:
                self.db.execute(
                    "UPDATE publisher_outbox SET sent=1 WHERE source_sequence=? AND sent=0",
                    (sequence,),
                )
            sent += 1
        return sent


class Mqtt5Publisher:
    def __init__(self, args: argparse.Namespace) -> None:
        if mqtt is None:
            raise RuntimeError("paho-mqtt is required to run the shadow publisher")
        self.connected = threading.Event()
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            protocol=mqtt.MQTTv5,
        )
        password = Path(args.password_file).read_text(encoding="utf-8").strip()
        self.client.username_pw_set(args.username, password)
        self.client.tls_set(ca_certs=args.ca_file)

        def on_connect(_client: object, _userdata: object, _flags: object, reason: object, _props: object) -> None:
            if getattr(reason, "is_failure", True):
                return
            self.connected.set()

        self.client.on_connect = on_connect
        self.client.connect(args.host, args.port, keepalive=30)
        self.client.loop_start()
        if not self.connected.wait(10):
            raise RuntimeError("MQTT 5 publisher did not connect")

    def publish(self, topic: str, payload: bytes) -> None:
        properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        properties.ContentType = "application/json"
        result = self.client.publish(topic, payload, qos=1, retain=False, properties=properties)
        result.wait_for_publish(timeout=15)
        if not result.is_published():
            raise RuntimeError("MQTT QoS 1 publish was not acknowledged")

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-log", required=True, type=Path)
    parser.add_argument("--state-db", required=True, type=Path)
    parser.add_argument("--host", default="192.168.1.201")
    parser.add_argument("--port", default=8883, type=int)
    parser.add_argument("--ca-file", required=True)
    parser.add_argument("--password-file", required=True)
    parser.add_argument("--username", default="gridedge-publisher")
    parser.add_argument("--client-id", default="gridedge-ths-quote-shadow-v1")
    parser.add_argument("--source-id", default="ths-sim-quote")
    parser.add_argument("--venue", default="XSHE")
    parser.add_argument("--symbol", default="002256")
    parser.add_argument("--poll-seconds", default=2, type=int)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spool = ShadowSpool(
        args.state_db,
        args.quote_log,
        source_id=args.source_id,
        venue=args.venue,
        symbol=args.symbol,
    )
    publisher = Mqtt5Publisher(args)
    try:
        while True:
            spool.stage_available()
            spool.drain(publisher.publish)
            if args.once:
                return 0
            now = datetime.now(SHANGHAI)
            if now.time() >= wall_time(15, 10):
                return 0
            time.sleep(args.poll_seconds)
    finally:
        publisher.close()
        spool.close()


if __name__ == "__main__":
    raise SystemExit(main())
