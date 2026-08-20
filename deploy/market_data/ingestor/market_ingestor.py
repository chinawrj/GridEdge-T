from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import ssl
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

try:
    import paho.mqtt.client as mqtt
    import psycopg
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:  # Unit validation tests intentionally need no network/DB packages.
    mqtt = None  # type: ignore[assignment]
    psycopg = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment,misc]


EVENT_TYPES = {
    "TRADE_TICK": "trade",
    "QUOTE_SNAPSHOT": "quote",
    "BOOK_SNAPSHOT": "book",
    "BOOK_DELTA": "book",
    "BAR": "bar",
    "SOURCE_STATUS": "status",
}
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
SOURCE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
TOKEN = re.compile(r"^[A-Z0-9._-]{1,32}$")
MAX_U64 = 18_446_744_073_709_551_615


class RejectedEvent(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedEvent:
    event_id: bytes
    schema_version: int
    event_type: str
    source_id: str
    source_instance_id: uuid.UUID
    source_sequence: int
    source_metadata: dict[str, Any]
    venue: str
    symbol: str
    asset_class: str
    currency: str
    ts_us: int
    recv_us: int
    payload: dict[str, Any]
    payload_sha256: bytes
    evidence_sha256: bytes
    raw: bytes


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def validate_delivery(content_type: Any, qos: int, retained: bool) -> None:
    if content_type != "application/json":
        raise RejectedEvent("MQTT 5 content type must be application/json")
    if qos != 1:
        raise RejectedEvent("market data must use MQTT QoS 1")
    if retained:
        raise RejectedEvent("market data events must not be retained")


def canonical_event_identity(document: dict[str, Any]) -> bytes:
    identity = {key: value for key, value in document.items() if key not in {"event_id", "recv_us"}}
    return hashlib.sha256(canonical_json(identity)).digest()


def validate_document(raw: bytes, topic: str) -> ValidatedEvent:
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RejectedEvent(f"payload is not canonical JSON: {error}") from error
    if not isinstance(document, dict):
        raise RejectedEvent("market event must be a JSON object")
    if canonical_json(document) != raw:
        raise RejectedEvent("market event JSON is not canonical")
    if document.get("spec") != "gridedge.market" or document.get("schema_version") != 1:
        raise RejectedEvent("unsupported market event specification")

    event_type = document.get("event_type")
    stream = EVENT_TYPES.get(event_type)
    if stream is None:
        raise RejectedEvent("unsupported market event type")
    event_id_text = document.get("event_id")
    evidence_text = document.get("evidence_sha256")
    if not isinstance(event_id_text, str) or HEX_64.fullmatch(event_id_text) is None:
        raise RejectedEvent("event_id is not lowercase SHA-256")
    if not isinstance(evidence_text, str) or HEX_64.fullmatch(evidence_text) is None:
        raise RejectedEvent("evidence_sha256 is not lowercase SHA-256")
    event_id = bytes.fromhex(event_id_text)
    if canonical_event_identity(document) != event_id:
        raise RejectedEvent("event_id does not match canonical market identity")

    source = document.get("source")
    instrument = document.get("instrument")
    payload = document.get("payload")
    if not isinstance(source, dict) or not isinstance(instrument, dict) or not isinstance(payload, dict):
        raise RejectedEvent("source, instrument, and payload must be objects")
    source_id = source.get("source_id")
    if not isinstance(source_id, str) or SOURCE_ID.fullmatch(source_id) is None:
        raise RejectedEvent("source_id is invalid")
    try:
        source_instance_id = uuid.UUID(source.get("source_instance_id", ""))
    except (ValueError, AttributeError) as error:
        raise RejectedEvent("source_instance_id is not UUID") from error

    venue = instrument.get("venue")
    symbol = instrument.get("symbol")
    asset_class = instrument.get("asset_class")
    currency = instrument.get("currency")
    if not isinstance(venue, str) or TOKEN.fullmatch(venue) is None:
        raise RejectedEvent("venue is invalid")
    if not isinstance(symbol, str) or TOKEN.fullmatch(symbol) is None:
        raise RejectedEvent("symbol is invalid")
    if not all(isinstance(value, str) and value for value in (asset_class, currency)):
        raise RejectedEvent("asset_class and currency are required")

    source_sequence = document.get("source_sequence")
    ts_us = document.get("ts_us")
    recv_us = document.get("recv_us")
    if (
        isinstance(source_sequence, bool)
        or not isinstance(source_sequence, int)
        or not 0 <= source_sequence <= MAX_U64
    ):
        raise RejectedEvent("source_sequence must be u64")
    if isinstance(ts_us, bool) or not isinstance(ts_us, int) or not 0 <= ts_us <= MAX_U64:
        raise RejectedEvent("ts_us must be u64")
    if (
        isinstance(recv_us, bool)
        or not isinstance(recv_us, int)
        or not 0 <= recv_us <= MAX_U64
    ):
        raise RejectedEvent("recv_us must be u64")

    parts = topic.split("/")
    if len(parts) < 6 or parts[:3] != ["gridedge", "market", "v1"]:
        raise RejectedEvent("MQTT topic is outside the market v1 namespace")
    if parts[3] != venue or parts[4] != symbol or parts[5] != stream:
        raise RejectedEvent("MQTT topic disagrees with market event identity")
    if event_type == "BAR" and len(parts) != 7:
        raise RejectedEvent("BAR topic must include one interval component")
    if event_type != "BAR" and len(parts) != 6:
        raise RejectedEvent("non-BAR topic has unexpected path components")

    return ValidatedEvent(
        event_id=event_id,
        schema_version=1,
        event_type=event_type,
        source_id=source_id,
        source_instance_id=source_instance_id,
        source_sequence=source_sequence,
        source_metadata=source,
        venue=venue,
        symbol=symbol,
        asset_class=asset_class,
        currency=currency,
        ts_us=ts_us,
        recv_us=recv_us,
        payload=payload,
        payload_sha256=hashlib.sha256(raw).digest(),
        evidence_sha256=bytes.fromhex(evidence_text),
        raw=raw,
    )


class EventStore:
    def __init__(self, connection_info: str):
        self.connection_info = connection_info
        self.connection: psycopg.Connection[Any] | None = None
        self.lock = threading.Lock()

    def _connect(self) -> psycopg.Connection[Any]:
        if psycopg is None:
            raise RuntimeError("psycopg is required to run the market ingestor")
        if self.connection is None or self.connection.closed:
            self.connection = psycopg.connect(self.connection_info, autocommit=False)
        return self.connection

    def ingest(self, event: ValidatedEvent, topic: str, qos: int, duplicate_flag: bool) -> str:
        with self.lock:
            connection = self._connect()
            try:
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT event_id, source_id, source_instance_id, source_sequence, "
                            "payload_sha256 FROM market_events "
                            "WHERE event_id=%s OR "
                            "(source_id=%s AND source_instance_id=%s AND source_sequence=%s) "
                            "FOR UPDATE",
                            (
                                event.event_id,
                                event.source_id,
                                event.source_instance_id,
                                event.source_sequence,
                            ),
                        )
                        existing = cursor.fetchone()
                        if existing is not None:
                            same_identity = (
                                bytes(existing[0]) == event.event_id
                                and existing[1] == event.source_id
                                and existing[2] == event.source_instance_id
                                and existing[3] == event.source_sequence
                                and bytes(existing[4]) == event.payload_sha256
                            )
                            if same_identity:
                                cursor.execute(
                                    "UPDATE market_events SET delivery_count=delivery_count+1, "
                                    "last_received_at=clock_timestamp() WHERE event_id=%s",
                                    (event.event_id,),
                                )
                                cursor.execute(
                                    "INSERT INTO market_duplicate_deliveries "
                                    "(event_id,payload_sha256,mqtt_topic,qos,duplicate_flag) "
                                    "VALUES (%s,%s,%s,%s,%s)",
                                    (event.event_id, event.payload_sha256, topic, qos, duplicate_flag),
                                )
                                return "duplicate"
                            cursor.execute(
                                "INSERT INTO market_conflicts "
                                "(claimed_event_id,source_id,source_instance_id,source_sequence,"
                                "payload_sha256,mqtt_topic,reason,payload_bytes) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                (
                                    event.event_id,
                                    event.source_id,
                                    event.source_instance_id,
                                    event.source_sequence,
                                    event.payload_sha256,
                                    topic,
                                    "event_id or source sequence reused with different content",
                                    event.raw,
                                ),
                            )
                            return "conflict"

                        cursor.execute(
                            "INSERT INTO market_events "
                            "(event_id,schema_version,event_type,source_id,source_instance_id,"
                            "source_sequence,source_metadata,venue,symbol,asset_class,currency,"
                            "ts_us,recv_us,payload_format,payload_json,payload_bytes,payload_sha256,"
                            "evidence_sha256,mqtt_topic,qos) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                event.event_id,
                                event.schema_version,
                                event.event_type,
                                event.source_id,
                                event.source_instance_id,
                                event.source_sequence,
                                Jsonb(event.source_metadata),
                                event.venue,
                                event.symbol,
                                event.asset_class,
                                event.currency,
                                event.ts_us,
                                event.recv_us,
                                "application/json",
                                Jsonb(event.payload),
                                event.raw,
                                event.payload_sha256,
                                event.evidence_sha256,
                                topic,
                                qos,
                            ),
                        )
                        return "inserted"
            except Exception:
                connection.rollback()
                self.connection = None
                raise

    def reject(self, topic: str, raw: bytes, reason: str) -> None:
        with self.lock:
            connection = self._connect()
            with connection.transaction():
                connection.execute(
                    "INSERT INTO market_rejections (mqtt_topic,payload_sha256,reason,payload_bytes) "
                    "VALUES (%s,%s,%s,%s)",
                    (topic, hashlib.sha256(raw).digest(), reason, raw),
                )


def read_secret(path: str) -> str:
    with open(path, encoding="utf-8") as secret_file:
        value = secret_file.read().strip()
    if not value:
        raise RuntimeError(f"secret is empty: {path}")
    return value


def main() -> int:
    if mqtt is None or psycopg is None or Jsonb is None:
        raise RuntimeError("paho-mqtt and psycopg are required to run the market ingestor")
    postgres_password = read_secret(os.environ["POSTGRES_PASSWORD_FILE"])
    mqtt_password = read_secret(os.environ["MQTT_PASSWORD_FILE"])
    connection_info = (
        f"host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']} "
        f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
        f"password={postgres_password} connect_timeout=5"
    )
    store = EventStore(connection_info)
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=os.environ["MQTT_CLIENT_ID"],
        protocol=mqtt.MQTTv5,
        manual_ack=True,
    )
    client.username_pw_set(os.environ["MQTT_USERNAME"], mqtt_password)
    client.tls_set(ca_certs=os.environ["MQTT_CA_FILE"], tls_version=ssl.PROTOCOL_TLS_CLIENT)

    def on_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        if reason_code.is_failure:
            print(f"MQTT connection rejected: {reason_code}", file=sys.stderr, flush=True)
            return
        client.subscribe(os.environ["MQTT_TOPIC"], qos=1)
        print("market ingestor connected with MQTT 5", flush=True)

    def on_message(client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage) -> None:
        try:
            content_type = getattr(message.properties, "ContentType", None)
            validate_delivery(content_type, message.qos, message.retain)
            event = validate_document(bytes(message.payload), message.topic)
            result = store.ingest(event, message.topic, message.qos, message.dup)
            if result == "conflict":
                print(f"market identity conflict: {message.topic}", file=sys.stderr, flush=True)
            client.ack(message.mid, message.qos)
        except RejectedEvent as error:
            try:
                store.reject(message.topic, bytes(message.payload), str(error))
                client.ack(message.mid, message.qos)
            except Exception as store_error:
                print(f"failed to persist rejected market event: {store_error}", file=sys.stderr, flush=True)
        except Exception as error:
            print(f"transient market ingest failure: {error}", file=sys.stderr, flush=True)

    client.on_connect = on_connect
    client.on_message = on_message
    stopping = threading.Event()

    def stop(_signal: int, _frame: Any) -> None:
        stopping.set()
        client.disconnect()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client.connect(
        os.environ["MQTT_HOST"],
        int(os.environ["MQTT_PORT"]),
        keepalive=30,
        clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY,
    )
    while not stopping.is_set():
        client.loop(timeout=1.0)
        time.sleep(0.01)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
