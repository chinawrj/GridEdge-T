"""Independent read-only migration evidence tests on real temporary SQLite DBs.

Minimal projection fixtures are not full Rust accounting/release certification.
"""
from contextlib import closing, contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

RUN = "ths-002256-20260819-grid15-opening-v1"
INSTANCE = "1234567890abcdef1234567890abcdef"
OLD = hashlib.sha256(b"old reviewed platform").hexdigest()
NEW = hashlib.sha256(b"new reviewed platform").hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def sha(value):
    return hashlib.sha256(value).hexdigest()


@contextmanager
def fixture_database(path):
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            yield connection


class PostcloseReaderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gridedge-sqlite-reader-test-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "runtime").mkdir()
        self.ledger = self.root / "runtime/002256-grid.db"
        self.outbox = self.root / "runtime/002256-outbox-opening-v1.db"
        with fixture_database(self.ledger) as db:
            db.executescript("""
                CREATE TABLE events(sequence_number INTEGER, event_type TEXT, payload TEXT, run_id TEXT, event_id TEXT);
                CREATE TABLE database_identity(singleton INTEGER PRIMARY KEY, instance_id TEXT);
                CREATE TABLE paper_accounts(run_id TEXT PRIMARY KEY, snapshot_json TEXT, updated_at TEXT);
            """)
            db.execute("INSERT INTO database_identity VALUES(1,?)", (INSTANCE,))
            db.execute("INSERT INTO paper_accounts VALUES(?,?,?)", (RUN, encoded({"open_order_ids": []}), "2026-09-14T19:30:00"))
        self.append("RUN_STARTED", {})
        self.append("ALGORITHM_REGISTERED", {"platform_sha256": OLD})
        self.append("SERVICE_MODE_CHANGED", {"mode": "READ_ONLY"})
        with fixture_database(self.outbox) as db:
            db.executescript("""
                CREATE TABLE outbox_metadata(singleton INTEGER PRIMARY KEY, schema_version INTEGER,
                    source_database_instance_id TEXT, run_id TEXT, cursor INTEGER);
                CREATE TABLE remote_adapter_binding(singleton INTEGER PRIMARY KEY,
                    source_database_instance_id TEXT, run_id TEXT, identity_sha256 TEXT,
                    identity_json TEXT, bound_at TEXT);
                CREATE TABLE remote_adapter_binding_history(revision INTEGER PRIMARY KEY,
                    source_database_instance_id TEXT, run_id TEXT, identity_sha256 TEXT,
                    identity_json TEXT, previous_identity_sha256 TEXT, bound_at TEXT, binding_kind TEXT);
                CREATE TABLE staged_intents(intent_id TEXT PRIMARY KEY, order_id TEXT,
                    source_intent_sequence INTEGER, source_submitted_sequence INTEGER,
                    source_event_time TEXT, request_sha256 TEXT, request_json TEXT, state TEXT,
                    remote_contract_id TEXT, cancel_state TEXT);
                CREATE TABLE remote_execution_facts(fact_id TEXT PRIMARY KEY, intent_id TEXT,
                    request_sha256 TEXT, remote_contract_id TEXT, terminal_kind TEXT,
                    evidence_contract_version INTEGER, observed_at TEXT, fill_row_count INTEGER,
                    filled_quantity INTEGER, filled_notional TEXT, evidence_json TEXT);
            """)
            db.execute("INSERT INTO outbox_metadata VALUES(1,5,?,?,3)", (INSTANCE, RUN))
            identity = encoded({"schema": "gridedge.ths-remote-execution-identity.v1", "adapter": "ANDROID", "platform_sha256": OLD})
            self.genesis_json = identity
            self.genesis_sha = sha(identity.encode())
            db.execute("INSERT INTO remote_adapter_binding VALUES(1,?,?,?,?,?)", (INSTANCE, RUN, self.genesis_sha, identity, "2026-09-14T19:30:00"))
            db.execute("INSERT INTO remote_adapter_binding_history VALUES(1,?,?,?,?,?,?,?)", (INSTANCE, RUN, self.genesis_sha, identity, None, "2026-09-14T19:30:00", "GENESIS"))
        spec = importlib.util.spec_from_file_location("postclose_reader_under_test", Path(__file__).with_name("postclose_core_migration.py"))
        self.subject = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.subject
        spec.loader.exec_module(self.subject)

    def append(self, kind, payload, run=RUN):
        with fixture_database(self.ledger) as db:
            sequence = db.execute("SELECT COALESCE(MAX(sequence_number),0)+1 FROM events WHERE run_id=?", (run,)).fetchone()[0]
            event_id = "e" + str(sequence)
            db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (sequence, kind, encoded(payload), run, event_id))
        return sequence, event_id

    def authorize(self):
        self.authorization = {
            "upgrade_id": "reviewed-upgrade-1", "from_platform_sha256": OLD, "to_platform_sha256": NEW,
            "algorithm_contract_sha256": sha(b"algorithm"), "config_content_sha256": sha(b"config"),
            "reason_code": "COMPLETION_GATE_FIX", "operator": "fixture-reviewer",
            "authorization_kind": "LOCAL_OPERATOR_EXPLICIT", "expected_head_sequence": 3,
            "certification_profile_version": "GRIDEDGE_PLATFORM_UPGRADE_CERTIFICATION_V1",
            "certification_evidence_sha256": sha(b"fixture certificate"), "target_binary_sha256": NEW,
            "authorized_at": "2026-09-14T19:30:00",
        }
        self.auth_seq, self.auth_id = self.append("PLATFORM_UPGRADE_AUTHORIZED", self.authorization)

    def activate(self, **changes):
        data = dict(upgrade_id=self.authorization["upgrade_id"], authorization_event_id=self.auth_id,
                    authorization_sequence=self.auth_seq, from_platform_sha256=OLD, to_platform_sha256=NEW,
                    observed_platform_sha256=NEW, validated_through_sequence=self.auth_seq,
                    full_rebuild_state_sha256=sha(b"fixture rebuild"), paper_snapshot_sha256=sha(b"fixture paper"),
                    paper_reconciled=True, activated_at="2026-09-14T19:30:01")
        data.update(changes)
        self.append("PLATFORM_UPGRADE_ACTIVATED", data)

    def read(self):
        before = {path: path.read_bytes() for path in (self.ledger, self.outbox)}
        try:
            return self.subject.read_state(self.root, RUN)
        finally:
            for path, value in before.items():
                self.assertEqual(path.read_bytes(), value, "read_state changed SQLite bytes/header")

    def unsafe(self):
        try:
            state = self.read()
        except (ValueError, sqlite3.DatabaseError):
            return
        self.assertFalse(state["terminal_ok"] and state["integrity_ok"], "unsafe state returned an execution permit")

    def test_clean_baseline_is_read_only_and_explicitly_source_bound(self):
        original_connect = sqlite3.connect
        traces = []
        def connect(path, *args, **kwargs):
            self.assertIn("mode=ro", str(path))
            self.assertTrue(kwargs.get("uri"))
            connection = original_connect(path, *args, **kwargs)
            connection.set_trace_callback(traces.append)
            return connection
        with patch("sqlite3.connect", side_effect=connect):
            state = self.read()
        self.assertEqual(state["effective"], OLD)
        self.assertIsNone(state["pending"])
        self.assertEqual((state["head"], state["cursor"]), (3, 3))
        self.assertEqual(state["source_database_instance_id"], INSTANCE)
        self.assertEqual(state["binding_sha256"], self.genesis_sha)
        self.assertEqual(state["binding_revision"], 1)
        self.assertEqual(state["binding_platform"], OLD)
        self.assertTrue(state["terminal_ok"] and state["integrity_ok"])
        self.assertTrue(any(query.strip().upper().startswith("BEGIN") for query in traces))
        self.assertTrue(any("integrity_check" in query.lower() for query in traces))
        self.assertFalse(any(query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER")) for query in traces))

    def test_authorization_committed_response_lost_is_pending_not_reauthorized(self):
        self.authorize()
        state = self.read()
        self.assertEqual(state["effective"], OLD)
        self.assertEqual(state["pending"], NEW)
        self.assertEqual(state["authorization"], self.authorization)
        self.assertEqual((state["head"], state["cursor"]), (4, 3))

    def test_valid_adjacent_activation_is_truth_even_before_binding_sync(self):
        self.authorize()
        self.activate()
        state = self.read()
        self.assertEqual(state["effective"], NEW)
        self.assertIsNone(state["pending"])
        self.assertEqual(state["binding_platform"], OLD)
        self.assertEqual((state["head"], state["cursor"]), (5, 3))

    def test_activation_must_bind_exact_authorization_reference(self):
        self.authorize()
        for field, value in (("authorization_event_id", "foreign"), ("authorization_sequence", 3),
                             ("upgrade_id", "foreign"), ("from_platform_sha256", "0" * 64),
                             ("observed_platform_sha256", OLD), ("paper_reconciled", False),
                             ("validated_through_sequence", 2)):
            with self.subTest(field=field):
                self.activate(**{field: value})
                with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                    self.read()
                with fixture_database(self.ledger) as db:
                    db.execute("DELETE FROM events WHERE sequence_number=5")

    def test_business_event_between_authorization_and_activation_is_rejected(self):
        self.authorize()
        self.append("SERVICE_MODE_CHANGED", {"mode": "READ_ONLY"})
        self.activate()
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            self.read()

    def test_authorization_wrong_head_target_or_from_is_rejected(self):
        self.authorize()
        for field, value in (("expected_head_sequence", 2), ("target_binary_sha256", OLD),
                             ("from_platform_sha256", NEW), ("to_platform_sha256", OLD)):
            with self.subTest(field=field):
                changed = dict(self.authorization, **{field: value})
                with fixture_database(self.ledger) as db:
                    db.execute("UPDATE events SET payload=? WHERE sequence_number=4", (encoded(changed),))
                with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                    self.read()

    def test_foreign_database_run_and_cursor_ahead_are_rejected(self):
        for column, value in (("source_database_instance_id", "f" * 32), ("run_id", "foreign"), ("cursor", 99)):
            with self.subTest(column=column):
                with fixture_database(self.outbox) as db:
                    db.execute(f"UPDATE outbox_metadata SET {column}=?", (value,))
                with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                    self.read()
                with fixture_database(self.outbox) as db:
                    db.execute("UPDATE outbox_metadata SET source_database_instance_id=?,run_id=?,cursor=3", (INSTANCE, RUN))

    def test_binding_hash_prior_revision_or_genesis_projection_drift_is_rejected(self):
        for column, value in (("identity_sha256", "0" * 64), ("previous_identity_sha256", "0" * 64),
                             ("revision", 2), ("run_id", "foreign"), ("binding_kind", "PLATFORM_UPGRADE")):
            with self.subTest(column=column):
                with fixture_database(self.outbox) as db:
                    original = db.execute(f"SELECT {column} FROM remote_adapter_binding_history").fetchone()[0]
                    db.execute(f"UPDATE remote_adapter_binding_history SET {column}=?", (value,))
                with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                    self.read()
                with fixture_database(self.outbox) as db:
                    db.execute(f"UPDATE remote_adapter_binding_history SET {column}=?", (original,))
        with fixture_database(self.outbox) as db:
            db.execute("UPDATE remote_adapter_binding SET identity_json='{}'")
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            self.read()

    def intent(self, state="SUBMITTED", cancel="NOT_REQUESTED"):
        with fixture_database(self.outbox) as db:
            db.execute("INSERT INTO staged_intents (intent_id,order_id,source_intent_sequence,"
                       "source_submitted_sequence,source_event_time,request_sha256,request_json,"
                       "state,remote_contract_id,cancel_state) VALUES(?,?,?,?,?,?,?,?,?,?)", (
                "intent-1", "order-1", 1, 2, "2026-09-14T10:00:00", sha(b"request"), "{}", state, "contract-1", cancel))

    def use_production_intent_schema(self):
        source = (Path(__file__).resolve().parents[2] / "src/ths_sim_outbox.rs").read_text()
        ddl = re.search(r"CREATE TABLE IF NOT EXISTS staged_intents\(.*?\n\s*\);", source, re.S)
        self.assertIsNotNone(ddl, "production intent DDL not found; review schema explicitly")
        with fixture_database(self.outbox) as db:
            db.execute("DROP TABLE staged_intents")
            db.execute(ddl.group())

    def test_production_schema_rejects_null_state_and_cancel_state(self):
        self.use_production_intent_schema()
        for arguments in ({"state": None}, {"cancel": None}):
            with self.subTest(arguments=arguments), self.assertRaises(sqlite3.IntegrityError):
                self.intent(**arguments)

    def test_nullable_schema_drift_cannot_turn_unknown_cancel_into_terminal(self):
        # Defensive legacy/corrupt-schema fixture, NOT reachable under normal
        # production NOT NULL constraints and NOT an observed production fault.
        self.intent(cancel=None)
        self.unsafe()

    def test_production_schema_nullable_contract_cannot_certify_cancel_terminal(self):
        # Physical schema admits these rows; application validity is separate.
        # This is a synthetic inconsistency, not evidence it occurred in service.
        self.use_production_intent_schema()
        self.intent(cancel="CANCELLED")
        self.assertTrue(self.read()["terminal_ok"])
        for value in (None, ""):
            with self.subTest(contract=value):
                with fixture_database(self.outbox) as db:
                    db.execute("UPDATE staged_intents SET remote_contract_id=?", (value,))
                    self.assertEqual(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
                self.unsafe()

    def test_production_schema_missing_submitted_reference_is_not_terminal(self):
        self.use_production_intent_schema()
        self.intent(cancel="CANCELLED")
        with fixture_database(self.outbox) as db:
            db.execute("UPDATE staged_intents SET source_submitted_sequence=NULL")
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
        self.unsafe()

    def test_unresolved_intent_and_wrong_contract_or_request_fact_cannot_pass(self):
        self.intent()
        self.unsafe()
        for column, value in (("remote_contract_id", "foreign"), ("request_sha256", "0" * 64)):
            with self.subTest(column=column):
                with fixture_database(self.outbox) as db:
                    db.execute("INSERT INTO remote_execution_facts VALUES(?,?,?,?,?,?,?,?,?,?,?)", (
                        "fact-1", "intent-1", sha(b"request"), "contract-1", "FILLED", 1,
                        "2026-09-14T10:00:01", 1, 100, "350", "{}"))
                    db.execute(f"UPDATE remote_execution_facts SET {column}=?", (value,))
                self.unsafe()
                with fixture_database(self.outbox) as db:
                    db.execute("DELETE FROM remote_execution_facts")

    def test_open_paper_order_or_ambiguous_remote_state_is_not_terminal(self):
        with fixture_database(self.ledger) as db:
            db.execute("UPDATE paper_accounts SET snapshot_json=?", (encoded({"open_order_ids": ["order-1"]}),))
        self.unsafe()
        with fixture_database(self.ledger) as db:
            db.execute("UPDATE paper_accounts SET snapshot_json=?", (encoded({"open_order_ids": []}),))
        self.intent(state="AMBIGUOUS", cancel="CANCELLED")
        self.unsafe()

    def test_missing_database_is_not_created_and_journal_gap_is_rejected(self):
        missing = self.root / "missing-installation"
        with self.assertRaises((ValueError, OSError, sqlite3.DatabaseError)):
            self.subject.read_state(missing, RUN)
        self.assertFalse(missing.exists())
        with fixture_database(self.ledger) as db:
            db.execute("DELETE FROM events WHERE sequence_number=2")
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            self.read()

    def test_valid_binding_history_uses_latest_revision_but_preserves_genesis(self):
        self.authorize()
        self.activate()
        target_json = encoded({"schema": "gridedge.ths-remote-execution-identity.v1", "adapter": "ANDROID", "platform_sha256": NEW})
        target_sha = sha(target_json.encode())
        with fixture_database(self.outbox) as db:
            db.execute("INSERT INTO remote_adapter_binding_history VALUES(2,?,?,?,?,?,?,?)", (
                INSTANCE, RUN, target_sha, target_json, self.genesis_sha, "2026-09-14T19:30:02", "PLATFORM_UPGRADE"))
        state = self.read()
        self.assertEqual(state["binding_revision"], 2)
        self.assertEqual(state["binding_sha256"], target_sha)
        self.assertEqual(state["binding_previous_sha256"], self.genesis_sha)
        self.assertEqual(state["binding_platform"], NEW)
        with fixture_database(self.outbox) as db:
            db.execute("INSERT INTO remote_adapter_binding_history VALUES(3,?,?,?,?,?,?,?)", (
                INSTANCE, RUN, self.genesis_sha, self.genesis_json, self.genesis_sha, "2026-09-14T19:30:03", "PLATFORM_UPGRADE"))
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            self.read()

    def test_prior_platform_cannot_be_reauthorized_as_a_file_rollback(self):
        self.authorize()
        self.activate()
        rollback = dict(self.authorization, upgrade_id="rollback-to-used-platform", from_platform_sha256=NEW,
                        to_platform_sha256=OLD, target_binary_sha256=OLD, expected_head_sequence=5)
        self.append("PLATFORM_UPGRADE_AUTHORIZED", rollback)
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            self.read()


if __name__ == "__main__":
    unittest.main()
