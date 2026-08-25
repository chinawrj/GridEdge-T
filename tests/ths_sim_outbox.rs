use anyhow::Result;
use chrono::{Duration, NaiveDateTime};
use gridedge_t::{
    domain::{Direction, Order, OrderIntent, OrderStatus},
    event::{current_event_schema, EventEnvelope, EventType},
    ths_sim_outbox::{StagedIntentState, StagedTerminalResolution, ThsSimOutbox},
};
use rusqlite::Connection;
use rust_decimal::Decimal;
use serde_json::json;
use tempfile::tempdir;

const SOURCE_A: &str = "0123456789abcdef0123456789abcdef";
const SOURCE_B: &str = "fedcba9876543210fedcba9876543210";

fn android_identity(platform: char, serial: &str) -> String {
    json!({
        "schema": "gridedge.ths-remote-execution-identity.v1",
        "adapter": "ANDROID",
        "platform_sha256": platform.to_string().repeat(64),
        "runner_sha256": "1".repeat(64),
        "launch_plist_sha256": "2".repeat(64),
        "adb_sha256": "3".repeat(64),
        "serial": serial,
        "avd_name_marker": "THSP_API_32",
        "package": "com.hexin.plat.android.supremacy",
        "tested_version": "10.94.09",
        "masked_account_sha256": "4".repeat(64),
        "confirmation_account_sha256": "5".repeat(64),
        "expected_symbol": "002256",
        "expected_security_name": "兆新股份",
        "money_actions_enabled": true,
    })
    .to_string()
}

#[test]
fn outbox_requires_an_explicit_history_boundary_and_immutable_source() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;

    assert!(outbox.bind_or_verify(SOURCE_A, "run-a", None).is_err());
    assert_eq!(outbox.bind_or_verify(SOURCE_A, "run-a", Some(120))?, 120);
    assert_eq!(outbox.bind_or_verify(SOURCE_A, "run-a", Some(120))?, 120);
    assert!(outbox.bind_or_verify(SOURCE_A, "run-a", Some(119)).is_err());
    assert!(outbox.bind_or_verify(SOURCE_B, "run-a", Some(120)).is_err());
    assert!(outbox.bind_or_verify(SOURCE_A, "run-b", Some(120)).is_err());
    Ok(())
}

#[test]
fn remote_execution_identity_is_durable_immutable_and_bound_to_run_platform_and_account(
) -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-remote-identity.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    let android_a = android_identity('a', "emulator-5554");
    let android_b = android_identity('a', "emulator-5556");

    let identity_sha = outbox.bind_execution_identity_once(&android_a)?;
    assert_eq!(identity_sha.len(), 64);
    assert_eq!(outbox.verify_execution_identity(&android_a)?, identity_sha);
    assert!(outbox.bind_execution_identity_once(&android_b).is_err());
    assert!(outbox.verify_execution_identity(&android_b).is_err());
    drop(outbox);

    let reopened = ThsSimOutbox::open(&path)?;
    assert_eq!(
        reopened.verify_execution_identity(&android_a)?,
        identity_sha
    );
    drop(reopened);
    let connection = Connection::open(&path)?;
    assert!(connection
        .execute(
            "UPDATE remote_adapter_binding SET identity_sha256=?1 WHERE singleton=1",
            ["c".repeat(64)],
        )
        .is_err());
    assert!(connection
        .execute("DELETE FROM remote_adapter_binding WHERE singleton=1", [])
        .is_err());
    Ok(())
}

#[test]
fn execution_identity_platform_upgrade_is_append_only_chained_idempotent_and_stable() -> Result<()>
{
    let directory = tempdir()?;
    let path = directory.path().join("ths-remote-identity-upgrade.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    let identity_a = android_identity('a', "emulator-5554");
    let identity_b = android_identity('b', "emulator-5554");
    let identity_changed_account = identity_b.replace(&"5".repeat(64), &"6".repeat(64));
    let sha_a = outbox.bind_execution_identity_once(&identity_a)?;
    assert!(outbox.verify_execution_identity(&identity_b).is_err());
    let sha_b = outbox.upgrade_execution_identity(&identity_b)?;
    assert_ne!(sha_a, sha_b);
    assert_eq!(outbox.verify_execution_identity(&identity_b)?, sha_b);
    assert!(outbox.verify_execution_identity(&identity_a).is_err());
    assert_eq!(outbox.upgrade_execution_identity(&identity_b)?, sha_b);
    assert!(outbox
        .upgrade_execution_identity(&identity_changed_account)
        .is_err());
    assert!(outbox.upgrade_execution_identity(&identity_a).is_err());
    drop(outbox);

    let connection = Connection::open(&path)?;
    let revisions: i64 = connection.query_row(
        "SELECT COUNT(*) FROM remote_adapter_binding_history",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(revisions, 2);
    let previous: String = connection.query_row(
        "SELECT previous_identity_sha256 FROM remote_adapter_binding_history WHERE revision=2",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(previous, sha_a);
    assert!(connection
        .execute(
            "UPDATE remote_adapter_binding_history SET identity_sha256=?1 WHERE revision=2",
            ["f".repeat(64)],
        )
        .is_err());
    assert!(connection
        .execute(
            "DELETE FROM remote_adapter_binding_history WHERE revision=2",
            [],
        )
        .is_err());
    Ok(())
}

#[test]
fn execution_identity_upgrade_requires_a_frozen_terminal_outbox() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-upgrade-frozen.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    outbox.ingest(&[intent_event(1, "intent-1", "order-1", 100)])?;
    assert!(outbox
        .upgrade_execution_identity(&android_identity('b', "emulator-5554"))
        .is_err());
    let connection = Connection::open(&path)?;
    let revisions: i64 = connection.query_row(
        "SELECT COUNT(*) FROM remote_adapter_binding_history",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(revisions, 1);
    Ok(())
}

#[test]
fn physical_v4_identity_binding_migrates_atomically_and_preserves_genesis_bytes() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-v4.db");
    let identity = android_identity('a', "emulator-5554");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    let identity_sha = outbox.bind_execution_identity_once(&identity)?;
    drop(outbox);

    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_delete", [])?;
    connection.execute("DROP TABLE remote_adapter_binding_history", [])?;
    connection.execute(
        "UPDATE outbox_metadata SET schema_version=4 WHERE singleton=1",
        [],
    )?;
    connection.execute_batch(
        "CREATE TRIGGER block_outbox_v5
         BEFORE UPDATE OF schema_version ON outbox_metadata
         WHEN NEW.schema_version=5
         BEGIN SELECT RAISE(ABORT,'blocked v5 migration'); END;",
    )?;
    drop(connection);

    assert!(ThsSimOutbox::open(&path).is_err());
    let connection = Connection::open(&path)?;
    let schema: i64 = connection.query_row(
        "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(schema, 4);
    let history_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='remote_adapter_binding_history'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(history_count, 0);
    connection.execute("DROP TRIGGER block_outbox_v5", [])?;
    drop(connection);

    let reopened = ThsSimOutbox::open(&path)?;
    assert_eq!(reopened.verify_execution_identity(&identity)?, identity_sha);
    drop(reopened);
    let connection = Connection::open(&path)?;
    let genesis: (String, String) = connection.query_row(
        "SELECT identity_sha256,identity_json FROM remote_adapter_binding WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let history: (String, String, Option<String>) = connection.query_row(
        "SELECT identity_sha256,identity_json,previous_identity_sha256
           FROM remote_adapter_binding_history WHERE revision=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(genesis.0, identity_sha);
    assert_eq!(genesis.0, history.0);
    assert_eq!(genesis.1, history.1);
    assert_eq!(history.2, None);
    Ok(())
}

#[test]
fn execution_identity_chain_tampering_fails_closed() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-chain-tamper.db");
    let identity_a = android_identity('a', "emulator-5554");
    let identity_b = android_identity('b', "emulator-5554");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&identity_a)?;
    outbox.upgrade_execution_identity(&identity_b)?;
    drop(outbox);
    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_delete", [])?;
    connection.execute(
        "UPDATE remote_adapter_binding_history SET previous_identity_sha256=?1 WHERE revision=2",
        ["f".repeat(64)],
    )?;
    connection.execute_batch(
        "CREATE TRIGGER remote_adapter_binding_history_no_update
         BEFORE UPDATE ON remote_adapter_binding_history
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;
         CREATE TRIGGER remote_adapter_binding_history_no_delete
         BEFORE DELETE ON remote_adapter_binding_history
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;",
    )?;
    drop(connection);
    let mut reopened = ThsSimOutbox::open(&path)?;
    assert!(reopened.verify_execution_identity(&identity_b).is_err());
    assert!(reopened
        .upgrade_execution_identity(&android_identity('c', "emulator-5554"))
        .is_err());
    Ok(())
}

#[test]
fn execution_identity_upgrade_rejects_wrong_source_or_run_before_any_revision() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-wrong-ledger.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    assert!(outbox.bind_or_verify(SOURCE_B, "run-a", None).is_err());
    assert!(outbox.bind_or_verify(SOURCE_A, "run-b", None).is_err());
    drop(outbox);
    let connection = Connection::open(&path)?;
    let revisions: i64 = connection.query_row(
        "SELECT COUNT(*) FROM remote_adapter_binding_history",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(revisions, 1);
    Ok(())
}

#[test]
fn schema_v5_rejects_missing_append_only_trigger() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-missing-trigger.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    drop(outbox);
    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    drop(connection);
    assert!(ThsSimOutbox::open(&path).is_err());
    Ok(())
}

#[test]
fn schema_v5_rejects_named_but_behaviorally_inert_append_only_triggers() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-noop-trigger.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    drop(outbox);
    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_delete", [])?;
    connection.execute_batch(
        "CREATE TRIGGER remote_adapter_binding_history_no_update
         BEFORE UPDATE ON remote_adapter_binding_history WHEN 0
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;
         CREATE TRIGGER remote_adapter_binding_history_no_delete
         BEFORE DELETE ON remote_adapter_binding_history WHEN 0
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;",
    )?;
    drop(connection);
    assert!(ThsSimOutbox::open(&path).is_err());
    Ok(())
}

#[test]
fn schema_v5_rejects_trigger_that_only_blocks_same_value_updates() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-conditional-update.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    drop(outbox);
    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    connection.execute_batch(
        "CREATE TRIGGER remote_adapter_binding_history_no_update
         BEFORE UPDATE ON remote_adapter_binding_history WHEN OLD.bound_at=NEW.bound_at
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;",
    )?;
    drop(connection);
    assert!(ThsSimOutbox::open(&path).is_err());
    Ok(())
}

#[test]
fn schema_v5_rejects_delete_trigger_that_only_protects_genesis() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-identity-conditional-delete.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.bind_execution_identity_once(&android_identity('a', "emulator-5554"))?;
    outbox.upgrade_execution_identity(&android_identity('b', "emulator-5554"))?;
    drop(outbox);
    let connection = Connection::open(&path)?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_delete", [])?;
    connection.execute_batch(
        "CREATE TRIGGER remote_adapter_binding_history_no_delete
         BEFORE DELETE ON remote_adapter_binding_history WHEN OLD.revision=1
         BEGIN SELECT RAISE(ABORT,'remote adapter binding history is append-only'); END;",
    )?;
    drop(connection);
    assert!(ThsSimOutbox::open(&path).is_err());
    Ok(())
}

#[test]
fn physical_v1_outbox_migrates_to_source_cancellation_state_without_rebinding_source() -> Result<()>
{
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox-v1.db");
    let connection = Connection::open(&path)?;
    connection.execute_batch(&format!(
        "CREATE TABLE outbox_metadata(
           singleton INTEGER PRIMARY KEY,
           schema_version INTEGER NOT NULL,
           source_database_instance_id TEXT NOT NULL,
           run_id TEXT NOT NULL,
           cursor INTEGER NOT NULL
         );
         INSERT INTO outbox_metadata VALUES(1,1,'{SOURCE_A}','run-a',42);
         CREATE TABLE staged_intents(
           intent_id TEXT PRIMARY KEY,
           order_id TEXT NOT NULL UNIQUE,
           source_intent_sequence INTEGER NOT NULL UNIQUE,
           source_submitted_sequence INTEGER UNIQUE,
           source_event_time TEXT NOT NULL,
           request_sha256 TEXT NOT NULL,
           request_json TEXT NOT NULL,
           state TEXT NOT NULL,
           baseline_contract_ids_json TEXT,
           remote_contract_id TEXT UNIQUE,
           remote_evidence_json TEXT,
           last_error TEXT
         );"
    ))?;
    drop(connection);

    let mut outbox = ThsSimOutbox::open(&path)?;
    assert_eq!(outbox.bind_or_verify(SOURCE_A, "run-a", None)?, 42);
    assert!(outbox.staged_intents()?.is_empty());
    drop(outbox);

    let connection = Connection::open(&path)?;
    let schema: i64 = connection.query_row(
        "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(schema, 5);
    let columns = connection
        .prepare("PRAGMA table_info(staged_intents)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(columns.contains(&"cancel_state".to_owned()));
    assert!(columns.contains(&"cancel_evidence_json".to_owned()));
    assert!(columns.contains(&"cancel_error".to_owned()));
    assert!(columns.contains(&"source_cancel_requested_sequence".to_owned()));
    assert!(columns.contains(&"source_cancel_reason".to_owned()));
    let remote_fill_table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type='table' AND name='remote_execution_facts'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(remote_fill_table_count, 1);

    drop(connection);
    let mut reopened = ThsSimOutbox::open(&path)?;
    assert_eq!(reopened.bind_or_verify(SOURCE_A, "run-a", None)?, 42);
    Ok(())
}

#[test]
fn physical_v1_and_v2_migrations_are_atomic_recoverable_and_idempotent() -> Result<()> {
    for legacy_version in [1_i64, 2_i64] {
        let directory = tempdir()?;
        let path = directory
            .path()
            .join(format!("ths-outbox-v{legacy_version}.db"));
        create_physical_legacy_outbox(&path, legacy_version)?;

        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TRIGGER block_outbox_v3
             BEFORE UPDATE OF schema_version ON outbox_metadata
             WHEN NEW.schema_version=3
             BEGIN SELECT RAISE(ABORT,'blocked v3 migration'); END;",
        )?;
        drop(connection);

        assert!(ThsSimOutbox::open(&path).is_err());
        let connection = Connection::open(&path)?;
        let stored_version: i64 = connection.query_row(
            "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(stored_version, legacy_version);
        let columns = table_columns(&connection)?;
        assert_eq!(
            columns.contains(&"cancel_state".to_owned()),
            legacy_version == 2
        );
        assert!(!columns.contains(&"source_cancel_requested_sequence".to_owned()));
        let source_cancel_index_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='index' AND name='idx_staged_intents_source_cancel_sequence'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(source_cancel_index_count, 0);
        connection.execute("DROP TRIGGER block_outbox_v3", [])?;
        drop(connection);

        let mut migrated = ThsSimOutbox::open(&path)?;
        assert_eq!(migrated.bind_or_verify(SOURCE_A, "run-a", None)?, 42);
        drop(migrated);
        let mut reopened = ThsSimOutbox::open(&path)?;
        assert_eq!(reopened.bind_or_verify(SOURCE_A, "run-a", None)?, 42);

        let connection = Connection::open(&path)?;
        let stored_version: i64 = connection.query_row(
            "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(stored_version, 5);
        let columns = table_columns(&connection)?;
        for required in [
            "cancel_state",
            "cancel_evidence_json",
            "cancel_error",
            "source_cancel_requested_sequence",
            "source_cancel_reason",
        ] {
            assert!(columns.contains(&required.to_owned()));
        }
        let source_cancel_index_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='index' AND name='idx_staged_intents_source_cancel_sequence'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(source_cancel_index_count, 1);
        let remote_fill_table_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='table' AND name='remote_execution_facts'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(remote_fill_table_count, 1);
    }
    Ok(())
}

#[test]
fn physical_v3_remote_fill_migration_is_atomic_recoverable_and_idempotent() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox-v3.db");
    create_physical_legacy_outbox(&path, 2)?;
    {
        let mut migrated_to_v3 = ThsSimOutbox::open(&path)?;
        migrated_to_v3.bind_or_verify(SOURCE_A, "run-a", None)?;
    }
    let connection = Connection::open(&path)?;
    connection.execute("DROP TABLE remote_execution_facts", [])?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_update", [])?;
    connection.execute("DROP TRIGGER remote_adapter_binding_history_no_delete", [])?;
    connection.execute("DROP TABLE remote_adapter_binding_history", [])?;
    connection.execute(
        "UPDATE outbox_metadata SET schema_version=3 WHERE singleton=1",
        [],
    )?;
    connection.execute_batch(
        "CREATE TRIGGER block_outbox_v4
         BEFORE UPDATE OF schema_version ON outbox_metadata
         WHEN NEW.schema_version=4
         BEGIN SELECT RAISE(ABORT,'blocked v4 migration'); END;",
    )?;
    drop(connection);

    assert!(ThsSimOutbox::open(&path).is_err());
    let connection = Connection::open(&path)?;
    let stored_version: i64 = connection.query_row(
        "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored_version, 3);
    let fact_table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type='table' AND name='remote_execution_facts'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(fact_table_count, 0);
    connection.execute("DROP TRIGGER block_outbox_v4", [])?;
    drop(connection);

    let mut migrated = ThsSimOutbox::open(&path)?;
    assert_eq!(migrated.bind_or_verify(SOURCE_A, "run-a", None)?, 42);
    drop(migrated);
    let reopened = ThsSimOutbox::open(&path)?;
    assert!(reopened.staged_intents()?.is_empty());
    let connection = Connection::open(&path)?;
    let stored_version: i64 = connection.query_row(
        "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored_version, 5);
    let fact_table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type='table' AND name='remote_execution_facts'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(fact_table_count, 1);
    Ok(())
}

#[test]
fn durable_cancel_request_is_bound_to_exactly_one_staged_order_and_survives_restart() -> Result<()>
{
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
        cancel_requested_event(3, "order-a", "strategy replacement"),
    ])?;

    let staged = outbox.staged_intents()?;
    assert_eq!(staged[0].source_cancel_requested_sequence, Some(3));
    assert_eq!(
        staged[0].source_cancel_reason.as_deref(),
        Some("strategy replacement")
    );
    let status = outbox.status()?;
    assert_eq!(status.cursor, 3);
    assert_eq!(status.cancel_requested, 1);

    assert!(outbox
        .ingest(&[cancel_requested_event(
            4,
            "order-a",
            "duplicate cancellation"
        )])
        .is_err());
    assert_eq!(outbox.status()?.cursor, 3);
    assert_eq!(outbox.staged_intents()?, staged);

    assert!(outbox
        .ingest(&[cancel_requested_event(4, "missing-order", "unknown")])
        .is_err());
    assert_eq!(outbox.status()?.cursor, 3);
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    assert_eq!(reopened.bind_or_verify(SOURCE_A, "run-a", None)?, 3);
    assert_eq!(reopened.staged_intents()?, staged);
    Ok(())
}

#[test]
fn intent_becomes_eligible_only_after_the_durable_submitted_event() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(100))?;

    let discovered = outbox.ingest(&[intent_event(101, "intent-a", "order-a", 1500)])?;
    assert_eq!(discovered.cursor, 101);
    assert_eq!(discovered.discovered, 1);
    assert_eq!(discovered.eligible, 0);
    let staged = outbox.staged_intents()?;
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].state, StagedIntentState::Discovered);
    assert_eq!(staged[0].order.symbol, "002256");

    let eligible = outbox.ingest(&[submitted_event(102, "order-a")])?;
    assert_eq!(eligible.cursor, 102);
    assert_eq!(eligible.discovered, 0);
    assert_eq!(eligible.eligible, 1);
    let staged = outbox.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Eligible);
    assert_eq!(staged[0].source_submitted_sequence, Some(102));
    let now = NaiveDateTime::parse_from_str("2026-08-17 09:36:00", "%Y-%m-%d %H:%M:%S")?;
    staged[0].verify_execution_freshness(now, Duration::minutes(5))?;
    assert!(staged[0]
        .verify_execution_freshness(now + Duration::minutes(10), Duration::minutes(5))
        .is_err());
    assert!(staged[0]
        .verify_execution_freshness(now - Duration::minutes(5), Duration::minutes(5))
        .is_err());

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    assert_eq!(reopened.bind_or_verify(SOURCE_A, "run-a", None)?, 102);
    assert_eq!(reopened.staged_intents()?, staged);
    Ok(())
}

#[test]
fn unmatched_submit_or_changed_intent_rolls_back_cursor_and_rows() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;

    assert!(outbox.ingest(&[submitted_event(1, "missing")]).is_err());
    assert_eq!(outbox.status()?.cursor, 0);
    assert!(outbox.staged_intents()?.is_empty());

    outbox.ingest(&[intent_event(1, "intent-a", "order-a", 1500)])?;
    assert!(outbox
        .ingest(&[intent_event(2, "intent-a", "order-a", 1600)])
        .is_err());
    assert_eq!(outbox.status()?.cursor, 1);
    assert_eq!(outbox.staged_intents()?[0].order.quantity, 1500);

    assert!(outbox
        .ingest(&[intent_event(3, "intent-b", "order-b", 1500)])
        .is_err());
    assert_eq!(outbox.status()?.cursor, 1);
    Ok(())
}

#[test]
fn submission_claim_is_exactly_once_and_restart_never_reverts_to_eligible() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();

    outbox.begin_submission("intent-a", &request_sha, &["existing-contract".to_owned()])?;
    assert!(outbox
        .begin_submission("intent-a", &request_sha, &[])
        .is_err());
    assert_eq!(outbox.status()?.submitting, 1);
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    let staged = reopened.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Submitting);
    assert_eq!(staged[0].baseline_contract_ids, ["existing-contract"]);
    reopened.complete_submission(
        "intent-a",
        &request_sha,
        "new-contract",
        r#"{"symbol":"002256","quantity":1500}"#,
    )?;
    assert!(reopened
        .complete_submission("intent-a", &request_sha, "other", "{}")
        .is_err());
    let staged = reopened.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Submitted);
    assert_eq!(
        staged[0].remote_contract_id.as_deref(),
        Some("new-contract")
    );
    assert!(reopened
        .begin_submission("intent-a", &request_sha, &[])
        .is_err());
    assert_eq!(reopened.status()?.submitted, 1);
    Ok(())
}

#[test]
fn completion_cannot_relabel_a_preexisting_contract_as_the_new_submission() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &["existing-contract".to_owned()])?;

    let error = outbox
        .complete_submission(
            "intent-a",
            &request_sha,
            "existing-contract",
            r#"{"contract_id":"existing-contract"}"#,
        )
        .unwrap_err();

    assert!(error.to_string().contains("new contract"));
    let staged = outbox.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Submitting);
    assert_eq!(staged[0].remote_contract_id, None);
    Ok(())
}

#[test]
fn one_remote_contract_cannot_be_bound_to_two_durable_intents() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
        intent_event(3, "intent-b", "order-b", 1500),
        submitted_event(4, "order-b"),
    ])?;
    let staged = outbox.staged_intents()?;
    let first_sha = staged[0].request_sha256.clone();
    let second_sha = staged[1].request_sha256.clone();
    outbox.begin_submission("intent-a", &first_sha, &[])?;
    outbox.begin_submission("intent-b", &second_sha, &[])?;
    outbox.complete_submission(
        "intent-a",
        &first_sha,
        "same-contract",
        r#"{"contract_id":"same-contract"}"#,
    )?;

    assert!(outbox
        .complete_submission(
            "intent-b",
            &second_sha,
            "same-contract",
            r#"{"contract_id":"same-contract"}"#,
        )
        .is_err());

    let staged = outbox.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Submitted);
    assert_eq!(
        staged[0].remote_contract_id.as_deref(),
        Some("same-contract")
    );
    assert_eq!(staged[1].state, StagedIntentState::Submitting);
    assert_eq!(staged[1].remote_contract_id, None);
    Ok(())
}

#[test]
fn uncertain_ui_result_becomes_terminal_ambiguous_and_cannot_be_reclicked() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("ths-outbox.db");
    let mut outbox = ThsSimOutbox::open(&path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;

    outbox.mark_ambiguous(
        "intent-a",
        &request_sha,
        "no unique new contract after UI timeout",
        Some(r#"{"matching_contracts":[]}"#),
    )?;

    drop(outbox);
    let mut outbox = ThsSimOutbox::open(&path)?;

    let staged = outbox.staged_intents()?;
    assert_eq!(staged[0].state, StagedIntentState::Ambiguous);
    assert!(staged[0]
        .last_error
        .as_deref()
        .unwrap()
        .contains("no unique"));
    assert!(outbox
        .begin_submission("intent-a", &request_sha, &[])
        .is_err());
    assert!(outbox
        .complete_submission("intent-a", &request_sha, "new-contract", "{}")
        .is_err());
    assert_eq!(outbox.status()?.ambiguous, 1);
    Ok(())
}

#[test]
fn raw_remote_fill_fact_recomputes_its_exact_canonical_evidence_before_commit() -> Result<()> {
    let attacks = [
        ("partial-quantity", 1_usize, 1_400_i64, "4914", None),
        ("wrong-row-count", 2, 1_500, "5265", None),
        ("wrong-notional", 1, 1_500, "1", None),
        ("wrong-contract", 1, 1_500, "5265", Some("other-contract")),
    ];
    for (name, row_count, quantity, notional, evidence_contract) in attacks {
        let directory = tempdir()?;
        let path = directory.path().join(format!("{name}.db"));
        let mut outbox = submitted_outbox_for_remote_fact(&path, "contract-a")?;
        let staged = outbox.staged_intents()?[0].clone();
        let evidence = remote_fill_evidence(evidence_contract.unwrap_or("contract-a"), 1_500);

        assert!(
            outbox
                .record_remote_filled(
                    &staged.intent_id,
                    &staged.request_sha256,
                    "contract-a",
                    "2026-08-19 10:40:00",
                    row_count,
                    quantity,
                    notional,
                    &serde_json::to_string(&evidence)?,
                )
                .is_err(),
            "{name} must be rejected by the durable outbox boundary"
        );
        assert_eq!(
            outbox.staged_intents()?[0].terminal_resolution,
            StagedTerminalResolution::Unresolved,
            "{name}"
        );
    }
    for (name, second_contract, second_fill_id) in [
        ("duplicate-fill-id", "contract-a", "fill-01"),
        ("cross-contract", "other-contract", "fill-02"),
    ] {
        let directory = tempdir()?;
        let path = directory.path().join(format!("{name}.db"));
        let mut outbox = submitted_outbox_for_remote_fact(&path, "contract-a")?;
        let staged = outbox.staged_intents()?[0].clone();
        let mut evidence = remote_fill_evidence("contract-a", 700);
        evidence["fills"]
            .as_array_mut()
            .expect("fill evidence array")
            .push(json!({
                "fill_date": "20260819",
                "fill_time": "103001",
                "symbol": "002256",
                "security_name": "兆新股份",
                "direction": "BUY",
                "quantity": 800,
                "price": "3.51",
                "amount": "2808",
                "contract_id": second_contract,
                "fill_id": second_fill_id,
                "fill_category": "普通成交"
            }));
        assert!(
            outbox
                .record_remote_filled(
                    &staged.intent_id,
                    &staged.request_sha256,
                    "contract-a",
                    "2026-08-19 10:40:00",
                    2,
                    1_500,
                    "5265",
                    &serde_json::to_string(&evidence)?,
                )
                .is_err(),
            "{name} must be rejected by the durable outbox boundary"
        );
        assert_eq!(
            outbox.staged_intents()?[0].terminal_resolution,
            StagedTerminalResolution::Unresolved,
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn identical_remote_fill_fact_is_restart_idempotent_but_changed_evidence_is_rejected() -> Result<()>
{
    let directory = tempdir()?;
    let path = directory.path().join("terminal-fill-idempotency.db");
    let mut outbox = submitted_outbox_for_remote_fact(&path, "contract-a")?;
    let staged = outbox.staged_intents()?[0].clone();
    outbox.begin_cancellation(&staged.intent_id, &staged.request_sha256, "contract-a")?;
    outbox.mark_cancellation_ambiguous(
        &staged.intent_id,
        &staged.request_sha256,
        "contract-a",
        "contract disappeared after cancel was claimed",
        None,
    )?;
    let evidence = serde_json::to_string(&remote_fill_evidence("contract-a", 1_500))?;
    outbox.record_remote_filled(
        &staged.intent_id,
        &staged.request_sha256,
        "contract-a",
        "2026-08-19 10:40:00",
        1,
        1_500,
        "5265",
        &evidence,
    )?;
    assert_eq!(outbox.status()?.cancel_ambiguous, 0);
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    reopened.record_remote_filled(
        &staged.intent_id,
        &staged.request_sha256,
        "contract-a",
        "2026-08-19 10:41:00",
        1,
        1_500,
        "5265",
        &evidence,
    )?;
    let mut changed = remote_fill_evidence("contract-a", 1_500);
    changed["fills"][0]["price"] = json!("3.50");
    changed["fills"][0]["amount"] = json!("5250");
    changed["order"]["fill_price"] = json!("3.50");
    assert!(reopened
        .record_remote_filled(
            &staged.intent_id,
            &staged.request_sha256,
            "contract-a",
            "2026-08-19 10:42:00",
            1,
            1_500,
            "5250",
            &serde_json::to_string(&changed)?,
        )
        .is_err());
    assert_eq!(
        reopened.staged_intents()?[0].terminal_resolution,
        StagedTerminalResolution::Filled
    );
    Ok(())
}

fn submitted_outbox_for_remote_fact(
    path: &std::path::Path,
    contract_id: &str,
) -> Result<ThsSimOutbox> {
    let mut outbox = ThsSimOutbox::open(path)?;
    outbox.bind_or_verify(SOURCE_A, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a", 1_500),
        submitted_event(2, "order-a"),
    ])?;
    let staged = outbox.staged_intents()?[0].clone();
    outbox.begin_submission(&staged.intent_id, &staged.request_sha256, &[])?;
    outbox.complete_submission(
        &staged.intent_id,
        &staged.request_sha256,
        contract_id,
        &serde_json::to_string(&json!({
            "order_date": "20260819",
            "order_time": "104000",
            "symbol": "002256",
            "security_name": "兆新股份",
            "direction": "BUY",
            "remark": "全部成交",
            "quantity": 1500,
            "cancelled_quantity": 0,
            "order_price": "3.55",
            "fill_price": "3.51",
            "contract_id": contract_id,
            "order_attribute": "限价委托",
        }))?,
    )?;
    Ok(outbox)
}

fn remote_fill_evidence(contract_id: &str, quantity: i64) -> serde_json::Value {
    json!({
        "evidence_contract_version": 1,
        "order": {
            "order_date": "20260819",
            "order_time": "104000",
            "symbol": "002256",
            "security_name": "兆新股份",
            "direction": "BUY",
            "remark": "全部成交",
            "quantity": 1500,
            "cancelled_quantity": 0,
            "order_price": "3.55",
            "fill_price": "3.51",
            "contract_id": contract_id,
            "order_attribute": "限价委托"
        },
        "fills": [{
            "fill_date": "20260819",
            "fill_time": "103000",
            "symbol": "002256",
            "security_name": "兆新股份",
            "direction": "BUY",
            "quantity": quantity,
            "price": "3.51",
            "amount": (Decimal::new(351, 2) * Decimal::from(quantity)).to_string(),
            "contract_id": contract_id,
            "fill_id": "fill-01",
            "fill_category": "普通成交"
        }]
    })
}

fn create_physical_legacy_outbox(path: &std::path::Path, schema_version: i64) -> Result<()> {
    let connection = Connection::open(path)?;
    connection.execute_batch(&format!(
        "CREATE TABLE outbox_metadata(
           singleton INTEGER PRIMARY KEY,
           schema_version INTEGER NOT NULL,
           source_database_instance_id TEXT NOT NULL,
           run_id TEXT NOT NULL,
           cursor INTEGER NOT NULL
         );
         INSERT INTO outbox_metadata VALUES(1,{schema_version},'{SOURCE_A}','run-a',42);
         CREATE TABLE staged_intents(
           intent_id TEXT PRIMARY KEY,
           order_id TEXT NOT NULL UNIQUE,
           source_intent_sequence INTEGER NOT NULL UNIQUE,
           source_submitted_sequence INTEGER UNIQUE,
           source_event_time TEXT NOT NULL,
           request_sha256 TEXT NOT NULL,
           request_json TEXT NOT NULL,
           state TEXT NOT NULL,
           baseline_contract_ids_json TEXT,
           remote_contract_id TEXT UNIQUE,
           remote_evidence_json TEXT,
           last_error TEXT
         );"
    ))?;
    if schema_version == 2 {
        connection.execute_batch(
            "ALTER TABLE staged_intents ADD COLUMN cancel_state TEXT NOT NULL DEFAULT 'NOT_REQUESTED';
             ALTER TABLE staged_intents ADD COLUMN cancel_evidence_json TEXT;
             ALTER TABLE staged_intents ADD COLUMN cancel_error TEXT;",
        )?;
    }
    Ok(())
}

fn table_columns(connection: &Connection) -> Result<Vec<String>> {
    Ok(connection
        .prepare("PRAGMA table_info(staged_intents)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn intent_event(sequence: i64, intent_id: &str, order_id: &str, quantity: i64) -> EventEnvelope {
    let time = NaiveDateTime::parse_from_str("2026-08-17 09:35:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let order = Order {
        order_id: order_id.to_owned(),
        intent: OrderIntent {
            intent_id: intent_id.to_owned(),
            origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
            right_id: "right-a".to_owned(),
            grid_index: -1,
            direction: Direction::Buy,
            limit_price: Decimal::new(355, 2),
            quantity,
            budget: Decimal::new(5325, 0),
            target_lot_ids: Vec::new(),
            target_lot_allocations: Vec::new(),
            profit_guard_version: String::new(),
            profit_guard_policy: None,
            created_at: time,
        },
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: Decimal::ZERO,
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    envelope(
        sequence,
        EventType::OrderIntentCreated,
        serde_json::to_value(order).unwrap(),
        time,
    )
}

fn submitted_event(sequence: i64, order_id: &str) -> EventEnvelope {
    let time = NaiveDateTime::parse_from_str("2026-08-17 09:35:00", "%Y-%m-%d %H:%M:%S").unwrap();
    envelope(
        sequence,
        EventType::OrderSubmitted,
        json!({"order_id": order_id}),
        time,
    )
}

fn cancel_requested_event(sequence: i64, order_id: &str, reason: &str) -> EventEnvelope {
    let time = NaiveDateTime::parse_from_str("2026-08-17 09:35:00", "%Y-%m-%d %H:%M:%S").unwrap();
    envelope(
        sequence,
        EventType::OrderCancelRequested,
        json!({"order_id": order_id, "reason": reason}),
        time,
    )
}

fn envelope(
    sequence: i64,
    event_type: EventType,
    payload: serde_json::Value,
    time: NaiveDateTime,
) -> EventEnvelope {
    EventEnvelope {
        event_id: format!("event-{sequence}"),
        event_type,
        schema_version: current_event_schema(event_type),
        run_id: "run-a".to_owned(),
        cycle_id: "cycle-a".to_owned(),
        symbol: "002256.SZ".to_owned(),
        event_time: time,
        recorded_at: time,
        sequence_number: sequence,
        correlation_id: format!("correlation-{sequence}"),
        causation_id: None,
        idempotency_key: format!("key-{sequence}"),
        payload,
        config_version: "test".to_owned(),
    }
}
