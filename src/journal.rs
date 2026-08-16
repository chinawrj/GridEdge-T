use crate::{
    config::Config,
    domain::StrategyState,
    event::{EventEnvelope, EventType},
};
use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::{cell::Cell, path::Path};

type StoredEventIdentity = (
    String,
    String,
    i32,
    String,
    String,
    String,
    String,
    String,
    String,
);
type StoredWebReceiptRow = (
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

const MIGRATION_1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_time TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    idempotency_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    config_version TEXT NOT NULL,
    UNIQUE(run_id, sequence_number),
    UNIQUE(run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_events_run_sequence ON events(run_id, sequence_number);
CREATE TRIGGER IF NOT EXISTS events_are_append_only_update
BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
CREATE TRIGGER IF NOT EXISTS events_are_append_only_delete
BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
CREATE TABLE IF NOT EXISTS snapshots (
    run_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, sequence_number)
);
CREATE TABLE IF NOT EXISTS duplicate_events (
    run_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    duplicate_count INTEGER NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY(run_id, idempotency_key)
);
"#;

const MIGRATION_2: &str = r#"
CREATE INDEX IF NOT EXISTS idx_events_market_run
ON events(run_id, sequence_number)
WHERE event_type='MARKET_DATA_RECEIVED';
"#;

const MIGRATION_3: &str = r#"
CREATE INDEX IF NOT EXISTS idx_events_processed_bar_run
ON events(run_id, sequence_number)
WHERE event_type='MARKET_BAR_PROCESSED';
"#;

const MIGRATION_4: &str = r#"
CREATE TABLE IF NOT EXISTS paper_accounts (
    run_id TEXT PRIMARY KEY,
    snapshot_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_orders (
    run_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, intent_id),
    UNIQUE(run_id, order_id)
);
"#;

const MIGRATION_5: &str = r#"
CREATE TABLE IF NOT EXISTS paper_trade_dates (
    run_id TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"#;

const MIGRATION_6: &str = r#"
CREATE INDEX IF NOT EXISTS idx_events_run_correlation_sequence
ON events(run_id, correlation_id, sequence_number DESC);
"#;

const MIGRATION_7: &str = r#"
CREATE TABLE IF NOT EXISTS web_playback_control (
    run_id TEXT PRIMARY KEY,
    command_version INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0,1)),
    interval_ms INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS web_command_inbox (
    run_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    command TEXT NOT NULL,
    expected_sequence INTEGER NOT NULL,
    expected_version INTEGER NOT NULL,
    accepted_version INTEGER NOT NULL,
    target_processed_bars INTEGER NOT NULL,
    receipt_state TEXT NOT NULL CHECK(receipt_state IN ('PENDING','COMPLETED')),
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id, request_id),
    UNIQUE(request_id)
);
CREATE INDEX IF NOT EXISTS idx_web_command_inbox_pending
ON web_command_inbox(run_id, receipt_state);
"#;

const MIGRATION_8: &str = r#"
ALTER TABLE web_command_inbox ADD COLUMN request_json TEXT;
ALTER TABLE web_command_inbox ADD COLUMN plan_sha256 TEXT;
"#;

const MIGRATION_9: &str = r#"
ALTER TABLE web_command_inbox ADD COLUMN config_sha256 TEXT;
ALTER TABLE web_command_inbox ADD COLUMN algorithm_sha256 TEXT;
"#;

const MIGRATION_10: &str = r#"
CREATE INDEX IF NOT EXISTS idx_events_run_type_sequence
ON events(run_id, event_type, sequence_number);
"#;

const MIGRATION_11: &str = r#"
CREATE INDEX IF NOT EXISTS idx_events_market_identity
ON events(run_id, event_type, symbol, event_time, sequence_number);
"#;

const MIGRATION_12: &str = r#"
DROP INDEX IF EXISTS idx_events_run_sequence;
DROP INDEX IF EXISTS idx_events_market_run;
DROP INDEX IF EXISTS idx_events_processed_bar_run;
"#;

const MIGRATION_13: &str = r#"
CREATE TABLE database_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    instance_id TEXT NOT NULL CHECK(length(instance_id)=32)
);
INSERT INTO database_identity(singleton,instance_id)
VALUES(1,lower(hex(randomblob(16))));
CREATE TRIGGER database_identity_is_immutable_insert
BEFORE INSERT ON database_identity WHEN EXISTS(SELECT 1 FROM database_identity) BEGIN
    SELECT RAISE(ABORT, 'database identity is immutable');
END;
CREATE TRIGGER database_identity_is_immutable_update
BEFORE UPDATE ON database_identity BEGIN
    SELECT RAISE(ABORT, 'database identity is immutable');
END;
CREATE TRIGGER database_identity_is_immutable_delete
BEFORE DELETE ON database_identity BEGIN
    SELECT RAISE(ABORT, 'database identity is immutable');
END;
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended(i64),
    Duplicate,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WebPlaybackControl {
    pub command_version: i64,
    pub active: bool,
    pub interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WebCommandReceipt {
    pub request_sha256: String,
    pub command: String,
    pub expected_sequence: i64,
    pub expected_version: i64,
    pub accepted_version: i64,
    pub target_processed_bars: usize,
    pub completed_response_json: Option<String>,
    pub request_json: Option<String>,
    pub plan_sha256: Option<String>,
    pub config_sha256: Option<String>,
    pub algorithm_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingWebCommand {
    pub request_id: String,
    pub receipt: WebCommandReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WebCommandClaim {
    Existing(WebCommandReceipt),
    Claimed(WebCommandReceipt),
}

pub(crate) fn web_command_plan_sha256(
    request_sha256: &str,
    command: &str,
    expected_sequence: i64,
    expected_version: i64,
    accepted_version: i64,
    target_processed_bars: usize,
    runtime_identity: (&str, &str),
) -> String {
    let (config_sha256, algorithm_sha256) = runtime_identity;
    let plan = serde_json::json!({
        "accepted_version": accepted_version,
        "algorithm_sha256": algorithm_sha256,
        "command": command,
        "config_sha256": config_sha256,
        "expected_sequence": expected_sequence,
        "expected_version": expected_version,
        "request_sha256": request_sha256,
        "target_processed_bars": target_processed_bars,
    });
    hex::encode(Sha256::digest(
        serde_json::to_vec(&plan).expect("command plan JSON is serializable"),
    ))
}

/// Read-only event-ledger port. Domain writes intentionally are not exposed
/// here; callers must use `ledger::LedgerWriter` so projection validation and
/// the sequence compare-and-swap cannot be bypassed.
///
/// ```compile_fail
/// use gridedge_t::journal::SqliteStore;
/// let _raw_append = SqliteStore::append_expected;
/// ```
pub trait EventReader {
    fn load_after(&self, run_id: &str, sequence: i64) -> Result<Vec<EventEnvelope>>;
}

pub trait StateReader {
    fn rebuild(&self, initial: StrategyState) -> Result<(StrategyState, i64)>;
}

pub struct SqliteStore {
    conn: Connection,
    snapshot_fallback_used: Cell<bool>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).context("failed to open SQLite database")?;
        Self::from_connection(conn)
    }

    /// Open an existing database without SQLite's implicit create behavior.
    ///
    /// Long-running Web processes use this after their one startup migration so
    /// a missing or replaced journal can never be silently initialized as a new
    /// empty ledger.
    pub(crate) fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("failed to open existing SQLite database")?;
        Self::from_connection(conn)
    }

    pub(crate) fn open_existing_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("failed to open existing SQLite database read-only")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self {
            conn,
            snapshot_fallback_used: Cell::new(false),
        })
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 1_000)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self {
            conn,
            snapshot_fallback_used: Cell::new(false),
        })
    }

    pub(crate) fn verify_current_schema(&self) -> Result<()> {
        const CURRENT_SCHEMA_VERSION: i64 = 13;
        let (minimum, maximum, count): (i64, i64, i64) = self
            .conn
            .query_row(
                "SELECT COALESCE(MIN(version),0),COALESCE(MAX(version),0),COUNT(*)
                 FROM schema_migrations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("failed to read database schema version")?;
        if minimum != 1 || maximum != CURRENT_SCHEMA_VERSION || count != CURRENT_SCHEMA_VERSION {
            bail!("Web database schema is not current: min={minimum}, max={maximum}, count={count}")
        }
        Ok(())
    }

    pub(crate) fn database_instance_id(&self) -> Result<String> {
        let mut statement = self
            .conn
            .prepare("SELECT instance_id FROM database_identity WHERE singleton=1")?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let [instance_id] = ids.as_slice() else {
            bail!("database identity must contain exactly one singleton row")
        };
        if instance_id.len() != 32
            || !instance_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("database identity is not canonical lowercase hex")
        }
        Ok(instance_id.clone())
    }

    pub fn migrate(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);",
        )?;
        let (current, count): (i64, i64) = self.conn.query_row(
            "SELECT COALESCE(MAX(version),0),COUNT(*) FROM schema_migrations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        const MIGRATIONS: &[(i64, &str)] = &[
            (1, MIGRATION_1),
            (2, MIGRATION_2),
            (3, MIGRATION_3),
            (4, MIGRATION_4),
            (5, MIGRATION_5),
            (6, MIGRATION_6),
            (7, MIGRATION_7),
            (8, MIGRATION_8),
            (9, MIGRATION_9),
            (10, MIGRATION_10),
            (11, MIGRATION_11),
            (12, MIGRATION_12),
            (13, MIGRATION_13),
        ];
        let latest = MIGRATIONS.last().map(|entry| entry.0).unwrap_or(0);
        if current != count || current > latest {
            return Err(anyhow!(
                "unsupported or non-contiguous database schema version {current}"
            ));
        }
        for (version, sql) in MIGRATIONS
            .iter()
            .copied()
            .filter(|(version, _)| *version > current)
        {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES(?1, ?2)",
                params![version, chrono::Utc::now().naive_utc().to_string()],
            )?;
            tx.commit()?;
        }
        self.conn.execute_batch("PRAGMA optimize")?;
        Ok(())
    }

    pub fn integrity_check(&self) -> Result<()> {
        let result: String = self
            .conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(anyhow!("SQLite integrity check failed: {result}"));
        }
        Ok(())
    }

    /// Run a group of read-only projections against one SQLite snapshot.
    ///
    /// Web readers use this to keep the journal head, aggregate projection,
    /// replay progress and control metadata on the same committed prefix while
    /// a WAL writer may continue appending from another connection.
    pub(crate) fn with_consistent_read<T>(
        &mut self,
        operation: impl FnOnce(&Self) -> Result<T>,
    ) -> Result<T> {
        self.conn.execute_batch("BEGIN DEFERRED TRANSACTION")?;
        match operation(self) {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn duplicate_count(&self, run_id: &str) -> Result<u64> {
        let count: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(duplicate_count), 0) FROM duplicate_events WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    pub fn run_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT run_id FROM events ORDER BY recorded_at DESC")?;
        let rows = stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn event_count_by_type(&self, run_id: &str, event_type: EventType) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM events WHERE run_id=?1 AND event_type=?2",
            params![run_id, event_type.to_string()],
            |row| row.get(0),
        )?;
        usize::try_from(count).context("event count is out of range")
    }

    pub(crate) fn event_type_summary(
        &self,
        run_id: &str,
        event_type: EventType,
    ) -> Result<(usize, Option<i64>)> {
        let (count, maximum_sequence): (i64, Option<i64>) = self.conn.query_row(
            "SELECT COUNT(*),MAX(sequence_number)
             FROM events WHERE run_id=?1 AND event_type=?2",
            params![run_id, event_type.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((
            usize::try_from(count).context("event count is out of range")?,
            maximum_sequence,
        ))
    }

    /// Load one bounded page of completed mechanical-opportunity anchors.
    ///
    /// A touch and its grant/skip resolution are committed before the market
    /// workflow records `MARKET_BAR_PROCESSED`.  Public readers therefore must
    /// prove the matching processed fact exists inside the caller's frozen
    /// prefix rather than exposing an internally committed partial bar.
    pub(crate) fn completed_opportunity_anchors(
        &self,
        run_id: &str,
        after_touch_sequence: i64,
        through_sequence: i64,
        limit: usize,
    ) -> Result<Vec<(EventEnvelope, i64)>> {
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            "SELECT touched.sequence_number,
                    MIN(processed.sequence_number) AS processed_sequence
             FROM events AS touched
             JOIN events AS processed
               ON processed.run_id=touched.run_id
              AND processed.event_type=?2
              AND processed.symbol=touched.symbol
              AND processed.event_time=touched.event_time
              AND processed.sequence_number>touched.sequence_number
              AND processed.sequence_number<=?5
             WHERE touched.run_id=?1
               AND touched.event_type=?3
               AND touched.sequence_number>?4
               AND touched.sequence_number<=?5
             GROUP BY touched.sequence_number
             ORDER BY touched.sequence_number
             LIMIT ?6",
        )?;
        let anchors = stmt
            .query_map(
                params![
                    run_id,
                    EventType::MarketBarProcessed.to_string(),
                    EventType::GridLevelTouched.to_string(),
                    after_touch_sequence,
                    through_sequence,
                    sql_limit
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        anchors
            .into_iter()
            .map(|(touch_sequence, processed_sequence)| {
                let touch = self
                    .load_after_limited(run_id, touch_sequence - 1, 1)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("opportunity touch disappeared from its read prefix"))?;
                if touch.sequence_number != touch_sequence
                    || touch.event_type != EventType::GridLevelTouched
                {
                    bail!("opportunity anchor does not resolve to its touch event")
                }
                Ok((touch, processed_sequence))
            })
            .collect()
    }

    pub(crate) fn opportunity_events_for_touch(
        &self,
        run_id: &str,
        touch: &EventEnvelope,
        through_sequence: i64,
    ) -> Result<Vec<EventEnvelope>> {
        let sql = if touch.schema_version >= 2 {
            "SELECT sequence_number FROM events
             WHERE run_id=?1 AND correlation_id=?2 AND sequence_number<=?3
             ORDER BY sequence_number"
        } else {
            "SELECT sequence_number FROM events
             WHERE run_id=?1 AND sequence_number>=?2 AND sequence_number<=?3
             ORDER BY sequence_number"
        };
        let mut stmt = self.conn.prepare(sql)?;
        let sequences = if touch.schema_version >= 2 {
            stmt.query_map(
                params![run_id, touch.correlation_id, through_sequence],
                |row| row.get::<_, i64>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            stmt.query_map(
                params![run_id, touch.sequence_number, through_sequence],
                |row| row.get::<_, i64>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        sequences
            .into_iter()
            .map(|sequence| {
                let event = self
                    .load_after_limited(run_id, sequence - 1, 1)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("opportunity event disappeared from its read prefix"))?;
                if event.sequence_number != sequence {
                    bail!("opportunity correlation resolved to a different event")
                }
                Ok(event)
            })
            .collect()
    }

    pub(crate) fn completed_opportunity_counts(
        &self,
        run_id: &str,
        through_sequence: i64,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        let mut touch_stmt = self.conn.prepare(
            "SELECT touched.schema_version,touched.symbol,touched.event_time,
                    touched.correlation_id,json_extract(touched.payload,'$.grid_index')
             FROM events AS touched
             WHERE touched.run_id=?1
               AND touched.event_type=?2
               AND touched.sequence_number<=?3
               AND EXISTS (
                 SELECT 1 FROM events AS processed
                 WHERE processed.run_id=touched.run_id
                   AND processed.event_type=?4
                   AND processed.symbol=touched.symbol
                   AND processed.event_time=touched.event_time
                   AND processed.sequence_number>touched.sequence_number
                   AND processed.sequence_number<=?3
               )",
        )?;
        let touches = touch_stmt
            .query_map(
                params![
                    run_id,
                    EventType::GridLevelTouched.to_string(),
                    through_sequence,
                    EventType::MarketBarProcessed.to_string()
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut resolution_stmt = self.conn.prepare(
            "SELECT event_type,symbol,event_time,correlation_id,
                    json_extract(payload,'$.grid_index')
             FROM events
             WHERE run_id=?1
               AND event_type IN (?2,?3)
               AND sequence_number<=?4",
        )?;
        let resolutions = resolution_stmt
            .query_map(
                params![
                    run_id,
                    EventType::GridRightGranted.to_string(),
                    EventType::GridLevelSkipped.to_string(),
                    through_sequence
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut resolution_counts = std::collections::HashMap::<String, (usize, usize)>::new();
        for (event_type, symbol, event_time, correlation_id, grid_index) in resolutions {
            let is_grant = event_type == EventType::GridRightGranted.to_string();
            for key in [
                format!("current:{correlation_id}"),
                format!("legacy:{symbol}:{event_time}:{grid_index}"),
            ] {
                let counts = resolution_counts.entry(key).or_default();
                if is_grant {
                    counts.0 += 1;
                } else {
                    counts.1 += 1;
                }
            }
        }
        let mut granted = 0;
        let mut skipped = 0;
        let mut legacy_unbound = 0;
        let mut invalid_current = 0;
        for (schema_version, symbol, event_time, correlation_id, grid_index) in &touches {
            let key = if *schema_version >= 2 {
                format!("current:{correlation_id}")
            } else {
                format!("legacy:{symbol}:{event_time}:{grid_index}")
            };
            match resolution_counts.get(&key).copied().unwrap_or_default() {
                (1, 0) => granted += 1,
                (0, 1) => skipped += 1,
                _ if *schema_version < 2 => legacy_unbound += 1,
                _ => invalid_current += 1,
            }
        }
        Ok((
            touches.len(),
            granted,
            skipped,
            legacy_unbound,
            invalid_current,
        ))
    }

    pub fn load_after_limited(
        &self,
        run_id: &str,
        sequence: i64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            "SELECT event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,recorded_at,sequence_number,correlation_id,causation_id,idempotency_key,payload,config_version
             FROM events WHERE run_id=?1 AND sequence_number>?2 ORDER BY sequence_number LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![run_id, sequence, sql_limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i32>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, String>(13)?,
            ))
        })?;
        rows.map(|row| {
            let r = row?;
            Ok(EventEnvelope {
                event_id: r.0,
                event_type: r.1.parse()?,
                schema_version: r.2,
                run_id: r.3,
                cycle_id: r.4,
                symbol: r.5,
                event_time: chrono::NaiveDateTime::parse_from_str(&r.6, "%Y-%m-%d %H:%M:%S%.f")?,
                recorded_at: chrono::NaiveDateTime::parse_from_str(&r.7, "%Y-%m-%d %H:%M:%S%.f")?,
                sequence_number: r.8,
                correlation_id: r.9,
                causation_id: r.10,
                idempotency_key: r.11,
                payload: serde_json::from_str(&r.12)?,
                config_version: r.13,
            })
        })
        .collect()
    }

    pub(crate) fn latest_sequence(&self, run_id: &str) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT COALESCE(MAX(sequence_number),0) FROM events WHERE run_id=?1",
                [run_id],
                |row| row.get(0),
            )
            .context("failed to read the current journal sequence")
    }

    pub(crate) fn web_playback_control(&self, run_id: &str) -> Result<WebPlaybackControl> {
        let stored: Option<(i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT command_version,active,interval_ms FROM web_playback_control WHERE run_id=?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((command_version, active, interval_ms)) = stored else {
            return Ok(WebPlaybackControl::default());
        };
        Ok(WebPlaybackControl {
            command_version,
            active: active != 0,
            interval_ms: u64::try_from(interval_ms).context("invalid stored playback interval")?,
        })
    }

    pub(crate) fn web_command_receipt(
        &self,
        run_id: &str,
        request_id: &str,
    ) -> Result<Option<WebCommandReceipt>> {
        let stored: Option<StoredWebReceiptRow> = self
            .conn
            .query_row(
                "SELECT request_sha256,command,expected_sequence,expected_version,accepted_version,
                        target_processed_bars,receipt_state,response_json,request_json,plan_sha256,
                        config_sha256,algorithm_sha256
                 FROM web_command_inbox WHERE run_id=?1 AND request_id=?2",
                params![run_id, request_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                    ))
                },
            )
            .optional()?;
        stored
            .map(
                |(
                    request_sha256,
                    command,
                    expected_sequence,
                    expected_version,
                    accepted_version,
                    target_processed_bars,
                    receipt_state,
                    response_json,
                    request_json,
                    plan_sha256,
                    config_sha256,
                    algorithm_sha256,
                )| {
                    let completed_response_json = match receipt_state.as_str() {
                        "PENDING" => None,
                        "COMPLETED" => Some(
                            response_json.context("completed Web command lacks its response")?,
                        ),
                        _ => bail!("invalid Web command receipt state"),
                    };
                    Ok(WebCommandReceipt {
                        request_sha256,
                        command,
                        expected_sequence,
                        expected_version,
                        accepted_version,
                        target_processed_bars: usize::try_from(target_processed_bars)
                            .context("invalid Web command target cursor")?,
                        completed_response_json,
                        request_json,
                        plan_sha256,
                        config_sha256,
                        algorithm_sha256,
                    })
                },
            )
            .transpose()
    }

    pub(crate) fn pending_web_command(&self, run_id: &str) -> Result<Option<PendingWebCommand>> {
        let request_id: Option<String> = self
            .conn
            .query_row(
                "SELECT request_id FROM web_command_inbox
                 WHERE run_id=?1 AND receipt_state='PENDING'
                 ORDER BY created_at LIMIT 1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(request_id) = request_id else {
            return Ok(None);
        };
        let receipt = self
            .web_command_receipt(run_id, &request_id)?
            .context("pending Web command disappeared")?;
        Ok(Some(PendingWebCommand {
            request_id,
            receipt,
        }))
    }

    pub(crate) fn pending_web_commands(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, PendingWebCommand)>> {
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let identities = {
            let mut statement = self.conn.prepare(
                "SELECT run_id,request_id FROM web_command_inbox
                 WHERE receipt_state='PENDING' ORDER BY created_at,run_id LIMIT ?1",
            )?;
            let rows = statement
                .query_map([sql_limit], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        identities
            .into_iter()
            .map(|(run_id, request_id)| {
                let receipt = self
                    .web_command_receipt(&run_id, &request_id)?
                    .context("pending Web command disappeared")?;
                Ok((
                    run_id,
                    PendingWebCommand {
                        request_id,
                        receipt,
                    },
                ))
            })
            .collect()
    }

    pub(crate) fn completed_playback_generation(&self, run_id: &str, version: i64) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM web_command_inbox
                    WHERE run_id=?1 AND command='play' AND accepted_version=?2
                      AND receipt_state='COMPLETED'
                 )",
                params![run_id, version],
                |row| row.get(0),
            )
            .context("failed to verify the completed PLAY generation")
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn claim_web_command(
        &mut self,
        run_id: &str,
        request_id: &str,
        request_sha256: &str,
        command: &str,
        expected_sequence: i64,
        expected_version: i64,
        target_processed_bars: usize,
        require_exact_sequence: bool,
        require_inactive: bool,
        require_active: bool,
        next_active: bool,
        interval_ms: u64,
        request_json: &str,
        config_sha256: &str,
        algorithm_sha256: &str,
    ) -> Result<WebCommandClaim> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<StoredWebReceiptRow> = tx
            .query_row(
                "SELECT request_sha256,command,expected_sequence,expected_version,accepted_version,
                        target_processed_bars,receipt_state,response_json,request_json,plan_sha256,
                        config_sha256,algorithm_sha256
                 FROM web_command_inbox WHERE run_id=?1 AND request_id=?2",
                params![run_id, request_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                    ))
                },
            )
            .optional()?;
        if let Some((
            stored_hash,
            stored_command,
            stored_sequence,
            stored_version,
            accepted_version,
            stored_target,
            receipt_state,
            response_json,
            stored_request_json,
            stored_plan_sha256,
            stored_config_sha256,
            stored_algorithm_sha256,
        )) = existing
        {
            if stored_hash != request_sha256
                || stored_command != command
                || stored_sequence != expected_sequence
                || stored_version != expected_version
            {
                bail!("request_id was reused with different command content")
            }
            let receipt = WebCommandReceipt {
                request_sha256: stored_hash,
                command: stored_command,
                expected_sequence: stored_sequence,
                expected_version: stored_version,
                accepted_version,
                target_processed_bars: usize::try_from(stored_target)
                    .context("invalid Web command target cursor")?,
                completed_response_json: match receipt_state.as_str() {
                    "PENDING" => None,
                    "COMPLETED" => {
                        Some(response_json.context("completed Web command lacks its response")?)
                    }
                    _ => bail!("invalid Web command receipt state"),
                },
                request_json: stored_request_json,
                plan_sha256: stored_plan_sha256,
                config_sha256: stored_config_sha256,
                algorithm_sha256: stored_algorithm_sha256,
            };
            tx.commit()?;
            return Ok(WebCommandClaim::Existing(receipt));
        }
        let another_pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM web_command_inbox WHERE run_id=?1 AND receipt_state='PENDING')",
            [run_id],
            |row| row.get(0),
        )?;
        if another_pending {
            bail!("another command for this run is awaiting recovery")
        }
        let current_sequence: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sequence_number),0) FROM events WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        if require_exact_sequence && current_sequence != expected_sequence {
            bail!(
                "stale command sequence: expected {expected_sequence}, current {current_sequence}"
            )
        }
        let (current_version, current_active): (i64, bool) = tx
            .query_row(
                "SELECT command_version,active FROM web_playback_control WHERE run_id=?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?
            .unwrap_or((0, false));
        if current_version != expected_version {
            bail!("stale command version: expected {expected_version}, current {current_version}")
        }
        if require_inactive && current_active {
            bail!("automatic playback is active for this run")
        }
        if require_active && !current_active {
            bail!("automatic playback is not active for this run")
        }
        let accepted_version = current_version
            .checked_add(1)
            .context("Web command version is exhausted")?;
        let plan_sha256 = web_command_plan_sha256(
            request_sha256,
            command,
            expected_sequence,
            expected_version,
            accepted_version,
            target_processed_bars,
            (config_sha256, algorithm_sha256),
        );
        let now = chrono::Utc::now().naive_utc().to_string();
        tx.execute(
            "INSERT INTO web_command_inbox(
                run_id,request_id,request_sha256,command,expected_sequence,expected_version,
                accepted_version,target_processed_bars,receipt_state,response_json,created_at,updated_at,
                request_json,plan_sha256,config_sha256,algorithm_sha256
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'PENDING',NULL,?9,?9,?10,?11,?12,?13)",
            params![
                run_id,
                request_id,
                request_sha256,
                command,
                expected_sequence,
                expected_version,
                accepted_version,
                i64::try_from(target_processed_bars).context("Web command target is too large")?,
                now,
                request_json,
                plan_sha256,
                config_sha256,
                algorithm_sha256,
            ],
        )?;
        tx.execute(
            "INSERT INTO web_playback_control(run_id,command_version,active,interval_ms,updated_at)
             VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(run_id) DO UPDATE SET
               command_version=excluded.command_version,
               active=excluded.active,
               interval_ms=excluded.interval_ms,
               updated_at=excluded.updated_at",
            params![
                run_id,
                accepted_version,
                i64::from(next_active),
                i64::try_from(interval_ms).context("playback interval is too large")?,
                now,
            ],
        )?;
        tx.commit()?;
        Ok(WebCommandClaim::Claimed(WebCommandReceipt {
            request_sha256: request_sha256.to_owned(),
            command: command.to_owned(),
            expected_sequence,
            expected_version,
            accepted_version,
            target_processed_bars,
            completed_response_json: None,
            request_json: Some(request_json.to_owned()),
            plan_sha256: Some(plan_sha256),
            config_sha256: Some(config_sha256.to_owned()),
            algorithm_sha256: Some(algorithm_sha256.to_owned()),
        }))
    }

    pub(crate) fn complete_web_command(
        &mut self,
        run_id: &str,
        request_id: &str,
        request_sha256: &str,
        response_json: &str,
    ) -> Result<String> {
        let changed = self.conn.execute(
            "UPDATE web_command_inbox
             SET receipt_state='COMPLETED',response_json=?4,updated_at=?5
             WHERE run_id=?1 AND request_id=?2 AND request_sha256=?3 AND receipt_state='PENDING'",
            params![
                run_id,
                request_id,
                request_sha256,
                response_json,
                chrono::Utc::now().naive_utc().to_string(),
            ],
        )?;
        if changed == 1 {
            return Ok(response_json.to_owned());
        }
        let completed: Option<(String, String, Option<String>)> = self
            .conn
            .query_row(
                "SELECT request_sha256,receipt_state,response_json
                 FROM web_command_inbox WHERE run_id=?1 AND request_id=?2",
                params![run_id, request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((_, _, Some(stored_response))) = completed
            .as_ref()
            .filter(|(stored_hash, state, _)| stored_hash == request_sha256 && state == "COMPLETED")
        {
            return Ok(stored_response.clone());
        }
        bail!("Web command receipt could not be completed")
    }

    pub(crate) fn deactivate_web_playback_if_version(
        &mut self,
        run_id: &str,
        version: i64,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE web_playback_control SET active=0,updated_at=?3
             WHERE run_id=?1 AND command_version=?2",
            params![run_id, version, chrono::Utc::now().naive_utc().to_string()],
        )?;
        Ok(())
    }

    pub fn first_payload_by_type(
        &self,
        run_id: &str,
        event_type: EventType,
    ) -> Result<Option<serde_json::Value>> {
        let payload: Option<String> = self
            .conn
            .query_row(
                "SELECT payload FROM events WHERE run_id=?1 AND event_type=?2 ORDER BY sequence_number LIMIT 1",
                params![run_id, event_type.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        payload
            .map(|payload| serde_json::from_str(&payload).context("invalid event payload JSON"))
            .transpose()
    }

    pub fn first_event_by_type(
        &self,
        run_id: &str,
        event_type: EventType,
    ) -> Result<Option<EventEnvelope>> {
        let sequence: Option<i64> = self
            .conn
            .query_row(
                "SELECT sequence_number FROM events
                 WHERE run_id=?1 AND event_type=?2
                 ORDER BY sequence_number LIMIT 1",
                params![run_id, event_type.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        Ok(self
            .load_after_limited(run_id, sequence - 1, 1)?
            .into_iter()
            .next())
    }

    pub fn has_idempotency_key(&self, run_id: &str, key: &str) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE run_id=?1 AND idempotency_key=?2)",
                params![run_id, key],
                |row| row.get(0),
            )
            .context("failed to check idempotency key")
    }

    pub fn payload_by_idempotency_key(
        &self,
        run_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        let payload: Option<String> = self
            .conn
            .query_row(
                "SELECT payload FROM events WHERE run_id=?1 AND idempotency_key=?2",
                params![run_id, key],
                |row| row.get(0),
            )
            .optional()?;
        payload
            .map(|payload| serde_json::from_str(&payload).context("invalid event payload JSON"))
            .transpose()
    }

    pub fn correlation_by_idempotency_key(
        &self,
        run_id: &str,
        key: &str,
    ) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT correlation_id FROM events WHERE run_id=?1 AND idempotency_key=?2",
                params![run_id, key],
                |row| row.get(0),
            )
            .optional()
            .context("failed to resolve durable workflow correlation")
    }

    pub fn latest_event_id_for_correlation(
        &self,
        run_id: &str,
        correlation_id: &str,
    ) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT event_id FROM events WHERE run_id=?1 AND correlation_id=?2 ORDER BY sequence_number DESC LIMIT 1",
                params![run_id, correlation_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed to resolve event causation")
    }

    pub fn recent_payloads_by_type(
        &self,
        run_id: &str,
        event_type: EventType,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let limit = i64::try_from(limit).context("payload limit is out of range")?;
        let mut stmt = self.conn.prepare(
            "SELECT payload FROM events WHERE run_id=?1 AND event_type=?2 ORDER BY sequence_number DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![run_id, event_type.to_string(), limit], |row| {
            row.get::<_, String>(0)
        })?;
        let mut payloads = rows
            .map(|row| {
                let payload = row?;
                serde_json::from_str(&payload).context("invalid event payload JSON")
            })
            .collect::<Result<Vec<_>>>()?;
        payloads.reverse();
        Ok(payloads)
    }

    pub(crate) fn has_grid_touch(
        &self,
        run_id: &str,
        symbol: &str,
        event_time: chrono::NaiveDateTime,
        grid_index: i32,
    ) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM events
                    WHERE run_id=?1 AND event_type=?2
                      AND symbol=?3 AND event_time=?4
                      AND CAST(json_extract(payload, '$.grid_index') AS INTEGER)=?5
                )",
                params![
                    run_id,
                    EventType::GridLevelTouched.to_string(),
                    symbol,
                    event_time.to_string(),
                    grid_index
                ],
                |row| row.get(0),
            )
            .context("failed to check grid-touch opportunity uniqueness")
    }

    pub(crate) fn has_market_stage(
        &self,
        run_id: &str,
        event_type: EventType,
        symbol: &str,
        event_time: chrono::NaiveDateTime,
    ) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM events
                    WHERE run_id=?1 AND event_type=?2 AND symbol=?3 AND event_time=?4
                )",
                params![
                    run_id,
                    event_type.to_string(),
                    symbol,
                    event_time.to_string()
                ],
                |row| row.get(0),
            )
            .context("failed to check durable market stage")
    }

    pub(crate) fn market_payload_at(
        &self,
        run_id: &str,
        symbol: &str,
        event_time: chrono::NaiveDateTime,
    ) -> Result<Option<serde_json::Value>> {
        let payload: Option<String> = self
            .conn
            .query_row(
                "SELECT payload FROM events
                 WHERE run_id=?1 AND event_type=?2 AND symbol=?3 AND event_time=?4
                 ORDER BY sequence_number DESC LIMIT 1",
                params![
                    run_id,
                    EventType::MarketDataReceived.to_string(),
                    symbol,
                    event_time.to_string()
                ],
                |row| row.get(0),
            )
            .optional()?;
        payload
            .map(|payload| serde_json::from_str(&payload).context("invalid market payload JSON"))
            .transpose()
    }

    pub(crate) fn latest_sell_fill_time(
        &self,
        run_id: &str,
    ) -> Result<Option<chrono::NaiveDateTime>> {
        let value: Option<String> = self
            .conn
            .query_row(
                "SELECT event_time FROM events
                 WHERE run_id=?1 AND event_type IN (?2,?3)
                   AND json_extract(payload, '$.fill.direction')='SELL'
                 ORDER BY sequence_number DESC LIMIT 1",
                params![
                    run_id,
                    EventType::OrderPartiallyFilled.to_string(),
                    EventType::OrderFilled.to_string()
                ],
                |row| row.get(0),
            )
            .optional()?;
        value
            .map(|value| {
                chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                    .context("invalid sell-fill event time")
            })
            .transpose()
    }

    pub(crate) fn latest_grid_rearm_time(
        &self,
        run_id: &str,
        symbol: &str,
        grid_index: i32,
    ) -> Result<Option<chrono::NaiveDateTime>> {
        let value: Option<String> = self
            .conn
            .query_row(
                "SELECT event_time FROM events
                 WHERE run_id=?1 AND event_type=?2 AND symbol=?3
                   AND CAST(json_extract(payload, '$.grid_index') AS INTEGER)=?4
                 ORDER BY sequence_number DESC LIMIT 1",
                params![
                    run_id,
                    EventType::GridLevelRearmed.to_string(),
                    symbol,
                    grid_index
                ],
                |row| row.get(0),
            )
            .optional()?;
        value
            .map(|value| {
                chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                    .context("invalid grid rearm event time")
            })
            .transpose()
    }

    pub fn snapshot_fallback_used(&self) -> bool {
        self.snapshot_fallback_used.get()
    }

    pub(crate) fn append_expected(
        &mut self,
        event: &mut EventEnvelope,
        expected_last_sequence: i64,
    ) -> Result<AppendOutcome> {
        let mut outcomes =
            self.append_batch_expected(std::slice::from_mut(event), expected_last_sequence)?;
        Ok(outcomes.remove(0))
    }

    pub(crate) fn append_batch_expected(
        &mut self,
        events: &mut [EventEnvelope],
        expected_last_sequence: i64,
    ) -> Result<Vec<AppendOutcome>> {
        self.append_batch_inner(events, Some(expected_last_sequence))
    }

    pub fn rebuild_full(&self, initial: StrategyState) -> Result<(StrategyState, i64)> {
        self.integrity_check()?;
        let run_id = initial.run_id.clone();
        let context = crate::run_context::RunContext::load(self, &run_id)?;
        let mut state = context
            .as_ref()
            .map_or(initial, crate::run_context::RunContext::initial_state);
        let mut last = 0;
        for event in self.load_after(&run_id, 0)? {
            if event.sequence_number != last + 1 {
                return Err(anyhow!("event sequence gap after {last}"));
            }
            state.apply_event(&event)?;
            last = event.sequence_number;
        }
        self.normalize_state_for_run(&mut state, &run_id)?;
        state.duplicate_events = self.duplicate_count(&run_id)?;
        state
            .validate_invariants()
            .context("full journal rebuild violates aggregate invariants")?;
        Ok((state, last))
    }

    /// Return the first market observation that does not yet have its durable
    /// public-bar completion fact.
    ///
    /// A bar is an application workflow spanning multiple journal commits. The
    /// journal must retain those internal commits for exact recovery, while a
    /// public snapshot must stop immediately before an unfinished bar.
    pub(crate) fn first_incomplete_market_sequence(&self, run_id: &str) -> Result<Option<i64>> {
        self.conn
            .query_row(
                "SELECT MIN(received.sequence_number)
                 FROM events AS received
                 WHERE received.run_id=?1 AND received.event_type=?2
                   AND NOT EXISTS (
                     SELECT 1 FROM events AS processed
                     WHERE processed.run_id=received.run_id
                       AND processed.event_type=?3
                       AND processed.symbol=received.symbol
                       AND processed.event_time=received.event_time
                   )",
                params![
                    run_id,
                    EventType::MarketDataReceived.to_string(),
                    EventType::MarketBarProcessed.to_string()
                ],
                |row| row.get(0),
            )
            .context("failed to locate an incomplete market bar")
    }

    /// Rebuild exactly through a journal sequence without consulting a newer
    /// saved snapshot.
    ///
    /// This deliberately takes the uncommon full-prefix path only while a bar
    /// is incomplete. It prevents any internal fact from that bar from leaking
    /// through the public snapshot even when no earlier snapshot was saved.
    pub(crate) fn rebuild_through_sequence(
        &self,
        initial: StrategyState,
        through_sequence: i64,
    ) -> Result<(StrategyState, i64)> {
        if through_sequence < 0 {
            return Err(anyhow!("journal prefix sequence cannot be negative"));
        }
        self.integrity_check()?;
        let run_id = initial.run_id.clone();
        let context = crate::run_context::RunContext::load(self, &run_id)?;
        let mut state = context
            .as_ref()
            .map_or(initial, crate::run_context::RunContext::initial_state);
        let mut last = 0;
        for event in self.load_after(&run_id, 0)? {
            if event.sequence_number > through_sequence {
                break;
            }
            if event.sequence_number != last + 1 {
                return Err(anyhow!("event sequence gap after {last}"));
            }
            state.apply_event(&event)?;
            last = event.sequence_number;
        }
        if last != through_sequence {
            return Err(anyhow!(
                "journal prefix ended at {last}; expected {through_sequence}"
            ));
        }
        self.normalize_state_for_run(&mut state, &run_id)?;
        state.duplicate_events = self.duplicate_count(&run_id)?;
        state
            .validate_invariants()
            .context("completed public-prefix rebuild violates aggregate invariants")?;
        Ok((state, last))
    }

    pub(crate) fn save_snapshot_validated(
        &mut self,
        state: &StrategyState,
        sequence: i64,
    ) -> Result<()> {
        let json = serde_json::to_string(state)?;
        let hash = checksum(json.as_bytes());
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sequence_number),0) FROM events WHERE run_id=?1",
            [&state.run_id],
            |row| row.get(0),
        )?;
        if current != sequence {
            return Err(anyhow!(
                "refusing stale snapshot at sequence {sequence}; journal is at {current}"
            ));
        }
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO snapshots(run_id,sequence_number,state_json,checksum,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![state.run_id, sequence, json, hash, chrono::Utc::now().naive_utc().to_string()],
        )?;
        if inserted == 0 {
            let existing: String = tx.query_row(
                "SELECT checksum FROM snapshots WHERE run_id=?1 AND sequence_number=?2",
                params![state.run_id, sequence],
                |row| row.get(0),
            )?;
            if existing != hash {
                return Err(anyhow!(
                    "immutable snapshot conflict at sequence {sequence}"
                ));
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn latest_snapshot(&self, run_id: &str) -> Result<Option<(i64, StrategyState)>> {
        self.snapshot_fallback_used.set(false);
        let journal_sequence: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(sequence_number),0) FROM events WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        let mut stmt = self.conn.prepare(
            "SELECT sequence_number,state_json,checksum FROM snapshots WHERE run_id=?1 ORDER BY sequence_number DESC",
        )?;
        let rows = stmt.query_map([run_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut saw_invalid = false;
        let mut saw_snapshot = false;
        for row in rows {
            saw_snapshot = true;
            let (sequence, json, expected) = row?;
            if sequence < 0 || sequence > journal_sequence || checksum(json.as_bytes()) != expected
            {
                saw_invalid = true;
                continue;
            }
            let mut state: StrategyState = match serde_json::from_str(&json) {
                Ok(state) => state,
                Err(_) => {
                    saw_invalid = true;
                    continue;
                }
            };
            if state.run_id != run_id {
                saw_invalid = true;
                continue;
            }
            self.normalize_state_for_run(&mut state, run_id)?;
            self.snapshot_fallback_used.set(saw_invalid);
            return Ok(Some((sequence, state)));
        }
        // Snapshots are acceleration artifacts, never the source of truth. If
        // every stored snapshot is corrupt, rebuild from the append-only
        // journal and let recovery policy decide whether writes remain safe.
        // Treating this as a hard read failure would make a disposable cache
        // more authoritative than the journal it was derived from.
        if saw_snapshot {
            self.snapshot_fallback_used.set(true);
        }
        Ok(None)
    }

    fn append_batch_inner(
        &mut self,
        events: &mut [EventEnvelope],
        expected_last_sequence: Option<i64>,
    ) -> Result<Vec<AppendOutcome>> {
        let batch_run_id = events
            .first()
            .context("cannot append an empty event batch")?
            .run_id
            .clone();
        if events.iter().any(|event| event.run_id != batch_run_id) {
            return Err(anyhow!("event batch cannot span multiple runs"));
        }
        let batch_symbol = events[0].symbol.clone();
        if events.iter().any(|event| event.symbol != batch_symbol) {
            return Err(anyhow!("event batch cannot span multiple symbols"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(expected) = expected_last_sequence {
            let current: i64 = tx.query_row(
                "SELECT COALESCE(MAX(sequence_number),0) FROM events WHERE run_id=?1",
                [&batch_run_id],
                |row| row.get(0),
            )?;
            if current != expected {
                return Err(anyhow!(
                    "stale run writer: expected sequence {expected}, current sequence {current}"
                ));
            }
        }
        let mut outcomes = Vec::with_capacity(events.len());
        for event in events {
            let existing: Option<StoredEventIdentity> = tx
                .query_row(
                    "SELECT event_id,event_type,schema_version,cycle_id,symbol,event_time,correlation_id,payload,config_version
                     FROM events WHERE run_id=?1 AND idempotency_key=?2",
                    params![event.run_id, event.idempotency_key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?)),
                )
                .optional()?;
            if let Some((
                event_id,
                event_type,
                schema_version,
                cycle_id,
                symbol,
                event_time,
                correlation_id,
                payload,
                config_version,
            )) = existing
            {
                let existing_payload: serde_json::Value = serde_json::from_str(&payload)?;
                if event_id != event.event_id
                    || event_type != event.event_type.to_string()
                    || schema_version != event.schema_version
                    || cycle_id != event.cycle_id
                    || symbol != event.symbol
                    || event_time != event.event_time.to_string()
                    || correlation_id != event.correlation_id
                    || existing_payload != event.payload
                    || config_version != event.config_version
                {
                    return Err(anyhow!(
                        "idempotency conflict for run {} key {}",
                        event.run_id,
                        event.idempotency_key
                    ));
                }
                tx.execute(
                    "INSERT INTO duplicate_events(run_id,idempotency_key,duplicate_count,last_seen_at) VALUES(?1,?2,1,?3)
                     ON CONFLICT(run_id,idempotency_key) DO UPDATE SET duplicate_count=duplicate_count+1,last_seen_at=excluded.last_seen_at",
                    params![event.run_id, event.idempotency_key, chrono::Utc::now().naive_utc().to_string()],
                )?;
                outcomes.push(AppendOutcome::Duplicate);
                continue;
            }
            let sequence: i64 = tx.query_row(
                "SELECT COALESCE(MAX(sequence_number),0)+1 FROM events WHERE run_id=?1",
                [&event.run_id],
                |row| row.get(0),
            )?;
            event.sequence_number = sequence;
            tx.execute(
                "INSERT INTO events(event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,recorded_at,sequence_number,correlation_id,causation_id,idempotency_key,payload,config_version)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                params![
                    event.event_id,
                    event.event_type.to_string(),
                    event.schema_version,
                    event.run_id,
                    event.cycle_id,
                    event.symbol,
                    event.event_time.to_string(),
                    event.recorded_at.to_string(),
                    event.sequence_number,
                    event.correlation_id,
                    event.causation_id,
                    event.idempotency_key,
                    event.payload.to_string(),
                    event.config_version,
                ],
            )?;
            outcomes.push(AppendOutcome::Appended(sequence));
        }
        tx.commit()?;
        Ok(outcomes)
    }
}

impl EventReader for SqliteStore {
    fn load_after(&self, run_id: &str, sequence: i64) -> Result<Vec<EventEnvelope>> {
        self.load_after_limited(run_id, sequence, usize::MAX)
    }
}

impl StateReader for SqliteStore {
    fn rebuild(&self, initial: StrategyState) -> Result<(StrategyState, i64)> {
        self.integrity_check()?;
        let run_id = initial.run_id.clone();
        let context = crate::run_context::RunContext::load(self, &run_id)?;
        let durable_initial = context
            .as_ref()
            .map_or(initial, crate::run_context::RunContext::initial_state);
        let (sequence, mut state) = self
            .latest_snapshot(&run_id)?
            .unwrap_or((0, durable_initial));
        if let Some(context) = context.as_ref() {
            context.validate_snapshot(&state)?;
        }
        // Legacy snapshots predate newly projected audit anchors. Hydrate them
        // from the hash-verified configuration before applying newer events.
        self.normalize_state_for_run(&mut state, &run_id)?;
        let events = self.load_after(&run_id, sequence)?;
        let mut last = sequence;
        for event in events {
            if event.sequence_number != last + 1 {
                return Err(anyhow!("event sequence gap after {last}"));
            }
            state.apply_event(&event)?;
            last = event.sequence_number;
        }
        self.normalize_state_for_run(&mut state, &run_id)?;
        state.duplicate_events = self.duplicate_count(&run_id)?;
        state
            .validate_invariants()
            .context("snapshot journal rebuild violates aggregate invariants")?;
        Ok((state, last))
    }
}

impl SqliteStore {
    fn normalize_state_for_run(&self, state: &mut StrategyState, run_id: &str) -> Result<()> {
        state.normalize_legacy_derived_fields();
        let snapshot_config = if state.audited_config.is_none()
            || state.audited_profit_guard_policy.is_none()
            || state.audited_standard_quantity == 0
        {
            self.first_payload_by_type(run_id, EventType::ConfigSnapshotted)?
                .map(|payload| {
                    Config::from_snapshot_payload(&payload)
                        .context("invalid configuration snapshot during state normalization")
                })
                .transpose()?
        } else {
            None
        };
        if state.audited_config.is_none() {
            state.audited_config = snapshot_config.clone();
        }
        if state.audited_profit_guard_policy.is_none() {
            if let Some(config) = snapshot_config.as_ref() {
                state.audited_profit_guard_policy =
                    Some(crate::domain::ProfitGuardPolicy::from(config));
                if config.uses_fixed_quantity() {
                    state.audited_standard_quantity = config.standard_quantity;
                    state.audited_lot_size = config.lot_size;
                }
            }
        } else if state.audited_standard_quantity == 0 {
            if let Some(config) = snapshot_config.as_ref() {
                if config.uses_fixed_quantity() {
                    state.audited_standard_quantity = config.standard_quantity;
                    state.audited_lot_size = config.lot_size;
                }
            }
        }
        Ok(())
    }
}

fn checksum(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
