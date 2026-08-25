use crate::{
    domain::Order,
    event::{current_event_schema, EventEnvelope, EventType},
    journal::{EventReader, SqliteStore},
    ths_sim::{RemoteFilledEvidence, SimulatedOrderDraft},
};
use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::Path;

const OUTBOX_SCHEMA_VERSION: i64 = 4;
const OUTBOX_SCHEMA_V3: i64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StagedIntentState {
    Discovered,
    Eligible,
    Submitting,
    Submitted,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StagedCancelState {
    NotRequested,
    Cancelling,
    Cancelled,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StagedTerminalResolution {
    Unresolved,
    Filled,
}

impl StagedTerminalResolution {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "UNRESOLVED" => Ok(Self::Unresolved),
            "FILLED" => Ok(Self::Filled),
            _ => bail!("unknown Tonghuashun terminal resolution: {value}"),
        }
    }
}

impl StagedCancelState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "NOT_REQUESTED" => Ok(Self::NotRequested),
            "CANCELLING" => Ok(Self::Cancelling),
            "CANCELLED" => Ok(Self::Cancelled),
            "AMBIGUOUS" => Ok(Self::Ambiguous),
            _ => bail!("unknown Tonghuashun cancel state: {value}"),
        }
    }
}

impl StagedIntentState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "DISCOVERED" => Ok(Self::Discovered),
            "ELIGIBLE" => Ok(Self::Eligible),
            "SUBMITTING" => Ok(Self::Submitting),
            "SUBMITTED" => Ok(Self::Submitted),
            "AMBIGUOUS" => Ok(Self::Ambiguous),
            _ => bail!("unknown Tonghuashun outbox state: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StagedIntent {
    pub intent_id: String,
    pub order_id: String,
    pub source_intent_sequence: i64,
    pub source_submitted_sequence: Option<i64>,
    pub source_event_time: NaiveDateTime,
    pub request_sha256: String,
    pub order: SimulatedOrderDraft,
    pub state: StagedIntentState,
    pub baseline_contract_ids: Vec<String>,
    pub remote_contract_id: Option<String>,
    pub remote_evidence_json: Option<String>,
    pub last_error: Option<String>,
    pub source_cancel_requested_sequence: Option<i64>,
    pub source_cancel_reason: Option<String>,
    pub cancel_state: StagedCancelState,
    pub cancel_evidence_json: Option<String>,
    pub cancel_error: Option<String>,
    pub terminal_resolution: StagedTerminalResolution,
    pub terminal_evidence_json: Option<String>,
}

impl StagedIntent {
    pub fn verify_execution_freshness(
        &self,
        now: NaiveDateTime,
        maximum_age: Duration,
    ) -> Result<()> {
        if self.state != StagedIntentState::Eligible {
            bail!("only an ELIGIBLE durable intent can prepare a simulation order");
        }
        if maximum_age <= Duration::zero() || maximum_age > Duration::minutes(10) {
            bail!("simulation execution freshness must be within ten minutes");
        }
        if self.source_event_time > now + Duration::minutes(2) {
            bail!("source order time is unexpectedly in the future");
        }
        if now - self.source_event_time > maximum_age {
            bail!("source order is stale and cannot be sent to the simulation UI");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutboxStatus {
    pub source_database_instance_id: String,
    pub run_id: String,
    pub cursor: i64,
    pub discovered: usize,
    pub eligible: usize,
    pub submitting: usize,
    pub submitted: usize,
    pub ambiguous: usize,
    pub cancel_requested: usize,
    pub cancelling: usize,
    pub cancelled: usize,
    pub cancel_ambiguous: usize,
}

pub struct ThsSimOutbox {
    conn: Connection,
}

fn canonical_execution_identity(identity_json: &str) -> Result<(String, String)> {
    let value: serde_json::Value = serde_json::from_str(identity_json)
        .context("Tonghuashun remote execution identity is invalid JSON")?;
    if !value.is_object() {
        bail!("Tonghuashun remote execution identity must be one JSON object");
    }
    let canonical = serde_json::to_string(&value)?;
    let identity_sha256 = hex::encode(Sha256::digest(canonical.as_bytes()));
    Ok((canonical, identity_sha256))
}

impl ThsSimOutbox {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn =
            Connection::open(path).context("failed to open Tonghuashun simulation outbox")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS outbox_metadata(
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               schema_version INTEGER NOT NULL,
               source_database_instance_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               cursor INTEGER NOT NULL CHECK(cursor>=0)
             );
             CREATE TABLE IF NOT EXISTS staged_intents(
               intent_id TEXT PRIMARY KEY,
               order_id TEXT NOT NULL UNIQUE,
               source_intent_sequence INTEGER NOT NULL UNIQUE,
               source_submitted_sequence INTEGER UNIQUE,
               source_event_time TEXT NOT NULL,
               request_sha256 TEXT NOT NULL,
               request_json TEXT NOT NULL,
               state TEXT NOT NULL CHECK(state IN ('DISCOVERED','ELIGIBLE','SUBMITTING','SUBMITTED','AMBIGUOUS')),
               baseline_contract_ids_json TEXT,
               remote_contract_id TEXT UNIQUE,
               remote_evidence_json TEXT,
               last_error TEXT,
               source_cancel_requested_sequence INTEGER UNIQUE,
               source_cancel_reason TEXT,
               cancel_state TEXT NOT NULL DEFAULT 'NOT_REQUESTED',
               cancel_evidence_json TEXT,
               cancel_error TEXT
             );
             CREATE TABLE IF NOT EXISTS remote_adapter_binding(
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               source_database_instance_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               identity_sha256 TEXT NOT NULL,
               identity_json TEXT NOT NULL,
               bound_at TEXT NOT NULL
             );
             CREATE TRIGGER IF NOT EXISTS remote_adapter_binding_no_update
             BEFORE UPDATE ON remote_adapter_binding
             BEGIN SELECT RAISE(ABORT,'remote adapter binding is immutable'); END;
             CREATE TRIGGER IF NOT EXISTS remote_adapter_binding_no_delete
             BEFORE DELETE ON remote_adapter_binding
             BEGIN SELECT RAISE(ABORT,'remote adapter binding is immutable'); END;
             COMMIT;",
        )?;
        migrate_outbox_to_v3(&conn)?;
        migrate_outbox_to_v4(&conn)?;
        Ok(Self { conn })
    }

    pub fn bind_or_verify(
        &mut self,
        source_database_instance_id: &str,
        run_id: &str,
        initial_after_sequence: Option<i64>,
    ) -> Result<i64> {
        if source_database_instance_id.len() != 32
            || !source_database_instance_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("source database instance id is not canonical lowercase hex");
        }
        if run_id.trim().is_empty() {
            bail!("run id cannot be empty");
        }
        let existing = self
            .conn
            .query_row(
                "SELECT schema_version,source_database_instance_id,run_id,cursor
                 FROM outbox_metadata WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((schema, stored_source, stored_run, cursor)) = existing {
            if schema != OUTBOX_SCHEMA_VERSION {
                bail!("unsupported Tonghuashun outbox schema {schema}");
            }
            if stored_source != source_database_instance_id || stored_run != run_id {
                bail!("Tonghuashun outbox source identity changed");
            }
            if let Some(requested) = initial_after_sequence {
                if requested != cursor {
                    bail!("existing Tonghuashun outbox cursor cannot be changed");
                }
            }
            return Ok(cursor);
        }
        let cursor = initial_after_sequence.context(
            "a new Tonghuashun outbox requires an explicit --after-sequence history boundary",
        )?;
        if cursor < 0 {
            bail!("initial outbox cursor cannot be negative");
        }
        self.conn.execute(
            "INSERT INTO outbox_metadata(singleton,schema_version,source_database_instance_id,run_id,cursor)
             VALUES(1,?1,?2,?3,?4)",
            params![OUTBOX_SCHEMA_VERSION, source_database_instance_id, run_id, cursor],
        )?;
        Ok(cursor)
    }

    pub fn has_binding(&self) -> Result<bool> {
        let schema = self
            .conn
            .query_row(
                "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        match schema {
            None => Ok(false),
            Some(OUTBOX_SCHEMA_VERSION) => {
                self.binding()?;
                Ok(true)
            }
            Some(version) => bail!("unsupported Tonghuashun outbox schema {version}"),
        }
    }

    pub fn bind_execution_identity_once(&mut self, identity_json: &str) -> Result<String> {
        let (source_database_instance_id, run_id, _) = self.binding()?;
        let (canonical, identity_sha256) = canonical_execution_identity(identity_json)?;
        let existing = self
            .conn
            .query_row(
                "SELECT source_database_instance_id,run_id,identity_sha256,identity_json
                   FROM remote_adapter_binding WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((stored_source, stored_run, stored_sha256, stored_json)) = existing {
            if stored_source != source_database_instance_id
                || stored_run != run_id
                || stored_sha256 != identity_sha256
                || stored_json != canonical
            {
                bail!("Tonghuashun remote execution identity changed");
            }
            return Ok(identity_sha256);
        }
        let status = self.status()?;
        if status.discovered != 0
            || status.eligible != 0
            || status.submitting != 0
            || status.ambiguous != 0
            || status.cancel_requested != 0
            || status.cancelling != 0
            || status.cancel_ambiguous != 0
            || self.staged_intents()?.iter().any(|intent| {
                intent.state != StagedIntentState::Submitted
                    || !(intent.terminal_resolution == StagedTerminalResolution::Filled
                        || intent.cancel_state == StagedCancelState::Cancelled)
            })
        {
            bail!("Tonghuashun remote execution identity requires a frozen terminal outbox");
        }
        self.conn.execute(
            "INSERT INTO remote_adapter_binding(
               singleton,source_database_instance_id,run_id,identity_sha256,identity_json,bound_at
             ) VALUES(1,?1,?2,?3,?4,?5)",
            params![
                source_database_instance_id,
                run_id,
                identity_sha256,
                canonical,
                Utc::now().naive_utc().to_string(),
            ],
        )?;
        Ok(identity_sha256)
    }

    pub fn verify_execution_identity(&self, identity_json: &str) -> Result<String> {
        let (source_database_instance_id, run_id, _) = self.binding()?;
        let (canonical, identity_sha256) = canonical_execution_identity(identity_json)?;
        let stored = self
            .conn
            .query_row(
                "SELECT source_database_instance_id,run_id,identity_sha256,identity_json
                   FROM remote_adapter_binding WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .context("Tonghuashun remote execution identity is not durably bound")?;
        if stored.0 != source_database_instance_id
            || stored.1 != run_id
            || stored.2 != identity_sha256
            || stored.3 != canonical
        {
            bail!("Tonghuashun remote execution identity changed");
        }
        Ok(identity_sha256)
    }

    pub fn ingest(&mut self, events: &[EventEnvelope]) -> Result<OutboxStatus> {
        let (source_database_instance_id, run_id, mut cursor) = self.binding()?;
        let transaction = self.conn.transaction()?;
        for event in events {
            if event.run_id != run_id {
                bail!("event run does not match the Tonghuashun outbox binding");
            }
            if event.sequence_number != cursor + 1 {
                bail!("event sequence is not contiguous with the Tonghuashun outbox cursor");
            }
            match event.event_type {
                EventType::OrderIntentCreated => stage_intent(&transaction, event)?,
                EventType::OrderSubmitted => mark_submitted(&transaction, event)?,
                EventType::OrderRejected | EventType::OrderCancelled => {
                    remove_terminal_without_execution(&transaction, event)?
                }
                EventType::OrderCancelRequested => mark_cancel_requested(&transaction, event)?,
                _ => {}
            }
            cursor = event.sequence_number;
        }
        transaction.execute(
            "UPDATE outbox_metadata SET cursor=?1 WHERE singleton=1",
            [cursor],
        )?;
        transaction.commit()?;
        self.status_with_binding(source_database_instance_id, run_id, cursor)
    }

    pub fn status(&self) -> Result<OutboxStatus> {
        let (source, run, cursor) = self.binding()?;
        self.status_with_binding(source, run, cursor)
    }

    pub fn staged_intents(&self) -> Result<Vec<StagedIntent>> {
        let mut statement = self.conn.prepare(
            "SELECT intent_id,order_id,source_intent_sequence,source_submitted_sequence,
                    source_event_time,request_sha256,request_json,state,
                    baseline_contract_ids_json,remote_contract_id,remote_evidence_json,last_error
                    ,source_cancel_requested_sequence,source_cancel_reason,
                    cancel_state,cancel_evidence_json,cancel_error,
                    COALESCE((SELECT terminal_kind FROM remote_execution_facts f
                              WHERE f.intent_id=staged_intents.intent_id),'UNRESOLVED'),
                    (SELECT evidence_json FROM remote_execution_facts f
                     WHERE f.intent_id=staged_intents.intent_id)
             FROM staged_intents ORDER BY source_intent_sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<i64>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, Option<String>>(16)?,
                row.get::<_, String>(17)?,
                row.get::<_, Option<String>>(18)?,
            ))
        })?;
        rows.map(|row| {
            let row = row?;
            Ok(StagedIntent {
                intent_id: row.0,
                order_id: row.1,
                source_intent_sequence: row.2,
                source_submitted_sequence: row.3,
                source_event_time: NaiveDateTime::parse_from_str(&row.4, "%Y-%m-%d %H:%M:%S%.f")?,
                request_sha256: row.5,
                order: serde_json::from_str(&row.6)?,
                state: StagedIntentState::parse(&row.7)?,
                baseline_contract_ids: row
                    .8
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_default(),
                remote_contract_id: row.9,
                remote_evidence_json: row.10,
                last_error: row.11,
                source_cancel_requested_sequence: row.12,
                source_cancel_reason: row.13,
                cancel_state: StagedCancelState::parse(&row.14)?,
                cancel_evidence_json: row.15,
                cancel_error: row.16,
                terminal_resolution: StagedTerminalResolution::parse(&row.17)?,
                terminal_evidence_json: row.18,
            })
        })
        .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_remote_filled(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        remote_contract_id: &str,
        observed_at: &str,
        fill_row_count: usize,
        filled_quantity: i64,
        filled_notional: &str,
        evidence_json: &str,
    ) -> Result<()> {
        validate_contract_ids(&[remote_contract_id.to_owned()])?;
        let evidence: RemoteFilledEvidence =
            serde_json::from_str(evidence_json).context("remote fill evidence is not canonical")?;
        if evidence.evidence_contract_version != 1 {
            bail!("remote fill evidence contract is not canonical");
        }
        let canonical_evidence_json = serde_json::to_string(&evidence)?;
        if fill_row_count == 0 || filled_quantity <= 0 {
            bail!("remote fill fact requires positive rows and quantity");
        }
        let _: rust_decimal::Decimal = filled_notional
            .parse()
            .context("remote fill notional is not decimal")?;
        if evidence.order.contract_id != remote_contract_id
            || evidence.order.remark != "全部成交"
            || evidence.order.cancelled_quantity != 0
            || evidence.order.quantity != filled_quantity
        {
            bail!("remote fill evidence order terminal does not match its aggregate");
        }
        if evidence.fills.len() != fill_row_count {
            bail!("remote fill evidence row count does not match its aggregate");
        }
        let mut unique_fill_ids = std::collections::BTreeSet::new();
        let mut recomputed_quantity = 0_i64;
        let mut recomputed_notional = rust_decimal::Decimal::ZERO;
        for fill in &evidence.fills {
            validate_contract_ids(&[fill.contract_id.clone(), fill.fill_id.clone()])?;
            if fill.contract_id != remote_contract_id
                || fill.symbol != evidence.order.symbol
                || fill.direction != evidence.order.direction
                || fill.quantity <= 0
                || fill.price <= rust_decimal::Decimal::ZERO
                || fill.amount <= rust_decimal::Decimal::ZERO
                || fill
                    .price
                    .checked_mul(rust_decimal::Decimal::from(fill.quantity))
                    != Some(fill.amount)
                || !unique_fill_ids.insert(fill.fill_id.as_str())
            {
                bail!("remote fill evidence contains a non-canonical transaction row");
            }
            recomputed_quantity = recomputed_quantity
                .checked_add(fill.quantity)
                .context("remote fill evidence quantity overflow")?;
            recomputed_notional = recomputed_notional
                .checked_add(fill.amount)
                .context("remote fill evidence notional overflow")?;
        }
        let provided_notional: rust_decimal::Decimal = filled_notional
            .parse()
            .context("remote fill notional is not decimal")?;
        if recomputed_quantity != filled_quantity || recomputed_notional != provided_notional {
            bail!("remote fill evidence aggregate does not match its transaction rows");
        }
        let fact_id = hex::encode(Sha256::digest(canonical_evidence_json.as_bytes()));
        let transaction = self.conn.transaction()?;
        let stored = transaction
            .query_row(
                "SELECT request_sha256,remote_contract_id,cancel_state,request_json
                 FROM staged_intents WHERE intent_id=?1 AND state='SUBMITTED'",
                [intent_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .context("remote fill fact lacks one durable submitted intent")?;
        if stored.0 != request_sha256 || stored.1.as_deref() != Some(remote_contract_id) {
            bail!("remote fill fact does not match its durable intent identity");
        }
        let durable_order: SimulatedOrderDraft =
            serde_json::from_str(&stored.3).context("durable remote order is not canonical")?;
        if evidence.order.symbol != durable_order.symbol
            || evidence.order.direction != durable_order.direction
            || evidence.order.quantity != durable_order.quantity
            || evidence.order.order_price != durable_order.limit_price
        {
            bail!("remote fill evidence order does not match its durable request");
        }
        if stored.2 == "CANCELLED" {
            bail!("durably cancelled contract conflicts with remote fill evidence");
        }
        let existing = transaction
            .query_row(
                "SELECT fact_id FROM remote_execution_facts WHERE intent_id=?1",
                [intent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing != fact_id {
                bail!("remote terminal fill evidence changed after it was recorded");
            }
            transaction.commit()?;
            return Ok(());
        }
        transaction.execute(
            "INSERT INTO remote_execution_facts(
               fact_id,intent_id,request_sha256,remote_contract_id,evidence_contract_version,
               observed_at,terminal_kind,fill_row_count,filled_quantity,filled_notional,evidence_json
             ) VALUES(?1,?2,?3,?4,1,?5,'FILLED',?6,?7,?8,?9)",
            params![
                fact_id,
                intent_id,
                request_sha256,
                remote_contract_id,
                observed_at,
                i64::try_from(fill_row_count).context("remote fill row count overflow")?,
                filled_quantity,
                filled_notional,
                canonical_evidence_json,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn begin_submission(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        baseline_contract_ids: &[String],
    ) -> Result<()> {
        validate_contract_ids(baseline_contract_ids)?;
        let baseline_json = serde_json::to_string(baseline_contract_ids)?;
        let transaction = self.conn.transaction()?;
        let changed = transaction.execute(
            "UPDATE staged_intents
             SET state='SUBMITTING',baseline_contract_ids_json=?1,last_error=NULL
             WHERE intent_id=?2 AND request_sha256=?3 AND state='ELIGIBLE'",
            params![baseline_json, intent_id, request_sha256],
        )?;
        if changed != 1 {
            bail!("eligible Tonghuashun intent could not be claimed exactly once");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_submission(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        remote_contract_id: &str,
        evidence_json: &str,
    ) -> Result<()> {
        validate_contract_ids(&[remote_contract_id.to_owned()])?;
        let _: serde_json::Value =
            serde_json::from_str(evidence_json).context("remote evidence is not JSON")?;
        let transaction = self.conn.transaction()?;
        let baseline_json = transaction
            .query_row(
                "SELECT baseline_contract_ids_json FROM staged_intents
                 WHERE intent_id=?1 AND request_sha256=?2 AND state='SUBMITTING'",
                params![intent_id, request_sha256],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .context("only a currently SUBMITTING intent can be completed")?;
        let baseline_contract_ids: Vec<String> = serde_json::from_str(&baseline_json)
            .context("stored baseline contract IDs are not JSON")?;
        validate_contract_ids(&baseline_contract_ids)?;
        if baseline_contract_ids
            .iter()
            .any(|contract_id| contract_id == remote_contract_id)
        {
            bail!("Tonghuashun completion must reference one new contract");
        }
        let changed = transaction.execute(
            "UPDATE staged_intents
             SET state='SUBMITTED',remote_contract_id=?1,remote_evidence_json=?2,last_error=NULL
             WHERE intent_id=?3 AND request_sha256=?4 AND state='SUBMITTING'",
            params![remote_contract_id, evidence_json, intent_id, request_sha256],
        )?;
        if changed != 1 {
            bail!("submitting Tonghuashun intent could not be completed exactly once");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_ambiguous(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        error: &str,
        evidence_json: Option<&str>,
    ) -> Result<()> {
        if error.trim().is_empty() {
            bail!("ambiguous submission requires a diagnostic");
        }
        if let Some(evidence) = evidence_json {
            let _: serde_json::Value =
                serde_json::from_str(evidence).context("ambiguous evidence is not JSON")?;
        }
        let changed = self.conn.execute(
            "UPDATE staged_intents
             SET state='AMBIGUOUS',remote_evidence_json=?1,last_error=?2
             WHERE intent_id=?3 AND request_sha256=?4 AND state='SUBMITTING'",
            params![evidence_json, error, intent_id, request_sha256],
        )?;
        if changed != 1 {
            bail!("only a currently SUBMITTING intent can become AMBIGUOUS");
        }
        Ok(())
    }

    pub fn begin_cancellation(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        remote_contract_id: &str,
    ) -> Result<()> {
        validate_contract_ids(&[remote_contract_id.to_owned()])?;
        let changed = self.conn.execute(
            "UPDATE staged_intents
             SET cancel_state='CANCELLING',cancel_error=NULL
             WHERE intent_id=?1 AND request_sha256=?2 AND state='SUBMITTED'
               AND remote_contract_id=?3 AND cancel_state='NOT_REQUESTED'",
            params![intent_id, request_sha256, remote_contract_id],
        )?;
        if changed != 1 {
            bail!(
                "submitted Tonghuashun contract could not be claimed for cancellation exactly once"
            );
        }
        Ok(())
    }

    pub fn complete_cancellation(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        remote_contract_id: &str,
        evidence_json: &str,
    ) -> Result<()> {
        validate_contract_ids(&[remote_contract_id.to_owned()])?;
        let _: serde_json::Value =
            serde_json::from_str(evidence_json).context("cancel evidence is not JSON")?;
        let changed = self.conn.execute(
            "UPDATE staged_intents
             SET cancel_state='CANCELLED',cancel_evidence_json=?1,cancel_error=NULL
             WHERE intent_id=?2 AND request_sha256=?3 AND state='SUBMITTED'
               AND remote_contract_id=?4 AND cancel_state='CANCELLING'",
            params![evidence_json, intent_id, request_sha256, remote_contract_id],
        )?;
        if changed != 1 {
            bail!("cancelling Tonghuashun contract could not be completed exactly once");
        }
        Ok(())
    }

    pub fn mark_cancellation_ambiguous(
        &mut self,
        intent_id: &str,
        request_sha256: &str,
        remote_contract_id: &str,
        error: &str,
        evidence_json: Option<&str>,
    ) -> Result<()> {
        validate_contract_ids(&[remote_contract_id.to_owned()])?;
        if error.trim().is_empty() {
            bail!("ambiguous cancellation requires a diagnostic");
        }
        if let Some(evidence) = evidence_json {
            let _: serde_json::Value =
                serde_json::from_str(evidence).context("ambiguous cancel evidence is not JSON")?;
        }
        let changed = self.conn.execute(
            "UPDATE staged_intents
             SET cancel_state='AMBIGUOUS',cancel_evidence_json=?1,cancel_error=?2
             WHERE intent_id=?3 AND request_sha256=?4 AND state='SUBMITTED'
               AND remote_contract_id=?5 AND cancel_state='CANCELLING'",
            params![
                evidence_json,
                error,
                intent_id,
                request_sha256,
                remote_contract_id
            ],
        )?;
        if changed != 1 {
            bail!("only a currently CANCELLING contract can become AMBIGUOUS");
        }
        Ok(())
    }

    fn binding(&self) -> Result<(String, String, i64)> {
        self.conn
            .query_row(
                "SELECT source_database_instance_id,run_id,cursor
                 FROM outbox_metadata WHERE singleton=1 AND schema_version=?1",
                [OUTBOX_SCHEMA_VERSION],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .context("Tonghuashun outbox is not bound to a source ledger")
    }

    fn status_with_binding(
        &self,
        source_database_instance_id: String,
        run_id: String,
        cursor: i64,
    ) -> Result<OutboxStatus> {
        let (
            discovered,
            eligible,
            submitting,
            submitted,
            ambiguous,
            cancel_requested,
            cancelling,
            cancelled,
            cancel_ambiguous,
        ): (i64, i64, i64, i64, i64, i64, i64, i64, i64) =
            self.conn.query_row(
                "SELECT COALESCE(SUM(state='DISCOVERED'),0),
                    COALESCE(SUM(state='ELIGIBLE'),0),
                    COALESCE(SUM(state='SUBMITTING'),0),
                    COALESCE(SUM(state='SUBMITTED'),0),
                    COALESCE(SUM(state='AMBIGUOUS'),0),
                    COALESCE(SUM(source_cancel_requested_sequence IS NOT NULL AND cancel_state='NOT_REQUESTED'),0),
                    COALESCE(SUM(cancel_state='CANCELLING'),0),
                    COALESCE(SUM(cancel_state='CANCELLED'),0),
                    COALESCE(SUM(cancel_state='AMBIGUOUS' AND NOT EXISTS(
                      SELECT 1 FROM remote_execution_facts f
                      WHERE f.intent_id=staged_intents.intent_id AND f.terminal_kind='FILLED'
                    )),0)
             FROM staged_intents",
                [],
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
                    ))
                },
            )?;
        Ok(OutboxStatus {
            source_database_instance_id,
            run_id,
            cursor,
            discovered: usize::try_from(discovered).unwrap_or_default(),
            eligible: usize::try_from(eligible).unwrap_or_default(),
            submitting: usize::try_from(submitting).unwrap_or_default(),
            submitted: usize::try_from(submitted).unwrap_or_default(),
            ambiguous: usize::try_from(ambiguous).unwrap_or_default(),
            cancel_requested: usize::try_from(cancel_requested).unwrap_or_default(),
            cancelling: usize::try_from(cancelling).unwrap_or_default(),
            cancelled: usize::try_from(cancelled).unwrap_or_default(),
            cancel_ambiguous: usize::try_from(cancel_ambiguous).unwrap_or_default(),
        })
    }
}

pub fn stage_from_source(
    source_database: impl AsRef<Path>,
    run_id: &str,
    outbox_path: impl AsRef<Path>,
    initial_after_sequence: Option<i64>,
) -> Result<OutboxStatus> {
    let source = SqliteStore::open_existing_read_only(source_database)?;
    source.verify_current_schema()?;
    let source_id = source.database_instance_id()?;
    let mut outbox = ThsSimOutbox::open(outbox_path)?;
    let cursor = outbox.bind_or_verify(&source_id, run_id, initial_after_sequence)?;
    let events = source.load_after(run_id, cursor)?;
    outbox.ingest(&events)
}

fn stage_intent(transaction: &Transaction<'_>, event: &EventEnvelope) -> Result<()> {
    if event.schema_version != current_event_schema(EventType::OrderIntentCreated) {
        bail!("only current order intents can enter the Tonghuashun simulation outbox");
    }
    let order: Order = serde_json::from_value(event.payload.clone())
        .context("ORDER_INTENT_CREATED payload is not a canonical order")?;
    let symbol = normalize_a_share_symbol(&event.symbol)?;
    let draft = SimulatedOrderDraft::new(
        order.intent.direction,
        symbol,
        order.intent.limit_price,
        order.intent.quantity,
    )?;
    let request_json = serde_json::to_string(&draft)?;
    let request_sha256 = hex::encode(Sha256::digest(request_json.as_bytes()));
    transaction.execute(
        "INSERT OR IGNORE INTO staged_intents(
           intent_id,order_id,source_intent_sequence,source_submitted_sequence,
           source_event_time,request_sha256,request_json,state
         ) VALUES(?1,?2,?3,NULL,?4,?5,?6,'DISCOVERED')",
        params![
            order.intent.intent_id,
            order.order_id,
            event.sequence_number,
            event.event_time.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            request_sha256,
            request_json,
        ],
    )?;
    let existing: (String, String, i64) = transaction.query_row(
        "SELECT order_id,request_sha256,source_intent_sequence
         FROM staged_intents WHERE intent_id=?1",
        [order.intent.intent_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if existing != (order.order_id, request_sha256, event.sequence_number) {
        bail!("same intent id changed its Tonghuashun order request");
    }
    Ok(())
}

fn mark_submitted(transaction: &Transaction<'_>, event: &EventEnvelope) -> Result<()> {
    let order_id = event
        .payload
        .get("order_id")
        .and_then(serde_json::Value::as_str)
        .context("ORDER_SUBMITTED lacks order_id")?;
    let changed = transaction.execute(
        "UPDATE staged_intents
         SET source_submitted_sequence=?1,state='ELIGIBLE'
         WHERE order_id=?2 AND state='DISCOVERED'",
        params![event.sequence_number, order_id],
    )?;
    if changed != 1 {
        bail!("ORDER_SUBMITTED does not match exactly one discovered intent");
    }
    Ok(())
}

fn remove_terminal_without_execution(
    transaction: &Transaction<'_>,
    event: &EventEnvelope,
) -> Result<()> {
    let order_id = event
        .payload
        .get("order_id")
        .and_then(serde_json::Value::as_str)
        .context("terminal order event lacks order_id")?;
    let changed = transaction.execute(
        "DELETE FROM staged_intents
         WHERE order_id=?1 AND state IN ('DISCOVERED','ELIGIBLE')",
        [order_id],
    )?;
    if changed != 1 {
        bail!("terminal Paper order does not match exactly one unexecuted simulation intent");
    }
    Ok(())
}

fn mark_cancel_requested(transaction: &Transaction<'_>, event: &EventEnvelope) -> Result<()> {
    if event.schema_version != current_event_schema(EventType::OrderCancelRequested) {
        bail!("only current cancel requests can enter the Tonghuashun simulation outbox");
    }
    let order_id = event
        .payload
        .get("order_id")
        .and_then(serde_json::Value::as_str)
        .filter(|order_id| !order_id.trim().is_empty())
        .context("ORDER_CANCEL_REQUESTED lacks order_id")?;
    let reason = event
        .payload
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .context("ORDER_CANCEL_REQUESTED lacks a non-empty reason")?;
    let changed = transaction.execute(
        "UPDATE staged_intents
         SET source_cancel_requested_sequence=?1,source_cancel_reason=?2
         WHERE order_id=?3 AND source_cancel_requested_sequence IS NULL",
        params![event.sequence_number, reason, order_id],
    )?;
    if changed != 1 {
        bail!("ORDER_CANCEL_REQUESTED does not match exactly one uncancelled staged intent");
    }
    Ok(())
}

fn normalize_a_share_symbol(symbol: &str) -> Result<String> {
    let normalized = symbol
        .strip_suffix(".SZ")
        .or_else(|| symbol.strip_suffix(".SH"))
        .unwrap_or(symbol);
    if normalized.len() != 6 || !normalized.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("source symbol cannot be represented by the Tonghuashun A-share form");
    }
    Ok(normalized.to_owned())
}

fn validate_contract_ids(ids: &[String]) -> Result<()> {
    let unique = ids.iter().collect::<std::collections::BTreeSet<_>>();
    if unique.len() != ids.len()
        || ids.iter().any(|id| {
            id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
    {
        bail!("Tonghuashun contract IDs are not canonical and unique");
    }
    Ok(())
}

fn migrate_outbox_to_v3(conn: &Connection) -> Result<()> {
    let mut statement = conn.prepare("PRAGMA table_info(staged_intents)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()?;
    drop(statement);
    let cancellation_columns = ["cancel_state", "cancel_evidence_json", "cancel_error"];
    let source_cancel_columns = ["source_cancel_requested_sequence", "source_cancel_reason"];
    let cancellation_count = cancellation_columns
        .iter()
        .filter(|column| columns.contains(**column))
        .count();
    let source_cancel_count = source_cancel_columns
        .iter()
        .filter(|column| columns.contains(**column))
        .count();
    if cancellation_count != 0 && cancellation_count != cancellation_columns.len() {
        bail!("Tonghuashun outbox has a partial cancellation migration");
    }
    if source_cancel_count != 0 && source_cancel_count != source_cancel_columns.len() {
        bail!("Tonghuashun outbox has a partial source-cancellation migration");
    }

    let stored_schema = conn
        .query_row(
            "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    match stored_schema {
        None if cancellation_count == cancellation_columns.len()
            && source_cancel_count == source_cancel_columns.len() => {}
        Some(OUTBOX_SCHEMA_V3) | Some(OUTBOX_SCHEMA_VERSION)
            if cancellation_count == cancellation_columns.len()
                && source_cancel_count == source_cancel_columns.len() => {}
        Some(1) if cancellation_count == 0 && source_cancel_count == 0 => {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE staged_intents ADD COLUMN cancel_state TEXT NOT NULL DEFAULT 'NOT_REQUESTED';
                 ALTER TABLE staged_intents ADD COLUMN cancel_evidence_json TEXT;
                 ALTER TABLE staged_intents ADD COLUMN cancel_error TEXT;
                 ALTER TABLE staged_intents ADD COLUMN source_cancel_requested_sequence INTEGER;
                 ALTER TABLE staged_intents ADD COLUMN source_cancel_reason TEXT;
                 CREATE UNIQUE INDEX idx_staged_intents_source_cancel_sequence
                   ON staged_intents(source_cancel_requested_sequence)
                   WHERE source_cancel_requested_sequence IS NOT NULL;
                 UPDATE outbox_metadata SET schema_version=3 WHERE singleton=1 AND schema_version=1;
                 COMMIT;",
            )?;
        }
        Some(2) if cancellation_count == cancellation_columns.len() && source_cancel_count == 0 => {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE staged_intents ADD COLUMN source_cancel_requested_sequence INTEGER;
                 ALTER TABLE staged_intents ADD COLUMN source_cancel_reason TEXT;
                 CREATE UNIQUE INDEX idx_staged_intents_source_cancel_sequence
                   ON staged_intents(source_cancel_requested_sequence)
                   WHERE source_cancel_requested_sequence IS NOT NULL;
                 UPDATE outbox_metadata SET schema_version=3 WHERE singleton=1 AND schema_version=2;
                 COMMIT;",
            )?;
        }
        Some(schema) => bail!(
            "Tonghuashun outbox schema {schema} does not match its physical cancellation columns"
        ),
        None => bail!("unbound Tonghuashun outbox has an unexpected physical schema"),
    }
    Ok(())
}

fn migrate_outbox_to_v4(conn: &Connection) -> Result<()> {
    let has_fact_table: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master
                            WHERE type='table' AND name='remote_execution_facts')",
        [],
        |row| row.get(0),
    )?;
    let stored_schema = conn
        .query_row(
            "SELECT schema_version FROM outbox_metadata WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    match stored_schema {
        None if has_fact_table => Ok(()),
        Some(OUTBOX_SCHEMA_VERSION) if has_fact_table => Ok(()),
        Some(OUTBOX_SCHEMA_V3) if has_fact_table => {
            bail!("Tonghuashun outbox has a partial v4 remote-fill migration")
        }
        Some(OUTBOX_SCHEMA_V3) | None => {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE remote_execution_facts(
                   fact_id TEXT PRIMARY KEY,
                   intent_id TEXT NOT NULL,
                   request_sha256 TEXT NOT NULL,
                   remote_contract_id TEXT NOT NULL,
                   evidence_contract_version INTEGER NOT NULL CHECK(evidence_contract_version=1),
                   observed_at TEXT NOT NULL,
                   terminal_kind TEXT NOT NULL CHECK(terminal_kind='FILLED'),
                   fill_row_count INTEGER NOT NULL CHECK(fill_row_count>0),
                   filled_quantity INTEGER NOT NULL CHECK(filled_quantity>0),
                   filled_notional TEXT NOT NULL,
                   evidence_json TEXT NOT NULL,
                   UNIQUE(intent_id,remote_contract_id,terminal_kind),
                   FOREIGN KEY(intent_id) REFERENCES staged_intents(intent_id)
                 );
                 UPDATE outbox_metadata SET schema_version=4
                   WHERE singleton=1 AND schema_version=3;
                 COMMIT;",
            )?;
            Ok(())
        }
        Some(schema) => bail!(
            "Tonghuashun outbox schema {schema} does not match its physical remote-fill table"
        ),
    }
}
