//! Durable, hash-verified identity used to seed every run projection.
//!
//! Runtime configuration selects the database to open, but it must never
//! change the initial cash, inventory or grid identity of an existing run.

use crate::{
    config::Config,
    decision::AlgorithmManifest,
    domain::{ProfitGuardPolicy, StrategyState},
    event::EventType,
    journal::SqliteStore,
};
use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct RunContext {
    pub run_id: String,
    pub cycle_id: String,
    pub config: Config,
    pub algorithm_manifest: Option<AlgorithmManifest>,
}

impl RunContext {
    /// Load a current run identity. A journal without a configuration snapshot
    /// remains a legacy caller-seeded projection and returns `None`.
    pub fn load(store: &SqliteStore, run_id: &str) -> Result<Option<Self>> {
        let Some(config_event) = store.first_event_by_type(run_id, EventType::ConfigSnapshotted)?
        else {
            return Ok(None);
        };
        if config_event.schema_version >= 2
            && config_event
                .payload
                .get("_content_sha256")
                .and_then(serde_json::Value::as_str)
                .is_none()
        {
            bail!("current durable run configuration lacks its content hash")
        }
        let config = Config::from_snapshot_payload(&config_event.payload)
            .context("invalid durable run configuration")?;
        let run_started = store
            .first_event_by_type(run_id, EventType::RunStarted)?
            .context("configured run lacks RUN_STARTED identity")?;
        let initial_cash_text = run_started
            .payload
            .get("initial_cash")
            .and_then(serde_json::Value::as_str)
            .context("RUN_STARTED lacks exact initial_cash")?;
        let initial_cash = initial_cash_text
            .parse::<Decimal>()
            .context("RUN_STARTED initial_cash is invalid")?;
        let initial_position = run_started
            .payload
            .get("initial_position")
            .and_then(serde_json::Value::as_i64)
            .context("RUN_STARTED lacks exact initial_position")?;
        let initial_sellable = run_started
            .payload
            .get("initial_sellable")
            .and_then(serde_json::Value::as_i64)
            .context("RUN_STARTED lacks exact initial_sellable")?;
        if run_started.run_id != run_id
            || config_event.run_id != run_id
            || config_event.cycle_id != run_started.cycle_id
            || config_event.correlation_id != run_started.correlation_id
            || run_started.symbol != config.symbol
            || config_event.symbol != config.symbol
            || run_started.config_version != config.config_version
            || config_event.config_version != config.config_version
            || initial_cash_text != config.initial_cash.to_string()
            || initial_cash != config.initial_cash
            || initial_position != config.initial_position
            || initial_sellable != config.initial_sellable
        {
            bail!("RUN_STARTED and CONFIG_SNAPSHOTTED identities differ")
        }
        let algorithm_event = store.first_event_by_type(run_id, EventType::AlgorithmRegistered)?;
        if config_event.schema_version >= 2 {
            if store.event_count_by_type(run_id, EventType::RunStarted)? != 1
                || store.event_count_by_type(run_id, EventType::ConfigSnapshotted)? != 1
                || store.event_count_by_type(run_id, EventType::AlgorithmRegistered)? != 1
            {
                bail!("current run requires one complete durable identity bootstrap")
            }
            let algorithm_sequence = algorithm_event
                .as_ref()
                .context("current run lacks ALGORITHM_REGISTERED identity")?
                .sequence_number;
            if !(run_started.sequence_number < config_event.sequence_number
                && config_event.sequence_number < algorithm_sequence)
            {
                bail!("durable run identity bootstrap is out of order")
            }
        }
        let algorithm_manifest = algorithm_event
            .map(|event| {
                if event.cycle_id != run_started.cycle_id
                    || event.correlation_id != run_started.correlation_id
                    || event.symbol != config.symbol
                    || event.config_version != config.config_version
                {
                    bail!("ALGORITHM_REGISTERED differs from its durable run identity")
                }
                let manifest: AlgorithmManifest = serde_json::from_value(event.payload)
                    .context("invalid durable algorithm manifest identity")?;
                if config_event.schema_version >= 2
                    && [
                        &manifest.artifact_sha256,
                        &manifest.environment_sha256,
                        &manifest.platform_sha256,
                    ]
                    .iter()
                    .any(|hash| {
                        hash.len() != 64
                            || !hash
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    })
                {
                    bail!("current algorithm manifest lacks a canonical SHA-256 identity")
                }
                Ok(manifest)
            })
            .transpose()?;
        Ok(Some(Self {
            run_id: run_id.to_owned(),
            cycle_id: run_started.cycle_id,
            config,
            algorithm_manifest,
        }))
    }

    pub fn initial_state(&self) -> StrategyState {
        StrategyState::new(
            self.run_id.clone(),
            self.cycle_id.clone(),
            self.config.symbol.clone(),
            self.config.anchor_price,
            self.config.initial_cash,
            self.config.initial_position,
            self.config.initial_sellable,
        )
    }

    pub fn validate_snapshot(&self, state: &StrategyState) -> Result<()> {
        if state.run_id != self.run_id
            || state.symbol != self.config.symbol
            || state.anchor_price != self.config.anchor_price
            || state
                .audited_config
                .as_ref()
                .is_some_and(|config| config != &self.config)
            || state
                .audited_profit_guard_policy
                .as_ref()
                .is_some_and(|policy| policy != &ProfitGuardPolicy::from(&self.config))
            || (state.audited_standard_quantity > 0
                && state.audited_standard_quantity != self.config.standard_quantity)
            || (state.audited_lot_size > 0 && state.audited_lot_size != self.config.lot_size)
        {
            bail!("snapshot identity differs from its durable run context")
        }
        Ok(())
    }
}
