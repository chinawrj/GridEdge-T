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
    platform_upgrade::{
        algorithm_contract_sha256, platform_upgrade_id, validate_sha256, PlatformUpgradeActivated,
        PlatformUpgradeAuthorized, PLATFORM_UPGRADE_AUTHORIZATION_KIND,
        PLATFORM_UPGRADE_CERTIFICATION_PROFILE,
    },
};
use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub struct RunContext {
    pub run_id: String,
    pub cycle_id: String,
    pub config: Config,
    pub base_algorithm_manifest: Option<AlgorithmManifest>,
    pub algorithm_manifest: Option<AlgorithmManifest>,
    pub effective_platform_sha256: Option<String>,
    pub platform_sha256_history: BTreeSet<String>,
    pub pending_platform_upgrade: Option<PendingPlatformUpgrade>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingPlatformUpgrade {
    pub event_id: String,
    pub sequence_number: i64,
    pub authorization: PlatformUpgradeAuthorized,
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
        let base_algorithm_manifest = algorithm_event
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
        let config_content_sha256 = config_event
            .payload
            .get("_content_sha256")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let (
            algorithm_manifest,
            effective_platform_sha256,
            platform_sha256_history,
            pending_platform_upgrade,
        ) = if let Some(base) = base_algorithm_manifest.as_ref() {
            let mut effective = base.clone();
            let mut seen = BTreeSet::from([base.platform_sha256.clone()]);
            let mut pending: Option<PendingPlatformUpgrade> = None;
            let contract_sha256 = algorithm_contract_sha256(base)?;
            for event in store.platform_upgrade_events(run_id)? {
                match event.event_type {
                    EventType::PlatformUpgradeAuthorized => {
                        if pending.is_some() {
                            bail!("platform upgrade chain contains two pending authorizations")
                        }
                        let authorization: PlatformUpgradeAuthorized =
                            serde_json::from_value(event.payload.clone())
                                .context("invalid platform upgrade authorization")?;
                        validate_upgrade_envelope(
                            &event,
                            &run_started,
                            &config,
                            &config_content_sha256,
                        )?;
                        validate_sha256(
                            &authorization.from_platform_sha256,
                            "upgrade source platform",
                        )?;
                        validate_sha256(
                            &authorization.to_platform_sha256,
                            "upgrade target platform",
                        )?;
                        validate_sha256(
                            &authorization.algorithm_contract_sha256,
                            "upgrade algorithm contract",
                        )?;
                        validate_sha256(
                            &authorization.certification_evidence_sha256,
                            "upgrade certification evidence",
                        )?;
                        if authorization.from_platform_sha256 != effective.platform_sha256
                            || authorization.to_platform_sha256
                                == authorization.from_platform_sha256
                            || authorization.target_binary_sha256
                                != authorization.to_platform_sha256
                            || authorization.algorithm_contract_sha256 != contract_sha256
                            || authorization.config_content_sha256 != config_content_sha256
                            || authorization.authorization_kind
                                != PLATFORM_UPGRADE_AUTHORIZATION_KIND
                            || authorization.certification_profile_version
                                != PLATFORM_UPGRADE_CERTIFICATION_PROFILE
                            || authorization.reason_code.trim().is_empty()
                            || authorization.operator.trim().is_empty()
                            || authorization.expected_head_sequence != event.sequence_number - 1
                            || seen.contains(&authorization.to_platform_sha256)
                            || authorization.upgrade_id
                                != platform_upgrade_id(
                                    run_id,
                                    &authorization.from_platform_sha256,
                                    &authorization.to_platform_sha256,
                                    authorization.expected_head_sequence,
                                    &authorization.certification_evidence_sha256,
                                )
                        {
                            bail!("platform upgrade authorization is not canonical")
                        }
                        pending = Some(PendingPlatformUpgrade {
                            event_id: event.event_id.clone(),
                            sequence_number: event.sequence_number,
                            authorization,
                        });
                    }
                    EventType::PlatformUpgradeActivated => {
                        let Some(expected) = pending.take() else {
                            bail!("platform upgrade activation lacks its authorization")
                        };
                        let activation: PlatformUpgradeActivated =
                            serde_json::from_value(event.payload.clone())
                                .context("invalid platform upgrade activation")?;
                        validate_upgrade_envelope(
                            &event,
                            &run_started,
                            &config,
                            &config_content_sha256,
                        )?;
                        validate_sha256(
                            &activation.full_rebuild_state_sha256,
                            "full rebuild state",
                        )?;
                        validate_sha256(&activation.paper_snapshot_sha256, "Paper snapshot")?;
                        if event.sequence_number != expected.sequence_number + 1
                            || activation.upgrade_id != expected.authorization.upgrade_id
                            || activation.authorization_event_id != expected.event_id
                            || activation.authorization_sequence != expected.sequence_number
                            || activation.from_platform_sha256
                                != expected.authorization.from_platform_sha256
                            || activation.to_platform_sha256
                                != expected.authorization.to_platform_sha256
                            || activation.observed_platform_sha256
                                != expected.authorization.to_platform_sha256
                            || activation.validated_through_sequence != expected.sequence_number
                            || !activation.paper_reconciled
                        {
                            bail!("platform upgrade activation is not canonical")
                        }
                        effective.platform_sha256 = activation.to_platform_sha256.clone();
                        seen.insert(effective.platform_sha256.clone());
                    }
                    _ => {}
                }
            }
            if let Some(upgrade) = pending.as_ref() {
                if store.latest_sequence(run_id)? != upgrade.sequence_number {
                    bail!("business event appears after a pending platform authorization")
                }
            }
            (
                Some(effective.clone()),
                Some(effective.platform_sha256),
                seen,
                pending,
            )
        } else {
            (None, None, BTreeSet::new(), None)
        };
        Ok(Some(Self {
            run_id: run_id.to_owned(),
            cycle_id: run_started.cycle_id,
            config,
            base_algorithm_manifest,
            algorithm_manifest,
            effective_platform_sha256,
            platform_sha256_history,
            pending_platform_upgrade,
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

fn validate_upgrade_envelope(
    event: &crate::event::EventEnvelope,
    run_started: &crate::event::EventEnvelope,
    config: &Config,
    config_content_sha256: &str,
) -> Result<()> {
    if event.schema_version != 1
        || event.run_id != run_started.run_id
        || event.cycle_id != run_started.cycle_id
        || event.symbol != config.symbol
        || event.config_version != config.config_version
        || config_content_sha256.is_empty()
    {
        bail!("platform upgrade event differs from its durable run identity")
    }
    Ok(())
}
