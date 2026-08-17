use crate::{
    config::Config,
    domain::Direction,
    gate::{context_hash_v4, GateContext, GateDecision, GatePolicy},
};
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const LEGACY_DECISION_CONTRACT_VERSION: u32 = 2;
pub const WHOLE_UNIT_DECISION_CONTRACT_VERSION: u32 = 3;
pub const DECISION_CONTRACT_VERSION: u32 = 4;

pub fn supported_contract_versions_are_canonical(versions: &[u32]) -> bool {
    !versions.is_empty()
        && versions.iter().all(|version| {
            matches!(
                *version,
                WHOLE_UNIT_DECISION_CONTRACT_VERSION | DECISION_CONTRACT_VERSION
            )
        })
        && versions.windows(2).all(|pair| pair[0] < pair[1])
}

pub fn whole_unit_partition(quantity: i64, standard_quantity: i64) -> Result<(i64, i64)> {
    if quantity < 0 || standard_quantity <= 0 {
        bail!("whole-unit partition requires non-negative quantity and positive unit")
    }
    let residual = quantity % standard_quantity;
    Ok((quantity - residual, residual))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RightDecision {
    Exercise {
        exercise_quantity: i64,
        defer_quantity: i64,
    },
    Defer {
        defer_quantity: i64,
    },
}

impl RightDecision {
    pub fn from_gate(
        decision: &GateDecision,
        algorithm_authorized_quantity: i64,
        standard_quantity: i64,
    ) -> Self {
        if algorithm_authorized_quantity < 0
            || standard_quantity <= 0
            || !(Decimal::ZERO..=Decimal::ONE).contains(&decision.alpha)
        {
            return Self::Defer {
                defer_quantity: algorithm_authorized_quantity.max(0),
            };
        }
        let exercise_quantity = if decision.alpha == Decimal::ONE {
            algorithm_authorized_quantity
        } else {
            let authorized_units = algorithm_authorized_quantity / standard_quantity;
            let units = (Decimal::from(authorized_units) * decision.alpha)
                .floor()
                .to_string()
                .parse::<i64>()
                .unwrap_or(0);
            units * standard_quantity
        };
        let defer_quantity = algorithm_authorized_quantity - exercise_quantity;
        if exercise_quantity > 0 {
            Self::Exercise {
                exercise_quantity,
                defer_quantity,
            }
        } else {
            Self::Defer { defer_quantity }
        }
    }

    pub fn quantities(&self) -> (i64, i64) {
        match self {
            Self::Exercise {
                exercise_quantity,
                defer_quantity,
            } => (*exercise_quantity, *defer_quantity),
            Self::Defer { defer_quantity } => (0, *defer_quantity),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GridRightStatus {
    Granted,
    Residual,
    Deferred,
    Reserved,
    PartiallyExercised,
    Exercised,
    Released,
    Transferred,
    Blocked,
    Expired,
    Revoked,
}

impl GridRightStatus {
    pub fn can_transition(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Granted,
                Self::Residual | Self::Deferred | Self::Blocked | Self::Reserved
            ) | (
                Self::Reserved,
                Self::PartiallyExercised | Self::Exercised | Self::Released
            ) | (
                Self::PartiallyExercised,
                Self::PartiallyExercised | Self::Exercised | Self::Released | Self::Expired
            ) | (
                Self::Residual | Self::Deferred | Self::Blocked | Self::Released,
                Self::Transferred | Self::Expired | Self::Revoked
            ) | (Self::PartiallyExercised, Self::Transferred | Self::Revoked)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridRightCapacity {
    #[serde(with = "rust_decimal::serde::str")]
    pub mechanical_budget_cap: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub deployed_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub available_budget: Decimal,
    /// Quantity-denominated mechanical capacity used by current buy rights.
    /// Historical budget-denominated rights deserialize these fields as zero.
    #[serde(default)]
    pub mechanical_quantity_cap: i64,
    #[serde(default)]
    pub deployed_quantity: i64,
    #[serde(default)]
    pub available_quantity: i64,
    pub eligible_quantity: i64,
    pub eligible_lot_ids: Vec<String>,
    pub accumulated_grid_indices: Vec<i32>,
    #[serde(default)]
    pub t_plus_one_blocked_quantity: i64,
    #[serde(default)]
    pub t_plus_one_blocked_lot_ids: Vec<String>,
    #[serde(default)]
    pub risk_blocked_quantity: i64,
    #[serde(default)]
    pub risk_blocked_lot_ids: Vec<String>,
    #[serde(default)]
    pub no_profit_blocked_quantity: i64,
    #[serde(default)]
    pub no_profit_blocked_lot_ids: Vec<String>,
    #[serde(default)]
    pub source_right_ids: Vec<String>,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub carried_in_budget: Decimal,
    #[serde(default)]
    pub carried_in_quantity: i64,
    /// Authoritative capacity is held in the tranche ledger. This list binds
    /// the algorithm-facing aggregate view to the exact inventory tokens.
    #[serde(default)]
    pub tranche_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RightTrancheUnit {
    Budget,
    Quantity,
}

/// One indivisible provenance slice minted by a directed grid crossing.
/// Balances use double-entry conservation: minted equals the sum of the five
/// disposition accounts at every ledger sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RightTranche {
    pub tranche_id: String,
    pub cycle_id: String,
    pub excursion_epoch: String,
    pub direction: Direction,
    pub birth_grid_index: i32,
    pub unit: RightTrancheUnit,
    pub owner_right_id: String,
    pub lot_id: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    pub minted_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub available_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reserved_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub consumed_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub revoked_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub expired_budget: Decimal,
    pub minted_quantity: i64,
    pub available_quantity: i64,
    pub reserved_quantity: i64,
    pub consumed_quantity: i64,
    pub revoked_quantity: i64,
    pub expired_quantity: i64,
}

impl RightTranche {
    pub fn id_for(
        cycle_id: &str,
        excursion_epoch: &str,
        direction: Direction,
        birth_grid_index: i32,
        lot_id: Option<&str>,
    ) -> String {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "{cycle_id}:tranche:{excursion_epoch}:{direction:?}:{birth_grid_index}:{}",
                lot_id.unwrap_or("budget")
            )
            .as_bytes(),
        )
        .to_string()
    }

    pub fn minted_budget(
        cycle_id: &str,
        excursion_epoch: &str,
        owner_right_id: &str,
        birth_grid_index: i32,
        amount: Decimal,
    ) -> Self {
        Self {
            tranche_id: Self::id_for(
                cycle_id,
                excursion_epoch,
                Direction::Buy,
                birth_grid_index,
                None,
            ),
            cycle_id: cycle_id.to_owned(),
            excursion_epoch: excursion_epoch.to_owned(),
            direction: Direction::Buy,
            birth_grid_index,
            unit: RightTrancheUnit::Budget,
            owner_right_id: owner_right_id.to_owned(),
            lot_id: None,
            minted_budget: amount,
            available_budget: amount,
            reserved_budget: Decimal::ZERO,
            consumed_budget: Decimal::ZERO,
            revoked_budget: Decimal::ZERO,
            expired_budget: Decimal::ZERO,
            minted_quantity: 0,
            available_quantity: 0,
            reserved_quantity: 0,
            consumed_quantity: 0,
            revoked_quantity: 0,
            expired_quantity: 0,
        }
    }

    pub fn minted_quantity(
        cycle_id: &str,
        excursion_epoch: &str,
        owner_right_id: &str,
        birth_grid_index: i32,
        lot_id: &str,
        quantity: i64,
    ) -> Self {
        Self {
            tranche_id: Self::id_for(
                cycle_id,
                excursion_epoch,
                Direction::Sell,
                birth_grid_index,
                Some(lot_id),
            ),
            cycle_id: cycle_id.to_owned(),
            excursion_epoch: excursion_epoch.to_owned(),
            direction: Direction::Sell,
            birth_grid_index,
            unit: RightTrancheUnit::Quantity,
            owner_right_id: owner_right_id.to_owned(),
            lot_id: Some(lot_id.to_owned()),
            minted_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            reserved_budget: Decimal::ZERO,
            consumed_budget: Decimal::ZERO,
            revoked_budget: Decimal::ZERO,
            expired_budget: Decimal::ZERO,
            minted_quantity: quantity,
            available_quantity: quantity,
            reserved_quantity: 0,
            consumed_quantity: 0,
            revoked_quantity: 0,
            expired_quantity: 0,
        }
    }

    pub fn minted_buy_quantity(
        cycle_id: &str,
        excursion_epoch: &str,
        owner_right_id: &str,
        birth_grid_index: i32,
        quantity: i64,
    ) -> Self {
        Self {
            tranche_id: Self::id_for(
                cycle_id,
                excursion_epoch,
                Direction::Buy,
                birth_grid_index,
                None,
            ),
            cycle_id: cycle_id.to_owned(),
            excursion_epoch: excursion_epoch.to_owned(),
            direction: Direction::Buy,
            birth_grid_index,
            unit: RightTrancheUnit::Quantity,
            owner_right_id: owner_right_id.to_owned(),
            lot_id: None,
            minted_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            reserved_budget: Decimal::ZERO,
            consumed_budget: Decimal::ZERO,
            revoked_budget: Decimal::ZERO,
            expired_budget: Decimal::ZERO,
            minted_quantity: quantity,
            available_quantity: quantity,
            reserved_quantity: 0,
            consumed_quantity: 0,
            revoked_quantity: 0,
            expired_quantity: 0,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let budgets = [
            self.minted_budget,
            self.available_budget,
            self.reserved_budget,
            self.consumed_budget,
            self.revoked_budget,
            self.expired_budget,
        ];
        let quantities = [
            self.minted_quantity,
            self.available_quantity,
            self.reserved_quantity,
            self.consumed_quantity,
            self.revoked_quantity,
            self.expired_quantity,
        ];
        if budgets.iter().any(|value| *value < Decimal::ZERO)
            || quantities.iter().any(|value| *value < 0)
            || self.owner_right_id.trim().is_empty()
            || self.excursion_epoch.trim().is_empty()
            || self.birth_grid_index == 0
        {
            bail!("invalid right tranche")
        }
        match self.unit {
            RightTrancheUnit::Budget => {
                let conserved_budget = [
                    self.available_budget,
                    self.reserved_budget,
                    self.consumed_budget,
                    self.revoked_budget,
                    self.expired_budget,
                ]
                .into_iter()
                .try_fold(Decimal::ZERO, |total, value| total.checked_add(value))
                .context("right tranche budget conservation overflow")?;
                if self.direction != Direction::Buy
                    || self.minted_budget <= Decimal::ZERO
                    || quantities.iter().any(|value| *value != 0)
                    || self.minted_budget != conserved_budget
                {
                    bail!("buy tranche violates budget conservation")
                }
            }
            RightTrancheUnit::Quantity => {
                let conserved_quantity = [
                    self.available_quantity,
                    self.reserved_quantity,
                    self.consumed_quantity,
                    self.revoked_quantity,
                    self.expired_quantity,
                ]
                .into_iter()
                .try_fold(0_i64, i64::checked_add)
                .context("right tranche quantity conservation overflow")?;
                let lot_binding_is_valid = match self.direction {
                    Direction::Buy => self.lot_id.is_none(),
                    Direction::Sell => self
                        .lot_id
                        .as_deref()
                        .is_some_and(|value| !value.is_empty()),
                };
                if self.minted_quantity <= 0
                    || budgets.iter().any(|value| *value != Decimal::ZERO)
                    || self.minted_quantity != conserved_quantity
                    || !lot_binding_is_valid
                {
                    bail!("quantity tranche violates conservation or source binding")
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridRight {
    pub right_id: String,
    pub cycle_id: String,
    pub symbol: String,
    pub direction: Direction,
    pub grid_index: i32,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_price: Decimal,
    pub granted_at: NaiveDateTime,
    pub capacity: GridRightCapacity,
    pub status: GridRightStatus,
    pub decision_id: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    pub reserved_budget: Decimal,
    pub reserved_quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub exercised_budget: Decimal,
    pub exercised_quantity: i64,
    /// Exact algorithm disposition recorded by GATE_DECISION_MADE schema v2.
    #[serde(default)]
    pub decision_contract_version: u32,
    #[serde(default)]
    pub decision_gross_available_quantity: i64,
    #[serde(default)]
    pub decision_platform_residual_quantity: i64,
    #[serde(default)]
    pub decision_algorithm_authorized_quantity: i64,
    /// Quantity offered to the v4 algorithm after pre-decision resource caps.
    #[serde(default)]
    pub decision_algorithm_offered_quantity: i64,
    /// Whole-Q quantity removed before the algorithm by funds/inventory caps.
    #[serde(default)]
    pub decision_pre_blocked_quantity: i64,
    #[serde(default)]
    pub decided_exercise_quantity: i64,
    #[serde(default)]
    pub decided_defer_quantity: i64,
    #[serde(default)]
    pub decision_platform_blocked_quantity: i64,
    /// Portion of `decision_platform_blocked_quantity` rejected after the
    /// algorithm selected E. The remainder is the v4 pre-decision block.
    #[serde(default)]
    pub decision_post_blocked_quantity: i64,
    #[serde(default)]
    pub decision_intent_quantity: i64,
    #[serde(default)]
    pub decision_remaining_quantity: i64,
    #[serde(default)]
    pub decision_is_algorithm: bool,
    #[serde(default)]
    pub decision_recorded: bool,
}

impl GridRight {
    pub fn id_for(
        run_id: &str,
        cycle_id: &str,
        grid_index: i32,
        granted_at: NaiveDateTime,
    ) -> String {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{run_id}:{cycle_id}:right:{grid_index}:{granted_at}").as_bytes(),
        )
        .to_string()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn granted(
        run_id: &str,
        cycle_id: &str,
        symbol: &str,
        direction: Direction,
        grid_index: i32,
        grid_price: Decimal,
        granted_at: NaiveDateTime,
        capacity: GridRightCapacity,
    ) -> Self {
        let right_id = Self::id_for(run_id, cycle_id, grid_index, granted_at);
        Self {
            right_id,
            cycle_id: cycle_id.to_owned(),
            symbol: symbol.to_owned(),
            direction,
            grid_index,
            grid_price,
            granted_at,
            capacity,
            status: GridRightStatus::Granted,
            decision_id: None,
            reserved_budget: Decimal::ZERO,
            reserved_quantity: 0,
            exercised_budget: Decimal::ZERO,
            exercised_quantity: 0,
            decision_contract_version: 0,
            decision_gross_available_quantity: 0,
            decision_platform_residual_quantity: 0,
            decision_algorithm_authorized_quantity: 0,
            decision_algorithm_offered_quantity: 0,
            decision_pre_blocked_quantity: 0,
            decided_exercise_quantity: 0,
            decided_defer_quantity: 0,
            decision_platform_blocked_quantity: 0,
            decision_post_blocked_quantity: 0,
            decision_intent_quantity: 0,
            decision_remaining_quantity: 0,
            decision_is_algorithm: false,
            decision_recorded: false,
        }
    }

    /// Unconsumed authority still owned by this right. Executed amounts are
    /// historical facts and are therefore never part of a revocation.
    pub fn remaining_budget(&self) -> Decimal {
        (self.capacity.available_budget - self.exercised_budget).max(Decimal::ZERO)
    }

    pub fn remaining_quantity(&self) -> i64 {
        match self.direction {
            Direction::Buy if self.capacity.available_quantity > 0 => {
                (self.capacity.available_quantity - self.exercised_quantity).max(0)
            }
            _ => (self.capacity.eligible_quantity
                + self.capacity.t_plus_one_blocked_quantity
                + self.capacity.risk_blocked_quantity
                + self.capacity.no_profit_blocked_quantity
                - self.exercised_quantity)
                .max(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRequest {
    pub contract_version: u32,
    pub request_id: String,
    pub right: GridRight,
    pub context: GateContext,
    pub deterministic_seed: u64,
}

impl DecisionRequest {
    pub fn new(right: GridRight, context: GateContext) -> Self {
        Self::new_with_contract(right, context, WHOLE_UNIT_DECISION_CONTRACT_VERSION)
    }

    pub fn new_with_contract(
        right: GridRight,
        context: GateContext,
        contract_version: u32,
    ) -> Self {
        let digest = Sha256::digest(right.right_id.as_bytes());
        let deterministic_seed = u64::from_be_bytes(
            digest[..8]
                .try_into()
                .expect("SHA-256 always contains eight bytes"),
        );
        Self {
            contract_version,
            request_id: format!("decision:{}", right.right_id),
            right,
            context,
            deterministic_seed,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.contract_version,
            LEGACY_DECISION_CONTRACT_VERSION
                | WHOLE_UNIT_DECISION_CONTRACT_VERSION
                | DECISION_CONTRACT_VERSION
        ) {
            bail!("unsupported decision contract version")
        }
        let gross_available_quantity = if self.right.direction == Direction::Buy
            && self.right.capacity.available_quantity > 0
        {
            self.right.capacity.available_quantity
        } else {
            self.right.capacity.eligible_quantity
        };
        if self.context.right_id != self.right.right_id
            || self.context.direction != self.right.direction
            || self.context.grid_index != self.right.grid_index
            || self.context.grid_price != self.right.grid_price
            || self.context.available_budget != self.right.capacity.available_budget
            || self.context.lot_size <= 0
            || self.context.available_quantity % self.context.lot_size != 0
            || self.context.standard_quantity <= 0
            || self.context.standard_quantity % self.context.lot_size != 0
        {
            bail!("decision context does not match granted grid right")
        }
        if self.contract_version == LEGACY_DECISION_CONTRACT_VERSION {
            if self.context.available_quantity != gross_available_quantity {
                bail!("legacy decision context does not match granted capacity")
            }
            if self.context.funds_inventory.is_some() {
                bail!("legacy decision context cannot carry v4 resource evidence")
            }
        } else {
            let (authorized, residual) =
                whole_unit_partition(gross_available_quantity, self.context.standard_quantity)?;
            if self.context.gross_available_quantity != gross_available_quantity
                || self.context.platform_residual_quantity != residual
                || self.context.algorithm_authorized_quantity != authorized
            {
                bail!("decision context does not use the canonical whole-unit partition")
            }
            if self.contract_version == WHOLE_UNIT_DECISION_CONTRACT_VERSION {
                if self.context.available_quantity != authorized
                    || self.context.funds_inventory.is_some()
                {
                    bail!("decision contract v3 cannot reinterpret resource-aware fields")
                }
            } else {
                let resource = self
                    .context
                    .funds_inventory
                    .as_ref()
                    .context("decision contract v4 requires funds/inventory evidence")?;
                resource.validate_canonical(&self.context)?;
                let authorized_units = authorized / self.context.standard_quantity;
                let offered_units = resource.resource_units.min(authorized_units);
                if resource.schema_version != crate::gate::FUNDS_INVENTORY_CONTEXT_VERSION
                    || resource.mechanical_authorized_units != authorized_units
                    || resource.resource_units < 0
                    || resource.predecision_blocked_units != authorized_units - offered_units
                    || self.context.available_quantity
                        != offered_units * self.context.standard_quantity
                {
                    bail!("decision contract v4 resource partition is not canonical")
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionResponse {
    pub contract_version: u32,
    pub request_id: String,
    pub right_id: String,
    pub outcome: RightDecision,
    pub decision: GateDecision,
}

impl DecisionResponse {
    pub fn validate_for(&self, request: &DecisionRequest) -> Result<()> {
        self.validate_for_algorithm_status(request, true)
    }

    pub fn validate_for_algorithm_status(
        &self,
        request: &DecisionRequest,
        algorithm_succeeded: bool,
    ) -> Result<()> {
        request.validate()?;
        if self.contract_version != request.contract_version
            || !matches!(
                self.contract_version,
                LEGACY_DECISION_CONTRACT_VERSION
                    | WHOLE_UNIT_DECISION_CONTRACT_VERSION
                    | DECISION_CONTRACT_VERSION
            )
            || self.request_id != request.request_id
            || self.right_id != request.right.right_id
        {
            bail!("decision response identity does not match request")
        }
        let available = request.context.available_quantity;
        let (exercise_quantity, defer_quantity) = self.outcome.quantities();
        let alignment = if request.contract_version >= WHOLE_UNIT_DECISION_CONTRACT_VERSION {
            request.context.standard_quantity
        } else {
            request.context.lot_size
        };
        if exercise_quantity < 0
            || defer_quantity < 0
            || exercise_quantity
                .checked_add(defer_quantity)
                .is_none_or(|quantity| quantity != available)
            || exercise_quantity % alignment != 0
            || defer_quantity % alignment != 0
        {
            bail!("decision quantities must partition authorization in whole decision units")
        }
        match &self.outcome {
            RightDecision::Exercise {
                exercise_quantity, ..
            } if *exercise_quantity > 0 => {}
            RightDecision::Defer { defer_quantity } if *defer_quantity == available => {}
            _ => bail!("typed decision kind is inconsistent with exact quantities"),
        }
        if request.contract_version == LEGACY_DECISION_CONTRACT_VERSION {
            self.decision.validate_for_legacy_v2(&request.context)
        } else {
            if request.contract_version == DECISION_CONTRACT_VERSION {
                if algorithm_succeeded {
                    let target_quantity = request
                        .context
                        .funds_inventory
                        .as_ref()
                        .context("decision contract v4 requires funds/inventory evidence")?
                        .target_units
                        .checked_mul(request.context.standard_quantity)
                        .context("v4 target quantity overflow")?;
                    if exercise_quantity != target_quantity {
                        bail!("v4 typed exercise quantity differs from its canonical target")
                    }
                } else if exercise_quantity != 0
                    || defer_quantity != available
                    || self.decision.alpha_numerator != Some(0)
                    || self.decision.action != "SKIP"
                    || self.decision.model_name != "resource-aware-whole-q-failsafe"
                    || !self
                        .decision
                        .reason_codes
                        .iter()
                        .any(|reason| reason == "RESOURCE_FAIL_CLOSED")
                {
                    bail!("v4 failed algorithm response is not canonical fail-closed output")
                }
                self.decision
                    .validate_for_v4(&request.context, exercise_quantity)?;
            } else {
                self.decision.validate_for(&request.context)?;
            }
            let expected_action = if exercise_quantity > 0 {
                "EXECUTE"
            } else {
                "SKIP"
            };
            if self.decision.action != expected_action {
                bail!("gate action differs from the typed whole-unit outcome")
            }
            Ok(())
        }
    }
}

pub trait QuantDecisionAlgorithm: Send + Sync {
    fn manifest(&self) -> AlgorithmManifest {
        AlgorithmManifest {
            algorithm_name: "in-process-experimental".to_owned(),
            algorithm_version: "1".to_owned(),
            supported_contract_versions: vec![WHOLE_UNIT_DECISION_CONTRACT_VERSION],
            deterministic: true,
            supports_checkpoint: false,
            artifact_sha256: builtin_artifact_sha256("in-process-experimental-v1"),
            canonical_arguments: Vec::new(),
            environment_sha256: builtin_artifact_sha256("env-clear:protocol-v1"),
            platform_sha256: current_platform_sha256(),
        }
    }

    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse>;

    fn checkpoint(&self) -> Result<Option<AlgorithmCheckpoint>> {
        Ok(None)
    }
}

pub struct ResourceAwareWholeQAlgorithm;

impl ResourceAwareWholeQAlgorithm {
    pub fn safe_response(request: &DecisionRequest, reason: &str) -> Result<DecisionResponse> {
        request.validate()?;
        if request.contract_version != DECISION_CONTRACT_VERSION {
            bail!("resource-aware safe response requires decision contract v4")
        }
        let offered_units = request.context.available_quantity / request.context.standard_quantity;
        let response = DecisionResponse {
            contract_version: DECISION_CONTRACT_VERSION,
            request_id: request.request_id.clone(),
            right_id: request.right.right_id.clone(),
            outcome: RightDecision::Defer {
                defer_quantity: request.context.available_quantity,
            },
            decision: GateDecision {
                probability: request
                    .context
                    .funds_inventory
                    .as_ref()
                    .context("resource-aware safe response lacks v4 evidence")?
                    .market_score,
                alpha: Decimal::ZERO,
                alpha_numerator: Some(0),
                alpha_denominator: Some(offered_units),
                action: "SKIP".to_owned(),
                reason_codes: vec![reason.to_owned(), "RESOURCE_FAIL_CLOSED".to_owned()],
                model_name: "resource-aware-whole-q-failsafe".to_owned(),
                model_version: "1".to_owned(),
                input_snapshot_hash: context_hash_v4(&request.context)?,
                decided_at: request.context.current_time,
            },
        };
        response.validate_for_algorithm_status(request, false)?;
        Ok(response)
    }

    fn target_units(request: &DecisionRequest) -> Result<i64> {
        let context = &request.context;
        let resource = context
            .funds_inventory
            .as_ref()
            .context("resource-aware algorithm requires v4 evidence")?;
        let offered_units = context.available_quantity / context.standard_quantity;
        let expected_pass = resource.history_bars >= resource.long_window
            && resource.market_score >= resource.market_threshold
            && (resource.trend_strength > Decimal::ZERO
                || resource.location_strength > Decimal::ZERO);
        if resource.market_signal_passed != expected_pass {
            bail!("resource-aware market gate evidence is inconsistent")
        }
        let target = if !expected_pass || offered_units == 0 {
            0
        } else {
            let scaled = Decimal::from(offered_units)
                .checked_mul(resource.pace)
                .context("resource-aware target pace overflow")?
                .floor()
                .to_string()
                .parse::<i64>()
                .context("resource-aware target units are not representable")?;
            offered_units.min(2).min(scaled.max(1))
        };
        if resource.target_units != target {
            bail!("resource-aware target quantity is not canonical")
        }
        Ok(target)
    }
}

impl QuantDecisionAlgorithm for ResourceAwareWholeQAlgorithm {
    fn manifest(&self) -> AlgorithmManifest {
        AlgorithmManifest {
            algorithm_name: "resource-aware-whole-q".to_owned(),
            algorithm_version: "1".to_owned(),
            supported_contract_versions: vec![DECISION_CONTRACT_VERSION],
            deterministic: true,
            supports_checkpoint: false,
            artifact_sha256: builtin_artifact_sha256("resource-aware-whole-q-v1"),
            canonical_arguments: Vec::new(),
            environment_sha256: builtin_artifact_sha256("in-process:no-environment"),
            platform_sha256: current_platform_sha256(),
        }
    }

    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
        request.validate()?;
        if request.contract_version != DECISION_CONTRACT_VERSION {
            bail!("resource-aware algorithm requires decision contract v4")
        }
        let resource = request
            .context
            .funds_inventory
            .as_ref()
            .context("resource-aware algorithm requires v4 evidence")?;
        let target_units = Self::target_units(request)?;
        let offered_units = request.context.available_quantity / request.context.standard_quantity;
        let exercise_quantity = target_units
            .checked_mul(request.context.standard_quantity)
            .context("resource-aware exercise quantity overflow")?;
        let defer_quantity = request
            .context
            .available_quantity
            .checked_sub(exercise_quantity)
            .context("resource-aware defer quantity underflow")?;
        let alpha = if offered_units == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(target_units)
                .checked_div(Decimal::from(offered_units))
                .context("resource-aware alpha is not representable")?
        };
        let mut reasons = Vec::new();
        if resource.history_bars < resource.long_window {
            reasons.push("RESOURCE_SIGNAL_WARMUP".to_owned());
        } else if !resource.market_signal_passed {
            reasons.push("RESOURCE_MARKET_THRESHOLD_NOT_MET".to_owned());
        } else {
            reasons.push("RESOURCE_MARKET_SIGNAL_PASSED".to_owned());
        }
        if resource.predecision_blocked_units > 0 {
            reasons.push("RESOURCE_CAP_APPLIED".to_owned());
        }
        if target_units > 0 {
            reasons.push("RESOURCE_TARGET_EXERCISE".to_owned());
        } else {
            reasons.push("RESOURCE_TARGET_SKIP".to_owned());
        }
        let decision = GateDecision {
            probability: resource.market_score,
            alpha,
            alpha_numerator: Some(target_units),
            alpha_denominator: Some(offered_units),
            action: if exercise_quantity > 0 {
                "EXECUTE".to_owned()
            } else {
                "SKIP".to_owned()
            },
            reason_codes: reasons,
            model_name: "resource-aware-whole-q".to_owned(),
            model_version: "1".to_owned(),
            input_snapshot_hash: context_hash_v4(&request.context)?,
            decided_at: request.context.current_time,
        };
        let outcome = if exercise_quantity > 0 {
            RightDecision::Exercise {
                exercise_quantity,
                defer_quantity,
            }
        } else {
            RightDecision::Defer { defer_quantity }
        };
        let response = DecisionResponse {
            contract_version: request.contract_version,
            request_id: request.request_id.clone(),
            right_id: request.right.right_id.clone(),
            outcome,
            decision,
        };
        response.validate_for(request)?;
        Ok(response)
    }
}

pub fn algorithm_from_config(config: &Config) -> Result<Box<dyn QuantDecisionAlgorithm>> {
    if config.gate.kind == "resource_aware" {
        Ok(Box::new(ResourceAwareWholeQAlgorithm))
    } else {
        Ok(Box::new(GateAlgorithmAdapter::new(
            crate::gate::from_config(config)?,
        )))
    }
}

const MAX_ALGORITHM_RESPONSE_BYTES: u64 = 1_048_576;

/// Killable process boundary for production decision algorithms.
///
/// The executable must implement `--gridedge-manifest-v1` and
/// `--gridedge-decide-v1`. Both commands emit one JSON document on stdout;
/// the decision command receives a `DecisionRequest` JSON document on stdin.
pub struct ProcessAlgorithm {
    executable: PathBuf,
    _artifact_dir: tempfile::TempDir,
    arguments: Vec<String>,
    timeout: Duration,
    manifest: AlgorithmManifest,
}

impl ProcessAlgorithm {
    pub fn connect(
        executable: impl AsRef<Path>,
        arguments: Vec<String>,
        timeout: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() {
            bail!("process algorithm timeout must be positive")
        }
        #[cfg(unix)]
        if unsafe { libc::geteuid() } == 0 {
            bail!("process algorithms refuse to run as root because process limits are bypassable")
        }
        let source = std::fs::canonicalize(executable.as_ref())?;
        let artifact_sha256 = sha256_file(&source)?;
        let artifact_dir = tempfile::tempdir()?;
        let executable = artifact_dir
            .path()
            .join(format!("artifact-{artifact_sha256}"));
        std::fs::copy(&source, &executable)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o500))?;
        }
        // Registration happens outside the market-data critical path and may
        // contend with other startup work. Keep it bounded, but give the
        // one-time manifest handshake more headroom than each decision.
        let manifest_timeout = timeout.saturating_mul(3);
        let raw = invoke_process(
            &executable,
            &arguments,
            "--gridedge-manifest-v1",
            None,
            manifest_timeout,
        )?;
        let mut manifest: AlgorithmManifest =
            serde_json::from_slice(&raw).map_err(|error| anyhow::anyhow!(error))?;
        if manifest.algorithm_name.trim().is_empty()
            || manifest.algorithm_version.trim().is_empty()
            || !supported_contract_versions_are_canonical(&manifest.supported_contract_versions)
            || !manifest.deterministic
        {
            bail!("process algorithm returned an incompatible manifest")
        }
        if manifest.supports_checkpoint {
            bail!(
                "stateful process algorithms are not supported until checkpoint/restore is durable"
            )
        }
        manifest.artifact_sha256 = artifact_sha256;
        manifest.canonical_arguments = arguments.clone();
        manifest.environment_sha256 =
            builtin_artifact_sha256("env-clear:GRIDEDGE_PROTOCOL_VERSION=1");
        manifest.platform_sha256 = current_platform_sha256();
        Ok(Self {
            executable,
            _artifact_dir: artifact_dir,
            arguments,
            timeout,
            manifest,
        })
    }
}

impl QuantDecisionAlgorithm for ProcessAlgorithm {
    fn manifest(&self) -> AlgorithmManifest {
        self.manifest.clone()
    }

    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
        request.validate()?;
        if sha256_file(&self.executable)? != self.manifest.artifact_sha256 {
            bail!("algorithm artifact changed after registration")
        }
        let input = serde_json::to_vec(request)?;
        let raw = invoke_process(
            &self.executable,
            &self.arguments,
            "--gridedge-decide-v1",
            Some(&input),
            self.timeout,
        )?;
        let response: DecisionResponse = serde_json::from_slice(&raw)?;
        response.validate_for(request)?;
        Ok(response)
    }
}

fn invoke_process(
    executable: &Path,
    arguments: &[String],
    operation: &str,
    input: Option<&[u8]>,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut stdout_file = tempfile::NamedTempFile::new()?;
    let mut stderr_file = tempfile::NamedTempFile::new()?;
    let mut stdin_file = if let Some(input) = input {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(input)?;
        Some(file)
    } else {
        None
    };
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .arg(operation)
        .env_clear()
        .env("GRIDEDGE_PROTOCOL_VERSION", "1")
        .stdin(if let Some(file) = stdin_file.as_mut() {
            Stdio::from(file.reopen()?)
        } else {
            Stdio::null()
        })
        .stdout(Stdio::from(stdout_file.reopen()?))
        .stderr(Stdio::from(stderr_file.reopen()?));
    #[cfg(unix)]
    {
        command.process_group(0);
        let cpu_seconds = timeout.as_secs().saturating_add(1).max(1);
        // SAFETY: this closure only calls async-signal-safe setrlimit between fork and exec.
        unsafe {
            command.pre_exec(move || {
                let file_limit = libc::rlimit {
                    rlim_cur: MAX_ALGORITHM_RESPONSE_BYTES + 16_384,
                    rlim_max: MAX_ALGORITHM_RESPONSE_BYTES + 16_384,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &file_limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let cpu_limit = libc::rlimit {
                    rlim_cur: cpu_seconds,
                    rlim_max: cpu_seconds,
                };
                if libc::setrlimit(libc::RLIMIT_CPU, &cpu_limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let process_limit = libc::rlimit {
                    rlim_cur: 1,
                    rlim_max: 1,
                };
                if libc::setrlimit(libc::RLIMIT_NPROC, &process_limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                #[cfg(target_os = "linux")]
                {
                    let address_space_limit = libc::rlimit {
                        rlim_cur: 512 * 1024 * 1024,
                        rlim_max: 512 * 1024 * 1024,
                    };
                    if libc::setrlimit(libc::RLIMIT_AS, &address_space_limit) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
    }
    let mut child = command
        .spawn()
        .map_err(|error| anyhow::anyhow!("failed to spawn algorithm process: {error}"))?;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_process_group(&mut child);
            let _ = child.kill();
            let _ = child.wait();
            bail!("algorithm process timed out")
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    // A well-behaved parent may exit while descendants retain output handles.
    // Always terminate the isolated group before reading regular files.
    terminate_process_group(&mut child);
    let mut stdout = Vec::new();
    stdout_file
        .as_file_mut()
        .take(MAX_ALGORITHM_RESPONSE_BYTES + 1)
        .read_to_end(&mut stdout)?;
    let mut stderr = Vec::new();
    stderr_file
        .as_file_mut()
        .take(16_385)
        .read_to_end(&mut stderr)?;
    if stdout.len() as u64 > MAX_ALGORITHM_RESPONSE_BYTES {
        bail!("algorithm response exceeds one MiB")
    }
    if !status.success() {
        bail!(
            "algorithm process exited with {status}: {}",
            String::from_utf8_lossy(&stderr)
        )
    }
    Ok(stdout)
}

fn terminate_process_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        let process_group = -(child.id() as i32);
        // SAFETY: negative PID targets only the process group created for this child.
        unsafe {
            libc::kill(process_group, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgorithmManifest {
    pub algorithm_name: String,
    pub algorithm_version: String,
    pub supported_contract_versions: Vec<u32>,
    pub deterministic: bool,
    pub supports_checkpoint: bool,
    /// Platform-measured identity. A subprocess cannot self-declare this value.
    #[serde(default)]
    pub artifact_sha256: String,
    /// Exact argument vector that affects the algorithm process.
    #[serde(default)]
    pub canonical_arguments: Vec<String>,
    /// Hash of the deliberately minimal environment supplied by the platform.
    #[serde(default)]
    pub environment_sha256: String,
    /// Hash of the GridEdge executable hosting the algorithm contract.
    #[serde(default)]
    pub platform_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmCheckpoint {
    pub algorithm_name: String,
    pub algorithm_version: String,
    pub sequence_number: i64,
    pub payload: Vec<u8>,
    pub sha256: String,
}

pub struct GateAlgorithmAdapter {
    policy: Box<dyn GatePolicy>,
}

impl GateAlgorithmAdapter {
    pub fn new(policy: Box<dyn GatePolicy>) -> Self {
        Self { policy }
    }
}

impl QuantDecisionAlgorithm for GateAlgorithmAdapter {
    fn manifest(&self) -> AlgorithmManifest {
        AlgorithmManifest {
            algorithm_name: "gate-policy-adapter".to_owned(),
            algorithm_version: "1".to_owned(),
            supported_contract_versions: vec![WHOLE_UNIT_DECISION_CONTRACT_VERSION],
            deterministic: true,
            supports_checkpoint: false,
            artifact_sha256: builtin_artifact_sha256("gate-policy-adapter-v1"),
            canonical_arguments: Vec::new(),
            environment_sha256: builtin_artifact_sha256("in-process:no-environment"),
            platform_sha256: current_platform_sha256(),
        }
    }

    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
        request.validate()?;
        let response = DecisionResponse {
            contract_version: request.contract_version,
            request_id: request.request_id.clone(),
            right_id: request.right.right_id.clone(),
            decision: self.policy.evaluate(&request.context)?,
            outcome: RightDecision::Defer {
                defer_quantity: request.context.available_quantity,
            },
        };
        let response = DecisionResponse {
            outcome: RightDecision::from_gate(
                &response.decision,
                request.context.available_quantity,
                request.context.standard_quantity,
            ),
            ..response
        };
        response.validate_for(request)?;
        Ok(response)
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn builtin_artifact_sha256(identity: &str) -> String {
    hex::encode(Sha256::digest(identity.as_bytes()))
}

fn current_platform_sha256() -> String {
    static PLATFORM_SHA256: OnceLock<String> = OnceLock::new();
    PLATFORM_SHA256
        .get_or_init(|| {
            std::env::current_exe()
                .ok()
                .and_then(|path| sha256_file(&path).ok())
                .unwrap_or_else(|| builtin_artifact_sha256("unavailable-platform-artifact"))
        })
        .clone()
}
