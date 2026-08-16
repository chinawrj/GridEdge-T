use crate::{config::Config, domain::Direction};
use anyhow::{bail, Result};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateContext {
    pub right_id: String,
    pub symbol: String,
    pub direction: Direction,
    pub grid_index: i32,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub current_price: Decimal,
    pub current_time: NaiveDateTime,
    #[serde(with = "rust_decimal::serde::str")]
    pub available_budget: Decimal,
    /// Whole-share-unit quantity visible to the algorithm in contract v3.
    pub available_quantity: i64,
    /// Exact lot/tranche-backed capacity before the platform removes a
    /// sub-standard-quantity residual.
    #[serde(default)]
    pub gross_available_quantity: i64,
    /// `gross_available_quantity mod standard_quantity`. This is platform
    /// inventory measured in shares, never an algorithm Defer decision.
    #[serde(default)]
    pub platform_residual_quantity: i64,
    /// `gross_available_quantity - platform_residual_quantity`; equal to
    /// `available_quantity` for contract v3.
    #[serde(default)]
    pub algorithm_authorized_quantity: i64,
    pub lot_size: i64,
    pub standard_quantity: i64,
    pub eligible_lot_ids: Vec<String>,
    pub accumulated_grid_indices: Vec<i32>,
    #[serde(with = "rust_decimal::serde::str")]
    pub deployed_budget: Decimal,
    pub current_position: i64,
    pub sellable_position: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub recent_return: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub vwap_deviation: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub volatility: Decimal,
    pub market_regime: String,
    pub cycle_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateDecision {
    #[serde(with = "rust_decimal::serde::str")]
    pub probability: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub alpha: Decimal,
    pub action: String,
    pub reason_codes: Vec<String>,
    pub model_name: String,
    pub model_version: String,
    pub input_snapshot_hash: String,
    pub decided_at: NaiveDateTime,
}

impl GateDecision {
    pub fn validate_for(&self, context: &GateContext) -> Result<()> {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&self.probability)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.alpha)
        {
            bail!("gate returned value outside [0, 1]")
        }
        self.validate_for_hash(context, context_hash(context)?, None)
    }

    pub fn validate_for_legacy_v2(&self, context: &GateContext) -> Result<()> {
        let expected_action = if self.alpha.is_zero() {
            "SKIP"
        } else {
            "EXECUTE"
        };
        self.validate_for_hash(
            context,
            legacy_v2_context_hash(context)?,
            Some(expected_action),
        )
    }

    fn validate_for_hash(
        &self,
        context: &GateContext,
        expected_hash: String,
        expected_action: Option<&str>,
    ) -> Result<()> {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&self.probability)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.alpha)
        {
            bail!("gate returned value outside [0, 1]")
        }
        if self.model_name.trim().is_empty()
            || self.model_version.trim().is_empty()
            || self.reason_codes.is_empty()
        {
            bail!("gate decision is missing audit metadata")
        }
        if !matches!(self.action.as_str(), "EXECUTE" | "SKIP") {
            bail!("gate action is not a supported audit value")
        }
        if expected_action.is_some_and(|expected| self.action != expected) {
            bail!("gate action is inconsistent with the discrete whole-unit outcome")
        }
        if self.input_snapshot_hash != expected_hash {
            bail!("gate decision input hash does not match request")
        }
        if self.decided_at != context.current_time {
            bail!("gate decision time does not match replay clock")
        }
        Ok(())
    }
}

fn current_discrete_action(context: &GateContext, alpha: Decimal) -> &'static str {
    if context.standard_quantity <= 0 || context.algorithm_authorized_quantity <= 0 {
        return "SKIP";
    }
    let authorized_units = context.algorithm_authorized_quantity / context.standard_quantity;
    let exercise_units = (Decimal::from(authorized_units) * alpha).floor();
    if exercise_units > Decimal::ZERO {
        "EXECUTE"
    } else {
        "SKIP"
    }
}

#[derive(Serialize)]
struct LegacyV2GateContext {
    right_id: String,
    symbol: String,
    direction: Direction,
    grid_index: i32,
    #[serde(with = "rust_decimal::serde::str")]
    grid_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    current_price: Decimal,
    current_time: NaiveDateTime,
    #[serde(with = "rust_decimal::serde::str")]
    available_budget: Decimal,
    available_quantity: i64,
    lot_size: i64,
    standard_quantity: i64,
    eligible_lot_ids: Vec<String>,
    accumulated_grid_indices: Vec<i32>,
    #[serde(with = "rust_decimal::serde::str")]
    deployed_budget: Decimal,
    current_position: i64,
    sellable_position: i64,
    #[serde(with = "rust_decimal::serde::str")]
    recent_return: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    vwap_deviation: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    volatility: Decimal,
    market_regime: String,
    cycle_id: String,
}

pub fn legacy_v2_context_hash(context: &GateContext) -> Result<String> {
    let legacy = LegacyV2GateContext {
        right_id: context.right_id.clone(),
        symbol: context.symbol.clone(),
        direction: context.direction,
        grid_index: context.grid_index,
        grid_price: context.grid_price,
        current_price: context.current_price,
        current_time: context.current_time,
        available_budget: context.available_budget,
        available_quantity: context.available_quantity,
        lot_size: context.lot_size,
        standard_quantity: context.standard_quantity,
        eligible_lot_ids: context.eligible_lot_ids.clone(),
        accumulated_grid_indices: context.accumulated_grid_indices.clone(),
        deployed_budget: context.deployed_budget,
        current_position: context.current_position,
        sellable_position: context.sellable_position,
        recent_return: context.recent_return,
        vwap_deviation: context.vwap_deviation,
        volatility: context.volatility,
        market_regime: context.market_regime.clone(),
        cycle_id: context.cycle_id.clone(),
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&legacy)?)))
}

pub trait GatePolicy: Send + Sync {
    fn evaluate(&self, context: &GateContext) -> Result<GateDecision>;
}

fn decision(
    context: &GateContext,
    probability: Decimal,
    alpha: Decimal,
    reasons: Vec<String>,
    model: &str,
) -> Result<GateDecision> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&probability)
        || !(Decimal::ZERO..=Decimal::ONE).contains(&alpha)
    {
        bail!("gate returned value outside [0, 1]")
    }
    let hash = context_hash(context)?;
    Ok(GateDecision {
        probability,
        alpha,
        action: current_discrete_action(context, alpha).to_owned(),
        reason_codes: reasons,
        model_name: model.to_owned(),
        model_version: "1".to_owned(),
        input_snapshot_hash: hash,
        decided_at: context.current_time,
    })
}

pub fn context_hash(context: &GateContext) -> Result<String> {
    let json = serde_json::to_vec(context)?;
    Ok(hex::encode(Sha256::digest(json)))
}

pub struct AlwaysExecuteGate;

impl GatePolicy for AlwaysExecuteGate {
    fn evaluate(&self, context: &GateContext) -> Result<GateDecision> {
        decision(
            context,
            Decimal::ONE,
            Decimal::ONE,
            vec!["MECHANICAL_BASELINE".to_owned()],
            "always-execute",
        )
    }
}

pub struct FixedGate {
    pub probability: Decimal,
    pub alpha: Decimal,
}

impl GatePolicy for FixedGate {
    fn evaluate(&self, context: &GateContext) -> Result<GateDecision> {
        decision(
            context,
            self.probability,
            self.alpha,
            vec!["FIXED_CONFIGURATION".to_owned()],
            "fixed",
        )
    }
}

pub struct SimpleRuleGate;

impl GatePolicy for SimpleRuleGate {
    fn evaluate(&self, context: &GateContext) -> Result<GateDecision> {
        let threshold = Config::decimal("0.01");
        let mut score = 2_i32;
        let mut reasons = Vec::new();
        if context.direction == Direction::Buy {
            if context.recent_return < -threshold {
                score += 1;
                reasons.push("RECENT_WEAKNESS".to_owned());
            }
            if context.vwap_deviation < Decimal::ZERO {
                score += 1;
                reasons.push("BELOW_VWAP".to_owned());
            }
            if context.market_regime == "DOWN" {
                score -= 1;
                reasons.push("MARKET_DOWN_RISK".to_owned());
            }
        } else if context.recent_return > threshold {
            score += 1;
            reasons.push("RECENT_STRENGTH".to_owned());
        }
        if context.volatility > Config::decimal("0.05") {
            score -= 1;
            reasons.push("HIGH_VOLATILITY".to_owned());
        }
        let score = score.clamp(0, 4);
        let alpha = Decimal::from(score) / Decimal::from(4);
        if reasons.is_empty() {
            reasons.push("NEUTRAL_RULES".to_owned());
        }
        decision(context, alpha, alpha, reasons, "simple-rule")
    }
}

pub fn from_config(config: &Config) -> Result<Box<dyn GatePolicy>> {
    match config.gate.kind.as_str() {
        "always_execute" => Ok(Box::new(AlwaysExecuteGate)),
        "fixed" => Ok(Box::new(FixedGate {
            probability: config.gate.probability,
            alpha: config.gate.alpha,
        })),
        "simple_rule" => Ok(Box::new(SimpleRuleGate)),
        other => bail!("unknown gate kind {other}"),
    }
}
