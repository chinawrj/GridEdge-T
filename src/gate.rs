use crate::{config::Config, domain::Direction};
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const FUNDS_INVENTORY_CONTEXT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundsInventoryContextV1 {
    pub schema_version: u32,
    pub feature_head_sequence: i64,
    pub feature_bar_ids: Vec<String>,
    pub feature_bars_sha256: String,
    pub history_bars: u32,
    pub short_window: u32,
    pub long_window: u32,
    pub trade_levels: i32,
    #[serde(with = "rust_decimal::serde::str")]
    pub weight_depth: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub weight_trend: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub weight_location: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub market_threshold: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub depth: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub recent_return_5: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub vwap_deviation_20: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub range_scale_20: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub trend_strength: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub location_strength: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub market_score: Decimal,
    pub market_signal_passed: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub cash_available: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub cash_frozen: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_free_cash: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub spendable_cash: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub adverse_buy_price: Option<Decimal>,
    pub cash_affordable_units: i64,
    pub position_total: i64,
    pub pending_buy_quantity: i64,
    pub position_exposure_quantity: i64,
    pub position_sellable: i64,
    pub position_today_bought: i64,
    pub position_frozen_sell: i64,
    pub max_position: i64,
    pub min_base_position: i64,
    pub target_position: i64,
    pub position_headroom_units: i64,
    pub target_headroom_units: i64,
    pub sellable_inventory_units: i64,
    pub mechanical_authorized_units: i64,
    pub resource_units: i64,
    pub predecision_blocked_units: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub resource_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub resource_multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub pace: Decimal,
    pub target_units: i64,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funds_inventory: Option<FundsInventoryContextV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateDecision {
    #[serde(with = "rust_decimal::serde::str")]
    pub probability: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub alpha: Decimal,
    /// Exact whole-unit audit ratio for decision contract v4. The integer
    /// outcome is authoritative; this pair prevents consumers from trying to
    /// recover it from a rounded Decimal alpha.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alpha_numerator: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alpha_denominator: Option<i64>,
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

    pub fn validate_for_v4(&self, context: &GateContext, exercise_quantity: i64) -> Result<()> {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&self.probability)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.alpha)
        {
            bail!("gate returned value outside [0, 1]")
        }
        let denominator = context.available_quantity / context.standard_quantity;
        let numerator = self
            .alpha_numerator
            .context("v4 decision lacks exact alpha numerator")?;
        let recorded_denominator = self
            .alpha_denominator
            .context("v4 decision lacks exact alpha denominator")?;
        let expected_numerator = exercise_quantity / context.standard_quantity;
        if numerator < 0
            || recorded_denominator < 0
            || recorded_denominator != denominator
            || numerator != expected_numerator
            || numerator > recorded_denominator
            || (recorded_denominator == 0 && numerator != 0)
        {
            bail!("v4 decision exact alpha ratio is invalid")
        }
        let exact_alpha = if recorded_denominator == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(numerator)
                .checked_div(Decimal::from(recorded_denominator))
                .ok_or_else(|| anyhow::anyhow!("v4 exact alpha is not representable"))?
        };
        if self.alpha != exact_alpha {
            bail!("v4 decision alpha differs from its exact unit ratio")
        }
        self.validate_for_hash(context, context_hash_v4(context)?, None)
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

impl FundsInventoryContextV1 {
    pub fn validate_canonical(&self, context: &GateContext) -> Result<()> {
        let half = Config::decimal("0.50");
        let expected_depth = Decimal::from(
            context
                .grid_index
                .checked_abs()
                .context("v4 grid index is not representable")?,
        )
        .checked_div(Decimal::from(self.trade_levels))
        .context("v4 depth is not representable")?;
        if self.schema_version != FUNDS_INVENTORY_CONTEXT_VERSION
            || self.short_window != 5
            || self.long_window != 20
            || self.trade_levels <= 0
            || self.weight_depth != Config::decimal("0.35")
            || self.weight_trend != Config::decimal("0.40")
            || self.weight_location != Config::decimal("0.25")
            || self.market_threshold != Config::decimal("0.60")
            || self.range_scale_20 < Config::decimal("0.005")
            || self.history_bars > self.long_window
            || self.feature_bar_ids.len() != self.history_bars as usize
            || self.feature_bar_ids.iter().any(|id| id.trim().is_empty())
            || self.feature_bars_sha256.len() != 64
            || !self
                .feature_bars_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.depth != expected_depth
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.trend_strength)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.location_strength)
            || self.mechanical_authorized_units < 0
            || self.resource_units < 0
            || self.pending_buy_quantity < 0
            || self
                .position_total
                .checked_add(self.pending_buy_quantity)
                .is_none_or(|quantity| quantity != self.position_exposure_quantity)
        {
            bail!("v4 funds/inventory evidence has non-canonical constants")
        }
        let signed_trend = match context.direction {
            Direction::Buy => Decimal::ZERO
                .checked_sub(self.recent_return_5)
                .context("v4 BUY trend is not representable")?,
            Direction::Sell => self.recent_return_5,
        };
        let signed_location = match context.direction {
            Direction::Buy => Decimal::ZERO
                .checked_sub(self.vwap_deviation_20)
                .context("v4 BUY location is not representable")?,
            Direction::Sell => self.vwap_deviation_20,
        };
        let expected_trend = signed_trend
            .checked_div(self.range_scale_20)
            .context("v4 trend strength is not representable")?
            .clamp(Decimal::ZERO, Decimal::ONE);
        let expected_location = signed_location
            .checked_div(self.range_scale_20)
            .context("v4 location strength is not representable")?
            .clamp(Decimal::ZERO, Decimal::ONE);
        let expected_score = self
            .depth
            .checked_mul(self.weight_depth)
            .and_then(|depth| {
                self.trend_strength
                    .checked_mul(self.weight_trend)
                    .and_then(|trend| depth.checked_add(trend))
            })
            .and_then(|score| {
                self.location_strength
                    .checked_mul(self.weight_location)
                    .and_then(|location| score.checked_add(location))
            })
            .context("v4 market score is not representable")?;
        let expected_pass = self.history_bars == self.long_window
            && expected_score >= self.market_threshold
            && (expected_trend > Decimal::ZERO || expected_location > Decimal::ZERO);
        let expected_ratio = Decimal::from(self.resource_units)
            .checked_div(Decimal::from(
                self.resource_units
                    .checked_add(4)
                    .context("v4 resource ratio denominator overflow")?,
            ))
            .context("v4 resource ratio is not representable")?;
        let expected_multiplier = half
            .checked_add(
                half.checked_mul(expected_ratio)
                    .context("v4 resource multiplier is not representable")?,
            )
            .context("v4 resource multiplier is not representable")?;
        let expected_pace = if expected_pass {
            expected_score
                .checked_mul(expected_multiplier)
                .context("v4 pace is not representable")?
                .min(half)
        } else {
            Decimal::ZERO
        };
        let offered_units = self.resource_units.min(self.mechanical_authorized_units);
        let expected_target = if !expected_pass || offered_units == 0 {
            0
        } else {
            let paced = Decimal::from(offered_units)
                .checked_mul(expected_pace)
                .context("v4 target pace is not representable")?
                .floor()
                .to_string()
                .parse::<i64>()
                .context("v4 target units are not representable")?;
            offered_units.min(2).min(paced.max(1))
        };
        if self.trend_strength != expected_trend
            || self.location_strength != expected_location
            || self.market_score != expected_score
            || self.market_signal_passed != expected_pass
            || self.resource_ratio != expected_ratio
            || self.resource_multiplier != expected_multiplier
            || self.pace != expected_pace
            || self.predecision_blocked_units
                != self
                    .mechanical_authorized_units
                    .checked_sub(offered_units)
                    .context("v4 resource partition underflow")?
            || self.target_units != expected_target
        {
            bail!("v4 funds/inventory evidence is not canonically derived")
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

/// Exact Decision Contract v3 serialization shape. New context fields must
/// never change the hash of an already journaled v3 request.
#[derive(Serialize)]
struct LegacyV3GateContext {
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
    gross_available_quantity: i64,
    platform_residual_quantity: i64,
    algorithm_authorized_quantity: i64,
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

impl From<&GateContext> for LegacyV3GateContext {
    fn from(context: &GateContext) -> Self {
        Self {
            right_id: context.right_id.clone(),
            symbol: context.symbol.clone(),
            direction: context.direction,
            grid_index: context.grid_index,
            grid_price: context.grid_price,
            current_price: context.current_price,
            current_time: context.current_time,
            available_budget: context.available_budget,
            available_quantity: context.available_quantity,
            gross_available_quantity: context.gross_available_quantity,
            platform_residual_quantity: context.platform_residual_quantity,
            algorithm_authorized_quantity: context.algorithm_authorized_quantity,
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
        }
    }
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
        alpha_numerator: None,
        alpha_denominator: None,
        action: current_discrete_action(context, alpha).to_owned(),
        reason_codes: reasons,
        model_name: model.to_owned(),
        model_version: "1".to_owned(),
        input_snapshot_hash: hash,
        decided_at: context.current_time,
    })
}

pub fn context_hash(context: &GateContext) -> Result<String> {
    let json = serde_json::to_vec(&LegacyV3GateContext::from(context))?;
    Ok(hex::encode(Sha256::digest(json)))
}

pub fn context_hash_v4(context: &GateContext) -> Result<String> {
    if context.funds_inventory.is_none() {
        bail!("decision contract v4 requires funds/inventory context")
    }
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
