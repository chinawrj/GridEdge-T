use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, path::Path, str::FromStr};

pub const MAX_SHARE_QUANTITY: i64 = 1_000_000_000_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapitalInventorySettings {
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_free_cash: Decimal,
    pub target_position: i64,
    /// Deploy the initial cash/stock balance once, from the first reviewed
    /// market bar. False is omitted so historical configuration hashes remain
    /// byte-for-byte compatible.
    #[serde(default, skip_serializing_if = "is_false")]
    pub deploy_initial_balance: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateSettings {
    pub kind: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub probability: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub alpha: Decimal,
    pub failure_mode: String,
    #[serde(default = "default_gate_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capital_inventory: Option<CapitalInventorySettings>,
}

fn default_gate_timeout_ms() -> u64 {
    100
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperSettings {
    pub reject_probability_bps: u16,
    pub partial_fill_bps: u16,
    pub latency_ms: u64,
    pub seed: u64,
    #[serde(default)]
    pub hold_open_probability_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub anchor_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_ratio: Decimal,
    pub trade_levels: i32,
    pub boundary_levels: i32,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub standard_budget: Decimal,
    /// Current mechanical authorization unit. New runs must use a positive
    /// board-lot multiple here; `standard_budget` remains only for replaying
    /// historical budget-denominated runs.
    #[serde(default)]
    pub standard_quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_cash: Decimal,
    pub initial_position: i64,
    pub initial_sellable: i64,
    pub max_position: i64,
    pub min_base_position: i64,
    pub lot_size: i64,
    pub price_scale: u32,
    #[serde(with = "rust_decimal::serde::str")]
    pub hysteresis_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_commission: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub sell_tax_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_rate: Decimal,
    pub gate: GateSettings,
    pub paper: PaperSettings,
    pub ambiguity_policy: String,
    pub database: String,
    pub config_version: String,
}

#[derive(Serialize)]
struct LegacyPaperSettings {
    reject_probability_bps: u16,
    partial_fill_bps: u16,
    latency_ms: u64,
    seed: u64,
}

#[derive(Serialize)]
struct LegacyConfigHash<'a> {
    symbol: &'a str,
    #[serde(with = "rust_decimal::serde::str")]
    anchor_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    grid_ratio: Decimal,
    trade_levels: i32,
    boundary_levels: i32,
    #[serde(with = "rust_decimal::serde::str")]
    standard_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    initial_cash: Decimal,
    initial_position: i64,
    initial_sellable: i64,
    max_position: i64,
    min_base_position: i64,
    lot_size: i64,
    price_scale: u32,
    #[serde(with = "rust_decimal::serde::str")]
    hysteresis_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    commission_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    minimum_commission: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    sell_tax_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    slippage_rate: Decimal,
    gate: &'a GateSettings,
    paper: LegacyPaperSettings,
    ambiguity_policy: &'a str,
    database: &'a str,
    config_version: &'a str,
}

/// Exact pre-quantity configuration shape. Struct field order is part of the
/// historical SHA-256 contract because older ledgers hashed the serialized
/// struct bytes rather than a sorted JSON object.
#[derive(Serialize)]
struct LegacyQuantityConfigHash<'a> {
    symbol: &'a str,
    #[serde(with = "rust_decimal::serde::str")]
    anchor_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    grid_ratio: Decimal,
    trade_levels: i32,
    boundary_levels: i32,
    #[serde(with = "rust_decimal::serde::str")]
    standard_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    initial_cash: Decimal,
    initial_position: i64,
    initial_sellable: i64,
    max_position: i64,
    min_base_position: i64,
    lot_size: i64,
    price_scale: u32,
    #[serde(with = "rust_decimal::serde::str")]
    hysteresis_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    commission_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    minimum_commission: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    sell_tax_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    slippage_rate: Decimal,
    gate: &'a GateSettings,
    paper: &'a PaperSettings,
    ambiguity_policy: &'a str,
    database: &'a str,
    config_version: &'a str,
}

impl Config {
    pub fn from_snapshot_payload(payload: &serde_json::Value) -> Result<Self> {
        let config: Self =
            serde_json::from_value(payload.clone()).context("invalid configuration snapshot")?;
        config.validate()?;
        if let Some(expected_hash) = payload
            .get("_content_sha256")
            .and_then(|value| value.as_str())
        {
            let has_hold_open = payload
                .get("paper")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|paper| paper.contains_key("hold_open_probability_bps"));
            let has_standard_quantity = payload.get("standard_quantity").is_some();
            let actual_hash = if has_standard_quantity {
                config.content_sha256()?
            } else if has_hold_open {
                config.legacy_content_sha256_without_standard_quantity(true)?
            } else {
                config.legacy_content_sha256_without_hold_open()?
            };
            if actual_hash != expected_hash {
                bail!("configuration snapshot content hash is invalid")
            }
        }
        Ok(config)
    }

    pub fn content_sha256(&self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.database.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
    }

    pub fn legacy_content_sha256_without_hold_open(&self) -> Result<String> {
        let legacy = LegacyConfigHash {
            symbol: &self.symbol,
            anchor_price: self.anchor_price,
            grid_ratio: self.grid_ratio,
            trade_levels: self.trade_levels,
            boundary_levels: self.boundary_levels,
            standard_budget: self.standard_budget,
            initial_cash: self.initial_cash,
            initial_position: self.initial_position,
            initial_sellable: self.initial_sellable,
            max_position: self.max_position,
            min_base_position: self.min_base_position,
            lot_size: self.lot_size,
            price_scale: self.price_scale,
            hysteresis_ratio: self.hysteresis_ratio,
            commission_rate: self.commission_rate,
            minimum_commission: self.minimum_commission,
            sell_tax_rate: self.sell_tax_rate,
            slippage_rate: self.slippage_rate,
            gate: &self.gate,
            paper: LegacyPaperSettings {
                reject_probability_bps: self.paper.reject_probability_bps,
                partial_fill_bps: self.paper.partial_fill_bps,
                latency_ms: self.paper.latency_ms,
                seed: self.paper.seed,
            },
            ambiguity_policy: &self.ambiguity_policy,
            database: "",
            config_version: &self.config_version,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&legacy)?)))
    }

    pub fn legacy_content_sha256_without_standard_quantity(
        &self,
        include_hold_open: bool,
    ) -> Result<String> {
        if !include_hold_open {
            return self.legacy_content_sha256_without_hold_open();
        }
        let legacy = LegacyQuantityConfigHash {
            symbol: &self.symbol,
            anchor_price: self.anchor_price,
            grid_ratio: self.grid_ratio,
            trade_levels: self.trade_levels,
            boundary_levels: self.boundary_levels,
            standard_budget: self.standard_budget,
            initial_cash: self.initial_cash,
            initial_position: self.initial_position,
            initial_sellable: self.initial_sellable,
            max_position: self.max_position,
            min_base_position: self.min_base_position,
            lot_size: self.lot_size,
            price_scale: self.price_scale,
            hysteresis_ratio: self.hysteresis_ratio,
            commission_rate: self.commission_rate,
            minimum_commission: self.minimum_commission,
            sell_tax_rate: self.sell_tax_rate,
            slippage_rate: self.slippage_rate,
            gate: &self.gate,
            paper: &self.paper,
            ambiguity_policy: &self.ambiguity_policy,
            database: "",
            config_version: &self.config_version,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&legacy)?)))
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config: Self = serde_yaml::from_str(&raw).context("invalid YAML configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let zero = Decimal::ZERO;
        if self.symbol.trim().is_empty() {
            bail!("symbol is required")
        }
        if self.anchor_price <= zero || self.initial_cash < zero {
            bail!("anchor and cash must be valid positive amounts")
        }
        if self.grid_ratio <= zero || self.grid_ratio >= Decimal::ONE {
            bail!("grid_ratio must be between 0 and 1")
        }
        if self.trade_levels < 1 || self.boundary_levels <= self.trade_levels {
            bail!("boundary_levels must exceed positive trade_levels")
        }
        if self.initial_position < 0
            || self.initial_sellable < 0
            || self.max_position < 0
            || self.min_base_position < 0
            || self.lot_size <= 0
            || self.max_position < self.initial_position
        {
            bail!("invalid lot size or position limits")
        }
        if [
            self.standard_quantity,
            self.lot_size,
            self.initial_position,
            self.initial_sellable,
            self.max_position,
            self.min_base_position,
        ]
        .into_iter()
        .any(|quantity| quantity > MAX_SHARE_QUANTITY)
        {
            bail!("configured share quantities cannot exceed {MAX_SHARE_QUANTITY}")
        }
        let quantity_mode = self.standard_quantity > 0;
        let legacy_budget_mode = self.standard_budget > zero;
        if self.standard_quantity < 0
            || self.standard_budget < zero
            || quantity_mode == legacy_budget_mode
            || (quantity_mode && self.standard_quantity % self.lot_size != 0)
        {
            bail!(
                "configure exactly one mechanical unit: positive board-lot standard_quantity for new runs or positive legacy standard_budget"
            )
        }
        if self.initial_sellable > self.initial_position
            || self.min_base_position > self.initial_position
        {
            bail!("sellable/base position cannot exceed initial position")
        }
        if self.hysteresis_ratio < zero || self.hysteresis_ratio > Decimal::ONE {
            bail!("hysteresis_ratio must be within [0, 1]")
        }
        if self.commission_rate < zero || self.commission_rate > Config::decimal("0.1") {
            bail!("commission_rate must be within [0, 0.1]")
        }
        if self.minimum_commission < zero {
            bail!("minimum_commission cannot be negative")
        }
        if self.sell_tax_rate < zero || self.sell_tax_rate > Config::decimal("0.1") {
            bail!("sell_tax_rate must be within [0, 0.1]")
        }
        if self.slippage_rate < zero || self.slippage_rate >= Decimal::ONE {
            bail!("slippage_rate must be within [0, 1)")
        }
        if self.price_scale > 8 {
            bail!("price_scale cannot exceed 8 decimal places")
        }
        let grid = crate::grid::GridSpec::from(self);
        grid.validate()
            .context("grid numeric envelope is invalid")?;
        if self.uses_fixed_quantity() {
            let grid_quantity = self
                .standard_quantity
                .checked_mul(i64::from(self.boundary_levels))
                .context("grid quantity envelope is not representable")?;
            if grid_quantity > MAX_SHARE_QUANTITY {
                bail!("grid quantity envelope cannot exceed {MAX_SHARE_QUANTITY}")
            }
            let max_grid_price = grid
                .price(self.boundary_levels)
                .context("maximum grid price is not representable")?;
            let adverse_buy_price = max_grid_price
                .checked_mul(
                    Decimal::ONE
                        .checked_add(self.slippage_rate)
                        .context("slippage factor is not representable")?,
                )
                .context("adverse BUY price is not representable")?;
            let notional = adverse_buy_price
                .checked_mul(Decimal::from(grid_quantity))
                .context("maximum grid order notional is not representable")?;
            let commission = notional
                .checked_mul(self.commission_rate)
                .context("maximum grid order commission is not representable")?
                .max(self.minimum_commission);
            let tax = notional
                .checked_mul(self.sell_tax_rate)
                .context("maximum grid order tax is not representable")?;
            notional
                .checked_add(commission)
                .and_then(|value| value.checked_add(tax))
                .context("maximum grid order cash envelope is not representable")?;
        } else {
            self.standard_budget
                .checked_mul(Decimal::from(self.boundary_levels))
                .context("legacy grid budget envelope is not representable")?;
        }
        if self.gate.alpha < zero
            || self.gate.alpha > Decimal::ONE
            || self.gate.probability < zero
            || self.gate.probability > Decimal::ONE
        {
            bail!("gate probability and alpha must be within [0, 1]")
        }
        if !["safe", "skip", "always_execute"].contains(&self.gate.failure_mode.as_str()) {
            bail!("gate failure_mode must be safe, skip, or always_execute")
        }
        if self.gate.timeout_ms == 0 || self.gate.timeout_ms > 60_000 {
            bail!("gate timeout_ms must be within 1..=60000")
        }
        match (&*self.gate.kind, &self.gate.capital_inventory) {
            ("resource_aware", Some(settings)) => {
                if settings.minimum_free_cash < zero
                    || settings.minimum_free_cash > self.initial_cash
                    || settings.target_position < self.min_base_position
                    || settings.target_position > self.max_position
                    || settings.target_position > MAX_SHARE_QUANTITY
                {
                    bail!("invalid resource-aware cash reserve or target position")
                }
                if self.gate.failure_mode == "always_execute" {
                    bail!("resource_aware gate cannot use an always_execute failure fallback")
                }
                if settings.deploy_initial_balance
                    && (self.initial_position != 0 || self.initial_sellable != 0)
                {
                    bail!("initial balance deployment requires an empty starting position")
                }
                if settings.deploy_initial_balance
                    && settings.minimum_free_cash
                        < self
                            .initial_cash
                            .checked_div(Decimal::from(2))
                            .context("initial cash midpoint is not representable")?
                {
                    bail!("initial balance deployment must retain at least half of initial cash")
                }
            }
            ("resource_aware", None) => {
                bail!("resource_aware gate requires capital_inventory settings")
            }
            (_, Some(_)) => {
                bail!("capital_inventory settings require gate kind resource_aware")
            }
            (_, None) => {}
        }
        for bps in [
            self.paper.reject_probability_bps,
            self.paper.partial_fill_bps,
            self.paper.hold_open_probability_bps,
        ] {
            if bps > 10_000 {
                bail!("paper probabilities must not exceed 10000 bps")
            }
        }
        if !["conservative", "skip", "lower_first", "upper_first"]
            .contains(&self.ambiguity_policy.as_str())
        {
            bail!("invalid ambiguity_policy")
        }
        Ok(())
    }

    pub fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("static decimal")
    }

    pub fn uses_fixed_quantity(&self) -> bool {
        self.standard_quantity > 0
    }
}
