use crate::{
    config::Config,
    domain::{GridLevelState, GridLevelStatus},
};
use anyhow::{bail, Context, Result};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub trait GridEngine {
    fn generate_levels(&self) -> Result<BTreeMap<i32, GridLevelState>>;
    fn detect_touches(
        &self,
        levels: &BTreeMap<i32, GridLevelState>,
        previous_close: Decimal,
        low: Decimal,
        high: Decimal,
    ) -> (Vec<i32>, TouchClassification);
}

#[derive(Debug, Clone)]
pub struct MechanicalGridEngine {
    pub spec: GridSpec,
}

impl GridEngine for MechanicalGridEngine {
    fn generate_levels(&self) -> Result<BTreeMap<i32, GridLevelState>> {
        self.spec.levels()
    }

    fn detect_touches(
        &self,
        levels: &BTreeMap<i32, GridLevelState>,
        previous_close: Decimal,
        low: Decimal,
        high: Decimal,
    ) -> (Vec<i32>, TouchClassification) {
        crossed_levels(levels, previous_close, low, high)
    }
}

/// Upper bound for one side of a geometric grid. The current generator
/// recomputes each indexed price from the anchor, so construction is O(B^2).
/// Raising this limit therefore requires a separate resource/SLO review.
pub const MAX_BOUNDARY_LEVELS: i32 = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridSpec {
    #[serde(with = "rust_decimal::serde::str")]
    pub anchor: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ratio: Decimal,
    pub trade_levels: i32,
    pub boundary_levels: i32,
    pub price_scale: u32,
}

impl From<&Config> for GridSpec {
    fn from(value: &Config) -> Self {
        Self {
            anchor: value.anchor_price,
            ratio: value.grid_ratio,
            trade_levels: value.trade_levels,
            boundary_levels: value.boundary_levels,
            price_scale: value.price_scale,
        }
    }
}

impl GridSpec {
    pub fn validate(&self) -> Result<()> {
        if self.anchor <= Decimal::ZERO {
            bail!("grid anchor must be positive")
        }
        if self.ratio <= Decimal::ZERO || self.ratio >= Decimal::ONE {
            bail!("grid ratio must be within (0, 1)")
        }
        if self.trade_levels < 1
            || self.boundary_levels <= self.trade_levels
            || self.boundary_levels > MAX_BOUNDARY_LEVELS
        {
            bail!(
                "grid levels must satisfy 1 <= trade_levels < boundary_levels <= {MAX_BOUNDARY_LEVELS}"
            )
        }
        if self.price_scale > 8 {
            bail!("grid price scale cannot exceed 8 decimal places")
        }
        for index in -self.boundary_levels..=self.boundary_levels {
            if index != 0 {
                self.price(index)?;
            }
        }
        Ok(())
    }

    pub fn price(&self, index: i32) -> Result<Decimal> {
        let magnitude = index
            .checked_abs()
            .context("grid index magnitude is not representable")?;
        if magnitude > self.boundary_levels || self.boundary_levels > MAX_BOUNDARY_LEVELS {
            bail!("grid index is outside the configured numeric envelope")
        }
        let factor = Decimal::ONE
            .checked_add(self.ratio)
            .context("grid ratio factor is not representable")?;
        if factor <= Decimal::ONE {
            bail!("grid ratio factor must exceed one")
        }
        let mut result = self.anchor;
        if index > 0 {
            for _ in 0..index {
                result = result
                    .checked_mul(factor)
                    .context("grid price multiplication overflow")?;
            }
        } else {
            for _ in index..0 {
                result = result
                    .checked_div(factor)
                    .context("grid price division overflow")?;
            }
        }
        let rounded =
            result.round_dp_with_strategy(self.price_scale, RoundingStrategy::MidpointAwayFromZero);
        if rounded <= Decimal::ZERO {
            bail!("grid price rounds to a non-positive value")
        }
        Ok(rounded)
    }

    pub fn levels(&self) -> Result<BTreeMap<i32, GridLevelState>> {
        self.validate()?;
        let mut levels = BTreeMap::new();
        for index in -self.boundary_levels..=self.boundary_levels {
            if index == 0 {
                continue;
            }
            levels.insert(
                index,
                GridLevelState {
                    index,
                    price: self.price(index)?,
                    status: GridLevelStatus::Armed,
                    touch_count: 0,
                },
            );
        }
        Ok(levels)
    }

    pub fn is_trade_level(&self, index: i32) -> bool {
        index != 0
            && index
                .checked_abs()
                .is_some_and(|magnitude| magnitude <= self.trade_levels)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TouchClassification {
    Normal,
    Ambiguous,
}

pub fn crosses_level(
    index: i32,
    level_price: Decimal,
    previous_close: Decimal,
    low: Decimal,
    high: Decimal,
) -> bool {
    if index < 0 {
        previous_close > level_price && low <= level_price
    } else {
        previous_close < level_price && high >= level_price
    }
}

pub fn crossed_levels(
    levels: &BTreeMap<i32, GridLevelState>,
    previous_close: Decimal,
    low: Decimal,
    high: Decimal,
) -> (Vec<i32>, TouchClassification) {
    let touched: Vec<i32> = levels
        .iter()
        .filter(|(index, level)| {
            matches!(
                level.status,
                GridLevelStatus::Armed | GridLevelStatus::Rearmed
            ) && crosses_level(**index, level.price, previous_close, low, high)
        })
        .map(|(index, _)| *index)
        .collect();
    let has_lower = touched.iter().any(|index| *index < 0);
    let has_upper = touched.iter().any(|index| *index > 0);
    let classification = if has_lower && has_upper {
        TouchClassification::Ambiguous
    } else {
        TouchClassification::Normal
    };
    (touched, classification)
}

pub fn maybe_rearm(
    level: &mut GridLevelState,
    current_price: Decimal,
    grid_width: Decimal,
    hysteresis_ratio: Decimal,
) -> Result<bool> {
    if !matches!(
        level.status,
        GridLevelStatus::Touched | GridLevelStatus::Executed | GridLevelStatus::Skipped
    ) {
        return Ok(false);
    }
    let width = if grid_width < Decimal::ZERO {
        grid_width
            .checked_mul(Decimal::NEGATIVE_ONE)
            .context("grid width magnitude is not representable")?
    } else {
        grid_width
    };
    let threshold = width
        .checked_mul(hysteresis_ratio)
        .context("grid rearm threshold overflow")?;
    let cleared_on_safe_side = if level.index < 0 {
        current_price
            >= level
                .price
                .checked_add(threshold)
                .context("lower grid rearm boundary overflow")?
    } else {
        current_price
            <= level
                .price
                .checked_sub(threshold)
                .context("upper grid rearm boundary overflow")?
    };
    if cleared_on_safe_side {
        level.status = GridLevelStatus::Rearmed;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub fn mechanical_cap(index: i32, standard_budget: Decimal) -> Result<Decimal> {
    let magnitude = index
        .checked_abs()
        .context("grid budget index magnitude is not representable")?;
    Decimal::from(magnitude)
        .checked_mul(standard_budget)
        .context("grid mechanical budget overflow")
}

pub fn available_deferred_budget(
    index: i32,
    standard_budget: Decimal,
    deployed: Decimal,
) -> Result<Decimal> {
    Ok(mechanical_cap(index, standard_budget)?
        .checked_sub(deployed)
        .context("grid deferred budget overflow")?
        .max(Decimal::ZERO))
}

pub fn board_lot_quantity(budget: Decimal, price: Decimal, lot_size: i64) -> Result<i64> {
    if budget <= Decimal::ZERO || price <= Decimal::ZERO || lot_size <= 0 {
        return Ok(0);
    }
    let shares = budget
        .checked_div(price)
        .context("board-lot quantity division overflow")?
        .floor();
    let raw = shares
        .to_string()
        .parse::<i64>()
        .context("board-lot quantity exceeds the i64 domain")?;
    raw.checked_div(lot_size)
        .and_then(|lots| lots.checked_mul(lot_size))
        .context("board-lot quantity overflow")
}
