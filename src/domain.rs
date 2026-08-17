use crate::decision::{GridRight, RightTranche, RightTrancheUnit};
use chrono::{NaiveDate, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ServiceMode {
    Running,
    Safe,
    ReadOnly,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GridLevelStatus {
    Armed,
    Touched,
    Executed,
    Skipped,
    Rearmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridLevelState {
    pub index: i32,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub status: GridLevelStatus,
    pub touch_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridCycle {
    pub cycle_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub anchor_price: Decimal,
    pub started_at: NaiveDateTime,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridTrigger {
    pub trigger_id: String,
    pub grid_index: i32,
    pub direction: Direction,
    pub touched_at: NaiveDateTime,
    pub ambiguous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeferredBudget {
    #[serde(with = "rust_decimal::serde::str")]
    pub mechanical_cap: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub deployed: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub available: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TradableQuantity {
    pub total: i64,
    pub sellable_today: i64,
    pub frozen_for_orders: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Created,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected,
    CancelPending,
    Cancelled,
}

impl OrderStatus {
    pub fn can_transition(self, next: Self) -> bool {
        use OrderStatus::*;
        matches!(
            (self, next),
            (Created, Submitted)
                | (Submitted, Accepted | Rejected | CancelPending)
                | (
                    Accepted,
                    PartiallyFilled | Filled | CancelPending | Rejected
                )
                | (PartiallyFilled, PartiallyFilled | Filled | CancelPending)
                | (CancelPending, Cancelled | PartiallyFilled | Filled)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub intent_id: String,
    #[serde(default)]
    pub right_id: String,
    pub grid_index: i32,
    pub direction: Direction,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    pub quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub budget: Decimal,
    pub target_lot_ids: Vec<String>,
    /// Exact source slices authorized for a sell. IDs alone are insufficient
    /// when an order only closes part of one of several lots.
    #[serde(default)]
    pub target_lot_allocations: Vec<LotQuantityAllocation>,
    #[serde(default)]
    pub profit_guard_version: String,
    /// Audited fee/slippage terms used to derive every canonical partial-fill
    /// slice. Schema-v1 orders omit this and replay as historical facts.
    #[serde(default)]
    pub profit_guard_policy: Option<ProfitGuardPolicy>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfitGuardPolicy {
    pub price_scale: u32,
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_commission: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub sell_tax_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_rate: Decimal,
}

impl From<&crate::config::Config> for ProfitGuardPolicy {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            price_scale: config.price_scale,
            commission_rate: config.commission_rate,
            minimum_commission: config.minimum_commission,
            sell_tax_rate: config.sell_tax_rate,
            slippage_rate: config.slippage_rate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LotQuantityAllocation {
    pub lot_id: String,
    #[serde(default)]
    pub tranche_id: String,
    pub quantity: i64,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub cost_basis: Decimal,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub worst_fill_price: Decimal,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub maximum_sell_fees: Decimal,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub worst_case_profit: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub intent: OrderIntent,
    pub status: OrderStatus,
    pub filled_quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub filled_notional: Decimal,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub commission_charged: Decimal,
    #[serde(default, with = "rust_decimal::serde::str")]
    pub reserved_cash_remaining: Decimal,
    #[serde(default)]
    pub reserved_quantity_remaining: i64,
    #[serde(default)]
    pub cancel_requested_reason: Option<String>,
    #[serde(default)]
    pub cancel_requested_at: Option<NaiveDateTime>,
}

impl Order {
    pub fn transition(&mut self, next: OrderStatus) -> Result<(), DomainError> {
        if !self.status.can_transition(next) {
            return Err(DomainError::InvalidOrderTransition(self.status, next));
        }
        self.status = next;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub fill_id: String,
    pub order_id: String,
    pub direction: Direction,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub commission: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub tax: Decimal,
    pub trade_date: NaiveDate,
    pub target_lot_ids: Vec<String>,
    #[serde(default)]
    pub target_lot_allocations: Vec<LotQuantityAllocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingLot {
    pub lot_id: String,
    pub source_order_id: String,
    pub grid_index: i32,
    pub opened_on: NaiveDate,
    pub original_quantity: i64,
    pub remaining_quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub open_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub allocated_cost: Decimal,
    /// Authoritative unamortized buy cost. Legacy events default to zero and
    /// are interpreted from allocated_cost and remaining quantity.
    #[serde(default, with = "rust_decimal::serde::str")]
    pub remaining_cost: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CashAccount {
    #[serde(with = "rust_decimal::serde::str")]
    pub available: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub frozen: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_fees: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Position {
    pub total: i64,
    pub sellable: i64,
    pub today_bought: i64,
    pub frozen_sell: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountSnapshot {
    pub cash: CashAccount,
    pub position: Position,
    pub open_order_ids: Vec<String>,
    pub cumulative_filled_quantity: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationResult {
    pub matched: bool,
    pub differences: Vec<String>,
    pub checked_at: NaiveDateTime,
    #[serde(default)]
    pub broker_source: String,
    #[serde(default)]
    pub broker_snapshot_sha256: String,
    #[serde(default)]
    pub checked_sequence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyState {
    pub run_id: String,
    pub cycle_id: String,
    pub symbol: String,
    pub mode: ServiceMode,
    #[serde(with = "rust_decimal::serde::str")]
    pub anchor_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub deployed_grid_budget: Decimal,
    pub levels: BTreeMap<i32, GridLevelState>,
    /// True after a durable cycle-start/reset fact. Current fixed-quantity
    /// cycles must always project a complete canonical level topology.
    #[serde(default)]
    pub grid_cycle_active: bool,
    #[serde(default)]
    pub historically_touched_levels: BTreeSet<i32>,
    #[serde(default)]
    pub historically_skipped_levels: BTreeSet<i32>,
    #[serde(default)]
    pub historically_executed_levels: BTreeSet<i32>,
    pub orders: BTreeMap<String, Order>,
    pub lots: BTreeMap<String, TradingLot>,
    pub processed_fills: BTreeSet<String>,
    #[serde(default)]
    pub grid_rights: BTreeMap<String, GridRight>,
    #[serde(default)]
    pub right_tranches: BTreeMap<String, RightTranche>,
    /// Canonical financial terms projected from CONFIG_SNAPSHOTTED. Every
    /// schema-v3 order must bind to this run-level policy.
    #[serde(default)]
    pub audited_profit_guard_policy: Option<ProfitGuardPolicy>,
    /// Mechanical-unit identity projected from the current CONFIG_SNAPSHOTTED
    /// event. Zero means a pre-quantity historical run.
    #[serde(default)]
    pub audited_standard_quantity: i64,
    #[serde(default)]
    pub audited_lot_size: i64,
    /// Hash-verified run configuration projected from CONFIG_SNAPSHOTTED.
    /// Current opportunity validation recomputes grid prices and risk
    /// partitions from this immutable source instead of trusting event
    /// payloads. Legacy snapshots are hydrated from their journal fact.
    #[serde(default)]
    pub audited_config: Option<crate::config::Config>,
    pub cash: CashAccount,
    pub position: Position,
    pub duplicate_events: u64,
    pub ambiguous_bars: u64,
    pub event_count: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
    pub current_trade_date: Option<NaiveDate>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub last_price: Option<Decimal>,
    pub last_recovery: Option<String>,
    pub last_reconciliation: Option<ReconciliationResult>,
    #[serde(default)]
    pub last_reconciliation_event_sequence: Option<i64>,
}

impl StrategyState {
    pub fn normalize_legacy_derived_fields(&mut self) {
        if !self.grid_cycle_active && !self.levels.is_empty() {
            self.grid_cycle_active = true;
        }
        for lot in self.lots.values_mut() {
            if lot.remaining_cost.is_zero()
                && lot.allocated_cost > Decimal::ZERO
                && lot.remaining_quantity > 0
                && lot.original_quantity > 0
            {
                lot.remaining_cost = lot.allocated_cost * Decimal::from(lot.remaining_quantity)
                    / Decimal::from(lot.original_quantity);
            }
            lot.remaining_cost = lot.remaining_cost.normalize();
        }
    }

    pub fn new(
        run_id: String,
        cycle_id: String,
        symbol: String,
        anchor_price: Decimal,
        initial_cash: Decimal,
        initial_position: i64,
        initial_sellable: i64,
    ) -> Self {
        Self {
            run_id,
            cycle_id,
            symbol,
            mode: ServiceMode::Running,
            anchor_price,
            deployed_grid_budget: Decimal::ZERO,
            levels: BTreeMap::new(),
            grid_cycle_active: false,
            historically_touched_levels: BTreeSet::new(),
            historically_skipped_levels: BTreeSet::new(),
            historically_executed_levels: BTreeSet::new(),
            orders: BTreeMap::new(),
            lots: BTreeMap::new(),
            processed_fills: BTreeSet::new(),
            grid_rights: BTreeMap::new(),
            right_tranches: BTreeMap::new(),
            audited_profit_guard_policy: None,
            audited_standard_quantity: 0,
            audited_lot_size: 0,
            audited_config: None,
            cash: CashAccount {
                available: initial_cash,
                frozen: Decimal::ZERO,
                total_fees: Decimal::ZERO,
            },
            position: Position {
                total: initial_position,
                sellable: initial_sellable,
                today_bought: 0,
                frozen_sell: 0,
            },
            duplicate_events: 0,
            ambiguous_bars: 0,
            event_count: 0,
            realized_pnl: Decimal::ZERO,
            current_trade_date: None,
            last_price: None,
            last_recovery: None,
            last_reconciliation: None,
            last_reconciliation_event_sequence: None,
        }
    }

    pub fn roll_trading_day(&mut self, date: NaiveDate) -> Result<(), DomainError> {
        if self.current_trade_date.is_some_and(|d| d < date) {
            self.position.sellable = self
                .position
                .sellable
                .checked_add(self.position.today_bought)
                .ok_or(DomainError::NumericRange)?;
            self.position.today_bought = 0;
        }
        self.current_trade_date = Some(date);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<AccountSnapshot, DomainError> {
        Ok(AccountSnapshot {
            cash: self.cash.clone(),
            position: self.position.clone(),
            open_order_ids: self
                .orders
                .iter()
                .filter(|(_, order)| {
                    matches!(
                        order.status,
                        OrderStatus::Created
                            | OrderStatus::Submitted
                            | OrderStatus::Accepted
                            | OrderStatus::PartiallyFilled
                            | OrderStatus::CancelPending
                    )
                })
                .map(|(id, _)| id.clone())
                .collect(),
            cumulative_filled_quantity: self
                .orders
                .values()
                .try_fold(0_i64, |total, order| {
                    total.checked_add(order.filled_quantity)
                })
                .ok_or(DomainError::NumericRange)?,
        })
    }

    /// Validate invariants that span multiple aggregates in the materialized
    /// projection. Event-local checks remain in `event.rs`; this read-only
    /// audit is used by case runners, rebuild verification and operator tools.
    pub fn validate_invariants(&self) -> anyhow::Result<()> {
        if self.cash.available < Decimal::ZERO
            || self.cash.frozen < Decimal::ZERO
            || self.cash.total_fees < Decimal::ZERO
            || self.position.total < 0
            || self.position.sellable < 0
            || self.position.today_bought < 0
            || self.position.frozen_sell < 0
            || self
                .position
                .sellable
                .checked_add(self.position.today_bought)
                .is_none_or(|quantity| quantity > self.position.total)
            || self.position.frozen_sell > self.position.sellable
        {
            anyhow::bail!("account projection violates non-negative position/cash invariants")
        }
        for tranche in self.right_tranches.values() {
            tranche.validate()?;
            let owner = self
                .grid_rights
                .get(&tranche.owner_right_id)
                .ok_or_else(|| anyhow::anyhow!("tranche owner right is missing"))?;
            if owner.cycle_id != tranche.cycle_id || owner.direction != tranche.direction {
                anyhow::bail!("tranche owner identity does not match its right")
            }
            if tranche.direction == Direction::Sell {
                let lot_id = tranche
                    .lot_id
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("sell tranche lacks a lot source"))?;
                if !self.lots.contains_key(lot_id) {
                    anyhow::bail!("sell tranche references a missing lot")
                }
            }
        }
        for right in self.grid_rights.values() {
            for tranche_id in &right.capacity.tranche_ids {
                let tranche = self.right_tranches.get(tranche_id).ok_or_else(|| {
                    anyhow::anyhow!("grid right advertises a tranche that was not minted")
                })?;
                if tranche.cycle_id != right.cycle_id || tranche.direction != right.direction {
                    anyhow::bail!("grid right advertises a tranche from another capacity domain")
                }
            }
        }
        if self.audited_standard_quantity > 0 {
            let audited_config = self
                .audited_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("fixed-quantity state lacks its audited config"))?;
            if self.audited_lot_size <= 0
                || self.audited_standard_quantity % self.audited_lot_size != 0
                || !audited_config.uses_fixed_quantity()
                || audited_config.standard_quantity != self.audited_standard_quantity
                || audited_config.lot_size != self.audited_lot_size
            {
                anyhow::bail!("audited mechanical quantity is not board-lot aligned")
            }
            if self.grid_cycle_active {
                let canonical_levels = crate::grid::GridSpec::from(audited_config).levels()?;
                if self.levels.len() != canonical_levels.len()
                    || canonical_levels.iter().any(|(index, canonical)| {
                        self.levels.get(index).is_none_or(|actual| {
                            actual.index != canonical.index || actual.price != canonical.price
                        })
                    })
                {
                    anyhow::bail!("active fixed-quantity cycle lacks its complete canonical grid")
                }
            }
            let mut buy_active_by_depth = BTreeMap::<(String, i32), i64>::new();
            for tranche in self.right_tranches.values().filter(|tranche| {
                tranche.direction == Direction::Buy && tranche.unit == RightTrancheUnit::Quantity
            }) {
                if tranche.minted_quantity != self.audited_standard_quantity
                    || tranche.lot_id.is_some()
                {
                    anyhow::bail!("buy tranche does not represent exactly one fixed share unit")
                }
                let tranche_quantity = tranche
                    .available_quantity
                    .checked_add(tranche.reserved_quantity)
                    .and_then(|value| value.checked_add(tranche.consumed_quantity))
                    .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
                let entry = buy_active_by_depth
                    .entry((tranche.cycle_id.clone(), tranche.birth_grid_index))
                    .or_default();
                *entry = entry
                    .checked_add(tranche_quantity)
                    .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
            }
            if buy_active_by_depth
                .values()
                .any(|quantity| *quantity > self.audited_standard_quantity)
            {
                anyhow::bail!("buy depth has more than one live/exercised unit in a cycle")
            }
            for right in self.grid_rights.values() {
                let advertised: Vec<_> = right
                    .capacity
                    .tranche_ids
                    .iter()
                    .map(|tranche_id| {
                        self.right_tranches
                            .get(tranche_id)
                            .expect("advertised tranche existence was validated above")
                    })
                    .collect();
                if !matches!(
                    right.status,
                    crate::decision::GridRightStatus::Transferred
                        | crate::decision::GridRightStatus::Expired
                        | crate::decision::GridRightStatus::Revoked
                ) && advertised.iter().any(|tranche| {
                    (tranche.available_quantity > 0 || tranche.reserved_quantity > 0)
                        && tranche.owner_right_id != right.right_id
                }) {
                    anyhow::bail!("active grid right does not own its available tranche inventory")
                }
                if right.status == crate::decision::GridRightStatus::Reserved {
                    let reserved: i64 = self
                        .right_tranches
                        .values()
                        .filter(|tranche| tranche.owner_right_id == right.right_id)
                        .try_fold(0_i64, |total, tranche| {
                            total.checked_add(tranche.reserved_quantity)
                        })
                        .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
                    if reserved != right.reserved_quantity {
                        anyhow::bail!(
                            "reserved grid right is not backed by an equal tranche reservation"
                        )
                    }
                }
            }
        }
        let mut active_sell_by_lot = BTreeMap::<String, i64>::new();
        for tranche in self.right_tranches.values().filter(|tranche| {
            tranche.direction == Direction::Sell && tranche.unit == RightTrancheUnit::Quantity
        }) {
            let lot_id = tranche
                .lot_id
                .as_ref()
                .expect("sell tranche source was validated above");
            let tranche_active = tranche
                .available_quantity
                .checked_add(tranche.reserved_quantity)
                .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
            let entry = active_sell_by_lot.entry(lot_id.clone()).or_default();
            *entry = entry
                .checked_add(tranche_active)
                .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
        }
        for (lot_id, active_quantity) in active_sell_by_lot {
            if active_quantity
                > self
                    .lots
                    .get(&lot_id)
                    .expect("sell tranche source was validated above")
                    .remaining_quantity
            {
                anyhow::bail!("sell lot has duplicate or excessive active authorization")
            }
        }
        for right in self
            .grid_rights
            .values()
            .filter(|right| right.decision_recorded)
        {
            let authorization = if right.direction == Direction::Buy {
                right.capacity.available_quantity
            } else {
                right.capacity.eligible_quantity
            };
            if right.decision_contract_version
                >= crate::decision::WHOLE_UNIT_DECISION_CONTRACT_VERSION
            {
                let unit = self.audited_standard_quantity;
                let residual = authorization % unit;
                let authorized = authorization - residual;
                let (offered, pre_blocked, post_blocked) = if right.decision_contract_version
                    >= crate::decision::DECISION_CONTRACT_VERSION
                {
                    (
                        right.decision_algorithm_offered_quantity,
                        right.decision_pre_blocked_quantity,
                        right.decision_post_blocked_quantity,
                    )
                } else {
                    (
                        right.decision_algorithm_authorized_quantity,
                        0,
                        right.decision_platform_blocked_quantity,
                    )
                };
                if right.decision_gross_available_quantity != authorization
                    || right.decision_platform_residual_quantity != residual
                    || right.decision_algorithm_authorized_quantity != authorized
                    || offered < 0
                    || pre_blocked < 0
                    || post_blocked < 0
                    || offered
                        .checked_add(pre_blocked)
                        .is_none_or(|quantity| quantity != authorized)
                    || right.decided_exercise_quantity < 0
                    || right.decided_defer_quantity < 0
                    || right
                        .decided_exercise_quantity
                        .checked_add(right.decided_defer_quantity)
                        .is_none_or(|quantity| quantity != offered)
                    || right.decided_exercise_quantity % unit != 0
                    || right.decided_defer_quantity % unit != 0
                    || right.decision_platform_blocked_quantity < 0
                    || right.decision_intent_quantity < 0
                    || right.decision_remaining_quantity < 0
                    || right
                        .decision_intent_quantity
                        .checked_add(post_blocked)
                        .is_none_or(|quantity| right.decided_exercise_quantity != quantity)
                    || pre_blocked
                        .checked_add(post_blocked)
                        .is_none_or(|quantity| right.decision_platform_blocked_quantity != quantity)
                    || right
                        .decided_defer_quantity
                        .checked_add(right.decision_platform_blocked_quantity)
                        .is_none_or(|quantity| right.decision_remaining_quantity != quantity)
                    || right.decision_platform_blocked_quantity % unit != 0
                    || offered % unit != 0
                    || pre_blocked % unit != 0
                    || post_blocked % unit != 0
                    || right.decision_intent_quantity % unit != 0
                    || right.decision_remaining_quantity % unit != 0
                {
                    anyhow::bail!("current algorithm decision violates its whole-unit partition")
                }
            } else if right.decision_is_algorithm
                && (right.decided_exercise_quantity < 0
                    || right.decided_defer_quantity < 0
                    || right
                        .decided_exercise_quantity
                        .checked_add(right.decided_defer_quantity)
                        .is_none_or(|quantity| quantity != authorization)
                    || (self.audited_lot_size > 0
                        && (right.decided_exercise_quantity % self.audited_lot_size != 0
                            || right.decided_defer_quantity % self.audited_lot_size != 0)))
            {
                anyhow::bail!("algorithm decision does not partition its eligible authorization")
            }
        }
        let lot_realized: Decimal = self
            .lots
            .values()
            .try_fold(Decimal::ZERO, |total, lot| {
                total.checked_add(lot.realized_pnl)
            })
            .ok_or_else(|| anyhow::anyhow!("NUMERIC_RANGE_VIOLATION"))?;
        if lot_realized != self.realized_pnl {
            anyhow::bail!("state realized PnL differs from the sum of lot PnL")
        }
        Ok(())
    }

    pub fn unrealized_grid_pnl(&self) -> Decimal {
        let Some(mark) = self.last_price else {
            return Decimal::ZERO;
        };
        self.lots
            .values()
            .map(|lot| {
                let remaining_cost = lot.effective_remaining_cost();
                mark * Decimal::from(lot.remaining_quantity) - remaining_cost
            })
            .sum()
    }

    pub fn apply_fill(&mut self, fill: &Fill, grid_index: i32) -> Result<bool, DomainError> {
        self.apply_fill_with_policy(fill, grid_index, true)
    }

    pub(crate) fn apply_legacy_fill(
        &mut self,
        fill: &Fill,
        grid_index: i32,
    ) -> Result<bool, DomainError> {
        self.apply_fill_with_policy(fill, grid_index, false)
    }

    fn apply_fill_with_policy(
        &mut self,
        fill: &Fill,
        grid_index: i32,
        enforce_no_loss: bool,
    ) -> Result<bool, DomainError> {
        if self.processed_fills.contains(&fill.fill_id) {
            return Ok(false);
        }
        let mut projected = self.clone();
        let applied = projected.apply_fill_inner(fill, grid_index, enforce_no_loss)?;
        *self = projected;
        Ok(applied)
    }

    fn apply_fill_inner(
        &mut self,
        fill: &Fill,
        grid_index: i32,
        enforce_no_loss: bool,
    ) -> Result<bool, DomainError> {
        if self.processed_fills.contains(&fill.fill_id) {
            return Ok(false);
        }
        if fill.quantity <= 0
            || fill.price <= Decimal::ZERO
            || fill.commission < Decimal::ZERO
            || fill.tax < Decimal::ZERO
        {
            return Err(DomainError::InvalidFill);
        }
        let notional = fill
            .price
            .checked_mul(Decimal::from(fill.quantity))
            .ok_or(DomainError::NumericRange)?;
        let fees = fill
            .commission
            .checked_add(fill.tax)
            .ok_or(DomainError::NumericRange)?;
        match fill.direction {
            Direction::Buy => {
                let total = notional
                    .checked_add(fees)
                    .ok_or(DomainError::NumericRange)?;
                if self.cash.available < total {
                    return Err(DomainError::InsufficientCash);
                }
                self.cash.available = self
                    .cash
                    .available
                    .checked_sub(total)
                    .ok_or(DomainError::NumericRange)?;
                self.cash.total_fees = self
                    .cash
                    .total_fees
                    .checked_add(fees)
                    .ok_or(DomainError::NumericRange)?;
                self.position.total = self
                    .position
                    .total
                    .checked_add(fill.quantity)
                    .ok_or(DomainError::NumericRange)?;
                self.position.today_bought = self
                    .position
                    .today_bought
                    .checked_add(fill.quantity)
                    .ok_or(DomainError::NumericRange)?;
                self.deployed_grid_budget = self
                    .deployed_grid_budget
                    .checked_add(notional)
                    .ok_or(DomainError::NumericRange)?;
                let lot_id = format!("lot:{}", fill.fill_id);
                self.lots.insert(
                    lot_id.clone(),
                    TradingLot {
                        lot_id,
                        source_order_id: fill.order_id.clone(),
                        grid_index,
                        opened_on: fill.trade_date,
                        original_quantity: fill.quantity,
                        remaining_quantity: fill.quantity,
                        open_price: fill.price,
                        allocated_cost: total,
                        remaining_cost: total.normalize(),
                        realized_pnl: Decimal::ZERO,
                    },
                );
            }
            Direction::Sell => {
                if self.position.sellable < fill.quantity {
                    return Err(DomainError::InsufficientSellable);
                }
                let allocations = self.sell_fill_allocations(fill)?;
                let explicit_fee_allocations = if fill.target_lot_allocations.is_empty() {
                    None
                } else {
                    Some(
                        crate::profit::allocate_actual_fill_fees(&allocations, fees)
                            .ok_or(DomainError::InvalidLotAllocation)?,
                    )
                };
                let mut projected = Vec::with_capacity(allocations.len());
                let mut allocated_fees = Decimal::ZERO;
                for (index, allocation) in allocations.iter().enumerate() {
                    let lot = self
                        .lots
                        .get(&allocation.lot_id)
                        .ok_or(DomainError::InsufficientLotQuantity)?;
                    if lot.opened_on >= fill.trade_date
                        || allocation.quantity <= 0
                        || allocation.quantity > lot.remaining_quantity
                    {
                        return Err(DomainError::InsufficientLotQuantity);
                    }
                    let remaining_cost = lot.effective_remaining_cost();
                    let buy_cost = if !fill.target_lot_allocations.is_empty() {
                        allocation.cost_basis
                    } else if allocation.quantity == lot.remaining_quantity {
                        remaining_cost
                    } else {
                        remaining_cost
                            .checked_mul(Decimal::from(allocation.quantity))
                            .and_then(|value| {
                                value.checked_div(Decimal::from(lot.remaining_quantity))
                            })
                            .ok_or(DomainError::NumericRange)?
                    };
                    if buy_cost < Decimal::ZERO || buy_cost > remaining_cost {
                        return Err(DomainError::InvalidLotAllocation);
                    }
                    let sell_fees = if let Some(explicit) = &explicit_fee_allocations {
                        explicit[index]
                    } else if index + 1 == allocations.len() {
                        fees.checked_sub(allocated_fees)
                            .ok_or(DomainError::NumericRange)?
                    } else {
                        fees.checked_mul(Decimal::from(allocation.quantity))
                            .and_then(|value| value.checked_div(Decimal::from(fill.quantity)))
                            .ok_or(DomainError::NumericRange)?
                    };
                    allocated_fees = allocated_fees
                        .checked_add(sell_fees)
                        .ok_or(DomainError::NumericRange)?;
                    let pnl = fill
                        .price
                        .checked_mul(Decimal::from(allocation.quantity))
                        .and_then(|value| value.checked_sub(buy_cost))
                        .and_then(|value| value.checked_sub(sell_fees))
                        .ok_or(DomainError::NumericRange)?;
                    if enforce_no_loss && pnl < Decimal::ZERO {
                        return Err(DomainError::LossMakingSell {
                            lot_id: allocation.lot_id.clone(),
                            projected_loss: -pnl,
                        });
                    }
                    projected.push((
                        allocation.clone(),
                        buy_cost,
                        remaining_cost
                            .checked_sub(buy_cost)
                            .ok_or(DomainError::NumericRange)?,
                        pnl,
                    ));
                }
                let projected_quantity = projected
                    .iter()
                    .try_fold(0_i64, |total, (item, _, _, _)| {
                        total.checked_add(item.quantity)
                    })
                    .ok_or(DomainError::NumericRange)?;
                if projected_quantity != fill.quantity {
                    return Err(DomainError::InsufficientLotQuantity);
                }
                for (allocation, _buy_cost, remaining_cost, pnl) in projected {
                    let lot = self
                        .lots
                        .get_mut(&allocation.lot_id)
                        .expect("sell allocation was prevalidated");
                    lot.remaining_quantity = lot
                        .remaining_quantity
                        .checked_sub(allocation.quantity)
                        .ok_or(DomainError::NumericRange)?;
                    lot.remaining_cost = remaining_cost.normalize();
                    lot.realized_pnl = lot
                        .realized_pnl
                        .checked_add(pnl)
                        .ok_or(DomainError::NumericRange)?;
                    self.realized_pnl = self
                        .realized_pnl
                        .checked_add(pnl)
                        .ok_or(DomainError::NumericRange)?;
                }
                self.cash.available = self
                    .cash
                    .available
                    .checked_add(
                        notional
                            .checked_sub(fees)
                            .ok_or(DomainError::NumericRange)?,
                    )
                    .ok_or(DomainError::NumericRange)?;
                self.cash.total_fees = self
                    .cash
                    .total_fees
                    .checked_add(fees)
                    .ok_or(DomainError::NumericRange)?;
                self.position.total = self
                    .position
                    .total
                    .checked_sub(fill.quantity)
                    .ok_or(DomainError::NumericRange)?;
                self.position.sellable = self
                    .position
                    .sellable
                    .checked_sub(fill.quantity)
                    .ok_or(DomainError::NumericRange)?;
            }
        }
        self.processed_fills.insert(fill.fill_id.clone());
        Ok(true)
    }

    fn sell_fill_allocations(
        &self,
        fill: &Fill,
    ) -> Result<Vec<LotQuantityAllocation>, DomainError> {
        if !fill.target_lot_allocations.is_empty() {
            let total = fill
                .target_lot_allocations
                .iter()
                .try_fold(0_i64, |total, allocation| {
                    total.checked_add(allocation.quantity)
                })
                .ok_or(DomainError::NumericRange)?;
            let unique = fill
                .target_lot_allocations
                .iter()
                .map(|allocation| allocation.lot_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if total != fill.quantity
                || unique.len() != fill.target_lot_allocations.len()
                || (!fill.target_lot_ids.is_empty()
                    && fill
                        .target_lot_allocations
                        .iter()
                        .any(|allocation| !fill.target_lot_ids.contains(&allocation.lot_id)))
            {
                return Err(DomainError::InvalidLotAllocation);
            }
            return Ok(fill.target_lot_allocations.clone());
        }

        let ids = if fill.target_lot_ids.is_empty() {
            self.lots.keys().cloned().collect::<Vec<_>>()
        } else {
            fill.target_lot_ids.clone()
        };
        let mut remaining = fill.quantity;
        let mut allocations = Vec::new();
        for id in ids {
            if remaining == 0 {
                break;
            }
            let Some(lot) = self.lots.get(&id) else {
                continue;
            };
            if lot.opened_on >= fill.trade_date {
                continue;
            }
            let quantity = remaining.min(lot.remaining_quantity);
            if quantity > 0 {
                allocations.push(LotQuantityAllocation {
                    lot_id: id,
                    tranche_id: String::new(),
                    quantity,
                    cost_basis: Decimal::ZERO,
                    worst_fill_price: Decimal::ZERO,
                    maximum_sell_fees: Decimal::ZERO,
                    worst_case_profit: Decimal::ZERO,
                });
                remaining -= quantity;
            }
        }
        if remaining != 0 {
            return Err(DomainError::InsufficientLotQuantity);
        }
        Ok(allocations)
    }
}

impl TradingLot {
    pub fn effective_remaining_cost(&self) -> Decimal {
        if self.remaining_cost.is_zero()
            && self.allocated_cost > Decimal::ZERO
            && self.remaining_quantity > 0
        {
            self.allocated_cost * Decimal::from(self.remaining_quantity)
                / Decimal::from(self.original_quantity)
        } else {
            self.remaining_cost
        }
    }
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("invalid order transition from {0:?} to {1:?}")]
    InvalidOrderTransition(OrderStatus, OrderStatus),
    #[error("insufficient available cash")]
    InsufficientCash,
    #[error("insufficient sellable position")]
    InsufficientSellable,
    #[error("eligible lots cannot cover sell fill")]
    InsufficientLotQuantity,
    #[error("sell allocation is invalid")]
    InvalidLotAllocation,
    #[error("sell would realize a loss of {projected_loss} on lot {lot_id}")]
    LossMakingSell {
        lot_id: String,
        projected_loss: Decimal,
    },
    #[error("invalid fill")]
    InvalidFill,
    #[error("minimum base position would be breached")]
    BasePositionProtected,
    #[error("maximum total position would be breached")]
    MaxPositionExceeded,
    #[error("numeric range violation")]
    NumericRange,
}

pub fn now_naive() -> NaiveDateTime {
    Utc::now().naive_utc()
}
