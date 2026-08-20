use crate::{
    config::Config,
    decision::GridRight,
    domain::{Direction, LotQuantityAllocation, StrategyState},
    grid::GridSpec,
    profit::{checked_buy_cash_reservation_with_policy, plan_non_loss_sales},
};
use rust_decimal::Decimal;
use thiserror::Error;

pub trait RiskChecker {
    fn check_buy(
        &self,
        state: &StrategyState,
        quantity: i64,
        estimated_cost: Decimal,
    ) -> Result<(), RiskError>;
    fn check_sell(&self, state: &StrategyState, quantity: i64) -> Result<(), RiskError>;
}

pub struct DefaultRiskChecker<'a> {
    pub config: &'a Config,
}

pub fn estimated_buy_reservation(
    config: &Config,
    limit_price: Decimal,
    quantity: i64,
) -> Result<Decimal, RiskError> {
    checked_buy_cash_reservation_with_policy(
        &crate::domain::ProfitGuardPolicy::from(config),
        limit_price,
        quantity,
    )
    .ok_or(RiskError::NumericRange)
}

pub fn affordable_buy_units(
    config: &Config,
    limit_price: Decimal,
    spendable_cash: Decimal,
    maximum_units: i64,
) -> Result<i64, RiskError> {
    if spendable_cash < Decimal::ZERO || maximum_units < 0 || config.standard_quantity <= 0 {
        return Err(RiskError::NumericRange);
    }
    let mut low = 0_i64;
    let mut high = maximum_units;
    while low < high {
        let span = high.checked_sub(low).ok_or(RiskError::NumericRange)?;
        let mid = low
            .checked_add(span / 2)
            .and_then(|value| value.checked_add(1))
            .ok_or(RiskError::NumericRange)?;
        let quantity = mid
            .checked_mul(config.standard_quantity)
            .ok_or(RiskError::NumericRange)?;
        if estimated_buy_reservation(config, limit_price, quantity)? <= spendable_cash {
            low = mid;
        } else {
            high = mid.checked_sub(1).ok_or(RiskError::NumericRange)?;
        }
    }
    Ok(low)
}

/// Canonical one-shot opening allocation for a resource-aware run. The
/// quantity is always a whole-Q amount, never breaches the configured cash
/// floor, and is capped by the number of trade levels so opening inventory
/// cannot turn into an unbounded high-price purchase.
pub fn canonical_initial_buy_units(
    config: &Config,
    state: &StrategyState,
    limit_price: Decimal,
) -> Result<i64, RiskError> {
    let Some(settings) = config.gate.capital_inventory.as_ref() else {
        return Ok(0);
    };
    if !settings.deploy_initial_balance || config.standard_quantity <= 0 {
        return Ok(0);
    }
    let free_cash = state
        .cash
        .available
        .checked_sub(state.cash.frozen)
        .ok_or(RiskError::NumericRange)?;
    let spendable_cash = free_cash
        .checked_sub(settings.minimum_free_cash)
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO)
        .min(
            config
                .initial_cash
                .checked_div(Decimal::from(2))
                .ok_or(RiskError::NumericRange)?,
        );
    let lower_boundary = GridSpec::from(config)
        .price(-config.boundary_levels)
        .map_err(|_| RiskError::NumericRange)?;
    if limit_price < lower_boundary || limit_price > config.anchor_price {
        return Ok(0);
    }
    let pending_buy_quantity = state
        .orders
        .values()
        .filter(|order| {
            order.intent.direction == Direction::Buy
                && !matches!(
                    order.status,
                    crate::domain::OrderStatus::Filled
                        | crate::domain::OrderStatus::Rejected
                        | crate::domain::OrderStatus::Cancelled
                )
        })
        .try_fold(0_i64, |total, order| {
            order
                .intent
                .quantity
                .checked_sub(order.filled_quantity)
                .and_then(|quantity| total.checked_add(quantity))
        })
        .ok_or(RiskError::NumericRange)?;
    let exposure = state
        .position
        .total
        .checked_add(pending_buy_quantity)
        .ok_or(RiskError::NumericRange)?;
    let position_units = config
        .max_position
        .checked_sub(exposure)
        .ok_or(RiskError::NumericRange)?
        .max(0)
        / config.standard_quantity;
    let target_units = settings
        .target_position
        .checked_sub(exposure)
        .ok_or(RiskError::NumericRange)?
        .max(0)
        / config.standard_quantity;
    let maximum_units = i64::from(config.trade_levels)
        .min(position_units)
        .min(target_units)
        .max(0);
    affordable_buy_units(config, limit_price, spendable_cash, maximum_units)
}

#[derive(Debug, Clone, PartialEq)]
pub struct InitialDeploymentPlan {
    pub maximum_units: i64,
    pub selected_units: i64,
    pub quantity: i64,
    pub pending_buy_quantity: i64,
    pub reservation: Decimal,
    pub projected_remaining_cash: Decimal,
}

/// Return the complete audited opening-allocation calculation. Keeping this
/// beside `canonical_initial_buy_units` gives the service and journal
/// projection one exact formula for both quantity and cash evidence.
pub fn canonical_initial_deployment_plan(
    config: &Config,
    state: &StrategyState,
    limit_price: Decimal,
) -> Result<InitialDeploymentPlan, RiskError> {
    let pending_buy_quantity = state
        .orders
        .values()
        .filter(|order| {
            order.intent.direction == Direction::Buy
                && !matches!(
                    order.status,
                    crate::domain::OrderStatus::Filled
                        | crate::domain::OrderStatus::Rejected
                        | crate::domain::OrderStatus::Cancelled
                )
        })
        .try_fold(0_i64, |total, order| {
            order
                .intent
                .quantity
                .checked_sub(order.filled_quantity)
                .and_then(|quantity| total.checked_add(quantity))
        })
        .ok_or(RiskError::NumericRange)?;
    let exposure = state
        .position
        .total
        .checked_add(pending_buy_quantity)
        .ok_or(RiskError::NumericRange)?;
    let maximum_units = if config.standard_quantity > 0 {
        i64::from(config.trade_levels)
            .min(
                config
                    .max_position
                    .checked_sub(exposure)
                    .ok_or(RiskError::NumericRange)?
                    .max(0)
                    / config.standard_quantity,
            )
            .min(
                config
                    .gate
                    .capital_inventory
                    .as_ref()
                    .map(|settings| settings.target_position)
                    .unwrap_or(0)
                    .checked_sub(exposure)
                    .ok_or(RiskError::NumericRange)?
                    .max(0)
                    / config.standard_quantity,
            )
            .max(0)
    } else {
        0
    };
    let selected_units = canonical_initial_buy_units(config, state, limit_price)?;
    let quantity = selected_units
        .checked_mul(config.standard_quantity)
        .ok_or(RiskError::NumericRange)?;
    let reservation = if quantity > 0 {
        estimated_buy_reservation(config, limit_price, quantity)?
    } else {
        Decimal::ZERO
    };
    let projected_remaining_cash = state
        .cash
        .available
        .checked_sub(reservation)
        .ok_or(RiskError::NumericRange)?;
    Ok(InitialDeploymentPlan {
        maximum_units,
        selected_units,
        quantity,
        pending_buy_quantity,
        reservation,
        projected_remaining_cash,
    })
}

/// Recompute the maximum whole-standard-quantity slice that the platform can
/// actually approve from an algorithm exercise. This is shared by the service
/// and the ledger writer so `B` cannot be invented independently of risk,
/// non-loss lot support, or existing reservations.
pub fn canonical_approved_quantity(
    config: &Config,
    state: &StrategyState,
    right: &GridRight,
    exercise_quantity: i64,
) -> Result<i64, RiskError> {
    Ok(canonical_approval(config, state, right, exercise_quantity)?.quantity)
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalApproval {
    pub quantity: i64,
    pub sell_allocations: Vec<LotQuantityAllocation>,
}

/// Return the largest whole-standard-quantity approval that is closed under
/// the exact platform plan. For SELL, quantity and lot allocations are one
/// indivisible result: flooring a larger plan's partial safe sum is invalid
/// because minimum fees can make the smaller quantity independently unsafe.
pub fn canonical_approval(
    config: &Config,
    state: &StrategyState,
    right: &GridRight,
    exercise_quantity: i64,
) -> Result<CanonicalApproval, RiskError> {
    if exercise_quantity <= 0 || config.standard_quantity <= 0 {
        return Ok(CanonicalApproval {
            quantity: 0,
            sell_allocations: Vec::new(),
        });
    }
    let risk = DefaultRiskChecker { config };
    match right.direction {
        Direction::Buy => {
            let mut approved = exercise_quantity;
            while approved > 0 {
                match risk.check_buy(
                    state,
                    approved,
                    estimated_buy_reservation(config, right.grid_price, approved)?,
                ) {
                    Ok(()) => break,
                    Err(RiskError::NumericRange) => return Err(RiskError::NumericRange),
                    Err(_) => {
                        approved = approved
                            .checked_sub(config.standard_quantity)
                            .ok_or(RiskError::NumericRange)?;
                    }
                }
            }
            Ok(CanonicalApproval {
                quantity: approved.max(0),
                sell_allocations: Vec::new(),
            })
        }
        Direction::Sell => {
            let eligible_lots: Vec<_> = right
                .capacity
                .eligible_lot_ids
                .iter()
                .filter_map(|lot_id| state.lots.get(lot_id))
                .collect();
            let mut candidate =
                exercise_quantity / config.standard_quantity * config.standard_quantity;
            while candidate > 0 {
                if risk.check_sell(state, candidate).is_ok() {
                    let allocations = plan_non_loss_sales(
                        config,
                        eligible_lots.iter().copied(),
                        candidate,
                        right.grid_price,
                    );
                    let allocation_quantity = allocations
                        .iter()
                        .try_fold(0_i64, |total, allocation| {
                            total.checked_add(allocation.quantity)
                        })
                        .ok_or(RiskError::NumericRange)?;
                    if allocation_quantity == candidate {
                        return Ok(CanonicalApproval {
                            quantity: candidate,
                            sell_allocations: allocations,
                        });
                    }
                }
                candidate = candidate
                    .checked_sub(config.standard_quantity)
                    .ok_or(RiskError::NumericRange)?;
            }
            Ok(CanonicalApproval {
                quantity: 0,
                sell_allocations: Vec::new(),
            })
        }
    }
}

impl RiskChecker for DefaultRiskChecker<'_> {
    fn check_buy(
        &self,
        state: &StrategyState,
        quantity: i64,
        estimated_cost: Decimal,
    ) -> Result<(), RiskError> {
        if !matches!(state.mode, crate::domain::ServiceMode::Running) {
            return Err(RiskError::SafeMode);
        }
        let available_cash = state
            .cash
            .available
            .checked_sub(state.cash.frozen)
            .ok_or(RiskError::NumericRange)?;
        if quantity <= 0 || available_cash < estimated_cost {
            return Err(RiskError::InsufficientCash);
        }
        if state.position.total < 0
            || state.position.total > self.config.max_position
            || quantity > self.config.max_position - state.position.total
        {
            return Err(RiskError::MaxPosition);
        }
        Ok(())
    }

    fn check_sell(&self, state: &StrategyState, quantity: i64) -> Result<(), RiskError> {
        if !matches!(state.mode, crate::domain::ServiceMode::Running) {
            return Err(RiskError::SafeMode);
        }
        let available_sellable = state
            .position
            .sellable
            .checked_sub(state.position.frozen_sell)
            .ok_or(RiskError::NumericRange)?;
        if quantity <= 0 || quantity > available_sellable {
            return Err(RiskError::InsufficientSellable);
        }
        let remaining = state
            .position
            .total
            .checked_sub(quantity)
            .ok_or(RiskError::NumericRange)?;
        if remaining < self.config.min_base_position {
            return Err(RiskError::BasePosition);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("service is not in RUNNING mode")]
    SafeMode,
    #[error("insufficient cash")]
    InsufficientCash,
    #[error("maximum position exceeded")]
    MaxPosition,
    #[error("numeric range violation")]
    NumericRange,
    #[error("insufficient T+1 sellable quantity")]
    InsufficientSellable,
    #[error("minimum base position protected")]
    BasePosition,
}
