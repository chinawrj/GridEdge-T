use crate::{
    config::Config,
    decision::GridRight,
    domain::{Direction, LotQuantityAllocation, StrategyState},
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
