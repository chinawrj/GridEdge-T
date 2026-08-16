//! Mechanical no-loss guard for sell-side lot slices.
//!
//! This module is deliberately independent from the decision algorithm.  The
//! algorithm may decide whether to exercise a sell right, but it can never see
//! or select inventory that fails this guard.

use crate::{
    config::Config,
    domain::{Direction, LotQuantityAllocation, ProfitGuardPolicy, StrategyState, TradingLot},
};
use rust_decimal::{Decimal, RoundingStrategy};

pub const PROFIT_GUARD_VERSION: &str = "LOT_SLICE_NO_LOSS_V1";
pub const UNREALIZED_VALUATION_POLICY_VERSION: &str = "LOT_CONSERVATIVE_EXIT_V1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SellSliceEconomics {
    pub cost_basis: Decimal,
    pub worst_fill_price: Decimal,
    pub maximum_sell_fees: Decimal,
    pub worst_case_profit: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrealizedGridValuation {
    pub mark_price: Option<Decimal>,
    pub mark_to_market_unrealized: Option<Decimal>,
    pub conservative_exit_unrealized: Option<Decimal>,
    /// Add this signed adjustment to mark-to-market PnL to obtain the
    /// conservative-exit PnL. It is normally negative because it includes
    /// adverse slippage and future sell charges.
    pub conservative_exit_adjustment: Option<Decimal>,
    pub unpriced_open_quantity: i64,
}

/// Return the broker's cumulative commission for one order at the supplied
/// cumulative notional. Minimum commission is charged once per order; partial
/// fills only contribute the delta from the amount already charged.
pub fn cumulative_order_commission_with_policy(
    policy: &ProfitGuardPolicy,
    cumulative_notional: Decimal,
) -> Decimal {
    (cumulative_notional * policy.commission_rate)
        .max(policy.minimum_commission)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero)
}

pub fn checked_cumulative_order_commission_with_policy(
    policy: &ProfitGuardPolicy,
    cumulative_notional: Decimal,
) -> Option<Decimal> {
    Some(
        cumulative_notional
            .checked_mul(policy.commission_rate)?
            .max(policy.minimum_commission)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero),
    )
}

/// Cash required to fund a complete BUY order at its adverse Paper fill
/// price. Splitting one order never creates another minimum commission.
pub fn buy_cash_reservation_with_policy(
    policy: &ProfitGuardPolicy,
    limit_price: Decimal,
    quantity: i64,
) -> Decimal {
    if quantity <= 0 {
        return Decimal::ZERO;
    }
    let worst_price = (limit_price * (Decimal::ONE + policy.slippage_rate))
        .round_dp_with_strategy(policy.price_scale, RoundingStrategy::ToPositiveInfinity);
    let notional = worst_price * Decimal::from(quantity);
    notional + cumulative_order_commission_with_policy(policy, notional)
}

pub fn checked_buy_cash_reservation_with_policy(
    policy: &ProfitGuardPolicy,
    limit_price: Decimal,
    quantity: i64,
) -> Option<Decimal> {
    if quantity <= 0 {
        return Some(Decimal::ZERO);
    }
    let worst_price = limit_price
        .checked_mul(Decimal::ONE.checked_add(policy.slippage_rate)?)?
        .round_dp_with_strategy(policy.price_scale, RoundingStrategy::ToPositiveInfinity);
    let notional = worst_price.checked_mul(Decimal::from(quantity))?;
    notional.checked_add(checked_cumulative_order_commission_with_policy(
        policy, notional,
    )?)
}

pub fn actual_order_fill_charges(
    config: &Config,
    direction: Direction,
    prior_filled_notional: Decimal,
    prior_commission: Decimal,
    fill_price: Decimal,
    fill_quantity: i64,
) -> (Decimal, Decimal) {
    actual_order_fill_charges_with_policy(
        &ProfitGuardPolicy::from(config),
        direction,
        prior_filled_notional,
        prior_commission,
        fill_price,
        fill_quantity,
    )
}

pub fn actual_order_fill_charges_with_policy(
    policy: &ProfitGuardPolicy,
    direction: Direction,
    prior_filled_notional: Decimal,
    prior_commission: Decimal,
    fill_price: Decimal,
    fill_quantity: i64,
) -> (Decimal, Decimal) {
    let fill_notional = fill_price * Decimal::from(fill_quantity);
    let cumulative_commission =
        cumulative_order_commission_with_policy(policy, prior_filled_notional + fill_notional);
    let commission = cumulative_commission - prior_commission;
    let tax = if direction == Direction::Sell {
        (fill_notional * policy.sell_tax_rate).round_dp(2)
    } else {
        Decimal::ZERO
    };
    (commission, tax)
}

pub fn checked_actual_order_fill_charges_with_policy(
    policy: &ProfitGuardPolicy,
    direction: Direction,
    prior_filled_notional: Decimal,
    prior_commission: Decimal,
    fill_price: Decimal,
    fill_quantity: i64,
) -> Option<(Decimal, Decimal)> {
    let fill_notional = fill_price.checked_mul(Decimal::from(fill_quantity))?;
    let cumulative_notional = prior_filled_notional.checked_add(fill_notional)?;
    let cumulative_commission =
        checked_cumulative_order_commission_with_policy(policy, cumulative_notional)?;
    let commission = cumulative_commission.checked_sub(prior_commission)?;
    let tax = if direction == Direction::Sell {
        fill_notional.checked_mul(policy.sell_tax_rate)?.round_dp(2)
    } else {
        Decimal::ZERO
    };
    Some((commission, tax))
}

/// Assign the broker's exact fill fee without exceeding any source slice's
/// independently proven fee ceiling. The order of the explicit allocation is
/// the deterministic remainder policy.
pub fn allocate_actual_fill_fees(
    allocations: &[LotQuantityAllocation],
    total_fees: Decimal,
) -> Option<Vec<Decimal>> {
    if total_fees < Decimal::ZERO {
        return None;
    }
    let mut remaining = total_fees;
    let mut result = Vec::with_capacity(allocations.len());
    for allocation in allocations {
        if allocation.maximum_sell_fees < Decimal::ZERO {
            return None;
        }
        let assigned = remaining.min(allocation.maximum_sell_fees);
        result.push(assigned);
        remaining -= assigned;
    }
    (remaining == Decimal::ZERO).then_some(result)
}

pub fn split_planned_allocation(
    config: &Config,
    planned: &LotQuantityAllocation,
    already_consumed_quantity: i64,
    quantity: i64,
    limit_price: Decimal,
) -> Option<LotQuantityAllocation> {
    split_planned_allocation_with_policy(
        &ProfitGuardPolicy::from(config),
        planned,
        already_consumed_quantity,
        quantity,
        limit_price,
    )
}

pub fn split_planned_allocation_with_policy(
    policy: &ProfitGuardPolicy,
    planned: &LotQuantityAllocation,
    already_consumed_quantity: i64,
    quantity: i64,
    limit_price: Decimal,
) -> Option<LotQuantityAllocation> {
    let available = planned.quantity.checked_sub(already_consumed_quantity)?;
    if quantity <= 0 || quantity > available {
        return None;
    }
    let prefix_cost = |consumed: i64| -> Option<Decimal> {
        if consumed == planned.quantity {
            Some(planned.cost_basis)
        } else {
            planned
                .cost_basis
                .checked_mul(Decimal::from(consumed))?
                .checked_div(Decimal::from(planned.quantity))
        }
    };
    let consumed = already_consumed_quantity.checked_add(quantity)?;
    let cost_basis = prefix_cost(consumed)?
        .checked_sub(prefix_cost(already_consumed_quantity)?)?
        .normalize();
    let worst_fill_price = checked_worst_sell_fill_price_with_policy(policy, limit_price)?;
    let maximum_sell_fees =
        checked_conservative_sell_fees_with_policy(policy, worst_fill_price, quantity)?;
    let worst_case_profit = worst_fill_price
        .checked_mul(Decimal::from(quantity))?
        .checked_sub(maximum_sell_fees)?
        .checked_sub(cost_basis)?;
    if worst_case_profit < Decimal::ZERO {
        return None;
    }
    Some(LotQuantityAllocation {
        lot_id: planned.lot_id.clone(),
        tranche_id: planned.tranche_id.clone(),
        quantity,
        cost_basis,
        worst_fill_price,
        maximum_sell_fees,
        worst_case_profit,
    })
}

pub fn canonical_fill_allocations(
    policy: &ProfitGuardPolicy,
    planned_allocations: &[LotQuantityAllocation],
    already_filled: i64,
    fill_quantity: i64,
    limit_price: Decimal,
) -> Option<Vec<LotQuantityAllocation>> {
    if already_filled < 0 || fill_quantity <= 0 {
        return None;
    }
    let mut skip = already_filled;
    let mut remaining = fill_quantity;
    let mut result = Vec::new();
    for planned in planned_allocations {
        if skip >= planned.quantity {
            skip -= planned.quantity;
            continue;
        }
        let already_in_planned = skip;
        let available = planned.quantity - already_in_planned;
        skip = 0;
        let quantity = available.min(remaining);
        if quantity > 0 {
            result.push(split_planned_allocation_with_policy(
                policy,
                planned,
                already_in_planned,
                quantity,
                limit_price,
            )?);
            remaining -= quantity;
        }
        if remaining == 0 {
            break;
        }
    }
    (remaining == 0).then_some(result)
}

pub fn worst_sell_fill_price(config: &Config, limit_price: Decimal) -> Decimal {
    worst_sell_fill_price_with_policy(&ProfitGuardPolicy::from(config), limit_price)
}

pub fn worst_sell_fill_price_with_policy(
    policy: &ProfitGuardPolicy,
    limit_price: Decimal,
) -> Decimal {
    (limit_price * (Decimal::ONE - policy.slippage_rate))
        .round_dp_with_strategy(policy.price_scale, RoundingStrategy::ToNegativeInfinity)
}

pub fn checked_worst_sell_fill_price_with_policy(
    policy: &ProfitGuardPolicy,
    limit_price: Decimal,
) -> Option<Decimal> {
    Some(
        limit_price
            .checked_mul(Decimal::ONE.checked_sub(policy.slippage_rate)?)?
            .round_dp_with_strategy(policy.price_scale, RoundingStrategy::ToNegativeInfinity),
    )
}

pub fn conservative_sell_fees(config: &Config, fill_price: Decimal, quantity: i64) -> Decimal {
    conservative_sell_fees_with_policy(&ProfitGuardPolicy::from(config), fill_price, quantity)
}

pub fn conservative_sell_fees_with_policy(
    policy: &ProfitGuardPolicy,
    fill_price: Decimal,
    quantity: i64,
) -> Decimal {
    if quantity <= 0 {
        return Decimal::ZERO;
    }
    let notional = fill_price * Decimal::from(quantity);
    let commission = (notional * policy.commission_rate)
        .max(policy.minimum_commission)
        .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity);
    let tax = (notional * policy.sell_tax_rate)
        .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity);
    commission + tax
}

pub fn checked_conservative_sell_fees_with_policy(
    policy: &ProfitGuardPolicy,
    fill_price: Decimal,
    quantity: i64,
) -> Option<Decimal> {
    if quantity <= 0 {
        return Some(Decimal::ZERO);
    }
    let notional = fill_price.checked_mul(Decimal::from(quantity))?;
    let commission = notional
        .checked_mul(policy.commission_rate)?
        .max(policy.minimum_commission)
        .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity);
    let tax = notional
        .checked_mul(policy.sell_tax_rate)?
        .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity);
    commission.checked_add(tax)
}

pub fn allocated_cost_for_close(lot: &TradingLot, quantity: i64) -> Option<Decimal> {
    if quantity <= 0 || quantity > lot.remaining_quantity || lot.remaining_quantity <= 0 {
        return None;
    }
    let remaining_cost = lot.effective_remaining_cost();
    // A missing/zero cost basis cannot prove that a sale is non-loss-making.
    // Fail closed instead of treating unknown acquisition cost as free stock.
    if remaining_cost <= Decimal::ZERO {
        return None;
    }
    Some(
        (if quantity == lot.remaining_quantity {
            remaining_cost
        } else {
            remaining_cost
                .checked_mul(Decimal::from(quantity))?
                .checked_div(Decimal::from(lot.remaining_quantity))?
        })
        .normalize(),
    )
}

/// Price one exact lot slice using the same adverse-price and fee rules as the
/// no-loss guard. Unlike `prove_non_loss`, this returns negative economics so
/// valuation can expose a conservative loss instead of silently dropping it.
pub fn evaluate_sell_slice_with_policy(
    policy: &ProfitGuardPolicy,
    lot: &TradingLot,
    quantity: i64,
    reference_price: Decimal,
) -> Option<SellSliceEconomics> {
    let cost_basis = allocated_cost_for_close(lot, quantity)?;
    let worst_fill_price = checked_worst_sell_fill_price_with_policy(policy, reference_price)?;
    let maximum_sell_fees =
        checked_conservative_sell_fees_with_policy(policy, worst_fill_price, quantity)?;
    let worst_case_profit = worst_fill_price
        .checked_mul(Decimal::from(quantity))?
        .checked_sub(maximum_sell_fees)?
        .checked_sub(cost_basis)?;
    Some(SellSliceEconomics {
        cost_basis,
        worst_fill_price,
        maximum_sell_fees,
        worst_case_profit,
    })
}

/// Value every remaining grid-acquired lot independently. Each lot therefore
/// bears the same minimum commission used by its standalone no-loss proof;
/// profitable lots never subsidize loss-making lots. T+1/frozen/base-risk
/// availability is deliberately irrelevant here: this is an economic
/// valuation, not a statement that the quantity is currently sellable.
pub fn unrealized_grid_valuation(state: &StrategyState) -> UnrealizedGridValuation {
    let mark_price = state.last_price;
    let open_lots = state
        .lots
        .values()
        .filter(|lot| lot.remaining_quantity > 0)
        .collect::<Vec<_>>();
    let unpriced_open_quantity = open_lots
        .iter()
        .filter(|lot| lot.effective_remaining_cost() <= Decimal::ZERO)
        .map(|lot| lot.remaining_quantity)
        .sum();
    let Some(mark) = mark_price else {
        return UnrealizedGridValuation {
            mark_price,
            mark_to_market_unrealized: None,
            conservative_exit_unrealized: None,
            conservative_exit_adjustment: None,
            unpriced_open_quantity,
        };
    };
    if unpriced_open_quantity > 0 {
        return UnrealizedGridValuation {
            mark_price,
            mark_to_market_unrealized: None,
            conservative_exit_unrealized: None,
            conservative_exit_adjustment: None,
            unpriced_open_quantity,
        };
    }
    let mark_to_market_unrealized = open_lots.iter().try_fold(Decimal::ZERO, |total, lot| {
        mark.checked_mul(Decimal::from(lot.remaining_quantity))?
            .checked_sub(lot.effective_remaining_cost())
            .and_then(|value| total.checked_add(value))
    });
    let conservative_exit_unrealized =
        state
            .audited_profit_guard_policy
            .as_ref()
            .and_then(|policy| {
                open_lots.iter().try_fold(Decimal::ZERO, |total, lot| {
                    total.checked_add(
                        evaluate_sell_slice_with_policy(policy, lot, lot.remaining_quantity, mark)?
                            .worst_case_profit,
                    )
                })
            });
    UnrealizedGridValuation {
        mark_price,
        mark_to_market_unrealized,
        conservative_exit_unrealized,
        conservative_exit_adjustment: conservative_exit_unrealized.and_then(|exit| {
            mark_to_market_unrealized.and_then(|mark_value| exit.checked_sub(mark_value))
        }),
        unpriced_open_quantity,
    }
}

pub fn prove_non_loss(
    config: &Config,
    lot: &TradingLot,
    quantity: i64,
    limit_price: Decimal,
) -> Option<LotQuantityAllocation> {
    let economics = evaluate_sell_slice_with_policy(
        &ProfitGuardPolicy::from(config),
        lot,
        quantity,
        limit_price,
    )?;
    if economics.worst_case_profit < Decimal::ZERO {
        return None;
    }
    Some(LotQuantityAllocation {
        lot_id: lot.lot_id.clone(),
        tranche_id: String::new(),
        quantity,
        cost_basis: economics.cost_basis,
        worst_fill_price: economics.worst_fill_price,
        maximum_sell_fees: economics.maximum_sell_fees,
        worst_case_profit: economics.worst_case_profit,
    })
}

pub fn plan_non_loss_sales<'a>(
    config: &Config,
    lots: impl IntoIterator<Item = &'a TradingLot>,
    requested_quantity: i64,
    limit_price: Decimal,
) -> Vec<LotQuantityAllocation> {
    let mut lots: Vec<_> = lots.into_iter().collect();
    lots.sort_by_key(|lot| {
        (
            std::cmp::Reverse(lot.grid_index.abs()),
            lot.opened_on,
            lot.lot_id.clone(),
        )
    });
    let mut remaining = requested_quantity.max(0);
    let mut allocations = Vec::new();
    for lot in lots {
        if remaining == 0 {
            break;
        }
        let quantity = remaining.min(lot.remaining_quantity);
        if let Some(allocation) = prove_non_loss(config, lot, quantity, limit_price) {
            remaining -= quantity;
            allocations.push(allocation);
        }
    }
    allocations
}
