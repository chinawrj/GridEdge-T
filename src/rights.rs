//! Grid-right and gating coordination policy.
//!
//! The grid engine only reports a touched level. This module translates that
//! opportunity into conserved fixed-share-quantity capacity. Historical buy
//! budget support below is replay compatibility and cannot start a new run. The
//! automation service then obtains an algorithm decision, applies risk checks,
//! and commits the resulting event plan through `ledger::LedgerWriter`.

use crate::{
    config::Config,
    decision::{GridRight, GridRightCapacity, GridRightStatus, RightTranche},
    domain::{Direction, StrategyState},
    grid::{available_deferred_budget, GridSpec},
    profit::prove_non_loss,
};
use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::BTreeSet;

/// Return the marginal sell-quantity tranches that must be minted for a right.
/// This is shared by capacity planning and event construction so a right can
/// never advertise a tranche that the ledger batch fails to create.
pub fn sell_tranches_to_mint(
    state: &StrategyState,
    right_id: &str,
    index: i32,
) -> Vec<RightTranche> {
    state
        .lots
        .values()
        .filter(|lot| {
            lot.remaining_quantity > 0
                && lot.grid_index < 0
                && lot.grid_index.abs() <= index.abs()
                && !state.right_tranches.values().any(|tranche| {
                    tranche.cycle_id == state.cycle_id
                        && tranche.direction == Direction::Sell
                        && tranche.lot_id.as_deref() == Some(lot.lot_id.as_str())
                        && (tranche.available_quantity > 0 || tranche.reserved_quantity > 0)
                })
        })
        .map(|lot| {
            RightTranche::minted_quantity(
                &state.cycle_id,
                right_id,
                right_id,
                index,
                &lot.lot_id,
                lot.remaining_quantity,
            )
        })
        .collect()
}

pub struct RightsGateCoordinator<'a> {
    config: &'a Config,
    state: &'a StrategyState,
}

impl<'a> RightsGateCoordinator<'a> {
    pub fn new(config: &'a Config, state: &'a StrategyState) -> Self {
        Self { config, state }
    }

    /// An exercised buy level owns inventory exposure for the remainder of the
    /// current cycle. Rearming detects a fresh crossing; it must not mint a
    /// fresh buy authorization while that exposure is still open. A fully
    /// deferred/revoked excursion has zero exercised balance and may therefore
    /// receive a new token when price returns later. Sell rights are sourced
    /// from concrete remaining lot tranches and follow their own capacity rule.
    pub fn has_exercised_at(&self, index: i32, direction: Direction) -> bool {
        if direction != Direction::Buy {
            return false;
        }
        self.state.grid_rights.values().any(|right| {
            right.cycle_id == self.state.cycle_id
                && right.direction == direction
                && right.grid_index == index
                && if right.capacity.available_quantity > 0 {
                    right.exercised_quantity > 0
                } else {
                    right.exercised_budget > Decimal::ZERO
                }
        })
    }

    /// A deferred or blocked right can only carry to a strictly deeper level.
    pub fn may_grant_at(&self, index: i32, direction: Direction) -> bool {
        if self.has_exercised_at(index, direction) {
            return false;
        }
        if self.state.right_tranches.values().any(|tranche| {
            tranche.cycle_id == self.state.cycle_id
                && tranche.direction == direction
                && (tranche.reserved_budget > Decimal::ZERO || tranche.reserved_quantity > 0)
        }) {
            return false;
        }
        let has_tranche_history = self.state.right_tranches.values().any(|tranche| {
            tranche.cycle_id == self.state.cycle_id && tranche.direction == direction
        });
        let tranche_frontier = self
            .state
            .right_tranches
            .values()
            .filter(|tranche| {
                tranche.cycle_id == self.state.cycle_id
                    && tranche.direction == direction
                    && (tranche.available_budget > Decimal::ZERO || tranche.available_quantity > 0)
            })
            .map(|tranche| {
                let owner_depth = self
                    .state
                    .grid_rights
                    .get(&tranche.owner_right_id)
                    .map(|right| right.grid_index.abs())
                    .unwrap_or_else(|| tranche.birth_grid_index.abs());
                let marginal_at_owner_was_revoked =
                    self.state.right_tranches.values().any(|candidate| {
                        candidate.cycle_id == tranche.cycle_id
                            && candidate.direction == tranche.direction
                            && candidate.owner_right_id == tranche.owner_right_id
                            && candidate.birth_grid_index.abs() == owner_depth
                            && (candidate.revoked_budget > Decimal::ZERO
                                || candidate.revoked_quantity > 0)
                    });
                if marginal_at_owner_was_revoked {
                    tranche.birth_grid_index.abs()
                } else {
                    owner_depth
                }
            })
            .max();
        let legacy_frontier = self
            .state
            .grid_rights
            .values()
            .filter(|right| {
                right.cycle_id == self.state.cycle_id
                    && right.direction == direction
                    && !matches!(
                        right.status,
                        GridRightStatus::Transferred
                            | GridRightStatus::Expired
                            | GridRightStatus::Revoked
                    )
                    && match direction {
                        Direction::Buy => {
                            right.capacity.available_quantity > 0
                                || right.capacity.available_budget > Decimal::ZERO
                        }
                        Direction::Sell => {
                            right.capacity.eligible_quantity
                                + right.capacity.t_plus_one_blocked_quantity
                                + right.capacity.risk_blocked_quantity
                                + right.capacity.no_profit_blocked_quantity
                                > 0
                        }
                    }
            })
            .map(|right| right.grid_index.abs())
            .max();
        let deepest_prior = if has_tranche_history {
            tranche_frontier.unwrap_or(0)
        } else {
            legacy_frontier.unwrap_or(0)
        };
        index.abs() > deepest_prior
    }

    pub fn carry_sources(&self, direction: Direction) -> Vec<&'a GridRight> {
        let has_tranche_history = self.state.right_tranches.values().any(|tranche| {
            tranche.cycle_id == self.state.cycle_id && tranche.direction == direction
        });
        let owners: BTreeSet<_> = self
            .state
            .right_tranches
            .values()
            .filter(|tranche| {
                tranche.cycle_id == self.state.cycle_id
                    && tranche.direction == direction
                    && (tranche.available_budget > Decimal::ZERO || tranche.available_quantity > 0)
            })
            .map(|tranche| tranche.owner_right_id.as_str())
            .collect();
        self.state
            .grid_rights
            .values()
            .filter(|right| {
                right.cycle_id == self.state.cycle_id
                    && right.direction == direction
                    && (!has_tranche_history || owners.contains(right.right_id.as_str()))
                    && matches!(
                        right.status,
                        GridRightStatus::Residual
                            | GridRightStatus::Deferred
                            | GridRightStatus::Blocked
                            | GridRightStatus::PartiallyExercised
                            | GridRightStatus::Released
                    )
                    && match direction {
                        Direction::Buy => {
                            if right.capacity.available_quantity > 0 {
                                right.remaining_quantity() > 0
                            } else {
                                right.remaining_budget() > Decimal::ZERO
                            }
                        }
                        Direction::Sell => right.remaining_quantity() > 0,
                    }
            })
            .max_by_key(|right| right.grid_index.abs())
            .into_iter()
            .collect()
    }

    pub fn capacity(
        &self,
        index: i32,
        direction: Direction,
        date: NaiveDate,
        excursion_epoch: &str,
    ) -> Result<GridRightCapacity> {
        let depth = index
            .checked_abs()
            .context("grid capacity index magnitude is not representable")?;
        if depth == 0 || depth > self.config.boundary_levels {
            bail!("grid capacity index is outside the configured boundary")
        }
        if direction == Direction::Buy {
            if self.config.uses_fixed_quantity() {
                let mechanical_quantity_cap = i64::from(depth)
                    .checked_mul(self.config.standard_quantity)
                    .context("grid mechanical quantity capacity overflow")?;
                let deployed_quantity: i64 = self
                    .state
                    .grid_rights
                    .values()
                    .filter(|right| {
                        right.cycle_id == self.state.cycle_id
                            && right.direction == Direction::Buy
                            && right.capacity.available_quantity > 0
                    })
                    .map(|right| {
                        if right.status == GridRightStatus::Reserved {
                            right.reserved_quantity.max(right.exercised_quantity)
                        } else {
                            right.exercised_quantity
                        }
                    })
                    .try_fold(0_i64, i64::checked_add)
                    .context("deployed BUY quantity overflow")?;
                let sources = self.carry_sources(direction);
                let active_tranches: Vec<_> = self
                    .state
                    .right_tranches
                    .values()
                    .filter(|tranche| {
                        tranche.cycle_id == self.state.cycle_id
                            && tranche.direction == Direction::Buy
                            && tranche.available_quantity > 0
                    })
                    .collect();
                let carried_in_quantity: i64 = active_tranches
                    .iter()
                    .try_fold(0_i64, |total, tranche| {
                        total.checked_add(tranche.available_quantity)
                    })
                    .context("carried BUY quantity overflow")?;
                let has_buy_tranche_history = self.state.right_tranches.values().any(|tranche| {
                    tranche.cycle_id == self.state.cycle_id && tranche.direction == Direction::Buy
                });
                let available_quantity = if !has_buy_tranche_history
                    && self.state.grid_rights.values().any(|right| {
                        right.cycle_id == self.state.cycle_id && right.direction == Direction::Buy
                    }) {
                    mechanical_quantity_cap
                        .checked_sub(deployed_quantity)
                        .context("available BUY quantity underflow")?
                        .max(0)
                } else {
                    carried_in_quantity
                        .checked_add(self.config.standard_quantity)
                        .context("available BUY quantity overflow")?
                };
                let mut tranche_ids: Vec<_> = active_tranches
                    .iter()
                    .map(|tranche| tranche.tranche_id.clone())
                    .collect();
                tranche_ids.push(RightTranche::id_for(
                    &self.state.cycle_id,
                    excursion_epoch,
                    Direction::Buy,
                    index,
                    None,
                ));
                return Ok(GridRightCapacity {
                    mechanical_budget_cap: Decimal::ZERO,
                    deployed_budget: Decimal::ZERO,
                    available_budget: Decimal::ZERO,
                    mechanical_quantity_cap,
                    deployed_quantity,
                    available_quantity,
                    eligible_quantity: 0,
                    eligible_lot_ids: Vec::new(),
                    accumulated_grid_indices: (1..=depth).map(|depth| -depth).collect(),
                    t_plus_one_blocked_quantity: 0,
                    t_plus_one_blocked_lot_ids: Vec::new(),
                    risk_blocked_quantity: 0,
                    risk_blocked_lot_ids: Vec::new(),
                    no_profit_blocked_quantity: 0,
                    no_profit_blocked_lot_ids: Vec::new(),
                    source_right_ids: sources.iter().map(|right| right.right_id.clone()).collect(),
                    carried_in_budget: Decimal::ZERO,
                    carried_in_quantity,
                    tranche_ids,
                });
            }
            let mechanical_budget_cap =
                crate::grid::mechanical_cap(index, self.config.standard_budget)?;
            let committed_budget: Decimal = self
                .state
                .grid_rights
                .values()
                .filter(|right| {
                    right.cycle_id == self.state.cycle_id && right.direction == Direction::Buy
                })
                .map(|right| {
                    if right.status == GridRightStatus::Reserved {
                        right.reserved_budget.max(right.exercised_budget)
                    } else {
                        right.exercised_budget
                    }
                })
                .try_fold(Decimal::ZERO, |total, value| total.checked_add(value))
                .context("legacy committed budget overflow")?;
            let sources = self.carry_sources(direction);
            let active_tranches: Vec<_> = self
                .state
                .right_tranches
                .values()
                .filter(|tranche| {
                    tranche.cycle_id == self.state.cycle_id
                        && tranche.direction == Direction::Buy
                        && tranche.available_budget > Decimal::ZERO
                })
                .collect();
            let carried_in_budget: Decimal = active_tranches
                .iter()
                .try_fold(Decimal::ZERO, |total, tranche| {
                    total.checked_add(tranche.available_budget)
                })
                .context("legacy carried budget overflow")?;
            let available_budget =
                if self.state.right_tranches.is_empty() && !self.state.grid_rights.is_empty() {
                    available_deferred_budget(index, self.config.standard_budget, committed_budget)?
                } else {
                    carried_in_budget
                        .checked_add(self.config.standard_budget)
                        .context("legacy available budget overflow")?
                };
            let mut tranche_ids: Vec<_> = active_tranches
                .iter()
                .map(|tranche| tranche.tranche_id.clone())
                .collect();
            tranche_ids.push(RightTranche::id_for(
                &self.state.cycle_id,
                excursion_epoch,
                Direction::Buy,
                index,
                None,
            ));
            return Ok(GridRightCapacity {
                mechanical_budget_cap,
                deployed_budget: committed_budget,
                available_budget,
                mechanical_quantity_cap: 0,
                deployed_quantity: 0,
                available_quantity: 0,
                eligible_quantity: 0,
                eligible_lot_ids: Vec::new(),
                accumulated_grid_indices: (1..=depth).map(|depth| -depth).collect(),
                t_plus_one_blocked_quantity: 0,
                t_plus_one_blocked_lot_ids: Vec::new(),
                risk_blocked_quantity: 0,
                risk_blocked_lot_ids: Vec::new(),
                no_profit_blocked_quantity: 0,
                no_profit_blocked_lot_ids: Vec::new(),
                source_right_ids: sources.iter().map(|right| right.right_id.clone()).collect(),
                carried_in_budget,
                carried_in_quantity: 0,
                tranche_ids,
            });
        }

        let matching_lots: Vec<_> = self
            .state
            .lots
            .values()
            .filter(|lot| {
                lot.remaining_quantity > 0
                    && lot.grid_index < 0
                    && lot
                        .grid_index
                        .checked_abs()
                        .is_some_and(|lot_depth| lot_depth <= depth)
            })
            .collect();
        let mut eligible_lots: Vec<_> = matching_lots
            .iter()
            .copied()
            .filter(|lot| lot.opened_on < date)
            .collect();
        eligible_lots.sort_by_key(|lot| {
            (
                std::cmp::Reverse(lot.grid_index.abs()),
                lot.opened_on,
                lot.lot_id.clone(),
            )
        });
        let mut t1_lots: Vec<_> = matching_lots
            .iter()
            .copied()
            .filter(|lot| lot.opened_on >= date)
            .collect();
        t1_lots.sort_by_key(|lot| (lot.grid_index.abs(), lot.opened_on, lot.lot_id.clone()));
        let lot_quantity: i64 = eligible_lots
            .iter()
            .try_fold(0_i64, |total, lot| {
                total.checked_add(lot.remaining_quantity)
            })
            .context("eligible lot quantity overflow")?;
        let sellable = self
            .state
            .position
            .sellable
            .checked_sub(self.state.position.frozen_sell)
            .context("sellable quantity underflow")?
            .max(0);
        let above_base = self
            .state
            .position
            .total
            .checked_sub(self.config.min_base_position)
            .context("base position underflow")?
            .max(0);
        let mut remaining_risk_capacity = lot_quantity.min(sellable).min(above_base);
        let sell_price = GridSpec::from(self.config).price(index)?;
        let mut eligible_quantity = 0_i64;
        let mut eligible_lot_ids = Vec::new();
        let mut risk_blocked_quantity = 0_i64;
        let mut risk_blocked_lot_ids = Vec::new();
        let mut no_profit_blocked_quantity = 0_i64;
        let mut no_profit_blocked_lot_ids = Vec::new();
        for lot in &eligible_lots {
            let permitted = lot.remaining_quantity.min(remaining_risk_capacity);
            if permitted > 0 {
                if prove_non_loss(self.config, lot, permitted, sell_price).is_some() {
                    eligible_quantity = eligible_quantity
                        .checked_add(permitted)
                        .context("eligible quantity overflow")?;
                    eligible_lot_ids.push(lot.lot_id.clone());
                    remaining_risk_capacity = remaining_risk_capacity
                        .checked_sub(permitted)
                        .context("risk capacity underflow")?;
                } else {
                    no_profit_blocked_quantity = no_profit_blocked_quantity
                        .checked_add(permitted)
                        .context("no-profit quantity overflow")?;
                    no_profit_blocked_lot_ids.push(lot.lot_id.clone());
                }
            }
            let risk_blocked = lot
                .remaining_quantity
                .checked_sub(permitted)
                .context("risk blocked quantity underflow")?;
            if risk_blocked > 0 {
                risk_blocked_quantity = risk_blocked_quantity
                    .checked_add(risk_blocked)
                    .context("risk blocked quantity overflow")?;
                risk_blocked_lot_ids.push(lot.lot_id.clone());
            }
        }
        let sources = self.carry_sources(direction);
        let t1_quantity = t1_lots
            .iter()
            .try_fold(0_i64, |total, lot| {
                total.checked_add(lot.remaining_quantity)
            })
            .context("T+1 quantity overflow")?;
        let total_current_capacity = eligible_quantity
            .checked_add(t1_quantity)
            .and_then(|value| value.checked_add(lot_quantity.checked_sub(eligible_quantity)?))
            .context("SELL current capacity overflow")?;
        let active_tranches: Vec<_> = self
            .state
            .right_tranches
            .values()
            .filter(|tranche| {
                tranche.cycle_id == self.state.cycle_id
                    && tranche.direction == Direction::Sell
                    && tranche.available_quantity > 0
            })
            .collect();
        let carried_in_quantity: i64 = active_tranches
            .iter()
            .try_fold(0_i64, |total, tranche| {
                total.checked_add(tranche.available_quantity)
            })
            .context("carried SELL quantity overflow")?
            .min(total_current_capacity);
        let mut accumulated_grid_indices: Vec<_> =
            eligible_lots.iter().map(|lot| lot.grid_index).collect();
        accumulated_grid_indices.sort_unstable();
        accumulated_grid_indices.dedup();
        let mut tranche_ids: Vec<_> = active_tranches
            .iter()
            .map(|tranche| tranche.tranche_id.clone())
            .collect();
        tranche_ids.extend(
            sell_tranches_to_mint(self.state, excursion_epoch, index)
                .into_iter()
                .map(|tranche| tranche.tranche_id),
        );
        Ok(GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: self.state.deployed_grid_budget,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: 0,
            deployed_quantity: 0,
            available_quantity: 0,
            eligible_quantity,
            eligible_lot_ids,
            accumulated_grid_indices,
            t_plus_one_blocked_quantity: t1_quantity,
            t_plus_one_blocked_lot_ids: t1_lots.iter().map(|lot| lot.lot_id.clone()).collect(),
            risk_blocked_quantity,
            risk_blocked_lot_ids,
            no_profit_blocked_quantity,
            no_profit_blocked_lot_ids,
            source_right_ids: sources.iter().map(|right| right.right_id.clone()).collect(),
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity,
            tranche_ids,
        })
    }
}
