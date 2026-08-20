use crate::data::{validate_bar, MarketBar};
use crate::decision::{GridRight, GridRightStatus, RightTranche, RightTrancheUnit};
use crate::domain::{
    Fill, GridLevelState, GridLevelStatus, InitialDeploymentEvaluation, InitialDeploymentOutcome,
    Order, OrderIntentOrigin, OrderStatus, PendingMarketEvidence, ReconciliationResult,
    ServiceMode, StrategyState,
};
use anyhow::{anyhow, Context, Result};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, str::FromStr};
use uuid::Uuid;

pub const EVENT_SCHEMA_VERSION: i32 = 1;
pub const INITIAL_DEPLOYMENT_POLICY_VERSION: &str = "INITIAL_DEPLOYMENT_V1";

pub fn market_evidence_sha256(bar: &MarketBar) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(bar)?)))
}

pub fn current_event_schema(event_type: EventType) -> i32 {
    match event_type {
        EventType::MarketDataReceived => 2,
        EventType::ConfigSnapshotted
        | EventType::GridCycleStarted
        | EventType::GridCycleReset
        | EventType::GridLevelArmed
        | EventType::GridLevelTouched
        | EventType::GridLevelSkipped
        | EventType::GridLevelRearmed
        | EventType::MarketBarDecisionsCommitted
        | EventType::MarketBarProcessed
        | EventType::GridRightGranted
        | EventType::GridRightPartiallyExercised
        | EventType::GridRightExercised
        | EventType::GridRightReleased
        | EventType::GridRightTransferred
        | EventType::GridRightExpired
        | EventType::GridRightRevoked
        | EventType::RightTrancheMinted
        | EventType::RightBalanceTransferred
        | EventType::RightBalanceReserved
        | EventType::RightBalanceReleased
        | EventType::RightBalanceConsumed
        | EventType::RightBalanceRevoked
        | EventType::RightBalanceExpired => 2,
        EventType::GridRightDeferred | EventType::GridRightResidualHeld => 3,
        EventType::GateDecisionMade => 4,
        EventType::GridRightBlocked | EventType::GridRightReserved => 4,
        EventType::OrderIntentCreated => 7,
        EventType::InitialDeploymentEvaluated => 1,
        EventType::OrderPartiallyFilled | EventType::OrderFilled => 3,
        _ => EVENT_SCHEMA_VERSION,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    RunStarted,
    ConfigSnapshotted,
    AlgorithmRegistered,
    PlatformUpgradeAuthorized,
    PlatformUpgradeActivated,
    OngoingResourcePolicyActivated,
    GridCycleStarted,
    GridLevelArmed,
    GridLevelTouched,
    GridLevelSkipped,
    GridLevelRearmed,
    GridRightGranted,
    GridRightDeferred,
    GridRightResidualHeld,
    GridRightBlocked,
    GridRightReserved,
    GridRightPartiallyExercised,
    GridRightExercised,
    GridRightReleased,
    GridRightTransferred,
    GridRightExpired,
    GridRightRevoked,
    RightTrancheMinted,
    RightBalanceTransferred,
    RightBalanceReserved,
    RightBalanceReleased,
    RightBalanceConsumed,
    RightBalanceRevoked,
    RightBalanceExpired,
    DeferredBudgetAccrued,
    DeferredQuantityAccrued,
    GateDecisionMade,
    InitialDeploymentEvaluated,
    OrderIntentCreated,
    OrderSubmitted,
    OrderAccepted,
    OrderCancelRequested,
    OrderPartiallyFilled,
    OrderFilled,
    OrderRejected,
    OrderCancelled,
    LotOpened,
    LotPartiallyClosed,
    LotClosed,
    CashChanged,
    PositionChanged,
    GridCycleReset,
    ReconciliationCompleted,
    ServiceStopped,
    ServiceModeChanged,
    RecoveryCompleted,
    AmbiguousBarDetected,
    TradingDayRolled,
    ErrorRecorded,
    MarketDataReceived,
    MarketBarDecisionsCommitted,
    MarketBarProcessed,
    ReplayInitialized,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = serde_json::to_value(self).map_err(|_| std::fmt::Error)?;
        f.write_str(value.as_str().ok_or(std::fmt::Error)?)
    }
}

impl FromStr for EventType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        serde_json::from_value(Value::String(s.to_owned())).context("unknown event type")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_type: EventType,
    pub schema_version: i32,
    pub run_id: String,
    pub cycle_id: String,
    pub symbol: String,
    pub event_time: NaiveDateTime,
    pub recorded_at: NaiveDateTime,
    pub sequence_number: i64,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub idempotency_key: String,
    pub payload: Value,
    pub config_version: String,
}

impl EventEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_type: EventType,
        run_id: &str,
        cycle_id: &str,
        symbol: &str,
        event_time: NaiveDateTime,
        correlation_id: &str,
        causation_id: Option<String>,
        idempotency_key: String,
        payload: Value,
        config_version: &str,
    ) -> Self {
        let event_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{run_id}:{idempotency_key}").as_bytes(),
        )
        .to_string();
        Self {
            event_id,
            event_type,
            schema_version: current_event_schema(event_type),
            run_id: run_id.to_owned(),
            cycle_id: cycle_id.to_owned(),
            symbol: symbol.to_owned(),
            event_time,
            recorded_at: chrono::Utc::now().naive_utc(),
            sequence_number: 0,
            correlation_id: correlation_id.to_owned(),
            causation_id,
            idempotency_key,
            payload,
            config_version: config_version.to_owned(),
        }
    }
}

impl StrategyState {
    pub fn apply_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let mut next = self.clone();
        next.apply_event_in_place(event)?;
        *self = next;
        Ok(())
    }

    /// Bind the algorithm-facing aggregate right to the exact pre-grant
    /// inventory.  This is deliberately evaluated before the grant and its
    /// marginal mint are applied, so a payload cannot invent carried capacity
    /// and then use its own advertised numbers as proof.
    fn validate_current_quantity_grant(
        &self,
        event: &EventEnvelope,
        right: &GridRight,
    ) -> Result<()> {
        let config = self
            .audited_config
            .as_ref()
            .context("current grid right lacks its audited run configuration")?;
        let grid = crate::grid::GridSpec::from(config);
        if right.cycle_id != self.cycle_id
            || event.cycle_id != self.cycle_id
            || right.symbol != self.symbol
            || right.granted_at != event.event_time
            || right.right_id
                != GridRight::id_for(
                    &self.run_id,
                    &self.cycle_id,
                    right.grid_index,
                    right.granted_at,
                )
            || right.grid_index == 0
            || (right.direction == crate::domain::Direction::Buy && right.grid_index > 0)
            || (right.direction == crate::domain::Direction::Sell && right.grid_index < 0)
            || !grid.is_trade_level(right.grid_index)
            || right.grid_price != grid.price(right.grid_index)?
        {
            return Err(anyhow!(
                "grid right identity does not match its run opportunity"
            ));
        }

        let expected_capacity = crate::rights::RightsGateCoordinator::new(config, self).capacity(
            right.grid_index,
            right.direction,
            event.event_time.date(),
            &right.right_id,
        )?;
        if right.capacity != expected_capacity {
            return Err(anyhow!(
                "grid right capacity does not match the audited pre-grant state"
            ));
        }

        let mut active_ids = BTreeSet::new();
        let mut source_right_ids = BTreeSet::new();
        let mut carried_quantity = 0_i64;
        for tranche in self.right_tranches.values().filter(|tranche| {
            tranche.cycle_id == self.cycle_id
                && tranche.direction == right.direction
                && tranche.available_quantity > 0
        }) {
            active_ids.insert(tranche.tranche_id.clone());
            source_right_ids.insert(tranche.owner_right_id.clone());
            carried_quantity = carried_quantity
                .checked_add(tranche.available_quantity)
                .context("carried quantity overflow")?;
        }

        let marginal_ids: BTreeSet<String> = match right.direction {
            crate::domain::Direction::Buy => [RightTranche::id_for(
                &self.cycle_id,
                &right.right_id,
                crate::domain::Direction::Buy,
                right.grid_index,
                None,
            )]
            .into_iter()
            .collect(),
            crate::domain::Direction::Sell => {
                crate::rights::sell_tranches_to_mint(self, &right.right_id, right.grid_index)
                    .into_iter()
                    .map(|tranche| tranche.tranche_id)
                    .collect()
            }
        };
        let mut expected_tranche_ids = active_ids;
        expected_tranche_ids.extend(marginal_ids);
        let advertised_tranche_ids: BTreeSet<_> =
            right.capacity.tranche_ids.iter().cloned().collect();
        let advertised_source_ids: BTreeSet<_> =
            right.capacity.source_right_ids.iter().cloned().collect();
        if advertised_tranche_ids.len() != right.capacity.tranche_ids.len()
            || advertised_source_ids.len() != right.capacity.source_right_ids.len()
            || advertised_tranche_ids != expected_tranche_ids
            || advertised_source_ids != source_right_ids
            || right.capacity.carried_in_quantity != carried_quantity
        {
            return Err(anyhow!(
                "grid right carry sources do not match the pre-grant tranche inventory"
            ));
        }

        if right.direction == crate::domain::Direction::Buy {
            let expected_deployed: i64 = self
                .grid_rights
                .values()
                .filter(|candidate| {
                    candidate.cycle_id == self.cycle_id
                        && candidate.direction == crate::domain::Direction::Buy
                        && candidate.capacity.available_quantity > 0
                })
                .map(|candidate| {
                    if candidate.status == GridRightStatus::Reserved {
                        candidate
                            .reserved_quantity
                            .max(candidate.exercised_quantity)
                    } else {
                        candidate.exercised_quantity
                    }
                })
                .try_fold(0_i64, i64::checked_add)
                .context("NUMERIC_RANGE_VIOLATION: deployed BUY quantity overflow")?;
            let depth = right
                .grid_index
                .checked_abs()
                .context("grid right depth is not representable")?;
            let expected_indices: Vec<_> = (1..=depth).map(|depth| -depth).collect();
            let expected_available = carried_quantity
                .checked_add(self.audited_standard_quantity)
                .context("NUMERIC_RANGE_VIOLATION: available BUY quantity overflow")?;
            if right.capacity.deployed_quantity != expected_deployed
                || right.capacity.available_quantity != expected_available
                || right.capacity.accumulated_grid_indices != expected_indices
                || right.capacity.t_plus_one_blocked_quantity != 0
                || !right.capacity.t_plus_one_blocked_lot_ids.is_empty()
                || right.capacity.risk_blocked_quantity != 0
                || !right.capacity.risk_blocked_lot_ids.is_empty()
                || right.capacity.no_profit_blocked_quantity != 0
                || !right.capacity.no_profit_blocked_lot_ids.is_empty()
            {
                return Err(anyhow!(
                    "buy right aggregate does not equal its exact fixed-quantity inventory"
                ));
            }
        } else if right.capacity.carried_in_quantity != carried_quantity {
            return Err(anyhow!(
                "sell right carry does not equal its exact lot-sourced inventory"
            ));
        }
        Ok(())
    }

    fn validate_current_order_intent(&self, event: &EventEnvelope, order: &Order) -> Result<()> {
        let intent = &order.intent;
        if intent.origin.is_initial_deployment() {
            return self.validate_initial_deployment_intent(event, order);
        }
        if !intent.origin.is_grid_right() {
            return Err(anyhow!("unknown current order-intent origin"));
        }
        let right = self
            .grid_rights
            .get(&intent.right_id)
            .context("order intent grid right is missing")?;
        let expected_intent_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "{}:{}:{}:{}",
                self.run_id, self.cycle_id, intent.grid_index, intent.created_at
            )
            .as_bytes(),
        )
        .to_string();
        let expected_order_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("paper:order:{}:0", intent.intent_id).as_bytes(),
        )
        .to_string();
        let expected_budget = intent
            .limit_price
            .checked_mul(rust_decimal::Decimal::from(intent.quantity))
            .context("NUMERIC_RANGE_VIOLATION: order intent budget overflow")?;
        if right.status != GridRightStatus::Reserved
            || right.cycle_id != self.cycle_id
            || event.cycle_id != self.cycle_id
            || right.direction != intent.direction
            || right.grid_index != intent.grid_index
            || right.grid_price != intent.limit_price
            || right.reserved_quantity != intent.quantity
            || !right.decision_recorded
            || if right.decision_contract_version
                >= crate::decision::WHOLE_UNIT_DECISION_CONTRACT_VERSION
            {
                right.decision_intent_quantity != intent.quantity
            } else {
                right.decided_exercise_quantity != intent.quantity
            }
            || intent.quantity <= 0
            || self.audited_lot_size <= 0
            || intent.quantity % self.audited_lot_size != 0
            || intent.budget != expected_budget
            || intent.limit_price <= rust_decimal::Decimal::ZERO
            || intent.created_at != event.event_time
            || intent.intent_id != expected_intent_id
            || order.order_id != expected_order_id
            || order.status != OrderStatus::Created
            || order.filled_quantity != 0
            || order.filled_notional != rust_decimal::Decimal::ZERO
            || order.commission_charged != rust_decimal::Decimal::ZERO
            || order.cancel_requested_reason.is_some()
            || order.cancel_requested_at.is_some()
        {
            return Err(anyhow!(
                "order intent is not the exact reserved disposition of its grid right"
            ));
        }

        let reserved_tranches: Vec<_> = self
            .right_tranches
            .values()
            .filter(|tranche| {
                tranche.owner_right_id == right.right_id && tranche.reserved_quantity > 0
            })
            .collect();
        let reserved_quantity: i64 = reserved_tranches
            .iter()
            .try_fold(0_i64, |total, tranche| {
                total.checked_add(tranche.reserved_quantity)
            })
            .context("NUMERIC_RANGE_VIOLATION: tranche reservation overflow")?;
        if reserved_quantity != intent.quantity {
            return Err(anyhow!(
                "order intent quantity is not backed by its tranche reservations"
            ));
        }

        let policy = order
            .intent
            .profit_guard_policy
            .as_ref()
            .context("order intent lacks its audited fee policy")?;
        match intent.direction {
            crate::domain::Direction::Buy => {
                let expected_reservation = if event.schema_version >= 6 {
                    crate::profit::checked_buy_cash_reservation_with_policy(
                        policy,
                        intent.limit_price,
                        intent.quantity,
                    )
                    .context("NUMERIC_RANGE_VIOLATION: BUY reservation overflow")?
                } else {
                    // Schemas 1-5 recorded the historical two-minimum
                    // reservation. Preserve their exact replay contract.
                    let worst_price = intent
                        .limit_price
                        .checked_mul(
                            rust_decimal::Decimal::ONE
                                .checked_add(policy.slippage_rate)
                                .context("legacy BUY slippage factor overflow")?,
                        )
                        .context("legacy BUY adverse price overflow")?
                        .round_dp_with_strategy(
                            policy.price_scale,
                            rust_decimal::RoundingStrategy::ToPositiveInfinity,
                        );
                    let notional = worst_price
                        .checked_mul(rust_decimal::Decimal::from(intent.quantity))
                        .context("legacy BUY notional overflow")?;
                    let commission = notional
                        .checked_mul(policy.commission_rate)
                        .context("legacy BUY commission overflow")?
                        .max(policy.minimum_commission);
                    notional
                        .checked_add(
                            commission
                                .checked_mul(rust_decimal::Decimal::from(2))
                                .context("legacy BUY duplicated commission overflow")?,
                        )
                        .context("legacy BUY reservation overflow")?
                };
                if !intent.target_lot_ids.is_empty()
                    || !intent.target_lot_allocations.is_empty()
                    || !intent.profit_guard_version.is_empty()
                    || order.reserved_cash_remaining != expected_reservation
                    || order.reserved_quantity_remaining != 0
                {
                    return Err(anyhow!("buy order intent has a non-canonical reservation"));
                }
            }
            crate::domain::Direction::Sell => {
                let allocation_lots: Vec<_> = intent
                    .target_lot_allocations
                    .iter()
                    .map(|allocation| allocation.lot_id.clone())
                    .collect();
                let reserved_allocations: std::collections::BTreeMap<_, _> = reserved_tranches
                    .iter()
                    .map(|tranche| {
                        (
                            (
                                tranche.lot_id.clone().unwrap_or_default(),
                                tranche.tranche_id.clone(),
                            ),
                            tranche.reserved_quantity,
                        )
                    })
                    .collect();
                let intent_allocations: std::collections::BTreeMap<_, _> = intent
                    .target_lot_allocations
                    .iter()
                    .map(|allocation| {
                        (
                            (allocation.lot_id.clone(), allocation.tranche_id.clone()),
                            allocation.quantity,
                        )
                    })
                    .collect();
                if intent.target_lot_ids != allocation_lots
                    || reserved_allocations.len() != reserved_tranches.len()
                    || intent_allocations.len() != intent.target_lot_allocations.len()
                    || reserved_allocations != intent_allocations
                    || order.reserved_cash_remaining != rust_decimal::Decimal::ZERO
                    || order.reserved_quantity_remaining != intent.quantity
                {
                    return Err(anyhow!("sell order intent has a non-canonical reservation"));
                }
            }
        }
        Ok(())
    }

    fn validate_initial_deployment_evaluation(
        &self,
        event: &EventEnvelope,
        evaluation: &InitialDeploymentEvaluation,
    ) -> Result<()> {
        let config = self
            .audited_config
            .as_ref()
            .context("initial deployment lacks its audited configuration")?;
        let settings = config
            .gate
            .capital_inventory
            .as_ref()
            .context("initial deployment lacks capital-inventory settings")?;
        let pending = self
            .pending_market_evidence
            .as_ref()
            .context("initial deployment lacks an unprocessed market source")?;
        let expected_deployment_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:initial-deployment:v1", self.run_id).as_bytes(),
        )
        .to_string();
        let plan = crate::risk::canonical_initial_deployment_plan(config, self, pending.bar.close)
            .context("initial deployment plan is not representable")?;
        let expected_outcome = if plan.selected_units > 0 {
            InitialDeploymentOutcome::Authorized
        } else {
            InitialDeploymentOutcome::Blocked
        };
        let expected_reason = if plan.selected_units > 0 {
            "AUTHORIZED"
        } else {
            "NO_AFFORDABLE_UNITS"
        };
        if !settings.deploy_initial_balance
            || self.initial_deployment_evaluation.is_some()
            || event.schema_version != 1
            || event.event_id == pending.event_id
            || event.event_time != pending.bar.timestamp
            || event.symbol != pending.bar.symbol
            || event.cycle_id != self.cycle_id
            || evaluation.deployment_id != expected_deployment_id
            || evaluation.policy_version != INITIAL_DEPLOYMENT_POLICY_VERSION
            || evaluation.source_market_event_id != pending.event_id
            || evaluation.source_evidence_sha256 != pending.evidence_sha256
            || evaluation.symbol != pending.bar.symbol
            || evaluation.market_time != pending.bar.timestamp
            || evaluation.limit_price != pending.bar.close
            || evaluation.pre_available_cash != self.cash.available
            || evaluation.pre_frozen_cash != self.cash.frozen
            || evaluation.pre_position != self.position.total
            || evaluation.pre_pending_buy_quantity != plan.pending_buy_quantity
            || evaluation.minimum_free_cash != settings.minimum_free_cash
            || evaluation.standard_quantity != config.standard_quantity
            || evaluation.maximum_units != plan.maximum_units
            || evaluation.selected_units != plan.selected_units
            || evaluation.quantity != plan.quantity
            || evaluation.canonical_reservation != plan.reservation
            || evaluation.projected_remaining_cash != plan.projected_remaining_cash
            || evaluation.outcome != expected_outcome
            || evaluation.reason != expected_reason
        {
            return Err(anyhow!(
                "initial deployment evaluation differs from its durable market and cash evidence"
            ));
        }
        Ok(())
    }

    fn validate_initial_deployment_intent(
        &self,
        event: &EventEnvelope,
        order: &Order,
    ) -> Result<()> {
        let intent = &order.intent;
        let OrderIntentOrigin::InitialDeploymentV1 {
            deployment_id,
            source_market_event_id,
            source_evidence_sha256,
            policy_version,
        } = &intent.origin
        else {
            return Err(anyhow!("initial deployment lacks its typed origin"));
        };
        let config = self
            .audited_config
            .as_ref()
            .context("initial deployment lacks its audited configuration")?;
        if event.schema_version != 7
            || !config
                .gate
                .capital_inventory
                .as_ref()
                .is_some_and(|settings| settings.deploy_initial_balance)
            || self
                .orders
                .values()
                .any(|existing| existing.intent.origin.is_initial_deployment())
        {
            return Err(anyhow!("initial deployment is not unique and enabled"));
        }
        let evaluation = self
            .initial_deployment_evaluation
            .as_ref()
            .context("initial deployment intent lacks its durable evaluation")?;
        let plan = crate::risk::canonical_initial_deployment_plan(config, self, intent.limit_price)
            .context("initial deployment quantity is not canonical")?;
        let expected_quantity = plan.quantity;
        let expected_intent_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:initial-deployment:v1", self.run_id).as_bytes(),
        )
        .to_string();
        let expected_order_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("paper:order:{expected_intent_id}:0").as_bytes(),
        )
        .to_string();
        let expected_budget = intent
            .limit_price
            .checked_mul(rust_decimal::Decimal::from(intent.quantity))
            .context("NUMERIC_RANGE_VIOLATION: initial deployment budget overflow")?;
        let policy = crate::domain::ProfitGuardPolicy::from(config);
        let expected_reservation = crate::profit::checked_buy_cash_reservation_with_policy(
            &policy,
            intent.limit_price,
            intent.quantity,
        )
        .context("NUMERIC_RANGE_VIOLATION: initial deployment reservation overflow")?;
        if evaluation.outcome != InitialDeploymentOutcome::Authorized
            || evaluation.selected_units <= 0
            || deployment_id != &evaluation.deployment_id
            || source_market_event_id != &evaluation.source_market_event_id
            || source_evidence_sha256 != &evaluation.source_evidence_sha256
            || policy_version != &evaluation.policy_version
            || policy_version != INITIAL_DEPLOYMENT_POLICY_VERSION
            || plan.selected_units != evaluation.selected_units
            || plan.reservation != evaluation.canonical_reservation
            || !intent.right_id.is_empty()
            || expected_quantity <= 0
            || intent.direction != crate::domain::Direction::Buy
            || intent.grid_index != 0
            || intent.quantity != expected_quantity
            || intent.quantity % config.standard_quantity != 0
            || intent.budget != expected_budget
            || intent.created_at != event.event_time
            || event.cycle_id != self.cycle_id
            || intent.intent_id != expected_intent_id
            || order.order_id != expected_order_id
            || !intent.target_lot_ids.is_empty()
            || !intent.target_lot_allocations.is_empty()
            || !intent.profit_guard_version.is_empty()
            || intent.profit_guard_policy.as_ref() != Some(&policy)
            || order.status != OrderStatus::Created
            || order.filled_quantity != 0
            || order.filled_notional != rust_decimal::Decimal::ZERO
            || order.commission_charged != rust_decimal::Decimal::ZERO
            || order.reserved_cash_remaining != expected_reservation
            || order.reserved_quantity_remaining != 0
            || order.cancel_requested_reason.is_some()
            || order.cancel_requested_at.is_some()
        {
            return Err(anyhow!(
                "initial deployment order differs from its audited cash-floor plan"
            ));
        }
        Ok(())
    }

    fn apply_event_in_place(&mut self, event: &EventEnvelope) -> Result<()> {
        let supported = match event.event_type {
            EventType::MarketDataReceived => matches!(event.schema_version, 1 | 2),
            EventType::ConfigSnapshotted
            | EventType::GridCycleStarted
            | EventType::GridCycleReset
            | EventType::GridLevelArmed
            | EventType::GridLevelTouched
            | EventType::GridLevelSkipped
            | EventType::GridLevelRearmed
            | EventType::MarketBarDecisionsCommitted
            | EventType::MarketBarProcessed
            | EventType::GridRightGranted
            | EventType::GridRightPartiallyExercised
            | EventType::GridRightExercised
            | EventType::GridRightReleased
            | EventType::GridRightTransferred
            | EventType::GridRightExpired
            | EventType::GridRightRevoked
            | EventType::RightTrancheMinted
            | EventType::RightBalanceTransferred
            | EventType::RightBalanceReserved
            | EventType::RightBalanceReleased
            | EventType::RightBalanceConsumed
            | EventType::RightBalanceRevoked
            | EventType::RightBalanceExpired => matches!(event.schema_version, 1..=3),
            EventType::GateDecisionMade => matches!(event.schema_version, 1..=4),
            EventType::GridRightDeferred => matches!(event.schema_version, 1..=3),
            EventType::GridRightBlocked | EventType::GridRightReserved => {
                matches!(event.schema_version, 1..=4)
            }
            EventType::GridRightResidualHeld => event.schema_version == 3,
            EventType::OrderIntentCreated => matches!(event.schema_version, 1..=7),
            EventType::OrderPartiallyFilled | EventType::OrderFilled => {
                matches!(event.schema_version, 1..=3)
            }
            _ => event.schema_version == current_event_schema(event.event_type),
        };
        if !supported {
            return Err(anyhow!(
                "unsupported event schema version {}",
                event.schema_version
            ));
        }
        self.event_count = self
            .event_count
            .checked_add(1)
            .context("NUMERIC_RANGE_VIOLATION: event counter overflow")?;
        match event.event_type {
            EventType::GridLevelArmed => {
                let level: GridLevelState = serde_json::from_value(event.payload.clone())?;
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current grid arm lacks its audited configuration")?;
                    let expected = crate::grid::GridSpec::from(config)
                        .levels()?
                        .remove(&level.index)
                        .context("current grid arm has an unknown index")?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || !self.grid_cycle_active
                        || self.levels.contains_key(&level.index)
                        || level.index != expected.index
                        || level.price != expected.price
                        || level.status != GridLevelStatus::Armed
                        || level.touch_count != 0
                    {
                        return Err(anyhow!("current grid arm is not a new canonical level"));
                    }
                }
                self.levels.insert(level.index, level);
            }
            EventType::GridLevelTouched => {
                let index = payload_i32(&event.payload, "grid_index")?;
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current grid touch lacks its audited configuration")?;
                    let canonical_price = crate::grid::GridSpec::from(config).price(index)?;
                    let level = self
                        .levels
                        .get(&index)
                        .context("current grid touch has no armed level")?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || level.price != canonical_price
                        || !matches!(
                            level.status,
                            GridLevelStatus::Armed | GridLevelStatus::Rearmed
                        )
                        || event.payload.get("price").and_then(Value::as_str)
                            != Some(canonical_price.to_string().as_str())
                    {
                        return Err(anyhow!(
                            "current grid touch is not a canonical armed opportunity"
                        ));
                    }
                }
                self.historically_touched_levels.insert(index);
                if let Some(level) = self.levels.get_mut(&index) {
                    level.status = GridLevelStatus::Touched;
                    level.touch_count = level
                        .touch_count
                        .checked_add(1)
                        .context("NUMERIC_RANGE_VIOLATION: touch counter overflow")?;
                }
            }
            EventType::GridLevelSkipped => {
                let index = payload_i32(&event.payload, "grid_index")?;
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current grid skip lacks its audited configuration")?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || self.levels.get(&index).map(|level| level.status)
                            != Some(GridLevelStatus::Touched)
                    {
                        return Err(anyhow!(
                            "current grid skip does not resolve its canonical touch"
                        ));
                    }
                }
                self.historically_skipped_levels.insert(index);
                if let Some(level) = self.levels.get_mut(&index) {
                    level.status = GridLevelStatus::Skipped;
                }
            }
            EventType::GridLevelRearmed => {
                let index = payload_i32(&event.payload, "grid_index")?;
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current grid rearm lacks its audited configuration")?;
                    let canonical_price = crate::grid::GridSpec::from(config).price(index)?;
                    let level = self
                        .levels
                        .get(&index)
                        .context("current grid rearm has no prior level")?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || level.price != canonical_price
                        || !matches!(
                            level.status,
                            GridLevelStatus::Touched
                                | GridLevelStatus::Executed
                                | GridLevelStatus::Skipped
                        )
                    {
                        return Err(anyhow!(
                            "current grid rearm has no canonical terminal touch state"
                        ));
                    }
                }
                if let Some(level) = self.levels.get_mut(&index) {
                    level.status = GridLevelStatus::Rearmed;
                }
            }
            EventType::GridRightGranted => {
                let right: GridRight = serde_json::from_value(event.payload.clone())?;
                if right.status != GridRightStatus::Granted
                    || right.reserved_budget != rust_decimal::Decimal::ZERO
                    || right.reserved_quantity != 0
                    || right.exercised_budget != rust_decimal::Decimal::ZERO
                    || right.exercised_quantity != 0
                    || right.capacity.mechanical_budget_cap < rust_decimal::Decimal::ZERO
                    || right.capacity.deployed_budget < rust_decimal::Decimal::ZERO
                    || right.capacity.available_budget < rust_decimal::Decimal::ZERO
                    || right.capacity.mechanical_quantity_cap < 0
                    || right.capacity.deployed_quantity < 0
                    || right.capacity.available_quantity < 0
                    || right.capacity.eligible_quantity < 0
                    || right.capacity.t_plus_one_blocked_quantity < 0
                    || right.capacity.risk_blocked_quantity < 0
                    || right.capacity.no_profit_blocked_quantity < 0
                    || right.capacity.carried_in_budget < rust_decimal::Decimal::ZERO
                    || right.capacity.carried_in_quantity < 0
                {
                    return Err(anyhow!("invalid granted grid right capacity or state"));
                }
                match right.direction {
                    crate::domain::Direction::Buy => {
                        let quantity_mode = right.capacity.available_quantity > 0
                            || right.capacity.mechanical_quantity_cap > 0;
                        if right.capacity.eligible_quantity != 0
                            || !right.capacity.eligible_lot_ids.is_empty()
                            || right.capacity.no_profit_blocked_quantity != 0
                            || !right.capacity.no_profit_blocked_lot_ids.is_empty()
                            || (quantity_mode
                                && (right.capacity.mechanical_budget_cap
                                    != rust_decimal::Decimal::ZERO
                                    || right.capacity.deployed_budget
                                        != rust_decimal::Decimal::ZERO
                                    || right.capacity.available_budget
                                        != rust_decimal::Decimal::ZERO
                                    || right.capacity.carried_in_budget
                                        != rust_decimal::Decimal::ZERO))
                            || (!quantity_mode
                                && (right.capacity.mechanical_quantity_cap != 0
                                    || right.capacity.deployed_quantity != 0
                                    || right.capacity.available_quantity != 0
                                    || right.capacity.carried_in_quantity != 0))
                        {
                            return Err(anyhow!(
                                "buy right mixes quantity and legacy budget capacity"
                            ));
                        }
                        if event.schema_version >= 2 {
                            let standard = self.audited_standard_quantity;
                            let expected_cap = i64::from(
                                right
                                    .grid_index
                                    .checked_abs()
                                    .context("grid right depth is not representable")?,
                            )
                            .checked_mul(standard)
                            .context("buy mechanical quantity cap overflow")?;
                            if !quantity_mode
                                || standard <= 0
                                || self.audited_lot_size <= 0
                                || standard % self.audited_lot_size != 0
                                || right.grid_index >= 0
                                || right.capacity.mechanical_quantity_cap != expected_cap
                                || right.capacity.available_quantity
                                    != right.capacity.carried_in_quantity + standard
                                || right.capacity.available_quantity > expected_cap
                                || right.capacity.deployed_quantity
                                    + right.capacity.available_quantity
                                    > expected_cap
                            {
                                return Err(anyhow!(
                                    "buy right violates the run fixed-quantity contract"
                                ));
                            }
                        }
                    }
                    crate::domain::Direction::Sell
                        if right.capacity.mechanical_budget_cap != rust_decimal::Decimal::ZERO
                            || right.capacity.available_budget != rust_decimal::Decimal::ZERO
                            || right.capacity.carried_in_budget != rust_decimal::Decimal::ZERO
                            || right.capacity.mechanical_quantity_cap != 0
                            || right.capacity.deployed_quantity != 0
                            || right.capacity.available_quantity != 0 =>
                    {
                        return Err(anyhow!("sell right cannot carry buy budget capacity"));
                    }
                    _ => {}
                }
                if event.schema_version >= 2 {
                    self.validate_current_quantity_grant(event, &right)?;
                }
                if self.grid_rights.contains_key(&right.right_id) {
                    return Err(anyhow!("grid right cannot be granted twice"));
                }
                self.grid_rights.insert(right.right_id.clone(), right);
            }
            EventType::RightTrancheMinted => {
                let tranche: RightTranche = serde_json::from_value(event.payload.clone())?;
                tranche.validate()?;
                let owner = self
                    .grid_rights
                    .get(&tranche.owner_right_id)
                    .context("tranche owner right is missing")?;
                if owner.cycle_id != tranche.cycle_id
                    || owner.direction != tranche.direction
                    || owner.grid_index != tranche.birth_grid_index
                    || !owner.capacity.tranche_ids.contains(&tranche.tranche_id)
                    || self.right_tranches.contains_key(&tranche.tranche_id)
                {
                    return Err(anyhow!("invalid or duplicate right tranche mint"));
                }
                if event.schema_version >= 2
                    && (self.audited_standard_quantity <= 0
                        || self.audited_lot_size <= 0
                        || (tranche.direction == crate::domain::Direction::Buy
                            && (tranche.unit != RightTrancheUnit::Quantity
                                || tranche.lot_id.is_some()
                                || tranche.minted_quantity != self.audited_standard_quantity))
                        || (tranche.direction == crate::domain::Direction::Sell
                            && (tranche.unit != RightTrancheUnit::Quantity
                                || tranche.lot_id.is_none())))
                {
                    return Err(anyhow!(
                        "right tranche mint violates the run fixed-quantity contract"
                    ));
                }
                if event.schema_version >= 2 && tranche.direction == crate::domain::Direction::Sell
                {
                    let canonical = crate::rights::sell_tranches_to_mint(
                        self,
                        &owner.right_id,
                        owner.grid_index,
                    );
                    if !canonical.iter().any(|expected| expected == &tranche) {
                        return Err(anyhow!(
                            "sell tranche mint lacks an exact canonical lot source"
                        ));
                    }
                }
                self.right_tranches
                    .insert(tranche.tranche_id.clone(), tranche);
            }
            EventType::RightBalanceTransferred => {
                let source_right_id = event
                    .payload
                    .get("source_right_id")
                    .and_then(Value::as_str)
                    .context("missing source_right_id")?;
                let target_right_id = event
                    .payload
                    .get("target_right_id")
                    .and_then(Value::as_str)
                    .context("missing target_right_id")?;
                let source = self
                    .grid_rights
                    .get(source_right_id)
                    .context("transfer source right is missing")?;
                let target = self
                    .grid_rights
                    .get(target_right_id)
                    .context("transfer target right is missing")?;
                if source.cycle_id != target.cycle_id || source.direction != target.direction {
                    return Err(anyhow!("right balance must transfer strictly deeper"));
                }
                if event.schema_version >= 2 {
                    let source_depth = source.grid_index.abs();
                    let target_depth = target.grid_index.abs();
                    let logical_frontier = self
                        .right_tranches
                        .values()
                        .filter(|candidate| {
                            candidate.owner_right_id == source_right_id
                                && (candidate.available_budget > rust_decimal::Decimal::ZERO
                                    || candidate.reserved_budget > rust_decimal::Decimal::ZERO
                                    || candidate.available_quantity > 0
                                    || candidate.reserved_quantity > 0
                                    || (source.direction == crate::domain::Direction::Buy
                                        && (candidate.consumed_budget
                                            > rust_decimal::Decimal::ZERO
                                            || candidate.consumed_quantity > 0)))
                        })
                        .map(|candidate| candidate.birth_grid_index.abs())
                        .max()
                        .unwrap_or(0);
                    let reversal_evidence = target_depth > source_depth
                        || self.right_tranches.values().any(|candidate| {
                            candidate.owner_right_id == source_right_id
                                && candidate.birth_grid_index.abs() == target_depth
                                && (candidate.revoked_budget > rust_decimal::Decimal::ZERO
                                    || candidate.revoked_quantity > 0)
                        });
                    if target_depth <= logical_frontier || !reversal_evidence {
                        return Err(anyhow!(
                            "right balance target is not beyond the active excursion frontier"
                        ));
                    }
                }
                let tranche_ids = event
                    .payload
                    .get("tranche_ids")
                    .and_then(Value::as_array)
                    .context("missing tranche_ids")?;
                if tranche_ids.is_empty() {
                    return Err(anyhow!("right balance transfer cannot be empty"));
                }
                for tranche_id in tranche_ids {
                    let tranche_id = tranche_id.as_str().context("invalid transfer tranche id")?;
                    let tranche = self
                        .right_tranches
                        .get_mut(tranche_id)
                        .context("transferred tranche is missing")?;
                    if tranche.owner_right_id != source_right_id
                        || !target.capacity.tranche_ids.contains(&tranche.tranche_id)
                        || tranche.reserved_budget != rust_decimal::Decimal::ZERO
                        || tranche.reserved_quantity != 0
                        || (tranche.available_budget == rust_decimal::Decimal::ZERO
                            && tranche.available_quantity == 0)
                    {
                        return Err(anyhow!("tranche is not transferable"));
                    }
                    tranche.owner_right_id = target_right_id.to_owned();
                    tranche.validate()?;
                }
            }
            EventType::RightBalanceReserved
            | EventType::RightBalanceReleased
            | EventType::RightBalanceConsumed
            | EventType::RightBalanceRevoked
            | EventType::RightBalanceExpired => {
                let right_id = event
                    .payload
                    .get("right_id")
                    .and_then(Value::as_str)
                    .context("missing tranche allocation right_id")?;
                let allocations = event
                    .payload
                    .get("allocations")
                    .and_then(Value::as_array)
                    .context("missing tranche allocations")?;
                if allocations.is_empty() {
                    return Err(anyhow!("tranche allocation cannot be empty"));
                }
                for allocation in allocations {
                    let tranche_id = allocation
                        .get("tranche_id")
                        .and_then(Value::as_str)
                        .context("missing allocation tranche_id")?;
                    let budget = allocation
                        .get("budget")
                        .and_then(Value::as_str)
                        .unwrap_or("0")
                        .parse::<rust_decimal::Decimal>()?;
                    let quantity = allocation
                        .get("quantity")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let tranche = self
                        .right_tranches
                        .get_mut(tranche_id)
                        .context("allocated tranche is missing")?;
                    if tranche.owner_right_id != right_id
                        || budget < rust_decimal::Decimal::ZERO
                        || quantity < 0
                        || (budget == rust_decimal::Decimal::ZERO && quantity == 0)
                        || (tranche.unit == RightTrancheUnit::Budget && quantity != 0)
                        || (tranche.unit == RightTrancheUnit::Quantity
                            && budget != rust_decimal::Decimal::ZERO)
                    {
                        return Err(anyhow!("invalid tranche allocation"));
                    }
                    match event.event_type {
                        EventType::RightBalanceReserved => {
                            if tranche.available_budget < budget
                                || tranche.available_quantity < quantity
                            {
                                return Err(anyhow!("tranche reservation exceeds availability"));
                            }
                            let available_budget = tranche
                                .available_budget
                                .checked_sub(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget underflow")?;
                            let reserved_budget = tranche
                                .reserved_budget
                                .checked_add(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget overflow")?;
                            let available_quantity = tranche
                                .available_quantity
                                .checked_sub(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity underflow")?;
                            let reserved_quantity = tranche
                                .reserved_quantity
                                .checked_add(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity overflow")?;
                            tranche.available_budget = available_budget;
                            tranche.reserved_budget = reserved_budget;
                            tranche.available_quantity = available_quantity;
                            tranche.reserved_quantity = reserved_quantity;
                        }
                        EventType::RightBalanceReleased => {
                            if tranche.reserved_budget < budget
                                || tranche.reserved_quantity < quantity
                            {
                                return Err(anyhow!("tranche release exceeds reservation"));
                            }
                            let reserved_budget = tranche
                                .reserved_budget
                                .checked_sub(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget underflow")?;
                            let available_budget = tranche
                                .available_budget
                                .checked_add(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget overflow")?;
                            let reserved_quantity = tranche
                                .reserved_quantity
                                .checked_sub(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity underflow")?;
                            let available_quantity = tranche
                                .available_quantity
                                .checked_add(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity overflow")?;
                            tranche.reserved_budget = reserved_budget;
                            tranche.available_budget = available_budget;
                            tranche.reserved_quantity = reserved_quantity;
                            tranche.available_quantity = available_quantity;
                        }
                        EventType::RightBalanceConsumed => {
                            if tranche.reserved_budget < budget
                                || tranche.reserved_quantity < quantity
                            {
                                return Err(anyhow!("tranche consumption exceeds reservation"));
                            }
                            let reserved_budget = tranche
                                .reserved_budget
                                .checked_sub(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget underflow")?;
                            let consumed_budget = tranche
                                .consumed_budget
                                .checked_add(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget overflow")?;
                            let reserved_quantity = tranche
                                .reserved_quantity
                                .checked_sub(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity underflow")?;
                            let consumed_quantity = tranche
                                .consumed_quantity
                                .checked_add(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity overflow")?;
                            tranche.reserved_budget = reserved_budget;
                            tranche.consumed_budget = consumed_budget;
                            tranche.reserved_quantity = reserved_quantity;
                            tranche.consumed_quantity = consumed_quantity;
                        }
                        EventType::RightBalanceRevoked => {
                            let tranche_depth = tranche
                                .birth_grid_index
                                .checked_abs()
                                .context("tranche grid depth is not representable")?;
                            let reversal_depth = event
                                .payload
                                .get("reversal_from_grid")
                                .and_then(Value::as_i64)
                                .and_then(|value| i32::try_from(value).ok())
                                .context("missing reversal_from_grid")?
                                .checked_abs()
                                .context("reversal grid depth is not representable")?;
                            if tranche.available_budget < budget
                                || tranche.available_quantity < quantity
                                || tranche_depth != reversal_depth
                            {
                                return Err(anyhow!("invalid tranche revocation"));
                            }
                            let available_budget = tranche
                                .available_budget
                                .checked_sub(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget underflow")?;
                            let revoked_budget = tranche
                                .revoked_budget
                                .checked_add(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget overflow")?;
                            let available_quantity = tranche
                                .available_quantity
                                .checked_sub(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity underflow")?;
                            let revoked_quantity =
                                tranche.revoked_quantity.checked_add(quantity).context(
                                    "NUMERIC_RANGE_VIOLATION: tranche quantity overflow",
                                )?;
                            tranche.available_budget = available_budget;
                            tranche.revoked_budget = revoked_budget;
                            tranche.available_quantity = available_quantity;
                            tranche.revoked_quantity = revoked_quantity;
                        }
                        EventType::RightBalanceExpired => {
                            if tranche.available_budget < budget
                                || tranche.available_quantity < quantity
                                || tranche.reserved_budget != rust_decimal::Decimal::ZERO
                                || tranche.reserved_quantity != 0
                            {
                                return Err(anyhow!("invalid tranche expiry"));
                            }
                            let available_budget = tranche
                                .available_budget
                                .checked_sub(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget underflow")?;
                            let expired_budget = tranche
                                .expired_budget
                                .checked_add(budget)
                                .context("NUMERIC_RANGE_VIOLATION: tranche budget overflow")?;
                            let available_quantity = tranche
                                .available_quantity
                                .checked_sub(quantity)
                                .context("NUMERIC_RANGE_VIOLATION: tranche quantity underflow")?;
                            let expired_quantity =
                                tranche.expired_quantity.checked_add(quantity).context(
                                    "NUMERIC_RANGE_VIOLATION: tranche quantity overflow",
                                )?;
                            tranche.available_budget = available_budget;
                            tranche.expired_budget = expired_budget;
                            tranche.available_quantity = available_quantity;
                            tranche.expired_quantity = expired_quantity;
                        }
                        _ => unreachable!(),
                    }
                    tranche.validate()?;
                }
            }
            EventType::GridRightDeferred
            | EventType::GridRightResidualHeld
            | EventType::GridRightBlocked
            | EventType::GridRightReserved
            | EventType::GridRightPartiallyExercised
            | EventType::GridRightExercised
            | EventType::GridRightReleased
            | EventType::GridRightTransferred
            | EventType::GridRightExpired
            | EventType::GridRightRevoked => {
                let right_id = event
                    .payload
                    .get("right_id")
                    .and_then(Value::as_str)
                    .context("missing right_id")?;
                if event.event_type == EventType::GridRightTransferred {
                    let target = event
                        .payload
                        .get("target_right_id")
                        .and_then(Value::as_str)
                        .context("transferred right requires target_right_id")?;
                    let tranche_ids = event
                        .payload
                        .get("tranche_ids")
                        .and_then(Value::as_array)
                        .context("transferred right requires tranche_ids")?;
                    let mut transferred_budget = rust_decimal::Decimal::ZERO;
                    let mut transferred_quantity = 0_i64;
                    for tranche_id in tranche_ids {
                        let tranche_id = tranche_id
                            .as_str()
                            .context("invalid transferred tranche id")?;
                        let tranche = self
                            .right_tranches
                            .get(tranche_id)
                            .context("transferred tranche is missing")?;
                        if tranche.owner_right_id != target {
                            return Err(anyhow!("transferred tranche target ownership mismatch"));
                        }
                        transferred_budget = transferred_budget
                            .checked_add(tranche.available_budget)
                            .context("NUMERIC_RANGE_VIOLATION: transferred budget overflow")?;
                        transferred_quantity = transferred_quantity
                            .checked_add(tranche.available_quantity)
                            .context("NUMERIC_RANGE_VIOLATION: transferred quantity overflow")?;
                    }
                    if payload_decimal(&event.payload, "transferred_budget")? != transferred_budget
                        || event
                            .payload
                            .get("transferred_quantity")
                            .and_then(Value::as_i64)
                            .context("missing transferred_quantity")?
                            != transferred_quantity
                    {
                        return Err(anyhow!(
                            "transferred right aggregate does not match tranches"
                        ));
                    }
                }
                let tranche_remaining = if event.event_type == EventType::GridRightExpired
                    && event.schema_version >= 2
                    && event.payload.get("reason").and_then(Value::as_str)
                        == Some("GRID_CYCLE_COMPLETED")
                {
                    let owned: Vec<_> = self
                        .right_tranches
                        .values()
                        .filter(|tranche| tranche.owner_right_id == right_id)
                        .collect();
                    (!owned.is_empty()).then(|| {
                        (
                            owned
                                .iter()
                                .map(|tranche| tranche.available_budget)
                                .sum::<rust_decimal::Decimal>(),
                            owned
                                .iter()
                                .map(|tranche| tranche.available_quantity)
                                .sum::<i64>(),
                        )
                    })
                } else {
                    None
                };
                let right = self
                    .grid_rights
                    .get_mut(right_id)
                    .with_context(|| format!("grid right {right_id} missing"))?;
                let quantity_buy = right.direction == crate::domain::Direction::Buy
                    && right.capacity.available_quantity > 0;
                if event.event_type == EventType::GridRightBlocked
                    && event
                        .payload
                        .get("partial")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                {
                    if right.status != GridRightStatus::Granted {
                        return Err(anyhow!(
                            "partial sell-right blocker requires a newly granted right"
                        ));
                    }
                    let decision_id = event
                        .payload
                        .get("decision_id")
                        .and_then(Value::as_str)
                        .context("partial sell-right blocker requires decision_id")?;
                    if right
                        .decision_id
                        .as_deref()
                        .is_some_and(|existing| existing != decision_id)
                    {
                        return Err(anyhow!("grid right decision identity cannot change"));
                    }
                    right.decision_id = Some(decision_id.to_owned());
                    let blocked_t1 = event
                        .payload
                        .get("t_plus_one_blocked_quantity")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let blocked_risk = event
                        .payload
                        .get("risk_blocked_quantity")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    let blocked_no_profit = event
                        .payload
                        .get("no_profit_blocked_quantity")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    if right.direction != crate::domain::Direction::Sell
                        || blocked_t1 < 0
                        || blocked_risk < 0
                        || blocked_no_profit < 0
                        || blocked_t1 + blocked_risk + blocked_no_profit == 0
                        || blocked_t1 > right.capacity.t_plus_one_blocked_quantity
                        || blocked_risk > right.capacity.risk_blocked_quantity
                        || blocked_no_profit > right.capacity.no_profit_blocked_quantity
                    {
                        return Err(anyhow!("invalid partial sell-right blocker audit"));
                    }
                    return Ok(());
                }
                let next = match event.event_type {
                    EventType::GridRightDeferred => GridRightStatus::Deferred,
                    EventType::GridRightResidualHeld => GridRightStatus::Residual,
                    EventType::GridRightBlocked => GridRightStatus::Blocked,
                    EventType::GridRightReserved => GridRightStatus::Reserved,
                    EventType::GridRightPartiallyExercised => GridRightStatus::PartiallyExercised,
                    EventType::GridRightExercised => GridRightStatus::Exercised,
                    EventType::GridRightReleased => GridRightStatus::Released,
                    EventType::GridRightTransferred => GridRightStatus::Transferred,
                    EventType::GridRightExpired => GridRightStatus::Expired,
                    EventType::GridRightRevoked => GridRightStatus::Revoked,
                    _ => unreachable!(),
                };
                if !right.status.can_transition(next) {
                    return Err(anyhow!(
                        "invalid grid right transition from {:?} to {:?}",
                        right.status,
                        next
                    ));
                }
                if let Some(decision_id) = event.payload.get("decision_id").and_then(Value::as_str)
                {
                    if right
                        .decision_id
                        .as_deref()
                        .is_some_and(|existing| existing != decision_id)
                    {
                        return Err(anyhow!("grid right decision identity cannot change"));
                    }
                    right.decision_id = Some(decision_id.to_owned());
                }
                if matches!(
                    next,
                    GridRightStatus::Residual
                        | GridRightStatus::Deferred
                        | GridRightStatus::Blocked
                        | GridRightStatus::Reserved
                ) && right.decision_id.is_none()
                {
                    return Err(anyhow!("grid right disposition requires decision_id"));
                }
                if event.schema_version >= 3
                    && matches!(
                        next,
                        GridRightStatus::Residual
                            | GridRightStatus::Deferred
                            | GridRightStatus::Blocked
                            | GridRightStatus::Reserved
                    )
                {
                    let gross = event
                        .payload
                        .get("gross_available_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks gross_available_quantity")?;
                    let residual = event
                        .payload
                        .get("platform_residual_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks platform_residual_quantity")?;
                    let authorized = event
                        .payload
                        .get("algorithm_authorized_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks algorithm_authorized_quantity")?;
                    let blocked = event
                        .payload
                        .get("platform_blocked_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks platform_blocked_quantity")?;
                    let intent = event
                        .payload
                        .get("order_intent_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks order_intent_quantity")?;
                    let remaining = event
                        .payload
                        .get("remaining_decision_quantity")
                        .and_then(Value::as_i64)
                        .context("current disposition lacks remaining_decision_quantity")?;
                    let (offered, pre_blocked, post_blocked) = if right.decision_contract_version
                        >= crate::decision::DECISION_CONTRACT_VERSION
                    {
                        (
                            event
                                .payload
                                .get("algorithm_offered_quantity")
                                .and_then(Value::as_i64)
                                .context("v4 disposition lacks algorithm_offered_quantity")?,
                            event
                                .payload
                                .get("predecision_blocked_quantity")
                                .and_then(Value::as_i64)
                                .context("v4 disposition lacks predecision block")?,
                            event
                                .payload
                                .get("postdecision_blocked_quantity")
                                .and_then(Value::as_i64)
                                .context("v4 disposition lacks postdecision block")?,
                        )
                    } else {
                        (authorized, 0, blocked)
                    };
                    let unit = self.audited_standard_quantity;
                    if right.decision_contract_version
                        < crate::decision::WHOLE_UNIT_DECISION_CONTRACT_VERSION
                        || unit <= 0
                        || gross != right.decision_gross_available_quantity
                        || residual != right.decision_platform_residual_quantity
                        || authorized != right.decision_algorithm_authorized_quantity
                        || authorized
                            .checked_add(residual)
                            .is_none_or(|quantity| gross != quantity)
                        || offered
                            .checked_add(pre_blocked)
                            .is_none_or(|quantity| authorized != quantity)
                        || right
                            .decided_exercise_quantity
                            .checked_add(right.decided_defer_quantity)
                            .is_none_or(|quantity| offered != quantity)
                        || intent
                            .checked_add(post_blocked)
                            .is_none_or(|quantity| right.decided_exercise_quantity != quantity)
                        || pre_blocked
                            .checked_add(post_blocked)
                            .is_none_or(|quantity| blocked != quantity)
                        || residual < 0
                        || residual >= unit
                        || offered < 0
                        || pre_blocked < 0
                        || post_blocked < 0
                        || blocked < 0
                        || intent < 0
                        || remaining < 0
                        || right
                            .decided_defer_quantity
                            .checked_add(blocked)
                            .is_none_or(|quantity| remaining != quantity)
                        || [
                            authorized,
                            offered,
                            pre_blocked,
                            right.decided_exercise_quantity,
                            right.decided_defer_quantity,
                            blocked,
                            post_blocked,
                            intent,
                            remaining,
                        ]
                        .iter()
                        .any(|quantity| quantity % unit != 0)
                        || (next == GridRightStatus::Deferred
                            && (right.decided_exercise_quantity != 0
                                || post_blocked != 0
                                || intent != 0))
                        || (next == GridRightStatus::Residual
                            && (authorized != 0
                                || residual <= 0
                                || right.decided_exercise_quantity != 0
                                || right.decided_defer_quantity != 0
                                || blocked != 0
                                || intent != 0
                                || remaining != 0))
                        || (next == GridRightStatus::Blocked
                            && (intent != 0 || post_blocked != right.decided_exercise_quantity))
                        || (next == GridRightStatus::Reserved && intent <= 0)
                    {
                        return Err(anyhow!(
                            "current grid-right disposition violates C/P/A/E/D/B/I/R"
                        ));
                    }
                    right.decision_platform_blocked_quantity = blocked;
                    right.decision_algorithm_offered_quantity = offered;
                    right.decision_pre_blocked_quantity = pre_blocked;
                    right.decision_post_blocked_quantity = post_blocked;
                    right.decision_intent_quantity = intent;
                    right.decision_remaining_quantity = remaining;
                }
                let mut has_reservation = false;
                let mut has_exercise = false;
                if let Some(value) = event.payload.get("reserved_budget").and_then(Value::as_str) {
                    let value: rust_decimal::Decimal = value.parse()?;
                    if right.direction != crate::domain::Direction::Buy {
                        if value != rust_decimal::Decimal::ZERO {
                            return Err(anyhow!("sell right cannot reserve buy budget"));
                        }
                    } else if value <= rust_decimal::Decimal::ZERO
                        || value > right.capacity.available_budget
                    {
                        return Err(anyhow!("invalid buy-right budget reservation"));
                    } else {
                        right.reserved_budget = value;
                        has_reservation = true;
                    }
                }
                if let Some(value) = event
                    .payload
                    .get("reserved_quantity")
                    .and_then(Value::as_i64)
                {
                    if right.direction == crate::domain::Direction::Buy && !quantity_buy {
                        if value != 0 {
                            return Err(anyhow!("buy right cannot reserve sell quantity"));
                        }
                    } else if value <= 0
                        || value
                            > if quantity_buy {
                                right.capacity.available_quantity
                            } else {
                                right.capacity.eligible_quantity
                            }
                    {
                        return Err(anyhow!("invalid right quantity reservation"));
                    } else {
                        right.reserved_quantity = value;
                        has_reservation = true;
                    }
                }
                if let Some(value) = event
                    .payload
                    .get("exercised_budget_delta")
                    .and_then(Value::as_str)
                {
                    let delta = value.parse::<rust_decimal::Decimal>()?;
                    if right.direction != crate::domain::Direction::Buy {
                        if delta != rust_decimal::Decimal::ZERO {
                            return Err(anyhow!("sell right cannot exercise buy budget"));
                        }
                    } else if delta <= rust_decimal::Decimal::ZERO {
                        return Err(anyhow!("invalid buy-right exercise delta"));
                    } else {
                        let exercised = right
                            .exercised_budget
                            .checked_add(delta)
                            .context("NUMERIC_RANGE_VIOLATION: exercised budget overflow")?;
                        if exercised > right.reserved_budget {
                            return Err(anyhow!("invalid buy-right exercise delta"));
                        }
                        right.exercised_budget = exercised;
                        has_exercise = true;
                    }
                }
                if let Some(value) = event
                    .payload
                    .get("exercised_quantity_delta")
                    .and_then(Value::as_i64)
                {
                    if right.direction == crate::domain::Direction::Buy && !quantity_buy {
                        if value != 0 {
                            return Err(anyhow!("buy right cannot exercise sell quantity"));
                        }
                    } else if value <= 0 {
                        return Err(anyhow!("invalid sell-right exercise delta"));
                    } else {
                        let exercised = right
                            .exercised_quantity
                            .checked_add(value)
                            .context("NUMERIC_RANGE_VIOLATION: exercised quantity overflow")?;
                        if exercised > right.reserved_quantity {
                            return Err(anyhow!("invalid sell-right exercise delta"));
                        }
                        right.exercised_quantity = exercised;
                        has_exercise = true;
                    }
                }
                if next == GridRightStatus::Reserved && !has_reservation {
                    return Err(anyhow!("grid right reservation is missing capacity"));
                }
                if event.schema_version >= 3
                    && next == GridRightStatus::Reserved
                    && right.reserved_quantity != right.decision_intent_quantity
                {
                    return Err(anyhow!(
                        "current reservation does not equal the platform-approved intent"
                    ));
                }
                if matches!(
                    next,
                    GridRightStatus::PartiallyExercised | GridRightStatus::Exercised
                ) && !has_exercise
                {
                    return Err(anyhow!("grid right exercise is missing a positive delta"));
                }
                let fully_consumed = match right.direction {
                    crate::domain::Direction::Buy if quantity_buy => {
                        right.exercised_quantity == right.capacity.available_quantity
                    }
                    crate::domain::Direction::Buy => {
                        right.exercised_budget == right.capacity.available_budget
                    }
                    crate::domain::Direction::Sell => {
                        right.exercised_quantity == right.capacity.eligible_quantity
                            && right.capacity.t_plus_one_blocked_quantity == 0
                            && right.capacity.risk_blocked_quantity == 0
                            && right.capacity.no_profit_blocked_quantity == 0
                    }
                };
                if next == GridRightStatus::Exercised && !fully_consumed {
                    return Err(anyhow!(
                        "grid right cannot be fully exercised with remainder"
                    ));
                }
                let remaining_budget = right.remaining_budget();
                let remaining_quantity = right.remaining_quantity();
                if event.event_type == EventType::GridRightRevoked {
                    let revoked_budget = payload_decimal(&event.payload, "revoked_budget")?;
                    let revoked_quantity = event
                        .payload
                        .get("revoked_quantity")
                        .and_then(Value::as_i64)
                        .context("missing revoked_quantity")?;
                    if revoked_budget != remaining_budget
                        || revoked_quantity != remaining_quantity
                        || (right.direction == crate::domain::Direction::Buy
                            && !quantity_buy
                            && revoked_quantity != 0)
                        || (right.direction == crate::domain::Direction::Buy
                            && quantity_buy
                            && revoked_budget != rust_decimal::Decimal::ZERO)
                        || (right.direction == crate::domain::Direction::Sell
                            && revoked_budget != rust_decimal::Decimal::ZERO)
                    {
                        return Err(anyhow!(
                            "grid-right revocation violates capacity conservation"
                        ));
                    }
                }
                if event.event_type == EventType::GridRightExpired && event.schema_version >= 2 {
                    let reason = event
                        .payload
                        .get("reason")
                        .and_then(Value::as_str)
                        .context("missing grid-right expiry reason")?;
                    let reported_budget = payload_decimal(&event.payload, "remaining_budget")?;
                    let reported_quantity = event
                        .payload
                        .get("remaining_quantity")
                        .and_then(Value::as_i64)
                        .context("missing remaining_quantity")?;
                    match reason {
                        "CARRIED_TO_DEEPER_RIGHT" => {
                            let target = event
                                .payload
                                .get("transferred_to_right_id")
                                .and_then(Value::as_str)
                                .filter(|value| !value.trim().is_empty())
                                .context("carried right requires a transfer target")?;
                            if target == right.right_id {
                                return Err(anyhow!("grid right cannot transfer to itself"));
                            }
                            let transferred_budget =
                                payload_decimal(&event.payload, "transferred_budget")?;
                            let transferred_quantity = event
                                .payload
                                .get("transferred_quantity")
                                .and_then(Value::as_i64)
                                .context("missing transferred_quantity")?;
                            if reported_budget != rust_decimal::Decimal::ZERO
                                || reported_quantity != 0
                                || transferred_budget != remaining_budget
                                || transferred_quantity != remaining_quantity
                            {
                                return Err(anyhow!(
                                    "grid-right transfer violates capacity conservation"
                                ));
                            }
                        }
                        "GRID_CYCLE_COMPLETED" => {
                            let expected =
                                tranche_remaining.unwrap_or((remaining_budget, remaining_quantity));
                            if reported_budget != expected.0 || reported_quantity != expected.1 {
                                return Err(anyhow!(
                                    "grid-right expiry violates capacity conservation"
                                ));
                            }
                        }
                        _ => return Err(anyhow!("unknown grid-right expiry reason")),
                    }
                }
                right.status = next;
                if matches!(
                    event.event_type,
                    EventType::GridRightReleased
                        | EventType::GridRightTransferred
                        | EventType::GridRightExpired
                        | EventType::GridRightRevoked
                ) {
                    right.reserved_budget = right.exercised_budget;
                    right.reserved_quantity = right.exercised_quantity;
                }
            }
            EventType::InitialDeploymentEvaluated => {
                let evaluation: InitialDeploymentEvaluation =
                    serde_json::from_value(event.payload.clone())?;
                self.validate_initial_deployment_evaluation(event, &evaluation)?;
                self.initial_deployment_evaluation = Some(evaluation);
            }
            EventType::OngoingResourcePolicyActivated => {
                let activation: crate::domain::OngoingResourcePolicyActivation =
                    serde_json::from_value(event.payload.clone())?;
                if self.ongoing_resource_policy.is_some() {
                    return Err(anyhow!("ongoing resource policy is already activated"));
                }
                let config = self
                    .audited_config
                    .as_ref()
                    .context("ongoing resource policy lacks audited configuration")?;
                let opening = self
                    .initial_deployment_evaluation
                    .as_ref()
                    .context("ongoing resource policy requires an opening evaluation")?;
                if opening.outcome != InitialDeploymentOutcome::Authorized
                    || activation.policy_version != crate::domain::ONGOING_RESOURCE_POLICY_VERSION
                    || activation.minimum_free_cash != rust_decimal::Decimal::ZERO
                    || activation.position_limit_mode != "NUMERIC_SAFETY_ONLY"
                    || activation.effective_max_position != config.max_position
                    || activation.config_content_sha256 != config.content_sha256()?
                    || activation.initial_deployment_id != opening.deployment_id
                    || activation.initial_deployment_sha256
                        != hex::encode(Sha256::digest(serde_json::to_vec(opening)?))
                    || activation.reason_code.trim().is_empty()
                    || activation.operator.trim().is_empty()
                {
                    return Err(anyhow!(
                        "ongoing resource policy activation is not canonical"
                    ));
                }
                self.ongoing_resource_policy = Some(activation);
            }
            EventType::OrderIntentCreated => {
                let order: Order = serde_json::from_value(event.payload.clone())?;
                if event.schema_version >= 4 {
                    self.validate_current_order_intent(event, &order)?;
                }
                if event.schema_version >= 5
                    && (self.audited_standard_quantity <= 0
                        || order.intent.quantity % self.audited_standard_quantity != 0)
                {
                    return Err(anyhow!(
                        "current order intent quantity must use whole standard units"
                    ));
                }
                if event.schema_version >= 3 {
                    let policy = order
                        .intent
                        .profit_guard_policy
                        .as_ref()
                        .context("order intent lacks its audited fee policy")?;
                    if policy.price_scale > 8
                        || policy.commission_rate < rust_decimal::Decimal::ZERO
                        || policy.commission_rate > crate::config::Config::decimal("0.1")
                        || policy.minimum_commission < rust_decimal::Decimal::ZERO
                        || policy.sell_tax_rate < rust_decimal::Decimal::ZERO
                        || policy.sell_tax_rate > crate::config::Config::decimal("0.1")
                        || policy.slippage_rate < rust_decimal::Decimal::ZERO
                        || policy.slippage_rate >= rust_decimal::Decimal::ONE
                        || self.audited_profit_guard_policy.as_ref() != Some(policy)
                    {
                        return Err(anyhow!("order intent has an invalid fee policy"));
                    }
                }
                if event.schema_version >= 2
                    && order.intent.direction == crate::domain::Direction::Sell
                {
                    let policy = order.intent.profit_guard_policy.as_ref();
                    let allocation_quantity = order
                        .intent
                        .target_lot_allocations
                        .iter()
                        .try_fold(0_i64, |total, allocation| {
                            total.checked_add(allocation.quantity)
                        })
                        .context("NUMERIC_RANGE_VIOLATION: intent allocation overflow")?;
                    let unique_lots = order
                        .intent
                        .target_lot_allocations
                        .iter()
                        .map(|allocation| allocation.lot_id.as_str())
                        .collect::<std::collections::BTreeSet<_>>();
                    if order.intent.profit_guard_version != crate::profit::PROFIT_GUARD_VERSION
                        || allocation_quantity != order.intent.quantity
                        || order.intent.target_lot_allocations.is_empty()
                        || unique_lots.len() != order.intent.target_lot_allocations.len()
                        || order
                            .intent
                            .target_lot_allocations
                            .iter()
                            .any(|allocation| {
                                allocation.quantity <= 0
                                    || allocation.tranche_id.is_empty()
                                    || allocation.worst_case_profit < rust_decimal::Decimal::ZERO
                                    || !order.intent.target_lot_ids.contains(&allocation.lot_id)
                            })
                    {
                        return Err(anyhow!("sell intent lacks a valid no-loss allocation"));
                    }
                    for allocation in &order.intent.target_lot_allocations {
                        let lot = self
                            .lots
                            .get(&allocation.lot_id)
                            .context("sell allocation lot is missing")?;
                        let expected_cost =
                            crate::profit::allocated_cost_for_close(lot, allocation.quantity)
                                .context("sell allocation quantity exceeds its lot")?;
                        let tranche = self
                            .right_tranches
                            .get(&allocation.tranche_id)
                            .context("sell allocation tranche is missing")?;
                        let reported_profit = allocation.worst_fill_price
                            * rust_decimal::Decimal::from(allocation.quantity)
                            - allocation.maximum_sell_fees
                            - allocation.cost_basis;
                        if expected_cost != allocation.cost_basis
                            || reported_profit != allocation.worst_case_profit
                            || tranche.lot_id.as_deref() != Some(allocation.lot_id.as_str())
                            || tranche.reserved_quantity < allocation.quantity
                        {
                            return Err(anyhow!(
                                "sell allocation proof does not match lot and tranche state"
                            ));
                        }
                        if event.schema_version >= 3 {
                            let policy = policy
                                .context("sell intent lacks its audited profit-guard policy")?;
                            let expected_worst_price =
                                crate::profit::checked_worst_sell_fill_price_with_policy(
                                    policy,
                                    order.intent.limit_price,
                                )
                                .context("NUMERIC_RANGE_VIOLATION: adverse SELL price overflow")?;
                            let expected_maximum_fees =
                                crate::profit::checked_conservative_sell_fees_with_policy(
                                    policy,
                                    expected_worst_price,
                                    allocation.quantity,
                                )
                                .context("NUMERIC_RANGE_VIOLATION: SELL fee proof overflow")?;
                            if allocation.worst_fill_price != expected_worst_price
                                || allocation.maximum_sell_fees != expected_maximum_fees
                            {
                                return Err(anyhow!(
                                    "sell allocation proof does not match its audited policy"
                                ));
                            }
                        }
                    }
                }
                if event.schema_version >= 4 {
                    if self.orders.contains_key(&order.order_id) {
                        return Err(anyhow!("order intent cannot be created twice"));
                    }
                    self.orders.insert(order.order_id.clone(), order);
                } else {
                    self.orders.entry(order.order_id.clone()).or_insert(order);
                }
            }
            EventType::OrderSubmitted => {
                let order_id = event
                    .payload
                    .get("order_id")
                    .and_then(Value::as_str)
                    .context("missing order_id")?;
                let order = self
                    .orders
                    .get_mut(order_id)
                    .with_context(|| format!("order {order_id} missing"))?;
                if order.status != OrderStatus::Submitted {
                    let frozen_cash = self
                        .cash
                        .frozen
                        .checked_add(order.reserved_cash_remaining)
                        .context("NUMERIC_RANGE_VIOLATION: frozen cash overflow")?;
                    let frozen_sell = self
                        .position
                        .frozen_sell
                        .checked_add(order.reserved_quantity_remaining)
                        .context("NUMERIC_RANGE_VIOLATION: frozen sell overflow")?;
                    order.transition(OrderStatus::Submitted)?;
                    self.cash.frozen = frozen_cash;
                    self.position.frozen_sell = frozen_sell;
                }
            }
            EventType::OrderAccepted
            | EventType::OrderCancelRequested
            | EventType::OrderRejected
            | EventType::OrderCancelled => {
                let order_id = event
                    .payload
                    .get("order_id")
                    .and_then(Value::as_str)
                    .context("missing order_id")?;
                let next = match event.event_type {
                    EventType::OrderAccepted => OrderStatus::Accepted,
                    EventType::OrderCancelRequested => OrderStatus::CancelPending,
                    EventType::OrderRejected => OrderStatus::Rejected,
                    EventType::OrderCancelled => OrderStatus::Cancelled,
                    _ => unreachable!(),
                };
                let order = self
                    .orders
                    .get_mut(order_id)
                    .with_context(|| format!("order {order_id} missing"))?;
                if event.event_type == EventType::OrderCancelRequested {
                    let reason = event
                        .payload
                        .get("reason")
                        .and_then(Value::as_str)
                        .filter(|reason| !reason.trim().is_empty())
                        .context("cancel request requires a reason")?;
                    if let Some(existing) = order.cancel_requested_reason.as_deref() {
                        if existing != reason || order.cancel_requested_at != Some(event.event_time)
                        {
                            return Err(anyhow!("conflicting cancel request"));
                        }
                    } else {
                        order.cancel_requested_reason = Some(reason.to_owned());
                        order.cancel_requested_at = Some(event.event_time);
                    }
                }
                if order.status != next {
                    if matches!(next, OrderStatus::Rejected | OrderStatus::Cancelled) {
                        let frozen_cash = self
                            .cash
                            .frozen
                            .checked_sub(order.reserved_cash_remaining)
                            .context("NUMERIC_RANGE_VIOLATION: frozen cash underflow")?;
                        let frozen_sell = self
                            .position
                            .frozen_sell
                            .checked_sub(order.reserved_quantity_remaining)
                            .context("NUMERIC_RANGE_VIOLATION: frozen sell underflow")?;
                        order.transition(next)?;
                        self.cash.frozen = frozen_cash;
                        self.position.frozen_sell = frozen_sell;
                        order.reserved_cash_remaining = rust_decimal::Decimal::ZERO;
                        order.reserved_quantity_remaining = 0;
                    } else {
                        order.transition(next)?;
                    }
                }
            }
            EventType::OrderPartiallyFilled | EventType::OrderFilled => {
                let fill: Fill = serde_json::from_value(
                    event.payload.get("fill").cloned().context("missing fill")?,
                )?;
                let grid_index = payload_i32(&event.payload, "grid_index")?;
                if self.processed_fills.contains(&fill.fill_id) {
                    return Ok(());
                }
                let next = if matches!(event.event_type, EventType::OrderFilled) {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
                let current = self
                    .orders
                    .get(&fill.order_id)
                    .with_context(|| format!("order {} missing", fill.order_id))?
                    .status;
                let order = self
                    .orders
                    .get(&fill.order_id)
                    .with_context(|| format!("order {} missing", fill.order_id))?;
                if fill.direction != order.intent.direction {
                    return Err(anyhow!("fill direction does not match order intent"));
                }
                let resulting_quantity = order
                    .filled_quantity
                    .checked_add(fill.quantity)
                    .context("NUMERIC_RANGE_VIOLATION: cumulative fill overflow")?;
                if fill.quantity <= 0 || resulting_quantity > order.intent.quantity {
                    return Err(anyhow!("fill quantity exceeds order intent"));
                }
                if fill
                    .target_lot_ids
                    .iter()
                    .any(|id| !order.intent.target_lot_ids.contains(id))
                {
                    return Err(anyhow!("fill targets a lot outside the order intent"));
                }
                if event.schema_version >= 3 {
                    let policy = order
                        .intent
                        .profit_guard_policy
                        .as_ref()
                        .context("order lacks its audited fee policy")?;
                    let (expected_commission, expected_tax) =
                        crate::profit::checked_actual_order_fill_charges_with_policy(
                            policy,
                            fill.direction,
                            order.filled_notional,
                            order.commission_charged,
                            fill.price,
                            fill.quantity,
                        )
                        .context("NUMERIC_RANGE_VIOLATION: fill charge overflow")?;
                    if fill.commission != expected_commission || fill.tax != expected_tax {
                        return Err(anyhow!("fill charges do not match the audited fee policy"));
                    }
                }
                if event.schema_version >= 2 && fill.direction == crate::domain::Direction::Sell {
                    let expected_allocations = if event.schema_version >= 3 {
                        let policy = order
                            .intent
                            .profit_guard_policy
                            .as_ref()
                            .context("sell order lacks its audited profit-guard policy")?;
                        Some(
                            crate::profit::canonical_fill_allocations(
                                policy,
                                &order.intent.target_lot_allocations,
                                order.filled_quantity,
                                fill.quantity,
                                order.intent.limit_price,
                            )
                            .context("sell fill cannot be sliced from its order allocation")?,
                        )
                    } else {
                        None
                    };
                    let allocated = fill
                        .target_lot_allocations
                        .iter()
                        .try_fold(0_i64, |total, allocation| {
                            total.checked_add(allocation.quantity)
                        })
                        .context("NUMERIC_RANGE_VIOLATION: fill allocation overflow")?;
                    if allocated != fill.quantity
                        || fill.target_lot_allocations.is_empty()
                        || expected_allocations
                            .as_ref()
                            .is_some_and(|expected| fill.target_lot_allocations != *expected)
                        || fill.target_lot_allocations.iter().any(|allocation| {
                            allocation.quantity <= 0
                                || allocation.tranche_id.is_empty()
                                || allocation.worst_case_profit < rust_decimal::Decimal::ZERO
                                || !order.intent.target_lot_allocations.iter().any(|planned| {
                                    planned.lot_id == allocation.lot_id
                                        && planned.tranche_id == allocation.tranche_id
                                })
                        })
                    {
                        return Err(anyhow!("sell fill violates its explicit no-loss proof"));
                    }
                }
                if (next == OrderStatus::Filled && resulting_quantity != order.intent.quantity)
                    || (next == OrderStatus::PartiallyFilled
                        && resulting_quantity >= order.intent.quantity)
                {
                    return Err(anyhow!("fill finality is inconsistent with quantity"));
                }
                if current != next && !current.can_transition(next) {
                    return Err(anyhow!(
                        "invalid fill transition from {current:?} to {next:?}"
                    ));
                }
                let fill_notional = fill
                    .price
                    .checked_mul(rust_decimal::Decimal::from(fill.quantity))
                    .context("NUMERIC_RANGE_VIOLATION: fill notional overflow")?;
                let fill_cost = fill_notional
                    .checked_add(fill.commission)
                    .and_then(|value| value.checked_add(fill.tax))
                    .context("NUMERIC_RANGE_VIOLATION: fill cost overflow")?;
                if fill.direction == crate::domain::Direction::Buy
                    && order.reserved_cash_remaining < fill_cost
                {
                    return Err(anyhow!("fill exceeds reserved cash"));
                }
                if fill.direction == crate::domain::Direction::Sell
                    && order.reserved_quantity_remaining < fill.quantity
                {
                    return Err(anyhow!("fill exceeds reserved sell quantity"));
                }
                let resulting_notional = order
                    .filled_notional
                    .checked_add(fill_notional)
                    .context("NUMERIC_RANGE_VIOLATION: order filled notional overflow")?;
                let resulting_commission = order
                    .commission_charged
                    .checked_add(fill.commission)
                    .context("NUMERIC_RANGE_VIOLATION: order commission overflow")?;
                let resulting_cash_reservation = if fill.direction == crate::domain::Direction::Buy
                {
                    order
                        .reserved_cash_remaining
                        .checked_sub(fill_cost)
                        .context("NUMERIC_RANGE_VIOLATION: order cash reservation underflow")?
                } else {
                    order.reserved_cash_remaining
                };
                let resulting_quantity_reservation = if fill.direction
                    == crate::domain::Direction::Sell
                {
                    order
                        .reserved_quantity_remaining
                        .checked_sub(fill.quantity)
                        .context("NUMERIC_RANGE_VIOLATION: order quantity reservation underflow")?
                } else {
                    order.reserved_quantity_remaining
                };
                let resulting_frozen_cash = if fill.direction == crate::domain::Direction::Buy {
                    Some(
                        self.cash
                            .frozen
                            .checked_sub(fill_cost)
                            .context("NUMERIC_RANGE_VIOLATION: frozen cash underflow")?,
                    )
                } else {
                    None
                };
                let resulting_frozen_sell = if fill.direction == crate::domain::Direction::Sell {
                    Some(
                        self.position
                            .frozen_sell
                            .checked_sub(fill.quantity)
                            .context("NUMERIC_RANGE_VIOLATION: frozen sell underflow")?,
                    )
                } else {
                    None
                };
                let terminal_frozen_cash = if next == OrderStatus::Filled
                    && fill.direction == crate::domain::Direction::Buy
                {
                    Some(
                        resulting_frozen_cash
                            .context("BUY fill lacks frozen cash projection")?
                            .checked_sub(resulting_cash_reservation)
                            .context("NUMERIC_RANGE_VIOLATION: final frozen cash underflow")?,
                    )
                } else {
                    None
                };
                let terminal_frozen_sell = if next == OrderStatus::Filled
                    && fill.direction == crate::domain::Direction::Sell
                {
                    Some(
                        resulting_frozen_sell
                            .context("SELL fill lacks frozen quantity projection")?
                            .checked_sub(resulting_quantity_reservation)
                            .context("NUMERIC_RANGE_VIOLATION: final frozen sell underflow")?,
                    )
                } else {
                    None
                };
                let opening_allocation = order.intent.origin.is_initial_deployment();
                if event.schema_version == 1 {
                    self.apply_legacy_fill(&fill, grid_index)?;
                } else if opening_allocation {
                    self.apply_initial_deployment_fill(&fill)?;
                } else {
                    self.apply_fill(&fill, grid_index)?;
                }
                let order = self
                    .orders
                    .get_mut(&fill.order_id)
                    .with_context(|| format!("order {} missing", fill.order_id))?;
                order.filled_quantity = resulting_quantity;
                order.filled_notional = resulting_notional;
                order.commission_charged = resulting_commission;
                if fill.direction == crate::domain::Direction::Buy {
                    order.reserved_cash_remaining = resulting_cash_reservation;
                    self.cash.frozen =
                        resulting_frozen_cash.context("BUY fill lacks frozen cash projection")?;
                } else {
                    order.reserved_quantity_remaining = resulting_quantity_reservation;
                    self.position.frozen_sell = resulting_frozen_sell
                        .context("SELL fill lacks frozen quantity projection")?;
                }
                if order.status != next {
                    order.transition(next)?;
                }
                if next == OrderStatus::Filled {
                    if let Some(frozen_cash) = terminal_frozen_cash {
                        self.cash.frozen = frozen_cash;
                    }
                    if let Some(frozen_sell) = terminal_frozen_sell {
                        self.position.frozen_sell = frozen_sell;
                    }
                    order.reserved_cash_remaining = rust_decimal::Decimal::ZERO;
                    order.reserved_quantity_remaining = 0;
                }
                if !opening_allocation {
                    self.historically_executed_levels.insert(grid_index);
                    if let Some(level) = self.levels.get_mut(&grid_index) {
                        level.status = GridLevelStatus::Executed;
                    }
                }
            }
            EventType::AmbiguousBarDetected => {
                self.ambiguous_bars = self
                    .ambiguous_bars
                    .checked_add(1)
                    .context("NUMERIC_RANGE_VIOLATION: ambiguous bar counter overflow")?;
            }
            EventType::TradingDayRolled => {
                let date = event
                    .payload
                    .get("date")
                    .and_then(Value::as_str)
                    .context("missing date")?;
                self.roll_trading_day(chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?)?;
            }
            EventType::RecoveryCompleted => {
                self.last_recovery = Some(event.payload.to_string());
            }
            EventType::ReconciliationCompleted => {
                let result: ReconciliationResult = serde_json::from_value(event.payload.clone())?;
                if !result.matched {
                    self.mode = ServiceMode::Safe;
                }
                self.last_reconciliation = Some(result);
                self.last_reconciliation_event_sequence = Some(event.sequence_number);
            }
            EventType::ServiceStopped => {
                if self.mode == ServiceMode::Running {
                    self.mode = ServiceMode::Stopped;
                }
            }
            EventType::ServiceModeChanged => {
                self.mode = serde_json::from_value(
                    event.payload.get("mode").cloned().context("missing mode")?,
                )?;
            }
            EventType::GridCycleReset => {
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current cycle reset lacks its audited configuration")?;
                    let previous_cycle = event
                        .payload
                        .get("previous_cycle_id")
                        .and_then(Value::as_str)
                        .context("current cycle reset lacks its previous cycle")?;
                    let new_cycle = event
                        .payload
                        .get("cycle_id")
                        .and_then(Value::as_str)
                        .context("current cycle reset lacks its new cycle")?;
                    let expected_cycle = Uuid::new_v5(
                        &Uuid::NAMESPACE_OID,
                        format!("{}:cycle-reset:{}", self.run_id, event.event_time).as_bytes(),
                    )
                    .to_string();
                    let active_right = self.grid_rights.values().any(|right| {
                        right.cycle_id == self.cycle_id
                            && !matches!(
                                right.status,
                                GridRightStatus::Exercised
                                    | GridRightStatus::Transferred
                                    | GridRightStatus::Expired
                                    | GridRightStatus::Revoked
                            )
                    });
                    let active_tranche = self.right_tranches.values().any(|tranche| {
                        tranche.cycle_id == self.cycle_id
                            && (tranche.available_budget > rust_decimal::Decimal::ZERO
                                || tranche.reserved_budget > rust_decimal::Decimal::ZERO
                                || tranche.available_quantity > 0
                                || tranche.reserved_quantity > 0)
                    });
                    let open_order = self.orders.values().any(|order| {
                        !matches!(
                            order.status,
                            OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Cancelled
                        )
                    });
                    let completed_sell = self.orders.values().any(|order| {
                        order.status == OrderStatus::Filled
                            && order.intent.direction == crate::domain::Direction::Sell
                            && self
                                .grid_rights
                                .get(&order.intent.right_id)
                                .is_some_and(|right| right.cycle_id == self.cycle_id)
                    });
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || previous_cycle != self.cycle_id
                        || new_cycle != expected_cycle
                        || self.lots.values().any(|lot| lot.remaining_quantity > 0)
                        || active_right
                        || active_tranche
                        || open_order
                        || !completed_sell
                    {
                        return Err(anyhow!(
                            "current cycle reset is not justified by a completed cycle"
                        ));
                    }
                }
                if let Some(cycle_id) = event.payload.get("cycle_id").and_then(Value::as_str) {
                    self.cycle_id = cycle_id.to_owned();
                    self.grid_cycle_active = true;
                    self.deployed_grid_budget = rust_decimal::Decimal::ZERO;
                    self.levels.clear();
                }
            }
            EventType::GridCycleStarted => {
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current cycle start lacks its audited configuration")?;
                    if (!self.cycle_id.is_empty() && event.cycle_id != self.cycle_id)
                        || event.config_version != config.config_version
                        || payload_decimal(&event.payload, "anchor")? != config.anchor_price
                        || !self.levels.is_empty()
                        || !self.grid_rights.is_empty()
                        || !self.right_tranches.is_empty()
                    {
                        return Err(anyhow!(
                            "current cycle start does not initialize an empty audited grid"
                        ));
                    }
                }
                self.cycle_id = event.cycle_id.clone();
                self.grid_cycle_active = true;
            }
            EventType::ConfigSnapshotted => {
                if event.schema_version >= 2
                    && event
                        .payload
                        .get("_content_sha256")
                        .and_then(Value::as_str)
                        .is_none()
                {
                    return Err(anyhow!(
                        "current configuration snapshot requires its content hash"
                    ));
                }
                let config = crate::config::Config::from_snapshot_payload(&event.payload)?;
                if event.schema_version >= 2
                    && (config.symbol != self.symbol
                        || event.symbol != config.symbol
                        || config.anchor_price != self.anchor_price
                        || event.config_version != config.config_version)
                {
                    return Err(anyhow!(
                        "current configuration snapshot does not match its run envelope"
                    ));
                }
                if event.schema_version >= 2 && !config.uses_fixed_quantity() {
                    return Err(anyhow!(
                        "current configuration snapshots require fixed standard_quantity"
                    ));
                }
                let policy = crate::domain::ProfitGuardPolicy::from(&config);
                if self
                    .audited_config
                    .as_ref()
                    .is_some_and(|existing| existing != &config)
                {
                    return Err(anyhow!(
                        "configuration snapshot cannot change the audited run configuration"
                    ));
                }
                self.audited_config = Some(config.clone());
                if self
                    .audited_profit_guard_policy
                    .as_ref()
                    .is_some_and(|existing| existing != &policy)
                {
                    return Err(anyhow!(
                        "configuration snapshot cannot change run fee policy"
                    ));
                }
                self.audited_profit_guard_policy = Some(policy);
                if config.uses_fixed_quantity() {
                    if (self.audited_standard_quantity != 0
                        && self.audited_standard_quantity != config.standard_quantity)
                        || (self.audited_lot_size != 0 && self.audited_lot_size != config.lot_size)
                    {
                        return Err(anyhow!(
                            "configuration snapshot cannot change the mechanical unit"
                        ));
                    }
                    self.audited_standard_quantity = config.standard_quantity;
                    self.audited_lot_size = config.lot_size;
                }
            }
            EventType::GateDecisionMade if event.schema_version >= 2 => {
                let request: crate::decision::DecisionRequest = serde_json::from_value(
                    event
                        .payload
                        .get("request")
                        .cloned()
                        .context("gate decision is missing its request")?,
                )?;
                let response: crate::decision::DecisionResponse = serde_json::from_value(
                    event
                        .payload
                        .get("response")
                        .cloned()
                        .context("gate decision is missing its response")?,
                )?;
                let algorithm_succeeded =
                    if request.contract_version >= crate::decision::DECISION_CONTRACT_VERSION {
                        event
                            .payload
                            .get("algorithm_succeeded")
                            .and_then(Value::as_bool)
                            .context("v4 gate decision lacks exact algorithm success status")?
                    } else {
                        event
                            .payload
                            .get("algorithm_succeeded")
                            .and_then(Value::as_bool)
                            .unwrap_or(true)
                    };
                response.validate_for_algorithm_status(&request, algorithm_succeeded)?;
                if request.context.standard_quantity != self.audited_standard_quantity
                    || request.context.lot_size != self.audited_lot_size
                {
                    return Err(anyhow!("decision request uses the wrong run quantity unit"));
                }
                let right = self
                    .grid_rights
                    .get_mut(&response.right_id)
                    .context("decision grid right is missing")?;
                if right.decision_recorded {
                    return Err(anyhow!("grid right cannot be decided twice"));
                }
                let (exercise_quantity, defer_quantity) = response.outcome.quantities();
                right.decision_contract_version = response.contract_version;
                right.decision_gross_available_quantity = if response.contract_version
                    >= crate::decision::WHOLE_UNIT_DECISION_CONTRACT_VERSION
                {
                    request.context.gross_available_quantity
                } else {
                    request.context.available_quantity
                };
                right.decision_platform_residual_quantity = if response.contract_version
                    >= crate::decision::WHOLE_UNIT_DECISION_CONTRACT_VERSION
                {
                    request.context.platform_residual_quantity
                } else {
                    0
                };
                right.decision_algorithm_authorized_quantity =
                    if response.contract_version >= crate::decision::DECISION_CONTRACT_VERSION {
                        request.context.algorithm_authorized_quantity
                    } else {
                        request.context.available_quantity
                    };
                right.decision_algorithm_offered_quantity = request.context.available_quantity;
                right.decision_pre_blocked_quantity = right
                    .decision_algorithm_authorized_quantity
                    .checked_sub(right.decision_algorithm_offered_quantity)
                    .context("v4 pre-decision resource partition underflow")?;
                right.decided_exercise_quantity = exercise_quantity;
                right.decided_defer_quantity = defer_quantity;
                right.decision_is_algorithm = algorithm_succeeded;
                right.decision_recorded = true;
            }
            EventType::DeferredBudgetAccrued if self.audited_standard_quantity > 0 => {
                return Err(anyhow!(
                    "fixed-quantity runs cannot append legacy budget-capacity facts"
                ));
            }
            EventType::RunStarted
            | EventType::AlgorithmRegistered
            | EventType::PlatformUpgradeAuthorized
            | EventType::PlatformUpgradeActivated
            | EventType::ReplayInitialized
            | EventType::DeferredBudgetAccrued
            | EventType::DeferredQuantityAccrued
            | EventType::LotOpened
            | EventType::LotPartiallyClosed
            | EventType::LotClosed
            | EventType::CashChanged
            | EventType::PositionChanged => {}
            EventType::GateDecisionMade => {}
            EventType::ErrorRecorded => {
                if event
                    .payload
                    .get("entered_safe")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    self.mode = ServiceMode::Safe;
                }
            }
            EventType::MarketBarProcessed => {
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current processed bar lacks its audited configuration")?;
                    let payload_symbol = event
                        .payload
                        .get("symbol")
                        .and_then(Value::as_str)
                        .context("current processed bar lacks its symbol")?;
                    let payload_time = event
                        .payload
                        .get("timestamp")
                        .cloned()
                        .context("current processed bar lacks its timestamp")?;
                    let payload_time: NaiveDateTime = serde_json::from_value(payload_time)?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || event.symbol != self.symbol
                        || payload_symbol != event.symbol
                        || payload_time != event.event_time
                    {
                        return Err(anyhow!(
                            "current processed bar does not match its event envelope"
                        ));
                    }
                }
                if let Some(close) = event.payload.get("close").and_then(Value::as_str) {
                    self.last_price = Some(close.parse()?);
                }
                if self
                    .pending_market_evidence
                    .as_ref()
                    .is_some_and(|pending| {
                        pending.bar.symbol == event.symbol
                            && pending.bar.timestamp == event.event_time
                    })
                {
                    self.pending_market_evidence = None;
                }
            }
            EventType::MarketDataReceived => {
                if event.schema_version == 1 {
                    if let Some(close) = event.payload.get("close").and_then(Value::as_str) {
                        self.last_price = Some(close.parse()?);
                    }
                } else {
                    let bar: MarketBar = serde_json::from_value(event.payload.clone())
                        .context("invalid current market-data payload")?;
                    validate_bar(&bar, &self.symbol)?;
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current market data lacks its audited configuration")?;
                    if event.run_id != self.run_id
                        || event.symbol != self.symbol
                        || event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || bar.symbol != event.symbol
                        || bar.timestamp != event.event_time
                    {
                        return Err(anyhow!(
                            "current market-data payload does not match its event envelope"
                        ));
                    }
                    self.pending_market_evidence = Some(PendingMarketEvidence {
                        event_id: event.event_id.clone(),
                        evidence_sha256: market_evidence_sha256(&bar)?,
                        bar,
                    });
                }
            }
            EventType::MarketBarDecisionsCommitted => {
                if event.schema_version >= 2 {
                    let config = self
                        .audited_config
                        .as_ref()
                        .context("current decision stage lacks its audited configuration")?;
                    let payload_symbol = event
                        .payload
                        .get("symbol")
                        .and_then(Value::as_str)
                        .context("current decision stage lacks its symbol")?;
                    let payload_time = event
                        .payload
                        .get("timestamp")
                        .cloned()
                        .context("current decision stage lacks its timestamp")?;
                    let payload_time: NaiveDateTime = serde_json::from_value(payload_time)?;
                    if event.cycle_id != self.cycle_id
                        || event.config_version != config.config_version
                        || event.symbol != self.symbol
                        || payload_symbol != event.symbol
                        || payload_time != event.event_time
                    {
                        return Err(anyhow!(
                            "current decision stage does not match its event envelope"
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn payload_i32(payload: &Value, field: &str) -> Result<i32> {
    let value = payload
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing integer {field}"))?;
    i32::try_from(value).context("integer out of range")
}

fn payload_decimal(payload: &Value, field: &str) -> Result<rust_decimal::Decimal> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing decimal {field}"))?
        .parse()
        .with_context(|| format!("invalid decimal {field}"))
}
