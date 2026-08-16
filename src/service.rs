use crate::{
    config::Config,
    data::{MarketBar, MarketDataFeed},
    decision::{
        whole_unit_partition, DecisionRequest, GateAlgorithmAdapter, GridRight, GridRightStatus,
        QuantDecisionAlgorithm, RightTranche,
    },
    domain::{
        Direction, Fill, LotQuantityAllocation, Order, OrderIntent, OrderStatus, ProfitGuardPolicy,
        ReconciliationResult, ServiceMode, StrategyState,
    },
    event::{EventEnvelope, EventType},
    execution::{ExecutionGateway, ExecutionReport, PaperExecutionGateway},
    gate::{AlwaysExecuteGate, FixedGate, GateContext, GatePolicy},
    grid::{crossed_levels, maybe_rearm, GridSpec, TouchClassification},
    journal::{AppendOutcome, EventReader, SqliteStore, StateReader},
    ledger::LedgerWriter,
    profit::{
        canonical_fill_allocations, checked_actual_order_fill_charges_with_policy,
        PROFIT_GUARD_VERSION,
    },
    rights::{sell_tranches_to_mint, RightsGateCoordinator},
    risk::{
        canonical_approval, canonical_approved_quantity, estimated_buy_reservation,
        DefaultRiskChecker, RiskChecker,
    },
};
use anyhow::{anyhow, Context, Result};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

type TrancheAllocationSource = (String, i32, Option<String>, Decimal, Decimal, i64, i64);

pub struct GridAutomationService {
    pub config: Config,
    pub state: StrategyState,
    pub store: SqliteStore,
    algorithm: Arc<dyn QuantDecisionAlgorithm>,
    gateway: PaperExecutionGateway,
    last_sequence: i64,
    recent_closes: Vec<Decimal>,
    injected_fault: Option<WorkflowFaultPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowFaultPoint {
    MarketReceived,
    IntentCommitted,
    OrderSubmitted,
    BrokerSideEffect,
    OrderAccepted,
    FillApplied,
    CancelRequested,
    DecisionsCommitted,
    BarProcessed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayDescriptor {
    pub dataset_id: String,
    pub data_sha256: String,
    #[serde(default)]
    pub symbol: String,
    pub total_bars: usize,
    pub first_timestamp: NaiveDateTime,
    pub last_timestamp: NaiveDateTime,
}

impl GridAutomationService {
    pub fn start_new(
        config: Config,
        store: SqliteStore,
        gate: Box<dyn GatePolicy>,
        run_id: Option<String>,
    ) -> Result<Self> {
        Self::start_new_with_algorithm_and_replay(
            config,
            store,
            Box::new(GateAlgorithmAdapter::new(gate)),
            run_id,
            None,
        )
    }

    pub fn start_new_replay(
        config: Config,
        store: SqliteStore,
        gate: Box<dyn GatePolicy>,
        run_id: Option<String>,
        descriptor: &ReplayDescriptor,
    ) -> Result<Self> {
        Self::start_new_with_algorithm_and_replay(
            config,
            store,
            Box::new(GateAlgorithmAdapter::new(gate)),
            run_id,
            Some(descriptor),
        )
    }

    pub fn start_new_with_algorithm(
        config: Config,
        store: SqliteStore,
        algorithm: Box<dyn QuantDecisionAlgorithm>,
        run_id: Option<String>,
    ) -> Result<Self> {
        Self::start_new_with_algorithm_and_replay(config, store, algorithm, run_id, None)
    }

    fn start_new_with_algorithm_and_replay(
        config: Config,
        mut store: SqliteStore,
        algorithm: Box<dyn QuantDecisionAlgorithm>,
        run_id: Option<String>,
        replay: Option<&ReplayDescriptor>,
    ) -> Result<Self> {
        config.validate()?;
        if !config.uses_fixed_quantity() || config.standard_budget != Decimal::ZERO {
            anyhow::bail!(
                "new runs require fixed standard_quantity; legacy standard_budget is replay-only"
            )
        }
        store.migrate()?;
        let manifest = validate_algorithm_manifest(algorithm.as_ref())?;
        let run_id = run_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let cycle_id =
            Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{run_id}:cycle:1").as_bytes()).to_string();
        let mut state = StrategyState::new(
            run_id.clone(),
            cycle_id.clone(),
            config.symbol.clone(),
            config.anchor_price,
            config.initial_cash,
            config.initial_position,
            config.initial_sellable,
        );
        let now = chrono::Utc::now().naive_utc();
        let correlation = format!("run:{run_id}");
        let mut events = vec![
            make_event(
                &config,
                &state,
                EventType::RunStarted,
                now,
                &correlation,
                "run-started".to_owned(),
                json!({
                    "initial_cash": config.initial_cash.to_string(),
                    "initial_position": config.initial_position,
                    "initial_sellable": config.initial_sellable
                }),
            ),
            make_event(
                &config,
                &state,
                EventType::ConfigSnapshotted,
                now,
                &correlation,
                "config-snapshot".to_owned(),
                config_snapshot_payload(&config)?,
            ),
            make_event(
                &config,
                &state,
                EventType::AlgorithmRegistered,
                now,
                &correlation,
                "algorithm-registered".to_owned(),
                serde_json::to_value(&manifest)?,
            ),
        ];
        if let Some(descriptor) = replay {
            events.push(make_event(
                &config,
                &state,
                EventType::ReplayInitialized,
                now,
                &correlation,
                "replay-initialized".to_owned(),
                serde_json::to_value(descriptor)?,
            ));
        }
        events.push(make_event(
            &config,
            &state,
            EventType::GridCycleStarted,
            now,
            &correlation,
            format!("cycle-start:{cycle_id}"),
            json!({"anchor": config.anchor_price.to_string()}),
        ));
        for level in GridSpec::from(&config).levels()?.into_values() {
            events.push(make_event(
                &config,
                &state,
                EventType::GridLevelArmed,
                now,
                &correlation,
                format!("level-armed:{cycle_id}:{}", level.index),
                serde_json::to_value(level)?,
            ));
        }
        let mut last_sequence = 0;
        LedgerWriter::new(&mut store, &mut state, &mut last_sequence).append_batch(&mut events)?;
        let snapshot = state.snapshot()?;
        let gateway = PaperExecutionGateway::new(&config, &run_id, snapshot)?;
        Ok(Self {
            config,
            state,
            store,
            algorithm: Arc::from(algorithm),
            gateway,
            last_sequence,
            recent_closes: Vec::new(),
            injected_fault: None,
        })
    }

    pub fn recover(
        config: Config,
        store: SqliteStore,
        gate: Box<dyn GatePolicy>,
        run_id: String,
    ) -> Result<Self> {
        Self::recover_with_algorithm(
            config,
            store,
            Box::new(GateAlgorithmAdapter::new(gate)),
            run_id,
        )
    }

    pub fn recover_with_algorithm(
        config: Config,
        mut store: SqliteStore,
        algorithm: Box<dyn QuantDecisionAlgorithm>,
        run_id: String,
    ) -> Result<Self> {
        config.validate()?;
        store.migrate()?;
        let manifest = validate_algorithm_manifest(algorithm.as_ref())?;
        let stored_config_payload = store
            .first_payload_by_type(&run_id, EventType::ConfigSnapshotted)?
            .context("run has no configuration snapshot")?;
        let stored_config = Config::from_snapshot_payload(&stored_config_payload)?;
        let mut expected_config = config.clone();
        expected_config.database = stored_config.database.clone();
        if expected_config != stored_config {
            return Err(anyhow!(
                "runtime configuration differs from the run's audited configuration snapshot"
            ));
        }
        let stored_manifest = store
            .first_payload_by_type(&run_id, EventType::AlgorithmRegistered)?
            .context(
                "run lacks an audited algorithm manifest; legacy history is replay-compatible but read-only",
            )?;
        {
            let stored_manifest: crate::decision::AlgorithmManifest =
                serde_json::from_value(stored_manifest)
                    .context("invalid algorithm manifest snapshot")?;
            let legacy_identity = stored_manifest.artifact_sha256.is_empty()
                && stored_manifest.environment_sha256.is_empty();
            let identity_matches = if legacy_identity {
                stored_manifest.algorithm_name == manifest.algorithm_name
                    && stored_manifest.algorithm_version == manifest.algorithm_version
                    && stored_manifest.supported_contract_versions
                        == manifest.supported_contract_versions
            } else if stored_manifest.platform_sha256.is_empty() {
                stored_manifest.algorithm_name == manifest.algorithm_name
                    && stored_manifest.algorithm_version == manifest.algorithm_version
                    && stored_manifest.supported_contract_versions
                        == manifest.supported_contract_versions
                    && stored_manifest.deterministic == manifest.deterministic
                    && stored_manifest.supports_checkpoint == manifest.supports_checkpoint
                    && stored_manifest.artifact_sha256 == manifest.artifact_sha256
                    && stored_manifest.canonical_arguments == manifest.canonical_arguments
                    && stored_manifest.environment_sha256 == manifest.environment_sha256
            } else {
                stored_manifest == manifest
            };
            if !identity_matches {
                return Err(anyhow!(
                    "runtime algorithm differs from the run's audited manifest"
                ));
            }
        }
        let initial = StrategyState::new(
            run_id.clone(),
            String::new(),
            config.symbol.clone(),
            config.anchor_price,
            config.initial_cash,
            config.initial_position,
            config.initial_sellable,
        );
        let (mut state, mut last_sequence) = match store.rebuild(initial.clone()) {
            Ok(result) => result,
            Err(error) => {
                let mut safe = StrategyState::new(
                    run_id,
                    String::new(),
                    config.symbol.clone(),
                    config.anchor_price,
                    config.initial_cash,
                    config.initial_position,
                    config.initial_sellable,
                );
                safe.mode = ServiceMode::Safe;
                return Err(error.context("recovery failed; SAFE mode required"));
            }
        };
        let checksum_fallback_used = store.snapshot_fallback_used();
        let (full_state, full_sequence) = store
            .rebuild_full(initial)
            .context("full-log verification of recovered state failed")?;
        let semantic_snapshot_mismatch = last_sequence != full_sequence
            || serde_json::to_value(&state)? != serde_json::to_value(&full_state)?;
        if semantic_snapshot_mismatch {
            state = full_state;
            last_sequence = full_sequence;
        }
        let snapshot_fallback_used = checksum_fallback_used || semantic_snapshot_mismatch;
        let snapshot = state.snapshot()?;
        let mut gateway = PaperExecutionGateway::new(&config, &run_id, snapshot.clone())?;
        gateway.synchronize_snapshot(snapshot);
        let recent_closes = store
            .recent_payloads_by_type(&run_id, EventType::MarketBarProcessed, 5)?
            .into_iter()
            .filter_map(|payload| {
                payload
                    .get("close")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok())
            })
            .collect();
        let mut service = Self {
            config,
            state,
            store,
            algorithm: Arc::from(algorithm),
            gateway,
            last_sequence,
            recent_closes,
            injected_fault: None,
        };
        if snapshot_fallback_used && service.state.mode == ServiceMode::Running {
            let event = service.event(
                EventType::ServiceModeChanged,
                chrono::Utc::now().naive_utc(),
                "recovery",
                format!("snapshot-fallback-read-only:{}", service.last_sequence),
                json!({
                    "mode": ServiceMode::ReadOnly,
                    "reason": if semantic_snapshot_mismatch {
                        "SNAPSHOT_PREFIX_SEMANTIC_MISMATCH"
                    } else {
                        "INVALID_NEWER_SNAPSHOT_FALLBACK"
                    }
                }),
            );
            service.record(event)?;
        }
        let created_intents: Vec<_> = if service.state.mode == ServiceMode::Running {
            service
                .state
                .orders
                .values()
                .filter(|order| order.status == OrderStatus::Created)
                .map(|order| order.intent.clone())
                .collect()
        } else {
            Vec::new()
        };
        for intent in created_intents {
            let created_at = intent.created_at;
            let correlation = service
                .store
                .correlation_by_idempotency_key(
                    &service.state.run_id,
                    &format!("intent:{}", intent.intent_id),
                )?
                .context("created order lacks its durable intent correlation")?;
            service.dispatch_intent(intent, created_at, &correlation)?;
        }
        let broker_resumable: Vec<_> = if service.state.mode == ServiceMode::Running {
            service
                .state
                .orders
                .values()
                .filter(|order| {
                    matches!(
                        order.status,
                        OrderStatus::Submitted
                            | OrderStatus::Accepted
                            | OrderStatus::PartiallyFilled
                            | OrderStatus::CancelPending
                    )
                })
                .map(|order| {
                    (
                        order.status,
                        order.order_id.clone(),
                        order.intent.clone(),
                        order.cancel_requested_reason.clone(),
                        order.cancel_requested_at,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        for (status, order_id, intent, cancel_reason, cancel_requested_at) in broker_resumable {
            let report = if status == OrderStatus::CancelPending {
                Some(
                    service.gateway.cancel(
                        &intent,
                        cancel_reason
                            .as_deref()
                            .context("cancel-pending order has no durable reason")?,
                    )?,
                )
            } else if let Some(report) = service.gateway.report_for_intent(&intent.intent_id)? {
                Some(report)
            } else if status == OrderStatus::Submitted {
                Some(service.gateway.submit(&intent)?)
            } else {
                None
            };
            if let Some(report) = report {
                let report_correlation = if status == OrderStatus::CancelPending {
                    format!("cancel:{order_id}")
                } else {
                    service
                        .store
                        .correlation_by_idempotency_key(
                            &service.state.run_id,
                            &format!("submit:{order_id}"),
                        )?
                        .context("resumable order lacks its durable submit correlation")?
                };
                service.apply_execution_report(
                    &intent,
                    cancel_requested_at.unwrap_or(intent.created_at),
                    &report_correlation,
                    report,
                )?;
            }
        }
        service.recover_missing_cycle_reset()?;
        let ambiguous_orders: Vec<_> = service
            .state
            .orders
            .values()
            .filter(|order| {
                matches!(
                    order.status,
                    OrderStatus::Submitted
                        | OrderStatus::Accepted
                        | OrderStatus::PartiallyFilled
                        | OrderStatus::CancelPending
                )
            })
            .map(|order| order.order_id.clone())
            .collect();
        if !ambiguous_orders.is_empty() && service.state.mode == ServiceMode::Running {
            let event = service.event(
                EventType::ServiceModeChanged,
                chrono::Utc::now().naive_utc(),
                "recovery",
                format!("recovery-read-only:{}", service.last_sequence),
                json!({
                    "mode": ServiceMode::ReadOnly,
                    "reason": "AMBIGUOUS_EXTERNAL_ORDER_STATE",
                    "order_ids": ambiguous_orders
                }),
            );
            service.record(event)?;
        }
        let pending = service
            .state
            .orders
            .values()
            .filter(|o| {
                !matches!(
                    o.status,
                    OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Cancelled
                )
            })
            .count();
        let event = service.event(
            EventType::RecoveryCompleted,
            chrono::Utc::now().naive_utc(),
            "recovery",
            format!(
                "recovery:{}:{}",
                service.state.run_id, service.last_sequence
            ),
            json!({"replayed_through": service.last_sequence, "unfinished_orders": pending}),
        );
        service.record(event)?;
        Ok(service)
    }

    pub fn run_feed(&mut self, feed: &mut dyn MarketDataFeed) -> Result<()> {
        while let Some(bar) = feed.next_bar() {
            self.on_bar(&bar)?;
        }
        self.save_snapshot()?;
        Ok(())
    }

    pub fn initialize_replay(&mut self, descriptor: &ReplayDescriptor) -> Result<()> {
        let event = self.event(
            EventType::ReplayInitialized,
            chrono::Utc::now().naive_utc(),
            "replay-initialization",
            format!("replay-initialized:{}", self.state.run_id),
            serde_json::to_value(descriptor)?,
        );
        self.record(event)?;
        self.save_snapshot()
    }

    pub fn on_bar(&mut self, bar: &MarketBar) -> Result<()> {
        let processed_key = format!("bar-processed:{}:{}", bar.symbol, bar.timestamp);
        let market_key = format!("market:{}:{}", bar.symbol, bar.timestamp);
        if self
            .store
            .has_idempotency_key(&self.state.run_id, &processed_key)?
        {
            let recorded = self
                .store
                .payload_by_idempotency_key(&self.state.run_id, &market_key)?
                .context("processed bar is missing its market-data fact")?;
            if recorded != serde_json::to_value(bar)? {
                return Err(anyhow!(
                    "market data conflicts with the already processed bar {}",
                    bar.timestamp
                ));
            }
            return Ok(());
        }
        if self.state.mode == ServiceMode::Stopped {
            return Err(anyhow!("stopped service cannot accept new market data"));
        }
        let previous_close = self.state.last_price;
        let market_event = self.event(
            EventType::MarketDataReceived,
            bar.timestamp,
            &market_key,
            market_key.clone(),
            serde_json::to_value(bar)?,
        );
        let market_duplicate = self.record(market_event)? == AppendOutcome::Duplicate;
        self.maybe_inject_fault(WorkflowFaultPoint::MarketReceived)?;
        if market_duplicate
            && self
                .store
                .has_idempotency_key(&self.state.run_id, &processed_key)?
        {
            return Ok(());
        }
        let decisions_committed = self.store.has_idempotency_key(
            &self.state.run_id,
            &format!("bar-decisions:{}:{}", bar.symbol, bar.timestamp),
        )?;

        if self.state.current_trade_date != Some(bar.timestamp.date()) {
            self.gateway.roll_trading_day(bar.timestamp.date())?;
            let day_event = self.event(
                EventType::TradingDayRolled,
                bar.timestamp,
                &market_key,
                format!("trading-day:{}", bar.timestamp.date()),
                json!({"date": bar.timestamp.date().to_string()}),
            );
            self.record(day_event)?;
        }

        if self.state.mode != ServiceMode::Running {
            if !decisions_committed {
                if let Some(previous_close) = previous_close {
                    let (mut touched, mut classification) =
                        crossed_levels(&self.state.levels, previous_close, bar.low, bar.high);
                    let same_side_reversal_ambiguity =
                        self.has_same_side_reversal_ambiguity(previous_close, bar, &touched)?;
                    if same_side_reversal_ambiguity {
                        classification = TouchClassification::Ambiguous;
                    }
                    if classification == TouchClassification::Ambiguous {
                        let ambiguous = self.event(
                            EventType::AmbiguousBarDetected,
                            bar.timestamp,
                            &market_key,
                            format!("ambiguous:{}", bar.timestamp),
                            json!({
                                "levels": touched,
                                "reason": if same_side_reversal_ambiguity {
                                    "SAME_SIDE_REVERSAL_ORDER_UNKNOWN"
                                } else {
                                    "BOTH_DIRECTIONS_TOUCHED"
                                }
                            }),
                        );
                        self.record(ambiguous)?;
                    } else {
                        self.revoke_rights_after_clear_reversal(previous_close, bar, &touched)?;
                    }
                    touched.sort_by_key(|index| index.abs());
                    let reason = format!("SERVICE_MODE_{:?}", self.state.mode).to_uppercase();
                    for index in touched {
                        self.record_touch_and_skip(index, bar, &reason)?;
                    }
                }
                self.record_bar_decisions_committed(bar)?;
            }
            self.maybe_inject_fault(WorkflowFaultPoint::DecisionsCommitted)?;
            self.rearm_levels(bar)?;
            self.push_close(bar.close);
            self.record_bar_processed(bar)?;
            self.maybe_inject_fault(WorkflowFaultPoint::BarProcessed)?;
            self.maybe_reset_cycle(bar.timestamp)?;
            return Ok(());
        }
        let Some(previous_close) = previous_close else {
            self.record_bar_decisions_committed(bar)?;
            self.maybe_inject_fault(WorkflowFaultPoint::DecisionsCommitted)?;
            self.push_close(bar.close);
            self.record_bar_processed(bar)?;
            self.maybe_inject_fault(WorkflowFaultPoint::BarProcessed)?;
            self.maybe_reset_cycle(bar.timestamp)?;
            return Ok(());
        };
        if decisions_committed {
            self.rearm_levels(bar)?;
            self.push_close(bar.close);
            self.record_bar_processed(bar)?;
            self.maybe_inject_fault(WorkflowFaultPoint::BarProcessed)?;
            self.maybe_reset_cycle(bar.timestamp)?;
            return Ok(());
        }
        let (mut touched, mut classification) =
            crossed_levels(&self.state.levels, previous_close, bar.low, bar.high);
        let same_side_reversal_ambiguity =
            self.has_same_side_reversal_ambiguity(previous_close, bar, &touched)?;
        if same_side_reversal_ambiguity {
            classification = TouchClassification::Ambiguous;
        }
        if classification == TouchClassification::Ambiguous {
            let ambiguous = self.event(
                EventType::AmbiguousBarDetected,
                bar.timestamp,
                &market_key,
                format!("ambiguous:{}", bar.timestamp),
                json!({
                    "levels": touched,
                    "reason": if same_side_reversal_ambiguity {
                        "SAME_SIDE_REVERSAL_ORDER_UNKNOWN"
                    } else {
                        "BOTH_DIRECTIONS_TOUCHED"
                    }
                }),
            );
            self.record(ambiguous)?;
            match self.config.ambiguity_policy.as_str() {
                "skip" | "conservative" => {
                    for index in touched {
                        self.record_touch_and_skip(index, bar, "AMBIGUOUS_INTRABAR_PATH")?;
                    }
                    self.record_bar_decisions_committed(bar)?;
                    self.maybe_inject_fault(WorkflowFaultPoint::DecisionsCommitted)?;
                    self.rearm_levels(bar)?;
                    self.push_close(bar.close);
                    self.record_bar_processed(bar)?;
                    self.maybe_inject_fault(WorkflowFaultPoint::BarProcessed)?;
                    self.maybe_reset_cycle(bar.timestamp)?;
                    return Ok(());
                }
                "lower_first" => touched.sort(),
                "upper_first" => touched.sort_by(|a, b| b.cmp(a)),
                _ => unreachable!(),
            }
        } else if touched.iter().all(|i| *i < 0) {
            touched.sort_by(|a, b| b.cmp(a));
        } else {
            touched.sort();
        }
        self.revoke_rights_after_clear_reversal(previous_close, bar, &touched)?;
        for index in touched {
            if self.state.mode == ServiceMode::Running {
                self.process_touch(index, bar)?;
            } else {
                let reason = format!("SERVICE_MODE_{:?}", self.state.mode).to_uppercase();
                self.record_touch_and_skip(index, bar, &reason)?;
            }
        }
        self.record_bar_decisions_committed(bar)?;
        self.maybe_inject_fault(WorkflowFaultPoint::DecisionsCommitted)?;
        self.rearm_levels(bar)?;
        self.push_close(bar.close);
        self.record_bar_processed(bar)?;
        self.maybe_inject_fault(WorkflowFaultPoint::BarProcessed)?;
        self.maybe_reset_cycle(bar.timestamp)
    }

    fn has_same_side_reversal_ambiguity(
        &self,
        previous_close: Decimal,
        bar: &MarketBar,
        forward_touches: &[i32],
    ) -> Result<bool> {
        let grid = GridSpec::from(&self.config);
        let results = [Direction::Buy, Direction::Sell]
            .into_iter()
            .map(|direction| -> Result<bool> {
                let deepest = self
                    .state
                    .right_tranches
                    .values()
                    .filter(|tranche| {
                        tranche.cycle_id == self.state.cycle_id
                            && tranche.direction == direction
                            && (tranche.available_budget > Decimal::ZERO
                                || tranche.available_quantity > 0)
                    })
                    .map(|tranche| tranche.birth_grid_index)
                    .max_by_key(|index| index.abs());
                let Some(deepest) = deepest else {
                    return Ok(false);
                };
                let reversal_price = match direction {
                    Direction::Buy => grid.price(deepest + 1)?,
                    Direction::Sell => grid.price(deepest - 1)?,
                };
                let reversed = match direction {
                    Direction::Buy => previous_close < reversal_price && bar.high >= reversal_price,
                    Direction::Sell => previous_close > reversal_price && bar.low <= reversal_price,
                };
                let moved_forward = forward_touches.iter().any(|index| match direction {
                    Direction::Buy => *index < 0,
                    Direction::Sell => *index > 0,
                });
                Ok(reversed && moved_forward)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(results.into_iter().any(|result| result))
    }

    /// A one-grid reversal destroys only the unconsumed authority at the
    /// active frontier. If the same OHLC bar also reaches a deeper level, the
    /// path order is unknowable, so no revocation is inferred here.
    fn revoke_rights_after_clear_reversal(
        &mut self,
        previous_close: Decimal,
        bar: &MarketBar,
        forward_touches: &[i32],
    ) -> Result<()> {
        let grid = GridSpec::from(&self.config);
        let mut event_groups = Vec::new();
        for direction in [Direction::Buy, Direction::Sell] {
            let has_forward_touch = forward_touches.iter().any(|index| match direction {
                Direction::Buy => *index < 0,
                Direction::Sell => *index > 0,
            });
            if has_forward_touch {
                continue;
            }
            let mut active_depths: Vec<_> = self
                .state
                .right_tranches
                .values()
                .filter(|tranche| {
                    tranche.cycle_id == self.state.cycle_id
                        && tranche.direction == direction
                        && (tranche.available_budget > Decimal::ZERO
                            || tranche.available_quantity > 0)
                })
                .map(|tranche| tranche.birth_grid_index)
                .collect();
            active_depths.sort_by_key(|index| std::cmp::Reverse(index.abs()));
            active_depths.dedup();
            for depth in active_depths {
                let reversal_index = match direction {
                    Direction::Buy => depth + 1,
                    Direction::Sell => depth - 1,
                };
                let reversal_price = grid.price(reversal_index)?;
                let crossed = match direction {
                    Direction::Buy => previous_close < reversal_price && bar.high >= reversal_price,
                    Direction::Sell => previous_close > reversal_price && bar.low <= reversal_price,
                };
                if !crossed {
                    continue;
                }
                let mut by_owner = std::collections::BTreeMap::<String, Vec<Value>>::new();
                for tranche in self.state.right_tranches.values().filter(|tranche| {
                    tranche.cycle_id == self.state.cycle_id
                        && tranche.direction == direction
                        && tranche.birth_grid_index == depth
                        && (tranche.available_budget > Decimal::ZERO
                            || tranche.available_quantity > 0)
                }) {
                    by_owner
                        .entry(tranche.owner_right_id.clone())
                        .or_default()
                        .push(json!({
                            "tranche_id": tranche.tranche_id,
                            "budget": tranche.available_budget.to_string(),
                            "quantity": tranche.available_quantity
                        }));
                }
                event_groups.extend(by_owner.into_iter().map(|(owner, allocations)| {
                    (owner, depth, reversal_index, reversal_price, allocations)
                }));
            }
        }
        if event_groups.is_empty() {
            return Ok(());
        }
        let mut revoking_by_owner =
            std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();
        for (owner, _, _, _, allocations) in &event_groups {
            for allocation in allocations {
                if let Some(tranche_id) = allocation.get("tranche_id").and_then(Value::as_str) {
                    revoking_by_owner
                        .entry(owner.clone())
                        .or_default()
                        .insert(tranche_id.to_owned());
                }
            }
        }
        let terminal_owners: Vec<_> = revoking_by_owner
            .iter()
            .filter_map(|(owner, revoking)| {
                let active_owned: Vec<_> = self
                    .state
                    .right_tranches
                    .values()
                    .filter(|tranche| {
                        tranche.owner_right_id == *owner
                            && (tranche.available_budget > Decimal::ZERO
                                || tranche.available_quantity > 0)
                    })
                    .map(|tranche| tranche.tranche_id.as_str())
                    .collect();
                (!active_owned.is_empty()
                    && active_owned
                        .iter()
                        .all(|tranche_id| revoking.contains(*tranche_id)))
                .then(|| {
                    let right = self
                        .state
                        .grid_rights
                        .get(owner)
                        .expect("tranche owner must reference a right");
                    (
                        owner.clone(),
                        right.remaining_budget(),
                        right.remaining_quantity(),
                    )
                })
            })
            .collect();
        let mut events = event_groups
            .into_iter()
            .map(
                |(owner, deepest, reversal_index, reversal_price, allocations)| {
                    self.event(
                        EventType::RightBalanceRevoked,
                        bar.timestamp,
                        &format!("right-reversal:{owner}"),
                        format!("tranche-revoked:{owner}:{deepest}:{}", bar.timestamp),
                        json!({
                            "right_id": owner,
                            "reason": "PRICE_REVERSED_ONE_GRID",
                            "reversal_from_grid": deepest,
                            "reversal_grid_index": reversal_index,
                            "reversal_price": reversal_price.to_string(),
                            "allocation_policy": "MARGINAL_TRANCHE_ONLY_V1",
                            "allocations": allocations
                        }),
                    )
                },
            )
            .collect::<Vec<_>>();
        events.extend(terminal_owners.into_iter().map(
            |(owner, revoked_budget, revoked_quantity)| {
                self.event(
                    EventType::GridRightRevoked,
                    bar.timestamp,
                    &format!("right-reversal:{owner}"),
                    format!("right-revoked:{owner}:{}", bar.timestamp),
                    json!({
                        "right_id": owner,
                        "reason": "ALL_UNEXERCISED_BALANCE_REVOKED_BY_PRICE_REVERSAL",
                        "revoked_budget": revoked_budget.to_string(),
                        "revoked_quantity": revoked_quantity
                    }),
                )
            },
        ));
        self.record_batch(&mut events)
    }

    fn process_touch(&mut self, index: i32, bar: &MarketBar) -> Result<()> {
        let grid = GridSpec::from(&self.config);
        if !grid.is_trade_level(index) {
            return self.record_touch_and_skip(index, bar, "OBSERVATION_BOUNDARY");
        }
        let price = self
            .state
            .levels
            .get(&index)
            .context("missing grid level")?
            .price;
        let direction = if index < 0 {
            Direction::Buy
        } else {
            Direction::Sell
        };
        let coordinator = RightsGateCoordinator::new(&self.config, &self.state);
        if coordinator.has_exercised_at(index, direction) {
            return self.record_touch_and_skip(index, bar, "RIGHT_ALREADY_EXERCISED_AT_LEVEL");
        }
        if !coordinator.may_grant_at(index, direction) {
            return self.record_touch_and_skip(index, bar, "RIGHT_CARRIED_TO_DEEPER_LEVEL");
        }
        let carry_sources: Vec<GridRight> = coordinator
            .carry_sources(direction)
            .into_iter()
            .cloned()
            .collect();
        let right_id = GridRight::id_for(
            &self.state.run_id,
            &self.state.cycle_id,
            index,
            bar.timestamp,
        );
        let capacity = coordinator.capacity(index, direction, bar.timestamp.date(), &right_id)?;
        if direction == Direction::Buy
            && if self.config.uses_fixed_quantity() {
                capacity.available_quantity <= 0
            } else {
                capacity.available_budget <= Decimal::ZERO
            }
        {
            return self.record_touch_and_skip(index, bar, "BUY_NO_AVAILABLE_CAPACITY");
        }
        if direction == Direction::Sell
            && capacity.eligible_quantity
                + capacity.t_plus_one_blocked_quantity
                + capacity.risk_blocked_quantity
                + capacity.no_profit_blocked_quantity
                == 0
        {
            return self.record_touch_and_skip(index, bar, "SELL_NO_STRATEGY_CAPACITY");
        }
        let right = GridRight::granted(
            &self.state.run_id,
            &self.state.cycle_id,
            &self.state.symbol,
            direction,
            index,
            price,
            bar.timestamp,
            capacity,
        );
        let context = self.gate_context(&right);
        let request = DecisionRequest::new(right.clone(), context);
        let residual_only = request.context.algorithm_authorized_quantity == 0
            && request.context.platform_residual_quantity > 0;
        let evaluated = if residual_only {
            GateAlgorithmAdapter::new(Box::new(FixedGate {
                probability: Decimal::ZERO,
                alpha: Decimal::ZERO,
            }))
            .decide(&request)
        } else {
            self.evaluate_algorithm(request.clone())
                .and_then(|response| {
                    response.validate_for(&request)?;
                    Ok(response)
                })
        };
        let (response, algorithm_failure, algorithm_error_event) = match evaluated {
            Ok(response) => (response, None, None),
            Err(error) => {
                let mode = self.config.gate.failure_mode.clone();
                let reason = if error.to_string().contains("timed out") {
                    "ALGORITHM_TIMEOUT"
                } else if error.to_string().contains("panicked") {
                    "ALGORITHM_PANIC"
                } else {
                    "INVALID_DECISION"
                };
                let error_event = self.event(
                    EventType::ErrorRecorded,
                    bar.timestamp,
                    "gate-failure",
                    format!(
                        "gate-failure:{}:{}:{index}",
                        self.state.cycle_id, bar.timestamp
                    ),
                    json!({
                        "component": "gate",
                        "error": error.to_string(),
                        "reason_code": reason,
                        "failure_mode": mode,
                        "entered_safe": mode == "safe",
                        "fallback_algorithm": if mode == "always_execute" {
                            "builtin-always-execute"
                        } else {
                            "none"
                        }
                    }),
                );
                let response = match mode.as_str() {
                    "always_execute" => {
                        GateAlgorithmAdapter::new(Box::new(AlwaysExecuteGate)).decide(&request)?
                    }
                    "safe" | "skip" => GateAlgorithmAdapter::new(Box::new(FixedGate {
                        probability: Decimal::ZERO,
                        alpha: Decimal::ZERO,
                    }))
                    .decide(&request)?,
                    _ => unreachable!("validated config"),
                };
                (response, Some(reason.to_owned()), Some(error_event))
            }
        };
        let (exercise_quantity, _defer_quantity) = response.outcome.quantities();
        let correlation = format!("touch:{}:{}:{index}", self.state.cycle_id, bar.timestamp);
        let mut events = vec![self.event(
            EventType::GridLevelTouched,
            bar.timestamp,
            &correlation,
            format!(
                "grid-touch:{}:{}:{index}",
                self.state.cycle_id, bar.timestamp
            ),
            json!({"grid_index": index, "price": price.to_string()}),
        )];
        if let Some(error_event) = algorithm_error_event {
            events.push(error_event);
        }
        events.push(self.event(
            EventType::GridRightGranted,
            bar.timestamp,
            &correlation,
            format!("right-granted:{}", right.right_id),
            serde_json::to_value(&right)?,
        ));
        for source in &carry_sources {
            let source_tranches: Vec<_> = self
                .state
                .right_tranches
                .values()
                .filter(|tranche| {
                    tranche.owner_right_id == source.right_id
                        && (tranche.available_budget > Decimal::ZERO
                            || tranche.available_quantity > 0)
                })
                .collect();
            let tranche_ids: Vec<_> = source_tranches
                .iter()
                .map(|tranche| tranche.tranche_id.clone())
                .collect();
            let transferred_budget: Decimal = source_tranches
                .iter()
                .map(|tranche| tranche.available_budget)
                .sum();
            let transferred_quantity: i64 = source_tranches
                .iter()
                .map(|tranche| tranche.available_quantity)
                .sum();
            if !tranche_ids.is_empty() {
                events.push(self.event(
                    EventType::RightBalanceTransferred,
                    bar.timestamp,
                    &correlation,
                    format!("tranche-transfer:{}:{}", source.right_id, right.right_id),
                    json!({
                        "source_right_id": source.right_id,
                        "target_right_id": right.right_id,
                        "tranche_ids": tranche_ids.clone()
                    }),
                ));
            }
            events.push(self.event(
                EventType::GridRightTransferred,
                bar.timestamp,
                &correlation,
                format!("right-carried:{}:{}", source.right_id, right.right_id),
                json!({
                    "right_id": source.right_id,
                    "reason": "CARRIED_TO_DEEPER_RIGHT",
                    "target_right_id": right.right_id,
                    "tranche_ids": tranche_ids,
                    "transferred_budget": transferred_budget.to_string(),
                    "transferred_quantity": transferred_quantity
                }),
            ));
        }
        let minted_tranches = if direction == Direction::Buy {
            if self.config.uses_fixed_quantity() {
                vec![RightTranche::minted_buy_quantity(
                    &self.state.cycle_id,
                    &right.right_id,
                    &right.right_id,
                    index,
                    self.config.standard_quantity,
                )]
            } else {
                vec![RightTranche::minted_budget(
                    &self.state.cycle_id,
                    &right.right_id,
                    &right.right_id,
                    index,
                    self.config.standard_budget,
                )]
            }
        } else {
            sell_tranches_to_mint(&self.state, &right.right_id, index)
        };
        for tranche in minted_tranches {
            events.push(self.event(
                EventType::RightTrancheMinted,
                bar.timestamp,
                &correlation,
                format!("tranche-minted:{}", tranche.tranche_id),
                serde_json::to_value(tranche)?,
            ));
        }
        events.push(self.event(
            EventType::GateDecisionMade,
            bar.timestamp,
            &correlation,
            format!("decision:{}", right.right_id),
            json!({
                "request": &request,
                "response": &response,
                "algorithm_succeeded": algorithm_failure.is_none() && !residual_only
            }),
        ));
        if direction == Direction::Buy {
            let capacity_event_type = if self.config.uses_fixed_quantity() {
                EventType::DeferredQuantityAccrued
            } else {
                EventType::DeferredBudgetAccrued
            };
            events.push(self.event(
                capacity_event_type,
                bar.timestamp,
                &correlation,
                format!("capacity:{}", right.right_id),
                json!({
                    "right_id": right.right_id,
                    "grid_index": index,
                    "mechanical_cap": right.capacity.mechanical_budget_cap.to_string(),
                    "deployed": right.capacity.deployed_budget.to_string(),
                    "available_budget": right.capacity.available_budget.to_string(),
                    "mechanical_quantity_cap": right.capacity.mechanical_quantity_cap,
                    "deployed_quantity": right.capacity.deployed_quantity,
                    "available_quantity": right.capacity.available_quantity,
                    "accumulated_grid_indices": right.capacity.accumulated_grid_indices,
                    "source_right_ids": right.capacity.source_right_ids,
                    "carried_in_budget": right.capacity.carried_in_budget.to_string()
                }),
            ));
        }
        if let Some(reason) = algorithm_failure.as_deref() {
            if self.config.gate.failure_mode != "always_execute" {
                self.push_blocked_events(&mut events, &right, &request, &response, bar, reason);
                return self.record_batch(&mut events);
            }
        }
        if direction == Direction::Sell
            && right.capacity.eligible_quantity == 0
            && right.capacity.t_plus_one_blocked_quantity
                + right.capacity.risk_blocked_quantity
                + right.capacity.no_profit_blocked_quantity
                > 0
        {
            let reason = if right.capacity.t_plus_one_blocked_quantity > 0 {
                "SELL_BLOCKED_T_PLUS_ONE"
            } else if right.capacity.risk_blocked_quantity > 0 {
                "SELL_BLOCKED_BY_POSITION_LIMIT"
            } else {
                "SELL_BLOCKED_BELOW_BREAK_EVEN"
            };
            self.push_blocked_events(&mut events, &right, &request, &response, bar, reason);
            return self.record_batch(&mut events);
        }
        if exercise_quantity == 0 {
            if request.context.algorithm_authorized_quantity == 0
                && request.context.platform_residual_quantity > 0
            {
                self.push_residual_event(&mut events, &right, &request, &response, bar);
            } else {
                self.push_deferred_events(
                    &mut events,
                    &right,
                    &request,
                    &response,
                    bar,
                    "ALGORITHM_DEFERRED_ALL_QUANTITY",
                );
            }
            return self.record_batch(&mut events);
        }
        let intent_result = if direction == Direction::Buy {
            let risk = DefaultRiskChecker {
                config: &self.config,
            };
            let approved_quantity =
                canonical_approved_quantity(&self.config, &self.state, &right, exercise_quantity)?;
            if approved_quantity == 0 {
                let last_error = risk
                    .check_buy(
                        &self.state,
                        exercise_quantity,
                        self.buy_reservation(price, exercise_quantity)?,
                    )
                    .err();
                Err(format!(
                    "BUY_RISK_REJECTED:{}",
                    last_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "no whole unit approved".to_owned())
                ))
            } else {
                Ok(self.make_intent(
                    &right.right_id,
                    index,
                    direction,
                    price,
                    approved_quantity,
                    price
                        .checked_mul(Decimal::from(approved_quantity))
                        .context("BUY intent budget exceeds numeric range")?,
                    Vec::new(),
                    bar.timestamp,
                ))
            }
        } else {
            let quantity = exercise_quantity;
            if right.capacity.eligible_lot_ids.is_empty() {
                Err(if right.capacity.t_plus_one_blocked_quantity > 0 {
                    "SELL_BLOCKED_T_PLUS_ONE".to_owned()
                } else if right.capacity.risk_blocked_quantity > 0 {
                    "SELL_BLOCKED_BY_POSITION_LIMIT".to_owned()
                } else if right.capacity.no_profit_blocked_quantity > 0 {
                    "SELL_BLOCKED_BELOW_BREAK_EVEN".to_owned()
                } else {
                    "SELL_NO_ELIGIBLE_LOTS".to_owned()
                })
            } else {
                let approval = canonical_approval(&self.config, &self.state, &right, quantity)?;
                let approved_quantity = approval.quantity;
                let mut allocations = approval.sell_allocations;
                for allocation in &mut allocations {
                    allocation.tranche_id = self
                        .state
                        .right_tranches
                        .values()
                        .find(|tranche| {
                            tranche.direction == Direction::Sell
                                && tranche.available_quantity > 0
                                && tranche.lot_id.as_deref() == Some(&allocation.lot_id)
                                && right.capacity.tranche_ids.contains(&tranche.tranche_id)
                        })
                        .map(|tranche| tranche.tranche_id.clone())
                        .unwrap_or_else(|| {
                            RightTranche::id_for(
                                &self.state.cycle_id,
                                &right.right_id,
                                Direction::Sell,
                                index,
                                Some(&allocation.lot_id),
                            )
                        });
                }
                let risk = DefaultRiskChecker {
                    config: &self.config,
                };
                if approved_quantity == 0 {
                    Err("SELL_BLOCKED_BELOW_BREAK_EVEN".to_owned())
                } else if let Err(error) = risk.check_sell(&self.state, approved_quantity) {
                    Err(format!("SELL_RISK_REJECTED:{error}"))
                } else {
                    Ok(self.make_intent(
                        &right.right_id,
                        index,
                        direction,
                        price,
                        approved_quantity,
                        price
                            .checked_mul(Decimal::from(approved_quantity))
                            .context("SELL intent budget exceeds numeric range")?,
                        allocations,
                        bar.timestamp,
                    ))
                }
            }
        };
        let intent = match intent_result {
            Ok(intent) => intent,
            Err(reason) => {
                self.push_blocked_events(&mut events, &right, &request, &response, bar, &reason);
                self.record_batch(&mut events)?;
                return Ok(());
            }
        };
        let order = self.order_for_intent(&intent)?;
        if right.capacity.t_plus_one_blocked_quantity > 0
            || right.capacity.risk_blocked_quantity > 0
            || right.capacity.no_profit_blocked_quantity > 0
        {
            events.push(self.event(
                EventType::GridRightBlocked,
                bar.timestamp,
                &correlation,
                format!("right-partial-blocked:{}", right.right_id),
                json!({
                    "right_id": right.right_id,
                    "decision_id": response.request_id,
                    "partial": true,
                    "reason": if right.capacity.t_plus_one_blocked_quantity > 0 {
                        "SELL_PARTIALLY_BLOCKED_T_PLUS_ONE"
                    } else if right.capacity.no_profit_blocked_quantity > 0 {
                        "SELL_PARTIALLY_BLOCKED_BELOW_BREAK_EVEN"
                    } else {
                        "SELL_PARTIALLY_BLOCKED_BY_POSITION_LIMIT"
                    },
                    "t_plus_one_blocked_quantity": right.capacity.t_plus_one_blocked_quantity,
                    "t_plus_one_blocked_lot_ids": right.capacity.t_plus_one_blocked_lot_ids,
                    "risk_blocked_quantity": right.capacity.risk_blocked_quantity,
                    "risk_blocked_lot_ids": right.capacity.risk_blocked_lot_ids,
                    "no_profit_blocked_quantity": right.capacity.no_profit_blocked_quantity,
                    "no_profit_blocked_lot_ids": right.capacity.no_profit_blocked_lot_ids
                }),
            ));
        }
        let reservation_payload = if intent.direction == Direction::Buy
            && !self.config.uses_fixed_quantity()
        {
            json!({
                "right_id": right.right_id,
                "decision_id": response.request_id,
                "reserved_budget": (intent.limit_price * Decimal::from(intent.quantity)).to_string()
            })
        } else {
            let (exercise_quantity, defer_quantity) = response.outcome.quantities();
            let platform_blocked_quantity = exercise_quantity - intent.quantity;
            json!({
                "right_id": right.right_id,
                "decision_id": response.request_id,
                "reserved_quantity": intent.quantity,
                "gross_available_quantity": request.context.gross_available_quantity,
                "platform_residual_quantity": request.context.platform_residual_quantity,
                "algorithm_authorized_quantity": request.context.algorithm_authorized_quantity,
                "platform_blocked_quantity": platform_blocked_quantity,
                "order_intent_quantity": intent.quantity,
                "remaining_decision_quantity": defer_quantity + platform_blocked_quantity
            })
        };
        let reservation_allocations = self.tranche_allocations(
            &right.right_id,
            Some(&right),
            intent.direction,
            if intent.direction == Direction::Buy && !self.config.uses_fixed_quantity() {
                intent.limit_price * Decimal::from(intent.quantity)
            } else {
                Decimal::ZERO
            },
            if intent.direction == Direction::Sell
                || (intent.direction == Direction::Buy && self.config.uses_fixed_quantity())
            {
                intent.quantity
            } else {
                0
            },
            &intent.target_lot_ids,
            &intent.target_lot_allocations,
            false,
        )?;
        if !reservation_allocations.is_empty() {
            events.push(self.event(
                EventType::RightBalanceReserved,
                bar.timestamp,
                &correlation,
                format!("tranche-reserved:{}", right.right_id),
                json!({
                    "right_id": right.right_id,
                    "allocation_policy": "DEEPEST_FIRST_V1",
                    "allocations": reservation_allocations
                }),
            ));
        }
        events.push(self.event(
            EventType::GridRightReserved,
            bar.timestamp,
            &correlation,
            format!("right-reserved:{}", right.right_id),
            reservation_payload,
        ));
        events.push(self.event(
            EventType::OrderIntentCreated,
            bar.timestamp,
            &correlation,
            format!("intent:{}", intent.intent_id),
            serde_json::to_value(&order)?,
        ));
        self.record_batch(&mut events)?;
        self.maybe_inject_fault(WorkflowFaultPoint::IntentCommitted)?;
        self.dispatch_intent(intent, bar.timestamp, &correlation)
    }

    fn evaluate_algorithm(
        &self,
        request: DecisionRequest,
    ) -> Result<crate::decision::DecisionResponse> {
        let algorithm = Arc::clone(&self.algorithm);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(format!("grid-decision-{}", request.right.right_id))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    algorithm.decide(&request)
                }))
                .map_err(|_| anyhow!("quant decision algorithm panicked"))
                .and_then(|result| result);
                let _ = sender.send(result);
            })
            .context("failed to start quant decision worker")?;
        receiver
            .recv_timeout(Duration::from_millis(self.config.gate.timeout_ms))
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => {
                    anyhow!("quant decision algorithm timed out")
                }
                std::sync::mpsc::RecvTimeoutError::Disconnected => {
                    anyhow!("quant decision worker disconnected")
                }
            })?
    }

    fn dispatch_intent(
        &mut self,
        intent: OrderIntent,
        time: NaiveDateTime,
        correlation: &str,
    ) -> Result<()> {
        let order_id = self.order_for_intent(&intent)?.order_id;
        let submitted = self.event(
            EventType::OrderSubmitted,
            time,
            correlation,
            format!("submit:{}", order_id),
            json!({"order_id": order_id}),
        );
        self.record(submitted)?;
        self.maybe_inject_fault(WorkflowFaultPoint::OrderSubmitted)?;
        let report = self.gateway.submit(&intent)?;
        self.maybe_inject_fault(WorkflowFaultPoint::BrokerSideEffect)?;
        self.apply_execution_report(&intent, time, correlation, report)
    }

    fn apply_execution_report(
        &mut self,
        intent: &OrderIntent,
        time: NaiveDateTime,
        correlation: &str,
        report: ExecutionReport,
    ) -> Result<()> {
        match report {
            ExecutionReport::Rejected { order_id, reason } => {
                let rejected = self.event(
                    EventType::OrderRejected,
                    time,
                    correlation,
                    format!("reject:{order_id}"),
                    json!({"order_id": order_id, "reason": reason}),
                );
                let mut events = vec![rejected];
                if !intent.right_id.is_empty() {
                    let release_allocations = self.tranche_allocations(
                        &intent.right_id,
                        None,
                        intent.direction,
                        Decimal::MAX,
                        i64::MAX,
                        &intent.target_lot_ids,
                        &intent.target_lot_allocations,
                        true,
                    )?;
                    if !release_allocations.is_empty() {
                        events.push(self.event(
                            EventType::RightBalanceReleased,
                            time,
                            correlation,
                            format!("tranche-released:{}", intent.right_id),
                            json!({
                                "right_id": intent.right_id,
                                "reason": "ORDER_REJECTED",
                                "allocations": release_allocations
                            }),
                        ));
                    }
                    events.push(self.event(
                        EventType::GridRightReleased,
                        time,
                        correlation,
                        format!("right-released:{}", intent.right_id),
                        json!({
                            "right_id": intent.right_id,
                            "reason": "ORDER_REJECTED"
                        }),
                    ));
                }
                self.record_batch(&mut events)?;
            }
            ExecutionReport::Cancelled { order_id, reason } => {
                let cancelled = self.event(
                    EventType::OrderCancelled,
                    time,
                    correlation,
                    format!("cancelled:{order_id}"),
                    json!({"order_id": order_id, "reason": reason}),
                );
                let mut events = vec![cancelled];
                if !intent.right_id.is_empty() {
                    let release_allocations = self.tranche_allocations(
                        &intent.right_id,
                        None,
                        intent.direction,
                        Decimal::MAX,
                        i64::MAX,
                        &intent.target_lot_ids,
                        &intent.target_lot_allocations,
                        true,
                    )?;
                    if !release_allocations.is_empty() {
                        events.push(self.event(
                            EventType::RightBalanceReleased,
                            time,
                            correlation,
                            format!("tranche-cancel-released:{}", intent.right_id),
                            json!({
                                "right_id": intent.right_id,
                                "reason": "ORDER_CANCELLED",
                                "allocations": release_allocations
                            }),
                        ));
                    }
                    events.push(self.event(
                        EventType::GridRightReleased,
                        time,
                        correlation,
                        format!("right-cancel-released:{}", intent.right_id),
                        json!({"right_id": intent.right_id, "reason": "ORDER_CANCELLED"}),
                    ));
                }
                self.record_batch(&mut events)?;
                self.gateway.synchronize_snapshot(self.state.snapshot()?);
            }
            ExecutionReport::Accepted { order_id, fills } => {
                let accepted = self.event(
                    EventType::OrderAccepted,
                    time,
                    correlation,
                    format!("accept:{order_id}"),
                    json!({"order_id": order_id}),
                );
                self.record(accepted)?;
                self.maybe_inject_fault(WorkflowFaultPoint::OrderAccepted)?;
                for report in fills {
                    if self.state.processed_fills.contains(&report.fill_id) {
                        continue;
                    }
                    let notional = report
                        .price
                        .checked_mul(Decimal::from(report.quantity))
                        .context("NUMERIC_RANGE_VIOLATION: broker fill notional overflow")?;
                    let order = self
                        .state
                        .orders
                        .get(&order_id)
                        .context("accepted order missing")?;
                    let (commission, tax) = checked_actual_order_fill_charges_with_policy(
                        &ProfitGuardPolicy::from(&self.config),
                        intent.direction,
                        order.filled_notional,
                        order.commission_charged,
                        report.price,
                        report.quantity,
                    )
                    .context("NUMERIC_RANGE_VIOLATION: broker fill charge overflow")?;
                    let fees = commission
                        .checked_add(tax)
                        .context("NUMERIC_RANGE_VIOLATION: broker fill fees overflow")?;
                    let fill = Fill {
                        fill_id: report.fill_id.clone(),
                        order_id: order_id.clone(),
                        direction: intent.direction,
                        price: report.price,
                        quantity: report.quantity,
                        commission,
                        tax,
                        trade_date: time.date(),
                        target_lot_ids: intent.target_lot_ids.clone(),
                        target_lot_allocations: self.fill_lot_allocations(
                            intent,
                            order.filled_quantity,
                            report.quantity,
                        )?,
                    };
                    let event_type = if report.final_fill {
                        EventType::OrderFilled
                    } else {
                        EventType::OrderPartiallyFilled
                    };
                    let mut fill_events = vec![
                        self.event(
                            event_type,
                            time,
                            correlation,
                            format!("fill:{}", report.fill_id),
                            json!({"fill": fill, "grid_index": intent.grid_index}),
                        ),
                        self.event(
                            EventType::CashChanged,
                            time,
                            correlation,
                            format!("cash:{}", report.fill_id),
                            json!({"fill_id": report.fill_id, "direction": intent.direction, "notional": notional.to_string(), "fees": fees.to_string()}),
                        ),
                        self.event(
                            EventType::PositionChanged,
                            time,
                            correlation,
                            format!("position:{}", report.fill_id),
                            json!({"fill_id": report.fill_id, "quantity": report.quantity, "direction": intent.direction}),
                        ),
                    ];
                    fill_events.push(self.event(
                        if intent.direction == Direction::Buy {
                            EventType::LotOpened
                        } else if report.final_fill {
                            EventType::LotClosed
                        } else {
                            EventType::LotPartiallyClosed
                        },
                        time,
                        correlation,
                        format!("lot-change:{}", report.fill_id),
                        json!({"fill_id": report.fill_id, "target_lots": intent.target_lot_ids}),
                    ));
                    if !intent.right_id.is_empty() {
                        let right = self
                            .state
                            .grid_rights
                            .get(&intent.right_id)
                            .context("fill references a missing grid right")?;
                        let (fully_exercised, exercise_payload) = if intent.direction
                            == Direction::Buy
                            && !self.config.uses_fixed_quantity()
                        {
                            let delta = intent.limit_price * Decimal::from(report.quantity);
                            (
                                right.exercised_budget + delta == right.capacity.available_budget,
                                json!({
                                    "right_id": intent.right_id,
                                    "exercised_budget_delta": delta.to_string()
                                }),
                            )
                        } else {
                            (
                                right.exercised_quantity + report.quantity
                                    == if intent.direction == Direction::Buy {
                                        right.capacity.available_quantity
                                    } else {
                                        right.capacity.eligible_quantity
                                    }
                                    && (intent.direction == Direction::Buy
                                        || (right.capacity.t_plus_one_blocked_quantity == 0
                                            && right.capacity.risk_blocked_quantity == 0
                                            && right.capacity.no_profit_blocked_quantity == 0)),
                                json!({
                                    "right_id": intent.right_id,
                                    "exercised_quantity_delta": report.quantity
                                }),
                            )
                        };
                        let consumed_allocations = self.tranche_allocations(
                            &intent.right_id,
                            None,
                            intent.direction,
                            if intent.direction == Direction::Buy
                                && !self.config.uses_fixed_quantity()
                            {
                                intent.limit_price * Decimal::from(report.quantity)
                            } else {
                                Decimal::ZERO
                            },
                            if intent.direction == Direction::Sell
                                || (intent.direction == Direction::Buy
                                    && self.config.uses_fixed_quantity())
                            {
                                report.quantity
                            } else {
                                0
                            },
                            &intent.target_lot_ids,
                            &fill.target_lot_allocations,
                            true,
                        )?;
                        if !consumed_allocations.is_empty() {
                            fill_events.push(self.event(
                                EventType::RightBalanceConsumed,
                                time,
                                correlation,
                                format!("tranche-consumed:{}", report.fill_id),
                                json!({
                                    "right_id": intent.right_id,
                                    "fill_id": report.fill_id,
                                    "allocation_policy": "DEEPEST_FIRST_V1",
                                    "allocations": consumed_allocations
                                }),
                            ));
                        }
                        fill_events.push(self.event(
                            if report.final_fill && fully_exercised {
                                EventType::GridRightExercised
                            } else {
                                EventType::GridRightPartiallyExercised
                            },
                            time,
                            correlation,
                            format!("right-fill:{}", report.fill_id),
                            exercise_payload,
                        ));
                    }
                    self.record_batch(&mut fill_events)?;
                    self.gateway.synchronize_snapshot(self.state.snapshot()?);
                    self.maybe_inject_fault(WorkflowFaultPoint::FillApplied)?;
                }
                if intent.direction == Direction::Sell {}
            }
        }
        Ok(())
    }

    pub fn reconcile(&mut self) -> Result<ReconciliationResult> {
        let local = self.state.snapshot()?;
        let broker = self.gateway.account_snapshot();
        let mut differences = Vec::new();
        if local.cash != broker.cash {
            differences.push("cash_or_frozen_cash".to_owned());
        }
        if local.position != broker.position {
            differences.push("position_or_frozen_stock".to_owned());
        }
        if local.open_order_ids != broker.open_order_ids {
            differences.push("unfinished_orders".to_owned());
        }
        if local.cumulative_filled_quantity != broker.cumulative_filled_quantity {
            differences.push("cumulative_fills".to_owned());
        }
        let result = ReconciliationResult {
            matched: differences.is_empty(),
            differences,
            checked_at: chrono::Utc::now().naive_utc(),
            broker_source: "paper-broker-sqlite-v1".to_owned(),
            broker_snapshot_sha256: hex::encode(sha2::Sha256::digest(serde_json::to_vec(&broker)?)),
            checked_sequence: self.last_sequence,
        };
        let event = self.event(
            EventType::ReconciliationCompleted,
            result.checked_at,
            "reconcile",
            format!("reconcile:{}:{}", self.state.run_id, self.last_sequence),
            serde_json::to_value(&result)?,
        );
        self.record(event)?;
        Ok(result)
    }

    pub fn cancel_order(&mut self, order_id: &str, reason: &str) -> Result<()> {
        if reason.trim().is_empty() {
            return Err(anyhow!("cancellation reason is required"));
        }
        let order = self
            .state
            .orders
            .get(order_id)
            .with_context(|| format!("order {order_id} is missing"))?
            .clone();
        if !matches!(
            order.status,
            OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        ) {
            return Err(anyhow!(
                "order is not cancellable in status {:?}",
                order.status
            ));
        }
        let correlation = format!("cancel:{order_id}");
        let requested_at = chrono::Utc::now().naive_utc();
        let requested = self.event(
            EventType::OrderCancelRequested,
            requested_at,
            &correlation,
            format!("cancel-request:{order_id}"),
            json!({"order_id": order_id, "reason": reason}),
        );
        self.record(requested)?;
        self.maybe_inject_fault(WorkflowFaultPoint::CancelRequested)?;
        let report = self.gateway.cancel(&order.intent, reason)?;
        self.apply_execution_report(&order.intent, requested_at, &correlation, report)
    }

    pub fn resume_after_reconciliation(&mut self, reason: &str) -> Result<()> {
        if reason.trim().is_empty() {
            anyhow::bail!("operator resume reason is required")
        }
        if !matches!(self.state.mode, ServiceMode::Safe | ServiceMode::ReadOnly) {
            anyhow::bail!("only SAFE or READ_ONLY runs can be resumed")
        }
        if !self
            .state
            .last_reconciliation
            .as_ref()
            .is_some_and(|result| result.matched)
        {
            anyhow::bail!("a clean independent reconciliation is required before resume")
        }
        let reconciliation_sequence = self
            .state
            .last_reconciliation_event_sequence
            .context("clean reconciliation sequence is unavailable")?;
        if reconciliation_sequence != self.last_sequence {
            let later = self
                .store
                .load_after(&self.state.run_id, reconciliation_sequence)?;
            if later.iter().any(|event| {
                !matches!(
                    event.event_type,
                    EventType::RecoveryCompleted | EventType::ServiceStopped
                )
            }) {
                anyhow::bail!("business state changed after the last clean reconciliation")
            }
        }
        let reconciliation = self
            .state
            .last_reconciliation
            .as_ref()
            .expect("checked above")
            .clone();
        let broker = self.gateway.account_snapshot();
        let current_broker_hash = hex::encode(sha2::Sha256::digest(serde_json::to_vec(&broker)?));
        if reconciliation.broker_source != "paper-broker-sqlite-v1"
            || reconciliation.broker_snapshot_sha256 != current_broker_hash
            || broker != self.state.snapshot()?
        {
            let event = self.event(
                EventType::ErrorRecorded,
                chrono::Utc::now().naive_utc(),
                "operator-resume",
                format!(
                    "operator-resume-rejected:{}:{}",
                    self.state.run_id, self.last_sequence
                ),
                json!({
                    "component": "reconciliation",
                    "reason_code": "BROKER_CHANGED_AFTER_CLEAN_RECONCILIATION",
                    "expected_broker_snapshot_sha256": reconciliation.broker_snapshot_sha256,
                    "actual_broker_snapshot_sha256": current_broker_hash,
                    "entered_safe": true
                }),
            );
            self.record(event)?;
            anyhow::bail!("broker changed after the last clean reconciliation")
        }
        if self.state.orders.values().any(|order| {
            !matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Cancelled
            )
        }) {
            anyhow::bail!("unfinished orders prevent operator resume")
        }
        let event = self.event(
            EventType::ServiceModeChanged,
            chrono::Utc::now().naive_utc(),
            "operator-resume",
            format!(
                "operator-resume:{}:{}",
                self.state.run_id, self.last_sequence
            ),
            json!({
                "mode": ServiceMode::Running,
                "reason": reason,
                "previous_mode": self.state.mode
            }),
        );
        self.record(event)?;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let event = self.event(
            EventType::ServiceStopped,
            chrono::Utc::now().naive_utc(),
            "stop",
            format!("stop:{}:{}", self.state.run_id, self.last_sequence),
            json!({"last_sequence": self.last_sequence}),
        );
        self.record(event)?;
        self.save_snapshot()
    }

    /// Installs a one-shot deterministic crash point used by recovery certification tests.
    pub fn inject_fault_once(&mut self, point: WorkflowFaultPoint) {
        self.injected_fault = Some(point);
    }

    fn maybe_inject_fault(&mut self, point: WorkflowFaultPoint) -> Result<()> {
        if self.injected_fault == Some(point) {
            self.injected_fault = None;
            anyhow::bail!("injected workflow fault at {point:?}")
        }
        Ok(())
    }

    pub fn save_snapshot(&mut self) -> Result<()> {
        LedgerWriter::new(&mut self.store, &mut self.state, &mut self.last_sequence).save_snapshot()
    }

    fn record(&mut self, event: EventEnvelope) -> Result<AppendOutcome> {
        LedgerWriter::new(&mut self.store, &mut self.state, &mut self.last_sequence).append(event)
    }

    fn record_batch(&mut self, events: &mut [EventEnvelope]) -> Result<()> {
        LedgerWriter::new(&mut self.store, &mut self.state, &mut self.last_sequence)
            .append_batch(events)
    }

    fn event(
        &self,
        event_type: EventType,
        time: NaiveDateTime,
        correlation: &str,
        key: String,
        payload: Value,
    ) -> EventEnvelope {
        make_event(
            &self.config,
            &self.state,
            event_type,
            time,
            correlation,
            key,
            payload,
        )
    }

    fn record_touch_and_skip(&mut self, index: i32, bar: &MarketBar, reason: &str) -> Result<()> {
        let correlation = format!(
            "touch-skip:{}:{}:{index}",
            self.state.cycle_id, bar.timestamp
        );
        let mut events = vec![self.event(
            EventType::GridLevelTouched,
            bar.timestamp,
            &correlation,
            format!(
                "grid-touch:{}:{}:{index}",
                self.state.cycle_id, bar.timestamp
            ),
            json!({
                "grid_index": index,
                "price": GridSpec::from(&self.config).price(index)?.to_string()
            }),
        )];
        events.push(self.event(
            EventType::GridLevelSkipped,
            bar.timestamp,
            &correlation,
            format!(
                "grid-skip:{}:{}:{index}",
                self.state.cycle_id, bar.timestamp
            ),
            json!({"grid_index": index, "reason": reason}),
        ));
        self.record_batch(&mut events)
    }

    fn rearm_levels(&mut self, bar: &MarketBar) -> Result<()> {
        let grid = GridSpec::from(&self.config);
        let mut rearmed = Vec::new();
        for (index, level) in &self.state.levels {
            let adjacent = if *index > 0 {
                grid.price(*index - 1)?
            } else {
                grid.price(*index + 1)?
            };
            let mut candidate = level.clone();
            if maybe_rearm(
                &mut candidate,
                bar.close,
                level
                    .price
                    .checked_sub(adjacent)
                    .context("grid width exceeds numeric range")?,
                self.config.hysteresis_ratio,
            )? {
                rearmed.push(*index);
            }
        }
        let mut events: Vec<_> = rearmed
            .into_iter()
            .map(|index| {
                self.event(
                    EventType::GridLevelRearmed,
                    bar.timestamp,
                    "rearm",
                    format!("rearm:{}:{}:{index}", self.state.cycle_id, bar.timestamp),
                    json!({"grid_index": index}),
                )
            })
            .collect();
        if !events.is_empty() {
            self.record_batch(&mut events)?;
        }
        Ok(())
    }

    fn record_bar_processed(&mut self, bar: &MarketBar) -> Result<()> {
        let event = self.event(
            EventType::MarketBarProcessed,
            bar.timestamp,
            &format!("market:{}:{}", bar.symbol, bar.timestamp),
            format!("bar-processed:{}:{}", bar.symbol, bar.timestamp),
            json!({
                "symbol": bar.symbol,
                "timestamp": bar.timestamp,
                "close": bar.close.to_string()
            }),
        );
        self.record(event)?;
        Ok(())
    }

    fn record_bar_decisions_committed(&mut self, bar: &MarketBar) -> Result<()> {
        let event = self.event(
            EventType::MarketBarDecisionsCommitted,
            bar.timestamp,
            &format!("market:{}:{}", bar.symbol, bar.timestamp),
            format!("bar-decisions:{}:{}", bar.symbol, bar.timestamp),
            json!({"symbol": bar.symbol, "timestamp": bar.timestamp}),
        );
        self.record(event)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn tranche_allocations(
        &self,
        right_id: &str,
        planned_right: Option<&GridRight>,
        direction: Direction,
        requested_budget: Decimal,
        requested_quantity: i64,
        target_lot_ids: &[String],
        target_lot_allocations: &[LotQuantityAllocation],
        from_reserved: bool,
    ) -> Result<Vec<Value>> {
        let planned_ids = planned_right
            .map(|right| right.capacity.tranche_ids.as_slice())
            .unwrap_or_default();
        let mut tranches: Vec<TrancheAllocationSource> = self
            .state
            .right_tranches
            .values()
            .filter(|tranche| {
                (tranche.owner_right_id == right_id || planned_ids.contains(&tranche.tranche_id))
                    && tranche.direction == direction
                    && (direction == Direction::Buy
                        || target_lot_ids.is_empty()
                        || tranche
                            .lot_id
                            .as_ref()
                            .is_some_and(|lot_id| target_lot_ids.contains(lot_id)))
            })
            .map(|tranche| {
                (
                    tranche.tranche_id.clone(),
                    tranche.birth_grid_index,
                    tranche.lot_id.clone(),
                    tranche.available_budget,
                    tranche.reserved_budget,
                    tranche.available_quantity,
                    tranche.reserved_quantity,
                )
            })
            .collect();
        if let Some(right) = planned_right {
            for tranche_id in &right.capacity.tranche_ids {
                if tranches.iter().any(|tranche| tranche.0 == *tranche_id) {
                    continue;
                }
                if direction == Direction::Buy {
                    if self.config.uses_fixed_quantity() {
                        tranches.push((
                            tranche_id.clone(),
                            right.grid_index,
                            None,
                            Decimal::ZERO,
                            Decimal::ZERO,
                            self.config.standard_quantity,
                            0,
                        ));
                    } else {
                        tranches.push((
                            tranche_id.clone(),
                            right.grid_index,
                            None,
                            self.config.standard_budget,
                            Decimal::ZERO,
                            0,
                            0,
                        ));
                    }
                } else if let Some(lot) = self.state.lots.values().find(|lot| {
                    RightTranche::id_for(
                        &self.state.cycle_id,
                        &right.right_id,
                        Direction::Sell,
                        right.grid_index,
                        Some(&lot.lot_id),
                    ) == *tranche_id
                }) {
                    if !target_lot_ids.is_empty() && !target_lot_ids.contains(&lot.lot_id) {
                        continue;
                    }
                    tranches.push((
                        tranche_id.clone(),
                        right.grid_index,
                        Some(lot.lot_id.clone()),
                        Decimal::ZERO,
                        Decimal::ZERO,
                        lot.remaining_quantity,
                        0,
                    ));
                }
            }
        }
        if tranches.is_empty() {
            return Ok(Vec::new());
        }
        let release_all = requested_budget == Decimal::MAX || requested_quantity == i64::MAX;
        if direction == Direction::Sell && !release_all && !target_lot_allocations.is_empty() {
            let mut allocations = Vec::with_capacity(target_lot_allocations.len());
            for target in target_lot_allocations {
                let tranche = tranches
                    .iter()
                    .find(|tranche| {
                        tranche.0 == target.tranche_id
                            && tranche.2.as_deref() == Some(target.lot_id.as_str())
                    })
                    .with_context(|| {
                        format!(
                            "explicit sell allocation has no matching tranche: lot={} tranche={}",
                            target.lot_id, target.tranche_id
                        )
                    })?;
                let source = if from_reserved { tranche.6 } else { tranche.5 };
                if target.quantity <= 0 || target.quantity > source {
                    return Err(anyhow!("explicit sell allocation exceeds tranche balance"));
                }
                allocations.push(json!({
                    "tranche_id": tranche.0,
                    "budget": "0",
                    "quantity": target.quantity
                }));
            }
            if target_lot_allocations
                .iter()
                .map(|allocation| allocation.quantity)
                .sum::<i64>()
                != requested_quantity
            {
                return Err(anyhow!(
                    "explicit sell allocation does not equal requested quantity"
                ));
            }
            return Ok(allocations);
        }
        tranches.sort_by_key(|tranche| (std::cmp::Reverse(tranche.1.abs()), tranche.0.clone()));
        let mut budget_remaining = if release_all {
            tranches
                .iter()
                .map(|tranche| if from_reserved { tranche.4 } else { tranche.3 })
                .sum()
        } else {
            requested_budget
        };
        let mut quantity_remaining = if release_all {
            tranches
                .iter()
                .map(|tranche| if from_reserved { tranche.6 } else { tranche.5 })
                .sum()
        } else {
            requested_quantity
        };
        let mut allocations = Vec::new();
        for tranche in tranches {
            let source_budget = if from_reserved { tranche.4 } else { tranche.3 };
            let source_quantity = if from_reserved { tranche.6 } else { tranche.5 };
            let budget = source_budget.min(budget_remaining);
            let quantity = source_quantity.min(quantity_remaining);
            if budget > Decimal::ZERO || quantity > 0 {
                allocations.push(json!({
                    "tranche_id": tranche.0,
                    "budget": budget.to_string(),
                    "quantity": quantity
                }));
                budget_remaining -= budget;
                quantity_remaining -= quantity;
            }
        }
        if budget_remaining != Decimal::ZERO || quantity_remaining != 0 {
            let inventory: Vec<_> = self
                .state
                .right_tranches
                .values()
                .map(|tranche| {
                    format!(
                        "{} owner={} lot={:?} aq={} rq={}",
                        tranche.tranche_id,
                        tranche.owner_right_id,
                        tranche.lot_id,
                        tranche.available_quantity,
                        tranche.reserved_quantity
                    )
                })
                .collect();
            return Err(anyhow!(
                "tranche inventory cannot satisfy requested allocation: right={right_id} direction={direction:?} from_reserved={from_reserved} target_lots={target_lot_ids:?} budget_remaining={budget_remaining} quantity_remaining={quantity_remaining} inventory={inventory:?}"
            ));
        }
        Ok(allocations)
    }

    fn push_deferred_events(
        &self,
        events: &mut Vec<EventEnvelope>,
        right: &GridRight,
        request: &DecisionRequest,
        response: &crate::decision::DecisionResponse,
        bar: &MarketBar,
        reason: &str,
    ) {
        let correlation = format!(
            "touch:{}:{}:{}",
            self.state.cycle_id, bar.timestamp, right.grid_index
        );
        events.push(self.event(
            EventType::GridRightDeferred,
            bar.timestamp,
            &correlation,
            format!("right-deferred:{}", right.right_id),
            json!({
                "right_id": right.right_id,
                "decision_id": response.request_id,
                "reason": reason,
                "gross_available_quantity": request.context.gross_available_quantity,
                "platform_residual_quantity": request.context.platform_residual_quantity,
                "algorithm_authorized_quantity": request.context.algorithm_authorized_quantity,
                "platform_blocked_quantity": 0,
                "order_intent_quantity": 0,
                "remaining_decision_quantity": response.outcome.quantities().1,
                "remaining_budget": right.capacity.available_budget.to_string(),
                "remaining_quantity": if right.direction == Direction::Buy {
                    right.capacity.available_quantity
                } else {
                    right.capacity.eligible_quantity
                }
            }),
        ));
    }

    fn push_residual_event(
        &self,
        events: &mut Vec<EventEnvelope>,
        right: &GridRight,
        request: &DecisionRequest,
        response: &crate::decision::DecisionResponse,
        bar: &MarketBar,
    ) {
        let correlation = format!(
            "touch:{}:{}:{}",
            self.state.cycle_id, bar.timestamp, right.grid_index
        );
        events.push(self.event(
            EventType::GridRightResidualHeld,
            bar.timestamp,
            &correlation,
            format!("right-residual:{}", right.right_id),
            json!({
                "right_id": right.right_id,
                "decision_id": response.request_id,
                "reason": "PLATFORM_RESIDUAL_BELOW_STANDARD_QUANTITY",
                "gross_available_quantity": request.context.gross_available_quantity,
                "platform_residual_quantity": request.context.platform_residual_quantity,
                "algorithm_authorized_quantity": 0,
                "platform_blocked_quantity": 0,
                "order_intent_quantity": 0,
                "remaining_decision_quantity": 0,
                "remaining_budget": right.capacity.available_budget.to_string(),
                "remaining_quantity": request.context.gross_available_quantity
            }),
        ));
    }

    fn push_blocked_events(
        &self,
        events: &mut Vec<EventEnvelope>,
        right: &GridRight,
        request: &DecisionRequest,
        response: &crate::decision::DecisionResponse,
        bar: &MarketBar,
        reason: &str,
    ) {
        let correlation = format!(
            "touch:{}:{}:{}",
            self.state.cycle_id, bar.timestamp, right.grid_index
        );
        let (exercise_quantity, defer_quantity) = response.outcome.quantities();
        events.push(self.event(
            EventType::GridRightBlocked,
            bar.timestamp,
            &correlation,
            format!("right-blocked:{}", right.right_id),
            json!({
                "right_id": right.right_id,
                "decision_id": response.request_id,
                "reason": reason,
                "gross_available_quantity": request.context.gross_available_quantity,
                "platform_residual_quantity": request.context.platform_residual_quantity,
                "algorithm_authorized_quantity": request.context.algorithm_authorized_quantity,
                "platform_blocked_quantity": exercise_quantity,
                "order_intent_quantity": 0,
                "remaining_decision_quantity": defer_quantity + exercise_quantity,
                "remaining_budget": right.capacity.available_budget.to_string(),
                "remaining_quantity": if right.direction == Direction::Buy {
                    right.capacity.available_quantity
                } else {
                    right.capacity.eligible_quantity
                }
            }),
        ));
    }

    fn gate_context(&self, right: &GridRight) -> GateContext {
        let recent_return = self
            .recent_closes
            .first()
            .filter(|price| !price.is_zero())
            .zip(self.recent_closes.last())
            .map(|(first, last)| (*last - *first) / *first)
            .unwrap_or(Decimal::ZERO);
        let volatility = self
            .recent_closes
            .iter()
            .min()
            .zip(self.recent_closes.iter().max())
            .zip(self.recent_closes.first().filter(|price| !price.is_zero()))
            .map(|((low, high), first)| (*high - *low) / *first)
            .unwrap_or(Decimal::ZERO);
        let gross_available_quantity =
            if right.direction == Direction::Buy && right.capacity.available_quantity > 0 {
                right.capacity.available_quantity
            } else {
                right.capacity.eligible_quantity
            };
        let (algorithm_authorized_quantity, platform_residual_quantity) =
            whole_unit_partition(gross_available_quantity, self.config.standard_quantity)
                .expect("validated configuration has a positive standard quantity");
        GateContext {
            right_id: right.right_id.clone(),
            symbol: self.state.symbol.clone(),
            direction: right.direction,
            grid_index: right.grid_index,
            grid_price: right.grid_price,
            current_price: right.grid_price,
            current_time: right.granted_at,
            available_budget: right.capacity.available_budget,
            available_quantity: algorithm_authorized_quantity,
            gross_available_quantity,
            platform_residual_quantity,
            algorithm_authorized_quantity,
            lot_size: self.config.lot_size,
            standard_quantity: self.config.standard_quantity,
            eligible_lot_ids: right.capacity.eligible_lot_ids.clone(),
            accumulated_grid_indices: right.capacity.accumulated_grid_indices.clone(),
            deployed_budget: self.state.deployed_grid_budget,
            current_position: self.state.position.total,
            sellable_position: self.state.position.sellable,
            recent_return,
            vwap_deviation: Decimal::ZERO,
            volatility,
            market_regime: if recent_return < Decimal::ZERO {
                "DOWN"
            } else {
                "UP"
            }
            .to_owned(),
            cycle_id: self.state.cycle_id.clone(),
        }
    }

    fn order_for_intent(&self, intent: &OrderIntent) -> Result<Order> {
        let order_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("paper:order:{}:0", intent.intent_id).as_bytes(),
        )
        .to_string();
        Ok(Order {
            order_id,
            intent: intent.clone(),
            status: OrderStatus::Created,
            filled_quantity: 0,
            filled_notional: Decimal::ZERO,
            commission_charged: Decimal::ZERO,
            reserved_cash_remaining: if intent.direction == Direction::Buy {
                self.buy_reservation(intent.limit_price, intent.quantity)?
            } else {
                Decimal::ZERO
            },
            reserved_quantity_remaining: if intent.direction == Direction::Sell {
                intent.quantity
            } else {
                0
            },
            cancel_requested_reason: None,
            cancel_requested_at: None,
        })
    }

    fn buy_reservation(&self, limit_price: Decimal, quantity: i64) -> Result<Decimal> {
        estimated_buy_reservation(&self.config, limit_price, quantity).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    fn make_intent(
        &self,
        right_id: &str,
        index: i32,
        direction: Direction,
        price: Decimal,
        quantity: i64,
        budget: Decimal,
        target_lot_allocations: Vec<LotQuantityAllocation>,
        time: NaiveDateTime,
    ) -> OrderIntent {
        let intent_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "{}:{}:{}:{}",
                self.state.run_id, self.state.cycle_id, index, time
            )
            .as_bytes(),
        )
        .to_string();
        let target_lot_ids = target_lot_allocations
            .iter()
            .map(|allocation| allocation.lot_id.clone())
            .collect();
        OrderIntent {
            intent_id,
            right_id: right_id.to_owned(),
            grid_index: index,
            direction,
            limit_price: price,
            quantity,
            budget,
            target_lot_ids,
            target_lot_allocations,
            profit_guard_version: if direction == Direction::Sell {
                PROFIT_GUARD_VERSION.to_owned()
            } else {
                String::new()
            },
            profit_guard_policy: Some(ProfitGuardPolicy::from(&self.config)),
            created_at: time,
        }
    }

    fn maybe_reset_cycle(&mut self, time: NaiveDateTime) -> Result<()> {
        let completed_sell_in_cycle = self.state.orders.values().any(|order| {
            order.status == OrderStatus::Filled
                && order.intent.direction == Direction::Sell
                && self
                    .state
                    .grid_rights
                    .get(&order.intent.right_id)
                    .is_some_and(|right| right.cycle_id == self.state.cycle_id)
        });
        if self
            .state
            .lots
            .values()
            .any(|lot| lot.remaining_quantity > 0)
            || !completed_sell_in_cycle
            || !self
                .state
                .grid_rights
                .values()
                .any(|right| right.cycle_id == self.state.cycle_id)
        {
            return Ok(());
        }
        let old_cycle = self.state.cycle_id.clone();
        let new_cycle = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:cycle-reset:{time}", self.state.run_id).as_bytes(),
        )
        .to_string();
        let mut terminal_events = Vec::new();
        terminal_events.extend(
            self.state
                .grid_rights
                .values()
                .filter(|right| {
                    right.cycle_id == old_cycle
                        && matches!(
                            right.status,
                            GridRightStatus::Residual
                                | GridRightStatus::Deferred
                                | GridRightStatus::Blocked
                                | GridRightStatus::Released
                                | GridRightStatus::PartiallyExercised
                        )
                })
                .map(|right| {
                    let owner_tranches: Vec<_> = self
                        .state
                        .right_tranches
                        .values()
                        .filter(|tranche| tranche.owner_right_id == right.right_id)
                        .collect();
                    let remaining_budget = if owner_tranches.is_empty() {
                        right.remaining_budget()
                    } else {
                        owner_tranches
                            .iter()
                            .map(|tranche| tranche.available_budget)
                            .sum()
                    };
                    let remaining_quantity = if owner_tranches.is_empty() {
                        right.remaining_quantity()
                    } else {
                        owner_tranches
                            .iter()
                            .map(|tranche| tranche.available_quantity)
                            .sum()
                    };
                    self.event(
                        EventType::GridRightExpired,
                        time,
                        "cycle-reset",
                        format!("right-expired:{}", right.right_id),
                        json!({
                            "right_id": right.right_id,
                            "reason": "GRID_CYCLE_COMPLETED",
                            "remaining_budget": remaining_budget.to_string(),
                            "remaining_quantity": remaining_quantity,
                            "t_plus_one_blocked_quantity": right.capacity.t_plus_one_blocked_quantity,
                            "risk_blocked_quantity": right.capacity.risk_blocked_quantity
                            ,"no_profit_blocked_quantity": right.capacity.no_profit_blocked_quantity
                        }),
                    )
                })
                .collect::<Vec<_>>(),
        );
        let mut tranche_expirations = std::collections::BTreeMap::<String, Vec<Value>>::new();
        for tranche in self.state.right_tranches.values().filter(|tranche| {
            tranche.cycle_id == old_cycle
                && (tranche.available_budget > Decimal::ZERO || tranche.available_quantity > 0)
        }) {
            tranche_expirations
                .entry(tranche.owner_right_id.clone())
                .or_default()
                .push(json!({
                    "tranche_id": tranche.tranche_id,
                    "budget": tranche.available_budget.to_string(),
                    "quantity": tranche.available_quantity
                }));
        }
        for (right_id, allocations) in tranche_expirations {
            terminal_events.push(self.event(
                EventType::RightBalanceExpired,
                time,
                "cycle-reset",
                format!("tranche-expired:{right_id}:{time}"),
                json!({
                    "right_id": right_id,
                    "reason": "GRID_CYCLE_COMPLETED",
                    "allocations": allocations
                }),
            ));
        }
        let reset = self.event(
            EventType::GridCycleReset,
            time,
            "cycle-reset",
            format!("cycle-reset:{old_cycle}:{time}"),
            json!({"cycle_id": new_cycle, "previous_cycle_id": old_cycle}),
        );
        terminal_events.push(reset);
        let mut armed: Vec<_> = GridSpec::from(&self.config)
            .levels()?
            .into_values()
            .map(|level| {
                let mut event = self.event(
                    EventType::GridLevelArmed,
                    time,
                    "cycle-reset",
                    format!("level-armed:{new_cycle}:{}", level.index),
                    serde_json::to_value(level).expect("grid level serializes"),
                );
                event.cycle_id = new_cycle.clone();
                event
            })
            .collect();
        terminal_events.append(&mut armed);
        self.record_batch(&mut terminal_events)
    }

    fn recover_missing_cycle_reset(&mut self) -> Result<()> {
        if self.state.mode != ServiceMode::Running
            || self
                .state
                .lots
                .values()
                .any(|lot| lot.remaining_quantity > 0)
            || !self
                .state
                .grid_rights
                .values()
                .any(|right| right.cycle_id == self.state.cycle_id)
        {
            return Ok(());
        }
        let reset_time = self
            .store
            .load_after(&self.state.run_id, 0)?
            .into_iter()
            .rev()
            .find(|event| {
                event.event_type == EventType::OrderFilled
                    && event.payload["fill"]["direction"] == "SELL"
            })
            .map(|event| event.event_time);
        if let Some(time) = reset_time {
            if self.store.has_idempotency_key(
                &self.state.run_id,
                &format!("bar-processed:{}:{time}", self.state.symbol),
            )? {
                self.maybe_reset_cycle(time)?;
            }
        }
        Ok(())
    }

    fn push_close(&mut self, close: Decimal) {
        self.recent_closes.push(close);
        if self.recent_closes.len() > 5 {
            self.recent_closes.remove(0);
        }
    }

    fn fill_lot_allocations(
        &self,
        intent: &OrderIntent,
        already_filled: i64,
        fill_quantity: i64,
    ) -> Result<Vec<LotQuantityAllocation>> {
        if intent.direction == Direction::Buy {
            return Ok(Vec::new());
        }
        let policy = intent
            .profit_guard_policy
            .as_ref()
            .context("sell intent lacks its audited profit-guard policy")?;
        canonical_fill_allocations(
            policy,
            &intent.target_lot_allocations,
            already_filled,
            fill_quantity,
            intent.limit_price,
        )
        .context("sell fill quantity or cost slice violates its explicit allocation")
    }
}

fn make_event(
    config: &Config,
    state: &StrategyState,
    event_type: EventType,
    time: NaiveDateTime,
    correlation: &str,
    key: String,
    payload: Value,
) -> EventEnvelope {
    EventEnvelope::new(
        event_type,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        time,
        correlation,
        None,
        key,
        payload,
        &config.config_version,
    )
}

fn config_snapshot_payload(config: &Config) -> Result<Value> {
    let mut payload = serde_json::to_value(config)?;
    payload
        .as_object_mut()
        .context("configuration must serialize as an object")?
        .insert(
            "_content_sha256".to_owned(),
            Value::String(config.content_sha256()?),
        );
    Ok(payload)
}

pub fn compare_states(left: &StrategyState, right: &StrategyState) -> Result<()> {
    let a = serde_json::to_value(left)?;
    let b = serde_json::to_value(right)?;
    if a != b {
        let differing_fields = a
            .as_object()
            .zip(b.as_object())
            .map(|(left, right)| {
                left.keys()
                    .chain(right.keys())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .filter(|key| left.get(*key) != right.get(*key))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let differing_lots = left
            .lots
            .keys()
            .chain(right.lots.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|lot_id| {
                left.lots
                    .get(*lot_id)
                    .zip(right.lots.get(*lot_id))
                    .is_none_or(|(left, right)| {
                        serde_json::to_value(left).ok() != serde_json::to_value(right).ok()
                    })
            })
            .take(5)
            .cloned()
            .collect::<Vec<_>>();
        return Err(anyhow!(
            "rebuilt state differs from current state in fields {differing_fields:?}; sample lots {differing_lots:?}"
        ));
    }
    Ok(())
}

fn validate_algorithm_manifest(
    algorithm: &dyn QuantDecisionAlgorithm,
) -> Result<crate::decision::AlgorithmManifest> {
    let manifest = algorithm.manifest();
    if manifest.algorithm_name.trim().is_empty()
        || manifest.algorithm_version.trim().is_empty()
        || !manifest
            .supported_contract_versions
            .contains(&crate::decision::DECISION_CONTRACT_VERSION)
        || !manifest.deterministic
        || manifest.artifact_sha256.len() != 64
        || manifest.environment_sha256.len() != 64
        || manifest.platform_sha256.len() != 64
        || manifest.supports_checkpoint
    {
        return Err(anyhow!(
            "algorithm manifest is invalid or not deterministic for replay"
        ));
    }
    Ok(manifest)
}
