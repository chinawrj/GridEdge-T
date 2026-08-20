//! Append-only ledger boundary.
//!
//! This module is the only application-facing writer for domain events. It
//! validates a complete projection before invoking the journal CAS, then
//! applies exactly the events that the journal accepted.

use crate::{
    data::MarketBar,
    decision::{DecisionRequest, DecisionResponse, GridRight, DECISION_CONTRACT_VERSION},
    domain::{
        Direction, GridLevelState, GridLevelStatus, InitialDeploymentEvaluation,
        InitialDeploymentOutcome, Order, OrderStatus, StrategyState,
    },
    event::{current_event_schema, EventEnvelope, EventType},
    journal::{AppendOutcome, SqliteStore},
    risk::{canonical_approval, canonical_approved_quantity},
    service::validate_funds_inventory_evidence,
};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;

pub struct LedgerWriter<'a> {
    store: &'a mut SqliteStore,
    state: &'a mut StrategyState,
    last_sequence: &'a mut i64,
}

impl<'a> LedgerWriter<'a> {
    pub fn new(
        store: &'a mut SqliteStore,
        state: &'a mut StrategyState,
        last_sequence: &'a mut i64,
    ) -> Self {
        Self {
            store,
            state,
            last_sequence,
        }
    }

    pub fn append(&mut self, event: EventEnvelope) -> Result<AppendOutcome> {
        self.append_inner(event, false)
    }

    pub(crate) fn append_platform_upgrade(
        &mut self,
        event: EventEnvelope,
    ) -> Result<AppendOutcome> {
        if !matches!(
            event.event_type,
            EventType::PlatformUpgradeAuthorized | EventType::PlatformUpgradeActivated
        ) {
            return Err(anyhow!(
                "platform identity writer received a business event"
            ));
        }
        self.append_inner(event, true)
    }

    pub(crate) fn append_ongoing_resource_policy(
        &mut self,
        event: EventEnvelope,
    ) -> Result<AppendOutcome> {
        if event.event_type != EventType::OngoingResourcePolicyActivated {
            return Err(anyhow!(
                "ongoing resource policy writer received an unrelated event"
            ));
        }
        self.append_inner(event, true)
    }

    fn append_inner(
        &mut self,
        mut event: EventEnvelope,
        offline_policy_boundary: bool,
    ) -> Result<AppendOutcome> {
        if matches!(
            event.event_type,
            EventType::PlatformUpgradeAuthorized | EventType::PlatformUpgradeActivated
        ) && !offline_policy_boundary
        {
            return Err(anyhow!(
                "platform identity events require the dedicated offline upgrade boundary"
            ));
        }
        if event.event_type == EventType::OngoingResourcePolicyActivated && !offline_policy_boundary
        {
            return Err(anyhow!(
                "ongoing resource policy requires its dedicated offline boundary"
            ));
        }
        if event.run_id != self.state.run_id {
            return Err(anyhow!(
                "event run {} does not match ledger run {}",
                event.run_id,
                self.state.run_id
            ));
        }
        if event.symbol != self.state.symbol {
            return Err(anyhow!(
                "event symbol {} does not match ledger symbol {}",
                event.symbol,
                self.state.symbol
            ));
        }
        let exists = self
            .store
            .has_idempotency_key(&self.state.run_id, &event.idempotency_key)?;
        if !exists && event.schema_version != current_event_schema(event.event_type) {
            return Err(anyhow!(
                "new event {} must use current schema {}, got {}",
                event.event_type,
                current_event_schema(event.event_type),
                event.schema_version
            ));
        }
        if !exists {
            self.validate_current_opportunity_binding(&event)?;
        }
        if !exists && event.event_type == EventType::MarketDataReceived {
            let current: MarketBar = serde_json::from_value(event.payload.clone())?;
            if let Some(previous) = self
                .store
                .recent_payloads_by_type(&self.state.run_id, EventType::MarketDataReceived, 1)?
                .pop()
            {
                let previous: MarketBar = serde_json::from_value(previous)?;
                if previous.timestamp >= current.timestamp {
                    return Err(anyhow!(
                        "new market data must be strictly later than the prior received bar"
                    ));
                }
            }
        }
        if !exists
            && matches!(
                event.event_type,
                EventType::GridRightGranted
                    | EventType::GridLevelTouched
                    | EventType::GridLevelSkipped
                    | EventType::GridCycleStarted
                    | EventType::GridCycleReset
                    | EventType::GridLevelArmed
            )
        {
            return Err(anyhow!(
                "current grid opportunities must be written as one atomic touch-resolution batch"
            ));
        }
        if !exists
            && (event.event_type == EventType::InitialDeploymentEvaluated
                || (event.event_type == EventType::OrderIntentCreated
                    && serde_json::from_value::<Order>(event.payload.clone())
                        .is_ok_and(|order| order.intent.origin.is_initial_deployment())))
        {
            return Err(anyhow!(
                "initial deployment evaluation and intent require one atomic batch"
            ));
        }
        if !exists && event.event_type == EventType::GridLevelRearmed {
            let latest_market = self
                .store
                .recent_payloads_by_type(&self.state.run_id, EventType::MarketDataReceived, 1)?
                .pop()
                .map(serde_json::from_value::<MarketBar>)
                .transpose()?;
            self.validate_current_rearm_evidence(self.state, &event, latest_market.as_ref())?;
        }
        if !exists
            && matches!(
                event.event_type,
                EventType::MarketBarDecisionsCommitted | EventType::MarketBarProcessed
            )
        {
            self.validate_current_market_stage(self.state, &event)?;
        }
        if !exists && event.event_type == EventType::GridCycleReset {
            self.validate_current_cycle_reset_evidence(self.state, &event)?;
        }
        if !exists {
            event.causation_id = self
                .store
                .latest_event_id_for_correlation(&event.run_id, &event.correlation_id)?;
        }
        if !exists {
            let mut projected = self.state.clone();
            self.validate_current_platform_approval(&projected, &event)?;
            projected.apply_event(&event)?;
            projected.validate_invariants()?;
        }
        let outcome = self
            .store
            .append_expected(&mut event, *self.last_sequence)?;
        match outcome {
            AppendOutcome::Appended(sequence) => {
                self.state.apply_event(&event)?;
                *self.last_sequence = sequence;
            }
            AppendOutcome::Duplicate => self.state.duplicate_events += 1,
        }
        Ok(outcome)
    }

    pub fn append_batch(&mut self, events: &mut [EventEnvelope]) -> Result<()> {
        if events.is_empty() {
            return Err(anyhow!("cannot append an empty event batch"));
        }
        if events.iter().any(|event| {
            matches!(
                event.event_type,
                EventType::PlatformUpgradeAuthorized
                    | EventType::PlatformUpgradeActivated
                    | EventType::OngoingResourcePolicyActivated
            )
        }) {
            return Err(anyhow!(
                "platform identity events cannot enter a business event batch"
            ));
        }
        if events.iter().any(|event| event.run_id != self.state.run_id) {
            return Err(anyhow!(
                "event batch contains a run other than ledger run {}",
                self.state.run_id
            ));
        }
        if events.iter().any(|event| event.symbol != self.state.symbol) {
            return Err(anyhow!(
                "event batch contains a symbol other than ledger symbol {}",
                self.state.symbol
            ));
        }
        let mut causes = HashMap::<String, Option<String>>::new();
        let mut existing = Vec::with_capacity(events.len());
        for event in events.iter_mut() {
            let exists = self
                .store
                .has_idempotency_key(&self.state.run_id, &event.idempotency_key)?;
            if !exists && event.schema_version != current_event_schema(event.event_type) {
                return Err(anyhow!(
                    "new event {} must use current schema {}, got {}",
                    event.event_type,
                    current_event_schema(event.event_type),
                    event.schema_version
                ));
            }
            if !exists {
                self.validate_current_opportunity_binding(event)?;
            }
            existing.push(exists);
            if exists {
                continue;
            }
            let prior = if let Some(prior) = causes.get(&event.correlation_id) {
                prior.clone()
            } else {
                self.store
                    .latest_event_id_for_correlation(&event.run_id, &event.correlation_id)?
            };
            event.causation_id = prior;
            causes.insert(event.correlation_id.clone(), Some(event.event_id.clone()));
        }
        for (evaluation_index, (event, exists)) in events.iter().zip(&existing).enumerate() {
            if *exists || event.event_type != EventType::InitialDeploymentEvaluated {
                continue;
            }
            let evaluation: InitialDeploymentEvaluation =
                serde_json::from_value(event.payload.clone())?;
            let matching_intents = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(intent_index, (candidate, candidate_exists))| {
                    if **candidate_exists
                        || *intent_index <= evaluation_index
                        || candidate.event_type != EventType::OrderIntentCreated
                        || candidate.correlation_id != event.correlation_id
                        || candidate.event_time != event.event_time
                    {
                        return false;
                    }
                    serde_json::from_value::<Order>(candidate.payload.clone()).is_ok_and(|order| {
                        match order.intent.origin {
                            crate::domain::OrderIntentOrigin::InitialDeploymentV1 {
                                deployment_id,
                                ..
                            } => deployment_id == evaluation.deployment_id,
                            crate::domain::OrderIntentOrigin::GridRight => false,
                        }
                    })
                })
                .count();
            let expected = usize::from(evaluation.outcome == InitialDeploymentOutcome::Authorized);
            if matching_intents != expected {
                return Err(anyhow!(
                    "initial deployment evaluation does not have its exact atomic intent outcome"
                ));
            }
        }
        for (intent_index, (event, exists)) in events.iter().zip(&existing).enumerate() {
            if *exists || event.event_type != EventType::OrderIntentCreated {
                continue;
            }
            let order: Order = serde_json::from_value(event.payload.clone())?;
            let crate::domain::OrderIntentOrigin::InitialDeploymentV1 { deployment_id, .. } =
                &order.intent.origin
            else {
                continue;
            };
            let matching_evaluations = events[..intent_index]
                .iter()
                .zip(&existing[..intent_index])
                .filter(|(candidate, candidate_exists)| {
                    !**candidate_exists
                        && candidate.event_type == EventType::InitialDeploymentEvaluated
                        && candidate.correlation_id == event.correlation_id
                        && candidate.event_time == event.event_time
                        && serde_json::from_value::<InitialDeploymentEvaluation>(
                            candidate.payload.clone(),
                        )
                        .is_ok_and(|evaluation| {
                            evaluation.deployment_id == *deployment_id
                                && evaluation.outcome == InitialDeploymentOutcome::Authorized
                        })
                })
                .count();
            if matching_evaluations != 1 {
                return Err(anyhow!(
                    "initial deployment intent lacks one prior atomic evaluation"
                ));
            }
        }
        let mut previous_market = self
            .store
            .recent_payloads_by_type(&self.state.run_id, EventType::MarketDataReceived, 1)?
            .pop()
            .map(serde_json::from_value::<MarketBar>)
            .transpose()?;
        for (event, exists) in events.iter().zip(&existing) {
            if *exists || event.event_type != EventType::MarketDataReceived {
                continue;
            }
            let current: MarketBar = serde_json::from_value(event.payload.clone())?;
            if previous_market
                .as_ref()
                .is_some_and(|previous| previous.timestamp >= current.timestamp)
            {
                return Err(anyhow!(
                    "new market data must be strictly later than the prior received bar"
                ));
            }
            previous_market = Some(current);
        }
        if events.iter().zip(&existing).any(|(event, exists)| {
            !*exists
                && matches!(
                    event.event_type,
                    EventType::MarketBarDecisionsCommitted | EventType::MarketBarProcessed
                )
        }) {
            return Err(anyhow!(
                "durable market stages must be committed after their prior workflow batch"
            ));
        }
        for (decision_index, (decision, decision_exists)) in
            events.iter().zip(&existing).enumerate()
        {
            if *decision_exists
                || decision.event_type != EventType::GateDecisionMade
                || decision.schema_version < 4
            {
                continue;
            }
            let request: DecisionRequest = serde_json::from_value(
                decision
                    .payload
                    .get("request")
                    .cloned()
                    .context("gate decision lacks request")?,
            )?;
            if request.contract_version != DECISION_CONTRACT_VERSION {
                continue;
            }
            let algorithm_succeeded = decision
                .payload
                .get("algorithm_succeeded")
                .and_then(serde_json::Value::as_bool)
                .context("v4 gate decision lacks exact algorithm success status")?;
            if algorithm_succeeded {
                continue;
            }
            let response: DecisionResponse = serde_json::from_value(
                decision
                    .payload
                    .get("response")
                    .cloned()
                    .context("v4 gate decision lacks response")?,
            )?;
            let failure_reason = response
                .decision
                .reason_codes
                .iter()
                .find(|reason| reason.as_str() != "RESOURCE_FAIL_CLOSED")
                .context("v4 fail-closed decision lacks its failure reason")?;
            let matching_errors = events[..decision_index]
                .iter()
                .zip(&existing[..decision_index])
                .filter(|(candidate, candidate_exists)| {
                    !**candidate_exists
                        && candidate.event_type == EventType::ErrorRecorded
                        && candidate.run_id == decision.run_id
                        && candidate.cycle_id == decision.cycle_id
                        && candidate.symbol == decision.symbol
                        && candidate.event_time == decision.event_time
                        && candidate.correlation_id == decision.correlation_id
                        && candidate
                            .payload
                            .get("component")
                            .and_then(serde_json::Value::as_str)
                            == Some("gate")
                        && candidate
                            .payload
                            .get("reason_code")
                            .and_then(serde_json::Value::as_str)
                            == Some(failure_reason.as_str())
                })
                .count();
            if matching_errors != 1 {
                return Err(anyhow!(
                    "v4 fail-closed decision requires exactly one matching prior gate error"
                ));
            }
        }
        for (touch, touch_exists) in events.iter().zip(&existing) {
            if *touch_exists || touch.event_type != EventType::GridLevelTouched {
                continue;
            }
            let recent = self.store.recent_payloads_by_type(
                &self.state.run_id,
                EventType::MarketDataReceived,
                2,
            )?;
            if recent.len() != 2 {
                return Err(anyhow!(
                    "current grid touch requires a prior close and its received market bar"
                ));
            }
            let previous: MarketBar = serde_json::from_value(recent[0].clone())?;
            let current: MarketBar = serde_json::from_value(recent[1].clone())?;
            let grid_index = touch
                .payload
                .get("grid_index")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| anyhow!("grid touch lacks a valid grid index"))?;
            if self
                .store
                .market_payload_at(&touch.run_id, &touch.symbol, touch.event_time)?
                .is_none()
                || self.store.has_market_stage(
                    &touch.run_id,
                    EventType::MarketBarDecisionsCommitted,
                    &touch.symbol,
                    touch.event_time,
                )?
                || self.store.has_market_stage(
                    &touch.run_id,
                    EventType::MarketBarProcessed,
                    &touch.symbol,
                    touch.event_time,
                )?
            {
                return Err(anyhow!(
                    "grid touch is outside the durable pre-decision market phase"
                ));
            }
            if self.store.has_grid_touch(
                &touch.run_id,
                &touch.symbol,
                touch.event_time,
                grid_index,
            )? {
                return Err(anyhow!("grid opportunity already has a recorded touch"));
            }
            if self
                .store
                .latest_grid_rearm_time(&touch.run_id, &touch.symbol, grid_index)?
                .is_some_and(|rearmed_at| touch.event_time <= rearmed_at)
                || events
                    .iter()
                    .zip(&existing)
                    .any(|(candidate, candidate_exists)| {
                        !*candidate_exists
                            && candidate.event_type == EventType::GridLevelRearmed
                            && candidate.symbol == touch.symbol
                            && candidate.event_time >= touch.event_time
                            && candidate
                                .payload
                                .get("grid_index")
                                .and_then(serde_json::Value::as_i64)
                                == Some(i64::from(grid_index))
                    })
            {
                return Err(anyhow!(
                    "grid touch cannot reuse the bar that established its rearm"
                ));
            }
            let batch_touches = events
                .iter()
                .zip(&existing)
                .filter(|(candidate, candidate_exists)| {
                    !*candidate_exists
                        && candidate.event_type == EventType::GridLevelTouched
                        && candidate.run_id == touch.run_id
                        && candidate.symbol == touch.symbol
                        && candidate.event_time == touch.event_time
                        && candidate
                            .payload
                            .get("grid_index")
                            .and_then(serde_json::Value::as_i64)
                            == Some(i64::from(grid_index))
                })
                .count();
            if batch_touches != 1 {
                return Err(anyhow!("grid opportunity must contain exactly one touch"));
            }
            let config = self
                .state
                .audited_config
                .as_ref()
                .ok_or_else(|| anyhow!("current grid touch lacks its audited configuration"))?;
            let grid = crate::grid::GridSpec::from(config);
            let canonical_price = grid.price(grid_index)?;
            let level = self
                .state
                .levels
                .get(&grid_index)
                .ok_or_else(|| anyhow!("grid touch has no canonical armed level"))?;
            let crossed = crate::grid::crosses_level(
                grid_index,
                canonical_price,
                previous.close,
                current.low,
                current.high,
            );
            if current.symbol != touch.symbol
                || current.timestamp != touch.event_time
                || previous.timestamp >= current.timestamp
                || !crossed
                || level.price != canonical_price
                || !matches!(
                    level.status,
                    GridLevelStatus::Armed | GridLevelStatus::Rearmed
                )
                || touch
                    .payload
                    .get("price")
                    .and_then(serde_json::Value::as_str)
                    != Some(canonical_price.to_string().as_str())
            {
                return Err(anyhow!(
                    "grid touch is not implied by its received market crossing"
                ));
            }
        }
        let mut projected = self.state.clone();
        let mut projection_market = self
            .store
            .recent_payloads_by_type(&self.state.run_id, EventType::MarketDataReceived, 1)?
            .pop()
            .map(serde_json::from_value::<MarketBar>)
            .transpose()?;
        for (event, exists) in events.iter().zip(&existing) {
            if !*exists {
                self.validate_current_platform_approval(&projected, event)?;
                if event.event_type == EventType::GridLevelRearmed {
                    self.validate_current_rearm_evidence(
                        &projected,
                        event,
                        projection_market.as_ref(),
                    )?;
                }
                if event.event_type == EventType::GridCycleReset {
                    self.validate_current_cycle_reset_evidence(&projected, event)?;
                }
                projected.apply_event(event)?;
                if event.event_type == EventType::MarketDataReceived {
                    projection_market =
                        Some(serde_json::from_value::<MarketBar>(event.payload.clone())?);
                }
            }
        }
        self.validate_current_cycle_batch(events, &existing, &projected)?;
        for (event_index, (event, exists)) in events.iter().zip(&existing).enumerate() {
            if *exists
                || event.event_type != EventType::GridRightGranted
                || event.schema_version < 2
            {
                continue;
            }
            let right: GridRight = serde_json::from_value(event.payload.clone())?;
            let right_price = right.grid_price.to_string();
            let matching_touches = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(touch_index, (candidate, candidate_exists))| {
                    !*candidate_exists
                        && *touch_index < event_index
                        && candidate.event_type == EventType::GridLevelTouched
                        && candidate.run_id == event.run_id
                        && candidate.cycle_id == event.cycle_id
                        && candidate.symbol == event.symbol
                        && candidate.event_time == event.event_time
                        && candidate.correlation_id == event.correlation_id
                        && candidate
                            .payload
                            .get("grid_index")
                            .and_then(serde_json::Value::as_i64)
                            == Some(i64::from(right.grid_index))
                        && candidate
                            .payload
                            .get("price")
                            .and_then(serde_json::Value::as_str)
                            == Some(right_price.as_str())
                })
                .count();
            if matching_touches != 1 {
                return Err(anyhow!(
                    "current grid right requires exactly one matching touch in the same batch"
                ));
            }
            let matching_decisions = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(decision_index, (candidate, candidate_exists))| {
                    !*candidate_exists
                        && *decision_index > event_index
                        && candidate.event_type == EventType::GateDecisionMade
                        && candidate.run_id == event.run_id
                        && candidate.cycle_id == event.cycle_id
                        && candidate.symbol == event.symbol
                        && candidate.event_time == event.event_time
                        && candidate.correlation_id == event.correlation_id
                        && candidate
                            .payload
                            .get("response")
                            .and_then(|response| response.get("right_id"))
                            .and_then(serde_json::Value::as_str)
                            == Some(right.right_id.as_str())
                })
                .count();
            if matching_decisions != 1 {
                return Err(anyhow!(
                    "current grid right requires exactly one matching decision in the same batch"
                ));
            }
            let matching_dispositions = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(disposition_index, (candidate, candidate_exists))| {
                    !*candidate_exists
                        && *disposition_index > event_index
                        && matches!(
                            candidate.event_type,
                            EventType::GridRightDeferred
                                | EventType::GridRightResidualHeld
                                | EventType::GridRightBlocked
                                | EventType::GridRightReserved
                        )
                        && !candidate
                            .payload
                            .get("partial")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                        && candidate.run_id == event.run_id
                        && candidate.cycle_id == event.cycle_id
                        && candidate.symbol == event.symbol
                        && candidate.event_time == event.event_time
                        && candidate.correlation_id == event.correlation_id
                        && candidate
                            .payload
                            .get("right_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(right.right_id.as_str())
                })
                .count();
            if matching_dispositions != 1 {
                return Err(anyhow!(
                    "current grid right requires one terminal C/P/A/E/D/B/I/R disposition"
                ));
            }
        }
        for (touch_index, (touch, touch_exists)) in events.iter().zip(&existing).enumerate() {
            if *touch_exists || touch.event_type != EventType::GridLevelTouched {
                continue;
            }
            let grid_index = touch
                .payload
                .get("grid_index")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| anyhow!("grid touch lacks its grid index"))?;
            let same_opportunity = |candidate: &EventEnvelope, candidate_exists: &bool| {
                !*candidate_exists
                    && candidate.run_id == touch.run_id
                    && candidate.cycle_id == touch.cycle_id
                    && candidate.symbol == touch.symbol
                    && candidate.event_time == touch.event_time
                    && candidate.correlation_id == touch.correlation_id
                    && candidate
                        .payload
                        .get("grid_index")
                        .and_then(serde_json::Value::as_i64)
                        == Some(grid_index)
            };
            let grants = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(index, (candidate, candidate_exists))| {
                    *index > touch_index
                        && candidate.event_type == EventType::GridRightGranted
                        && same_opportunity(candidate, candidate_exists)
                })
                .count();
            let skips = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(index, (candidate, candidate_exists))| {
                    *index > touch_index
                        && candidate.event_type == EventType::GridLevelSkipped
                        && same_opportunity(candidate, candidate_exists)
                })
                .count();
            if grants + skips != 1 {
                return Err(anyhow!(
                    "grid touch must resolve to one grant or one explicit skip in the same batch"
                ));
            }
        }
        for (skip_index, (skip, skip_exists)) in events.iter().zip(&existing).enumerate() {
            if *skip_exists || skip.event_type != EventType::GridLevelSkipped {
                continue;
            }
            let grid_index = skip
                .payload
                .get("grid_index")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| anyhow!("grid skip lacks its grid index"))?;
            let matching_touches = events
                .iter()
                .zip(&existing)
                .enumerate()
                .filter(|(index, (candidate, candidate_exists))| {
                    *index < skip_index
                        && !*candidate_exists
                        && candidate.event_type == EventType::GridLevelTouched
                        && candidate.run_id == skip.run_id
                        && candidate.cycle_id == skip.cycle_id
                        && candidate.symbol == skip.symbol
                        && candidate.event_time == skip.event_time
                        && candidate.correlation_id == skip.correlation_id
                        && candidate
                            .payload
                            .get("grid_index")
                            .and_then(serde_json::Value::as_i64)
                            == Some(grid_index)
                })
                .count();
            if matching_touches != 1 {
                return Err(anyhow!(
                    "grid skip requires exactly one matching touch in the same batch"
                ));
            }
        }
        for (event, exists) in events.iter().zip(&existing) {
            if !*exists
                && event.event_type == EventType::GridRightGranted
                && event.schema_version >= 2
            {
                let right: GridRight = serde_json::from_value(event.payload.clone())?;
                for tranche_id in &right.capacity.tranche_ids {
                    let tranche = projected.right_tranches.get(tranche_id).ok_or_else(|| {
                        anyhow!("granted right batch omitted an advertised tranche")
                    })?;
                    if (tranche.available_budget > rust_decimal::Decimal::ZERO
                        || tranche.reserved_budget > rust_decimal::Decimal::ZERO
                        || tranche.available_quantity > 0
                        || tranche.reserved_quantity > 0)
                        && tranche.owner_right_id != right.right_id
                    {
                        return Err(anyhow!(
                            "granted right batch did not transfer its available tranche inventory"
                        ));
                    }
                }
            }
        }
        projected.validate_invariants()?;
        let outcomes = self
            .store
            .append_batch_expected(events, *self.last_sequence)?;
        for (event, outcome) in events.iter().zip(outcomes) {
            match outcome {
                AppendOutcome::Appended(sequence) => {
                    self.state.apply_event(event)?;
                    *self.last_sequence = sequence;
                }
                AppendOutcome::Duplicate => self.state.duplicate_events += 1,
            }
        }
        Ok(())
    }

    pub fn save_snapshot(&mut self) -> Result<()> {
        self.store
            .save_snapshot_validated(self.state, *self.last_sequence)
    }

    fn validate_current_platform_approval(
        &self,
        state: &StrategyState,
        event: &EventEnvelope,
    ) -> Result<()> {
        if event.event_type == EventType::GateDecisionMade {
            let request: DecisionRequest = serde_json::from_value(
                event
                    .payload
                    .get("request")
                    .cloned()
                    .context("gate decision lacks request")?,
            )?;
            if request.contract_version == DECISION_CONTRACT_VERSION {
                let right = state
                    .grid_rights
                    .get(&request.right.right_id)
                    .context("v4 decision right is absent from the projected prefix")?;
                let config = state
                    .audited_config
                    .as_ref()
                    .context("v4 decision lacks audited configuration")?;
                if serde_json::to_value(right)? != serde_json::to_value(&request.right)? {
                    return Err(anyhow!(
                        "v4 decision request right differs from the projected grant"
                    ));
                }
                validate_funds_inventory_evidence(
                    config,
                    state,
                    self.store,
                    right,
                    &request.context,
                )?;
            }
            return Ok(());
        }
        if event.event_type == EventType::RightBalanceReserved {
            if event
                .payload
                .get("allocation_policy")
                .and_then(serde_json::Value::as_str)
                != Some("DEEPEST_FIRST_V1")
            {
                return Err(anyhow!(
                    "current reservation does not declare the canonical allocation policy"
                ));
            }
            return Ok(());
        }
        if event.event_type == EventType::OrderIntentCreated && event.schema_version >= 5 {
            let order: Order = serde_json::from_value(event.payload.clone())?;
            if order.intent.direction == Direction::Sell {
                let right = state
                    .grid_rights
                    .get(&order.intent.right_id)
                    .ok_or_else(|| anyhow!("current sell intent has no granted right"))?;
                let config = state
                    .audited_config
                    .as_ref()
                    .ok_or_else(|| anyhow!("current sell intent lacks audited controls"))?;
                let expected =
                    canonical_approval(config, state, right, right.decided_exercise_quantity)?;
                let actual = &order.intent.target_lot_allocations;
                let allocations_match = actual.len() == expected.sell_allocations.len()
                    && actual.iter().zip(&expected.sell_allocations).all(
                        |(recorded, canonical)| {
                            recorded.lot_id == canonical.lot_id
                                && recorded.quantity == canonical.quantity
                                && recorded.cost_basis == canonical.cost_basis
                                && recorded.worst_fill_price == canonical.worst_fill_price
                                && recorded.maximum_sell_fees == canonical.maximum_sell_fees
                                && recorded.worst_case_profit == canonical.worst_case_profit
                        },
                    );
                let expected_lot_ids = expected
                    .sell_allocations
                    .iter()
                    .map(|allocation| allocation.lot_id.as_str())
                    .collect::<Vec<_>>();
                let actual_lot_ids = order
                    .intent
                    .target_lot_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                if order.intent.quantity != expected.quantity
                    || actual_lot_ids != expected_lot_ids
                    || !allocations_match
                {
                    return Err(anyhow!(
                        "current sell intent differs from the canonical ordered lot plan"
                    ));
                }
            }
            return Ok(());
        }
        if event.schema_version < 3
            || !matches!(
                event.event_type,
                EventType::GridRightDeferred
                    | EventType::GridRightResidualHeld
                    | EventType::GridRightBlocked
                    | EventType::GridRightReserved
            )
            || event
                .payload
                .get("partial")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        {
            return Ok(());
        }
        let right_id = event
            .payload
            .get("right_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow!("current disposition lacks its right identity"))?;
        let right = state
            .grid_rights
            .get(right_id)
            .ok_or_else(|| anyhow!("current disposition has no granted right"))?;
        let config = state
            .audited_config
            .as_ref()
            .ok_or_else(|| anyhow!("current disposition lacks audited platform controls"))?;
        let recorded_intent = event
            .payload
            .get("order_intent_quantity")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| anyhow!("current disposition lacks order_intent_quantity"))?;
        let expected_intent =
            canonical_approved_quantity(config, state, right, right.decided_exercise_quantity)?;
        if recorded_intent != expected_intent {
            return Err(anyhow!(
                "platform approval differs from the canonical whole-unit risk decision"
            ));
        }
        Ok(())
    }

    fn validate_current_opportunity_binding(&self, event: &EventEnvelope) -> Result<()> {
        if !matches!(
            event.event_type,
            EventType::MarketDataReceived
                | EventType::GridLevelTouched
                | EventType::GridLevelSkipped
                | EventType::MarketBarDecisionsCommitted
                | EventType::MarketBarProcessed
        ) {
            return Ok(());
        }
        let config = self
            .state
            .audited_config
            .as_ref()
            .ok_or_else(|| anyhow!("current opportunity lacks its audited configuration"))?;
        if event.cycle_id != self.state.cycle_id {
            return Err(anyhow!(
                "current opportunity does not belong to the active grid cycle"
            ));
        }
        if event.config_version != config.config_version {
            return Err(anyhow!(
                "current opportunity does not use the audited configuration version"
            ));
        }
        Ok(())
    }

    fn validate_current_rearm_evidence(
        &self,
        state: &StrategyState,
        event: &EventEnvelope,
        market: Option<&MarketBar>,
    ) -> Result<()> {
        if event.schema_version < 2 {
            return Ok(());
        }
        let index = event
            .payload
            .get("grid_index")
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| anyhow!("current grid rearm lacks a valid grid index"))?;
        let config = state
            .audited_config
            .as_ref()
            .ok_or_else(|| anyhow!("current grid rearm lacks its audited configuration"))?;
        let grid = crate::grid::GridSpec::from(config);
        let level = state
            .levels
            .get(&index)
            .ok_or_else(|| anyhow!("current grid rearm has no prior level"))?;
        let market = market.ok_or_else(|| anyhow!("current grid rearm lacks its market bar"))?;
        if !self.store.has_market_stage(
            &state.run_id,
            EventType::MarketBarDecisionsCommitted,
            &state.symbol,
            event.event_time,
        )? || self.store.has_market_stage(
            &state.run_id,
            EventType::MarketBarProcessed,
            &state.symbol,
            event.event_time,
        )? {
            return Err(anyhow!(
                "current grid rearm is outside the durable post-decision pre-processed phase"
            ));
        }
        let adjacent = if index > 0 {
            grid.price(index - 1)?
        } else {
            grid.price(index + 1)?
        };
        let canonical_price = grid.price(index)?;
        let mut candidate = level.clone();
        if event.cycle_id != state.cycle_id
            || event.config_version != config.config_version
            || market.symbol != state.symbol
            || market.timestamp != event.event_time
            || level.price != canonical_price
            || !crate::grid::maybe_rearm(
                &mut candidate,
                market.close,
                level
                    .price
                    .checked_sub(adjacent)
                    .context("grid width exceeds numeric range")?,
                config.hysteresis_ratio,
            )?
        {
            return Err(anyhow!(
                "current grid rearm is not justified by market hysteresis"
            ));
        }
        Ok(())
    }

    fn validate_current_market_stage(
        &self,
        state: &StrategyState,
        event: &EventEnvelope,
    ) -> Result<()> {
        if event.schema_version < 2 {
            return Ok(());
        }
        let market_payload = self
            .store
            .market_payload_at(&state.run_id, &state.symbol, event.event_time)?
            .ok_or_else(|| anyhow!("current market stage lacks its received market fact"))?;
        let market: MarketBar = serde_json::from_value(market_payload)?;
        if market.symbol != event.symbol || market.timestamp != event.event_time {
            return Err(anyhow!(
                "current market stage does not identify its received market fact"
            ));
        }
        if event.event_type == EventType::MarketBarProcessed {
            let config = state
                .audited_config
                .as_ref()
                .ok_or_else(|| anyhow!("processed bar lacks its audited grid"))?;
            let grid = crate::grid::GridSpec::from(config);
            let omitted_rearm = state
                .levels
                .iter()
                .map(|(index, level)| -> Result<bool> {
                    let adjacent = if *index > 0 {
                        grid.price(*index - 1)?
                    } else {
                        grid.price(*index + 1)?
                    };
                    let mut candidate = level.clone();
                    crate::grid::maybe_rearm(
                        &mut candidate,
                        market.close,
                        level
                            .price
                            .checked_sub(adjacent)
                            .context("grid width exceeds numeric range")?,
                        config.hysteresis_ratio,
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(|value| value);
            if !self.store.has_market_stage(
                &state.run_id,
                EventType::MarketBarDecisionsCommitted,
                &state.symbol,
                event.event_time,
            )? || self.store.has_market_stage(
                &state.run_id,
                EventType::MarketBarProcessed,
                &state.symbol,
                event.event_time,
            )? || event
                .payload
                .get("close")
                .and_then(serde_json::Value::as_str)
                != Some(market.close.to_string().as_str())
                || omitted_rearm
            {
                return Err(anyhow!(
                    "processed bar does not close its complete durable decision workflow"
                ));
            }
            return Ok(());
        }
        if event.event_type == EventType::MarketBarDecisionsCommitted {
            if self.store.has_market_stage(
                &state.run_id,
                EventType::MarketBarDecisionsCommitted,
                &state.symbol,
                event.event_time,
            )? || self.store.has_market_stage(
                &state.run_id,
                EventType::MarketBarProcessed,
                &state.symbol,
                event.event_time,
            )? {
                return Err(anyhow!(
                    "market decisions cannot be committed after bar processing"
                ));
            }
            let recent = self.store.recent_payloads_by_type(
                &state.run_id,
                EventType::MarketDataReceived,
                2,
            )?;
            if recent.len() == 2 {
                let previous: MarketBar = serde_json::from_value(recent[0].clone())?;
                let current: MarketBar = serde_json::from_value(recent[1].clone())?;
                if current.timestamp != event.event_time {
                    return Err(anyhow!(
                        "market decisions do not belong to the latest received bar"
                    ));
                }
                let grid = crate::grid::GridSpec::from(
                    state
                        .audited_config
                        .as_ref()
                        .ok_or_else(|| anyhow!("market decisions lack audited grid"))?,
                );
                let unresolved = state
                    .levels
                    .iter()
                    .map(|(index, level)| -> Result<bool> {
                        if !matches!(
                            level.status,
                            GridLevelStatus::Armed | GridLevelStatus::Rearmed
                        ) {
                            return Ok(false);
                        }
                        Ok(crate::grid::crosses_level(
                            *index,
                            grid.price(*index)?,
                            previous.close,
                            current.low,
                            current.high,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .any(|value| value);
                if unresolved {
                    return Err(anyhow!(
                        "market decisions leave a mechanical grid crossing unresolved"
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_current_cycle_reset_evidence(
        &self,
        state: &StrategyState,
        event: &EventEnvelope,
    ) -> Result<()> {
        if event.schema_version < 2 {
            return Ok(());
        }
        let completed_sell = state.orders.values().any(|order| {
            order.status == OrderStatus::Filled
                && order.intent.direction == Direction::Sell
                && state
                    .grid_rights
                    .get(&order.intent.right_id)
                    .is_some_and(|right| right.cycle_id == state.cycle_id)
        });
        let latest_sell_fill_time = self.store.latest_sell_fill_time(&state.run_id)?;
        if !completed_sell
            || latest_sell_fill_time != Some(event.event_time)
            || !self.store.has_market_stage(
                &state.run_id,
                EventType::MarketBarProcessed,
                &state.symbol,
                event.event_time,
            )?
        {
            return Err(anyhow!(
                "current cycle reset lacks a completed sell and processed market bar"
            ));
        }
        Ok(())
    }

    fn validate_current_cycle_batch(
        &self,
        events: &[EventEnvelope],
        existing: &[bool],
        projected: &StrategyState,
    ) -> Result<()> {
        let has_current_cycle_control = events.iter().zip(existing).any(|(event, exists)| {
            !*exists
                && event.schema_version >= 2
                && matches!(
                    event.event_type,
                    EventType::GridCycleStarted
                        | EventType::GridCycleReset
                        | EventType::GridLevelArmed
                )
        });
        if !has_current_cycle_control {
            return Ok(());
        }
        let config = projected
            .audited_config
            .as_ref()
            .ok_or_else(|| anyhow!("current cycle batch lacks its audited configuration"))?;
        let canonical = crate::grid::GridSpec::from(config).levels()?;
        let mut lifecycle = Vec::<(usize, String)>::new();
        for (index, (event, exists)) in events.iter().zip(existing).enumerate() {
            if *exists || event.schema_version < 2 {
                continue;
            }
            if matches!(
                event.event_type,
                EventType::GridCycleStarted | EventType::GridCycleReset
            ) {
                let cycle_id = if event.event_type == EventType::GridCycleStarted {
                    event.cycle_id.clone()
                } else {
                    event
                        .payload
                        .get("cycle_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| anyhow!("current cycle reset lacks its target cycle"))?
                        .to_owned()
                };
                lifecycle.push((index, cycle_id));
            }
        }
        for (index, cycle_id) in &lifecycle {
            let suffix = events
                .iter()
                .zip(existing)
                .enumerate()
                .skip(index + 1)
                .filter(|(_, (_, exists))| !**exists)
                .collect::<Vec<_>>();
            if suffix.len() != canonical.len()
                || suffix.iter().any(|(_, (event, _))| {
                    event.event_type != EventType::GridLevelArmed || event.cycle_id != *cycle_id
                })
            {
                return Err(anyhow!(
                    "current cycle must atomically arm its complete canonical grid"
                ));
            }
            let mut seen = std::collections::BTreeMap::new();
            for (_, (event, _)) in suffix {
                let level: GridLevelState = serde_json::from_value(event.payload.clone())?;
                if seen.insert(level.index, level.price).is_some()
                    || canonical
                        .get(&level.index)
                        .is_none_or(|expected| expected.price != level.price)
                {
                    return Err(anyhow!(
                        "current cycle contains a duplicate or non-canonical grid arm"
                    ));
                }
            }
            if seen.len() != canonical.len() {
                return Err(anyhow!("current cycle omitted a canonical grid arm"));
            }
        }
        for (arm_index, (event, exists)) in events.iter().zip(existing).enumerate() {
            if *exists || event.schema_version < 2 || event.event_type != EventType::GridLevelArmed
            {
                continue;
            }
            let owners = lifecycle
                .iter()
                .filter(|(cycle_index, cycle_id)| {
                    *cycle_index < arm_index && *cycle_id == event.cycle_id
                })
                .count();
            if owners != 1 {
                return Err(anyhow!(
                    "current grid arm lacks exactly one atomic cycle owner"
                ));
            }
        }
        Ok(())
    }
}
