use crate::{
    ths_sim::{
        cancel_contract_with, prepare_with, probe_fills_with, probe_orders_with,
        submit_prepared_form_with, RemoteFilledEvidence, SimulatedOrderDraft, SimulationFillTable,
        SimulationOrderRecord, SimulationOrderTable, SystemOsaScript,
    },
    ths_sim_outbox::{
        stage_from_source, OutboxStatus, StagedCancelState, StagedIntent, StagedIntentState,
        StagedTerminalResolution, ThsSimOutbox,
    },
};
use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDateTime};
use rust_decimal::Decimal;
use serde::Serialize;
use std::{collections::BTreeSet, path::Path};

pub trait SimulationUiDriver {
    fn orders(&mut self) -> Result<SimulationOrderTable>;
    fn fills(&mut self) -> Result<SimulationFillTable>;
    fn startup_preflight(&mut self) -> Result<()> {
        self.orders()?;
        self.fills()?;
        Ok(())
    }
    fn prepare(&mut self, order: &SimulatedOrderDraft) -> Result<()>;
    fn submit_once(&mut self, order: &SimulatedOrderDraft) -> Result<()>;
    fn cancel_contract_once(&mut self, contract_id: &str) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MacOsSimulationUiDriver {
    executor: SystemOsaScript,
}

impl SimulationUiDriver for MacOsSimulationUiDriver {
    fn orders(&mut self) -> Result<SimulationOrderTable> {
        probe_orders_with(&self.executor)
    }

    fn fills(&mut self) -> Result<SimulationFillTable> {
        probe_fills_with(&self.executor)
    }

    fn prepare(&mut self, order: &SimulatedOrderDraft) -> Result<()> {
        prepare_with(&self.executor, order.clone()).map(|_| ())
    }

    fn submit_once(&mut self, order: &SimulatedOrderDraft) -> Result<()> {
        submit_prepared_form_with(&self.executor, order)
    }

    fn cancel_contract_once(&mut self, contract_id: &str) -> Result<()> {
        cancel_contract_with(&self.executor, contract_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationExecutionReceipt {
    pub intent_id: String,
    pub request_sha256: String,
    pub remote_order: SimulationOrderRecord,
    pub clicked_in_this_call: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationCancellationReceipt {
    pub intent_id: String,
    pub request_sha256: String,
    pub remote_order: SimulationOrderRecord,
    pub clicked_in_this_call: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationAutomationCycleReceipt {
    pub source_cursor: i64,
    pub executed: Vec<SimulationExecutionReceipt>,
    pub cancelled: Vec<SimulationCancellationReceipt>,
    pub status: OutboxStatus,
}

pub fn run_automation_cycle(
    source_database: impl AsRef<Path>,
    run_id: &str,
    outbox_path: impl AsRef<Path>,
    initial_after_sequence: Option<i64>,
    now: NaiveDateTime,
    maximum_age: Duration,
    driver: &mut dyn SimulationUiDriver,
) -> Result<SimulationAutomationCycleReceipt> {
    let status = stage_from_source(
        source_database,
        run_id,
        outbox_path.as_ref(),
        initial_after_sequence,
    )?;
    let mut outbox = ThsSimOutbox::open(outbox_path)?;
    let mut receipt = process_outbox_once(&mut outbox, now, maximum_age, driver)?;
    receipt.source_cursor = status.cursor;
    Ok(receipt)
}

pub fn process_outbox_once(
    outbox: &mut ThsSimOutbox,
    now: NaiveDateTime,
    maximum_age: Duration,
    driver: &mut dyn SimulationUiDriver,
) -> Result<SimulationAutomationCycleReceipt> {
    let mut initial = outbox.staged_intents()?;
    if initial.iter().any(|intent| {
        intent.cancel_state == StagedCancelState::Ambiguous
            && intent.terminal_resolution == StagedTerminalResolution::Unresolved
    }) {
        recover_full_fills_after_ambiguous_cancel(outbox, now, driver)?;
        initial = outbox.staged_intents()?;
    }
    if initial.iter().any(|intent| {
        intent.state == StagedIntentState::Ambiguous
            || (intent.cancel_state == StagedCancelState::Ambiguous
                && intent.terminal_resolution == StagedTerminalResolution::Unresolved)
    }) {
        bail!(
            "Tonghuashun outbox contains an AMBIGUOUS operation; refusing all automatic UI actions"
        );
    }

    let mut executed = Vec::new();
    for intent in initial {
        if matches!(
            intent.state,
            StagedIntentState::Eligible | StagedIntentState::Submitting
        ) {
            executed.push(execute_staged_intent(
                outbox,
                &intent.intent_id,
                now,
                maximum_age,
                driver,
            )?);
        }
    }

    let after_submission = outbox.staged_intents()?;
    if after_submission.iter().any(|intent| {
        intent.state == StagedIntentState::Ambiguous
            || (intent.cancel_state == StagedCancelState::Ambiguous
                && intent.terminal_resolution == StagedTerminalResolution::Unresolved)
    }) {
        bail!("Tonghuashun outbox became AMBIGUOUS; refusing further automatic UI actions");
    }

    let mut cancelled = Vec::new();
    for intent in after_submission {
        if intent.terminal_resolution == StagedTerminalResolution::Filled {
            continue;
        }
        let should_cancel = intent.state == StagedIntentState::Submitted
            && (intent.cancel_state == StagedCancelState::Cancelling
                || (intent.cancel_state == StagedCancelState::NotRequested
                    && intent.source_cancel_requested_sequence.is_some()));
        if should_cancel {
            cancelled.push(cancel_submitted_intent(outbox, &intent.intent_id, driver)?);
        }
    }

    let status = outbox.status()?;
    Ok(SimulationAutomationCycleReceipt {
        source_cursor: status.cursor,
        executed,
        cancelled,
        status,
    })
}

pub fn reconcile_ambiguous_terminal_fills(
    outbox_path: impl AsRef<Path>,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
) -> Result<()> {
    let mut outbox = ThsSimOutbox::open(outbox_path)?;
    recover_full_fills_after_ambiguous_cancel(&mut outbox, now, driver)
}

fn recover_full_fills_after_ambiguous_cancel(
    outbox: &mut ThsSimOutbox,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
) -> Result<()> {
    let unresolved = outbox
        .staged_intents()?
        .into_iter()
        .filter(|intent| {
            intent.state == StagedIntentState::Submitted
                && intent.cancel_state == StagedCancelState::Ambiguous
                && intent.terminal_resolution == StagedTerminalResolution::Unresolved
        })
        .collect::<Vec<_>>();
    if unresolved.is_empty() {
        return Ok(());
    }
    let order_records = driver.orders()?.records()?;
    let fill_records = driver.fills()?.records()?;
    for intent in unresolved {
        let contract_id = intent
            .remote_contract_id
            .as_deref()
            .context("ambiguous cancellation lacks its remote contract ID")?;
        let order_matches = order_records
            .iter()
            .filter(|record| record.contract_id == contract_id)
            .collect::<Vec<_>>();
        let [order_record] = order_matches.as_slice() else {
            bail!(
                "remote-filled recovery expected one order row for contract {contract_id}, found {}",
                order_matches.len()
            );
        };
        if order_record.symbol != intent.order.symbol
            || order_record.direction != intent.order.direction
            || order_record.quantity != intent.order.quantity
            || order_record.order_price != intent.order.limit_price
            || order_record.remark != "全部成交"
            || order_record.cancelled_quantity != 0
            || order_record.fill_price.is_none()
        {
            bail!("remote-filled recovery order row does not match its durable intent");
        }
        let mut matching_fills = fill_records
            .iter()
            .filter(|fill| fill.contract_id == contract_id)
            .cloned()
            .collect::<Vec<_>>();
        matching_fills.sort_by(|left, right| left.fill_id.cmp(&right.fill_id));
        if matching_fills.is_empty() {
            bail!("remote-filled recovery lacks transaction rows for its contract");
        }
        let mut fill_ids = BTreeSet::new();
        let mut filled_quantity = 0_i64;
        let mut filled_notional = Decimal::ZERO;
        let mut minimum_fill_price: Option<Decimal> = None;
        let mut maximum_fill_price: Option<Decimal> = None;
        for fill in &matching_fills {
            if !fill_ids.insert(fill.fill_id.as_str()) {
                bail!("remote-filled recovery contains a duplicate fill ID");
            }
            if fill.symbol != intent.order.symbol || fill.direction != intent.order.direction {
                bail!("remote-filled recovery transaction row changed symbol or direction");
            }
            filled_quantity = filled_quantity
                .checked_add(fill.quantity)
                .context("remote-filled recovery quantity overflow")?;
            filled_notional = filled_notional
                .checked_add(fill.amount)
                .context("remote-filled recovery notional overflow")?;
            minimum_fill_price =
                Some(minimum_fill_price.map_or(fill.price, |value| value.min(fill.price)));
            maximum_fill_price =
                Some(maximum_fill_price.map_or(fill.price, |value| value.max(fill.price)));
        }
        if filled_quantity != intent.order.quantity {
            bail!(
                "remote-filled recovery quantity {filled_quantity} does not equal durable quantity {}",
                intent.order.quantity
            );
        }
        let order_fill_price = order_record
            .fill_price
            .as_deref()
            .context("remote-filled recovery order row lacks its displayed fill price")?
            .parse::<Decimal>()
            .context("remote-filled recovery order fill price is invalid")?;
        if order_fill_price < minimum_fill_price.context("missing minimum fill price")?
            || order_fill_price > maximum_fill_price.context("missing maximum fill price")?
        {
            bail!("remote-filled recovery displayed price is outside its transaction prices");
        }
        let evidence = RemoteFilledEvidence {
            evidence_contract_version: 1,
            order: (*order_record).clone(),
            fills: matching_fills,
        };
        let evidence_json = serde_json::to_string(&evidence)?;
        outbox.record_remote_filled(
            &intent.intent_id,
            &intent.request_sha256,
            contract_id,
            &now.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            evidence.fills.len(),
            filled_quantity,
            &filled_notional.normalize().to_string(),
            &evidence_json,
        )?;
    }
    Ok(())
}

pub fn execute_staged_intent(
    outbox: &mut ThsSimOutbox,
    intent_id: &str,
    now: NaiveDateTime,
    maximum_age: Duration,
    driver: &mut dyn SimulationUiDriver,
) -> Result<SimulationExecutionReceipt> {
    let intent = find_intent(outbox, intent_id)?;
    match intent.state {
        StagedIntentState::Eligible => {
            intent.verify_execution_freshness(now, maximum_age)?;
            let baseline = driver.orders()?.records()?;
            let baseline_contract_ids = baseline
                .iter()
                .map(|record| record.contract_id.clone())
                .collect::<Vec<_>>();
            driver.prepare(&intent.order)?;
            outbox.begin_submission(
                &intent.intent_id,
                &intent.request_sha256,
                &baseline_contract_ids,
            )?;
            let claimed = find_intent(outbox, intent_id)?;
            if let Err(error) = driver.submit_once(&intent.order) {
                mark_ambiguous_after_claim(outbox, &claimed, &error, None)?;
                return Err(error.context(
                    "Tonghuashun submit result is ambiguous; the durable intent cannot be clicked again",
                ));
            }
            reconcile_claimed_intent(outbox, &claimed, driver, true)
        }
        StagedIntentState::Submitting => {
            reconcile_claimed_intent(outbox, &intent, driver, false)
        }
        StagedIntentState::Submitted => {
            let remote_order = parse_completed_record(&intent)?;
            Ok(SimulationExecutionReceipt {
                intent_id: intent.intent_id,
                request_sha256: intent.request_sha256,
                remote_order,
                clicked_in_this_call: false,
            })
        }
        StagedIntentState::Discovered => {
            bail!("order intent is not eligible until its durable ORDER_SUBMITTED event exists")
        }
        StagedIntentState::Ambiguous => bail!(
            "Tonghuashun submission is AMBIGUOUS and requires operator reconciliation; refusing another click"
        ),
    }
}

pub fn cancel_submitted_intent(
    outbox: &mut ThsSimOutbox,
    intent_id: &str,
    driver: &mut dyn SimulationUiDriver,
) -> Result<SimulationCancellationReceipt> {
    let intent = find_intent(outbox, intent_id)?;
    if intent.state != StagedIntentState::Submitted {
        bail!("only a SUBMITTED Tonghuashun intent can be cancelled");
    }
    let remote_order = parse_completed_record(&intent)?;
    match intent.cancel_state {
        StagedCancelState::NotRequested => {
            ensure_contract_is_visible(driver, &remote_order.contract_id)?;
            outbox.begin_cancellation(
                &intent.intent_id,
                &intent.request_sha256,
                &remote_order.contract_id,
            )?;
            let claimed = find_intent(outbox, intent_id)?;
            if let Err(error) = driver.cancel_contract_once(&remote_order.contract_id) {
                mark_cancel_ambiguous(outbox, &claimed, &remote_order.contract_id, &error, None)?;
                return Err(error.context(
                    "Tonghuashun cancel result is ambiguous; the durable contract cannot be clicked again",
                ));
            }
            reconcile_cancellation(outbox, &claimed, driver, true)
        }
        StagedCancelState::Cancelling => {
            reconcile_cancellation(outbox, &intent, driver, false)
        }
        StagedCancelState::Cancelled => {
            let remote_order = parse_cancelled_record(&intent)?;
            Ok(SimulationCancellationReceipt {
                intent_id: intent.intent_id,
                request_sha256: intent.request_sha256,
                remote_order,
                clicked_in_this_call: false,
            })
        }
        StagedCancelState::Ambiguous => bail!(
            "Tonghuashun cancellation is AMBIGUOUS and requires operator reconciliation; refusing another click"
        ),
    }
}

pub fn cancel_submitted_contract(
    outbox: &mut ThsSimOutbox,
    remote_contract_id: &str,
    driver: &mut dyn SimulationUiDriver,
) -> Result<SimulationCancellationReceipt> {
    let matches = outbox
        .staged_intents()?
        .into_iter()
        .filter(|intent| intent.remote_contract_id.as_deref() == Some(remote_contract_id))
        .collect::<Vec<_>>();
    let [intent] = matches.as_slice() else {
        bail!(
            "expected exactly one durable submitted intent bound to Tonghuashun contract {remote_contract_id}, found {}",
            matches.len()
        );
    };
    cancel_submitted_intent(outbox, &intent.intent_id, driver)
}

fn reconcile_claimed_intent(
    outbox: &mut ThsSimOutbox,
    intent: &StagedIntent,
    driver: &mut dyn SimulationUiDriver,
    clicked_in_this_call: bool,
) -> Result<SimulationExecutionReceipt> {
    let table = match driver.orders() {
        Ok(table) => table,
        Err(error) => {
            mark_ambiguous_after_claim(outbox, intent, &error, None)?;
            return Err(error.context("failed to read the post-submit Tonghuashun order table"));
        }
    };
    let evidence_json = serde_json::to_string(&table)?;
    let record = match table.unique_new_match(&intent.order, &intent.baseline_contract_ids) {
        Ok(record) => record,
        Err(error) => {
            mark_ambiguous_after_claim(outbox, intent, &error, Some(&evidence_json))?;
            return Err(error.context(
                "Tonghuashun submission could not be reconciled to exactly one new contract",
            ));
        }
    };
    let record_json = serde_json::to_string(&record)?;
    outbox.complete_submission(
        &intent.intent_id,
        &intent.request_sha256,
        &record.contract_id,
        &record_json,
    )?;
    Ok(SimulationExecutionReceipt {
        intent_id: intent.intent_id.clone(),
        request_sha256: intent.request_sha256.clone(),
        remote_order: record,
        clicked_in_this_call,
    })
}

fn mark_ambiguous_after_claim(
    outbox: &mut ThsSimOutbox,
    intent: &StagedIntent,
    error: &anyhow::Error,
    evidence_json: Option<&str>,
) -> Result<()> {
    outbox.mark_ambiguous(
        &intent.intent_id,
        &intent.request_sha256,
        &format!("{error:#}"),
        evidence_json,
    )
}

fn ensure_contract_is_visible(
    driver: &mut dyn SimulationUiDriver,
    contract_id: &str,
) -> Result<()> {
    let records = driver.orders()?.records()?;
    let matches = records
        .iter()
        .filter(|record| record.contract_id == contract_id)
        .count();
    if matches != 1 {
        bail!("expected exactly one visible Tonghuashun contract {contract_id}, found {matches}");
    }
    Ok(())
}

fn reconcile_cancellation(
    outbox: &mut ThsSimOutbox,
    intent: &StagedIntent,
    driver: &mut dyn SimulationUiDriver,
    clicked_in_this_call: bool,
) -> Result<SimulationCancellationReceipt> {
    let contract_id = intent
        .remote_contract_id
        .as_deref()
        .context("CANCELLING intent lacks its remote contract ID")?;
    let table = match driver.orders() {
        Ok(table) => table,
        Err(error) => {
            mark_cancel_ambiguous(outbox, intent, contract_id, &error, None)?;
            return Err(error.context("failed to read the post-cancel Tonghuashun order table"));
        }
    };
    let evidence_json = serde_json::to_string(&table)?;
    let matches = table
        .records()?
        .into_iter()
        .filter(|record| record.contract_id == contract_id)
        .collect::<Vec<_>>();
    let [record] = matches.as_slice() else {
        let error = anyhow::anyhow!(
            "expected one cancelled Tonghuashun contract {contract_id}, found {}",
            matches.len()
        );
        mark_cancel_ambiguous(outbox, intent, contract_id, &error, Some(&evidence_json))?;
        return Err(error);
    };
    if record.cancelled_quantity != record.quantity || !record.remark.contains("撤单") {
        let error = anyhow::anyhow!(
            "Tonghuashun contract {contract_id} is not a fully cancelled zero-fill order"
        );
        mark_cancel_ambiguous(outbox, intent, contract_id, &error, Some(&evidence_json))?;
        return Err(error);
    }
    let record_json = serde_json::to_string(record)?;
    outbox.complete_cancellation(
        &intent.intent_id,
        &intent.request_sha256,
        contract_id,
        &record_json,
    )?;
    Ok(SimulationCancellationReceipt {
        intent_id: intent.intent_id.clone(),
        request_sha256: intent.request_sha256.clone(),
        remote_order: record.clone(),
        clicked_in_this_call,
    })
}

fn mark_cancel_ambiguous(
    outbox: &mut ThsSimOutbox,
    intent: &StagedIntent,
    contract_id: &str,
    error: &anyhow::Error,
    evidence_json: Option<&str>,
) -> Result<()> {
    outbox.mark_cancellation_ambiguous(
        &intent.intent_id,
        &intent.request_sha256,
        contract_id,
        &format!("{error:#}"),
        evidence_json,
    )
}

fn find_intent(outbox: &ThsSimOutbox, intent_id: &str) -> Result<StagedIntent> {
    let matches = outbox
        .staged_intents()?
        .into_iter()
        .filter(|intent| intent.intent_id == intent_id)
        .collect::<Vec<_>>();
    let [intent] = matches.as_slice() else {
        bail!("expected exactly one staged intent named {intent_id}");
    };
    Ok(intent.clone())
}

fn parse_completed_record(intent: &StagedIntent) -> Result<SimulationOrderRecord> {
    let evidence = intent
        .remote_evidence_json
        .as_deref()
        .context("SUBMITTED Tonghuashun intent lacks its remote evidence")?;
    let record: SimulationOrderRecord = serde_json::from_str(evidence)
        .context("stored Tonghuashun evidence is not an order row")?;
    if Some(record.contract_id.as_str()) != intent.remote_contract_id.as_deref() {
        bail!("stored Tonghuashun contract ID does not match its evidence");
    }
    Ok(record)
}

fn parse_cancelled_record(intent: &StagedIntent) -> Result<SimulationOrderRecord> {
    let evidence = intent
        .cancel_evidence_json
        .as_deref()
        .context("CANCELLED Tonghuashun intent lacks its cancel evidence")?;
    let record: SimulationOrderRecord =
        serde_json::from_str(evidence).context("stored cancel evidence is not an order row")?;
    if Some(record.contract_id.as_str()) != intent.remote_contract_id.as_deref() {
        bail!("stored cancelled contract ID does not match its submission evidence");
    }
    Ok(record)
}
