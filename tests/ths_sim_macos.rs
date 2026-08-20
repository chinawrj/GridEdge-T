#![cfg(target_os = "macos")]

use anyhow::{ensure, Result};
use chrono::Local;
use gridedge_t::{
    domain::{Direction, Order, OrderIntent, OrderStatus},
    event::{current_event_schema, EventEnvelope, EventType},
    ths_sim::{
        cancel_contract_with, probe_current_market_quote, probe_orders_with, SystemOsaScript,
    },
    ths_sim_execution::{
        cancel_submitted_contract, execute_staged_intent, MacOsSimulationUiDriver,
    },
    ths_sim_outbox::{StagedCancelState, StagedIntentState, ThsSimOutbox},
};
use rust_decimal::Decimal;
use serde_json::json;
use tempfile::tempdir;

#[test]
#[ignore = "requires the reviewed Tonghuashun 5.3.2 模拟练习 account and creates then cancels one 100-share simulation order"]
fn current_simulation_account_submits_reconciles_and_cancels_one_contract() -> Result<()> {
    let smoke_limit_price = Decimal::new(320, 2);
    let minimum_non_marketable_ratio = Decimal::new(105, 2);
    let observed_quote = probe_current_market_quote("002256")?;
    ensure!(
        observed_quote.last_price >= smoke_limit_price * minimum_non_marketable_ratio,
        "hardware smoke BUY price must remain at least 5% below the simulation quote"
    );
    let directory = tempdir()?;
    let outbox_path = directory.path().join("ths-hardware-smoke.db");
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.bind_or_verify(
        "0123456789abcdef0123456789abcdef",
        "ths-hardware-smoke",
        Some(0),
    )?;
    let event_time = Local::now().naive_local();
    let order = Order {
        order_id: "ths-hardware-order".to_owned(),
        intent: OrderIntent {
            intent_id: "ths-hardware-intent".to_owned(),
            origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
            right_id: "ths-hardware-right".to_owned(),
            grid_index: -1,
            direction: Direction::Buy,
            limit_price: smoke_limit_price,
            quantity: 100,
            budget: Decimal::new(320, 0),
            target_lot_ids: Vec::new(),
            target_lot_allocations: Vec::new(),
            profit_guard_version: String::new(),
            profit_guard_policy: None,
            created_at: event_time,
        },
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: Decimal::ZERO,
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    outbox.ingest(&[
        envelope(
            1,
            EventType::OrderIntentCreated,
            current_event_schema(EventType::OrderIntentCreated),
            serde_json::to_value(order)?,
            event_time,
        ),
        envelope(
            2,
            EventType::OrderSubmitted,
            current_event_schema(EventType::OrderSubmitted),
            json!({"order_id":"ths-hardware-order"}),
            event_time,
        ),
    ])?;

    let mut driver = MacOsSimulationUiDriver::default();
    let submitted = execute_staged_intent(
        &mut outbox,
        "ths-hardware-intent",
        event_time,
        chrono::Duration::minutes(5),
        &mut driver,
    )?;
    assert!(submitted.clicked_in_this_call);
    assert_eq!(submitted.remote_order.symbol, "002256");
    assert_eq!(submitted.remote_order.quantity, 100);
    assert_eq!(submitted.remote_order.order_price, smoke_limit_price);
    assert!(submitted.remote_order.fill_price.is_none());
    assert_eq!(submitted.remote_order.cancelled_quantity, 0);

    let cancelled = cancel_submitted_contract(
        &mut outbox,
        &submitted.remote_order.contract_id,
        &mut driver,
    )?;
    assert!(cancelled.clicked_in_this_call);
    assert_eq!(
        cancelled.remote_order.contract_id,
        submitted.remote_order.contract_id
    );
    assert_eq!(cancelled.remote_order.cancelled_quantity, 100);
    assert!(cancelled.remote_order.remark.contains("撤单"));

    let staged = outbox.staged_intents()?;
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].state, StagedIntentState::Submitted);
    assert_eq!(staged[0].cancel_state, StagedCancelState::Cancelled);
    assert_eq!(
        staged[0].remote_contract_id.as_deref(),
        Some(submitted.remote_order.contract_id.as_str())
    );
    Ok(())
}

#[test]
#[ignore = "requires the reviewed Tonghuashun 5.3.2 模拟练习 account and cancels the exact contract in GRIDEDGE_THS_SMOKE_CONTRACT_ID"]
fn current_simulation_account_cancels_one_existing_exact_contract() -> Result<()> {
    let contract_id = std::env::var("GRIDEDGE_THS_SMOKE_CONTRACT_ID")?;
    let executor = SystemOsaScript;
    let before = probe_orders_with(&executor)?;
    let before_records = before.records()?;
    let matching_before: Vec<_> = before_records
        .iter()
        .filter(|order| order.contract_id == contract_id)
        .collect();
    assert_eq!(matching_before.len(), 1);
    assert!(matching_before[0].fill_price.is_none());
    assert_eq!(matching_before[0].cancelled_quantity, 0);

    cancel_contract_with(&executor, &contract_id)?;

    let after = probe_orders_with(&executor)?;
    let after_records = after.records()?;
    let matching_after: Vec<_> = after_records
        .iter()
        .filter(|order| order.contract_id == contract_id)
        .collect();
    assert_eq!(matching_after.len(), 1);
    assert!(matching_after[0].fill_price.is_none());
    assert_eq!(
        matching_after[0].cancelled_quantity,
        matching_after[0].quantity
    );
    assert!(matching_after[0].remark.contains("撤单"));
    Ok(())
}

fn envelope(
    sequence: i64,
    event_type: EventType,
    schema_version: i32,
    payload: serde_json::Value,
    event_time: chrono::NaiveDateTime,
) -> EventEnvelope {
    EventEnvelope {
        event_id: format!("ths-hardware-event-{sequence}"),
        event_type,
        schema_version,
        run_id: "ths-hardware-smoke".to_owned(),
        cycle_id: "ths-hardware-cycle".to_owned(),
        symbol: "002256.SZ".to_owned(),
        event_time,
        recorded_at: event_time,
        sequence_number: sequence,
        correlation_id: "ths-hardware-correlation".to_owned(),
        causation_id: None,
        idempotency_key: format!("ths-hardware-key-{sequence}"),
        payload,
        config_version: "ths-hardware-smoke".to_owned(),
    }
}
