use anyhow::{bail, Result};
use chrono::{Duration, NaiveDateTime};
use gridedge_t::{
    decision::algorithm_from_config,
    domain::{Direction, Order, OrderIntent, OrderStatus},
    event::{current_event_schema, EventEnvelope, EventType},
    journal::SqliteStore,
    service::GridAutomationService,
    ths_sim::{
        SimulatedOrderDraft, SimulationFillTable, SimulationFillTableRow, SimulationOrderTable,
        SimulationOrderTableRow,
    },
    ths_sim_execution::{
        cancel_submitted_contract, cancel_submitted_intent, execute_staged_intent,
        process_outbox_once, run_automation_cycle, SimulationUiDriver,
    },
    ths_sim_outbox::{
        StagedCancelState, StagedIntentState, StagedTerminalResolution, ThsSimOutbox,
    },
    Config,
};
use rust_decimal::Decimal;
use serde_json::json;
use std::{collections::VecDeque, process::Command};
use tempfile::tempdir;

const SOURCE: &str = "0123456789abcdef0123456789abcdef";

#[derive(Debug, Default)]
struct FakeUiDriver {
    order_tables: VecDeque<SimulationOrderTable>,
    fill_tables: VecDeque<SimulationFillTable>,
    order_calls: usize,
    fill_calls: usize,
    prepare_calls: usize,
    submit_calls: usize,
    submit_fails: bool,
    cancel_calls: usize,
    cancelled_contract_ids: Vec<String>,
    cancel_fails: bool,
}

impl SimulationUiDriver for FakeUiDriver {
    fn orders(&mut self) -> Result<SimulationOrderTable> {
        self.order_calls += 1;
        self.order_tables
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("unexpected order-table probe"))
    }

    fn fills(&mut self) -> Result<SimulationFillTable> {
        self.fill_calls += 1;
        self.fill_tables
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("unexpected fill-table probe"))
    }

    fn prepare(&mut self, _order: &SimulatedOrderDraft) -> Result<()> {
        self.prepare_calls += 1;
        Ok(())
    }

    fn submit_once(&mut self, _order: &SimulatedOrderDraft) -> Result<()> {
        self.submit_calls += 1;
        if self.submit_fails {
            bail!("simulated UI timeout after the final click");
        }
        Ok(())
    }

    fn cancel_contract_once(&mut self, contract_id: &str) -> Result<()> {
        self.cancel_calls += 1;
        self.cancelled_contract_ids.push(contract_id.to_owned());
        if self.cancel_fails {
            bail!("simulated UI timeout after the cancel click");
        }
        Ok(())
    }
}

#[test]
fn full_fill_evidence_resolves_an_ambiguous_cancel_without_another_money_action() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("filled-after-cancel-ambiguity.db");
    let mut outbox = submitted_outbox_with_quantity(&path, "6210000001", 27_500)?;
    let staged = outbox.staged_intents()?[0].clone();
    outbox.begin_cancellation(&staged.intent_id, &staged.request_sha256, "6210000001")?;
    outbox.mark_cancellation_ambiguous(
        &staged.intent_id,
        &staged.request_sha256,
        "6210000001",
        "contract disappeared from the cancellable-order table",
        None,
    )?;
    assert_eq!(outbox.status()?.cancel_ambiguous, 1);

    let fills = (0..11)
        .map(|index| {
            let price = if index < 5 { "3.37" } else { "3.38" };
            fill_row(&format!("fill-{index:02}"), "6210000001", 2_500, price)
        })
        .collect::<Vec<_>>();
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[filled_order_row_with_quantity(
            "6210000001",
            27_500,
            "3.370",
        )])]),
        fill_tables: VecDeque::from([fill_table(&fills)]),
        ..FakeUiDriver::default()
    };

    let receipt = process_outbox_once(
        &mut outbox,
        time("2026-08-19 10:40:00"),
        Duration::minutes(5),
        &mut driver,
    )?;
    assert!(receipt.executed.is_empty());
    assert!(receipt.cancelled.is_empty());
    assert_eq!(driver.submit_calls, 0);
    assert_eq!(driver.cancel_calls, 0);
    assert_eq!(driver.order_calls, 1);
    assert_eq!(driver.fill_calls, 1);
    let resolved = outbox.staged_intents()?[0].clone();
    assert_eq!(
        resolved.terminal_resolution,
        StagedTerminalResolution::Filled
    );
    assert_eq!(resolved.cancel_state, StagedCancelState::Ambiguous);
    assert!(resolved.terminal_evidence_json.is_some());
    assert_eq!(outbox.status()?.cancel_ambiguous, 0);

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut no_ui = FakeUiDriver::default();
    process_outbox_once(
        &mut reopened,
        time("2026-08-19 10:41:00"),
        Duration::minutes(5),
        &mut no_ui,
    )?;
    assert_eq!(no_ui.order_calls, 0);
    assert_eq!(no_ui.fill_calls, 0);
    assert_eq!(no_ui.prepare_calls, 0);
    assert_eq!(no_ui.submit_calls, 0);
    assert_eq!(no_ui.cancel_calls, 0);
    assert_eq!(
        reopened.staged_intents()?[0].terminal_resolution,
        StagedTerminalResolution::Filled
    );
    Ok(())
}

#[test]
fn remote_fill_recovery_rejects_inconsistent_partial_cross_contract_or_duplicate_facts(
) -> Result<()> {
    let cases = [
        (
            "order-price-outside-fill-range",
            "3.50",
            vec![
                fill_row("fill-01", "contract-a", 700, "3.51"),
                fill_row("fill-02", "contract-a", 800, "3.51"),
            ],
        ),
        (
            "partial",
            "3.51",
            vec![fill_row("fill-01", "contract-a", 700, "3.51")],
        ),
        (
            "overfill",
            "3.51",
            vec![
                fill_row("fill-01", "contract-a", 700, "3.51"),
                fill_row("fill-02", "contract-a", 900, "3.51"),
            ],
        ),
        (
            "duplicate-fill-id",
            "3.51",
            vec![
                fill_row("fill-01", "contract-a", 700, "3.51"),
                fill_row("fill-01", "contract-a", 800, "3.51"),
            ],
        ),
        (
            "cross-contract",
            "3.51",
            vec![
                fill_row("fill-01", "contract-a", 700, "3.51"),
                fill_row("fill-02", "other-contract", 800, "3.51"),
            ],
        ),
    ];
    for (name, order_average, fills) in cases {
        let directory = tempdir()?;
        let path = directory.path().join(format!("{name}.db"));
        let mut outbox = ambiguous_cancel_outbox(&path, "contract-a", 1_500)?;
        let mut driver = FakeUiDriver {
            order_tables: VecDeque::from([table(&[filled_order_row_with_quantity(
                "contract-a",
                1_500,
                order_average,
            )])]),
            fill_tables: VecDeque::from([fill_table(&fills)]),
            ..FakeUiDriver::default()
        };

        assert!(
            process_outbox_once(
                &mut outbox,
                time("2026-08-19 10:40:00"),
                Duration::minutes(5),
                &mut driver,
            )
            .is_err(),
            "{name} must not resolve the durable ambiguity"
        );
        assert_eq!(driver.prepare_calls, 0, "{name}");
        assert_eq!(driver.submit_calls, 0, "{name}");
        assert_eq!(driver.cancel_calls, 0, "{name}");
        assert_eq!(
            outbox.staged_intents()?[0].terminal_resolution,
            StagedTerminalResolution::Unresolved,
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn noneligible_or_stale_durable_intents_never_reach_any_ui_action() -> Result<()> {
    let directory = tempdir()?;
    let eligible_path = directory.path().join("eligible.db");
    let mut eligible = eligible_outbox(&eligible_path)?;
    let mut stale_driver = FakeUiDriver::default();
    assert!(execute_staged_intent(
        &mut eligible,
        "intent-a",
        time("2026-08-17 09:46:00"),
        Duration::minutes(5),
        &mut stale_driver,
    )
    .is_err());
    assert_eq!(stale_driver.order_calls, 0);
    assert_eq!(stale_driver.prepare_calls, 0);
    assert_eq!(stale_driver.submit_calls, 0);
    assert_eq!(
        eligible.staged_intents()?[0].state,
        StagedIntentState::Eligible
    );

    let discovered_path = directory.path().join("discovered.db");
    let mut discovered = ThsSimOutbox::open(&discovered_path)?;
    discovered.bind_or_verify(SOURCE, "run-a", Some(0))?;
    discovered.ingest(&[intent_event(1, "intent-a", "order-a")])?;
    let mut discovered_driver = FakeUiDriver::default();
    assert!(execute_staged_intent(
        &mut discovered,
        "intent-a",
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut discovered_driver,
    )
    .is_err());
    assert_eq!(discovered_driver.order_calls, 0);
    assert_eq!(discovered_driver.prepare_calls, 0);
    assert_eq!(discovered_driver.submit_calls, 0);
    assert_eq!(
        discovered.staged_intents()?[0].state,
        StagedIntentState::Discovered
    );
    Ok(())
}

#[test]
fn execution_cli_accepts_only_a_durable_outbox_intent_identity() -> Result<()> {
    let binary = env!("CARGO_BIN_EXE_gridedge_ths_sim");
    for command in ["execute-intent", "cancel-intent"] {
        let output = Command::new(binary).args([command, "--help"]).output()?;
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout)?;
        assert!(help.contains("--outbox"));
        assert!(help.contains("--intent-id"));
        for forbidden in [
            "--direction",
            "--symbol",
            "--price",
            "--quantity",
            "--contract-id",
        ] {
            assert!(
                !help.contains(forbidden),
                "{command} exposes unsafe ad-hoc argument {forbidden}"
            );
        }
    }

    let output = Command::new(binary)
        .args(["cancel-contract", "--help"])
        .output()?;
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)?;
    assert!(help.contains("--outbox"));
    assert!(help.contains("--contract-id"));
    for forbidden in [
        "--intent-id",
        "--direction",
        "--symbol",
        "--price",
        "--quantity",
    ] {
        assert!(
            !help.contains(forbidden),
            "cancel-contract exposes unsafe ad-hoc argument {forbidden}"
        );
    }

    let output = Command::new(binary)
        .args(["run-worker", "--help"])
        .output()?;
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)?;
    for required in ["--database", "--run-id", "--outbox", "--after-sequence"] {
        assert!(help.contains(required));
    }
    for forbidden in [
        "--intent-id",
        "--contract-id",
        "--direction",
        "--symbol",
        "--price",
        "--quantity",
    ] {
        assert!(
            !help.contains(forbidden),
            "run-worker exposes unsafe ad-hoc argument {forbidden}"
        );
    }
    Ok(())
}

#[test]
fn automation_cycle_submits_then_exactly_cancels_a_source_requested_order() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = eligible_outbox(&path)?;
    outbox.ingest(&[cancel_requested_event(
        3,
        "order-a",
        "source strategy cancelled the order",
    )])?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[]),
            table(&[order_row("new-contract")]),
            table(&[order_row("new-contract")]),
            table(&[cancelled_order_row("new-contract")]),
        ]),
        ..FakeUiDriver::default()
    };

    let receipt = process_outbox_once(
        &mut outbox,
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )?;

    assert_eq!(receipt.executed.len(), 1);
    assert_eq!(receipt.cancelled.len(), 1);
    assert_eq!(
        receipt.cancelled[0].remote_order.contract_id,
        "new-contract"
    );
    assert_eq!(driver.submit_calls, 1);
    assert_eq!(driver.cancel_calls, 1);
    assert_eq!(driver.cancelled_contract_ids, ["new-contract"]);
    assert_eq!(receipt.status.cancelled, 1);
    assert_eq!(receipt.status.cancel_requested, 0);

    let mut no_ui = FakeUiDriver::default();
    let replay = process_outbox_once(
        &mut outbox,
        time("2026-08-17 09:36:30"),
        Duration::minutes(5),
        &mut no_ui,
    )?;
    assert!(replay.executed.is_empty());
    assert!(replay.cancelled.is_empty());
    assert_eq!(no_ui.order_calls, 0);
    assert_eq!(no_ui.submit_calls, 0);
    assert_eq!(no_ui.cancel_calls, 0);
    Ok(())
}

#[test]
fn automation_cycle_restart_uses_the_persisted_outbox_cursor_without_rescanning_or_ui() -> Result<()>
{
    let directory = tempdir()?;
    let source_path = directory.path().join("source.db");
    let outbox_path = directory.path().join("outbox.db");
    let mut config = Config::load("configs/default.yaml")?;
    config.database = source_path.display().to_string();
    let service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some("ths-live-restart".to_owned()),
    )?;
    drop(service);

    let mut first_driver = FakeUiDriver::default();
    let first = run_automation_cycle(
        &source_path,
        "ths-live-restart",
        &outbox_path,
        Some(0),
        time("2026-08-18 09:31:00"),
        Duration::minutes(5),
        &mut first_driver,
    )?;
    assert!(first.source_cursor > 0);
    assert!(first.executed.is_empty());
    assert!(first.cancelled.is_empty());
    assert_eq!(first_driver.order_calls, 0);

    let reopened = ThsSimOutbox::open(&outbox_path)?;
    assert!(reopened.has_binding()?);
    drop(reopened);
    let mut restart_driver = FakeUiDriver::default();
    let restarted = run_automation_cycle(
        &source_path,
        "ths-live-restart",
        &outbox_path,
        None,
        time("2026-08-18 09:31:30"),
        Duration::minutes(5),
        &mut restart_driver,
    )?;
    assert_eq!(restarted.source_cursor, first.source_cursor);
    assert!(restarted.executed.is_empty());
    assert!(restarted.cancelled.is_empty());
    assert_eq!(restart_driver.order_calls, 0);
    assert_eq!(restart_driver.prepare_calls, 0);
    assert_eq!(restart_driver.submit_calls, 0);
    assert_eq!(restart_driver.cancel_calls, 0);

    let mut wrong_boundary_driver = FakeUiDriver::default();
    assert!(run_automation_cycle(
        &source_path,
        "ths-live-restart",
        &outbox_path,
        Some(0),
        time("2026-08-18 09:32:00"),
        Duration::minutes(5),
        &mut wrong_boundary_driver,
    )
    .is_err());
    assert_eq!(wrong_boundary_driver.order_calls, 0);
    assert_eq!(wrong_boundary_driver.submit_calls, 0);
    assert_eq!(wrong_boundary_driver.cancel_calls, 0);
    Ok(())
}

#[test]
fn automation_cycle_refuses_all_ui_work_when_any_operation_is_ambiguous() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = eligible_outbox(&path)?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;
    outbox.mark_ambiguous("intent-a", &request_sha, "operator review required", None)?;
    let mut driver = FakeUiDriver::default();

    assert!(process_outbox_once(
        &mut outbox,
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )
    .is_err());
    assert_eq!(driver.order_calls, 0);
    assert_eq!(driver.prepare_calls, 0);
    assert_eq!(driver.submit_calls, 0);
    assert_eq!(driver.cancel_calls, 0);
    Ok(())
}

#[test]
fn ambiguous_cancellation_allows_only_read_only_recovery_before_blocking_other_work() -> Result<()>
{
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "contract-a")?;
    outbox.ingest(&[
        intent_event(3, "intent-b", "order-b"),
        submitted_event(4, "order-b"),
    ])?;

    let first = outbox
        .staged_intents()?
        .into_iter()
        .find(|intent| intent.intent_id == "intent-a")
        .expect("submitted first intent");
    outbox.begin_cancellation(&first.intent_id, &first.request_sha256, "contract-a")?;
    outbox.mark_cancellation_ambiguous(
        &first.intent_id,
        &first.request_sha256,
        "contract-a",
        "operator review required",
        None,
    )?;
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[order_row("contract-a")])]),
        fill_tables: VecDeque::from([fill_table(&[])]),
        ..FakeUiDriver::default()
    };
    assert!(process_outbox_once(
        &mut reopened,
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )
    .is_err());
    assert_eq!(driver.order_calls, 1);
    assert_eq!(driver.fill_calls, 1);
    assert_eq!(driver.prepare_calls, 0);
    assert_eq!(driver.submit_calls, 0);
    assert_eq!(driver.cancel_calls, 0);
    let second = reopened
        .staged_intents()?
        .into_iter()
        .find(|intent| intent.intent_id == "intent-b")
        .expect("eligible second intent");
    assert_eq!(second.state, StagedIntentState::Eligible);
    assert_eq!(second.cancel_state, StagedCancelState::NotRequested);
    let first = reopened
        .staged_intents()?
        .into_iter()
        .find(|intent| intent.intent_id == "intent-a")
        .expect("ambiguous first intent");
    assert_eq!(
        first.terminal_resolution,
        StagedTerminalResolution::Unresolved
    );
    Ok(())
}

#[test]
fn worker_restart_reconciles_submitting_and_cancelling_without_a_second_click() -> Result<()> {
    let directory = tempdir()?;

    let submission_path = directory.path().join("submitting.db");
    let mut submitting = eligible_outbox(&submission_path)?;
    let staged = submitting.staged_intents()?[0].clone();
    submitting.begin_submission(
        &staged.intent_id,
        &staged.request_sha256,
        &["manual-old".to_owned()],
    )?;
    drop(submitting);
    let mut submitting = ThsSimOutbox::open(&submission_path)?;
    let mut submit_driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[order_row("manual-old"), order_row("contract-a")])]),
        ..FakeUiDriver::default()
    };
    let submit_receipt = process_outbox_once(
        &mut submitting,
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut submit_driver,
    )?;
    assert_eq!(submit_receipt.executed.len(), 1);
    assert!(!submit_receipt.executed[0].clicked_in_this_call);
    assert_eq!(submit_driver.prepare_calls, 0);
    assert_eq!(submit_driver.submit_calls, 0);

    let cancellation_path = directory.path().join("cancelling.db");
    let mut cancelling = submitted_outbox(&cancellation_path, "contract-b")?;
    cancelling.ingest(&[cancel_requested_event(
        3,
        "order-a",
        "source strategy cancelled the order",
    )])?;
    let staged = cancelling.staged_intents()?[0].clone();
    cancelling.begin_cancellation(&staged.intent_id, &staged.request_sha256, "contract-b")?;
    drop(cancelling);
    let mut cancelling = ThsSimOutbox::open(&cancellation_path)?;
    let mut cancel_driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[cancelled_order_row("contract-b")])]),
        ..FakeUiDriver::default()
    };
    let cancel_receipt = process_outbox_once(
        &mut cancelling,
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut cancel_driver,
    )?;
    assert_eq!(cancel_receipt.cancelled.len(), 1);
    assert!(!cancel_receipt.cancelled[0].clicked_in_this_call);
    assert_eq!(cancel_driver.cancel_calls, 0);
    Ok(())
}

#[test]
fn eligible_intent_clicks_once_and_ignores_an_identical_preexisting_manual_order() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = eligible_outbox(&path)?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[order_row("manual-old")]),
            table(&[order_row("manual-old"), order_row("new-contract")]),
        ]),
        ..FakeUiDriver::default()
    };

    let receipt = execute_staged_intent(
        &mut outbox,
        "intent-a",
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )?;

    assert!(receipt.clicked_in_this_call);
    assert_eq!(receipt.remote_order.contract_id, "new-contract");
    assert_eq!(driver.prepare_calls, 1);
    assert_eq!(driver.submit_calls, 1);
    assert_eq!(outbox.status()?.submitted, 1);

    let mut no_ui_allowed = FakeUiDriver::default();
    let replay = execute_staged_intent(
        &mut outbox,
        "intent-a",
        time("2026-08-17 09:37:00"),
        Duration::minutes(5),
        &mut no_ui_allowed,
    )?;
    assert!(!replay.clicked_in_this_call);
    assert_eq!(replay.remote_order, receipt.remote_order);
    assert_eq!(no_ui_allowed.prepare_calls, 0);
    assert_eq!(no_ui_allowed.submit_calls, 0);
    Ok(())
}

#[test]
fn timeout_after_claim_becomes_terminal_ambiguous_and_never_clicks_again() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = eligible_outbox(&path)?;
    let mut failing = FakeUiDriver {
        order_tables: VecDeque::from([table(&[])]),
        submit_fails: true,
        ..FakeUiDriver::default()
    };

    assert!(execute_staged_intent(
        &mut outbox,
        "intent-a",
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut failing,
    )
    .is_err());
    assert_eq!(failing.submit_calls, 1);
    assert_eq!(outbox.status()?.ambiguous, 1);

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut second = FakeUiDriver::default();
    assert!(execute_staged_intent(
        &mut reopened,
        "intent-a",
        time("2026-08-17 09:36:30"),
        Duration::minutes(5),
        &mut second,
    )
    .is_err());
    assert_eq!(second.prepare_calls, 0);
    assert_eq!(second.submit_calls, 0);
    Ok(())
}

#[test]
fn restart_in_submitting_state_only_reconciles_and_never_reclicks() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = eligible_outbox(&path)?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &["manual-old".to_owned()])?;
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[
            order_row("manual-old"),
            order_row("new-contract"),
        ])]),
        ..FakeUiDriver::default()
    };
    let receipt = execute_staged_intent(
        &mut reopened,
        "intent-a",
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )?;

    assert!(!receipt.clicked_in_this_call);
    assert_eq!(receipt.remote_order.contract_id, "new-contract");
    assert_eq!(driver.prepare_calls, 0);
    assert_eq!(driver.submit_calls, 0);
    assert_eq!(reopened.status()?.submitted, 1);
    Ok(())
}

#[test]
fn zero_or_multiple_new_matches_become_ambiguous_without_a_second_click() -> Result<()> {
    for post_submit in [table(&[]), table(&[order_row("new-a"), order_row("new-b")])] {
        let directory = tempdir()?;
        let path = directory.path().join("outbox.db");
        let mut outbox = eligible_outbox(&path)?;
        let mut driver = FakeUiDriver {
            order_tables: VecDeque::from([table(&[]), post_submit]),
            ..FakeUiDriver::default()
        };

        assert!(execute_staged_intent(
            &mut outbox,
            "intent-a",
            time("2026-08-17 09:36:00"),
            Duration::minutes(5),
            &mut driver,
        )
        .is_err());
        assert_eq!(driver.submit_calls, 1);
        assert_eq!(outbox.status()?.ambiguous, 1);
        assert_eq!(
            outbox.staged_intents()?[0].state,
            StagedIntentState::Ambiguous
        );
    }
    Ok(())
}

#[test]
fn submitted_contract_is_cancelled_once_and_restart_returns_the_same_evidence() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "new-contract")?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[order_row("new-contract")]),
            table(&[cancelled_order_row("new-contract")]),
        ]),
        ..FakeUiDriver::default()
    };

    let receipt = cancel_submitted_intent(&mut outbox, "intent-a", &mut driver)?;

    assert!(receipt.clicked_in_this_call);
    assert_eq!(receipt.remote_order.contract_id, "new-contract");
    assert_eq!(receipt.remote_order.cancelled_quantity, 1500);
    assert_eq!(driver.cancel_calls, 1);
    assert_eq!(driver.cancelled_contract_ids, ["new-contract"]);
    assert_eq!(
        outbox.staged_intents()?[0].cancel_state,
        StagedCancelState::Cancelled
    );

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut no_ui_allowed = FakeUiDriver::default();
    let replay = cancel_submitted_intent(&mut reopened, "intent-a", &mut no_ui_allowed)?;
    assert!(!replay.clicked_in_this_call);
    assert_eq!(replay.remote_order, receipt.remote_order);
    assert_eq!(no_ui_allowed.cancel_calls, 0);
    Ok(())
}

#[test]
fn contract_id_interface_only_cancels_the_uniquely_bound_durable_contract() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "new-contract")?;

    let mut unknown = FakeUiDriver::default();
    assert!(cancel_submitted_contract(&mut outbox, "manual-other", &mut unknown).is_err());
    assert_eq!(unknown.order_calls, 0);
    assert_eq!(unknown.cancel_calls, 0);
    assert_eq!(
        outbox.staged_intents()?[0].cancel_state,
        StagedCancelState::NotRequested
    );

    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[order_row("manual-other"), order_row("new-contract")]),
            table(&[
                order_row("manual-other"),
                cancelled_order_row("new-contract"),
            ]),
        ]),
        ..FakeUiDriver::default()
    };
    let receipt = cancel_submitted_contract(&mut outbox, "new-contract", &mut driver)?;
    assert!(receipt.clicked_in_this_call);
    assert_eq!(receipt.intent_id, "intent-a");
    assert_eq!(receipt.remote_order.contract_id, "new-contract");
    assert_eq!(driver.cancelled_contract_ids, ["new-contract"]);

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut no_ui_allowed = FakeUiDriver::default();
    let replay = cancel_submitted_contract(&mut reopened, "new-contract", &mut no_ui_allowed)?;
    assert!(!replay.clicked_in_this_call);
    assert_eq!(replay.remote_order, receipt.remote_order);
    assert_eq!(no_ui_allowed.order_calls, 0);
    assert_eq!(no_ui_allowed.cancel_calls, 0);
    Ok(())
}

#[test]
fn cancel_timeout_is_terminal_ambiguous_and_restart_never_recancels() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "new-contract")?;
    let mut failing = FakeUiDriver {
        order_tables: VecDeque::from([table(&[order_row("new-contract")])]),
        cancel_fails: true,
        ..FakeUiDriver::default()
    };

    assert!(cancel_submitted_intent(&mut outbox, "intent-a", &mut failing).is_err());
    assert_eq!(failing.cancel_calls, 1);
    assert_eq!(
        outbox.staged_intents()?[0].cancel_state,
        StagedCancelState::Ambiguous
    );

    drop(outbox);
    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut second = FakeUiDriver::default();
    assert!(cancel_submitted_contract(&mut reopened, "new-contract", &mut second).is_err());
    assert_eq!(second.cancel_calls, 0);
    Ok(())
}

#[test]
fn restart_while_cancelling_only_reconciles_and_never_clicks_again() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "new-contract")?;
    let staged = outbox.staged_intents()?[0].clone();
    outbox.begin_cancellation(&staged.intent_id, &staged.request_sha256, "new-contract")?;
    drop(outbox);

    let mut reopened = ThsSimOutbox::open(&path)?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([table(&[cancelled_order_row("new-contract")])]),
        ..FakeUiDriver::default()
    };
    let receipt = cancel_submitted_contract(&mut reopened, "new-contract", &mut driver)?;

    assert!(!receipt.clicked_in_this_call);
    assert_eq!(driver.cancel_calls, 0);
    assert_eq!(
        reopened.staged_intents()?[0].cancel_state,
        StagedCancelState::Cancelled
    );
    Ok(())
}

#[test]
fn partial_or_wrong_contract_cancel_evidence_fails_closed() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("outbox.db");
    let mut outbox = submitted_outbox(&path, "new-contract")?;
    let mut wrong = FakeUiDriver {
        order_tables: VecDeque::from([table(&[order_row("other-contract")])]),
        ..FakeUiDriver::default()
    };
    assert!(cancel_submitted_intent(&mut outbox, "intent-a", &mut wrong).is_err());
    assert_eq!(wrong.cancel_calls, 0);
    assert_eq!(
        outbox.staged_intents()?[0].cancel_state,
        StagedCancelState::NotRequested
    );

    let mut partial = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[order_row("new-contract")]),
            table(&[SimulationOrderTableRow {
                cells: [
                    "20260817",
                    "093600",
                    "002256",
                    "兆新股份",
                    "买入",
                    "部分撤单",
                    "1500",
                    "700",
                    "3.55",
                    "3.55",
                    "new-contract",
                    "限价委托",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            }]),
        ]),
        ..FakeUiDriver::default()
    };
    assert!(cancel_submitted_contract(&mut outbox, "new-contract", &mut partial).is_err());
    assert_eq!(partial.cancel_calls, 1);
    assert_eq!(
        outbox.staged_intents()?[0].cancel_state,
        StagedCancelState::Ambiguous
    );
    Ok(())
}

fn eligible_outbox(path: &std::path::Path) -> Result<ThsSimOutbox> {
    let mut outbox = ThsSimOutbox::open(path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a"),
        submitted_event(2, "order-a"),
    ])?;
    Ok(outbox)
}

fn submitted_outbox(path: &std::path::Path, contract_id: &str) -> Result<ThsSimOutbox> {
    submitted_outbox_with_quantity(path, contract_id, 1_500)
}

fn submitted_outbox_with_quantity(
    path: &std::path::Path,
    contract_id: &str,
    quantity: i64,
) -> Result<ThsSimOutbox> {
    let mut outbox = ThsSimOutbox::open(path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event_with_quantity(1, "intent-a", "order-a", quantity),
        submitted_event(2, "order-a"),
    ])?;
    let mut driver = FakeUiDriver {
        order_tables: VecDeque::from([
            table(&[]),
            table(&[order_row_with_quantity(contract_id, quantity)]),
        ]),
        ..FakeUiDriver::default()
    };
    execute_staged_intent(
        &mut outbox,
        "intent-a",
        time("2026-08-17 09:36:00"),
        Duration::minutes(5),
        &mut driver,
    )?;
    Ok(outbox)
}

fn ambiguous_cancel_outbox(
    path: &std::path::Path,
    contract_id: &str,
    quantity: i64,
) -> Result<ThsSimOutbox> {
    let mut outbox = submitted_outbox_with_quantity(path, contract_id, quantity)?;
    let staged = outbox.staged_intents()?[0].clone();
    outbox.begin_cancellation(&staged.intent_id, &staged.request_sha256, contract_id)?;
    outbox.mark_cancellation_ambiguous(
        &staged.intent_id,
        &staged.request_sha256,
        contract_id,
        "contract disappeared from the cancellable-order table",
        None,
    )?;
    Ok(outbox)
}

fn fill_table(rows: &[SimulationFillTableRow]) -> SimulationFillTable {
    SimulationFillTable {
        headers: [
            "成交日期",
            "成交时间",
            "证券代码",
            "证券名称",
            "操作",
            "成交数量",
            "成交均价",
            "成交金额",
            "合同编号",
            "成交编号",
            "成交类别",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        rows: rows.to_vec(),
    }
}

fn fill_row(
    fill_id: &str,
    contract_id: &str,
    quantity: i64,
    price: &str,
) -> SimulationFillTableRow {
    let amount = price.parse::<Decimal>().unwrap() * Decimal::from(quantity);
    SimulationFillTableRow {
        cells: [
            "20260819".to_owned(),
            "103000".to_owned(),
            "002256".to_owned(),
            "兆新股份".to_owned(),
            "买入".to_owned(),
            quantity.to_string(),
            price.to_owned(),
            amount.to_string(),
            contract_id.to_owned(),
            fill_id.to_owned(),
            "普通成交".to_owned(),
        ]
        .to_vec(),
    }
}

fn table(rows: &[SimulationOrderTableRow]) -> SimulationOrderTable {
    SimulationOrderTable {
        headers: [
            "委托日期",
            "委托时间",
            "证券代码",
            "证券名称",
            "操作",
            "备注",
            "委托数量",
            "撤销数量",
            "委托价格",
            "成交价格",
            "合同编号",
            "委托属性",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        rows: rows.to_vec(),
    }
}

fn order_row(contract_id: &str) -> SimulationOrderTableRow {
    order_row_with_quantity(contract_id, 1_500)
}

fn order_row_with_quantity(contract_id: &str, quantity: i64) -> SimulationOrderTableRow {
    SimulationOrderTableRow {
        cells: [
            "20260817".to_owned(),
            "093600".to_owned(),
            "002256".to_owned(),
            "兆新股份".to_owned(),
            "买入".to_owned(),
            "未成交".to_owned(),
            quantity.to_string(),
            "0".to_owned(),
            "3.55".to_owned(),
            "0".to_owned(),
            contract_id.to_owned(),
            "限价委托".to_owned(),
        ]
        .to_vec(),
    }
}

fn filled_order_row_with_quantity(
    contract_id: &str,
    quantity: i64,
    fill_price: &str,
) -> SimulationOrderTableRow {
    let mut row = order_row_with_quantity(contract_id, quantity);
    row.cells[5] = "全部成交".to_owned();
    row.cells[9] = fill_price.to_owned();
    row
}

fn cancelled_order_row(contract_id: &str) -> SimulationOrderTableRow {
    let mut row = order_row(contract_id);
    row.cells[5] = "全部撤单".to_owned();
    row.cells[7] = "1500".to_owned();
    row
}

fn intent_event(sequence: i64, intent_id: &str, order_id: &str) -> EventEnvelope {
    intent_event_with_quantity(sequence, intent_id, order_id, 1_500)
}

fn intent_event_with_quantity(
    sequence: i64,
    intent_id: &str,
    order_id: &str,
    quantity: i64,
) -> EventEnvelope {
    let event_time = time("2026-08-17 09:35:00");
    let order = Order {
        order_id: order_id.to_owned(),
        intent: OrderIntent {
            intent_id: intent_id.to_owned(),
            origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
            right_id: "right-a".to_owned(),
            grid_index: -1,
            direction: Direction::Buy,
            limit_price: Decimal::new(355, 2),
            quantity,
            budget: Decimal::new(355, 2) * Decimal::from(quantity),
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
    envelope(
        sequence,
        EventType::OrderIntentCreated,
        serde_json::to_value(order).unwrap(),
        event_time,
    )
}

fn submitted_event(sequence: i64, order_id: &str) -> EventEnvelope {
    envelope(
        sequence,
        EventType::OrderSubmitted,
        json!({"order_id": order_id}),
        time("2026-08-17 09:35:00"),
    )
}

fn cancel_requested_event(sequence: i64, order_id: &str, reason: &str) -> EventEnvelope {
    envelope(
        sequence,
        EventType::OrderCancelRequested,
        json!({"order_id": order_id, "reason": reason}),
        time("2026-08-17 09:35:00"),
    )
}

fn envelope(
    sequence: i64,
    event_type: EventType,
    payload: serde_json::Value,
    event_time: NaiveDateTime,
) -> EventEnvelope {
    EventEnvelope {
        event_id: format!("event-{sequence}"),
        event_type,
        schema_version: current_event_schema(event_type),
        run_id: "run-a".to_owned(),
        cycle_id: "cycle-a".to_owned(),
        symbol: "002256.SZ".to_owned(),
        event_time,
        recorded_at: event_time,
        sequence_number: sequence,
        correlation_id: format!("correlation-{sequence}"),
        causation_id: None,
        idempotency_key: format!("key-{sequence}"),
        payload,
        config_version: "test".to_owned(),
    }
}

fn time(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}
