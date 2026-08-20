use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gridedge_t::ths_sim::{
    direction_from_cli, dry_run, prepare_current_form, probe_current_fills,
    probe_current_market_quote, probe_current_orders, probe_current_ui, SimulatedOrderDraft,
};
use gridedge_t::ths_sim_execution::{
    cancel_submitted_contract, cancel_submitted_intent, execute_staged_intent,
    run_automation_cycle, MacOsSimulationUiDriver,
};
use gridedge_t::ths_sim_outbox::{stage_from_source, ThsSimOutbox};
use rust_decimal::Decimal;
use std::{path::PathBuf, str::FromStr};

#[derive(Debug, Parser)]
#[command(
    name = "gridedge-ths-sim",
    about = "Fail-closed macOS Tonghuashun simulation UI probe"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Probe {
        #[arg(long)]
        json: bool,
    },
    QuoteProbe {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        json: bool,
    },
    DryRun {
        #[arg(long)]
        direction: String,
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        price: String,
        #[arg(long)]
        quantity: i64,
        #[arg(long)]
        json: bool,
    },
    PrepareForm {
        #[arg(long)]
        direction: String,
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        price: String,
        #[arg(long)]
        quantity: i64,
        #[arg(long)]
        json: bool,
    },
    StageIntents {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        after_sequence: Option<i64>,
        #[arg(long)]
        json: bool,
    },
    OutboxStatus {
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        json: bool,
    },
    OrdersProbe {
        #[arg(long)]
        json: bool,
    },
    FillsProbe {
        #[arg(long)]
        json: bool,
    },
    ExecuteIntent {
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        intent_id: String,
        #[arg(long, default_value_t = 300)]
        maximum_age_seconds: i64,
        #[arg(long)]
        json: bool,
    },
    CancelIntent {
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        intent_id: String,
        #[arg(long)]
        json: bool,
    },
    CancelContract {
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        contract_id: String,
        #[arg(long)]
        json: bool,
    },
    RunWorker {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        outbox: PathBuf,
        #[arg(long)]
        after_sequence: Option<i64>,
        #[arg(long, default_value_t = 300)]
        maximum_age_seconds: i64,
        #[arg(long, default_value_t = 2)]
        poll_interval_seconds: u64,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        json: bool,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Probe { json } => {
            let verified = probe_current_ui()?.verify_simulation()?;
            if json {
                println!("{}", serde_json::to_string_pretty(&verified)?);
            } else {
                println!(
                    "verified Tonghuashun {} simulation UI: {} ({})",
                    verified.version, verified.marker, verified.submit_label
                );
            }
        }
        Command::QuoteProbe { symbol, json } => {
            let quote = probe_current_market_quote(&symbol)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&quote)?);
            } else {
                println!(
                    "verified Tonghuashun simulation quote: {} {} at {}",
                    quote.symbol, quote.last_price, quote.observed_at
                );
            }
        }
        Command::DryRun {
            direction,
            symbol,
            price,
            quantity,
            json,
        } => {
            let direction = direction_from_cli(&direction)?;
            let price = Decimal::from_str(&price).context("invalid decimal price")?;
            let order = SimulatedOrderDraft::new(direction, symbol, price, quantity)?;
            let plan = dry_run(probe_current_ui()?, order)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                println!(
                    "dry-run verified: {:?} {} shares of {} at {}; no UI value was changed and no order was submitted",
                    plan.order.direction,
                    plan.order.quantity,
                    plan.order.symbol,
                    plan.order.limit_price
                );
            }
        }
        Command::PrepareForm {
            direction,
            symbol,
            price,
            quantity,
            json,
        } => {
            let direction = direction_from_cli(&direction)?;
            let price = Decimal::from_str(&price).context("invalid decimal price")?;
            let order = SimulatedOrderDraft::new(direction, symbol, price, quantity)?;
            let prepared = prepare_current_form(order)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&prepared)?);
            } else {
                println!(
                    "prepared and re-read the {:?} simulation form for {} shares of {} at {}; no order was submitted",
                    prepared.order.direction,
                    prepared.order.quantity,
                    prepared.order.symbol,
                    prepared.order.limit_price
                );
            }
        }
        Command::StageIntents {
            database,
            run_id,
            outbox,
            after_sequence,
            json,
        } => {
            let status = stage_from_source(database, &run_id, outbox, after_sequence)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                println!(
                    "staged source through sequence {}: {} discovered, {} eligible",
                    status.cursor, status.discovered, status.eligible
                );
            }
        }
        Command::OutboxStatus { outbox, json } => {
            let outbox = ThsSimOutbox::open(outbox)?;
            let status = outbox.status()?;
            let intents = outbox.staged_intents()?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "status": status,
                        "intents": intents,
                    }))?
                );
            } else {
                println!(
                    "outbox at sequence {}: {} discovered, {} eligible",
                    status.cursor, status.discovered, status.eligible
                );
            }
        }
        Command::OrdersProbe { json } => {
            let table = probe_current_orders()?;
            if json {
                println!("{}", serde_json::to_string_pretty(&table)?);
            } else {
                println!(
                    "verified Tonghuashun simulation order table with {} rows",
                    table.rows.len()
                );
            }
        }
        Command::FillsProbe { json } => {
            let table = probe_current_fills()?;
            if json {
                println!("{}", serde_json::to_string_pretty(&table)?);
            } else {
                println!(
                    "verified Tonghuashun simulation fill table with {} rows",
                    table.rows.len()
                );
            }
        }
        Command::ExecuteIntent {
            outbox,
            intent_id,
            maximum_age_seconds,
            json,
        } => {
            if maximum_age_seconds <= 0 {
                anyhow::bail!("maximum-age-seconds must be positive");
            }
            let mut outbox = ThsSimOutbox::open(outbox)?;
            let mut driver = MacOsSimulationUiDriver::default();
            let receipt = execute_staged_intent(
                &mut outbox,
                &intent_id,
                chrono::Local::now().naive_local(),
                chrono::Duration::seconds(maximum_age_seconds),
                &mut driver,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&receipt)?);
            } else {
                println!(
                    "simulation intent {} reconciled to contract {} (clicked={})",
                    receipt.intent_id,
                    receipt.remote_order.contract_id,
                    receipt.clicked_in_this_call
                );
            }
        }
        Command::CancelIntent {
            outbox,
            intent_id,
            json,
        } => {
            let mut outbox = ThsSimOutbox::open(outbox)?;
            let mut driver = MacOsSimulationUiDriver::default();
            let receipt = cancel_submitted_intent(&mut outbox, &intent_id, &mut driver)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&receipt)?);
            } else {
                println!(
                    "simulation contract {} is fully cancelled (clicked={})",
                    receipt.remote_order.contract_id, receipt.clicked_in_this_call
                );
            }
        }
        Command::CancelContract {
            outbox,
            contract_id,
            json,
        } => {
            let mut outbox = ThsSimOutbox::open(outbox)?;
            let mut driver = MacOsSimulationUiDriver::default();
            let receipt = cancel_submitted_contract(&mut outbox, &contract_id, &mut driver)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&receipt)?);
            } else {
                println!(
                    "simulation contract {} is fully cancelled (intent={}, clicked={})",
                    receipt.remote_order.contract_id,
                    receipt.intent_id,
                    receipt.clicked_in_this_call
                );
            }
        }
        Command::RunWorker {
            database,
            run_id,
            outbox,
            mut after_sequence,
            maximum_age_seconds,
            poll_interval_seconds,
            once,
            json,
        } => {
            if !(1..=600).contains(&maximum_age_seconds) {
                anyhow::bail!("maximum-age-seconds must be between 1 and 600");
            }
            if !(1..=60).contains(&poll_interval_seconds) {
                anyhow::bail!("poll-interval-seconds must be between 1 and 60");
            }
            let mut driver = MacOsSimulationUiDriver::default();
            loop {
                let receipt = run_automation_cycle(
                    &database,
                    &run_id,
                    &outbox,
                    after_sequence.take(),
                    chrono::Local::now().naive_local(),
                    chrono::Duration::seconds(maximum_age_seconds),
                    &mut driver,
                )?;
                if json {
                    println!("{}", serde_json::to_string(&receipt)?);
                } else if !receipt.executed.is_empty() || !receipt.cancelled.is_empty() {
                    println!(
                        "simulation worker reached source sequence {}: {} submitted, {} cancelled",
                        receipt.source_cursor,
                        receipt.executed.len(),
                        receipt.cancelled.len()
                    );
                }
                if once {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_secs(poll_interval_seconds));
            }
        }
    }
    Ok(())
}
