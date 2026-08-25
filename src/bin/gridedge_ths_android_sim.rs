use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gridedge_t::{
    domain::Direction,
    ths_android_sim::{AndroidThsConfig, AndroidThsSimulationUiDriver},
    ths_sim::SimulatedOrderDraft,
    ths_sim_execution::SimulationUiDriver,
};
use rust_decimal::Decimal;
use std::{fs, path::PathBuf, str::FromStr, thread, time::Duration};

#[derive(Debug, Parser)]
#[command(about = "Fail-closed Android Tonghuashun simulation-account adapter probe")]
struct Cli {
    #[arg(long, default_value = "emulator-5554")]
    serial: String,
    #[arg(long, default_value = "THS")]
    avd_marker: String,
    #[arg(long, default_value = "**0000")]
    masked_account: String,
    #[arg(long, default_value = "002256")]
    symbol: String,
    #[arg(long, default_value = "兆新股份")]
    security_name: String,
    #[arg(long)]
    confirmation_account_sha256_file: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    enable_money_actions: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify the emulator, package, version and simulation-account UI identity.
    Probe,
    /// Read and normalize the current-day simulation order table.
    Orders,
    /// Read and normalize the current-day simulation fill table.
    Fills,
    /// Read stable IDs from only the top-level cancel tab's cancellable region.
    Cancellable,
    /// Fill and verify the simulation form without submitting an order.
    Prepare {
        #[arg(long)]
        direction: String,
        #[arg(long)]
        price: String,
        #[arg(long)]
        quantity: i64,
    },
    /// Cancel one exact synthetic Android contract through the top Cancel tab.
    Cancel {
        #[arg(long)]
        contract_id: String,
    },
    /// Submit and cancel one exact simulation order as a hardware gate.
    SubmitCancelSmoke {
        #[arg(long)]
        direction: String,
        #[arg(long)]
        price: String,
        #[arg(long, default_value_t = 100)]
        quantity: i64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = AndroidThsConfig {
        serial: cli.serial,
        avd_name_marker: cli.avd_marker,
        masked_account: cli.masked_account,
        expected_symbol: cli.symbol,
        expected_security_name: cli.security_name,
        confirmation_account_sha256: cli
            .confirmation_account_sha256_file
            .as_ref()
            .map(|path| fs::read_to_string(path).map(|value| value.trim().to_owned()))
            .transpose()?,
        money_actions_enabled: cli.enable_money_actions,
        ..AndroidThsConfig::default()
    };
    let mut driver = AndroidThsSimulationUiDriver::new(config.clone())?;
    match cli.command {
        Command::Probe => println!(
            "{}",
            serde_json::to_string_pretty(&driver.probe_identity()?)?
        ),
        Command::Orders => println!("{}", serde_json::to_string_pretty(&driver.orders()?)?),
        Command::Fills => {
            driver.orders()?;
            println!("{}", serde_json::to_string_pretty(&driver.fills()?)?)
        }
        Command::Cancellable => println!(
            "{}",
            serde_json::to_string_pretty(&driver.probe_cancellable_contract_ids()?)?
        ),
        Command::Prepare {
            direction,
            price,
            quantity,
        } => {
            let direction = match direction.as_str() {
                "buy" => Direction::Buy,
                "sell" => Direction::Sell,
                _ => anyhow::bail!("direction must be buy or sell"),
            };
            driver.prepare(&SimulatedOrderDraft {
                direction,
                symbol: config.expected_symbol,
                limit_price: Decimal::from_str(&price)?,
                quantity,
            })?;
            println!("PREPARED_NOT_SUBMITTED");
        }
        Command::Cancel { contract_id } => {
            driver.cancel_contract_once(&contract_id)?;
            println!("CANCEL_CLICKED_ONCE");
        }
        Command::SubmitCancelSmoke {
            direction,
            price,
            quantity,
        } => {
            let direction = match direction.as_str() {
                "buy" => Direction::Buy,
                "sell" => Direction::Sell,
                _ => anyhow::bail!("direction must be buy or sell"),
            };
            let order = SimulatedOrderDraft {
                direction,
                symbol: config.expected_symbol,
                limit_price: Decimal::from_str(&price)?,
                quantity,
            };
            let baseline = driver.orders()?.records()?;
            let baseline_ids = baseline
                .iter()
                .map(|record| record.contract_id.clone())
                .collect::<Vec<_>>();
            driver.prepare(&order)?;
            driver.submit_once(&order)?;
            let mut submitted = None;
            for attempt in 0..5 {
                let table = driver.orders()?;
                if let Ok(record) = table.unique_new_match(&order, &baseline_ids) {
                    submitted = Some(record);
                    break;
                }
                if attempt < 4 {
                    thread::sleep(Duration::from_millis(500));
                }
            }
            let submitted =
                submitted.context("submitted Android simulation order did not appear")?;
            driver.cancel_contract_once(&submitted.contract_id)?;
            let mut terminal = false;
            for attempt in 0..5 {
                terminal = driver.orders()?.records()?.iter().any(|record| {
                    record.contract_id == submitted.contract_id
                        && record.cancelled_quantity > 0
                        && record.remark.contains('撤')
                });
                if terminal {
                    break;
                }
                if attempt < 4 {
                    thread::sleep(Duration::from_millis(500));
                }
            }
            if !terminal {
                anyhow::bail!(
                    "Android simulation smoke contract did not reach cancelled terminal state"
                );
            }
            println!("SUBMITTED_RECONCILED_CANCELLED {}", submitted.contract_id);
        }
    }
    Ok(())
}
