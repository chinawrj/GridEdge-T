use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use clap::{Parser, Subcommand};
use gridedge_t::{
    config::Config,
    data::CsvReplayFeed,
    domain::StrategyState,
    gate,
    grid::GridSpec,
    journal::{EventReader, SqliteStore, StateReader},
    market_data::{
        default_paths, default_three_year_range, fetch_klines, Adjustment, FetchRequest,
    },
    service::{compare_states, GridAutomationService, ReplayDescriptor},
};
use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, path::Path, path::PathBuf, str::FromStr};

#[derive(Parser)]
#[command(
    name = "gridedge",
    version,
    about = "Auditable A-share grid research infrastructure"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    InitDb {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
    },
    ValidateConfig {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
    },
    ValidateData {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long)]
        data: PathBuf,
    },
    FetchKlines {
        #[arg(long, default_value = "002256.SZ")]
        symbol: String,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, default_value_t = 5)]
        frequency: u16,
        #[arg(long, default_value = "none")]
        adjustment: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        raw_output: Option<PathBuf>,
        #[arg(long)]
        report: Option<PathBuf>,
        #[arg(long, default_value_t = 3)]
        retries: u8,
        #[arg(long, default_value_t = 250)]
        rate_limit_ms: u64,
        #[arg(long)]
        json: bool,
    },
    GenerateGrid {
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Replay {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    RunPaper {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Status(QueryArgs),
    Ledger(QueryArgs),
    Orders(QueryArgs),
    Positions(QueryArgs),
    Reconcile(QueryArgs),
    Resume {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        json: bool,
    },
    RebuildState(QueryArgs),
    CompareBaseline(QueryArgs),
    Web {
        #[arg(long, default_value = "configs/default.yaml")]
        config: PathBuf,
        #[arg(long, default_value = "tests/fixtures/sample.csv")]
        data: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
}

#[derive(clap::Args, Clone)]
struct QueryArgs {
    #[arg(long, default_value = "configs/default.yaml")]
    config: PathBuf,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::InitDb { config } => {
            let config = Config::load(config)?;
            let mut store = SqliteStore::open(&config.database)?;
            store.migrate()?;
            println!("initialized SQLite database: {}", config.database);
        }
        Command::ValidateConfig { config } => {
            let config = Config::load(config)?;
            println!(
                "configuration valid: {} ({})",
                config.symbol, config.config_version
            );
        }
        Command::ValidateData { config, data } => {
            let config = Config::load(config)?;
            let feed = CsvReplayFeed::load(data, &config.symbol)?;
            println!(
                "market data valid: {} bars for {}",
                feed.bars().len(),
                config.symbol
            );
        }
        Command::FetchKlines {
            symbol,
            start,
            end,
            frequency,
            adjustment,
            output,
            raw_output,
            report,
            retries,
            rate_limit_ms,
            json,
        } => {
            let today = Local::now().date_naive();
            let (default_start, default_end) = default_three_year_range(today);
            let start = parse_date(start.as_deref(), default_start, "start")?;
            let end = parse_date(end.as_deref(), default_end, "end")?;
            let adjustment = Adjustment::from_str(&adjustment)?;
            let (default_output, default_raw, default_report) =
                default_paths(&symbol, frequency, adjustment, start, end);
            let request = FetchRequest {
                symbol,
                start,
                end,
                frequency_minutes: frequency,
                adjustment,
                output: output.unwrap_or(default_output),
                raw_output: raw_output.unwrap_or(default_raw),
                report_output: report.unwrap_or(default_report),
                retries,
                rate_limit_ms,
            };
            let quality = fetch_klines(&request)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&quality)?);
            } else {
                println!(
                    "downloaded {} bars for {}: {} through {}",
                    quality.rows, quality.symbol, quality.first_timestamp, quality.last_timestamp
                );
                println!("replay CSV: {}", request.output.display());
                println!("raw CSV: {}", request.raw_output.display());
                println!("quality report: {}", request.report_output.display());
                println!(
                    "quality: duplicates={} invalid_ohlc={} off_session={} missing_observed_day_bars={}",
                    quality.duplicate_timestamps,
                    quality.invalid_ohlc_rows,
                    quality.off_session_rows,
                    quality.missing_expected_bars_on_observed_days
                );
            }
        }
        Command::GenerateGrid {
            anchor,
            config,
            json,
        } => {
            let mut config = Config::load(config)?;
            if let Some(anchor) = anchor {
                config.anchor_price = Decimal::from_str(&anchor).context("invalid anchor")?;
            }
            config.validate()?;
            let spec = GridSpec::from(&config);
            let levels = spec.levels()?;
            if json {
                println!("{}", serde_json::to_string_pretty(&levels)?);
            } else {
                println!(
                    "Grid {} anchor={} ratio={}",
                    config.symbol, spec.anchor, spec.ratio
                );
                for level in levels.values() {
                    let role = if spec.is_trade_level(level.index) {
                        "TRADE"
                    } else {
                        "BOUNDARY"
                    };
                    println!("{:+3}  {:>10}  {role}", level.index, level.price);
                }
            }
        }
        Command::Replay {
            config,
            data,
            run_id,
            json,
        }
        | Command::RunPaper {
            config,
            data,
            run_id,
            json,
        } => replay(config, data, run_id, json)?,
        Command::Status(args) => {
            let (_, state, config) = load_state(&args)?;
            print_summary(&state, &config, args.json)?;
        }
        Command::Ledger(args) => {
            let (store, state, _) = load_state(&args)?;
            let events = store.load_after(&state.run_id, 0)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&events)?);
            } else {
                for event in events {
                    println!(
                        "{:>6} {} {} {}",
                        event.sequence_number,
                        event.event_time,
                        event.event_type,
                        event.idempotency_key
                    );
                }
            }
        }
        Command::Orders(args) => {
            let (_, state, _) = load_state(&args)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&state.orders)?);
            } else if state.orders.is_empty() {
                println!("no orders");
            } else {
                for order in state.orders.values() {
                    println!(
                        "{} {:?} {:?} qty={} filled={}",
                        order.order_id,
                        order.intent.direction,
                        order.status,
                        order.intent.quantity,
                        order.filled_quantity
                    );
                }
            }
        }
        Command::Positions(args) => {
            let (_, state, _) = load_state(&args)?;
            let value = json!({"position": state.position, "lots": state.lots, "realized_pnl": state.realized_pnl});
            if args.json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!(
                    "total={} sellable={} today_bought={} frozen={}",
                    state.position.total,
                    state.position.sellable,
                    state.position.today_bought,
                    state.position.frozen_sell
                );
                for lot in state.lots.values() {
                    println!(
                        "{} grid={:+} remaining={}/{} open={} pnl={}",
                        lot.lot_id,
                        lot.grid_index,
                        lot.remaining_quantity,
                        lot.original_quantity,
                        lot.open_price,
                        lot.realized_pnl
                    );
                }
            }
        }
        Command::Reconcile(args) => {
            let config = Config::load(&args.config)?;
            let mut store = SqliteStore::open(&config.database)?;
            store.migrate()?;
            let run_id = resolve_run_id(&store, args.run_id)?;
            let mut service = GridAutomationService::recover(
                config.clone(),
                store,
                gate::from_config(&config)?,
                run_id,
            )?;
            let result = service.reconcile()?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "reconciliation matched={} differences={:?}",
                    result.matched, result.differences
                );
            }
        }
        Command::Resume {
            config,
            run_id,
            reason,
            json,
        } => {
            let config = Config::load(&config)?;
            let mut store = SqliteStore::open(&config.database)?;
            store.migrate()?;
            let run_id = resolve_run_id(&store, run_id)?;
            let mut service = GridAutomationService::recover(
                config.clone(),
                store,
                gate::from_config(&config)?,
                run_id.clone(),
            )?;
            service.resume_after_reconciliation(&reason)?;
            if json {
                println!(
                    "{}",
                    json!({"run_id": run_id, "mode": service.state.mode, "resumed": true})
                );
            } else {
                println!("run {run_id} resumed after audited reconciliation");
            }
        }
        Command::RebuildState(args) => {
            let config = Config::load(&args.config)?;
            let mut store = SqliteStore::open(&config.database)?;
            store.migrate()?;
            let run_id = resolve_run_id(&store, args.run_id)?;
            let initial = initial_state(&config, &run_id);
            let (from_snapshot, snapshot_seq) = store.rebuild(initial.clone())?;
            let (from_full, full_seq) = store.rebuild_full(initial)?;
            compare_states(&from_snapshot, &from_full)?;
            if args.json {
                println!(
                    "{}",
                    json!({"run_id": run_id, "matched": true, "snapshot_sequence": snapshot_seq, "full_sequence": full_seq})
                );
            } else {
                println!(
                    "state rebuild verified: run={} sequence={}",
                    run_id, full_seq
                );
            }
        }
        Command::CompareBaseline(args) => {
            let (_, state, config) = load_state(&args)?;
            let value = json!({
                "run_id": state.run_id, "configured_gate": config.gate.kind,
                "mechanical_standard_quantity": config.standard_quantity,
                "legacy_mechanical_standard_budget": config.standard_budget,
                "deployed_purchase_notional": state.deployed_grid_budget,
                "same_grid_and_fee_boundaries": true, "orders": state.orders.len(),
                "realized_pnl": state.realized_pnl
            });
            if args.json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("baseline comparison: {value}");
            }
        }
        Command::Web {
            config,
            data,
            host,
            port,
        } => gridedge_t::web::serve(config, data, host, port).await?,
    }
    Ok(())
}

fn parse_date(value: Option<&str>, default: NaiveDate, label: &str) -> Result<NaiveDate> {
    value
        .map(|value| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .with_context(|| format!("invalid {label} date; expected YYYY-MM-DD"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn replay(
    config_path: PathBuf,
    data: PathBuf,
    run_id: Option<String>,
    json_output: bool,
) -> Result<()> {
    let config = Config::load(config_path)?;
    let bytes = fs::read(&data)
        .with_context(|| format!("failed to read replay file {}", data.display()))?;
    let mut feed = CsvReplayFeed::load_bytes(&bytes, &data.display().to_string(), &config.symbol)?;
    let descriptor = ReplayDescriptor {
        dataset_id: replay_dataset_id(&data)?,
        data_sha256: hex::encode(Sha256::digest(&bytes)),
        symbol: config.symbol.clone(),
        total_bars: feed.bars().len(),
        first_timestamp: feed
            .bars()
            .first()
            .context("dataset contains no bars")?
            .timestamp,
        last_timestamp: feed
            .bars()
            .last()
            .context("dataset contains no bars")?
            .timestamp,
    };
    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    let exists = run_id
        .as_ref()
        .is_some_and(|id| store.run_ids().is_ok_and(|runs| runs.contains(id)));
    if exists {
        let existing = store
            .first_payload_by_type(
                run_id.as_deref().expect("checked"),
                gridedge_t::event::EventType::ReplayInitialized,
            )?
            .context("existing replay has no durable dataset binding and is read-only")?;
        let existing: ReplayDescriptor =
            serde_json::from_value(existing).context("invalid replay descriptor")?;
        if existing != descriptor {
            bail!("replay dataset changed; refusing unsafe continuation")
        }
    }
    let gate = gate::from_config(&config)?;
    let mut service = if exists {
        GridAutomationService::recover(config.clone(), store, gate, run_id.expect("checked"))?
    } else {
        GridAutomationService::start_new_replay(config, store, gate, run_id, &descriptor)?
    };
    service.run_feed(&mut feed)?;
    service.stop()?;
    print_summary(&service.state, &service.config, json_output)
}

fn replay_dataset_id(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("dataset path has no UTF-8 filename")?;
    let id: String = name
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
        .take(160)
        .collect();
    if id.is_empty() {
        bail!("dataset filename is invalid")
    }
    Ok(id)
}

fn load_state(args: &QueryArgs) -> Result<(SqliteStore, StrategyState, Config)> {
    let runtime_config = Config::load(&args.config)?;
    let mut store = SqliteStore::open(&runtime_config.database)?;
    store.migrate()?;
    let run_id = resolve_run_id(&store, args.run_id.clone())?;
    let config = gridedge_t::run_context::RunContext::load(&store, &run_id)?
        .map(|context| context.config)
        .unwrap_or(runtime_config);
    let (state, _) = store.rebuild(initial_state(&config, &run_id))?;
    Ok((store, state, config))
}

fn initial_state(config: &Config, run_id: &str) -> StrategyState {
    StrategyState::new(
        run_id.to_owned(),
        String::new(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    )
}

fn resolve_run_id(store: &SqliteStore, requested: Option<String>) -> Result<String> {
    if let Some(id) = requested {
        return Ok(id);
    }
    store
        .run_ids()?
        .into_iter()
        .next()
        .context("no runs found; execute replay first")
}

fn print_summary(state: &StrategyState, config: &Config, json_output: bool) -> Result<()> {
    let touched: Vec<_> = state.historically_touched_levels.iter().copied().collect();
    let skipped: Vec<_> = state.historically_skipped_levels.iter().copied().collect();
    let executed: Vec<_> = state.historically_executed_levels.iter().copied().collect();
    let open_lots = state
        .lots
        .values()
        .filter(|l| l.remaining_quantity > 0)
        .count();
    let closed_lots = state
        .lots
        .values()
        .filter(|l| l.remaining_quantity == 0)
        .count();
    let open_orders = state.snapshot()?.open_order_ids.len();
    let deepest_lower = state
        .levels
        .values()
        .filter(|l| l.index < 0 && l.touch_count > 0)
        .map(|l| l.index.abs())
        .max()
        .unwrap_or(0)
        .min(config.trade_levels);
    let current_deferred_budget = if config.uses_fixed_quantity() {
        Decimal::ZERO
    } else {
        (Decimal::from(deepest_lower) * config.standard_budget - state.deployed_grid_budget)
            .max(Decimal::ZERO)
    };
    let (deployed_grid_quantity, current_deferred_quantity) = state
        .right_tranches
        .values()
        .filter(|tranche| {
            tranche.cycle_id == state.cycle_id
                && tranche.direction == gridedge_t::domain::Direction::Buy
        })
        .fold((0_i64, 0_i64), |(deployed, deferred), tranche| {
            (
                deployed + tranche.reserved_quantity + tranche.consumed_quantity,
                deferred + tranche.available_quantity,
            )
        });
    let current_deferred_units = if config.standard_quantity > 0 {
        Decimal::from(current_deferred_quantity) / Decimal::from(config.standard_quantity)
    } else {
        Decimal::ZERO
    };
    let valuation = gridedge_t::profit::unrealized_grid_valuation(state);
    let summary = json!({
        "run_id": state.run_id, "cycle_id": state.cycle_id, "symbol": state.symbol,
        "mode": state.mode, "anchor": state.anchor_price, "levels": state.levels,
        "touched_levels": touched, "skipped_levels": skipped, "executed_levels": executed,
        "standard_quantity": config.standard_quantity,
        "deployed_grid_quantity": deployed_grid_quantity,
        "current_deferred_quantity": current_deferred_quantity,
        "current_deferred_units": current_deferred_units,
        "deployed_purchase_notional": state.deployed_grid_budget,
        "legacy_current_deferred_budget": current_deferred_budget,
        "available_cash": state.cash.available,
        "total_position": state.position.total, "sellable_position": state.position.sellable,
        "open_lots": open_lots, "closed_lots": closed_lots, "open_orders": open_orders,
        "total_fees": state.cash.total_fees, "realized_grid_pnl": state.realized_pnl,
        "unrealized_pnl": valuation.mark_to_market_unrealized, "last_price": state.last_price,
        "valuation_policy_version": state.audited_profit_guard_policy.as_ref().map(|_| gridedge_t::profit::UNREALIZED_VALUATION_POLICY_VERSION),
        "mark_to_market_unrealized_grid_pnl": valuation.mark_to_market_unrealized,
        "total_mark_to_market_grid_pnl": valuation.mark_to_market_unrealized.and_then(|value| state.realized_pnl.checked_add(value)),
        "conservative_exit_unrealized_grid_pnl": valuation.conservative_exit_unrealized,
        "total_conservative_exit_grid_pnl": valuation.conservative_exit_unrealized.and_then(|value| state.realized_pnl.checked_add(value)),
        "conservative_exit_adjustment": valuation.conservative_exit_adjustment,
        "unpriced_open_quantity": valuation.unpriced_open_quantity,
        "event_count": state.event_count,
        "duplicate_events": state.duplicate_events, "ambiguous_bars": state.ambiguous_bars,
        "last_recovery": state.last_recovery, "last_reconciliation": state.last_reconciliation
    });
    if json_output {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!(
            "GridEdge-T run={} mode={:?} cycle={}",
            state.run_id, state.mode, state.cycle_id
        );
        println!("cash={} position={} sellable={} deployed_quantity={} deferred_quantity={} deferred_units={} purchase_notional={} fees={} realized={} mark_unrealized={} conservative_exit_unrealized={}", state.cash.available, state.position.total, state.position.sellable, deployed_grid_quantity, current_deferred_quantity, current_deferred_units, state.deployed_grid_budget, state.cash.total_fees, state.realized_pnl, valuation.mark_to_market_unrealized.map_or_else(|| "N/A".to_owned(), |value| value.to_string()), valuation.conservative_exit_unrealized.map_or_else(|| "N/A".to_owned(), |value| value.to_string()));
        println!("touched={touched:?} skipped={skipped:?} executed={executed:?}");
        println!("lots(open/closed)={open_lots}/{closed_lots} open_orders={open_orders} events={} duplicates={} ambiguous={}", state.event_count, state.duplicate_events, state.ambiguous_bars);
    }
    Ok(())
}
