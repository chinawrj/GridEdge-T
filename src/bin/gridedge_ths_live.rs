use anyhow::{bail, Context, Result};
use chrono::{Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime};
use clap::{Parser, ValueEnum};
use gridedge_t::{
    config::Config,
    data::{validate_bar, MarketBar},
    decision::algorithm_from_config,
    domain::{Direction, Fill},
    event::EventType,
    journal::{EventReader, SqliteStore},
    market_mqtt::{MarketMqttClient, MarketMqttMessage},
    service::GridAutomationService,
    ths_android_sim::{AndroidThsConfig, AndroidThsSimulationUiDriver},
    ths_sim::{RemoteFilledEvidence, SimulationMarketQuote, SimulationOrderRecord},
    ths_sim_execution::{
        reconcile_ambiguous_terminal_fills, run_automation_cycle, MacOsSimulationUiDriver,
        SimulationUiDriver,
    },
    ths_sim_outbox::{
        stage_from_source, StagedCancelState, StagedIntent, StagedIntentState,
        StagedTerminalResolution, ThsSimOutbox,
    },
    web_market::{SourceCompletionKind, WebTradeBarBuilder},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SimulationAdapter {
    Macos,
    Android,
}
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};

#[allow(dead_code)]
const QUOTE_PROBE_BUDGET_SECONDS: i64 = 20;

#[derive(Debug, Parser)]
#[command(
    name = "gridedge-ths-live",
    about = "Fail-closed Tonghuashun simulation quote-to-grid worker"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    outbox: PathBuf,
    #[arg(long)]
    quote_log: PathBuf,
    #[arg(long)]
    bar_log: PathBuf,
    #[arg(long)]
    market_event_log: PathBuf,
    #[arg(long, default_value = "127.0.0.1")]
    market_mqtt_host: String,
    #[arg(long, default_value_t = 8883)]
    market_mqtt_port: u16,
    #[arg(long, default_value = "gridedge-publisher")]
    market_mqtt_username: String,
    #[arg(long)]
    market_mqtt_password_file: PathBuf,
    #[arg(long)]
    market_mqtt_ca_file: PathBuf,
    #[arg(long, default_value_t = 5)]
    interval_minutes: u32,
    #[arg(long, default_value_t = 5)]
    poll_seconds: u64,
    #[arg(long, default_value_t = 60)]
    maximum_unchanged_seconds: i64,
    #[arg(long, default_value_t = 300)]
    maximum_order_age_seconds: i64,
    /// Permit an explicitly-audited current-session boundary to replace
    /// unavailable earlier history. The missing prefix never generates bars
    /// or orders, and recovery still needs a later contiguous live receipt.
    #[arg(long, default_value_t = false)]
    allow_partial_session_resume_boundary: bool,
    #[arg(long, value_enum, default_value_t = SimulationAdapter::Macos)]
    simulation_adapter: SimulationAdapter,
    #[arg(long, default_value = "emulator-5554")]
    android_serial: String,
    #[arg(long, default_value = "THS")]
    android_avd_marker: String,
    #[arg(long, default_value = "**0000")]
    android_masked_account: String,
    #[arg(long, default_value = "兆新股份")]
    android_security_name: String,
    #[arg(long, default_value = "/opt/homebrew/bin/adb")]
    android_adb_path: PathBuf,
    #[arg(long)]
    android_confirmation_account_sha256_file: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    android_money_actions_enabled: bool,
    #[arg(long)]
    execution_runner_file: Option<PathBuf>,
    #[arg(long)]
    execution_launch_plist_file: Option<PathBuf>,
    /// Bind the exact preflighted remote adapter/device/account/deployment
    /// identity to an already frozen outbox and exit without market work.
    #[arg(long, default_value_t = false)]
    bind_execution_identity_only: bool,
    /// Activate an already-authorized platform identity and exit before any
    /// market, outbox, or macOS UI work.
    #[arg(long, default_value_t = false)]
    activate_platform_upgrade_only: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    if !(2..=30).contains(&args.poll_seconds) {
        bail!("poll-seconds must be within 2..=30");
    }
    if !(15..=300).contains(&args.maximum_unchanged_seconds) {
        bail!("maximum-unchanged-seconds must be within 15..=300");
    }
    if !(1..=600).contains(&args.maximum_order_age_seconds) {
        bail!("maximum-order-age-seconds must be within 1..=600");
    }

    let config = Config::load(&args.config)?;
    config.validate()?;
    if args.activate_platform_upgrade_only {
        activate_authorized_platform_upgrade(&args, &config)?;
        return Ok(());
    }
    let quote_symbol = config
        .symbol
        .strip_suffix(".SZ")
        .or_else(|| config.symbol.strip_suffix(".SH"))
        .context("market symbol lacks a reviewed suffix")?;
    if args.bind_execution_identity_only {
        bind_execution_identity_only(&args, &config, quote_symbol)?;
        return Ok(());
    }
    let recovery_started_at = Local::now().naive_local();
    let mut builder = WebTradeBarBuilder::new(config.symbol.clone(), args.interval_minutes)?;
    ensure_parent(&args.quote_log)?;
    ensure_parent(&args.bar_log)?;
    ensure_parent(&args.market_event_log)?;
    ensure_parent(Path::new(&config.database))?;
    ensure_parent(&args.outbox)?;

    let mut bars = load_unique_bars(&args.bar_log, &config.symbol)?;
    validate_recovery_bars_not_future(bars.values(), recovery_started_at)?;
    let recovered_completed_bars = replay_market_messages(
        &mut builder,
        load_market_messages(&args.market_event_log)?,
        recovery_started_at,
    )?;

    let venue = if config.symbol.ends_with(".SZ") {
        "XSHE"
    } else {
        "XSHG"
    };
    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    let exists = store.run_ids()?.iter().any(|run_id| run_id == &args.run_id);
    let (mut driver, execution_identity) =
        build_driver_and_execution_identity(&args, quote_symbol)?;
    ThsSimOutbox::open(&args.outbox)?.verify_execution_identity(&execution_identity)?;
    driver.startup_preflight()?;
    let mut service = if exists {
        GridAutomationService::recover_with_algorithm(
            config.clone(),
            store,
            algorithm_from_config(&config)?,
            args.run_id.clone(),
        )?
    } else {
        GridAutomationService::start_new_with_algorithm(
            config.clone(),
            store,
            algorithm_from_config(&config)?,
            Some(args.run_id.clone()),
        )?
    };
    let mut initial_after_sequence = if ThsSimOutbox::open(&args.outbox)?.has_binding()? {
        None
    } else {
        Some(0)
    };
    let mut latest_reviewed_completion_received_at_us = None;
    service.enter_market_data_recovery("EASTMONEY_SESSION_HISTORY_BACKFILL")?;
    for bar in bars.values() {
        if is_executable_strategy_bar(bar) {
            service.on_bar(bar)?;
        }
    }
    for bar in recovered_completed_bars {
        process_market_recovery_bar(&args.bar_log, &mut bars, &mut service, bar)?;
    }

    let mut market = MarketMqttClient::connect(
        &args.market_mqtt_host,
        args.market_mqtt_port,
        &args.market_mqtt_username,
        &args.market_mqtt_password_file,
        &args.market_mqtt_ca_file,
        &format!("gridedge-paper-committed-{quote_symbol}"),
        venue,
        quote_symbol,
    )?;
    market.await_ready(StdDuration::from_secs(10))?;
    loop {
        if is_after_daily_window(Local::now().naive_local()) {
            reconcile_terminal_boundary(
                &args,
                Path::new(&config.database),
                &mut initial_after_sequence,
                Local::now().naive_local(),
                driver.as_mut(),
            )?;
            return Ok(());
        }
        let Some(message) = market.receive(StdDuration::from_secs(args.poll_seconds))? else {
            let now = Local::now().naive_local();
            if is_market_session(now)
                && source_observation_timeout_requires_recovery(
                    service.state.mode,
                    preferred_market_liveness_watermark(
                        builder
                            .latest_source_observation()
                            .map(|observation| observation.observed_at_us),
                        latest_reviewed_completion_received_at_us,
                    ),
                    now,
                    args.maximum_unchanged_seconds,
                )?
            {
                service.enter_market_data_recovery("EASTMONEY_SOURCE_OBSERVATION_TIMEOUT")?;
            }
            continue;
        };
        append_json_line(
            &args.market_event_log,
            &DurableMarketMessage::from(&message),
        )?;
        let now = Local::now().naive_local();
        let receipt = builder.ingest_with_receipt_at(&message.topic, &message.payload, now)?;
        if let Some(reason) = &receipt.quarantine_reason {
            eprintln!(
                "quarantined market event timestamp_us={} reason={reason}",
                receipt.event_timestamp_us
            );
        }
        market_timestamp_not_future(receipt.event_timestamp_us, now, "market event")?;
        market_timestamp_not_future(receipt.received_timestamp_us, now, "market receipt")?;
        let date = now.date();
        let released_bar_count = receipt.bars.len();
        let received_source_observation = receipt.source_observation.is_some();
        let source_observation_gate = builder
            .latest_source_observation()
            .map(|observation| {
                let current = market_watermark_is_current(
                    Some(observation.observed_at_us),
                    now,
                    args.maximum_unchanged_seconds,
                )?;
                let previous_current = market_watermarks_are_contiguous(
                    observation.previous_observed_at_us,
                    observation.observed_at_us,
                    args.maximum_unchanged_seconds,
                )?;
                Ok::<_, anyhow::Error>((observation, current, previous_current))
            })
            .transpose()?;
        let completion_gate = receipt
            .completion
            .map(|completion| {
                let released_bars_current = receipt
                    .bars
                    .iter()
                    .map(|bar| {
                        market_bar_is_current(bar.timestamp, now, args.maximum_unchanged_seconds)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .all(std::convert::identity);
                let (current, previous_current, observation_matches_today) =
                    source_observation_gate
                        .map(|(observation, current, previous_current)| {
                            Ok::<_, anyhow::Error>((
                                current,
                                previous_current,
                                observation.session_date == date,
                            ))
                        })
                        .unwrap_or_else(|| {
                            Ok((
                                market_watermark_is_current(
                                    Some(completion.covered_through_us),
                                    now,
                                    args.maximum_unchanged_seconds,
                                )?,
                                market_watermarks_are_contiguous(
                                    completion.previous_covered_through_us,
                                    completion.covered_through_us,
                                    args.maximum_unchanged_seconds,
                                )?,
                                completion.session_date == date,
                            ))
                        })?;
                Ok::<_, anyhow::Error>((
                    completion,
                    current,
                    previous_current,
                    observation_matches_today,
                    released_bars_current,
                ))
            })
            .transpose()?;
        if let Some((
            completion,
            current,
            previous_current,
            observation_matches_today,
            released_bars_current,
        )) = completion_gate
        {
            latest_reviewed_completion_received_at_us = reviewed_completion_liveness_watermark(
                latest_reviewed_completion_received_at_us,
                completion.kind == SourceCompletionKind::LiveContiguous,
                current,
                previous_current,
                completion.session_date == date && observation_matches_today,
                released_bar_count,
                released_bars_current,
                receipt.received_timestamp_us,
            );
        }
        if let Some((
            completion,
            current,
            previous_current,
            observation_matches_today,
            released_bars_current,
        )) = completion_gate
        {
            if completion_requires_recovery(
                service.state.mode,
                completion.kind == SourceCompletionKind::LiveContiguous,
                current,
                previous_current,
                completion.session_date == date && observation_matches_today,
                released_bar_count,
                released_bars_current,
            ) {
                service.enter_market_data_recovery("EASTMONEY_LIVE_WATERMARK_CATCHUP")?;
            }
        }
        for bar in receipt.bars {
            if service.state.mode == gridedge_t::domain::ServiceMode::Running {
                if !is_executable_strategy_bar(&bar) {
                    store_bar_if_new(&args.bar_log, &mut bars, bar)?;
                    continue;
                }
                reconcile_and_process_bar(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    now,
                    driver.as_mut(),
                    &mut bars,
                    &mut service,
                    bar,
                )?;
            } else {
                process_market_recovery_bar(&args.bar_log, &mut bars, &mut service, bar)?;
            }
        }
        if let Some((
            completion,
            current,
            previous_current,
            observation_matches_today,
            released_bars_current,
        )) = completion_gate
        {
            if completion_allows_resume(
                service.state.mode,
                completion.kind == SourceCompletionKind::LiveContiguous,
                current,
                previous_current,
                completion.session_date == date && observation_matches_today,
                released_bar_count,
                released_bars_current,
            ) && market_session_boundary_allows_resume(
                builder.has_complete_session(date),
                builder.partial_session_resume_is_safe(date),
                args.allow_partial_session_resume_boundary,
            ) && is_market_session(now)
            {
                resume_after_market_data_recovery(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    now,
                    driver.as_mut(),
                    &mut service,
                    preferred_market_liveness_watermark(
                        builder
                            .latest_source_observation()
                            .map(|observation| observation.observed_at_us),
                        latest_reviewed_completion_received_at_us,
                    )
                    .context("reviewed completion resume lacks a liveness watermark")?,
                )?;
            }
        }
        if received_source_observation {
            if let Some((observation, current, previous_current)) = source_observation_gate {
                if source_observation_requires_recovery(
                    service.state.mode,
                    current,
                    previous_current,
                    observation.session_date == date,
                ) {
                    service.enter_market_data_recovery("EASTMONEY_SOURCE_OBSERVATION_CATCHUP")?;
                }
                if source_observation_allows_resume(
                    service.state.mode,
                    current,
                    previous_current,
                    observation.session_date == date,
                ) && market_session_boundary_allows_resume(
                    builder.has_complete_session(date),
                    builder.partial_session_resume_is_safe(date),
                    args.allow_partial_session_resume_boundary,
                ) && is_market_session(now)
                {
                    resume_after_market_data_recovery(
                        &args,
                        Path::new(&config.database),
                        &mut initial_after_sequence,
                        now,
                        driver.as_mut(),
                        &mut service,
                        observation.observed_at_us,
                    )?;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableMarketMessage {
    topic: String,
    payload: Vec<u8>,
}

impl From<&MarketMqttMessage> for DurableMarketMessage {
    fn from(value: &MarketMqttMessage) -> Self {
        Self {
            topic: value.topic.clone(),
            payload: value.payload.clone(),
        }
    }
}

fn activate_authorized_platform_upgrade(args: &Args, config: &Config) -> Result<()> {
    ensure_parent(Path::new(&config.database))?;
    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    let exists = store.run_ids()?.iter().any(|run_id| run_id == &args.run_id);
    if !exists
        || gridedge_t::run_context::RunContext::load(&store, &args.run_id)?
            .and_then(|context| context.pending_platform_upgrade)
            .is_none()
    {
        bail!("activate-only requires one pending durable platform authorization")
    }
    let service = GridAutomationService::recover_with_algorithm(
        config.clone(),
        store,
        algorithm_from_config(config)?,
        args.run_id.clone(),
    )?;
    drop(service);

    let verify = SqliteStore::open_existing_read_only(&config.database)?;
    let context = gridedge_t::run_context::RunContext::load(&verify, &args.run_id)?
        .context("activated run context disappeared")?;
    let current_platform_sha256 = gridedge_t::decision::current_platform_sha256();
    if context.pending_platform_upgrade.is_some()
        || context.effective_platform_sha256.as_deref() != Some(current_platform_sha256.as_str())
    {
        bail!("platform upgrade activation did not reach the target identity")
    }
    Ok(())
}

fn bind_execution_identity_only(args: &Args, config: &Config, quote_symbol: &str) -> Result<()> {
    verify_activated_binding_platform(args, config)?;
    let (mut driver, identity_before_preflight) =
        build_driver_and_execution_identity(args, quote_symbol)?;
    driver.startup_preflight()?;
    verify_activated_binding_platform(args, config)?;
    let (_, identity_after_preflight) = build_driver_and_execution_identity(args, quote_symbol)?;
    if identity_after_preflight != identity_before_preflight {
        bail!("remote execution identity changed during startup preflight")
    }
    let mut outbox = ThsSimOutbox::open(&args.outbox)?;
    let initial_after_sequence = (!outbox.has_binding()?).then_some(0);
    drop(outbox);
    stage_from_source(
        &config.database,
        &args.run_id,
        &args.outbox,
        initial_after_sequence,
    )?;
    outbox = ThsSimOutbox::open(&args.outbox)?;
    let identity_sha256 = if outbox.has_execution_identity()? {
        outbox.upgrade_execution_identity(&identity_after_preflight)?
    } else {
        outbox.bind_execution_identity_once(&identity_after_preflight)?
    };
    println!("bound remote simulation execution identity {identity_sha256}");
    Ok(())
}

fn verify_activated_binding_platform(args: &Args, config: &Config) -> Result<()> {
    let store = SqliteStore::open_existing_read_only(&config.database)?;
    let context = gridedge_t::run_context::RunContext::load(&store, &args.run_id)?
        .context("execution identity binding requires an existing durable run")?;
    let current_platform_sha256 = gridedge_t::decision::current_platform_sha256();
    if context.pending_platform_upgrade.is_some()
        || context.effective_platform_sha256.as_deref() != Some(current_platform_sha256.as_str())
    {
        bail!("execution identity binding requires the activated platform identity")
    }
    Ok(())
}

fn build_driver_and_execution_identity(
    args: &Args,
    quote_symbol: &str,
) -> Result<(Box<dyn SimulationUiDriver>, String)> {
    let runner = args
        .execution_runner_file
        .as_deref()
        .context("remote execution identity requires --execution-runner-file")?;
    let launch_plist = args
        .execution_launch_plist_file
        .as_deref()
        .context("remote execution identity requires --execution-launch-plist-file")?;
    let runner_sha256 = gridedge_t::platform_upgrade::sha256_file(runner)?;
    let launch_plist_sha256 = gridedge_t::platform_upgrade::sha256_file(launch_plist)?;
    let platform_sha256 = gridedge_t::decision::current_platform_sha256();
    match args.simulation_adapter {
        SimulationAdapter::Macos => {
            let identity = serde_json::json!({
                "schema": "gridedge.ths-remote-execution-identity.v1",
                "adapter": "MACOS",
                "platform_sha256": platform_sha256,
                "runner_sha256": runner_sha256,
                "launch_plist_sha256": launch_plist_sha256,
                "money_actions_enabled": true,
            });
            Ok((
                Box::new(MacOsSimulationUiDriver::default()),
                serde_json::to_string(&identity)?,
            ))
        }
        SimulationAdapter::Android => {
            let confirmation_account_sha256 = args
                .android_confirmation_account_sha256_file
                .as_ref()
                .map(|path| {
                    fs::read_to_string(path)
                        .with_context(|| {
                            format!(
                                "failed to read Android confirmation account hash {}",
                                path.display()
                            )
                        })
                        .map(|value| value.trim().to_owned())
                })
                .transpose()?;
            let config = AndroidThsConfig {
                adb_path: args.android_adb_path.clone(),
                serial: args.android_serial.clone(),
                avd_name_marker: args.android_avd_marker.clone(),
                masked_account: args.android_masked_account.clone(),
                expected_symbol: quote_symbol.to_owned(),
                expected_security_name: args.android_security_name.clone(),
                confirmation_account_sha256: confirmation_account_sha256.clone(),
                money_actions_enabled: args.android_money_actions_enabled,
                ..AndroidThsConfig::default()
            };
            config.validate()?;
            let account_sha256 = confirmation_account_sha256.context(
                "Android execution identity requires the full confirmation account hash",
            )?;
            let identity = serde_json::json!({
                "schema": "gridedge.ths-remote-execution-identity.v1",
                "adapter": "ANDROID",
                "platform_sha256": platform_sha256,
                "runner_sha256": runner_sha256,
                "launch_plist_sha256": launch_plist_sha256,
                "adb_sha256": gridedge_t::platform_upgrade::sha256_file(&config.adb_path)?,
                "serial": config.serial.clone(),
                "avd_name_marker": config.avd_name_marker.clone(),
                "package": config.package.clone(),
                "tested_version": config.tested_version.clone(),
                "masked_account_sha256": gridedge_t::platform_upgrade::sha256_bytes(
                    config.masked_account.as_bytes()
                ),
                "confirmation_account_sha256": account_sha256,
                "expected_symbol": config.expected_symbol.clone(),
                "expected_security_name": config.expected_security_name.clone(),
                "money_actions_enabled": config.money_actions_enabled,
            });
            Ok((
                Box::new(AndroidThsSimulationUiDriver::new(config)?),
                serde_json::to_string(&identity)?,
            ))
        }
    }
}

fn load_market_messages(path: &Path) -> Result<Vec<DurableMarketMessage>> {
    load_json_lines(path)
}

fn replay_market_messages(
    builder: &mut WebTradeBarBuilder,
    messages: Vec<DurableMarketMessage>,
    recovery_started_at: NaiveDateTime,
) -> Result<Vec<MarketBar>> {
    let mut recovered = Vec::new();
    for message in messages {
        let receipt = builder.ingest_with_receipt_at(
            &message.topic,
            &message.payload,
            recovery_started_at,
        )?;
        market_timestamp_not_future(
            receipt.event_timestamp_us,
            recovery_started_at,
            "replayed market event",
        )?;
        market_timestamp_not_future(
            receipt.received_timestamp_us,
            recovery_started_at,
            "replayed market receipt",
        )?;
        if let Some(completion) = receipt.completion {
            market_timestamp_not_future(
                completion.covered_through_us,
                recovery_started_at,
                "replayed market completion watermark",
            )?;
        }
        validate_recovery_bars_not_future(receipt.bars.iter(), recovery_started_at)?;
        recovered.extend(receipt.bars);
    }
    Ok(recovered)
}

fn validate_recovery_bars_not_future<'a>(
    bars: impl IntoIterator<Item = &'a MarketBar>,
    recovery_started_at: NaiveDateTime,
) -> Result<()> {
    for bar in bars {
        if bar.timestamp > recovery_started_at {
            bail!(
                "replayed completed market bar {} is later than recovery start {}",
                bar.timestamp,
                recovery_started_at
            )
        }
    }
    Ok(())
}

fn market_timestamp_not_future(
    timestamp_us: u64,
    now: NaiveDateTime,
    evidence: &str,
) -> Result<()> {
    let timestamp = i64::try_from(timestamp_us)
        .with_context(|| format!("{evidence} exceeds the supported clock"))?;
    let timestamp_utc = chrono::DateTime::from_timestamp_micros(timestamp)
        .with_context(|| format!("{evidence} timestamp is invalid"))?;
    let timestamp_local = (timestamp_utc + chrono::Duration::hours(8)).naive_utc();
    if timestamp_local > now {
        bail!("{evidence} is in the future")
    }
    Ok(())
}

fn probe_remote_orders(driver: &mut dyn SimulationUiDriver) -> Result<Vec<SimulationOrderRecord>> {
    driver.orders()?.records()
}

fn verify_remote_contracts(
    records: &[SimulationOrderRecord],
    outbox_path: &Path,
    require_bound_terminal: bool,
) -> Result<()> {
    let intents = ThsSimOutbox::open(outbox_path)?.staged_intents()?;
    let violations =
        remote_contract_violations_with_mode(records, &intents, require_bound_terminal)?;
    if !violations.is_empty() {
        bail!(
            "Tonghuashun simulation contract audit failed: {}",
            violations.join(";")
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RemoteTerminalPermit {
    _private: (),
}

fn remote_terminal_permit(
    records: &[SimulationOrderRecord],
    source_database: &Path,
    run_id: &str,
    outbox_path: &Path,
) -> Result<RemoteTerminalPermit> {
    let intents = ThsSimOutbox::open(outbox_path)?.staged_intents()?;
    let modeled_fills = load_modeled_fills(source_database, run_id)?;
    remote_terminal_permit_for_modeled(records, &intents, &modeled_fills)
}

#[allow(clippy::too_many_arguments)]
fn reconcile_and_process_bar(
    args: &Args,
    source_database: &Path,
    initial_after_sequence: &mut Option<i64>,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
    bars: &mut BTreeMap<NaiveDateTime, MarketBar>,
    service: &mut GridAutomationService,
    bar: MarketBar,
) -> Result<()> {
    reconcile_ambiguous_terminal_fills(&args.outbox, now, driver)?;
    let mut remote_records = probe_remote_orders(driver)?;
    verify_remote_contracts(&remote_records, &args.outbox, false)?;
    if run_outbox(args, source_database, initial_after_sequence, now, driver)? {
        remote_records = probe_remote_orders(driver)?;
    }
    let permit =
        remote_terminal_permit(&remote_records, source_database, &args.run_id, &args.outbox)?;
    process_bar(args, bars, service, bar, &permit)?;
    run_outbox(args, source_database, initial_after_sequence, now, driver)?;
    Ok(())
}

fn reconcile_terminal_boundary(
    args: &Args,
    source_database: &Path,
    initial_after_sequence: &mut Option<i64>,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
) -> Result<()> {
    reconcile_ambiguous_terminal_fills(&args.outbox, now, driver)?;
    let mut remote_records = probe_remote_orders(driver)?;
    verify_remote_contracts(&remote_records, &args.outbox, false)?;
    if run_outbox(args, source_database, initial_after_sequence, now, driver)? {
        remote_records = probe_remote_orders(driver)?;
    }
    remote_terminal_permit(&remote_records, source_database, &args.run_id, &args.outbox)?;
    Ok(())
}

fn resume_after_market_data_recovery(
    args: &Args,
    source_database: &Path,
    initial_after_sequence: &mut Option<i64>,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
    service: &mut GridAutomationService,
    liveness_watermark_us: u64,
) -> Result<()> {
    reconcile_terminal_boundary(args, source_database, initial_after_sequence, now, driver)?;
    let post_audit_now = Local::now().naive_local();
    if !is_market_session(post_audit_now)
        || !market_watermark_is_current(
            Some(liveness_watermark_us),
            post_audit_now,
            args.maximum_unchanged_seconds,
        )?
    {
        return Ok(());
    }
    let reconciliation = service.reconcile()?;
    if !reconciliation.matched {
        bail!("market-data recovery cannot resume before Paper reconciliation matches")
    }
    let pre_resume_now = Local::now().naive_local();
    if !is_market_session(pre_resume_now)
        || !market_watermark_is_current(
            Some(liveness_watermark_us),
            pre_resume_now,
            args.maximum_unchanged_seconds,
        )?
    {
        return Ok(());
    }
    service.resume_after_reconciliation(
        "Eastmoney complete source watermark and Paper/remote terminal audit matched",
    )
}

#[cfg(test)]
#[allow(dead_code)]
fn remote_terminal_permit_for(
    records: &[SimulationOrderRecord],
    intents: &[StagedIntent],
) -> Result<RemoteTerminalPermit> {
    let modeled_fills = intents
        .iter()
        .map(|intent| {
            let notional = intent
                .order
                .limit_price
                .checked_mul(rust_decimal::Decimal::from(intent.order.quantity))
                .context("test modeled fill notional overflow")?;
            Ok((
                intent.order_id.clone(),
                ModeledFill {
                    direction: intent.order.direction,
                    quantity: intent.order.quantity,
                    notional,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    remote_terminal_permit_for_modeled(records, intents, &modeled_fills)
}

fn remote_terminal_permit_for_modeled(
    records: &[SimulationOrderRecord],
    intents: &[StagedIntent],
    modeled_fills: &BTreeMap<String, ModeledFill>,
) -> Result<RemoteTerminalPermit> {
    let violations = remote_contract_violations_with_mode(records, intents, true)?;
    if !violations.is_empty() {
        bail!(
            "Tonghuashun simulation contract audit failed: {}",
            violations.join(";")
        );
    }
    verify_remote_fill_dominance(records, intents, modeled_fills)?;
    Ok(RemoteTerminalPermit { _private: () })
}

#[derive(Debug, Clone, Copy)]
struct ModeledFill {
    direction: Direction,
    quantity: i64,
    notional: rust_decimal::Decimal,
}

fn load_modeled_fills(
    source_database: &Path,
    run_id: &str,
) -> Result<BTreeMap<String, ModeledFill>> {
    let source = SqliteStore::open_existing_read_only(source_database)?;
    source.verify_current_schema()?;
    let mut fills = BTreeMap::<String, ModeledFill>::new();
    for event in source.load_after(run_id, 0)?.into_iter().filter(|event| {
        matches!(
            event.event_type,
            EventType::OrderPartiallyFilled | EventType::OrderFilled
        )
    }) {
        let fill: Fill = serde_json::from_value(
            event
                .payload
                .get("fill")
                .cloned()
                .context("modeled fill event lacks its canonical fill")?,
        )?;
        let notional = fill
            .price
            .checked_mul(rust_decimal::Decimal::from(fill.quantity))
            .context("modeled Tonghuashun comparison notional overflow")?;
        let entry = fills.entry(fill.order_id).or_insert(ModeledFill {
            direction: fill.direction,
            quantity: 0,
            notional: rust_decimal::Decimal::ZERO,
        });
        if entry.direction != fill.direction {
            bail!("modeled order fill direction changed");
        }
        entry.quantity = entry
            .quantity
            .checked_add(fill.quantity)
            .context("modeled order fill quantity overflow")?;
        entry.notional = entry
            .notional
            .checked_add(notional)
            .context("modeled order fill notional overflow")?;
    }
    Ok(fills)
}

fn verify_remote_fill_dominance(
    records: &[SimulationOrderRecord],
    intents: &[StagedIntent],
    modeled_fills: &BTreeMap<String, ModeledFill>,
) -> Result<()> {
    let records = records
        .iter()
        .map(|record| (record.contract_id.as_str(), record))
        .collect::<BTreeMap<_, _>>();
    for intent in intents {
        let Some(contract_id) = intent.remote_contract_id.as_deref() else {
            continue;
        };
        let record = records.get(contract_id).copied();
        let terminal = match record {
            Some(record) => remote_terminal(record)?,
            None if intent.terminal_resolution == StagedTerminalResolution::Filled => {
                Some(RemoteTerminal::Filled)
            }
            None => None,
        };
        match terminal {
            Some(RemoteTerminal::Filled) => {
                let modeled = modeled_fills.get(&intent.order_id).with_context(|| {
                    format!("contract {contract_id} filled before a modeled fill was durable")
                })?;
                if modeled.direction != intent.order.direction
                    || modeled.quantity != intent.order.quantity
                {
                    bail!("contract {contract_id} quantity does not match the modeled full fill");
                }
                let modeled_average = modeled
                    .notional
                    .checked_div(rust_decimal::Decimal::from(modeled.quantity))
                    .context("modeled average fill price is not representable")?;
                let remote_price = if intent.terminal_resolution == StagedTerminalResolution::Filled
                {
                    let evidence: RemoteFilledEvidence = serde_json::from_str(
                        intent
                            .terminal_evidence_json
                            .as_deref()
                            .context("durable remote fill lacks its terminal evidence")?,
                    )
                    .context("durable remote fill evidence is invalid")?;
                    if evidence.evidence_contract_version != 1
                        || evidence.order.contract_id != contract_id
                    {
                        bail!("durable remote fill evidence changed its contract identity");
                    }
                    let mut quantity = 0_i64;
                    let mut notional = rust_decimal::Decimal::ZERO;
                    let mut fill_ids = BTreeSet::new();
                    for fill in &evidence.fills {
                        if fill.contract_id != contract_id
                            || fill.symbol != intent.order.symbol
                            || fill.direction != intent.order.direction
                            || !fill_ids.insert(fill.fill_id.as_str())
                        {
                            bail!("durable remote fill contains a mismatched transaction row");
                        }
                        quantity = quantity
                            .checked_add(fill.quantity)
                            .context("durable remote fill quantity overflow")?;
                        notional = notional
                            .checked_add(fill.amount)
                            .context("durable remote fill notional overflow")?;
                    }
                    if quantity != modeled.quantity {
                        bail!("durable remote fill quantity differs from the modeled fill");
                    }
                    notional
                        .checked_div(rust_decimal::Decimal::from(quantity))
                        .context("durable remote average fill price is not representable")?
                } else {
                    record
                        .context("filled Tonghuashun contract lacks current order evidence")?
                        .fill_price
                        .as_deref()
                        .context("filled Tonghuashun contract lacks a fill price")?
                        .parse::<rust_decimal::Decimal>()
                        .context("filled Tonghuashun contract has an invalid fill price")?
                };
                let is_conservative = match modeled.direction {
                    Direction::Buy => remote_price <= modeled_average,
                    Direction::Sell => remote_price >= modeled_average,
                };
                if !is_conservative {
                    bail!(
                        "contract {contract_id} remote fill price is worse than the modeled conservative fill"
                    );
                }
            }
            Some(RemoteTerminal::Cancelled) if modeled_fills.contains_key(&intent.order_id) => {
                bail!("contract {contract_id} was cancelled after the model recorded a fill");
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
fn untracked_open_contracts(
    records: &[SimulationOrderRecord],
    known_contracts: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    records
        .iter()
        .filter(|record| {
            remote_terminal(record).ok().flatten().is_none()
                && !known_contracts.contains(&record.contract_id)
        })
        .map(|record| record.contract_id.clone())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteTerminal {
    Filled,
    Cancelled,
}

fn remote_terminal(record: &SimulationOrderRecord) -> Result<Option<RemoteTerminal>> {
    if record.quantity <= 0
        || record.cancelled_quantity < 0
        || record.cancelled_quantity > record.quantity
    {
        bail!(
            "contract {} has an invalid quantity partition",
            record.contract_id
        );
    }
    if record.remark == "全部撤单" && record.cancelled_quantity == record.quantity {
        return Ok(Some(RemoteTerminal::Cancelled));
    }
    if record.remark == "全部成交" && record.cancelled_quantity == 0 {
        let fill_price = record
            .fill_price
            .as_deref()
            .context("filled Tonghuashun contract lacks a fill price")?
            .parse::<rust_decimal::Decimal>()
            .context("filled Tonghuashun contract has an invalid fill price")?;
        if fill_price <= rust_decimal::Decimal::ZERO {
            bail!("filled Tonghuashun contract has a non-positive fill price");
        }
        return Ok(Some(RemoteTerminal::Filled));
    }
    Ok(None)
}

#[cfg(test)]
#[allow(dead_code)]
fn remote_contract_violations(
    records: &[SimulationOrderRecord],
    intents: &[StagedIntent],
) -> Result<Vec<String>> {
    remote_contract_violations_with_mode(records, intents, true)
}

fn remote_contract_violations_with_mode(
    records: &[SimulationOrderRecord],
    intents: &[StagedIntent],
    require_bound_terminal: bool,
) -> Result<Vec<String>> {
    let mut known = BTreeMap::<String, &StagedIntent>::new();
    for intent in intents {
        if intent.state == StagedIntentState::Ambiguous
            || (intent.cancel_state == StagedCancelState::Ambiguous
                && intent.terminal_resolution == StagedTerminalResolution::Unresolved)
        {
            return Ok(vec![format!("{} is AMBIGUOUS", intent.intent_id)]);
        }
        if let Some(contract_id) = intent.remote_contract_id.as_ref() {
            if known.insert(contract_id.clone(), intent).is_some() {
                bail!("multiple durable intents bind contract {contract_id}");
            }
        }
    }

    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    for record in records {
        if !seen.insert(record.contract_id.clone()) {
            violations.push(format!(
                "{} appears more than once in the reviewed order table",
                record.contract_id
            ));
            continue;
        }
        let terminal = remote_terminal(record)?;
        match known.get(&record.contract_id) {
            None if terminal.is_none() => violations.push(format!(
                "{} is untracked and non-terminal ({})",
                record.contract_id, record.remark
            )),
            Some(intent) if !remote_order_matches_intent(record, intent) => {
                violations.push(format!(
                    "{} no longer matches its durable order intent",
                    record.contract_id
                ));
            }
            Some(intent) => match (intent.cancel_state, terminal) {
                (StagedCancelState::Ambiguous, Some(RemoteTerminal::Filled))
                    if intent.terminal_resolution == StagedTerminalResolution::Filled => {}
                (StagedCancelState::Cancelled, Some(RemoteTerminal::Cancelled)) => {}
                (StagedCancelState::Cancelled, _) => violations.push(format!(
                    "{} is durably CANCELLED but the UI is not fully cancelled",
                    record.contract_id
                )),
                (StagedCancelState::NotRequested, Some(RemoteTerminal::Cancelled)) => {
                    violations.push(format!(
                        "{} was cancelled without a durable cancel request",
                        record.contract_id
                    ));
                }
                (StagedCancelState::Cancelling, Some(RemoteTerminal::Filled)) => {
                    violations.push(format!(
                        "{} filled while durable cancellation is pending",
                        record.contract_id
                    ));
                }
                (_, None) if require_bound_terminal => violations.push(format!(
                    "{} remains non-terminal after durable outbox reconciliation",
                    record.contract_id
                )),
                _ => {}
            },
            None => {}
        }
    }
    for (contract_id, intent) in &known {
        if !seen.contains(contract_id) {
            if intent.terminal_resolution == StagedTerminalResolution::Filled
                && intent.terminal_evidence_json.is_some()
            {
                continue;
            }
            violations.push(format!(
                "{} is durably bound but missing from the reviewed order table",
                contract_id
            ));
        }
    }
    Ok(violations)
}

fn remote_order_matches_intent(record: &SimulationOrderRecord, intent: &StagedIntent) -> bool {
    record.symbol == intent.order.symbol
        && record.direction == intent.order.direction
        && record.quantity == intent.order.quantity
        && record.order_price == intent.order.limit_price
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct PriceFreshness {
    session: Option<(NaiveDate, u8)>,
    last_observed_at: Option<NaiveDateTime>,
}

#[allow(dead_code)]
impl PriceFreshness {
    fn observe(
        &mut self,
        quote: &SimulationMarketQuote,
        maximum_unchanged: chrono::Duration,
    ) -> Result<()> {
        let session = market_session_key(quote.observed_at)
            .context("Tonghuashun quote freshness evidence is outside market hours")?;
        if quote.observed_from > quote.observed_at
            || market_session_key(quote.observed_from) != Some(session)
        {
            bail!("Tonghuashun quote freshness evidence crosses a session boundary");
        }
        if quote.observed_at - quote.observed_from > maximum_unchanged {
            bail!("Tonghuashun quote evidence collection exceeded its maximum duration");
        }
        if self.session != Some(session) {
            self.session = Some(session);
            self.last_observed_at = Some(quote.observed_at);
            return Ok(());
        }
        let last_observed_at = self
            .last_observed_at
            .context("Tonghuashun quote freshness suffix lacks its previous evidence time")?;
        if quote.observed_at <= last_observed_at {
            bail!("Tonghuashun quote evidence did not advance; refusing stale market data");
        }
        self.last_observed_at = Some(quote.observed_at);
        Ok(())
    }
}

#[allow(dead_code)]
fn market_session_key(value: NaiveDateTime) -> Option<(NaiveDate, u8)> {
    if value.weekday().number_from_monday() > 5 {
        return None;
    }
    let time = value.time();
    if (at(9, 30)..at(11, 30)).contains(&time) {
        Some((value.date(), 0))
    } else if (at(13, 0)..at(15, 0)).contains(&time) {
        Some((value.date(), 1))
    } else {
        None
    }
}

fn process_bar(
    args: &Args,
    bars: &mut BTreeMap<NaiveDateTime, MarketBar>,
    service: &mut GridAutomationService,
    bar: MarketBar,
    _permit: &RemoteTerminalPermit,
) -> Result<()> {
    store_bar_if_new(&args.bar_log, bars, bar.clone())?;
    service.on_bar(&bar)
}

fn process_market_recovery_bar(
    bar_log: &Path,
    bars: &mut BTreeMap<NaiveDateTime, MarketBar>,
    service: &mut GridAutomationService,
    bar: MarketBar,
) -> Result<()> {
    if service.state.mode != gridedge_t::domain::ServiceMode::ReadOnly {
        bail!("market recovery bar cannot be applied while the service is RUNNING")
    }
    let already_durable = bars.contains_key(&bar.timestamp);
    store_bar_if_new(bar_log, bars, bar.clone())?;
    if is_executable_strategy_bar(&bar) && !already_durable {
        service.on_bar(&bar)?;
    }
    Ok(())
}

fn market_watermark_is_current(
    covered_through_us: Option<u64>,
    now: NaiveDateTime,
    maximum_age_seconds: i64,
) -> Result<bool> {
    let Some(covered_through_us) = covered_through_us else {
        return Ok(false);
    };
    let timestamp = i64::try_from(covered_through_us)
        .context("market completion watermark exceeds the supported clock")?;
    let covered_utc = chrono::DateTime::from_timestamp_micros(timestamp)
        .context("market completion watermark is invalid")?;
    let covered = (covered_utc + chrono::Duration::hours(8)).naive_utc();
    let age = now - covered;
    if age < chrono::Duration::zero() {
        bail!("market completion watermark is in the future")
    }
    Ok(age <= chrono::Duration::seconds(maximum_age_seconds))
}

fn market_watermarks_are_contiguous(
    previous_covered_through_us: Option<u64>,
    covered_through_us: u64,
    maximum_gap_seconds: i64,
) -> Result<bool> {
    let Some(previous_covered_through_us) = previous_covered_through_us else {
        return Ok(false);
    };
    let gap_us = covered_through_us
        .checked_sub(previous_covered_through_us)
        .context("market completion watermark moved backwards")?;
    let maximum_gap_us = u64::try_from(maximum_gap_seconds)
        .context("market maximum gap must be non-negative")?
        .checked_mul(1_000_000)
        .context("market maximum gap overflow")?;
    Ok(gap_us <= maximum_gap_us)
}

fn market_bar_is_current(
    bar_timestamp: NaiveDateTime,
    now: NaiveDateTime,
    maximum_age_seconds: i64,
) -> Result<bool> {
    let age = now - bar_timestamp;
    if age < chrono::Duration::zero() {
        bail!("completed market bar is in the future")
    }
    Ok(age <= chrono::Duration::seconds(maximum_age_seconds))
}

fn completion_requires_recovery(
    mode: gridedge_t::domain::ServiceMode,
    completion_is_live: bool,
    completion_is_current: bool,
    previous_completion_is_current: bool,
    session_matches_today: bool,
    released_bar_count: usize,
    released_bars_are_current: bool,
) -> bool {
    mode == gridedge_t::domain::ServiceMode::Running
        && (!completion_is_live
            || !completion_is_current
            || !previous_completion_is_current
            || !session_matches_today
            || released_bar_count > 1
            || !released_bars_are_current)
}

fn completion_allows_resume(
    mode: gridedge_t::domain::ServiceMode,
    completion_is_live: bool,
    completion_is_current: bool,
    previous_completion_is_current: bool,
    session_matches_today: bool,
    released_bar_count: usize,
    released_bars_are_current: bool,
) -> bool {
    mode == gridedge_t::domain::ServiceMode::ReadOnly
        && completion_is_live
        && completion_is_current
        && previous_completion_is_current
        && session_matches_today
        && released_bar_count <= 1
        && released_bars_are_current
}

fn source_observation_requires_recovery(
    mode: gridedge_t::domain::ServiceMode,
    observation_is_current: bool,
    previous_observation_is_contiguous: bool,
    session_matches_today: bool,
) -> bool {
    mode == gridedge_t::domain::ServiceMode::Running
        && (!observation_is_current
            || !previous_observation_is_contiguous
            || !session_matches_today)
}

fn source_observation_allows_resume(
    mode: gridedge_t::domain::ServiceMode,
    observation_is_current: bool,
    previous_observation_is_contiguous: bool,
    session_matches_today: bool,
) -> bool {
    mode == gridedge_t::domain::ServiceMode::ReadOnly
        && observation_is_current
        && previous_observation_is_contiguous
        && session_matches_today
}

fn source_observation_timeout_requires_recovery(
    mode: gridedge_t::domain::ServiceMode,
    observed_at_us: Option<u64>,
    now: NaiveDateTime,
    maximum_age_seconds: i64,
) -> Result<bool> {
    Ok(mode == gridedge_t::domain::ServiceMode::Running
        && !market_watermark_is_current(observed_at_us, now, maximum_age_seconds)?)
}

fn preferred_market_liveness_watermark(
    observed_at_us: Option<u64>,
    reviewed_completion_received_at_us: Option<u64>,
) -> Option<u64> {
    observed_at_us.or(reviewed_completion_received_at_us)
}

#[allow(clippy::too_many_arguments)]
fn reviewed_completion_liveness_watermark(
    previous_received_at_us: Option<u64>,
    completion_is_live: bool,
    completion_is_current: bool,
    previous_completion_is_contiguous: bool,
    session_matches_today: bool,
    released_bar_count: usize,
    released_bars_are_current: bool,
    source_received_at_us: u64,
) -> Option<u64> {
    if completion_is_live
        && completion_is_current
        && previous_completion_is_contiguous
        && session_matches_today
        && released_bar_count <= 1
        && released_bars_are_current
    {
        Some(source_received_at_us)
    } else {
        previous_received_at_us
    }
}

fn market_session_boundary_allows_resume(
    has_complete_history: bool,
    has_safe_resume_boundary: bool,
    allow_partial_boundary: bool,
) -> bool {
    has_complete_history || (allow_partial_boundary && has_safe_resume_boundary)
}

fn run_outbox(
    args: &Args,
    source_database: &Path,
    initial_after_sequence: &mut Option<i64>,
    now: NaiveDateTime,
    driver: &mut dyn SimulationUiDriver,
) -> Result<bool> {
    let receipt = run_automation_cycle(
        source_database,
        &args.run_id,
        &args.outbox,
        initial_after_sequence.take(),
        now,
        chrono::Duration::seconds(args.maximum_order_age_seconds),
        driver,
    )?;
    let changed_remote_state = !receipt.executed.is_empty() || !receipt.cancelled.is_empty();
    if changed_remote_state {
        println!(
            "source={} submitted={} cancelled={}",
            receipt.source_cursor,
            receipt.executed.len(),
            receipt.cancelled.len()
        );
    }
    Ok(changed_remote_state)
}

fn store_bar_if_new(
    path: &Path,
    bars: &mut BTreeMap<NaiveDateTime, MarketBar>,
    bar: MarketBar,
) -> Result<()> {
    if let Some(existing) = bars.get(&bar.timestamp) {
        if serde_json::to_value(existing)? != serde_json::to_value(&bar)? {
            bail!("completed Tonghuashun bar changed at {}", bar.timestamp);
        }
        return Ok(());
    }
    append_json_line(path, &bar)?;
    bars.insert(bar.timestamp, bar);
    Ok(())
}

fn append_json_line(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open append-only log {}", path.display()))?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn load_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            Ok(line) => {
                Some(serde_json::from_str(&line).with_context(|| {
                    format!("invalid JSON line {} in {}", index + 1, path.display())
                }))
            }
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

fn load_unique_bars(
    path: &Path,
    expected_symbol: &str,
) -> Result<BTreeMap<NaiveDateTime, MarketBar>> {
    let mut bars = BTreeMap::new();
    let mut previous = None;
    for bar in load_json_lines::<MarketBar>(path)? {
        validate_bar(&bar, expected_symbol)?;
        if previous.is_some_and(|timestamp| bar.timestamp <= timestamp) {
            bail!("completed Tonghuashun bar log is not strictly append-only");
        }
        previous = Some(bar.timestamp);
        if bars.insert(bar.timestamp, bar).is_some() {
            bail!("completed Tonghuashun bar log contains a duplicate timestamp");
        }
    }
    Ok(bars)
}

#[allow(dead_code)]
fn load_validated_quotes(path: &Path, expected_symbol: &str) -> Result<Vec<SimulationMarketQuote>> {
    let quotes = load_json_lines::<SimulationMarketQuote>(path)?;
    let mut previous = None;
    for quote in &quotes {
        quote.validate_reviewed_evidence(expected_symbol)?;
        if previous.is_some_and(|timestamp| quote.observed_at <= timestamp) {
            bail!("Tonghuashun quote evidence log is not strictly append-only");
        }
        previous = Some(quote.observed_at);
    }
    Ok(quotes)
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[allow(dead_code)]
fn is_market_session(now: NaiveDateTime) -> bool {
    if now.weekday().number_from_monday() > 5 {
        return false;
    }
    let time = now.time();
    (at(9, 30)..at(11, 30)).contains(&time) || (at(13, 0)..at(15, 0)).contains(&time)
}

#[allow(dead_code)]
fn quote_probe_boundary_delay(
    now: NaiveDateTime,
    interval_minutes: u32,
) -> Result<Option<StdDuration>> {
    let width_seconds = i64::from(interval_minutes)
        .checked_mul(60)
        .context("Tonghuashun quote interval overflowed")?;
    if width_seconds <= 0
        || QUOTE_PROBE_BUDGET_SECONDS <= 0
        || QUOTE_PROBE_BUDGET_SECONDS >= width_seconds
    {
        bail!("Tonghuashun quote boundary budget is outside the interval");
    }
    let session_start = match now.time() {
        time if (at(9, 30)..at(11, 30)).contains(&time) => at(9, 30),
        time if (at(13, 0)..at(15, 0)).contains(&time) => at(13, 0),
        _ => return Ok(None),
    };
    let elapsed_seconds = (now - now.date().and_time(session_start)).num_seconds();
    if elapsed_seconds < 0 {
        bail!("Tonghuashun quote time precedes its trading session");
    }
    let elapsed_in_bucket = elapsed_seconds % width_seconds;
    if elapsed_in_bucket == 0 {
        return Ok(None);
    }
    let remaining_seconds = width_seconds - elapsed_in_bucket;
    if remaining_seconds > QUOTE_PROBE_BUDGET_SECONDS {
        return Ok(None);
    }
    let delay_seconds =
        u64::try_from(remaining_seconds).context("Tonghuashun quote boundary delay is invalid")?;
    Ok(Some(StdDuration::from_secs(delay_seconds)))
}

fn is_after_daily_window(now: NaiveDateTime) -> bool {
    now.weekday().number_from_monday() <= 5 && now.time() >= at(15, 5)
}

fn is_executable_strategy_bar(bar: &MarketBar) -> bool {
    let time = bar.timestamp.time();
    (at(9, 35)..=at(11, 25)).contains(&time) || (at(13, 5)..=at(14, 55)).contains(&time)
}

#[allow(dead_code)]
fn is_lunch_settlement_window(now: NaiveDateTime) -> bool {
    now.weekday().number_from_monday() <= 5 && (at(11, 25)..at(13, 0)).contains(&now.time())
}

fn at(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, 0).expect("valid deployment time")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use tempfile::tempdir;

    #[test]
    fn trading_session_boundaries_exclude_lunch_close_and_weekends() {
        assert!(is_market_session(timestamp("2026-08-18 09:30:00")));
        assert!(is_market_session(timestamp("2026-08-18 11:29:59")));
        assert!(!is_market_session(timestamp("2026-08-18 11:30:00")));
        assert!(!is_market_session(timestamp("2026-08-18 12:59:59")));
        assert!(is_market_session(timestamp("2026-08-18 13:00:00")));
        assert!(!is_market_session(timestamp("2026-08-18 15:00:00")));
        assert!(is_after_daily_window(timestamp("2026-08-18 15:05:00")));
        assert!(!is_market_session(timestamp("2026-08-22 10:00:00")));
    }

    #[test]
    fn append_only_bar_log_rejects_a_changed_completed_bucket() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("bars.jsonl");
        let mut bars = BTreeMap::new();
        let original = bar("3.55");

        store_bar_if_new(&path, &mut bars, original.clone())?;
        store_bar_if_new(&path, &mut bars, original)?;
        assert_eq!(load_json_lines::<MarketBar>(&path)?.len(), 1);

        let changed = bar("3.56");
        assert!(store_bar_if_new(&path, &mut bars, changed).is_err());
        assert_eq!(load_json_lines::<MarketBar>(&path)?.len(), 1);
        Ok(())
    }

    #[test]
    fn unknown_open_contract_blocks_automation_but_cancelled_and_bound_rows_do_not() {
        let open = remote_order("OPEN", "未成交", 0);
        let cancelled = remote_order("CANCELLED", "全部撤单", 100);
        let known = std::collections::BTreeSet::from(["KNOWN".to_owned()]);
        let bound = remote_order("KNOWN", "未成交", 0);

        assert_eq!(
            untracked_open_contracts(&[open, cancelled, bound], &known),
            vec!["OPEN".to_owned()]
        );
    }

    fn bar(close: &str) -> MarketBar {
        let close = Decimal::from_str(close).unwrap();
        MarketBar {
            timestamp: timestamp("2026-08-18 09:35:00"),
            symbol: "002256.SZ".to_owned(),
            open: Decimal::from_str("3.55").unwrap(),
            high: close.max(Decimal::from_str("3.55").unwrap()),
            low: close.min(Decimal::from_str("3.55").unwrap()),
            close,
            volume: 0,
            amount: None,
        }
    }

    fn remote_order(
        contract_id: &str,
        remark: &str,
        cancelled_quantity: i64,
    ) -> SimulationOrderRecord {
        SimulationOrderRecord {
            order_date: "20260818".to_owned(),
            order_time: "09:31:00".to_owned(),
            symbol: "002256".to_owned(),
            security_name: "兆新股份".to_owned(),
            direction: gridedge_t::domain::Direction::Buy,
            remark: remark.to_owned(),
            quantity: 100,
            cancelled_quantity,
            order_price: Decimal::from_str("3.55").unwrap(),
            fill_price: None,
            contract_id: contract_id.to_owned(),
            order_attribute: "0".to_owned(),
        }
    }

    fn timestamp(value: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
    }
}
