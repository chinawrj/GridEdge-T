use anyhow::{bail, Context, Result};
use chrono::{Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime};
use clap::Parser;
use gridedge_t::{
    config::Config,
    data::{validate_bar, MarketBar},
    decision::algorithm_from_config,
    domain::{Direction, Fill},
    event::EventType,
    journal::{EventReader, SqliteStore},
    service::GridAutomationService,
    ths_market::ThsQuoteBarBuilder,
    ths_sim::{
        probe_current_market_quote, probe_current_orders, RemoteFilledEvidence,
        SimulationMarketQuote, SimulationOrderRecord,
    },
    ths_sim_execution::{
        reconcile_ambiguous_terminal_fills, run_automation_cycle, MacOsSimulationUiDriver,
    },
    ths_sim_outbox::{
        StagedCancelState, StagedIntent, StagedIntentState, StagedTerminalResolution, ThsSimOutbox,
    },
};
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration as StdDuration,
};

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
    #[arg(long, default_value_t = 5)]
    interval_minutes: u32,
    #[arg(long, default_value_t = 5)]
    poll_seconds: u64,
    #[arg(long, default_value_t = 60)]
    maximum_unchanged_seconds: i64,
    #[arg(long, default_value_t = 300)]
    maximum_order_age_seconds: i64,
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
    let mut builder = ThsQuoteBarBuilder::new(config.symbol.clone(), args.interval_minutes)?;
    ensure_parent(&args.quote_log)?;
    ensure_parent(&args.bar_log)?;
    ensure_parent(Path::new(&config.database))?;
    ensure_parent(&args.outbox)?;

    let mut bars = load_unique_bars(&args.bar_log, &config.symbol)?;
    let last_bar_timestamp = bars.keys().next_back().copied();
    let historical_quotes = load_validated_quotes(&args.quote_log, builder.quote_symbol())?;
    let pending_quotes = historical_quotes
        .iter()
        .filter(|quote| last_bar_timestamp.is_none_or(|timestamp| quote.observed_at >= timestamp))
        .cloned()
        .collect::<Vec<_>>();
    let mut freshness = PriceFreshness::default();
    for quote in &pending_quotes {
        freshness.observe(
            quote,
            chrono::Duration::seconds(args.maximum_unchanged_seconds),
        )?;
    }
    let mut recovered_completed_bars = Vec::new();
    for quote in pending_quotes
        .into_iter()
        .filter(|quote| last_bar_timestamp.is_none_or(|timestamp| quote.observed_at >= timestamp))
    {
        if let Some(bar) = builder.push(quote)? {
            recovered_completed_bars.push(bar);
        }
    }
    if let Some(bar) = builder.flush_through(Local::now().naive_local())? {
        recovered_completed_bars.push(bar);
    }

    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    let exists = store.run_ids()?.iter().any(|run_id| run_id == &args.run_id);
    if args.activate_platform_upgrade_only
        && (!exists
            || gridedge_t::run_context::RunContext::load(&store, &args.run_id)?
                .and_then(|context| context.pending_platform_upgrade)
                .is_none())
    {
        bail!("activate-only requires one pending durable platform authorization")
    }
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
    if args.activate_platform_upgrade_only {
        let verify = SqliteStore::open_existing_read_only(&config.database)?;
        let context = gridedge_t::run_context::RunContext::load(&verify, &args.run_id)?
            .context("activated run context disappeared")?;
        let current_platform_sha256 = gridedge_t::decision::current_platform_sha256();
        if context.pending_platform_upgrade.is_some()
            || context.effective_platform_sha256.as_deref()
                != Some(current_platform_sha256.as_str())
        {
            bail!("platform upgrade activation did not reach the target identity")
        }
        return Ok(());
    }
    for bar in bars.values() {
        service.on_bar(bar)?;
    }

    let mut driver = MacOsSimulationUiDriver::default();
    let mut initial_after_sequence = if ThsSimOutbox::open(&args.outbox)?.has_binding()? {
        None
    } else {
        Some(0)
    };
    for bar in recovered_completed_bars {
        reconcile_and_process_bar(
            &args,
            Path::new(&config.database),
            &mut initial_after_sequence,
            bar.timestamp,
            &mut driver,
            &mut bars,
            &mut service,
            bar,
        )?;
    }
    let mut settled_lunch_date = None;
    loop {
        let now = Local::now().naive_local();
        if is_after_daily_window(now) {
            if let Some(bar) = builder.flush_through(now)? {
                reconcile_and_process_bar(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    now,
                    &mut driver,
                    &mut bars,
                    &mut service,
                    bar,
                )?;
            }
            reconcile_terminal_boundary(
                &args,
                Path::new(&config.database),
                &mut initial_after_sequence,
                now,
                &mut driver,
            )?;
            return Ok(());
        }
        if is_lunch_settlement_window(now) {
            if settled_lunch_date == Some(now.date()) {
                thread::sleep(StdDuration::from_secs(15));
                continue;
            }
            if let Some(bar) = builder.flush_through(now)? {
                reconcile_and_process_bar(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    now,
                    &mut driver,
                    &mut bars,
                    &mut service,
                    bar,
                )?;
            }
            reconcile_terminal_boundary(
                &args,
                Path::new(&config.database),
                &mut initial_after_sequence,
                now,
                &mut driver,
            )?;
            settled_lunch_date = Some(now.date());
            thread::sleep(StdDuration::from_secs(15));
            continue;
        }
        if is_market_session(now) {
            if let Some(delay) = quote_probe_boundary_delay(now, args.interval_minutes)? {
                thread::sleep(delay);
                continue;
            }
            let quote = probe_current_market_quote(builder.quote_symbol())?;
            freshness.observe(
                &quote,
                chrono::Duration::seconds(args.maximum_unchanged_seconds),
            )?;
            append_json_line(&args.quote_log, &quote)?;
            let evidence_time = quote.observed_at;
            if let Some(bar) = builder.push(quote)? {
                reconcile_and_process_bar(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    evidence_time,
                    &mut driver,
                    &mut bars,
                    &mut service,
                    bar,
                )?;
            }
            if let Some(bar) = builder.flush_through(evidence_time)? {
                reconcile_and_process_bar(
                    &args,
                    Path::new(&config.database),
                    &mut initial_after_sequence,
                    evidence_time,
                    &mut driver,
                    &mut bars,
                    &mut service,
                    bar,
                )?;
            }
            thread::sleep(StdDuration::from_secs(args.poll_seconds));
        } else {
            thread::sleep(StdDuration::from_secs(15));
        }
    }
}

fn probe_remote_orders() -> Result<Vec<SimulationOrderRecord>> {
    probe_current_orders()?.records()
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
    driver: &mut MacOsSimulationUiDriver,
    bars: &mut BTreeMap<NaiveDateTime, MarketBar>,
    service: &mut GridAutomationService,
    bar: MarketBar,
) -> Result<()> {
    reconcile_ambiguous_terminal_fills(&args.outbox, now, driver)?;
    let mut remote_records = probe_remote_orders()?;
    verify_remote_contracts(&remote_records, &args.outbox, false)?;
    if run_outbox(args, source_database, initial_after_sequence, now, driver)? {
        remote_records = probe_remote_orders()?;
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
    driver: &mut MacOsSimulationUiDriver,
) -> Result<()> {
    reconcile_ambiguous_terminal_fills(&args.outbox, now, driver)?;
    let mut remote_records = probe_remote_orders()?;
    verify_remote_contracts(&remote_records, &args.outbox, false)?;
    if run_outbox(args, source_database, initial_after_sequence, now, driver)? {
        remote_records = probe_remote_orders()?;
    }
    remote_terminal_permit(&remote_records, source_database, &args.run_id, &args.outbox)?;
    Ok(())
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

#[derive(Debug, Default, Clone)]
struct PriceFreshness {
    session: Option<(NaiveDate, u8)>,
    last_observed_at: Option<NaiveDateTime>,
}

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

fn run_outbox(
    args: &Args,
    source_database: &Path,
    initial_after_sequence: &mut Option<i64>,
    now: NaiveDateTime,
    driver: &mut MacOsSimulationUiDriver,
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

fn is_market_session(now: NaiveDateTime) -> bool {
    if now.weekday().number_from_monday() > 5 {
        return false;
    }
    let time = now.time();
    (at(9, 30)..at(11, 30)).contains(&time) || (at(13, 0)..at(15, 0)).contains(&time)
}

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
    now.weekday().number_from_monday() <= 5 && now.time() >= at(14, 55)
}

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
