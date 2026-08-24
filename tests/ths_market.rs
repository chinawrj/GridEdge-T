use anyhow::Result;
use chrono::{Duration, NaiveDateTime};
use gridedge_t::{
    data::MarketBar,
    decision::{algorithm_from_config, DecisionRequest},
    event::EventType,
    journal::{EventReader, SqliteStore},
    service::GridAutomationService,
    ths_market::ThsQuoteBarBuilder,
    ths_sim::SimulationMarketQuote,
};
use rust_decimal::Decimal;
use std::str::FromStr;

#[test]
fn simulation_last_price_samples_never_form_completed_ohlc() -> Result<()> {
    let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
    assert_eq!(builder.quote_symbol(), "002256");

    assert!(builder
        .push(quote("2026-08-18 09:30:01", "3.55"))?
        .is_none());
    assert!(builder
        .push(quote("2026-08-18 09:32:00", "3.50"))?
        .is_none());
    assert!(builder
        .push(quote("2026-08-18 09:34:59", "3.57"))?
        .is_none());
    assert!(builder.push(quote("2026-08-18 09:35:01", "3.56")).is_err());
    assert!(builder.flush_through(at("2026-08-18 09:40:00")).is_err());
    Ok(())
}

#[test]
fn last_price_samples_cannot_claim_complete_ohlc_or_prove_that_no_grid_level_was_touched(
) -> Result<()> {
    let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;

    // These are valid reviewed snapshots, but they contain only last_price.
    // A real trade at 3.30 between the samples is observationally
    // indistinguishable from a flat 3.36 interval to the current DTO.  The
    // builder must not publish a complete OHLC bar (and thereby claim no
    // crossing) until same-page bucket OHLC or a complete, cursor-bounded
    // transaction reconstruction proves the high and low.
    builder.push(quote("2026-08-20 14:55:05", "3.36"))?;
    builder.push(quote("2026-08-20 14:59:55", "3.36"))?;

    assert!(
        builder
            .flush_through(at("2026-08-20 15:00:00"))
            .is_err(),
        "last-price sampling must fail closed instead of emitting a flat OHLC bar that can hide an intrabar 3.30 touch"
    );
    Ok(())
}

#[test]
fn quote_builder_never_invents_lunch_bars_or_accepts_future_reordering() -> Result<()> {
    let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
    builder.push(quote("2026-08-18 11:29:59", "3.55"))?;
    assert!(builder.flush_through(at("2026-08-18 11:30:00")).is_err());
    assert!(builder.push(quote("2026-08-18 12:00:00", "3.56")).is_err());
    let mut afternoon = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
    afternoon.push(quote("2026-08-18 13:00:01", "3.56"))?;
    assert!(afternoon
        .push(quote("2026-08-18 13:00:01", "3.57"))
        .is_err());
    assert!(afternoon
        .push(quote("2026-08-18 12:59:59", "3.57"))
        .is_err());
    Ok(())
}

#[test]
fn quote_builder_rejects_non_a_share_symbols_and_non_dividing_intervals() {
    assert!(ThsQuoteBarBuilder::new("002256", 5).is_err());
    assert!(ThsQuoteBarBuilder::new("002256.SZ", 7).is_err());
    assert!(ThsQuoteBarBuilder::new("bad.SZ", 5).is_err());
}

#[test]
fn persisted_quote_identity_and_price_are_validated_before_mutating_a_bar() -> Result<()> {
    let baseline = quote("2026-08-18 09:30:01", "3.55");
    let mut invalid_quotes = Vec::new();

    let mut invalid = baseline.clone();
    invalid.last_price = Decimal::ZERO;
    invalid_quotes.push(invalid);
    let mut invalid = baseline.clone();
    invalid.source_sha256 = "A".repeat(64);
    invalid_quotes.push(invalid);
    let mut invalid = baseline.clone();
    invalid.source_sha256 = "a".repeat(63);
    invalid_quotes.push(invalid);
    let mut invalid = baseline.clone();
    invalid.application_bundle = "unreviewed.bundle".to_owned();
    invalid_quotes.push(invalid);
    let mut invalid = baseline.clone();
    invalid.application_version = "5.4.0".to_owned();
    invalid_quotes.push(invalid);
    let mut invalid = baseline.clone();
    invalid.account_marker = "实盘".to_owned();
    invalid_quotes.push(invalid);

    for invalid in invalid_quotes {
        let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
        assert!(builder.push(invalid).is_err());
        assert!(builder.push(baseline.clone())?.is_none());
        assert!(builder.flush_through(at("2026-08-18 09:35:00")).is_err());
    }
    Ok(())
}

#[test]
fn current_bucket_is_never_flushed_before_its_exact_end() -> Result<()> {
    let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
    builder.push(quote("2026-08-18 09:30:01", "3.55"))?;

    assert!(builder.flush_through(at("2026-08-18 09:34:59"))?.is_none());
    assert!(builder.flush_through(at("2026-08-18 09:35:00")).is_err());
    Ok(())
}

#[test]
fn quote_evidence_that_crosses_a_five_minute_boundary_is_rejected_atomically() -> Result<()> {
    let mut builder = ThsQuoteBarBuilder::new("002256.SZ", 5)?;
    let mut crossed = quote("2026-08-18 09:35:01", "3.56");
    crossed.observed_from = at("2026-08-18 09:34:59");

    assert!(builder.push(crossed).is_err());
    assert!(builder
        .push(quote("2026-08-18 09:35:02", "3.57"))?
        .is_none());
    assert!(builder.flush_through(at("2026-08-18 09:40:00")).is_err());
    Ok(())
}

#[test]
fn deployment_config_keeps_half_cash_and_uses_only_the_numeric_position_envelope() -> Result<()> {
    let config = gridedge_t::Config::load("configs/ths_002256_sim.yaml")?;
    let capital = config
        .gate
        .capital_inventory
        .as_ref()
        .expect("resource-aware deployment requires capital settings");

    assert_eq!(config.symbol, "002256.SZ");
    assert_eq!(config.grid_ratio, Decimal::from_str("0.015")?);
    assert_eq!(config.initial_cash, Decimal::from_str("200000.00")?);
    assert_eq!(capital.minimum_free_cash, Decimal::from_str("100000.00")?);
    assert_eq!(config.max_position, gridedge_t::config::MAX_SHARE_QUANTITY);
    assert_eq!(
        capital.target_position,
        gridedge_t::config::MAX_SHARE_QUANTITY
    );
    assert_eq!(config.standard_quantity, 5500);
    assert_eq!(config.trade_levels, 5);
    config.validate()?;
    Ok(())
}

#[test]
fn legacy_v4_prefix_keeps_its_historical_cash_floor_before_policy_activation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = gridedge_t::Config::load("configs/ths_002256_sim.yaml")?;
    config.database = directory.path().join("cash-floor.db").display().to_string();
    config.initial_cash = Decimal::from_str("110000.00")?;
    config.config_version = "ths-cash-floor-regression".to_owned();
    config.validate()?;
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some("ths-cash-floor".to_owned()),
    )?;

    let start = at("2026-08-18 09:35:00");
    for offset in 0..20 {
        let close = match offset {
            0..=14 => Decimal::from_str("3.70")?,
            15 => Decimal::from_str("3.68")?,
            16 => Decimal::from_str("3.63")?,
            17 => Decimal::from_str("3.58")?,
            18 => Decimal::from_str("3.53")?,
            // Stay above the 1.5% first BUY level (3.498) so the opportunity
            // is evaluated only after all twenty prefix bars are processed.
            19 => Decimal::from_str("3.51")?,
            _ => unreachable!(),
        };
        service.on_bar(&flat_bar(
            start + Duration::minutes(i64::from(offset) * 5),
            &config.symbol,
            close,
        ))?;
    }
    service.on_bar(&MarketBar {
        timestamp: start + Duration::minutes(100),
        symbol: config.symbol.clone(),
        open: Decimal::from_str("3.51")?,
        high: Decimal::from_str("3.51")?,
        low: Decimal::from_str("3.47")?,
        close: Decimal::from_str("3.47")?,
        volume: 0,
        amount: None,
    })?;

    assert_eq!(
        service.state.cash.available,
        Decimal::from_str("110000.00")?
    );
    assert!(service.state.orders.is_empty());
    assert_eq!(service.state.position.total, 0);
    let events = service.store.load_after("ths-cash-floor", 0)?;
    let decision = events
        .iter()
        .rev()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .expect("cash floor case must reach a final market-passing opportunity");
    let request: DecisionRequest = serde_json::from_value(decision.payload["request"].clone())?;
    let resource = request
        .context
        .funds_inventory
        .expect("deployment uses resource-aware v4 evidence");
    assert!(service.state.ongoing_resource_policy.is_none());
    assert!(resource.market_signal_passed);
    assert_eq!(resource.cash_affordable_units, 0);
    assert_eq!(resource.predecision_blocked_units, 1);
    Ok(())
}

fn quote(timestamp: &str, price: &str) -> SimulationMarketQuote {
    SimulationMarketQuote {
        observed_from: at(timestamp),
        observed_at: at(timestamp),
        symbol: "002256".to_owned(),
        last_price: Decimal::from_str(price).unwrap(),
        source_sha256: "a".repeat(64),
        application_bundle: "cn.com.10jqka.macstockPro".to_owned(),
        application_version: "5.3.2".to_owned(),
        account_marker: "模拟练习".to_owned(),
    }
}

fn flat_bar(timestamp: NaiveDateTime, symbol: &str, close: Decimal) -> MarketBar {
    MarketBar {
        timestamp,
        symbol: symbol.to_owned(),
        open: close,
        high: close,
        low: close,
        close,
        volume: 0,
        amount: None,
    }
}

fn at(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}
