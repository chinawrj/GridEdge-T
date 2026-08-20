use chrono::{NaiveDate, NaiveDateTime};
use gridedge_t::{
    config::Config,
    data::{CsvReplayFeed, MarketBar},
    decision::{
        whole_unit_partition, DecisionRequest, DecisionResponse, GridRight, GridRightCapacity,
        GridRightStatus, ProcessAlgorithm, QuantDecisionAlgorithm, RightDecision, RightTranche,
        WHOLE_UNIT_DECISION_CONTRACT_VERSION,
    },
    domain::{
        AccountSnapshot, Direction, Fill, LotQuantityAllocation, Order, OrderIntent, OrderStatus,
        ServiceMode, StrategyState, TradingLot,
    },
    event::{current_event_schema, EventEnvelope, EventType},
    execution::{ExecutionGateway, ExecutionReport, PaperExecutionGateway},
    gate::{
        context_hash, context_hash_v4, legacy_v2_context_hash, FixedGate, GateContext,
        GateDecision, GatePolicy, SimpleRuleGate,
    },
    grid::{available_deferred_budget, board_lot_quantity, crossed_levels, GridSpec},
    journal::{AppendOutcome, EventReader, SqliteStore, StateReader},
    ledger::LedgerWriter,
    profit::{
        evaluate_sell_slice_with_policy, plan_non_loss_sales, prove_non_loss,
        unrealized_grid_valuation, UNREALIZED_VALUATION_POLICY_VERSION,
    },
    rights::{sell_tranches_to_mint, RightsGateCoordinator},
    risk::{
        affordable_buy_units, canonical_approval, canonical_approved_quantity,
        estimated_buy_reservation, DefaultRiskChecker, RiskChecker,
    },
    service::{GridAutomationService, WorkflowFaultPoint},
};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Deserialize;
use serde_json::json;
use std::{
    process::Command,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tempfile::TempDir;

use proptest::prelude::*;

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}
fn dt(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}

fn config_in(temp: &TempDir) -> Config {
    let mut config = Config::load("configs/default.yaml").unwrap();
    config.database = temp.path().join("test.db").to_string_lossy().to_string();
    config
}

#[test]
fn financial_configuration_rejects_every_unsafe_boundary_before_storage() {
    let base = Config::load("configs/default.yaml").unwrap();
    let mut invalid = Vec::new();
    macro_rules! case {
        ($field:ident, $value:expr) => {{
            let mut candidate = base.clone();
            candidate.$field = $value;
            invalid.push(candidate);
        }};
    }
    case!(commission_rate, d("-0.0001"));
    case!(commission_rate, d("0.1001"));
    case!(minimum_commission, d("-0.01"));
    case!(sell_tax_rate, d("-0.0001"));
    case!(sell_tax_rate, d("0.1001"));
    case!(slippage_rate, d("-0.0001"));
    case!(slippage_rate, Decimal::ONE);
    case!(hysteresis_ratio, d("-0.0001"));
    case!(hysteresis_ratio, d("1.0001"));
    case!(initial_position, -1);
    case!(initial_sellable, -1);
    case!(min_base_position, -1);
    case!(max_position, -1);
    case!(price_scale, 9);
    let mut sellable = base.clone();
    sellable.initial_sellable = sellable.initial_position + 1;
    invalid.push(sellable);
    let mut base_exceeds = base.clone();
    base_exceeds.min_base_position = base_exceeds.initial_position + 1;
    invalid.push(base_exceeds);
    for candidate in invalid {
        assert!(candidate.validate().is_err(), "unsafe config was accepted");
    }
    assert!(base.validate().is_ok());

    let mut both_units = base.clone();
    both_units.standard_budget = d("15000");
    assert!(both_units.validate().is_err());
    let mut no_unit = base.clone();
    no_unit.standard_quantity = 0;
    assert!(no_unit.validate().is_err());
    let mut fractional_board_lot = base.clone();
    fractional_board_lot.standard_quantity = 1_550;
    assert!(fractional_board_lot.validate().is_err());
}

fn assert_grid_config_rejected_without_storage(
    mut config: Config,
    run_id: &str,
    start_only_after_validation_rejects: bool,
) {
    let temp = TempDir::new().unwrap();
    let database = temp.path().join(format!("{run_id}.db"));
    config.database = database.to_string_lossy().to_string();
    let validation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| config.validate()));
    let validation_rejected = matches!(validation, Ok(Err(_)));
    let start = if start_only_after_validation_rejects && !validation_rejected {
        None
    } else {
        let store = SqliteStore::open(&config.database).unwrap();
        Some(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                GridAutomationService::start_new(
                    config,
                    store,
                    Box::new(gridedge_t::gate::AlwaysExecuteGate),
                    Some(run_id.to_owned()),
                )
            },
        )))
    };

    assert!(
        validation_rejected,
        "unsafe derived grid geometry passed Config::validate"
    );
    assert!(
        matches!(start, Some(Ok(Err(_)))),
        "unsafe derived grid geometry did not return a normal start error"
    );

    let connection = rusqlite::Connection::open(database).unwrap();
    for table in ["events", "snapshots", "paper_accounts", "paper_orders"] {
        let exists: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        if exists == 1 {
            let count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "unsafe config wrote {table}");
        }
    }
}

#[test]
fn maximum_decimal_anchor_cannot_overflow_derived_grid_prices_or_write_storage() {
    let mut config = Config::load("configs/default.yaml").unwrap();
    config.anchor_price = Decimal::MAX;
    config.grid_ratio = d("0.99999999");
    assert_grid_config_rejected_without_storage(config, "maximum-anchor-grid", false);
}

#[test]
fn a_positive_anchor_that_rounds_any_grid_node_to_zero_is_rejected_before_storage() {
    let mut config = Config::load("configs/default.yaml").unwrap();
    config.anchor_price = d("0.001");
    config.grid_ratio = d("0.99");
    config.price_scale = 2;
    assert_grid_config_rejected_without_storage(config, "zero-rounded-grid", false);
}

#[test]
fn an_i32_max_grid_boundary_is_rejected_before_level_enumeration_or_storage() {
    let mut config = Config::load("configs/default.yaml").unwrap();
    config.trade_levels = i32::MAX - 1;
    config.boundary_levels = i32::MAX;
    // Calling start while the old validator accepts this value would attempt
    // billions of level allocations.  Once validation rejects it, this same
    // helper also proves start returns the error before migration or writes.
    assert_grid_config_rejected_without_storage(config, "i32-max-grid", true);
}

#[test]
fn legacy_configuration_hash_is_stable_when_new_default_fields_are_added() {
    let config = Config::load("configs/zhaoxin_5m_certification_v3.yaml").unwrap();
    assert_eq!(
        config.legacy_content_sha256_without_hold_open().unwrap(),
        "db62b4bfec9100e21fc1f9385a3f3ba40825f14f10b22f7941ac83dbed72886e"
    );
    let config_with_hold_open = Config::load("configs/zhaoxin_5m_certification_v5.yaml").unwrap();
    assert_eq!(
        config_with_hold_open
            .legacy_content_sha256_without_standard_quantity(true)
            .unwrap(),
        "f293cb4323fa4886d07b0792a93d2184856cfa2ab04f1e89ed811025b9566065"
    );
}

#[test]
fn current_configuration_snapshot_requires_a_valid_content_hash() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let mut store = SqliteStore::open(temp.path().join("config-anchor.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "config-anchor".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let before = serde_json::to_value(&state).unwrap();
    let mut sequence = 0;
    let run_id = state.run_id.clone();
    let cycle_id = state.cycle_id.clone();
    let symbol = state.symbol.clone();
    let make_snapshot = |key: &str, payload| {
        EventEnvelope::new(
            EventType::ConfigSnapshotted,
            &run_id,
            &cycle_id,
            &symbol,
            dt("2026-01-05 09:30:00"),
            key,
            None,
            key.into(),
            payload,
            &config.config_version,
        )
    };
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(make_snapshot(
            "missing-hash",
            serde_json::to_value(&config).unwrap(),
        ))
        .is_err());
    let mut forged = config_snapshot_payload(&config);
    forged["_content_sha256"] = json!("00");
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(make_snapshot("forged-hash", forged))
        .is_err());
    let mut wrong_symbol = config.clone();
    wrong_symbol.symbol = "000001.SZ".into();
    let mut wrong_anchor = config.clone();
    wrong_anchor.anchor_price += Decimal::ONE;
    let mut wrong_version = config.clone();
    wrong_version.config_version = "forged-version".into();
    for (key, candidate) in [
        ("wrong-symbol", wrong_symbol),
        ("wrong-anchor", wrong_anchor),
        ("wrong-version", wrong_version),
    ] {
        assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(make_snapshot(key, config_snapshot_payload(&candidate)))
            .is_err());
    }
    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);

    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(make_snapshot(
            "valid-hash",
            config_snapshot_payload(&config),
        ))
        .unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(state.audited_config.as_ref(), Some(&config));
}

#[test]
fn legacy_budget_configuration_cannot_start_a_new_run_or_migrate_storage() {
    let temp = TempDir::new().unwrap();
    let database = temp.path().join("legacy-new-run.db");
    let mut config = Config::load("configs/zhaoxin_5m_certification_v6.yaml").unwrap();
    config.database = database.to_string_lossy().to_string();
    let store = SqliteStore::open(&config.database).unwrap();
    let result = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("forbidden-legacy-new-run".into()),
    );
    assert!(result.is_err());
    let connection = rusqlite::Connection::open(database).unwrap();
    let event_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='events'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_table_count, 0);
}

fn write_paper_snapshot(config: &Config, run_id: &str, snapshot: &AccountSnapshot) {
    let connection = rusqlite::Connection::open(&config.database).unwrap();
    connection
        .execute(
            "UPDATE paper_accounts SET snapshot_json=?1 WHERE run_id=?2",
            rusqlite::params![serde_json::to_string(snapshot).unwrap(), run_id],
        )
        .unwrap();
}

fn bar(time: &str, open: &str, high: &str, low: &str, close: &str) -> MarketBar {
    MarketBar {
        timestamp: dt(time),
        symbol: "600000.SH".to_owned(),
        open: d(open),
        high: d(high),
        low: d(low),
        close: d(close),
        volume: 100_000,
        amount: None,
    }
}

#[test]
fn symmetric_geometric_grid_and_boundary_are_exact() {
    let config = Config::load("configs/default.yaml").unwrap();
    let grid = GridSpec::from(&config);
    assert_eq!(grid.price(1).unwrap(), d("10.20"));
    assert_eq!(grid.price(-1).unwrap(), d("9.80"));
    assert_eq!(grid.price(4).unwrap(), d("10.82"));
    assert_eq!(grid.price(-5).unwrap(), d("9.06"));
    assert!(grid.is_trade_level(4));
    assert!(!grid.is_trade_level(5));
    assert_eq!(
        grid.price(2).unwrap(),
        (d("10") * d("1.02") * d("1.02")).round_dp(2)
    );
}

#[test]
fn an_opening_gap_records_every_mechanical_level_crossed_between_bars() {
    let config = Config::load("configs/default.yaml").unwrap();
    let levels = GridSpec::from(&config).levels().unwrap();
    let (down, down_classification) = crossed_levels(&levels, d("10"), d("9.59"), d("9.60"));
    assert_eq!(down, vec![-2, -1]);
    assert_eq!(
        down_classification,
        gridedge_t::grid::TouchClassification::Normal
    );

    let (up, up_classification) = crossed_levels(&levels, d("10"), d("10.40"), d("10.41"));
    assert_eq!(up, vec![1, 2]);
    assert_eq!(
        up_classification,
        gridedge_t::grid::TouchClassification::Normal
    );
}

#[test]
fn service_and_ledger_commit_every_level_crossed_by_an_opening_gap() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FixedGate {
            probability: Decimal::ZERO,
            alpha: Decimal::ZERO,
        }),
        Some("opening-gap-ledger".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let gap = bar("2026-01-05 09:31:00", "9.60", "9.60", "9.59", "9.60");
    service.on_bar(&gap).unwrap();
    let events = service.store.load_after("opening-gap-ledger", 0).unwrap();
    let mut touched = events
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridLevelTouched && event.event_time == gap.timestamp
        })
        .map(|event| event.payload["grid_index"].as_i64().unwrap())
        .collect::<Vec<_>>();
    touched.sort();
    assert_eq!(touched, vec![-2, -1]);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == EventType::MarketBarDecisionsCommitted
                    && event.event_time == gap.timestamp
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == EventType::MarketBarProcessed
                    && event.event_time == gap.timestamp
            })
            .count(),
        1
    );
}

#[test]
fn legacy_deferred_budget_uses_entire_mechanical_cap() {
    assert_eq!(
        available_deferred_budget(-1, d("15000"), Decimal::ZERO).unwrap(),
        d("15000")
    );
    assert_eq!(
        available_deferred_budget(-2, d("15000"), Decimal::ZERO).unwrap(),
        d("30000")
    );
    let third = available_deferred_budget(-3, d("15000"), Decimal::ZERO).unwrap();
    assert_eq!(third, d("45000"));
    assert_eq!(third * d("0.5"), d("22500.0"));
    assert_eq!(
        available_deferred_budget(-3, d("15000"), d("46000")).unwrap(),
        Decimal::ZERO
    );
}

#[test]
fn fixed_share_unit_never_changes_with_the_grid_price() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(FixedGate {
            probability: Decimal::ONE,
            alpha: Decimal::ONE,
        }),
        Some("fixed-share-unit".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-05 09:33:00", "9.62", "9.63", "9.40", "9.43"),
    ] {
        service.on_bar(&market).unwrap();
    }

    let mut orders: Vec<_> = service.state.orders.values().collect();
    orders.sort_by_key(|order| order.intent.grid_index);
    assert_eq!(orders.len(), 3);
    assert!(orders
        .iter()
        .all(|order| order.intent.quantity == config.standard_quantity));
    assert!(orders
        .windows(2)
        .all(|pair| pair[0].intent.budget != pair[1].intent.budget));

    let buy_tranches: Vec<_> = service
        .state
        .right_tranches
        .values()
        .filter(|tranche| tranche.direction == Direction::Buy)
        .collect();
    assert_eq!(buy_tranches.len(), 3);
    assert!(buy_tranches.iter().all(|tranche| {
        tranche.minted_quantity == config.standard_quantity
            && tranche.minted_budget == Decimal::ZERO
    }));
    for event in service
        .store
        .load_after("fixed-share-unit", 0)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == EventType::GateDecisionMade)
    {
        let request: DecisionRequest =
            serde_json::from_value(event.payload["request"].clone()).unwrap();
        let response: gridedge_t::decision::DecisionResponse =
            serde_json::from_value(event.payload["response"].clone()).unwrap();
        response.validate_for(&request).unwrap();
        let (exercise_quantity, defer_quantity) = response.outcome.quantities();
        assert_eq!(
            exercise_quantity + defer_quantity,
            request.context.available_quantity
        );
        assert_eq!(exercise_quantity % config.lot_size, 0);

        let mut forged = response.clone();
        forged.outcome = gridedge_t::decision::RightDecision::Exercise {
            exercise_quantity,
            defer_quantity: defer_quantity + config.lot_size,
        };
        assert!(forged.validate_for(&request).is_err());
    }
}

proptest! {
    #[test]
    fn every_budget_tranche_partition_obeys_double_entry_conservation(
        consumed in 0_i64..=15_000,
        revoked_seed in 0_i64..=15_000,
    ) {
        let remaining = 15_000 - consumed;
        let revoked = revoked_seed.min(remaining);
        let mut tranche = RightTranche::minted_budget(
            "cycle",
            "excursion",
            "right",
            -1,
            d("15000"),
        );
        tranche.available_budget = Decimal::from(remaining - revoked);
        tranche.consumed_budget = Decimal::from(consumed);
        tranche.revoked_budget = Decimal::from(revoked);
        prop_assert!(tranche.validate().is_ok());

        tranche.available_budget += Decimal::ONE;
        prop_assert!(tranche.validate().is_err());
    }

    #[test]
    fn every_quantity_tranche_partition_obeys_double_entry_conservation(
        reserved in 0_i64..=2_000,
        consumed_seed in 0_i64..=2_000,
    ) {
        let consumed = consumed_seed.min(2_000 - reserved);
        let mut tranche = RightTranche::minted_quantity(
            "cycle",
            "excursion",
            "right",
            1,
            "lot-1",
            2_000,
        );
        tranche.available_quantity = 2_000 - reserved - consumed;
        tranche.reserved_quantity = reserved;
        tranche.consumed_quantity = consumed;
        prop_assert!(tranche.validate().is_ok());

        tranche.revoked_quantity = 1;
        prop_assert!(tranche.validate().is_err());
    }

    #[test]
    fn raising_the_sell_price_never_shrinks_the_non_loss_set(
        cost_cents in 10_000_i64..=200_000,
        quantity in 1_i64..=2_000,
        low_price_cents in 100_i64..=5_000,
        price_step_cents in 0_i64..=1_000,
    ) {
        let config = Config::load("configs/default.yaml").unwrap();
        let lot = priced_lot(
            "monotone",
            -1,
            quantity,
            Decimal::new(cost_cents, 2),
        );
        let low = Decimal::new(low_price_cents, 2);
        let high = Decimal::new(low_price_cents + price_step_cents, 2);
        let low_is_safe = prove_non_loss(&config, &lot, quantity, low).is_some();
        let high_is_safe = prove_non_loss(&config, &lot, quantity, high).is_some();
        prop_assert!(!low_is_safe || high_is_safe);
    }
}

#[test]
fn buy_quantity_rounds_down_to_board_lot() {
    assert_eq!(
        board_lot_quantity(d("15000"), d("9.80"), 100).unwrap(),
        1500
    );
    assert_eq!(board_lot_quantity(d("99"), d("10"), 100).unwrap(), 0);
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RightsCaseCatalog {
    version: u32,
    allocation_policy: String,
    standard_quantity: i64,
    cases: Vec<RightsCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseCoverageContract {
    case_schema: String,
    required_case_ids: Vec<String>,
    dimensions: std::collections::BTreeMap<String, std::collections::BTreeMap<String, Vec<String>>>,
    automatic_expansions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RightsCase {
    id: String,
    description: String,
    #[serde(default)]
    config: CaseConfigOverrides,
    decisions: Vec<CaseDecision>,
    bars: Vec<[String; 5]>,
    expected_orders: usize,
    expected_ambiguous_bars: u64,
    expected_events: std::collections::BTreeMap<String, usize>,
    forbidden_events: Vec<String>,
    #[serde(default)]
    expected_touches: std::collections::BTreeMap<i32, usize>,
    #[serde(default)]
    expected_rights: Vec<ExpectedRightSummary>,
    #[serde(default)]
    expected_order_quantities: std::collections::BTreeMap<String, Vec<i64>>,
    #[serde(default = "default_true")]
    restart_each_bar: bool,
    #[serde(default)]
    equivalent_to: Option<String>,
    balances: Vec<ExpectedTrancheBalance>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseConfigOverrides {
    #[serde(default)]
    initial_cash: Option<String>,
    #[serde(default)]
    initial_position: Option<i64>,
    #[serde(default)]
    initial_sellable: Option<i64>,
    #[serde(default)]
    max_position: Option<i64>,
    #[serde(default)]
    min_base_position: Option<i64>,
    #[serde(default)]
    commission_rate: Option<String>,
    #[serde(default)]
    minimum_commission: Option<String>,
    #[serde(default)]
    sell_tax_rate: Option<String>,
    #[serde(default)]
    slippage_rate: Option<String>,
}

impl CaseConfigOverrides {
    fn apply(&self, config: &mut Config) {
        if let Some(value) = &self.initial_cash {
            config.initial_cash = d(value);
        }
        if let Some(value) = self.initial_position {
            config.initial_position = value;
        }
        if let Some(value) = self.initial_sellable {
            config.initial_sellable = value;
        }
        if let Some(value) = self.max_position {
            config.max_position = value;
        }
        if let Some(value) = self.min_base_position {
            config.min_base_position = value;
        }
        if let Some(value) = &self.commission_rate {
            config.commission_rate = d(value);
        }
        if let Some(value) = &self.minimum_commission {
            config.minimum_commission = d(value);
        }
        if let Some(value) = &self.sell_tax_rate {
            config.sell_tax_rate = d(value);
        }
        if let Some(value) = &self.slippage_rate {
            config.slippage_rate = d(value);
        }
        config.validate().expect("case config override is valid");
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseDecision {
    at: String,
    direction: String,
    grid_index: i32,
    exercise_quantity: i64,
    defer_quantity: i64,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedRightSummary {
    direction: String,
    grid_index: i32,
    count: usize,
    exercise_quantity: i64,
    defer_quantity: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedTrancheBalance {
    direction: String,
    birth_grid_index: i32,
    count: usize,
    #[serde(default)]
    available_quantity: Option<i64>,
    #[serde(default)]
    exercised_quantity: Option<i64>,
    #[serde(default)]
    revoked_quantity: Option<i64>,
    #[serde(default)]
    minted_quantity: Option<i64>,
}

struct CatalogAlgorithm {
    decisions: std::collections::BTreeMap<String, (i64, i64)>,
}

impl CatalogAlgorithm {
    fn from_case(case: &RightsCase) -> Self {
        let decisions = case
            .decisions
            .iter()
            .map(|decision| {
                (
                    format!(
                        "{}|{}:{}",
                        decision.at, decision.direction, decision.grid_index
                    ),
                    (decision.exercise_quantity, decision.defer_quantity),
                )
            })
            .collect();
        Self { decisions }
    }
}

impl QuantDecisionAlgorithm for CatalogAlgorithm {
    fn decide(&self, request: &DecisionRequest) -> anyhow::Result<DecisionResponse> {
        let context = &request.context;
        let direction = format!("{:?}", context.direction).to_uppercase();
        let key = format!(
            "{}|{}:{}",
            context.current_time, direction, context.grid_index
        );
        let (exercise_quantity, defer_quantity) = self
            .decisions
            .get(&key)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unscripted case decision {key}"))?;
        let alpha = if context.available_quantity == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(exercise_quantity) / Decimal::from(context.available_quantity)
        };
        let decision = GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if exercise_quantity == 0 {
                "SKIP"
            } else {
                "EXECUTE"
            }
            .into(),
            reason_codes: vec!["CASE_CATALOG".into()],
            model_name: "case-catalog-algorithm".into(),
            model_version: "2".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        };
        let outcome = if exercise_quantity > 0 {
            RightDecision::Exercise {
                exercise_quantity,
                defer_quantity,
            }
        } else {
            RightDecision::Defer { defer_quantity }
        };
        let response = DecisionResponse {
            contract_version: request.contract_version,
            request_id: request.request_id.clone(),
            right_id: request.right.right_id.clone(),
            outcome,
            decision,
        };
        response.validate_for(request)?;
        Ok(response)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn generated_buy_random_walk_matches_the_independent_layer_stack_model(
        moves in proptest::collection::vec(any::<bool>(), 1..24)
    ) {
        let temp = TempDir::new().unwrap();
        let config = config_in(&temp);
        let grid = GridSpec::from(&config);
        let start = dt("2026-02-02 09:30:00");
        let mut depth = 0_i32;
        let mut transitions = Vec::new();
        let mut decisions = std::collections::BTreeMap::new();
        let mut minted = [0_i64; 5];
        let mut active = [0_i64; 5];
        let mut revoked = [0_i64; 5];
        for (offset, deepen) in moves.into_iter().enumerate() {
            let next = if depth == 0 {
                1
            } else if depth == config.trade_levels {
                depth - 1
            } else if deepen {
                depth + 1
            } else {
                depth - 1
            };
            let time = start + chrono::Duration::minutes((offset + 1) as i64);
            if next > depth {
                minted[next as usize] += 1;
                active[next as usize] += 1;
                decisions.insert(
                    format!("{}|BUY:{}", time, -next),
                    (0, i64::from(next) * config.standard_quantity),
                );
            } else {
                prop_assert!(active[depth as usize] > 0);
                active[depth as usize] -= 1;
                revoked[depth as usize] += 1;
            }
            transitions.push((time, depth, next));
            depth = next;
        }
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new_with_algorithm(
            config.clone(),
            store,
            Box::new(CatalogAlgorithm { decisions }),
            Some("generated-layer-stack".into()),
        )
        .unwrap();
        service
            .on_bar(&MarketBar {
                timestamp: start,
                symbol: config.symbol.clone(),
                open: config.anchor_price,
                high: config.anchor_price,
                low: config.anchor_price,
                close: config.anchor_price,
                volume: 100_000,
                amount: None,
            })
            .unwrap();
        for (time, from, to) in transitions {
            let from_price = if from == 0 {
                config.anchor_price
            } else {
                grid.price(-from).unwrap()
            };
            let to_price = if to == 0 {
                config.anchor_price
            } else {
                grid.price(-to).unwrap()
            };
            service
                .on_bar(&MarketBar {
                    timestamp: time,
                    symbol: config.symbol.clone(),
                    open: from_price,
                    high: from_price.max(to_price),
                    low: from_price.min(to_price),
                    close: to_price,
                    volume: 100_000,
                    amount: None,
                })
                .unwrap();
            service.state.validate_invariants().unwrap();
        }
        prop_assert!(service.state.orders.is_empty());
        for layer in 1..=config.trade_levels as usize {
            let matching: Vec<_> = service
                .state
                .right_tranches
                .values()
                .filter(|tranche| {
                    tranche.direction == Direction::Buy
                        && tranche.birth_grid_index == -(layer as i32)
                })
                .collect();
            prop_assert_eq!(matching.len() as i64, minted[layer]);
            prop_assert_eq!(
                matching
                    .iter()
                    .map(|tranche| tranche.available_quantity)
                    .sum::<i64>(),
                active[layer] * config.standard_quantity
            );
            prop_assert_eq!(
                matching
                    .iter()
                    .map(|tranche| tranche.revoked_quantity)
                    .sum::<i64>(),
                revoked[layer] * config.standard_quantity
            );
            prop_assert_eq!(
                matching
                    .iter()
                    .map(|tranche| tranche.consumed_quantity)
                    .sum::<i64>(),
                0
            );
        }
    }
}

fn run_rights_case(
    case: &RightsCase,
    temp: &TempDir,
    recover_each_bar: bool,
    duplicate_each_bar: bool,
) -> GridAutomationService {
    let mut config = config_in(temp);
    case.config.apply(&mut config);
    let run_id = format!("case-{}", case.id);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        store,
        Box::new(CatalogAlgorithm::from_case(case)),
        Some(run_id.clone()),
    )
    .unwrap();
    for (position, values) in case.bars.iter().enumerate() {
        if recover_each_bar && position > 0 {
            drop(service);
            service = GridAutomationService::recover_with_algorithm(
                config.clone(),
                SqliteStore::open(&config.database).unwrap(),
                Box::new(CatalogAlgorithm::from_case(case)),
                run_id.clone(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "case {} recovery at bar {position} failed: {error:#}",
                    case.id
                )
            });
        }
        let market = bar(&values[0], &values[1], &values[2], &values[3], &values[4]);
        service
            .on_bar(&market)
            .unwrap_or_else(|error| panic!("case {} failed: {error:#}", case.id));
        service.state.validate_invariants().unwrap_or_else(|error| {
            panic!(
                "case {} invariant failure after bar {position}: {error:#}",
                case.id
            )
        });
        if duplicate_each_bar {
            service
                .on_bar(&market)
                .unwrap_or_else(|error| panic!("case {} duplicate bar failed: {error:#}", case.id));
            service.state.validate_invariants().unwrap_or_else(|error| {
                panic!(
                    "case {} invariant failure after duplicate bar {position}: {error:#}",
                    case.id
                )
            });
        }
    }
    service
}

fn rights_case_opportunity_trace(service: &GridAutomationService) -> Vec<(EventType, String)> {
    service
        .store
        .load_after(&service.state.run_id, 0)
        .unwrap()
        .into_iter()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::GridLevelTouched
                    | EventType::GridLevelSkipped
                    | EventType::GridLevelRearmed
                    | EventType::GridRightGranted
                    | EventType::GridRightDeferred
                    | EventType::GridRightBlocked
                    | EventType::GridRightReserved
                    | EventType::GridRightTransferred
                    | EventType::GridRightRevoked
                    | EventType::RightTrancheMinted
                    | EventType::RightBalanceTransferred
                    | EventType::RightBalanceReserved
                    | EventType::RightBalanceRevoked
                    | EventType::DeferredQuantityAccrued
                    | EventType::GateDecisionMade
                    | EventType::OrderIntentCreated
                    | EventType::AmbiguousBarDetected
                    | EventType::ErrorRecorded
            )
        })
        .map(|event| (event.event_type, event.payload.to_string()))
        .collect()
}

fn rights_case_semantic_summary(state: &StrategyState) -> serde_json::Value {
    let mut rights: Vec<_> = state
        .grid_rights
        .values()
        .map(|right| {
            (
                format!("{:?}", right.direction),
                right.grid_index,
                right.decided_exercise_quantity,
                right.decided_defer_quantity,
                right.capacity.carried_in_quantity,
                right.capacity.source_right_ids.len(),
            )
        })
        .collect();
    rights.sort();
    let mut tranches: Vec<_> = state
        .right_tranches
        .values()
        .map(|tranche| {
            (
                format!("{:?}", tranche.direction),
                tranche.birth_grid_index,
                tranche.minted_quantity,
                tranche.revoked_quantity,
            )
        })
        .collect();
    tranches.sort();
    let mut orders: Vec<_> = state
        .orders
        .values()
        .map(|order| {
            (
                format!("{:?}", order.intent.direction),
                order.intent.grid_index,
                order.intent.quantity,
            )
        })
        .collect();
    orders.sort();
    json!({
        "rights": rights,
        "tranches": tranches,
        "orders": orders,
        "touched": state.historically_touched_levels,
        "last_price": state.last_price.map(|price| price.to_string())
    })
}

#[derive(Debug, Clone)]
struct OpportunityTrancheBalance {
    tranche: RightTranche,
    exercised_quantity: i64,
    revoked_quantity: i64,
}

fn opportunity_tranche_balances(
    events: &[EventEnvelope],
) -> std::collections::BTreeMap<String, OpportunityTrancheBalance> {
    let mut balances = std::collections::BTreeMap::new();
    for event in events {
        match event.event_type {
            EventType::RightTrancheMinted => {
                let tranche: RightTranche = serde_json::from_value(event.payload.clone()).unwrap();
                balances.insert(
                    tranche.tranche_id.clone(),
                    OpportunityTrancheBalance {
                        tranche,
                        exercised_quantity: 0,
                        revoked_quantity: 0,
                    },
                );
            }
            EventType::RightBalanceReserved | EventType::RightBalanceRevoked => {
                for allocation in event.payload["allocations"].as_array().unwrap() {
                    let tranche_id = allocation["tranche_id"].as_str().unwrap();
                    let quantity = allocation["quantity"].as_i64().unwrap_or(0);
                    let balance = balances
                        .get_mut(tranche_id)
                        .expect("opportunity allocation must reference a minted tranche");
                    if event.event_type == EventType::RightBalanceReserved {
                        balance.exercised_quantity += quantity;
                    } else {
                        balance.revoked_quantity += quantity;
                    }
                }
            }
            _ => {}
        }
    }
    balances
}

#[test]
fn machine_readable_rights_case_catalog_is_the_executable_semantic_contract() {
    let catalog: RightsCaseCatalog =
        serde_yaml::from_str(include_str!("fixtures/rights_cases.yaml")).unwrap();
    assert_eq!(catalog.version, 4);
    assert_eq!(catalog.allocation_policy, "DEEPEST_FIRST_V1");
    assert_eq!(catalog.standard_quantity, 1_500);
    assert!(catalog.cases.len() >= 20);
    let coverage: CaseCoverageContract =
        serde_yaml::from_str(include_str!("fixtures/case_coverage.yaml")).unwrap();
    assert_eq!(coverage.case_schema, "gridedge.case-coverage/v1");
    let case_ids: std::collections::BTreeSet<_> =
        catalog.cases.iter().map(|case| case.id.as_str()).collect();
    assert_eq!(
        case_ids.len(),
        catalog.cases.len(),
        "case IDs must be unique"
    );
    for required in &coverage.required_case_ids {
        assert!(
            case_ids.contains(required.as_str()),
            "missing case {required}"
        );
    }
    for (dimension, values) in &coverage.dimensions {
        assert!(!values.is_empty(), "empty coverage dimension {dimension}");
        for (value, cases) in values {
            assert!(
                !cases.is_empty(),
                "coverage value {dimension}.{value} has no case"
            );
            for case_id in cases {
                assert!(
                    case_ids.contains(case_id.as_str()),
                    "coverage {dimension}.{value} references missing case {case_id}"
                );
            }
        }
    }
    let required_expansions = [
        "RESTART_BEFORE_EACH_BAR",
        "DUPLICATE_EACH_BAR",
        "EXACT_OPPORTUNITY_TRACE",
        "EXACT_RIGHT_SET",
        "EXACT_TRANCHE_SET",
        "EXACT_ORDER_INTENT_SET",
        "TRANCHE_CONSERVATION_AFTER_EACH_BAR",
        "GROSS_EQUALS_AUTHORIZATION_PLUS_PLATFORM_RESIDUAL",
        "EXERCISE_PLUS_DEFER_EQUALS_AUTHORIZATION",
        "INTENT_EQUALS_EXERCISE_MINUS_PLATFORM_BLOCK",
        "REMAINING_EQUALS_DEFER_PLUS_PLATFORM_BLOCK",
        "CURRENT_ACTIVE_QUANTITIES_ARE_WHOLE_ALGORITHM_UNITS",
    ];
    for expansion in required_expansions {
        assert!(
            coverage
                .automatic_expansions
                .iter()
                .any(|value| value == expansion),
            "missing automatic case expansion {expansion}"
        );
    }
    for case in &catalog.cases {
        assert!(!case.bars.is_empty(), "{} has no market path", case.id);
        assert!(
            !case.expected_touches.is_empty(),
            "{} has no exact touch set",
            case.id
        );
        assert!(
            !case.expected_rights.is_empty(),
            "{} has no exact right set",
            case.id
        );
        assert!(
            !case.forbidden_events.is_empty(),
            "{} has no forbidden-event assertion",
            case.id
        );
        let mut decision_keys = std::collections::BTreeSet::new();
        for decision in &case.decisions {
            assert!(
                decision.exercise_quantity >= 0 && decision.defer_quantity >= 0,
                "{} has a negative decision quantity",
                case.id
            );
            assert!(
                decision_keys.insert(format!(
                    "{}|{}:{}",
                    decision.at, decision.direction, decision.grid_index
                )),
                "{} has a duplicate decision selector",
                case.id
            );
            assert_eq!(
                decision.exercise_quantity % catalog.standard_quantity,
                0,
                "{} exercise quantity must use whole algorithm units",
                case.id
            );
            assert_eq!(
                decision.defer_quantity % catalog.standard_quantity,
                0,
                "{} defer quantity must use whole algorithm units",
                case.id
            );
        }
        for expected in &case.expected_rights {
            assert_eq!(
                expected.exercise_quantity % catalog.standard_quantity,
                0,
                "{} expected exercise quantity must use whole algorithm units",
                case.id
            );
            assert_eq!(
                expected.defer_quantity % catalog.standard_quantity,
                0,
                "{} expected defer quantity must use whole algorithm units",
                case.id
            );
        }
        for quantity in case.expected_order_quantities.values().flatten() {
            assert_eq!(
                quantity % catalog.standard_quantity,
                0,
                "{} order intent must use whole algorithm units",
                case.id
            );
        }
    }

    let catalog_standard_quantity = catalog.standard_quantity;
    let mut semantic_summaries = std::collections::BTreeMap::new();
    let mut equivalences = Vec::new();
    for case in catalog.cases {
        let temp = TempDir::new().unwrap();
        let service = run_rights_case(&case, &temp, false, false);
        if case.restart_each_bar {
            let recovered_temp = TempDir::new().unwrap();
            let recovered = run_rights_case(&case, &recovered_temp, true, false);
            assert_eq!(
                rights_case_opportunity_trace(&recovered),
                rights_case_opportunity_trace(&service),
                "{} continuous/restart opportunity trace",
                case.id
            );
            assert_eq!(
                rights_case_semantic_summary(&recovered.state),
                rights_case_semantic_summary(&service.state),
                "{} continuous/restart grid semantics",
                case.id
            );
        }
        let duplicate_temp = TempDir::new().unwrap();
        let duplicated = run_rights_case(&case, &duplicate_temp, false, true);
        assert_eq!(
            rights_case_opportunity_trace(&duplicated),
            rights_case_opportunity_trace(&service),
            "{} duplicate-bar opportunity trace",
            case.id
        );
        assert_eq!(
            rights_case_semantic_summary(&duplicated.state),
            rights_case_semantic_summary(&service.state),
            "{} duplicate-bar grid semantics",
            case.id
        );
        assert_eq!(
            service.state.ambiguous_bars, case.expected_ambiguous_bars,
            "{}",
            case.id
        );
        for tranche in service.state.right_tranches.values() {
            tranche
                .validate()
                .unwrap_or_else(|error| panic!("{}: {error:#}", case.id));
        }
        for right in service.state.grid_rights.values() {
            assert_eq!(
                right.decision_contract_version, 3,
                "{} must use the current decision contract",
                case.id
            );
            let unit = catalog_standard_quantity;
            let expected_gross = if right.direction == Direction::Buy {
                right.capacity.available_quantity
            } else {
                right.capacity.eligible_quantity
            };
            assert_eq!(
                right.decision_gross_available_quantity, expected_gross,
                "{} C excludes SELL pre-classified T+1/risk/no-profit shares",
                case.id
            );
            assert_eq!(
                right.decision_platform_residual_quantity,
                expected_gross % unit,
                "{} P=C mod Q",
                case.id
            );
            assert_eq!(
                right.decision_gross_available_quantity,
                right.decision_algorithm_authorized_quantity
                    + right.decision_platform_residual_quantity,
                "{} C=A+P",
                case.id
            );
            assert_eq!(
                right.decision_algorithm_authorized_quantity,
                right.decided_exercise_quantity + right.decided_defer_quantity,
                "{} A=E+D",
                case.id
            );
            assert_eq!(
                right.decision_intent_quantity,
                right.decided_exercise_quantity - right.decision_platform_blocked_quantity,
                "{} I=E-B",
                case.id
            );
            assert_eq!(
                right.decision_remaining_quantity,
                right.decided_defer_quantity + right.decision_platform_blocked_quantity,
                "{} R=D+B",
                case.id
            );
            for quantity in [
                right.decision_algorithm_authorized_quantity,
                right.decided_exercise_quantity,
                right.decided_defer_quantity,
                right.decision_platform_blocked_quantity,
                right.decision_intent_quantity,
                right.decision_remaining_quantity,
            ] {
                assert_eq!(
                    quantity % unit,
                    0,
                    "{} active quantity is fractional",
                    case.id
                );
            }
        }
        for (event_type, count) in &case.expected_events {
            let event_type = EventType::from_str(event_type).unwrap();
            assert_eq!(
                service
                    .store
                    .event_count_by_type(&service.state.run_id, event_type)
                    .unwrap(),
                *count,
                "{} event {}",
                case.id,
                event_type
            );
        }
        for event_type in &case.forbidden_events {
            let event_type = EventType::from_str(event_type).unwrap();
            assert_eq!(
                service
                    .store
                    .event_count_by_type(&service.state.run_id, event_type)
                    .unwrap(),
                0,
                "{} forbids {}",
                case.id,
                event_type
            );
        }
        let case_events = service.store.load_after(&service.state.run_id, 0).unwrap();
        for touch in case_events
            .iter()
            .filter(|event| event.event_type == EventType::GridLevelTouched)
        {
            let index = touch.payload["grid_index"].as_i64().unwrap();
            let grants = case_events
                .iter()
                .filter(|event| {
                    event.event_type == EventType::GridRightGranted
                        && event.event_time == touch.event_time
                        && event.payload["grid_index"].as_i64() == Some(index)
                })
                .count();
            let skips = case_events
                .iter()
                .filter(|event| {
                    event.event_type == EventType::GridLevelSkipped
                        && event.event_time == touch.event_time
                        && event.payload["grid_index"].as_i64() == Some(index)
                })
                .count();
            assert_eq!(
                grants + skips,
                1,
                "{} grid opportunity {}@{} must resolve to exactly one grant or skip",
                case.id,
                index,
                touch.event_time
            );
            if grants == 1 {
                assert_eq!(
                    case_events
                        .iter()
                        .filter(|event| {
                            event.event_type == EventType::GateDecisionMade
                                && event.event_time == touch.event_time
                                && event.payload["request"]["right"]["grid_index"].as_i64()
                                    == Some(index)
                        })
                        .count(),
                    1,
                    "{} granted opportunity {}@{} lacks its exact decision log",
                    case.id,
                    index,
                    touch.event_time
                );
            }
        }
        let order_intents: Vec<Order> = case_events
            .iter()
            .filter(|event| event.event_type == EventType::OrderIntentCreated)
            .map(|event| serde_json::from_value(event.payload.clone()).unwrap())
            .collect();
        assert_eq!(
            order_intents.len(),
            case.expected_orders,
            "{}: {}",
            case.id,
            case.description
        );
        assert_eq!(
            case.expected_touches.values().sum::<usize>(),
            case_events
                .iter()
                .filter(|event| event.event_type == EventType::GridLevelTouched)
                .count(),
            "{} exact touch set",
            case.id
        );
        for (grid_index, expected_count) in &case.expected_touches {
            let actual = case_events
                .iter()
                .filter(|event| {
                    event.event_type == EventType::GridLevelTouched
                        && event.payload["grid_index"].as_i64() == Some(i64::from(*grid_index))
                })
                .count();
            assert_eq!(actual, *expected_count, "{} touch {grid_index}", case.id);
        }
        for expected in &case.expected_rights {
            let direction = match expected.direction.as_str() {
                "BUY" => Direction::Buy,
                "SELL" => Direction::Sell,
                value => panic!("unknown case direction {value}"),
            };
            let matching: Vec<_> = service
                .state
                .grid_rights
                .values()
                .filter(|right| {
                    right.direction == direction && right.grid_index == expected.grid_index
                })
                .collect();
            assert_eq!(matching.len(), expected.count, "{} rights", case.id);
            assert_eq!(
                matching
                    .iter()
                    .map(|right| right.decided_exercise_quantity)
                    .sum::<i64>(),
                expected.exercise_quantity,
                "{} exercised decision quantity",
                case.id
            );
            assert_eq!(
                matching
                    .iter()
                    .map(|right| right.decided_defer_quantity)
                    .sum::<i64>(),
                expected.defer_quantity,
                "{} deferred decision quantity",
                case.id
            );
        }
        assert_eq!(
            case.expected_rights
                .iter()
                .map(|expected| expected.count)
                .sum::<usize>(),
            service.state.grid_rights.len(),
            "{} exact right set",
            case.id
        );
        for (key, expected_quantities) in &case.expected_order_quantities {
            let mut actual: Vec<_> = order_intents
                .iter()
                .filter(|order| {
                    format!("{:?}:{}", order.intent.direction, order.intent.grid_index)
                        .to_uppercase()
                        == *key
                })
                .map(|order| order.intent.quantity)
                .collect();
            actual.sort_unstable();
            let mut expected_quantities = expected_quantities.clone();
            expected_quantities.sort_unstable();
            assert_eq!(actual, expected_quantities, "{} orders {key}", case.id);
        }
        assert_eq!(
            case.expected_order_quantities
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            order_intents.len(),
            "{} exact order set",
            case.id
        );
        let opportunity_balances = opportunity_tranche_balances(&case_events);
        for expected in &case.balances {
            let direction = match expected.direction.as_str() {
                "BUY" => Direction::Buy,
                "SELL" => Direction::Sell,
                value => panic!("unknown case direction {value}"),
            };
            let matching: Vec<_> = opportunity_balances
                .values()
                .filter(|balance| {
                    balance.tranche.direction == direction
                        && balance.tranche.birth_grid_index == expected.birth_grid_index
                })
                .collect();
            assert_eq!(matching.len(), expected.count, "{}", case.id);
            let exercised_quantity: i64 = matching
                .iter()
                .map(|balance| balance.exercised_quantity)
                .sum();
            let revoked_quantity: i64 = matching
                .iter()
                .map(|balance| balance.revoked_quantity)
                .sum();
            let minted_quantity: i64 = matching
                .iter()
                .map(|balance| balance.tranche.minted_quantity)
                .sum();
            let available_quantity = minted_quantity - exercised_quantity - revoked_quantity;
            assert!(
                available_quantity >= 0,
                "{} opportunity tranche overspent its minted quantity",
                case.id
            );
            if let Some(value) = expected.available_quantity {
                assert_eq!(available_quantity, value, "{}", case.id);
            }
            if let Some(value) = expected.exercised_quantity {
                assert_eq!(exercised_quantity, value, "{}", case.id);
            }
            if let Some(value) = expected.revoked_quantity {
                assert_eq!(revoked_quantity, value, "{}", case.id);
            }
            if let Some(value) = expected.minted_quantity {
                assert_eq!(minted_quantity, value, "{}", case.id);
            }
        }
        assert_eq!(
            case.balances
                .iter()
                .map(|expected| expected.count)
                .sum::<usize>(),
            opportunity_balances.len(),
            "{} exact tranche set",
            case.id
        );
        semantic_summaries.insert(
            case.id.clone(),
            rights_case_semantic_summary(&service.state),
        );
        if let Some(reference) = &case.equivalent_to {
            equivalences.push((case.id.clone(), reference.clone()));
        }
    }
    for (case_id, reference_id) in equivalences {
        assert_eq!(
            semantic_summaries.get(&case_id),
            semantic_summaries.get(&reference_id),
            "metamorphic rights equivalence {case_id} -> {reference_id}"
        );
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoLossCaseCatalog {
    case_schema: String,
    cases: Vec<NoLossCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoLossCase {
    id: String,
    description: String,
    original_quantity: i64,
    remaining_quantity: i64,
    allocated_cost: String,
    quantity: i64,
    limit_price: String,
    commission_rate: String,
    minimum_commission: String,
    sell_tax_rate: String,
    slippage_rate: String,
    allowed: bool,
}

fn priced_lot(id: &str, grid_index: i32, quantity: i64, cost: Decimal) -> TradingLot {
    TradingLot {
        lot_id: id.to_owned(),
        source_order_id: format!("source-{id}"),
        grid_index,
        opening_allocation: false,
        opened_on: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        original_quantity: quantity,
        remaining_quantity: quantity,
        open_price: cost / Decimal::from(quantity),
        allocated_cost: cost,
        remaining_cost: cost,
        realized_pnl: Decimal::ZERO,
    }
}

#[test]
fn mark_to_market_and_per_lot_conservative_exit_share_the_no_loss_economics() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let policy = gridedge_t::domain::ProfitGuardPolicy::from(&config);
    let lot = priced_lot("valuation-one", -1, 100, d("1005"));

    let losing = evaluate_sell_slice_with_policy(&policy, &lot, 100, d("10.10")).unwrap();
    assert_eq!(losing.cost_basis, d("1005"));
    assert_eq!(losing.worst_fill_price, d("10.09"));
    assert_eq!(losing.maximum_sell_fees, d("6.01"));
    assert_eq!(losing.worst_case_profit, d("-2.01"));
    assert!(prove_non_loss(&config, &lot, 100, d("10.10")).is_none());

    let profitable = evaluate_sell_slice_with_policy(&policy, &lot, 100, d("10.20")).unwrap();
    let proof = prove_non_loss(&config, &lot, 100, d("10.20")).unwrap();
    assert_eq!(profitable.cost_basis, proof.cost_basis);
    assert_eq!(profitable.worst_fill_price, proof.worst_fill_price);
    assert_eq!(profitable.maximum_sell_fees, proof.maximum_sell_fees);
    assert_eq!(profitable.worst_case_profit, proof.worst_case_profit);
    assert_eq!(profitable.worst_case_profit, d("7.98"));

    let mut state = StrategyState::new(
        "valuation".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("10000"),
        100,
        0,
    );
    state.lots.insert(lot.lot_id.clone(), lot);
    state.last_price = Some(d("10.10"));
    state.audited_profit_guard_policy = Some(policy);
    let valuation = unrealized_grid_valuation(&state);
    assert_eq!(
        UNREALIZED_VALUATION_POLICY_VERSION,
        "LOT_CONSERVATIVE_EXIT_V1"
    );
    assert_eq!(valuation.mark_price, Some(d("10.10")));
    assert_eq!(valuation.mark_to_market_unrealized, Some(d("5")));
    assert_eq!(valuation.conservative_exit_unrealized, Some(d("-2.01")));
    assert_eq!(valuation.conservative_exit_adjustment, Some(d("-7.01")));
    assert_eq!(valuation.unpriced_open_quantity, 0);
}

#[test]
fn conservative_exit_values_each_lot_and_ignores_t1_and_frozen_availability() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let policy = gridedge_t::domain::ProfitGuardPolicy::from(&config);
    let mut state = StrategyState::new(
        "valuation-two".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("10000"),
        200,
        0,
    );
    state
        .lots
        .insert("lot-a".into(), priced_lot("lot-a", -1, 100, d("1005")));
    state
        .lots
        .insert("lot-b".into(), priced_lot("lot-b", -2, 100, d("1005")));
    state.last_price = Some(d("10.10"));
    state.audited_profit_guard_policy = Some(policy);
    state.position.today_bought = 200;
    state.position.sellable = 0;

    let t1_blocked = unrealized_grid_valuation(&state);
    assert_eq!(t1_blocked.mark_to_market_unrealized, Some(d("10")));
    assert_eq!(t1_blocked.conservative_exit_unrealized, Some(d("-4.02")));
    assert_eq!(t1_blocked.conservative_exit_adjustment, Some(d("-14.02")));

    state.position.today_bought = 0;
    state.position.sellable = 200;
    state.position.frozen_sell = 200;
    assert_eq!(unrealized_grid_valuation(&state), t1_blocked);
}

#[test]
fn unrealized_valuation_is_unavailable_without_mark_cost_or_audited_policy() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let policy = gridedge_t::domain::ProfitGuardPolicy::from(&config);
    let lot = priced_lot("known", -1, 100, d("1005"));
    let mut state = StrategyState::new(
        "valuation-unavailable".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("10000"),
        100,
        100,
    );
    state.lots.insert(lot.lot_id.clone(), lot);
    state.audited_profit_guard_policy = Some(policy.clone());

    let no_mark = unrealized_grid_valuation(&state);
    assert_eq!(no_mark.mark_price, None);
    assert_eq!(no_mark.mark_to_market_unrealized, None);
    assert_eq!(no_mark.conservative_exit_unrealized, None);
    assert_eq!(no_mark.conservative_exit_adjustment, None);
    assert_eq!(no_mark.unpriced_open_quantity, 0);

    state.last_price = Some(d("10.10"));
    state.audited_profit_guard_policy = None;
    let no_policy = unrealized_grid_valuation(&state);
    assert_eq!(no_policy.mark_to_market_unrealized, Some(d("5")));
    assert_eq!(no_policy.conservative_exit_unrealized, None);
    assert_eq!(no_policy.conservative_exit_adjustment, None);

    let mut unknown_cost = priced_lot("unknown", -2, 100, Decimal::ZERO);
    unknown_cost.open_price = d("10");
    state.lots.clear();
    state.lots.insert(unknown_cost.lot_id.clone(), unknown_cost);
    state.audited_profit_guard_policy = Some(policy);
    let unknown = unrealized_grid_valuation(&state);
    assert_eq!(unknown.mark_price, Some(d("10.10")));
    assert_eq!(unknown.mark_to_market_unrealized, None);
    assert_eq!(unknown.conservative_exit_unrealized, None);
    assert_eq!(unknown.conservative_exit_adjustment, None);
    assert_eq!(unknown.unpriced_open_quantity, 100);
}

#[test]
fn machine_readable_no_loss_cases_are_the_mechanical_sell_contract() {
    let catalog: NoLossCaseCatalog =
        serde_yaml::from_str(include_str!("fixtures/no_loss_cases.yaml")).unwrap();
    assert_eq!(catalog.case_schema, "gridedge.no-loss/v1");
    let required = ["NL01", "NL02", "NL03", "NL04", "NL05", "NL06", "NL07"];
    for prefix in required {
        assert!(catalog.cases.iter().any(|case| case.id.starts_with(prefix)));
    }
    for case in catalog.cases {
        let temp = TempDir::new().unwrap();
        let mut config = config_in(&temp);
        config.commission_rate = d(&case.commission_rate);
        config.minimum_commission = d(&case.minimum_commission);
        config.sell_tax_rate = d(&case.sell_tax_rate);
        config.slippage_rate = d(&case.slippage_rate);
        config.validate().unwrap();
        let mut lot = priced_lot(
            &case.id,
            -1,
            case.original_quantity,
            d(&case.allocated_cost),
        );
        lot.remaining_quantity = case.remaining_quantity;
        lot.remaining_cost = lot.allocated_cost * Decimal::from(case.remaining_quantity)
            / Decimal::from(case.original_quantity);
        let proof = prove_non_loss(&config, &lot, case.quantity, d(&case.limit_price));
        assert_eq!(
            proof.is_some(),
            case.allowed,
            "{}: {}",
            case.id,
            case.description
        );
        if let Some(proof) = proof {
            assert!(proof.worst_case_profit >= Decimal::ZERO, "{}", case.id);
        }
    }
}

#[test]
fn a_profitable_lot_cannot_subsidize_a_loss_making_lot() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.slippage_rate = Decimal::ZERO;
    let losing = priced_lot("losing", -1, 100, d("1100"));
    let profitable = priced_lot("profitable", -2, 100, d("900"));
    let plan = plan_non_loss_sales(&config, [&losing, &profitable], 200, d("10.20"));
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].lot_id, "profitable");
    assert_eq!(plan[0].quantity, 100);
    assert!(plan[0].worst_case_profit >= Decimal::ZERO);
}

#[test]
fn an_unknown_cost_basis_fails_closed_in_the_no_loss_guard() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let lot = priced_lot("unknown-cost", -1, 100, Decimal::ZERO);
    assert!(prove_non_loss(&config, &lot, 100, d("1000")).is_none());
    assert!(plan_non_loss_sales(&config, [&lot], 100, d("1000")).is_empty());
}

#[test]
fn domain_rejects_a_loss_making_fill_without_mutating_the_lot() {
    let mut state = StrategyState::new(
        "no-loss-domain".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        .unwrap();
    state.apply_fill(&buy_fill("loss-source", 100), -1).unwrap();
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .unwrap();
    let before = serde_json::to_value(&state).unwrap();
    let sell = Fill {
        fill_id: "loss-fill".into(),
        order_id: "loss-order".into(),
        direction: Direction::Sell,
        price: d("10.05"),
        quantity: 100,
        commission: d("5"),
        tax: d("1.01"),
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        target_lot_ids: vec!["lot:loss-source".into()],
        target_lot_allocations: vec![],
    };
    assert!(state.apply_fill(&sell, 1).is_err());
    assert_eq!(serde_json::to_value(state).unwrap(), before);
}

#[test]
fn below_break_even_sell_capacity_is_blocked_before_the_algorithm_can_order() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.minimum_commission = d("500");
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("no-loss-service".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    assert_eq!(
        service
            .state
            .orders
            .values()
            .filter(|order| order.intent.direction == Direction::Sell)
            .count(),
        0
    );
    let sell_right = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell)
        .unwrap();
    assert_eq!(sell_right.status, GridRightStatus::Blocked);
    assert_eq!(sell_right.capacity.eligible_quantity, 0);
    assert!(sell_right.capacity.no_profit_blocked_quantity > 0);
    assert!(service
        .state
        .right_tranches
        .values()
        .filter(|tranche| tranche.direction == Direction::Sell)
        .all(|tranche| tranche.available_quantity == tranche.minted_quantity));
}

#[test]
fn a_below_break_even_sell_right_carries_until_the_lot_becomes_profitable() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.minimum_commission = d("500");
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("no-loss-carry".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
        bar("2026-01-06 09:31:00", "10.15", "10.41", "10.14", "10.35"),
        bar("2026-01-06 09:32:00", "10.35", "10.62", "10.34", "10.58"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let mut sell_rights: Vec<_> = service
        .state
        .grid_rights
        .values()
        .filter(|right| right.direction == Direction::Sell)
        .collect();
    sell_rights.sort_by_key(|right| right.grid_index);
    assert_eq!(
        sell_rights.len(),
        3,
        "{:?}",
        sell_rights
            .iter()
            .map(|right| (right.grid_index, right.status))
            .collect::<Vec<_>>()
    );
    assert_eq!(sell_rights[0].status, GridRightStatus::Transferred);
    assert_eq!(sell_rights[1].status, GridRightStatus::Transferred);
    assert!(matches!(
        sell_rights[2].status,
        GridRightStatus::Exercised | GridRightStatus::PartiallyExercised
    ));
    let sell_order = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap();
    assert!(sell_order
        .intent
        .target_lot_allocations
        .iter()
        .all(|allocation| allocation.worst_case_profit >= Decimal::ZERO));
    for allocation in &sell_order.intent.target_lot_allocations {
        let tranche = &service.state.right_tranches[&allocation.tranche_id];
        assert_eq!(tranche.owner_right_id, sell_rights[2].right_id);
        assert_eq!(tranche.consumed_quantity, allocation.quantity);
    }
}

#[test]
fn a_deeper_sell_excursion_mints_a_source_for_every_unowned_matching_lot() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let mut state = StrategyState::new(
        "late-sell-source".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        20_000,
        20_000,
    );
    state.current_trade_date = Some(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap());
    let lot = priced_lot("deep-buy", -3, 6_100, d("14950"));
    state.lots.insert(lot.lot_id.clone(), lot);
    let right_id = GridRight::id_for(&state.run_id, &state.cycle_id, 4, dt("2026-01-06 10:00:00"));
    let capacity = RightsGateCoordinator::new(&config, &state)
        .capacity(
            4,
            Direction::Sell,
            NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            &right_id,
        )
        .unwrap();
    let expected = RightTranche::id_for(
        &state.cycle_id,
        &right_id,
        Direction::Sell,
        4,
        Some("deep-buy"),
    );
    assert!(capacity.eligible_lot_ids.contains(&"deep-buy".to_owned()));
    assert!(capacity.tranche_ids.contains(&expected));
    let minted = sell_tranches_to_mint(&state, &right_id, 4);
    assert_eq!(minted.len(), 1);
    assert_eq!(minted[0].tranche_id, expected);
    assert_eq!(minted[0].lot_id.as_deref(), Some("deep-buy"));
    assert_eq!(minted[0].available_quantity, 6_100);
}

fn sample_intent() -> OrderIntent {
    OrderIntent {
        intent_id: "intent-1".to_owned(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: String::new(),
        grid_index: -1,
        direction: Direction::Buy,
        limit_price: d("9.80"),
        quantity: 100,
        budget: d("980"),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
        profit_guard_version: String::new(),
        profit_guard_policy: None,
        created_at: dt("2026-01-05 09:31:00"),
    }
}

#[test]
fn order_intent_does_not_change_account_and_transitions_are_checked() {
    let state = StrategyState::new(
        "r".into(),
        "c".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        1000,
        1000,
    );
    let before = state.snapshot().unwrap();
    let intent = sample_intent();
    assert_eq!(state.snapshot().unwrap(), before);
    let mut order = Order {
        order_id: "o".into(),
        intent,
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: Decimal::ZERO,
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    assert!(order.transition(OrderStatus::Filled).is_err());
    order.transition(OrderStatus::Submitted).unwrap();
    order.transition(OrderStatus::Accepted).unwrap();
    order.transition(OrderStatus::PartiallyFilled).unwrap();
    order.transition(OrderStatus::Filled).unwrap();
}

fn buy_fill(id: &str, quantity: i64) -> Fill {
    Fill {
        fill_id: id.into(),
        order_id: "o".into(),
        direction: Direction::Buy,
        price: d("10"),
        quantity,
        commission: d("5"),
        tax: Decimal::ZERO,
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
    }
}

#[test]
fn only_unique_fills_change_cash_position_and_t_plus_one() {
    let mut state = StrategyState::new(
        "r".into(),
        "c".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        1000,
        1000,
    );
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        .unwrap();
    assert!(state.apply_fill(&buy_fill("f1", 100), -1).unwrap());
    assert_eq!(state.cash.available, d("8995"));
    assert_eq!(state.position.total, 1100);
    assert_eq!(state.position.sellable, 1000);
    assert_eq!(state.position.today_bought, 100);
    assert!(!state.apply_fill(&buy_fill("f1", 100), -1).unwrap());
    assert_eq!(state.position.total, 1100);
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .unwrap();
    assert_eq!(state.position.sellable, 1100);
    assert_eq!(state.position.today_bought, 0);
}

fn boundary_buy_intent(config: &Config, intent_id: &str) -> OrderIntent {
    let quantity = config.standard_quantity;
    let limit_price = d("10");
    OrderIntent {
        intent_id: intent_id.to_owned(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: "boundary-buy-right".into(),
        grid_index: -1,
        direction: Direction::Buy,
        limit_price,
        quantity,
        budget: estimated_buy_reservation(config, limit_price, quantity).unwrap(),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
        profit_guard_version: gridedge_t::profit::PROFIT_GUARD_VERSION.into(),
        profit_guard_policy: Some(gridedge_t::domain::ProfitGuardPolicy::from(config)),
        created_at: dt("2026-01-05 09:31:00"),
    }
}

fn boundary_buy_state(
    config: &Config,
    run_id: &str,
    position: i64,
    intent: &OrderIntent,
) -> StrategyState {
    let mut state = StrategyState::new(
        run_id.into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        position,
        position,
    );
    state.current_trade_date = Some(intent.created_at.date());
    state.audited_config = Some(config.clone());
    state.audited_profit_guard_policy = intent.profit_guard_policy.clone();
    state.audited_standard_quantity = config.standard_quantity;
    state.audited_lot_size = config.lot_size;
    state.cash.frozen = intent.budget;
    state.orders.insert(
        "boundary-buy-order".into(),
        Order {
            order_id: "boundary-buy-order".into(),
            intent: intent.clone(),
            status: OrderStatus::Accepted,
            filled_quantity: 0,
            filled_notional: Decimal::ZERO,
            commission_charged: Decimal::ZERO,
            reserved_cash_remaining: intent.budget,
            reserved_quantity_remaining: 0,
            cancel_requested_reason: None,
            cancel_requested_at: None,
        },
    );
    state
}

fn boundary_buy_fill_event(config: &Config, run_id: &str, fill_id: &str) -> EventEnvelope {
    let quantity = config.standard_quantity;
    let price = (d("10") * (Decimal::ONE + config.slippage_rate))
        .round_dp_with_strategy(config.price_scale, RoundingStrategy::ToPositiveInfinity);
    let (commission, tax) = gridedge_t::profit::actual_order_fill_charges(
        config,
        Direction::Buy,
        Decimal::ZERO,
        Decimal::ZERO,
        price,
        quantity,
    );
    EventEnvelope::new(
        EventType::OrderFilled,
        run_id,
        "cycle",
        &config.symbol,
        dt("2026-01-05 09:31:00"),
        "boundary-buy-fill",
        None,
        fill_id.into(),
        json!({
            "grid_index": -1,
            "fill": Fill {
                fill_id: fill_id.into(),
                order_id: "boundary-buy-order".into(),
                direction: Direction::Buy,
                price,
                quantity,
                commission,
                tax,
                trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
                target_lot_ids: vec![],
                target_lot_allocations: vec![],
            }
        }),
        &config.config_version,
    )
}

#[test]
fn ledger_rejects_an_overflowing_buy_fill_atomically_but_accepts_exact_i64_headroom() {
    let quantity = Config::load("configs/default.yaml")
        .unwrap()
        .standard_quantity;
    for (name, position, should_accept) in [
        ("exact", i64::MAX - quantity, true),
        ("overflow", i64::MAX - (quantity - 1), false),
    ] {
        let temp = TempDir::new().unwrap();
        let mut config = config_in(&temp);
        config.initial_cash = d("1000000");
        config.max_position = i64::MAX;
        // Deliberately bypass the public configuration cap: this exercises the
        // ledger's checked-arithmetic boundary independently of config parsing.
        let run_id = format!("ledger-boundary-{name}");
        let intent = boundary_buy_intent(&config, &format!("intent-{name}"));
        let mut state = boundary_buy_state(&config, &run_id, position, &intent);
        let before = serde_json::to_value(&state).unwrap();
        let mut store = SqliteStore::open(&config.database).unwrap();
        store.migrate().unwrap();
        let mut sequence = 0;
        let fill = boundary_buy_fill_event(&config, &run_id, &format!("fill-{name}"));
        let append = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            LedgerWriter::new(&mut store, &mut state, &mut sequence).append(fill)
        }));
        assert!(append.is_ok(), "ledger fill escaped as a panic: {name}");
        if should_accept {
            append.unwrap().unwrap();
            assert_eq!(state.position.total, i64::MAX);
            assert_eq!(sequence, 1);
        } else {
            assert!(append.unwrap().is_err());
            assert_eq!(serde_json::to_value(&state).unwrap(), before);
            assert_eq!(sequence, 0);
            assert!(store.load_after(&run_id, 0).unwrap().is_empty());
        }
    }
}

#[test]
fn paper_rejects_an_overflowing_buy_fill_without_account_or_report_side_effects() {
    let quantity = Config::load("configs/default.yaml")
        .unwrap()
        .standard_quantity;
    for (name, position, should_accept) in [
        ("exact", i64::MAX - quantity, true),
        ("overflow", i64::MAX - (quantity - 1), false),
    ] {
        let temp = TempDir::new().unwrap();
        let mut config = config_in(&temp);
        config.initial_cash = d("1000000");
        config.initial_position = position;
        config.initial_sellable = position;
        config.max_position = i64::MAX;
        config.min_base_position = 0;
        // Deliberately bypass the public configuration cap: execution still
        // must reject an overflowing externally supplied state without panic.
        let run_id = format!("paper-boundary-{name}");
        let intent = boundary_buy_intent(&config, &format!("paper-intent-{name}"));
        let state = StrategyState::new(
            run_id.clone(),
            "cycle".into(),
            config.symbol.clone(),
            config.anchor_price,
            config.initial_cash,
            position,
            position,
        );
        let mut store = SqliteStore::open(&config.database).unwrap();
        store.migrate().unwrap();
        let mut gateway =
            PaperExecutionGateway::new(&config, &run_id, state.snapshot().unwrap()).unwrap();
        gateway.roll_trading_day(intent.created_at.date()).unwrap();
        let connection = rusqlite::Connection::open(&config.database).unwrap();
        let before_account: String = connection
            .query_row(
                "SELECT snapshot_json FROM paper_accounts WHERE run_id=?1",
                [&run_id],
                |row| row.get(0),
            )
            .unwrap();
        let submit =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gateway.submit(&intent)));
        assert!(submit.is_ok(), "Paper fill escaped as a panic: {name}");
        if should_accept {
            let ExecutionReport::Accepted { fills, .. } = submit.unwrap().unwrap() else {
                panic!("exact headroom Paper order was not accepted")
            };
            assert_eq!(
                fills.iter().map(|fill| fill.quantity).sum::<i64>(),
                quantity
            );
            assert_eq!(gateway.account_snapshot().position.total, i64::MAX);
        } else {
            assert!(submit.unwrap().is_err());
            assert_eq!(gateway.account_snapshot().position.total, position);
            let after_account: String = connection
                .query_row(
                    "SELECT snapshot_json FROM paper_accounts WHERE run_id=?1",
                    [&run_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(after_account, before_account);
            let report_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM paper_orders WHERE run_id=?1",
                    [&run_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(report_count, 0);
        }
    }
}

#[test]
fn realized_pnl_includes_allocated_buy_commission_and_sell_fees() {
    let mut state = StrategyState::new(
        "pnl".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        .unwrap();
    state.apply_fill(&buy_fill("buy-pnl", 100), -1).unwrap();
    state.last_price = Some(d("10"));
    assert_eq!(state.unrealized_grid_pnl(), d("-5"));
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .unwrap();
    let sell = Fill {
        fill_id: "sell-pnl".into(),
        order_id: "sell-order".into(),
        direction: Direction::Sell,
        price: d("11"),
        quantity: 100,
        commission: d("5"),
        tax: Decimal::ZERO,
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        target_lot_ids: vec!["lot:buy-pnl".into()],
        target_lot_allocations: vec![],
    };
    state.apply_fill(&sell, 1).unwrap();
    assert_eq!(state.realized_pnl, d("90"));
    assert_eq!(state.lots["lot:buy-pnl"].realized_pnl, d("90"));
    assert_eq!(
        state
            .lots
            .values()
            .map(|lot| lot.realized_pnl)
            .sum::<Decimal>(),
        state.realized_pnl
    );
}

#[test]
fn risk_checks_cash_sellable_base_and_max_position() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let mut state = StrategyState::new(
        "r".into(),
        "c".into(),
        config.symbol.clone(),
        d("10"),
        d("100"),
        config.max_position,
        5000,
    );
    let risk = DefaultRiskChecker { config: &config };
    assert!(risk.check_buy(&state, 100, d("1000")).is_err());
    assert!(risk.check_sell(&state, 6000).is_err());
    state.position.total = config.min_base_position;
    state.position.sellable = config.min_base_position;
    assert!(risk.check_sell(&state, 100).is_err());
}

#[test]
fn buy_headroom_uses_checked_integer_arithmetic_at_the_i64_boundary() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.max_position = i64::MAX;
    // Deliberately bypass the public configuration cap so the low-level risk
    // arithmetic itself is proved safe at i64::MAX.
    let quantity = config.standard_quantity;
    let right = quantity_decision_request(quantity, quantity, config.lot_size).right;

    let make_state = |position| {
        StrategyState::new(
            "max-headroom".into(),
            "cycle".into(),
            config.symbol.clone(),
            config.anchor_price,
            d("1000000"),
            position,
            position,
        )
    };
    let insufficient = make_state(i64::MAX - (quantity - 1));
    let direct_rejection = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        DefaultRiskChecker { config: &config }.check_buy(
            &insufficient,
            quantity,
            estimated_buy_reservation(&config, right.grid_price, quantity).unwrap(),
        )
    }));
    let canonical_rejection = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        canonical_approved_quantity(&config, &insufficient, &right, quantity)
    }));
    assert!(matches!(direct_rejection, Ok(Err(_))));
    assert_eq!(canonical_rejection.unwrap().unwrap(), 0);
    assert_eq!(insufficient.position.total, i64::MAX - (quantity - 1));

    let exact = make_state(i64::MAX - quantity);
    let direct_approval = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        DefaultRiskChecker { config: &config }.check_buy(
            &exact,
            quantity,
            estimated_buy_reservation(&config, right.grid_price, quantity).unwrap(),
        )
    }));
    let canonical_approval = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        canonical_approved_quantity(&config, &exact, &right, quantity)
    }));
    assert!(matches!(direct_approval, Ok(Ok(()))));
    assert_eq!(canonical_approval.unwrap().unwrap(), quantity);
    assert_eq!(exact.position.total.checked_add(quantity), Some(i64::MAX));

    let ordinary = make_state(10_000);
    assert_eq!(
        canonical_approved_quantity(&config, &ordinary, &right, quantity).unwrap(),
        quantity,
        "ordinary whole-Q approval changed while hardening the integer boundary"
    );
}

fn event(run: &str, key: &str, event_type: EventType, payload: serde_json::Value) -> EventEnvelope {
    EventEnvelope::new(
        event_type,
        run,
        "cycle",
        "600000.SH",
        dt("2026-01-05 09:30:00"),
        key,
        None,
        key.to_owned(),
        payload,
        "v1",
    )
}

fn typed_decision_event(
    config: &Config,
    run_id: &str,
    right: &GridRight,
    exercise_quantity: i64,
    correlation: &str,
    key: &str,
) -> EventEnvelope {
    let gross_available_quantity = if right.direction == Direction::Buy {
        right.capacity.available_quantity
    } else {
        right.capacity.eligible_quantity
    };
    let (available_quantity, platform_residual_quantity) =
        whole_unit_partition(gross_available_quantity, config.standard_quantity).unwrap();
    let context = GateContext {
        right_id: right.right_id.clone(),
        symbol: right.symbol.clone(),
        direction: right.direction,
        grid_index: right.grid_index,
        grid_price: right.grid_price,
        current_price: right.grid_price,
        current_time: right.granted_at,
        available_budget: right.capacity.available_budget,
        available_quantity,
        gross_available_quantity,
        platform_residual_quantity,
        algorithm_authorized_quantity: available_quantity,
        lot_size: config.lot_size,
        standard_quantity: config.standard_quantity,
        eligible_lot_ids: right.capacity.eligible_lot_ids.clone(),
        accumulated_grid_indices: right.capacity.accumulated_grid_indices.clone(),
        deployed_budget: right.capacity.deployed_budget,
        current_position: 0,
        sellable_position: 0,
        recent_return: Decimal::ZERO,
        vwap_deviation: Decimal::ZERO,
        volatility: Decimal::ZERO,
        market_regime: "TEST".into(),
        cycle_id: right.cycle_id.clone(),
        funds_inventory: None,
    };
    let request = DecisionRequest::new(right.clone(), context.clone());
    let defer_quantity = available_quantity - exercise_quantity;
    let outcome = if exercise_quantity > 0 {
        RightDecision::Exercise {
            exercise_quantity,
            defer_quantity,
        }
    } else {
        RightDecision::Defer { defer_quantity }
    };
    let decision = GateDecision {
        probability: if exercise_quantity > 0 {
            Decimal::ONE
        } else {
            Decimal::ZERO
        },
        alpha: if exercise_quantity > 0 {
            Decimal::ONE
        } else {
            Decimal::ZERO
        },
        alpha_numerator: None,
        alpha_denominator: None,
        action: if exercise_quantity > 0 {
            "EXECUTE".into()
        } else {
            "SKIP".into()
        },
        reason_codes: vec!["TEST_TYPED_DECISION".into()],
        model_name: "test".into(),
        model_version: "1".into(),
        input_snapshot_hash: context_hash(&context).unwrap(),
        decided_at: right.granted_at,
    };
    let response = DecisionResponse {
        contract_version: request.contract_version,
        request_id: request.request_id.clone(),
        right_id: right.right_id.clone(),
        outcome,
        decision,
    };
    EventEnvelope::new(
        EventType::GateDecisionMade,
        run_id,
        &right.cycle_id,
        &right.symbol,
        right.granted_at,
        correlation,
        None,
        key.into(),
        json!({"request": request, "response": response, "algorithm_succeeded": true}),
        &config.config_version,
    )
}

fn config_snapshot_payload(config: &Config) -> serde_json::Value {
    let mut payload = serde_json::to_value(config).unwrap();
    payload.as_object_mut().unwrap().insert(
        "_content_sha256".into(),
        serde_json::Value::String(config.content_sha256().unwrap()),
    );
    payload
}

#[test]
fn sqlite_journal_is_idempotent_and_rebuilds_from_snapshot_or_full_log() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("events.db")).unwrap();
    store.migrate().unwrap();
    let run = "journal-run";
    let config = Config::load("configs/default.yaml").unwrap();
    let run_payload = json!({"test": true});
    let mut state = StrategyState::new(
        run.into(),
        "cycle".into(),
        config.symbol.clone(),
        d("10"),
        d("1000"),
        0,
        0,
    );
    state.audited_config = Some(config.clone());
    state.audited_standard_quantity = config.standard_quantity;
    state.audited_lot_size = config.lot_size;
    let initial = state.clone();
    let mut sequence = 0;
    let started = event(
        run,
        "run-started-test",
        EventType::RunStarted,
        run_payload.clone(),
    );
    assert_eq!(
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(started)
            .unwrap(),
        AppendOutcome::Appended(1)
    );
    let duplicate = event(run, "run-started-test", EventType::RunStarted, run_payload);
    assert_eq!(
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(duplicate)
            .unwrap(),
        AppendOutcome::Duplicate
    );
    let conflict = event(run, "run-started-test", EventType::RunStarted, json!({}));
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(conflict)
        .is_err());
    let (rebuilt, sequence) = store.rebuild_full(initial.clone()).unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(rebuilt.duplicate_events, 1);
    let mut snapshot_state = rebuilt.clone();
    let mut snapshot_sequence = sequence;
    LedgerWriter::new(&mut store, &mut snapshot_state, &mut snapshot_sequence)
        .save_snapshot()
        .unwrap();
    let (snap, snap_sequence) = store.rebuild(initial).unwrap();
    assert_eq!(snap_sequence, 1);
    assert_eq!(
        serde_json::to_value(snap).unwrap(),
        serde_json::to_value(rebuilt).unwrap()
    );
}

#[test]
fn ledger_writer_rejects_foreign_runs_and_symbols_without_any_mutation() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("scope.db")).unwrap();
    store.migrate().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let mut state = StrategyState::new(
        "scope-run".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("1000"),
        0,
        0,
    );
    let audited_config = Config::load("configs/default.yaml").unwrap();
    state.audited_config = Some(audited_config.clone());
    state.audited_standard_quantity = audited_config.standard_quantity;
    state.audited_lot_size = audited_config.lot_size;
    let before = serde_json::to_value(&state).unwrap();
    let mut sequence = 0;
    let level = serde_json::to_value(
        GridSpec::from(&config)
            .levels()
            .unwrap()
            .remove(&-1)
            .unwrap(),
    )
    .unwrap();

    let foreign_run = event(
        "foreign-run",
        "foreign-run-event",
        EventType::GridLevelArmed,
        level.clone(),
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(foreign_run)
        .is_err());

    let mut foreign_symbol = event(
        "scope-run",
        "foreign-symbol-event",
        EventType::GridLevelArmed,
        level.clone(),
    );
    foreign_symbol.symbol = "000001.SZ".into();
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(foreign_symbol)
        .is_err());

    let mut mixed_runs = vec![
        event(
            "scope-run",
            "batch-local-run",
            EventType::GridLevelArmed,
            level.clone(),
        ),
        event(
            "foreign-run",
            "batch-foreign-run",
            EventType::GridLevelArmed,
            level.clone(),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut mixed_runs)
        .is_err());

    let mut mixed_symbols = vec![
        event(
            "scope-run",
            "batch-local-symbol",
            EventType::GridLevelArmed,
            level.clone(),
        ),
        event(
            "scope-run",
            "batch-foreign-symbol",
            EventType::GridLevelArmed,
            level,
        ),
    ];
    mixed_symbols[1].symbol = "000001.SZ".into();
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut mixed_symbols)
        .is_err());

    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    assert!(store.load_after("scope-run", 0).unwrap().is_empty());
    assert!(store.load_after("foreign-run", 0).unwrap().is_empty());
}

#[test]
fn ledger_rejects_nonstandard_buy_mints_and_unsourced_sell_mints() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("mechanical-unit.db")).unwrap();
    store.migrate().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let run = "mechanical-unit";
    let mut state = StrategyState::new(
        run.into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let mut sequence = 0;
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            run,
            "config",
            EventType::ConfigSnapshotted,
            config_snapshot_payload(&config),
        ))
        .unwrap();
    assert_eq!(state.audited_standard_quantity, config.standard_quantity);

    let unit_boundary_sequence = sequence;
    let unit_boundary_state = serde_json::to_value(&state).unwrap();
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            run,
            "legacy-budget-fact",
            EventType::DeferredBudgetAccrued,
            json!({"available_budget": "999999.99"}),
        ))
        .is_err());
    assert_eq!(sequence, unit_boundary_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), unit_boundary_state);

    let ghost_time = dt("2026-01-05 09:30:00");
    let ghost_right_id = GridRight::id_for(run, "cycle", -2, ghost_time);
    let ghost_tranche = RightTranche::minted_buy_quantity(
        "cycle",
        &ghost_right_id,
        &ghost_right_id,
        -2,
        config.standard_quantity,
    );
    let ghost_right = GridRight::granted(
        run,
        "cycle",
        &config.symbol,
        Direction::Buy,
        -2,
        d("9.60"),
        ghost_time,
        GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: config.standard_quantity * 2,
            deployed_quantity: 0,
            available_quantity: config.standard_quantity * 2,
            eligible_quantity: 0,
            eligible_lot_ids: vec![],
            accumulated_grid_indices: vec![-1, -2],
            t_plus_one_blocked_quantity: 0,
            t_plus_one_blocked_lot_ids: vec![],
            risk_blocked_quantity: 0,
            risk_blocked_lot_ids: vec![],
            no_profit_blocked_quantity: 0,
            no_profit_blocked_lot_ids: vec![],
            source_right_ids: vec![],
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity: config.standard_quantity,
            tranche_ids: vec![ghost_tranche.tranche_id.clone()],
        },
    );
    let mut ghost_carry_batch = vec![
        event(
            run,
            "ghost-carry-grant",
            EventType::GridRightGranted,
            serde_json::to_value(ghost_right).unwrap(),
        ),
        event(
            run,
            "ghost-carry-mint",
            EventType::RightTrancheMinted,
            serde_json::to_value(ghost_tranche).unwrap(),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut ghost_carry_batch)
        .is_err());
    assert_eq!(sequence, unit_boundary_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), unit_boundary_state);

    let buy_time = dt("2026-01-05 09:31:00");
    let buy_right_id = GridRight::id_for(run, "cycle", -1, buy_time);
    let buy_tranche_id = RightTranche::id_for("cycle", &buy_right_id, Direction::Buy, -1, None);
    let buy_right = GridRight::granted(
        run,
        "cycle",
        &config.symbol,
        Direction::Buy,
        -1,
        d("9.80"),
        buy_time,
        GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: config.standard_quantity,
            deployed_quantity: 0,
            available_quantity: config.standard_quantity,
            eligible_quantity: 0,
            eligible_lot_ids: vec![],
            accumulated_grid_indices: vec![-1],
            t_plus_one_blocked_quantity: 0,
            t_plus_one_blocked_lot_ids: vec![],
            risk_blocked_quantity: 0,
            risk_blocked_lot_ids: vec![],
            no_profit_blocked_quantity: 0,
            no_profit_blocked_lot_ids: vec![],
            source_right_ids: vec![],
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity: 0,
            tranche_ids: vec![buy_tranche_id],
        },
    );
    let double_buy = RightTranche::minted_buy_quantity(
        "cycle",
        &buy_right.right_id,
        &buy_right.right_id,
        -1,
        config.standard_quantity * 2,
    );
    let mut invalid_buy_batch = vec![
        event(
            run,
            "buy-grant",
            EventType::GridRightGranted,
            serde_json::to_value(&buy_right).unwrap(),
        ),
        event(
            run,
            "double-buy-mint",
            EventType::RightTrancheMinted,
            serde_json::to_value(double_buy).unwrap(),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut invalid_buy_batch)
        .is_err());
    let before_bad_buy = sequence;
    assert_eq!(sequence, before_bad_buy);

    let bad_time = dt("2026-01-05 09:32:00");
    let bad_right_id = GridRight::id_for(run, "cycle", -2, bad_time);
    let mut bad_capacity = buy_right.capacity.clone();
    bad_capacity.mechanical_quantity_cap = config.standard_quantity * 3;
    bad_capacity.tranche_ids = vec![RightTranche::id_for(
        "cycle",
        &bad_right_id,
        Direction::Buy,
        -2,
        None,
    )];
    let bad_right = GridRight::granted(
        run,
        "cycle",
        &config.symbol,
        Direction::Buy,
        -2,
        d("9.60"),
        bad_time,
        bad_capacity,
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            run,
            "bad-buy-cap",
            EventType::GridRightGranted,
            serde_json::to_value(bad_right).unwrap(),
        ))
        .is_err());
    assert_eq!(sequence, before_bad_buy);

    let lot = priced_lot("real-lot", -1, config.standard_quantity, d("12000"));
    state.lots.insert(lot.lot_id.clone(), lot.clone());
    let sell_time = dt("2026-01-06 09:31:00");
    let sell_right_id = GridRight::id_for(run, "cycle", 1, sell_time);
    let sell_tranche = RightTranche::minted_quantity(
        "cycle",
        &sell_right_id,
        &sell_right_id,
        1,
        &lot.lot_id,
        lot.remaining_quantity,
    );
    let sell_right = GridRight::granted(
        run,
        "cycle",
        &config.symbol,
        Direction::Sell,
        1,
        d("10.20"),
        sell_time,
        GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: 0,
            deployed_quantity: 0,
            available_quantity: 0,
            eligible_quantity: lot.remaining_quantity,
            eligible_lot_ids: vec![lot.lot_id.clone()],
            accumulated_grid_indices: vec![-1],
            t_plus_one_blocked_quantity: 0,
            t_plus_one_blocked_lot_ids: vec![],
            risk_blocked_quantity: 0,
            risk_blocked_lot_ids: vec![],
            no_profit_blocked_quantity: 0,
            no_profit_blocked_lot_ids: vec![],
            source_right_ids: vec![],
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity: 0,
            tranche_ids: vec![sell_tranche.tranche_id.clone()],
        },
    );
    let mut overmint = sell_tranche.clone();
    overmint.minted_quantity *= 2;
    overmint.available_quantity *= 2;
    let mut invalid_sell_batch = vec![
        event(
            run,
            "sell-grant",
            EventType::GridRightGranted,
            serde_json::to_value(&sell_right).unwrap(),
        ),
        event(
            run,
            "sell-overmint",
            EventType::RightTrancheMinted,
            serde_json::to_value(overmint).unwrap(),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut invalid_sell_batch)
        .is_err());
    let mut ghost_mint = sell_tranche;
    ghost_mint.lot_id = Some("missing-lot".into());
    let mut invalid_ghost_batch = vec![
        event(
            run,
            "sell-grant-ghost",
            EventType::GridRightGranted,
            serde_json::to_value(&sell_right).unwrap(),
        ),
        event(
            run,
            "sell-missing-lot",
            EventType::RightTrancheMinted,
            serde_json::to_value(ghost_mint).unwrap(),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut invalid_ghost_batch)
        .is_err());
}

#[test]
fn shallow_buy_reentry_has_no_capacity_after_deeper_whole_units_are_deployed() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let mut state = StrategyState::new(
        "shallow-buy-reentry".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let capacity = |index: i32| GridRightCapacity {
        mechanical_budget_cap: Decimal::ZERO,
        deployed_budget: Decimal::ZERO,
        available_budget: Decimal::ZERO,
        mechanical_quantity_cap: i64::from(index.abs()) * config.standard_quantity,
        deployed_quantity: 0,
        available_quantity: config.standard_quantity,
        eligible_quantity: 0,
        eligible_lot_ids: vec![],
        accumulated_grid_indices: (1..=index.abs()).map(|depth| -depth).collect(),
        t_plus_one_blocked_quantity: 0,
        t_plus_one_blocked_lot_ids: vec![],
        risk_blocked_quantity: 0,
        risk_blocked_lot_ids: vec![],
        no_profit_blocked_quantity: 0,
        no_profit_blocked_lot_ids: vec![],
        source_right_ids: vec![],
        carried_in_budget: Decimal::ZERO,
        carried_in_quantity: 0,
        tranche_ids: vec![],
    };
    for (offset, index) in [-4, -3].into_iter().enumerate() {
        let time = dt(&format!("2026-01-06 09:3{}:00", offset));
        let mut right = GridRight::granted(
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            Direction::Buy,
            index,
            GridSpec::from(&config).price(index).unwrap(),
            time,
            capacity(index),
        );
        right.status = GridRightStatus::Exercised;
        right.exercised_quantity = config.standard_quantity;
        state.grid_rights.insert(right.right_id.clone(), right);
    }
    let mut consumed = RightTranche::minted_buy_quantity(
        &state.cycle_id,
        "consumed-deeper-unit",
        "historical-owner",
        -4,
        config.standard_quantity,
    );
    consumed.available_quantity = 0;
    consumed.consumed_quantity = config.standard_quantity;
    state
        .right_tranches
        .insert(consumed.tranche_id.clone(), consumed);

    let time = dt("2026-01-06 09:40:00");
    let right_id = GridRight::id_for(&state.run_id, &state.cycle_id, -1, time);
    let shallow = RightsGateCoordinator::new(&config, &state)
        .capacity(-1, Direction::Buy, time.date(), &right_id)
        .unwrap();

    assert_eq!(shallow.mechanical_quantity_cap, config.standard_quantity);
    assert_eq!(shallow.deployed_quantity, config.standard_quantity * 2);
    assert_eq!(shallow.available_quantity, 0);
    assert_eq!(shallow.carried_in_quantity, 0);
    assert!(shallow.tranche_ids.is_empty());
}

#[test]
fn service_skips_consumed_shallow_birth_after_deep_whole_quantity_is_deployed_and_rebuilds() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let run_id = "deployed-deep-shallow-reentry";
    let gate = || {
        Box::new(SequenceGate {
            call: AtomicUsize::new(0),
        }) as Box<dyn GatePolicy>
    };
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        gate(),
        Some(run_id.into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-05 09:33:00", "9.62", "9.63", "9.40", "9.43"),
    ] {
        service.on_bar(&market).unwrap();
    }
    assert_eq!(service.state.orders.len(), 1);
    let deep_order = service.state.orders.values().next().unwrap();
    assert_eq!(deep_order.intent.grid_index, -3);
    assert_eq!(deep_order.intent.quantity, 3 * config.standard_quantity);
    assert_eq!(deep_order.status, OrderStatus::Filled);
    service.state.validate_invariants().unwrap();

    for market in [
        bar("2026-01-05 09:34:00", "9.43", "9.63", "9.42", "9.62"),
        bar("2026-01-05 09:35:00", "9.62", "9.83", "9.61", "9.82"),
        bar("2026-01-05 09:36:00", "9.82", "10.01", "9.81", "10"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let rights_before_reentry = service.state.grid_rights.len();
    let orders_before_reentry = service.state.orders.len();
    let reentry = bar("2026-01-05 09:37:00", "10", "10.01", "9.79", "9.82");
    service.on_bar(&reentry).unwrap();

    assert_eq!(service.state.grid_rights.len(), rights_before_reentry);
    assert_eq!(service.state.orders.len(), orders_before_reentry);
    service.state.validate_invariants().unwrap();
    let hot_events = service.store.load_after(run_id, 0).unwrap();
    let reentry_events = hot_events
        .iter()
        .filter(|event| event.event_time == reentry.timestamp)
        .collect::<Vec<_>>();
    assert!(reentry_events.iter().any(|event| {
        event.event_type == EventType::GridLevelTouched && event.payload["grid_index"] == -1
    }));
    assert!(reentry_events.iter().any(|event| {
        event.event_type == EventType::GridLevelSkipped
            && event.payload["grid_index"] == -1
            && event.payload["reason"] == "RIGHT_ALREADY_EXERCISED_AT_LEVEL"
    }));
    assert!(!reentry_events.iter().any(|event| {
        matches!(
            event.event_type,
            EventType::GridRightGranted | EventType::OrderIntentCreated
        )
    }));

    let hot_rights = serde_json::to_value(&service.state.grid_rights).unwrap();
    let hot_orders = serde_json::to_value(&service.state.orders).unwrap();
    let hot_lots = serde_json::to_value(&service.state.lots).unwrap();
    let hot_cash = service.state.cash.clone();
    let hot_position = service.state.position.clone();
    drop(service);
    let recovered = GridAutomationService::recover(
        config,
        SqliteStore::open(temp.path().join("test.db")).unwrap(),
        gate(),
        run_id.into(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&recovered.state.grid_rights).unwrap(),
        hot_rights
    );
    assert_eq!(
        serde_json::to_value(&recovered.state.orders).unwrap(),
        hot_orders
    );
    assert_eq!(
        serde_json::to_value(&recovered.state.lots).unwrap(),
        hot_lots
    );
    assert_eq!(recovered.state.cash, hot_cash);
    assert_eq!(recovered.state.position, hot_position);
    recovered.state.validate_invariants().unwrap();
    assert_eq!(
        recovered
            .store
            .load_after(run_id, 0)
            .unwrap()
            .iter()
            .filter(|event| {
                event.event_type == EventType::GridLevelSkipped
                    && event.payload["grid_index"] == -1
                    && event.payload["reason"] == "RIGHT_ALREADY_EXERCISED_AT_LEVEL"
            })
            .count(),
        1
    );
}

#[test]
fn consumed_birth_tranche_blocks_same_depth_reentry_after_deeper_execution_and_rebuilds() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let run_id = "consumed-birth-depth-reentry";
    let gate = || {
        Box::new(SequenceGate {
            call: AtomicUsize::new(0),
        }) as Box<dyn GatePolicy>
    };
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        gate(),
        Some(run_id.into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-05 09:33:00", "9.62", "9.63", "9.40", "9.43"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let birth_right = service
        .state
        .grid_rights
        .values()
        .find(|right| right.grid_index == -2)
        .unwrap();
    assert_eq!(birth_right.status, GridRightStatus::Transferred);
    assert_eq!(birth_right.exercised_quantity, 0);
    let birth_tranche = service
        .state
        .right_tranches
        .values()
        .find(|tranche| tranche.birth_grid_index == -2)
        .unwrap();
    assert_ne!(birth_tranche.owner_right_id, birth_right.right_id);
    assert_eq!(birth_tranche.available_quantity, 0);
    assert_eq!(birth_tranche.reserved_quantity, 0);
    assert_eq!(birth_tranche.consumed_quantity, config.standard_quantity);

    service
        .on_bar(&bar("2026-01-05 09:34:00", "9.43", "9.63", "9.42", "9.62"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:35:00", "9.62", "9.83", "9.61", "9.82"))
        .unwrap();
    let rights_before_reentry = service.state.grid_rights.len();
    let tranches_before_reentry = service.state.right_tranches.len();
    let orders_before_reentry = service.state.orders.len();
    let reentry = bar("2026-01-05 09:36:00", "9.82", "9.83", "9.58", "9.59");
    service.on_bar(&reentry).unwrap();

    assert_eq!(service.state.grid_rights.len(), rights_before_reentry);
    assert_eq!(service.state.right_tranches.len(), tranches_before_reentry);
    assert_eq!(service.state.orders.len(), orders_before_reentry);
    service.state.validate_invariants().unwrap();
    let reentry_events = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_time == reentry.timestamp)
        .collect::<Vec<_>>();
    assert!(
        reentry_events.iter().any(|event| {
            event.event_type == EventType::GridLevelTouched && event.payload["grid_index"] == -2
        }),
        "re-entry events: {:?}",
        reentry_events
            .iter()
            .map(|event| (&event.event_type, &event.payload))
            .collect::<Vec<_>>()
    );
    assert!(reentry_events.iter().any(|event| {
        event.event_type == EventType::GridLevelSkipped
            && event.payload["grid_index"] == -2
            && event.payload["reason"] == "RIGHT_ALREADY_EXERCISED_AT_LEVEL"
    }));
    assert!(!reentry_events.iter().any(|event| {
        matches!(
            event.event_type,
            EventType::GridRightGranted
                | EventType::RightTrancheMinted
                | EventType::OrderIntentCreated
        )
    }));

    let hot_rights = serde_json::to_value(&service.state.grid_rights).unwrap();
    let hot_tranches = serde_json::to_value(&service.state.right_tranches).unwrap();
    let hot_orders = serde_json::to_value(&service.state.orders).unwrap();
    let hot_lots = serde_json::to_value(&service.state.lots).unwrap();
    let hot_cash = service.state.cash.clone();
    let hot_position = service.state.position.clone();
    drop(service);
    let recovered = GridAutomationService::recover(
        config,
        SqliteStore::open(temp.path().join("test.db")).unwrap(),
        gate(),
        run_id.into(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&recovered.state.grid_rights).unwrap(),
        hot_rights
    );
    assert_eq!(
        serde_json::to_value(&recovered.state.right_tranches).unwrap(),
        hot_tranches
    );
    assert_eq!(
        serde_json::to_value(&recovered.state.orders).unwrap(),
        hot_orders
    );
    assert_eq!(
        serde_json::to_value(&recovered.state.lots).unwrap(),
        hot_lots
    );
    assert_eq!(recovered.state.cash, hot_cash);
    assert_eq!(recovered.state.position, hot_position);
    recovered.state.validate_invariants().unwrap();
    assert_eq!(
        recovered
            .store
            .load_after(run_id, 0)
            .unwrap()
            .iter()
            .filter(|event| {
                event.event_type == EventType::GridLevelSkipped
                    && event.payload["grid_index"] == -2
                    && event.payload["reason"] == "RIGHT_ALREADY_EXERCISED_AT_LEVEL"
            })
            .count(),
        1
    );
}

#[test]
fn balance_transfer_cannot_move_back_to_a_shallower_owner() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("back-transfer.db")).unwrap();
    store.migrate().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let mut state = StrategyState::new(
        "back-transfer".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    state.audited_standard_quantity = config.standard_quantity;
    state.audited_lot_size = config.lot_size;
    let source_time = dt("2026-01-06 09:31:00");
    let target_time = dt("2026-01-06 09:32:00");
    let source_id = GridRight::id_for("back-transfer", "cycle", 3, source_time);
    let target_id = GridRight::id_for("back-transfer", "cycle", 2, target_time);
    let tranche = RightTranche::minted_quantity(
        "cycle",
        "birth",
        &source_id,
        1,
        "lot",
        config.standard_quantity,
    );
    let sell_capacity = |tranche_ids: Vec<String>| GridRightCapacity {
        mechanical_budget_cap: Decimal::ZERO,
        deployed_budget: Decimal::ZERO,
        available_budget: Decimal::ZERO,
        mechanical_quantity_cap: 0,
        deployed_quantity: 0,
        available_quantity: 0,
        eligible_quantity: config.standard_quantity,
        eligible_lot_ids: vec!["lot".into()],
        accumulated_grid_indices: vec![-1],
        t_plus_one_blocked_quantity: 0,
        t_plus_one_blocked_lot_ids: vec![],
        risk_blocked_quantity: 0,
        risk_blocked_lot_ids: vec![],
        no_profit_blocked_quantity: 0,
        no_profit_blocked_lot_ids: vec![],
        source_right_ids: vec![],
        carried_in_budget: Decimal::ZERO,
        carried_in_quantity: 0,
        tranche_ids,
    };
    let source = GridRight::granted(
        "back-transfer",
        "cycle",
        &config.symbol,
        Direction::Sell,
        3,
        d("10.60"),
        source_time,
        sell_capacity(vec![tranche.tranche_id.clone()]),
    );
    let target = GridRight::granted(
        "back-transfer",
        "cycle",
        &config.symbol,
        Direction::Sell,
        2,
        d("10.40"),
        target_time,
        sell_capacity(vec![tranche.tranche_id.clone()]),
    );
    state.grid_rights.insert(source_id.clone(), source);
    state.grid_rights.insert(target_id.clone(), target);
    state
        .right_tranches
        .insert(tranche.tranche_id.clone(), tranche.clone());
    let mut sequence = 0;
    let transfer_payload = json!({
        "source_right_id": source_id,
        "target_right_id": target_id,
        "tranche_ids": [tranche.tranche_id]
    });
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            "back-transfer",
            "shallower-transfer",
            EventType::RightBalanceTransferred,
            transfer_payload.clone(),
        ))
        .is_err());
    assert_eq!(sequence, 0);

    // Historical schema-1 transfer facts remain replayable; the sole writer
    // rejects new schema downgrades, while rebuild may project the old fact.
    let mut historical = event(
        "back-transfer",
        "historical-shallower-transfer",
        EventType::RightBalanceTransferred,
        transfer_payload,
    );
    historical.schema_version = 1;
    state.apply_event(&historical).unwrap();
    assert_eq!(
        state.right_tranches[&tranche.tranche_id].owner_right_id,
        target_id
    );
}

#[test]
fn granted_right_batch_must_transfer_every_carried_tranche() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(FixedGate {
            probability: Decimal::ZERO,
            alpha: Decimal::ZERO,
        }),
        Some("missing-carry-transfer".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    let time = dt("2026-01-05 09:32:00");
    let right_id = GridRight::id_for(&service.state.run_id, &service.state.cycle_id, -2, time);
    let capacity = RightsGateCoordinator::new(&config, &service.state)
        .capacity(-2, Direction::Buy, time.date(), &right_id)
        .unwrap();
    let right = GridRight::granted(
        &service.state.run_id,
        &service.state.cycle_id,
        &service.state.symbol,
        Direction::Buy,
        -2,
        GridSpec::from(&config).price(-2).unwrap(),
        time,
        capacity,
    );
    let marginal = RightTranche::minted_buy_quantity(
        &service.state.cycle_id,
        &right_id,
        &right_id,
        -2,
        config.standard_quantity,
    );
    let make_event = |event_type, key: &str, payload| {
        EventEnvelope::new(
            event_type,
            &service.state.run_id,
            &service.state.cycle_id,
            &service.state.symbol,
            time,
            "missing-transfer",
            None,
            key.into(),
            payload,
            &config.config_version,
        )
    };
    let mut incomplete = vec![
        make_event(
            EventType::GridRightGranted,
            "missing-transfer-grant",
            serde_json::to_value(right).unwrap(),
        ),
        make_event(
            EventType::RightTrancheMinted,
            "missing-transfer-mint",
            serde_json::to_value(marginal).unwrap(),
        ),
    ];
    let mut sequence = service
        .store
        .load_after(&service.state.run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let before_sequence = sequence;
    let before = serde_json::to_value(&service.state).unwrap();
    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence)
            .append_batch(&mut incomplete)
            .is_err()
    );
    assert_eq!(sequence, before_sequence);
    assert_eq!(serde_json::to_value(&service.state).unwrap(), before);
}

#[test]
fn ledger_writer_enforces_grid_right_transition_and_quantity_conservation() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("rights.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "right-sm".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    let mut audited_config = Config::load("configs/default.yaml").unwrap();
    audited_config.standard_quantity = 1_000;
    state.levels = GridSpec::from(&audited_config).levels().unwrap();
    state.audited_standard_quantity = 1_000;
    state.audited_lot_size = 100;
    state.audited_config = Some(audited_config.clone());
    let grant_time = dt("2026-01-05 09:30:00");
    let right_id = GridRight::id_for("right-sm", "cycle", -1, grant_time);
    let tranche = RightTranche::minted_buy_quantity("cycle", &right_id, &right_id, -1, 1_000);
    let right = GridRight::granted(
        "right-sm",
        "cycle",
        "600000.SH",
        Direction::Buy,
        -1,
        d("9.80"),
        grant_time,
        GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: 1_000,
            deployed_quantity: 0,
            available_quantity: 1_000,
            eligible_quantity: 0,
            eligible_lot_ids: vec![],
            accumulated_grid_indices: vec![-1],
            t_plus_one_blocked_quantity: 0,
            t_plus_one_blocked_lot_ids: vec![],
            risk_blocked_quantity: 0,
            risk_blocked_lot_ids: vec![],
            no_profit_blocked_quantity: 0,
            no_profit_blocked_lot_ids: vec![],
            source_right_ids: vec![],
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity: 0,
            tranche_ids: vec![tranche.tranche_id.clone()],
        },
    );
    let right_id = right.right_id.clone();
    let decision_id = format!("decision:{right_id}");
    let decision_event = typed_decision_event(
        &audited_config,
        "right-sm",
        &right,
        1_000,
        "grant",
        "decision",
    );
    let right_payload = serde_json::to_value(&right).unwrap();
    let mut sequence = 0;
    for (key, market) in [
        (
            "market-before",
            bar("2026-01-02 15:00:00", "10", "10.01", "9.99", "10"),
        ),
        (
            "market-current",
            bar("2026-01-05 09:30:00", "10", "10.01", "9.79", "9.80"),
        ),
    ] {
        let market_event = EventEnvelope::new(
            EventType::MarketDataReceived,
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            market.timestamp,
            key,
            None,
            key.into(),
            serde_json::to_value(market).unwrap(),
            "v1",
        );
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(market_event)
            .unwrap();
    }
    let mut touch = event(
        "right-sm",
        "touch",
        EventType::GridLevelTouched,
        json!({"grid_index": -1, "price": "9.80"}),
    );
    touch.correlation_id = "grant".into();
    let mut grant_batch = vec![
        touch,
        event(
            "right-sm",
            "grant",
            EventType::GridRightGranted,
            right_payload.clone(),
        ),
        event(
            "right-sm",
            "mint",
            EventType::RightTrancheMinted,
            serde_json::to_value(&tranche).unwrap(),
        ),
        decision_event,
        event(
            "right-sm",
            "reserve-balance",
            EventType::RightBalanceReserved,
            json!({
                "right_id": right_id,
                "allocation_policy": "DEEPEST_FIRST_V1",
                "allocations": [{"tranche_id": tranche.tranche_id.clone(), "budget": "0", "quantity": 1000}]
            }),
        ),
        event(
            "right-sm",
            "reserve",
            EventType::GridRightReserved,
            json!({
                "right_id": right_id,
                "decision_id": decision_id,
                "reserved_quantity": 1000,
                "gross_available_quantity": 1000,
                "platform_residual_quantity": 0,
                "algorithm_authorized_quantity": 1000,
                "platform_blocked_quantity": 0,
                "order_intent_quantity": 1000,
                "remaining_decision_quantity": 0
            }),
        ),
    ];
    for event in &mut grant_batch {
        event.correlation_id = "grant".into();
    }
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut grant_batch)
        .unwrap();
    assert_eq!(sequence, 8);

    let illegal_direct = event(
        "right-sm",
        "direct-exercise",
        EventType::GridRightExercised,
        json!({"right_id": right_id, "exercised_quantity_delta": 100}),
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(illegal_direct)
        .is_err());
    assert_eq!(sequence, 8);
    assert_eq!(
        state.grid_rights[&right_id].status,
        GridRightStatus::Reserved
    );

    let excessive_reserve = event(
        "right-sm",
        "excessive-reserve",
        EventType::GridRightReserved,
        json!({
            "right_id": right_id,
            "decision_id": decision_id,
            "reserved_quantity": 1100,
            "gross_available_quantity": 1000,
            "platform_residual_quantity": 0,
            "algorithm_authorized_quantity": 1000,
            "platform_blocked_quantity": 0,
            "order_intent_quantity": 1000,
            "remaining_decision_quantity": 0
        }),
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(excessive_reserve)
        .is_err());
    assert_eq!(sequence, 8);
    let mut partial_fill = vec![
        event(
            "right-sm",
            "consume-balance",
            EventType::RightBalanceConsumed,
            json!({
                "right_id": right_id,
                "allocations": [{"tranche_id": tranche.tranche_id.clone(), "budget": "0", "quantity": 200}]
            }),
        ),
        event(
            "right-sm",
            "partial",
            EventType::GridRightPartiallyExercised,
            json!({"right_id": right_id, "exercised_quantity_delta": 200}),
        ),
    ];
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut partial_fill)
        .unwrap();
    assert_eq!(
        state.grid_rights[&right_id].status,
        GridRightStatus::PartiallyExercised
    );
    let before = sequence;
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            "right-sm",
            "over-exercise",
            EventType::GridRightPartiallyExercised,
            json!({"right_id": right_id, "exercised_quantity_delta": 801}),
        ))
        .is_err());
    assert_eq!(sequence, before);

    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            "right-sm",
            "expire",
            EventType::GridRightExpired,
            json!({
                "right_id": right_id,
                "reason": "GRID_CYCLE_COMPLETED",
                "remaining_budget": "0",
                "remaining_quantity": 0
            }),
        ))
        .unwrap();
    let before_terminal_attack = sequence;
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            "right-sm",
            "terminal-partial-block",
            EventType::GridRightBlocked,
            json!({
                "right_id": right_id,
                "decision_id": decision_id,
                "partial": true,
                "t_plus_one_blocked_quantity": 1,
                "risk_blocked_quantity": 0
            }),
        ))
        .is_err());
    assert_eq!(sequence, before_terminal_attack);
    let mut invalid_batch = vec![
        event(
            "right-sm",
            "duplicate-grant",
            EventType::GridRightGranted,
            right_payload,
        ),
        event(
            "right-sm",
            "would-stop",
            EventType::ServiceStopped,
            json!({}),
        ),
    ];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut invalid_batch)
        .is_err());
    assert_eq!(sequence, before_terminal_attack);
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(event(
            "right-sm",
            "revive",
            EventType::GridRightReserved,
            json!({"right_id": right_id, "decision_id": decision_id, "reserved_quantity": 100}),
        ))
        .is_err());
    assert_eq!(
        store.load_after("right-sm", 0).unwrap().len() as i64,
        sequence
    );
}

struct SequenceGate {
    call: AtomicUsize,
}

struct MixedDayGate;

impl GatePolicy for MixedDayGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        let alpha = if context.direction == Direction::Sell && context.grid_index == 1 {
            Decimal::ZERO
        } else {
            Decimal::ONE
        };
        Ok(GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if alpha.is_zero() { "SKIP" } else { "EXECUTE" }.into(),
            reason_codes: vec!["MIXED_DAY_TEST".into()],
            model_name: "mixed-day-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

struct DirectionalGate;

impl GatePolicy for DirectionalGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        let alpha = if context.direction == Direction::Buy {
            d("0.5")
        } else {
            Decimal::ONE
        };
        Ok(GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["DIRECTIONAL_TEST".into()],
            model_name: "directional-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}
impl GatePolicy for SequenceGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        let call = self.call.fetch_add(1, Ordering::SeqCst);
        let alpha = if call < 2 {
            Decimal::ZERO
        } else {
            Decimal::ONE
        };
        Ok(GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if alpha.is_zero() {
                "SKIP".into()
            } else {
                "EXECUTE".into()
            },
            reason_codes: vec![format!("STEP_{call}")],
            model_name: "sequence-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

#[test]
fn service_defers_two_levels_then_uses_third_level_full_deferred_capacity() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let gate = Box::new(SequenceGate {
        call: AtomicUsize::new(0),
    });
    let mut service =
        GridAutomationService::start_new(config, store, gate, Some("deferred-run".into())).unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:33:00", "9.62", "9.63", "9.40", "9.43"))
        .unwrap();
    assert_eq!(service.state.orders.len(), 1);
    let order = service.state.orders.values().next().unwrap();
    assert_eq!(order.intent.quantity, 4_500);
    assert_eq!(
        order.intent.budget,
        order.intent.limit_price * Decimal::from(order.intent.quantity)
    );
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(
        service.state.levels[&-1].status,
        gridedge_t::domain::GridLevelStatus::Touched
    );
    assert_eq!(
        service.state.levels[&-2].status,
        gridedge_t::domain::GridLevelStatus::Touched
    );
    assert_eq!(
        service
            .state
            .grid_rights
            .values()
            .filter(|right| right.status == GridRightStatus::Transferred)
            .count(),
        2
    );
}

#[test]
fn reversal_pops_only_the_marginal_tranche_and_new_excursion_uses_a_new_token() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FixedGate {
            probability: Decimal::ZERO,
            alpha: Decimal::ZERO,
        }),
        Some("same-level-carry".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-05 09:33:00", "9.62", "9.83", "9.61", "9.82"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let shallow = service
        .state
        .right_tranches
        .values()
        .find(|tranche| tranche.birth_grid_index == -1)
        .unwrap();
    let deepest = service
        .state
        .right_tranches
        .values()
        .find(|tranche| tranche.birth_grid_index == -2)
        .unwrap();
    assert_eq!(shallow.available_quantity, 1_500);
    assert_eq!(shallow.revoked_quantity, 0);
    assert_eq!(deepest.available_quantity, 0);
    assert_eq!(deepest.revoked_quantity, 1_500);
    assert!(service
        .store
        .load_after("same-level-carry", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::RightBalanceRevoked
            && event.payload["reason"] == "PRICE_REVERSED_ONE_GRID"));

    service
        .on_bar(&bar("2026-01-05 09:34:00", "9.82", "9.83", "9.59", "9.62"))
        .unwrap();
    let depth_two: Vec<_> = service
        .state
        .right_tranches
        .values()
        .filter(|tranche| tranche.birth_grid_index == -2)
        .collect();
    assert_eq!(depth_two.len(), 2);
    assert_ne!(depth_two[0].tranche_id, depth_two[1].tranche_id);
    assert_eq!(
        depth_two
            .iter()
            .filter(|tranche| tranche.available_quantity > 0)
            .count(),
        1
    );
}

#[test]
fn same_bar_deeper_touch_and_reversal_is_ambiguous_and_does_not_change_tranches() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FixedGate {
            probability: Decimal::ZERO,
            alpha: Decimal::ZERO,
        }),
        Some("same-side-ambiguous".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let before = service.state.right_tranches.clone();
    service
        .on_bar(&bar("2026-01-05 09:33:00", "9.62", "9.83", "9.39", "9.60"))
        .unwrap();
    assert_eq!(service.state.right_tranches, before);
    assert_eq!(service.state.ambiguous_bars, 1);
    assert!(service
        .store
        .load_after("same-side-ambiguous", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::AmbiguousBarDetected
            && event.payload["reason"] == "SAME_SIDE_REVERSAL_ORDER_UNKNOWN"));
}

#[test]
fn mixed_old_and_same_day_lots_emit_partial_t1_block_and_partial_exercise() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(MixedDayGate),
        Some("mixed-t1".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "9.83", "9.81", "9.82"),
        bar("2026-01-06 09:31:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-06 09:32:00", "9.62", "10.41", "9.61", "10.35"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let sell_right = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell && right.grid_index == 2)
        .unwrap();
    assert!(sell_right.capacity.t_plus_one_blocked_quantity > 0);
    assert!(sell_right.exercised_quantity > 0);
    assert_eq!(sell_right.status, GridRightStatus::PartiallyExercised);
    assert_eq!(
        sell_right.decision_gross_available_quantity, sell_right.capacity.eligible_quantity,
        "T+1 shares are audited separately and must not enter C"
    );
    assert_eq!(sell_right.decision_platform_residual_quantity, 0);
    assert_eq!(
        sell_right.decision_algorithm_authorized_quantity,
        config.standard_quantity
    );
    assert_eq!(
        sell_right.decided_exercise_quantity,
        config.standard_quantity
    );
    assert_eq!(sell_right.decided_defer_quantity, 0);
    assert_eq!(sell_right.decision_platform_blocked_quantity, 0);
    assert_eq!(
        sell_right.decision_intent_quantity,
        config.standard_quantity
    );
    assert_eq!(sell_right.decision_remaining_quantity, 0);
    assert!(service
        .store
        .load_after("mixed-t1", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::GridRightBlocked
            && event.payload["partial"] == true
            && event.payload["t_plus_one_blocked_quantity"]
                .as_i64()
                .is_some_and(|quantity| quantity > 0)));
}

#[test]
fn zero_capacity_sell_touch_does_not_consume_a_future_sell_right() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("zero-sell-capacity".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.21", "9.99", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    assert!(!service
        .state
        .grid_rights
        .values()
        .any(|right| right.direction == Direction::Sell));
    service
        .on_bar(&bar(
            "2026-01-05 09:32:00",
            "10.15",
            "10.16",
            "9.79",
            "9.82",
        ))
        .unwrap();
    service
        .on_bar(&bar(
            "2026-01-06 09:30:00",
            "9.82",
            "10.21",
            "9.81",
            "10.15",
        ))
        .unwrap();
    assert!(service
        .state
        .grid_rights
        .values()
        .any(|right| right.direction == Direction::Sell && right.grid_index == 1));
}

#[test]
fn partial_right_reversal_revokes_only_the_unexercised_remainder() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(DirectionalGate),
        Some("partial-expiry".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let buy = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Buy)
        .unwrap();
    assert_eq!(buy.status, GridRightStatus::Revoked);
    let expected = buy.capacity.available_quantity - buy.exercised_quantity;
    assert!(expected > 0);
    let tranche = service
        .state
        .right_tranches
        .values()
        .find(|tranche| tranche.birth_grid_index == -1)
        .unwrap();
    assert_eq!(tranche.consumed_quantity, buy.exercised_quantity);
    assert_eq!(tranche.revoked_quantity, expected);
    assert_eq!(
        tranche.minted_quantity,
        tranche.consumed_quantity + tranche.revoked_quantity
    );
    let revocation = service
        .store
        .load_after("partial-expiry", 0)
        .unwrap()
        .into_iter()
        .find(|event| {
            event.event_type == EventType::RightBalanceRevoked
                && event.payload["right_id"] == buy.right_id
        })
        .unwrap();
    assert_eq!(revocation.payload["reversal_from_grid"], -1);
}

#[test]
fn partial_fill_and_recovery_are_deterministic_and_duplicate_market_is_harmless() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.partial_fill_bps = 10_000;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("recover-run".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let first = bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82");
    service.on_bar(&first).unwrap();
    assert_eq!(service.state.processed_fills.len(), 2);
    let position = service.state.position.total;
    let deployed = service.state.deployed_grid_budget;
    let orders = service.state.orders.len();
    service.save_snapshot().unwrap();
    drop(service);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut recovered = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "recover-run".into(),
    )
    .unwrap();
    assert_eq!(recovered.state.position.total, position);
    assert_eq!(recovered.state.deployed_grid_budget, deployed);
    let before = serde_json::to_value(&recovered.state).unwrap();
    recovered.on_bar(&first).unwrap();
    assert_eq!(recovered.state.orders.len(), orders);
    assert_eq!(serde_json::to_value(&recovered.state).unwrap(), before);
    let conflicting = bar("2026-01-05 09:31:00", "10", "10.01", "9.70", "9.71");
    let sequence_before_conflict = recovered.store.load_after("recover-run", 0).unwrap().len();
    assert!(recovered.on_bar(&conflicting).is_err());
    assert_eq!(
        recovered.store.load_after("recover-run", 0).unwrap().len(),
        sequence_before_conflict
    );
    assert_eq!(recovered.state.orders.len(), orders);
    assert_eq!(serde_json::to_value(&recovered.state).unwrap(), before);
}

#[test]
fn ambiguous_bar_is_counted_and_conservatively_skipped() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("ambiguous-run".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.21", "9.79", "10"))
        .unwrap();
    assert_eq!(service.state.ambiguous_bars, 1);
    assert!(service.state.orders.is_empty());
}

#[test]
fn reconciliation_difference_enters_safe_mode() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("reconcile-run".into()),
    )
    .unwrap();
    let mut broker = service.state.snapshot().unwrap();
    broker.cash.available += Decimal::ONE;
    drop(service);
    write_paper_snapshot(&config, "reconcile-run", &broker);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "reconcile-run".into(),
    )
    .unwrap();
    let result = service.reconcile().unwrap();
    assert!(!result.matched);
    assert_eq!(service.state.mode, ServiceMode::Safe);
}

#[test]
fn safe_mode_requires_clean_reconciliation_and_audited_operator_resume() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("operator-resume".into()),
    )
    .unwrap();
    let clean = service.state.snapshot().unwrap();
    let mut mismatch = clean.clone();
    mismatch.cash.available += Decimal::ONE;
    drop(service);
    write_paper_snapshot(&config, "operator-resume", &mismatch);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "operator-resume".into(),
    )
    .unwrap();
    service.reconcile().unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.resume_after_reconciliation("premature").is_err());
    service.stop().unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    drop(service);
    write_paper_snapshot(&config, "operator-resume", &clean);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "operator-resume".into(),
    )
    .unwrap();
    service.reconcile().unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    drop(service);

    write_paper_snapshot(&config, "operator-resume", &mismatch);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "operator-resume".into(),
    )
    .unwrap();
    assert!(service
        .resume_after_reconciliation("broker changed after clean check")
        .is_err());
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service
        .store
        .load_after("operator-resume", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::ErrorRecorded
            && event.payload["reason_code"] == "BROKER_CHANGED_AFTER_CLEAN_RECONCILIATION"));
    drop(service);

    write_paper_snapshot(&config, "operator-resume", &clean);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "operator-resume".into(),
    )
    .unwrap();
    service.reconcile().unwrap();
    drop(service);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "operator-resume".into(),
    )
    .unwrap();
    service
        .resume_after_reconciliation("independent broker discrepancy investigated")
        .unwrap();
    assert_eq!(service.state.mode, ServiceMode::Running);
    assert!(service
        .store
        .load_after("operator-resume", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::ServiceModeChanged
            && event.payload["reason"] == "independent broker discrepancy investigated"));
}

#[test]
fn sample_csv_is_valid_and_contains_ambiguous_demo_bar() {
    let config = Config::load("configs/default.yaml").unwrap();
    let feed = CsvReplayFeed::load("tests/fixtures/sample.csv", &config.symbol).unwrap();
    assert_eq!(feed.bars().len(), 21);
    let last = feed.bars().last().unwrap();
    assert!(last.low < d("9.80") && last.high > d("10.20"));
}

fn gate_context() -> GateContext {
    GateContext {
        right_id: "right-1".into(),
        symbol: "600000.SH".into(),
        direction: Direction::Buy,
        grid_index: -3,
        grid_price: d("9.42"),
        current_price: d("9.40"),
        current_time: dt("2026-01-05 09:32:00"),
        available_budget: d("45000"),
        available_quantity: 4_500,
        gross_available_quantity: 4_500,
        platform_residual_quantity: 0,
        algorithm_authorized_quantity: 4_500,
        lot_size: 100,
        standard_quantity: 1_500,
        eligible_lot_ids: vec![],
        accumulated_grid_indices: vec![-1, -2, -3],
        deployed_budget: Decimal::ZERO,
        current_position: 10000,
        sellable_position: 10000,
        recent_return: d("-0.02"),
        vwap_deviation: d("-0.01"),
        volatility: d("0.03"),
        market_regime: "DOWN".into(),
        cycle_id: "cycle".into(),
        funds_inventory: None,
    }
}

fn quantity_decision_request(
    gross_available_quantity: i64,
    standard_quantity: i64,
    lot_size: i64,
) -> DecisionRequest {
    let capacity = GridRightCapacity {
        mechanical_budget_cap: Decimal::ZERO,
        deployed_budget: Decimal::ZERO,
        available_budget: Decimal::ZERO,
        mechanical_quantity_cap: gross_available_quantity,
        deployed_quantity: 0,
        available_quantity: gross_available_quantity,
        eligible_quantity: 0,
        eligible_lot_ids: Vec::new(),
        accumulated_grid_indices: vec![-1],
        t_plus_one_blocked_quantity: 0,
        t_plus_one_blocked_lot_ids: Vec::new(),
        risk_blocked_quantity: 0,
        risk_blocked_lot_ids: Vec::new(),
        no_profit_blocked_quantity: 0,
        no_profit_blocked_lot_ids: Vec::new(),
        source_right_ids: Vec::new(),
        carried_in_budget: Decimal::ZERO,
        carried_in_quantity: 0,
        tranche_ids: vec!["quantity-contract-tranche".into()],
    };
    let right = GridRight::granted(
        "quantity-contract",
        "cycle",
        "600000.SH",
        Direction::Buy,
        -1,
        d("9.80"),
        dt("2026-01-05 09:31:00"),
        capacity,
    );
    let (authorized, residual) =
        whole_unit_partition(gross_available_quantity, standard_quantity).unwrap();
    DecisionRequest::new_with_contract(
        right.clone(),
        GateContext {
            right_id: right.right_id,
            symbol: right.symbol,
            direction: right.direction,
            grid_index: right.grid_index,
            grid_price: right.grid_price,
            current_price: right.grid_price,
            current_time: right.granted_at,
            available_budget: Decimal::ZERO,
            available_quantity: authorized,
            gross_available_quantity,
            platform_residual_quantity: residual,
            algorithm_authorized_quantity: authorized,
            lot_size,
            standard_quantity,
            eligible_lot_ids: Vec::new(),
            accumulated_grid_indices: vec![-1],
            deployed_budget: Decimal::ZERO,
            current_position: 0,
            sellable_position: 0,
            recent_return: Decimal::ZERO,
            vwap_deviation: Decimal::ZERO,
            volatility: Decimal::ZERO,
            market_regime: "TEST".into(),
            cycle_id: "cycle".into(),
            funds_inventory: None,
        },
        WHOLE_UNIT_DECISION_CONTRACT_VERSION,
    )
}

fn quantity_decision_response(
    request: &DecisionRequest,
    exercise_quantity: i64,
    defer_quantity: i64,
) -> DecisionResponse {
    let context = &request.context;
    let alpha = if context.available_quantity > 0 {
        Decimal::from(exercise_quantity) / Decimal::from(context.available_quantity)
    } else {
        Decimal::ZERO
    };
    DecisionResponse {
        contract_version: request.contract_version,
        request_id: request.request_id.clone(),
        right_id: request.right.right_id.clone(),
        outcome: if exercise_quantity > 0 {
            RightDecision::Exercise {
                exercise_quantity,
                defer_quantity,
            }
        } else {
            RightDecision::Defer { defer_quantity }
        },
        decision: GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if exercise_quantity > 0 {
                "EXECUTE"
            } else {
                "SKIP"
            }
            .into(),
            reason_codes: vec!["QUANTITY_CONTRACT_TEST".into()],
            model_name: "quantity-contract-test".into(),
            model_version: "3".into(),
            input_snapshot_hash: context_hash(context).unwrap(),
            decided_at: context.current_time,
        },
    }
}

#[test]
fn platform_approval_is_recomputed_in_complete_standard_quantity_units() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.standard_quantity = 1_500;
    config.lot_size = 100;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.validate().unwrap();

    let buy_right = quantity_decision_request(3_000, 1_500, 100).right;
    let rich = StrategyState::new(
        "approval-rich-buy".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("200000"),
        0,
        0,
    );
    assert_eq!(
        canonical_approved_quantity(&config, &rich, &buy_right, 1_500).unwrap(),
        1_500
    );
    assert_eq!(
        canonical_approved_quantity(&config, &rich, &buy_right, 3_000).unwrap(),
        3_000
    );

    let one_unit_cash = estimated_buy_reservation(&config, buy_right.grid_price, 1_500).unwrap();
    let cash_limited = StrategyState::new(
        "approval-one-buy".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        one_unit_cash,
        0,
        0,
    );
    assert_eq!(
        canonical_approved_quantity(&config, &cash_limited, &buy_right, 3_000).unwrap(),
        1_500
    );

    let mut sell_right = quantity_decision_request(1_500, 1_500, 100).right;
    sell_right.direction = Direction::Sell;
    sell_right.grid_price = d("20");
    sell_right.capacity.available_quantity = 0;
    sell_right.capacity.eligible_quantity = 1_500;
    sell_right.capacity.eligible_lot_ids = vec!["sell-700".into(), "sell-800".into()];
    let mut jointly_supported = StrategyState::new(
        "approval-joined-sell".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        Decimal::ZERO,
        1_500,
        1_500,
    );
    jointly_supported.lots.insert(
        "sell-700".into(),
        priced_lot("sell-700", -2, 700, d("7000")),
    );
    jointly_supported.lots.insert(
        "sell-800".into(),
        priced_lot("sell-800", -1, 800, d("8000")),
    );
    assert_eq!(
        canonical_approved_quantity(&config, &jointly_supported, &sell_right, 1_500).unwrap(),
        1_500
    );
    let mut cash_rich_sell = jointly_supported.clone();
    cash_rich_sell.cash.available = d("1000000");
    assert_eq!(
        canonical_approved_quantity(&config, &cash_rich_sell, &sell_right, 1_500).unwrap(),
        canonical_approved_quantity(&config, &jointly_supported, &sell_right, 1_500).unwrap(),
        "SELL approval depends on eligible no-loss inventory, never cash"
    );
    let joined_approval =
        canonical_approval(&config, &jointly_supported, &sell_right, 1_500).unwrap();
    assert_eq!(joined_approval.quantity, 1_500);
    assert_eq!(
        joined_approval
            .sell_allocations
            .iter()
            .map(|allocation| allocation.quantity)
            .sum::<i64>(),
        1_500
    );

    sell_right.capacity.eligible_lot_ids = vec!["sell-1200".into()];
    let mut only_partial_support = StrategyState::new(
        "approval-partial-sell".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        Decimal::ZERO,
        1_500,
        1_500,
    );
    only_partial_support.lots.insert(
        "sell-1200".into(),
        priced_lot("sell-1200", -1, 1_200, d("12000")),
    );
    assert_eq!(
        canonical_approved_quantity(&config, &only_partial_support, &sell_right, 1_500).unwrap(),
        0
    );

    config.commission_rate = Decimal::ZERO;
    config.minimum_commission = d("5");
    config.sell_tax_rate = Decimal::ZERO;
    config.slippage_rate = Decimal::ZERO;
    let mut non_closed_right = sell_right;
    non_closed_right.capacity.eligible_quantity = 3_200;
    non_closed_right.capacity.eligible_lot_ids = vec!["fee-a".into(), "fee-b".into()];
    non_closed_right.grid_price = d("10");
    let mut non_closed_state = StrategyState::new(
        "approval-fee-discontinuity".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        Decimal::ZERO,
        3_200,
        3_200,
    );
    non_closed_state.lots.insert(
        "fee-a".into(),
        priced_lot("fee-a", -2, 1_600, d("15994.88")),
    );
    non_closed_state.lots.insert(
        "fee-b".into(),
        priced_lot("fee-b", -1, 1_600, d("15994.88")),
    );
    let large_plan = plan_non_loss_sales(&config, non_closed_state.lots.values(), 3_000, d("10"));
    assert_eq!(
        large_plan
            .iter()
            .map(|allocation| allocation.quantity)
            .sum::<i64>(),
        1_600
    );
    assert!(
        plan_non_loss_sales(&config, non_closed_state.lots.values(), 1_500, d("10")).is_empty()
    );
    let approval =
        canonical_approval(&config, &non_closed_state, &non_closed_right, 3_000).unwrap();
    assert_eq!(approval.quantity, 0);
    assert!(approval.sell_allocations.is_empty());
}

#[test]
fn ongoing_buy_units_use_all_free_cash_while_the_opening_floor_remains_a_report() {
    let config = Config::load("configs/ths_002256_sim.yaml").unwrap();
    let one_unit = estimated_buy_reservation(&config, d("3.36"), config.standard_quantity).unwrap();
    assert_eq!(one_unit, d("18491.05"));
    assert_eq!(
        affordable_buy_units(&config, d("3.36"), one_unit - d("0.01"), 10).unwrap(),
        0
    );
    assert_eq!(
        affordable_buy_units(&config, d("3.36"), one_unit, 10).unwrap(),
        1
    );

    let opening_quantity = 5 * config.standard_quantity;
    let opening_reservation =
        estimated_buy_reservation(&config, d("3.51"), opening_quantity).unwrap();
    let post_opening_cash = config.initial_cash - opening_reservation;
    assert_eq!(post_opening_cash, d("103418.53"));
    assert_eq!(
        affordable_buy_units(&config, d("3.36"), post_opening_cash, 10).unwrap(),
        5
    );
    assert!(
        config
            .gate
            .capital_inventory
            .as_ref()
            .unwrap()
            .minimum_free_cash
            > post_opening_cash
                - estimated_buy_reservation(&config, d("3.36"), 5 * config.standard_quantity)
                    .unwrap(),
        "the configured opening floor remains reportable but does not reduce ongoing cash"
    );
}

fn minimum_commission_buy_config(temp: &TempDir, initial_cash: Decimal) -> Config {
    let mut config = config_in(temp);
    config.anchor_price = d("10.00");
    config.standard_quantity = 1_500;
    config.lot_size = 100;
    config.initial_cash = initial_cash;
    config.initial_position = 0;
    config.initial_sellable = 0;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.commission_rate = d("0.0003");
    config.minimum_commission = d("5");
    config.sell_tax_rate = d("0.001");
    config.slippage_rate = d("0.0001");
    config.validate().unwrap();
    config
}

#[test]
fn buy_approval_reserves_one_cumulative_minimum_commission_at_the_cash_boundary() {
    let temp = TempDir::new().unwrap();
    let config = minimum_commission_buy_config(&temp, d("14720.00"));
    let right = quantity_decision_request(1_500, 1_500, 100).right;
    assert_eq!(right.grid_price, d("9.80"));
    assert_eq!(
        (right.grid_price * (Decimal::ONE + config.slippage_rate))
            .round_dp_with_strategy(2, RoundingStrategy::ToPositiveInfinity),
        d("9.81")
    );
    assert_eq!(
        estimated_buy_reservation(&config, right.grid_price, 1_500).unwrap(),
        d("14720.00"),
        "one BUY order reserves its cumulative minimum commission once"
    );
    assert_eq!(
        estimated_buy_reservation(&config, d("5004.50"), 1).unwrap(),
        d("5010.01"),
        "adverse price rounding must reserve the fractional-cent boundary upward"
    );
    assert_eq!(
        estimated_buy_reservation(&config, right.grid_price, 3_000).unwrap(),
        d("29438.83"),
        "two units share one cumulative percentage commission"
    );

    for (cash, expected_intent, expected_blocked) in
        [(d("14719.99"), 0, 1_500), (d("14720.00"), 1_500, 0)]
    {
        let state = StrategyState::new(
            format!("buy-cash-{cash}"),
            "cycle".into(),
            config.symbol.clone(),
            config.anchor_price,
            cash,
            0,
            0,
        );
        let approved = canonical_approved_quantity(&config, &state, &right, 1_500).unwrap();
        assert_eq!(approved, expected_intent, "cash boundary {cash}");
        assert_eq!(1_500 - approved, expected_blocked, "cash boundary {cash}");
    }

    let two_unit_right = quantity_decision_request(3_000, 1_500, 100).right;
    for (cash, expected_intent) in [(d("14720.00"), 1_500), (d("29438.83"), 3_000)] {
        let state = StrategyState::new(
            format!("buy-multi-unit-{cash}"),
            "cycle".into(),
            config.symbol.clone(),
            config.anchor_price,
            cash,
            0,
            0,
        );
        assert_eq!(
            canonical_approved_quantity(&config, &state, &two_unit_right, 3_000).unwrap(),
            expected_intent,
            "whole-unit fallback at cash {cash}"
        );
    }
}

#[test]
fn buy_cash_boundary_drives_exact_b_i_and_partial_fills_rebuild_without_double_fee() {
    fn execute(temp: &TempDir, cash: Decimal, partial: bool) -> (Config, StrategyState) {
        let mut config = minimum_commission_buy_config(temp, cash);
        config.paper.partial_fill_bps = if partial { 10_000 } else { 0 };
        let run_id = if partial {
            "buy-minimum-partial"
        } else {
            "buy-minimum-blocked"
        };
        let mut service = GridAutomationService::start_new(
            config.clone(),
            SqliteStore::open(&config.database).unwrap(),
            Box::new(gridedge_t::gate::AlwaysExecuteGate),
            Some(run_id.into()),
        )
        .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
            .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
            .unwrap();
        service.save_snapshot().unwrap();
        (config, service.state)
    }

    let blocked_temp = TempDir::new().unwrap();
    let (_, blocked) = execute(&blocked_temp, d("14719.99"), false);
    let blocked_right = blocked
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Buy && right.grid_index == -1)
        .unwrap();
    assert_eq!(blocked_right.decided_exercise_quantity, 1_500);
    assert_eq!(blocked_right.decision_intent_quantity, 0);
    assert_eq!(blocked_right.decision_platform_blocked_quantity, 1_500);
    assert_eq!(blocked_right.decision_remaining_quantity, 1_500);
    assert_eq!(blocked_right.status, GridRightStatus::Blocked);
    assert!(blocked.orders.is_empty());
    assert_eq!(blocked.cash.available, d("14719.99"));
    assert_eq!(blocked.cash.frozen, Decimal::ZERO);

    let exact_temp = TempDir::new().unwrap();
    let (config, exact) = execute(&exact_temp, d("14720.00"), true);
    let exact_right = exact
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Buy && right.grid_index == -1)
        .unwrap();
    assert_eq!(exact_right.decided_exercise_quantity, 1_500);
    assert_eq!(exact_right.decision_intent_quantity, 1_500);
    assert_eq!(exact_right.decision_platform_blocked_quantity, 0);
    assert_eq!(exact_right.decision_remaining_quantity, 0);
    let order = exact.orders.values().next().unwrap();
    assert_eq!(order.intent.quantity, 1_500);
    assert_eq!(order.filled_quantity, 1_500);
    assert_eq!(order.filled_notional, d("14715"));
    assert_eq!(order.commission_charged, d("5"));
    assert_eq!(order.reserved_cash_remaining, Decimal::ZERO);
    assert_eq!(exact.cash.available, Decimal::ZERO);
    assert_eq!(exact.cash.frozen, Decimal::ZERO);
    assert_eq!(exact.cash.total_fees, d("5"));

    let store = SqliteStore::open(&config.database).unwrap();
    let events = store.load_after("buy-minimum-partial", 0).unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == EventType::GridRightReserved && event.schema_version == 4
    }));
    assert!(events.iter().any(|event| {
        event.event_type == EventType::OrderIntentCreated && event.schema_version == 7
    }));
    let fills = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::OrderPartiallyFilled | EventType::OrderFilled
            )
        })
        .map(|event| serde_json::from_value::<Fill>(event.payload["fill"].clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        fills.iter().map(|fill| fill.quantity).collect::<Vec<_>>(),
        vec![700, 800]
    );
    assert_eq!(
        fills.iter().map(|fill| fill.commission).collect::<Vec<_>>(),
        vec![d("5"), Decimal::ZERO]
    );
    assert_eq!(
        fills.iter().map(|fill| fill.commission).sum::<Decimal>(),
        d("5")
    );

    let initial = StrategyState::new(
        "buy-minimum-partial".into(),
        String::new(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let (hot, hot_sequence) = store.rebuild(initial.clone()).unwrap();
    let (cold, cold_sequence) = store.rebuild_full(initial).unwrap();
    assert_eq!(hot_sequence, cold_sequence);
    assert_eq!(
        serde_json::to_value(&hot).unwrap(),
        serde_json::to_value(&cold).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&hot).unwrap(),
        serde_json::to_value(&exact).unwrap()
    );
}

#[test]
fn ledger_rejects_joint_buy_reservation_and_intent_forgery_below_the_cash_boundary() {
    let source_temp = TempDir::new().unwrap();
    let config = minimum_commission_buy_config(&source_temp, d("20000"));
    let run_id = "buy-minimum-ledger-source";
    let mut service = GridAutomationService::start_new(
        config,
        SqliteStore::open(source_temp.path().join("test.db")).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some(run_id.into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let pre_crossing = service.state.clone();
    let before_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let exact_temp = TempDir::new().unwrap();
    let exact_database = exact_temp.path().join("exact.db");
    let forged_temp = TempDir::new().unwrap();
    let forged_database = forged_temp.path().join("forged.db");
    copy_sqlite_database(
        std::path::Path::new(&service.state.audited_config.as_ref().unwrap().database),
        &exact_database,
    );
    copy_sqlite_database(
        std::path::Path::new(&service.state.audited_config.as_ref().unwrap().database),
        &forged_database,
    );
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .is_err());
    let mut expected = service.store.load_after(run_id, before_sequence).unwrap();
    let order_event = expected
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    let mut order: Order = serde_json::from_value(order_event.payload.clone()).unwrap();
    order.reserved_cash_remaining = d("14720.00");
    order_event.payload = serde_json::to_value(order).unwrap();

    let mut exact_state = pre_crossing.clone();
    exact_state.cash.available = d("14720.00");
    assert_canonical_batch_is_accepted_from(&exact_state, &expected, &exact_database);

    let mut insufficient_state = pre_crossing;
    insufficient_state.cash.available = d("14719.99");
    assert_forged_batch_is_atomically_rejected_from(
        &insufficient_state,
        expected,
        &forged_database,
    );
}

#[test]
fn buy_minimum_commission_replays_complete_legacy_and_current_prefixes_by_schema() {
    let source_temp = TempDir::new().unwrap();
    let config = minimum_commission_buy_config(&source_temp, d("40000"));
    let run_id = "buy-minimum-schema-prefix";
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some(run_id.into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let pre_crossing = service.state.clone();
    let before_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .is_err());
    let current_prefix = service.store.load_after(run_id, before_sequence).unwrap();
    assert!(current_prefix.iter().any(|event| {
        event.event_type == EventType::GridRightReserved && event.schema_version == 4
    }));
    assert!(current_prefix.iter().any(|event| {
        event.event_type == EventType::OrderIntentCreated && event.schema_version == 7
    }));

    let mut schema6_replay_prefix = current_prefix.clone();
    schema6_replay_prefix
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap()
        .schema_version = 6;
    let mut schema6_replay = pre_crossing.clone();
    for event in &schema6_replay_prefix {
        schema6_replay.apply_event(event).unwrap();
    }
    schema6_replay.validate_invariants().unwrap();
    assert_eq!(schema6_replay.orders.len(), 1);
    assert!(schema6_replay
        .orders
        .values()
        .all(|order| order.intent.origin.is_grid_right()));

    let mut legacy_blocked_prefix = current_prefix.clone();
    legacy_blocked_prefix.retain(|event| {
        !matches!(
            event.event_type,
            EventType::RightBalanceReserved | EventType::OrderIntentCreated
        )
    });
    let blocked = legacy_blocked_prefix
        .iter_mut()
        .find(|event| event.event_type == EventType::GridRightReserved)
        .unwrap();
    blocked.event_type = EventType::GridRightBlocked;
    blocked.schema_version = 3;
    blocked.payload["reason"] = json!("BUY_RISK_REJECTED:legacy-two-minimum-reservation");
    blocked.payload["platform_blocked_quantity"] = json!(1_500);
    blocked.payload["order_intent_quantity"] = json!(0);
    blocked.payload["remaining_decision_quantity"] = json!(1_500);
    blocked
        .payload
        .as_object_mut()
        .unwrap()
        .remove("reserved_quantity");
    let mut legacy_blocked = pre_crossing.clone();
    legacy_blocked.cash.available = d("14720.00");
    for event in &legacy_blocked_prefix {
        legacy_blocked.apply_event(event).unwrap();
    }
    legacy_blocked.validate_invariants().unwrap();
    let blocked_right = legacy_blocked
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Buy && right.grid_index == -1)
        .unwrap();
    assert_eq!(blocked_right.status, GridRightStatus::Blocked);
    assert_eq!(blocked_right.decision_platform_blocked_quantity, 1_500);
    assert_eq!(blocked_right.decision_intent_quantity, 0);
    assert!(legacy_blocked.orders.is_empty());
    assert_eq!(legacy_blocked.cash.available, d("14720.00"));

    let mut legacy_reserved_prefix = current_prefix;
    let disposition = legacy_reserved_prefix
        .iter_mut()
        .find(|event| event.event_type == EventType::GridRightReserved)
        .unwrap();
    disposition.schema_version = 3;
    let intent = legacy_reserved_prefix
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    intent.schema_version = 5;
    intent.payload["reserved_cash_remaining"] = json!("14725.00");
    let mut legacy_reserved = pre_crossing.clone();
    legacy_reserved.cash.available = d("29440.00");
    for event in &legacy_reserved_prefix {
        legacy_reserved.apply_event(event).unwrap();
    }
    legacy_reserved.validate_invariants().unwrap();
    let reserved_right = legacy_reserved
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Buy && right.grid_index == -1)
        .unwrap();
    assert_eq!(reserved_right.status, GridRightStatus::Reserved);
    assert_eq!(reserved_right.decision_intent_quantity, 1_500);
    let order = legacy_reserved.orders.values().next().unwrap();
    assert_eq!(order.reserved_cash_remaining, d("14725.00"));
    assert_eq!(legacy_reserved.cash.available, d("29440.00"));
    assert_eq!(legacy_reserved.cash.frozen, Decimal::ZERO);

    let disposition_index = legacy_blocked_prefix
        .iter()
        .position(|event| event.event_type == EventType::GridRightBlocked)
        .unwrap();
    let mut before_legacy_disposition = pre_crossing.clone();
    before_legacy_disposition.cash.available = d("14720.00");
    for event in &legacy_blocked_prefix[..disposition_index] {
        before_legacy_disposition.apply_event(event).unwrap();
    }
    let before = serde_json::to_value(&before_legacy_disposition).unwrap();
    let rejection_temp = TempDir::new().unwrap();
    let mut rejection_store =
        SqliteStore::open(rejection_temp.path().join("no-new-legacy.db")).unwrap();
    rejection_store.migrate().unwrap();
    let mut sequence = 0;
    assert!(LedgerWriter::new(
        &mut rejection_store,
        &mut before_legacy_disposition,
        &mut sequence,
    )
    .append(legacy_blocked_prefix[disposition_index].clone())
    .is_err());
    assert_eq!(sequence, 0);
    assert_eq!(
        serde_json::to_value(before_legacy_disposition).unwrap(),
        before
    );
}

#[test]
fn sell_approval_replans_the_final_whole_unit_and_completes_the_bar_when_it_is_unsafe() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.anchor_price = d("9.80");
    config.standard_quantity = 1_500;
    config.lot_size = 100;
    config.initial_position = 3_200;
    config.initial_sellable = 3_200;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.commission_rate = Decimal::ZERO;
    config.minimum_commission = d("5");
    config.sell_tax_rate = Decimal::ZERO;
    config.slippage_rate = Decimal::ZERO;
    config.validate().unwrap();

    let run_id = "sell-final-whole-unit-replan";
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some(run_id.into()),
    )
    .unwrap();
    for lot_id in ["fee-a", "fee-b"] {
        service
            .state
            .lots
            .insert(lot_id.into(), priced_lot(lot_id, -1, 1_600, d("15994.88")));
    }
    service.state.validate_invariants().unwrap();
    let prefix_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let pre_market = service.state.clone();
    let opening = bar("2026-01-06 09:30:00", "9.80", "9.81", "9.79", "9.80");
    let crossing = bar("2026-01-06 09:31:00", "10", "10.01", "9.99", "10");

    service.on_bar(&opening).unwrap();
    service.on_bar(&crossing).unwrap();

    let right = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell && right.grid_index == 1)
        .unwrap();
    assert_eq!(right.capacity.eligible_quantity, 3_200);
    assert_eq!(right.decision_gross_available_quantity, 3_200);
    assert_eq!(right.decision_platform_residual_quantity, 200);
    assert_eq!(right.decision_algorithm_authorized_quantity, 3_000);
    assert_eq!(right.decided_exercise_quantity, 3_000);
    assert_eq!(right.decided_defer_quantity, 0);
    assert_eq!(right.decision_intent_quantity, 0);
    assert_eq!(right.decision_platform_blocked_quantity, 3_000);
    assert_eq!(right.decision_remaining_quantity, 3_000);
    assert_eq!(right.status, GridRightStatus::Blocked);
    assert!(service.state.orders.is_empty());

    let events = service.store.load_after(run_id, prefix_sequence).unwrap();
    for event_type in [
        EventType::MarketDataReceived,
        EventType::MarketBarDecisionsCommitted,
        EventType::MarketBarProcessed,
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.event_time == crossing.timestamp && event.event_type == event_type
                })
                .count(),
            1,
            "unsafe whole-unit SELL must still finish {event_type}"
        );
    }
    assert!(!events
        .iter()
        .any(|event| event.event_type == EventType::OrderIntentCreated));

    let mut recovered = pre_market;
    for event in &events {
        recovered.apply_event(event).unwrap();
    }
    assert_eq!(
        serde_json::to_value(&recovered).unwrap(),
        serde_json::to_value(&service.state).unwrap(),
        "recovery from the pre-bar projection must reproduce the blocked disposition"
    );
}

#[test]
fn sell_approval_can_combine_board_lot_slices_into_one_algorithm_unit() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.anchor_price = d("9.80");
    config.standard_quantity = 1_500;
    config.lot_size = 100;
    config.initial_position = 1_500;
    config.initial_sellable = 1_500;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.commission_rate = Decimal::ZERO;
    config.minimum_commission = d("5");
    config.sell_tax_rate = Decimal::ZERO;
    config.slippage_rate = Decimal::ZERO;
    config.validate().unwrap();

    let run_id = "sell-combined-whole-unit";
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some(run_id.into()),
    )
    .unwrap();
    service.state.lots.insert(
        "sell-700".into(),
        priced_lot("sell-700", -1, 700, d("6990")),
    );
    service.state.lots.insert(
        "sell-800".into(),
        priced_lot("sell-800", -1, 800, d("7990")),
    );
    service.state.validate_invariants().unwrap();

    service
        .on_bar(&bar("2026-01-06 09:30:00", "9.80", "9.81", "9.79", "9.80"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-06 09:31:00", "10", "10.01", "9.99", "10"))
        .unwrap();

    let right = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell && right.grid_index == 1)
        .unwrap();
    assert_eq!(right.decision_gross_available_quantity, 1_500);
    assert_eq!(right.decision_platform_residual_quantity, 0);
    assert_eq!(right.decided_exercise_quantity, 1_500);
    assert_eq!(right.decision_platform_blocked_quantity, 0);
    assert_eq!(right.decision_intent_quantity, 1_500);
    let order = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap();
    let mut quantities = order
        .intent
        .target_lot_allocations
        .iter()
        .map(|allocation| allocation.quantity)
        .collect::<Vec<_>>();
    quantities.sort_unstable();
    assert_eq!(quantities, vec![700, 800]);
    assert_eq!(order.intent.quantity, 1_500);
}

struct DeepSellHalfGate;

impl GatePolicy for DeepSellHalfGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        let alpha = if context.direction == Direction::Sell && context.grid_index == 2 {
            d("0.5")
        } else {
            Decimal::ZERO
        };
        Ok(GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if alpha.is_zero() { "SKIP" } else { "EXECUTE" }.into(),
            reason_codes: vec!["DEEP_SELL_HALF_TEST".into()],
            model_name: "deep-sell-half-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

fn copy_sqlite_database(source: &std::path::Path, target: &std::path::Path) {
    let connection = rusqlite::Connection::open(source).unwrap();
    connection
        .execute("VACUUM INTO ?1", [target.to_string_lossy().as_ref()])
        .unwrap();
}

fn workflow_batches(events: &[EventEnvelope]) -> Vec<Vec<EventEnvelope>> {
    let mut batches: Vec<Vec<EventEnvelope>> = Vec::new();
    for event in events {
        if batches
            .last()
            .is_none_or(|batch| batch[0].correlation_id != event.correlation_id)
        {
            batches.push(Vec::new());
        }
        batches.last_mut().unwrap().push(event.clone());
    }
    batches
}

fn assert_canonical_batch_is_accepted_from(
    state: &StrategyState,
    events: &[EventEnvelope],
    database: &std::path::Path,
) {
    let mut store = SqliteStore::open(database).unwrap();
    store.migrate().unwrap();
    let mut projected = state.clone();
    let mut sequence = store
        .load_after(&state.run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let prefix_sequence = sequence;
    let decision_start = events
        .iter()
        .position(|event| event.event_type != EventType::MarketDataReceived)
        .unwrap();
    let mut market = events[..decision_start].to_vec();
    assert_eq!(market.len(), 1);
    LedgerWriter::new(&mut store, &mut projected, &mut sequence)
        .append_batch(&mut market)
        .unwrap();
    for mut canonical in workflow_batches(&events[decision_start..]) {
        LedgerWriter::new(&mut store, &mut projected, &mut sequence)
            .append_batch(&mut canonical)
            .unwrap();
    }
    assert_eq!(sequence, prefix_sequence + events.len() as i64);
}

fn assert_forged_batch_is_atomically_rejected_from(
    state: &StrategyState,
    mut events: Vec<EventEnvelope>,
    database: &std::path::Path,
) {
    let mut store = SqliteStore::open(database).unwrap();
    store.migrate().unwrap();
    let mut projected = state.clone();
    let mut sequence = store
        .load_after(&state.run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let decision_start = events
        .iter()
        .position(|event| event.event_type != EventType::MarketDataReceived)
        .unwrap();
    let mut market = events.drain(..decision_start).collect::<Vec<_>>();
    assert_eq!(market.len(), 1);
    LedgerWriter::new(&mut store, &mut projected, &mut sequence)
        .append_batch(&mut market)
        .unwrap();
    let mut batches = workflow_batches(&events);
    let attack_index = batches
        .iter()
        .position(|batch| {
            batch
                .iter()
                .any(|event| event.event_type == EventType::OrderIntentCreated)
        })
        .unwrap();
    for mut prefix in batches.drain(..attack_index) {
        LedgerWriter::new(&mut store, &mut projected, &mut sequence)
            .append_batch(&mut prefix)
            .unwrap();
    }
    assert_eq!(
        batches.len(),
        1,
        "fault boundary must end at the forged intent batch"
    );
    let before_sequence = sequence;
    let before = serde_json::to_value(&projected).unwrap();
    assert!(LedgerWriter::new(&mut store, &mut projected, &mut sequence)
        .append_batch(&mut batches[0])
        .is_err());
    assert_eq!(sequence, before_sequence);
    assert!(store
        .load_after(&state.run_id, before_sequence)
        .unwrap()
        .is_empty());
    assert_eq!(serde_json::to_value(&projected).unwrap(), before);
}

#[test]
fn ledger_rejects_a_joint_reservation_and_intent_swap_from_deep_to_shallow_lot() {
    let source_temp = TempDir::new().unwrap();
    let mut config = config_in(&source_temp);
    config.initial_position = 3_000;
    config.initial_sellable = 3_000;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.paper.hold_open_probability_bps = 10_000;
    config.validate().unwrap();
    let run_id = "sell-deep-shallow-source";
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(DeepSellHalfGate),
        Some(run_id.into()),
    )
    .unwrap();
    service.state.lots.insert(
        "deep-lot".into(),
        priced_lot("deep-lot", -2, 1_500, d("12000")),
    );
    service.state.lots.insert(
        "shallow-lot".into(),
        priced_lot("shallow-lot", -1, 1_500, d("12000")),
    );
    service.state.validate_invariants().unwrap();
    service
        .on_bar(&bar("2026-01-06 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let before_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let pre_crossing = service.state.clone();
    let valid_temp = TempDir::new().unwrap();
    let valid_database = valid_temp.path().join("canonical.db");
    let attack_temp = TempDir::new().unwrap();
    let attack_database = attack_temp.path().join("attack.db");
    let policy_attack_temp = TempDir::new().unwrap();
    let policy_attack_database = policy_attack_temp.path().join("policy-attack.db");
    copy_sqlite_database(std::path::Path::new(&config.database), &valid_database);
    copy_sqlite_database(std::path::Path::new(&config.database), &attack_database);
    copy_sqlite_database(
        std::path::Path::new(&config.database),
        &policy_attack_database,
    );
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-06 09:31:00", "10", "10.45", "9.99", "10.40",))
        .is_err());
    let canonical = service.store.load_after(run_id, before_sequence).unwrap();
    let order_event = canonical
        .iter()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    let canonical_order: Order = serde_json::from_value(order_event.payload.clone()).unwrap();
    assert_eq!(canonical_order.intent.quantity, 1_500);
    assert_eq!(canonical_order.intent.target_lot_ids, vec!["deep-lot"]);
    let right = service
        .state
        .grid_rights
        .get(&canonical_order.intent.right_id)
        .unwrap();
    assert_eq!(right.grid_index, 2);
    let shallow_tranche_id = service
        .state
        .right_tranches
        .values()
        .find(|tranche| {
            tranche.owner_right_id == canonical_order.intent.right_id
                && tranche.lot_id.as_deref() == Some("shallow-lot")
        })
        .unwrap()
        .tranche_id
        .clone();

    assert_canonical_batch_is_accepted_from(&pre_crossing, &canonical, &valid_database);

    let mut forged_policy_order = canonical.clone();
    let reservation_index = forged_policy_order
        .iter()
        .position(|event| {
            event.event_type == EventType::RightBalanceReserved
                && event.payload["right_id"] == canonical_order.intent.right_id
        })
        .unwrap();
    let mut early_reservation = forged_policy_order.remove(reservation_index);
    early_reservation
        .payload
        .as_object_mut()
        .unwrap()
        .remove("allocation_policy");
    let decision_index = forged_policy_order
        .iter()
        .position(|event| {
            event.event_type == EventType::GateDecisionMade
                && event.payload["response"]["right_id"] == canonical_order.intent.right_id
        })
        .unwrap();
    forged_policy_order.insert(decision_index, early_reservation);
    assert_forged_batch_is_atomically_rejected_from(
        &pre_crossing,
        forged_policy_order,
        &policy_attack_database,
    );

    let mut forged = canonical.clone();
    let reservation = forged
        .iter_mut()
        .find(|event| {
            event.event_type == EventType::RightBalanceReserved
                && event.payload["right_id"] == canonical_order.intent.right_id
        })
        .unwrap();
    reservation.payload["allocations"] = json!([{
        "tranche_id": shallow_tranche_id,
        "budget": "0",
        "quantity": 1500
    }]);
    let mut shallow_allocation = plan_non_loss_sales(
        &config,
        [pre_crossing.lots.get("shallow-lot").unwrap()],
        1_500,
        canonical_order.intent.limit_price,
    )
    .remove(0);
    shallow_allocation.tranche_id = shallow_tranche_id;
    let intent = forged
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    let mut forged_order: Order = serde_json::from_value(intent.payload.clone()).unwrap();
    forged_order.intent.target_lot_ids = vec!["shallow-lot".into()];
    forged_order.intent.target_lot_allocations = vec![shallow_allocation];
    intent.payload = serde_json::to_value(forged_order).unwrap();
    assert_forged_batch_is_atomically_rejected_from(&pre_crossing, forged, &attack_database);
}

#[test]
fn ledger_rejects_reordered_700_and_800_sell_allocations_even_when_both_sides_match() {
    let source_temp = TempDir::new().unwrap();
    let mut config = config_in(&source_temp);
    config.anchor_price = d("9.80");
    config.initial_position = 1_500;
    config.initial_sellable = 1_500;
    config.min_base_position = 0;
    config.max_position = 100_000;
    config.commission_rate = Decimal::ZERO;
    config.minimum_commission = d("5");
    config.sell_tax_rate = Decimal::ZERO;
    config.slippage_rate = Decimal::ZERO;
    config.paper.hold_open_probability_bps = 10_000;
    config.validate().unwrap();
    let source_database = config.database.clone();
    let run_id = "sell-700-800-order-source";
    let mut service = GridAutomationService::start_new(
        config,
        SqliteStore::open(source_temp.path().join("test.db")).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some(run_id.into()),
    )
    .unwrap();
    service.state.lots.insert(
        "sell-700".into(),
        priced_lot("sell-700", -1, 700, d("6990")),
    );
    service.state.lots.insert(
        "sell-800".into(),
        priced_lot("sell-800", -1, 800, d("7990")),
    );
    service.state.validate_invariants().unwrap();
    service
        .on_bar(&bar("2026-01-06 09:30:00", "9.80", "9.81", "9.79", "9.80"))
        .unwrap();
    let before_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let pre_crossing = service.state.clone();
    let valid_temp = TempDir::new().unwrap();
    let valid_database = valid_temp.path().join("canonical.db");
    let attack_temp = TempDir::new().unwrap();
    let attack_database = attack_temp.path().join("attack.db");
    copy_sqlite_database(std::path::Path::new(&source_database), &valid_database);
    copy_sqlite_database(std::path::Path::new(&source_database), &attack_database);
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-06 09:31:00", "10", "10.01", "9.99", "10",))
        .is_err());
    let canonical = service.store.load_after(run_id, before_sequence).unwrap();
    let order: Order = serde_json::from_value(
        canonical
            .iter()
            .find(|event| event.event_type == EventType::OrderIntentCreated)
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    assert_eq!(order.intent.quantity, 1_500);
    assert_eq!(order.intent.target_lot_allocations.len(), 2);
    assert_eq!(
        order
            .intent
            .target_lot_allocations
            .iter()
            .map(|allocation| allocation.quantity)
            .sum::<i64>(),
        1_500
    );

    assert_canonical_batch_is_accepted_from(&pre_crossing, &canonical, &valid_database);

    let mut forged = canonical.clone();
    let reservation = forged
        .iter_mut()
        .find(|event| {
            event.event_type == EventType::RightBalanceReserved
                && event.payload["right_id"] == order.intent.right_id
        })
        .unwrap();
    reservation.payload["allocations"]
        .as_array_mut()
        .unwrap()
        .reverse();
    let intent = forged
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    let mut forged_order: Order = serde_json::from_value(intent.payload.clone()).unwrap();
    forged_order.intent.target_lot_ids.reverse();
    forged_order.intent.target_lot_allocations.reverse();
    intent.payload = serde_json::to_value(forged_order).unwrap();
    assert_forged_batch_is_atomically_rejected_from(&pre_crossing, forged, &attack_database);
}

#[test]
fn decision_v3_partitions_gross_capacity_into_residual_and_whole_algorithm_units() {
    for (gross, unit, authorized, residual) in [
        (0, 10_000, 0, 0),
        (8_700, 10_000, 0, 8_700),
        (18_700, 10_000, 10_000, 8_700),
        (15_000, 5_000, 15_000, 0),
    ] {
        let request = quantity_decision_request(gross, unit, 100);
        assert_eq!(request.context.gross_available_quantity, gross);
        assert_eq!(request.context.platform_residual_quantity, residual);
        assert_eq!(request.context.algorithm_authorized_quantity, authorized);
        assert_eq!(request.context.available_quantity, authorized);
        request.validate().unwrap();
        quantity_decision_response(&request, 0, authorized)
            .validate_for(&request)
            .unwrap();
    }
}

#[test]
fn decision_v3_rejects_board_lot_aligned_fractional_algorithm_units() {
    let request = quantity_decision_request(3_000, 1_500, 100);
    quantity_decision_response(&request, 1_500, 1_500)
        .validate_for(&request)
        .unwrap();
    for (exercise, defer) in [(1_200, 1_800), (2_200, 800), (700, 2_300)] {
        assert!(
            quantity_decision_response(&request, exercise, defer)
                .validate_for(&request)
                .is_err(),
            "fractional-unit decision {exercise}/{defer} was accepted"
        );
    }

    let custom = quantity_decision_request(15_000, 5_000, 100);
    quantity_decision_response(&custom, 5_000, 10_000)
        .validate_for(&custom)
        .unwrap();
    assert!(quantity_decision_response(&custom, 7_500, 7_500)
        .validate_for(&custom)
        .is_err());
}

#[test]
fn gate_fraction_is_discretized_by_standard_quantity_not_lot_size() {
    let context = quantity_decision_request(15_000, 5_000, 100).context;
    let half = GateDecision {
        probability: d("0.5"),
        alpha: d("0.5"),
        alpha_numerator: None,
        alpha_denominator: None,
        action: "EXECUTE".into(),
        reason_codes: vec!["HALF".into()],
        model_name: "test".into(),
        model_version: "3".into(),
        input_snapshot_hash: context_hash(&context).unwrap(),
        decided_at: context.current_time,
    };
    assert_eq!(
        RightDecision::from_gate(
            &half,
            context.algorithm_authorized_quantity,
            context.standard_quantity,
        ),
        RightDecision::Exercise {
            exercise_quantity: 5_000,
            defer_quantity: 10_000,
        }
    );
}

#[test]
fn gate_action_describes_the_discrete_whole_unit_outcome() {
    let one_unit = quantity_decision_request(1_500, 1_500, 100).context;
    for alpha in [d("0.25"), d("0.5"), d("0.75")] {
        let decision = FixedGate {
            probability: alpha,
            alpha,
        }
        .evaluate(&one_unit)
        .unwrap();
        assert_eq!(decision.action, "SKIP");
        decision.validate_for(&one_unit).unwrap();
        assert_eq!(
            RightDecision::from_gate(
                &decision,
                one_unit.algorithm_authorized_quantity,
                one_unit.standard_quantity,
            ),
            RightDecision::Defer {
                defer_quantity: 1_500,
            }
        );
    }

    let two_units = quantity_decision_request(3_000, 1_500, 100).context;
    let half = FixedGate {
        probability: d("0.5"),
        alpha: d("0.5"),
    }
    .evaluate(&two_units)
    .unwrap();
    assert_eq!(half.action, "EXECUTE");
    half.validate_for(&two_units).unwrap();
    assert_eq!(
        RightDecision::from_gate(
            &half,
            two_units.algorithm_authorized_quantity,
            two_units.standard_quantity,
        ),
        RightDecision::Exercise {
            exercise_quantity: 1_500,
            defer_quantity: 1_500,
        }
    );
}

#[test]
fn current_gate_validation_rejects_extreme_or_noncanonical_outcomes_without_panicking() {
    let request = quantity_decision_request(3_000, 1_500, 100);

    let mut extreme = quantity_decision_response(&request, 1_500, 1_500);
    extreme.decision.alpha = Decimal::MAX;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        extreme.validate_for(&request)
    }));
    assert!(result.is_ok(), "invalid alpha escaped as a panic");
    assert!(result.unwrap().is_err());

    let mut forged = quantity_decision_response(&request, 3_000, 0);
    forged.decision.probability = d("0.5");
    forged.decision.alpha = d("0.5");
    forged.decision.action = "SKIP".into();
    assert!(forged.validate_for(&request).is_err());

    let mut typed_authority = quantity_decision_response(&request, 3_000, 0);
    typed_authority.decision.probability = d("0.5");
    typed_authority.decision.alpha = d("0.5");
    typed_authority.decision.action = "EXECUTE".into();
    typed_authority.validate_for(&request).unwrap();

    let mut canonical = quantity_decision_response(&request, 1_500, 1_500);
    canonical.decision.probability = d("0.5");
    canonical.decision.alpha = d("0.5");
    canonical.decision.action = "EXECUTE".into();
    canonical.validate_for(&request).unwrap();
}

#[test]
fn fixed_and_simple_rule_gates_are_bounded_transparent_and_discrete() {
    let fixed = FixedGate {
        probability: d("0.7"),
        alpha: d("0.5"),
    }
    .evaluate(&gate_context())
    .unwrap();
    assert_eq!(fixed.alpha, d("0.5"));
    assert_eq!(fixed.action, "EXECUTE");
    let simple = SimpleRuleGate.evaluate(&gate_context()).unwrap();
    assert!([d("0"), d("0.25"), d("0.5"), d("0.75"), d("1")].contains(&simple.alpha));
    assert!(!simple.reason_codes.is_empty());
    assert!(!simple.input_snapshot_hash.is_empty());
}

struct FailingGate;
impl GatePolicy for FailingGate {
    fn evaluate(&self, _context: &GateContext) -> anyhow::Result<GateDecision> {
        anyhow::bail!("deliberate gate failure")
    }
}

struct FailOnceGate {
    calls: AtomicUsize,
}

impl GatePolicy for FailOnceGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            anyhow::bail!("deliberate first-decision failure")
        }
        Ok(GateDecision {
            probability: Decimal::ONE,
            alpha: Decimal::ONE,
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["SECOND_CALL_WOULD_EXECUTE".into()],
            model_name: "fail-once-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

#[test]
fn gate_failure_is_audited_and_enters_configured_safe_mode() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FailingGate),
        Some("gate-failure-run".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    assert!(service
        .store
        .load_after("gate-failure-run", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::ErrorRecorded));
}

#[test]
fn safe_mode_logs_every_later_grid_opportunity_without_creating_an_order() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(FailingGate),
        Some("safe-opportunity-log".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    let safe_crossing = bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62");
    service.on_bar(&safe_crossing).unwrap();
    let events = service.store.load_after("safe-opportunity-log", 0).unwrap();
    let safe_touches = events
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridLevelTouched
                && event.event_time == safe_crossing.timestamp
        })
        .count();
    let safe_skips = events
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridLevelSkipped
                && event.event_time == safe_crossing.timestamp
                && event.payload["reason"] == "SERVICE_MODE_SAFE"
        })
        .count();
    assert_eq!(safe_touches, 1);
    assert_eq!(safe_skips, 1);
    assert!(service.state.orders.is_empty());
    assert!(!service
        .state
        .grid_rights
        .values()
        .any(|right| right.grid_index == -2));
    let event_count = events.len();
    service.on_bar(&safe_crossing).unwrap();
    assert_eq!(
        service
            .store
            .load_after("safe-opportunity-log", 0)
            .unwrap()
            .len(),
        event_count
    );
    drop(service);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut recovered = GridAutomationService::recover(
        config,
        store,
        Box::new(FailingGate),
        "safe-opportunity-log".into(),
    )
    .unwrap();
    recovered.on_bar(&safe_crossing).unwrap();
    assert_eq!(
        recovered
            .store
            .load_after("safe-opportunity-log", 0)
            .unwrap()
            .len(),
        event_count + 1,
        "recovery adds only its explicit recovery audit event"
    );
}

#[test]
fn safe_and_read_only_modes_apply_reversal_before_resume_and_reentry() {
    for read_only in [false, true] {
        let temp = TempDir::new().unwrap();
        let config = config_in(&temp);
        let run_id = if read_only {
            "read-only-reversal"
        } else {
            "safe-reversal"
        };
        let deferred_gate = || {
            Box::new(FixedGate {
                probability: Decimal::ZERO,
                alpha: Decimal::ZERO,
            }) as Box<dyn GatePolicy>
        };
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new(
            config.clone(),
            store,
            deferred_gate(),
            Some(run_id.into()),
        )
        .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
            .unwrap();
        if read_only {
            service.save_snapshot().unwrap();
        }
        service
            .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
            .unwrap();
        assert_eq!(
            service
                .state
                .right_tranches
                .values()
                .map(|tranche| tranche.available_quantity)
                .sum::<i64>(),
            config.standard_quantity
        );
        if read_only {
            service.save_snapshot().unwrap();
            drop(service);
            let connection = rusqlite::Connection::open(&config.database).unwrap();
            connection
                .execute(
                    "UPDATE snapshots SET checksum='corrupt' WHERE run_id=?1 AND sequence_number=(SELECT MAX(sequence_number) FROM snapshots WHERE run_id=?1)",
                    [run_id],
                )
                .unwrap();
            drop(connection);
            let store = SqliteStore::open(&config.database).unwrap();
            service = GridAutomationService::recover(
                config.clone(),
                store,
                deferred_gate(),
                run_id.into(),
            )
            .unwrap();
            assert_eq!(service.state.mode, ServiceMode::ReadOnly);
        } else {
            let mut mismatch = service.state.snapshot().unwrap();
            mismatch.cash.available += Decimal::ONE;
            drop(service);
            write_paper_snapshot(&config, run_id, &mismatch);
            let store = SqliteStore::open(&config.database).unwrap();
            service = GridAutomationService::recover(
                config.clone(),
                store,
                deferred_gate(),
                run_id.into(),
            )
            .unwrap();
            assert!(!service.reconcile().unwrap().matched);
            assert_eq!(service.state.mode, ServiceMode::Safe);
        }

        service
            .on_bar(&bar("2026-01-05 09:32:00", "9.82", "10.01", "9.81", "10"))
            .unwrap();
        assert_eq!(
            service
                .state
                .right_tranches
                .values()
                .map(|tranche| tranche.revoked_quantity)
                .sum::<i64>(),
            config.standard_quantity,
            "the marginal authority must be revoked even when trading is disabled"
        );
        assert_eq!(
            service
                .state
                .right_tranches
                .values()
                .map(|tranche| tranche.available_quantity)
                .sum::<i64>(),
            0
        );

        let broker = service.state.snapshot().unwrap();
        drop(service);
        write_paper_snapshot(&config, run_id, &broker);
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service =
            GridAutomationService::recover(config.clone(), store, deferred_gate(), run_id.into())
                .unwrap();
        assert!(service.reconcile().unwrap().matched);
        service
            .resume_after_reconciliation("mechanical reversal audit verified")
            .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:33:00", "10", "10.01", "9.79", "9.82"))
            .unwrap();
        assert_eq!(
            service
                .state
                .right_tranches
                .values()
                .map(|tranche| tranche.available_quantity)
                .sum::<i64>(),
            config.standard_quantity,
            "re-entry may mint one new epoch but cannot carry the revoked token"
        );
        assert_eq!(
            service.state.right_tranches.len(),
            2,
            "the old token remains terminal and the new epoch is distinct"
        );
    }
}

#[test]
fn entering_safe_on_the_first_touch_blocks_every_later_touch_in_the_same_bar() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FailOnceGate {
            calls: AtomicUsize::new(0),
        }),
        Some("same-bar-safe-barrier".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let two_levels = bar("2026-01-05 09:31:00", "10", "10.01", "9.59", "9.62");
    service.on_bar(&two_levels).unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    let events = service
        .store
        .load_after("same-bar-safe-barrier", 0)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == EventType::GridLevelTouched
                    && event.event_time == two_levels.timestamp
            })
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == EventType::GridLevelSkipped
                    && event.event_time == two_levels.timestamp
                    && event.payload["reason"] == "SERVICE_MODE_SAFE"
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::GateDecisionMade)
            .count(),
        1,
        "the second mechanical opportunity must not call the algorithm"
    );
}

#[test]
fn full_mechanical_replay_closes_correct_lots_resets_cycle_and_rebuilds() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let first_cycle =
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"mechanical-run:cycle:1").to_string();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("mechanical-run".into()),
    )
    .unwrap();
    let mut feed = CsvReplayFeed::load("tests/fixtures/sample.csv", &config.symbol).unwrap();
    service.run_feed(&mut feed).unwrap();
    assert_ne!(service.state.cycle_id, first_cycle);
    assert_eq!(service.state.deployed_grid_budget, Decimal::ZERO);
    assert_eq!(service.state.position.total, config.initial_position);
    assert!(service
        .state
        .lots
        .values()
        .all(|lot| lot.remaining_quantity == 0));
    assert_eq!(
        service.state.historically_executed_levels,
        [-4, -3, -2, -1, 1, 2, 3, 4].into_iter().collect()
    );
    service.stop().unwrap();
    let expected = serde_json::to_value(&service.state).unwrap();
    let initial = StrategyState::new(
        "mechanical-run".into(),
        String::new(),
        config.symbol,
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let (rebuilt, _) = service.store.rebuild_full(initial).unwrap();
    gridedge_t::service::compare_states(&rebuilt, &service.state).unwrap();
    assert_eq!(serde_json::to_value(rebuilt).unwrap(), expected);
}

struct SellCarryGate;

impl GatePolicy for SellCarryGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        let alpha = match (context.direction, context.grid_index) {
            (Direction::Sell, 1) => Decimal::ZERO,
            (Direction::Sell, 2) => d("0.5"),
            _ => Decimal::ONE,
        };
        Ok(GateDecision {
            probability: alpha,
            alpha,
            alpha_numerator: None,
            alpha_denominator: None,
            action: if alpha.is_zero() {
                "SKIP".into()
            } else {
                "EXECUTE".into()
            },
            reason_codes: vec!["SELL_CARRY_TEST".into()],
            model_name: "sell-carry-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

#[test]
fn sell_rights_carry_from_skipped_level_and_alpha_controls_quantity() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(SellCarryGate),
        Some("sell-carry".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
        bar("2026-01-06 09:30:00", "9.62", "10.21", "9.61", "10.15"),
        bar("2026-01-06 09:31:00", "10.15", "10.41", "10.14", "10.35"),
    ] {
        service.on_bar(&market).unwrap();
    }

    let deferred = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell && right.grid_index == 1)
        .unwrap();
    assert_eq!(deferred.status, GridRightStatus::Transferred);
    let exercised = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell && right.grid_index == 2)
        .unwrap();
    assert_eq!(exercised.status, GridRightStatus::PartiallyExercised);
    assert_eq!(exercised.capacity.accumulated_grid_indices, vec![-2, -1]);
    assert_eq!(exercised.capacity.eligible_lot_ids.len(), 2);
    let expected_quantity = ((exercised.capacity.eligible_quantity / 2) / 100) * 100;
    assert_eq!(exercised.exercised_quantity, expected_quantity);
    let sell_order = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap();
    assert!(!sell_order.intent.target_lot_allocations.is_empty());
    assert_eq!(
        sell_order
            .intent
            .target_lot_allocations
            .iter()
            .map(|allocation| allocation.quantity)
            .sum::<i64>(),
        expected_quantity
    );
    assert!(sell_order
        .intent
        .target_lot_allocations
        .iter()
        .all(|allocation| allocation.worst_case_profit >= Decimal::ZERO));
    for allocation in &sell_order.intent.target_lot_allocations {
        let tranche = &service.state.right_tranches[&allocation.tranche_id];
        assert_eq!(tranche.lot_id.as_deref(), Some(allocation.lot_id.as_str()));
        assert_eq!(tranche.consumed_quantity, allocation.quantity);
    }
    assert_eq!(sell_order.intent.quantity, expected_quantity);
}

#[test]
fn platform_risk_block_is_distinct_from_algorithm_defer() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.initial_cash = d("100");
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("risk-blocked-right".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    let right = service.state.grid_rights.values().next().unwrap();
    assert_eq!(right.status, GridRightStatus::Blocked);
    assert!(service.state.orders.is_empty());
    assert!(service
        .store
        .load_after("risk-blocked-right", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::GridRightBlocked));
}

#[test]
fn same_day_sell_right_is_blocked_by_t_plus_one_and_can_execute_next_day() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("t-plus-one-right".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let blocked = service
        .state
        .grid_rights
        .values()
        .find(|right| right.direction == Direction::Sell)
        .unwrap();
    assert_eq!(blocked.status, GridRightStatus::Blocked);
    let events = service.store.load_after("t-plus-one-right", 0).unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == EventType::GridRightBlocked
            && event.payload["reason"] == "SELL_BLOCKED_T_PLUS_ONE"
    }));

    service
        .on_bar(&bar(
            "2026-01-06 09:30:00",
            "10.15",
            "10.41",
            "10.14",
            "10.35",
        ))
        .unwrap();
    assert!(service
        .state
        .grid_rights
        .values()
        .any(|right| right.direction == Direction::Sell
            && right.status == GridRightStatus::Exercised));
    assert!(service.reconcile().unwrap().matched);
}

#[test]
fn gate_context_snapshot_is_persisted_and_hash_is_offline_verifiable() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("auditable-gate-context".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    let events = service
        .store
        .load_after("auditable-gate-context", 0)
        .unwrap();
    assert!(events.iter().any(|event| event.causation_id.is_some()));
    let payload = events
        .into_iter()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap()
        .payload;
    let context: GateContext =
        serde_json::from_value(payload["request"]["context"].clone()).unwrap();
    assert_eq!(
        context_hash(&context).unwrap(),
        payload["response"]["decision"]["input_snapshot_hash"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn rejected_order_releases_right_and_does_not_consume_deferred_capacity() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.reject_probability_bps = 10_000;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("released-right".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-05 09:32:00", "9.82", "9.83", "9.59", "9.62"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let first = service
        .state
        .grid_rights
        .values()
        .find(|right| right.grid_index == -1)
        .unwrap();
    let second = service
        .state
        .grid_rights
        .values()
        .find(|right| right.grid_index == -2)
        .unwrap();
    assert_eq!(first.status, GridRightStatus::Transferred);
    assert_eq!(second.status, GridRightStatus::Released);
    assert_eq!(second.capacity.deployed_budget, Decimal::ZERO);
    assert_eq!(
        second.capacity.available_budget,
        config.standard_budget * d("2")
    );
    assert_eq!(service.state.cash.frozen, Decimal::ZERO);
}

#[test]
fn cancelling_an_open_sell_atomically_releases_lot_right_and_frozen_stock() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.hold_open_probability_bps = 10_000;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("cancel-open-sell".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let sell_order = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap()
        .clone();
    assert_eq!(sell_order.status, OrderStatus::Accepted);
    assert_eq!(
        service.state.position.frozen_sell,
        sell_order.intent.quantity
    );
    service
        .cancel_order(&sell_order.order_id, "CASE_CANCEL_REMAINDER")
        .unwrap();
    assert_eq!(
        service.state.orders[&sell_order.order_id].status,
        OrderStatus::Cancelled
    );
    assert_eq!(service.state.position.frozen_sell, 0);
    assert_eq!(
        service.state.grid_rights[&sell_order.intent.right_id].status,
        GridRightStatus::Released
    );
    for allocation in &sell_order.intent.target_lot_allocations {
        let tranche = &service.state.right_tranches[&allocation.tranche_id];
        assert_eq!(tranche.reserved_quantity, 0);
        assert_eq!(tranche.available_quantity, tranche.minted_quantity);
        assert_eq!(tranche.consumed_quantity, 0);
    }
    assert_eq!(
        service
            .store
            .event_count_by_type("cancel-open-sell", EventType::OrderCancelRequested)
            .unwrap(),
        1
    );
    assert_eq!(
        service
            .store
            .event_count_by_type("cancel-open-sell", EventType::OrderCancelled)
            .unwrap(),
        1
    );
    drop(service);
    let recovered = GridAutomationService::recover(
        config,
        SqliteStore::open(temp.path().join("test.db")).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "cancel-open-sell".into(),
    )
    .unwrap();
    assert_eq!(
        recovered.state.orders[&sell_order.order_id].status,
        OrderStatus::Cancelled
    );
    assert_eq!(recovered.state.position.frozen_sell, 0);
}

#[test]
fn recovery_finishes_a_cancel_requested_before_the_broker_side_effect() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.hold_open_probability_bps = 10_000;
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("cancel-recovery".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let order_id = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap()
        .order_id
        .clone();
    service.inject_fault_once(WorkflowFaultPoint::CancelRequested);
    assert!(service.cancel_order(&order_id, "RECOVER_CANCEL").is_err());
    assert_eq!(
        service.state.orders[&order_id].status,
        OrderStatus::CancelPending
    );
    drop(service);
    let recovered = GridAutomationService::recover(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "cancel-recovery".into(),
    )
    .unwrap();
    assert_eq!(
        recovered.state.orders[&order_id].status,
        OrderStatus::Cancelled
    );
    assert_eq!(recovered.state.position.frozen_sell, 0);
    assert_eq!(recovered.state.mode, ServiceMode::Running);
}

#[test]
fn cancel_requested_recovery_preserves_reason_time_causation_and_broker_report() {
    type CancelTrace = Vec<(EventType, String, String, String, Option<String>)>;
    fn run(temp: &TempDir, fault: bool) -> (StrategyState, CancelTrace, String) {
        let mut config = config_in(temp);
        config.paper.hold_open_probability_bps = 10_000;
        let mut service = GridAutomationService::start_new(
            config.clone(),
            SqliteStore::open(&config.database).unwrap(),
            Box::new(gridedge_t::gate::AlwaysExecuteGate),
            Some("cancel-determinism".into()),
        )
        .unwrap();
        for market in [
            bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
            bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
            bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
        ] {
            service.on_bar(&market).unwrap();
        }
        let order = service
            .state
            .orders
            .values()
            .find(|order| order.intent.direction == Direction::Sell)
            .unwrap()
            .clone();
        if fault {
            service.inject_fault_once(WorkflowFaultPoint::CancelRequested);
            assert!(service
                .cancel_order(&order.order_id, "DURABLE_OPERATOR_REASON")
                .is_err());
            drop(service);
            service = GridAutomationService::recover(
                config.clone(),
                SqliteStore::open(&config.database).unwrap(),
                Box::new(gridedge_t::gate::AlwaysExecuteGate),
                "cancel-determinism".into(),
            )
            .unwrap();
        } else {
            service
                .cancel_order(&order.order_id, "DURABLE_OPERATOR_REASON")
                .unwrap();
        }
        let trace = service
            .store
            .load_after("cancel-determinism", 0)
            .unwrap()
            .into_iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    EventType::OrderCancelRequested
                        | EventType::OrderCancelled
                        | EventType::RightBalanceReleased
                        | EventType::GridRightReleased
                )
            })
            .map(|event| {
                (
                    event.event_type,
                    event.idempotency_key,
                    event.payload.to_string(),
                    event.correlation_id,
                    event.causation_id,
                )
            })
            .collect();
        let report: String = rusqlite::Connection::open(&config.database)
            .unwrap()
            .query_row(
                "SELECT report_json FROM paper_orders WHERE run_id=?1 AND intent_id=?2",
                rusqlite::params!["cancel-determinism", order.intent.intent_id],
                |row| row.get(0),
            )
            .unwrap();
        (service.state, trace, report)
    }

    let uninterrupted_temp = TempDir::new().unwrap();
    let recovered_temp = TempDir::new().unwrap();
    let (uninterrupted, uninterrupted_trace, uninterrupted_report) =
        run(&uninterrupted_temp, false);
    let (recovered, recovered_trace, recovered_report) = run(&recovered_temp, true);
    assert_eq!(uninterrupted_trace, recovered_trace);
    assert_eq!(uninterrupted_report, recovered_report);
    assert!(uninterrupted_report.contains("DURABLE_OPERATOR_REASON"));
    let uninterrupted_cancel = uninterrupted
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap();
    let recovered_cancel = recovered
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap();
    assert_eq!(uninterrupted_cancel.status, recovered_cancel.status);
    assert_eq!(
        uninterrupted_cancel.cancel_requested_reason,
        recovered_cancel.cancel_requested_reason
    );
    assert!(uninterrupted_cancel.cancel_requested_at.is_some());
    assert!(recovered_cancel.cancel_requested_at.is_some());
    assert_eq!(
        serde_json::to_value(uninterrupted.right_tranches).unwrap(),
        serde_json::to_value(recovered.right_tranches).unwrap()
    );
    assert_eq!(uninterrupted.cash, recovered.cash);
    assert_eq!(uninterrupted.position, recovered.position);
}

#[test]
fn forged_current_sell_policy_cost_fees_or_schema_downgrade_never_enter_the_ledger() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.hold_open_probability_bps = 10_000;
    let mut service = GridAutomationService::start_new(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("forged-v2-fill".into()),
    )
    .unwrap();
    for market in [
        bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"),
        bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15"),
    ] {
        service.on_bar(&market).unwrap();
    }
    let order = service
        .state
        .orders
        .values()
        .find(|order| order.intent.direction == Direction::Sell)
        .unwrap()
        .clone();
    assert_eq!(order.status, OrderStatus::Accepted);
    let policy = order.intent.profit_guard_policy.as_ref().unwrap();
    let price = gridedge_t::profit::worst_sell_fill_price(&config, order.intent.limit_price);
    let canonical = gridedge_t::profit::canonical_fill_allocations(
        policy,
        &order.intent.target_lot_allocations,
        0,
        order.intent.quantity,
        order.intent.limit_price,
    )
    .unwrap();
    let (commission, tax) = gridedge_t::profit::actual_order_fill_charges_with_policy(
        policy,
        Direction::Sell,
        Decimal::ZERO,
        Decimal::ZERO,
        price,
        order.intent.quantity,
    );
    let mut sequence = service
        .store
        .load_after("forged-v2-fill", 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let before_sequence = sequence;
    let before = serde_json::to_value(&service.state).unwrap();
    let cycle_id = service.state.cycle_id.clone();
    let symbol = config.symbol.clone();
    let config_version = config.config_version.clone();

    let mut forged_policy_order = order.clone();
    forged_policy_order.order_id = "forged-policy-order".into();
    forged_policy_order.intent.intent_id = "forged-policy-intent".into();
    forged_policy_order.status = OrderStatus::Created;
    let forged_policy = gridedge_t::domain::ProfitGuardPolicy {
        price_scale: config.price_scale,
        commission_rate: Decimal::ZERO,
        minimum_commission: Decimal::ZERO,
        sell_tax_rate: Decimal::ZERO,
        slippage_rate: Decimal::ZERO,
    };
    forged_policy_order.intent.profit_guard_policy = Some(forged_policy);
    for allocation in &mut forged_policy_order.intent.target_lot_allocations {
        allocation.worst_fill_price = forged_policy_order.intent.limit_price;
        allocation.maximum_sell_fees = Decimal::ZERO;
        allocation.worst_case_profit = allocation.worst_fill_price
            * Decimal::from(allocation.quantity)
            - allocation.cost_basis;
    }
    let forged_policy_event = EventEnvelope::new(
        EventType::OrderIntentCreated,
        "forged-v2-fill",
        &cycle_id,
        &symbol,
        dt("2026-01-06 09:30:00"),
        "forged-policy",
        None,
        "forged-policy-intent".into(),
        serde_json::to_value(forged_policy_order).unwrap(),
        &config_version,
    );
    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence,)
            .append(forged_policy_event.clone())
            .is_err()
    );
    let mut downgraded_policy_event = forged_policy_event;
    downgraded_policy_event.schema_version = 2;
    downgraded_policy_event.idempotency_key = "forged-policy-intent-v2".into();
    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence,)
            .append(downgraded_policy_event)
            .is_err()
    );

    let make_fill_event =
        |key: &str, allocations: Vec<LotQuantityAllocation>, commission: Decimal, tax: Decimal| {
            EventEnvelope::new(
                EventType::OrderFilled,
                "forged-v2-fill",
                &cycle_id,
                &symbol,
                dt("2026-01-06 09:30:00"),
                "forged-fill",
                None,
                key.into(),
                json!({
                    "grid_index": order.intent.grid_index,
                    "fill": Fill {
                        fill_id: key.into(),
                        order_id: order.order_id.clone(),
                        direction: Direction::Sell,
                        price,
                        quantity: order.intent.quantity,
                        commission,
                        tax,
                        trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
                        target_lot_ids: order.intent.target_lot_ids.clone(),
                        target_lot_allocations: allocations,
                    }
                }),
                &config_version,
            )
        };

    let mut forged_cost = canonical.clone();
    forged_cost[0].cost_basis = Decimal::ZERO;
    forged_cost[0].worst_case_profit = forged_cost[0].worst_fill_price
        * Decimal::from(forged_cost[0].quantity)
        - forged_cost[0].maximum_sell_fees;
    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence,)
            .append(make_fill_event(
                "forged-cost-fill",
                forged_cost,
                commission,
                tax,
            ))
            .is_err()
    );

    let mut downgraded_fill = make_fill_event(
        "forged-schema-v2-fill",
        canonical.clone(),
        Decimal::ZERO,
        Decimal::ZERO,
    );
    downgraded_fill.schema_version = 2;
    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence,)
            .append(downgraded_fill)
            .is_err()
    );

    assert!(
        LedgerWriter::new(&mut service.store, &mut service.state, &mut sequence,)
            .append(make_fill_event(
                "forged-zero-fee-fill",
                canonical,
                Decimal::ZERO,
                Decimal::ZERO,
            ))
            .is_err()
    );
    assert_eq!(sequence, before_sequence);
    assert_eq!(serde_json::to_value(&service.state).unwrap(), before);
    assert_eq!(
        service
            .store
            .load_after("forged-v2-fill", before_sequence)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn repeated_partial_sales_conserve_remaining_buy_cost_and_never_realize_a_loss() {
    let mut state = StrategyState::new(
        "partial-cost".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        .unwrap();
    state
        .apply_fill(&buy_fill("partial-cost", 100), -1)
        .unwrap();
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .unwrap();
    for (id, quantity) in [("sell-a", 40), ("sell-b", 60)] {
        let fill = Fill {
            fill_id: id.into(),
            order_id: format!("order-{id}"),
            direction: Direction::Sell,
            price: d("11"),
            quantity,
            commission: d("5"),
            tax: Decimal::ZERO,
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            target_lot_ids: vec!["lot:partial-cost".into()],
            target_lot_allocations: vec![],
        };
        state.apply_fill(&fill, 1).unwrap();
        assert!(state.lots["lot:partial-cost"].realized_pnl >= Decimal::ZERO);
    }
    let lot = &state.lots["lot:partial-cost"];
    assert_eq!(lot.remaining_quantity, 0);
    assert_eq!(lot.remaining_cost, Decimal::ZERO);
    assert_eq!(lot.realized_pnl, state.realized_pnl);
}

#[test]
fn paper_acceptance_and_projection_share_the_exact_fee_allocation_function() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.price_scale = 3;
    config.lot_size = 1;
    config.commission_rate = Decimal::ZERO;
    config.minimum_commission = Decimal::ZERO;
    config.sell_tax_rate = d("0.001");
    config.slippage_rate = Decimal::ZERO;
    config.paper.partial_fill_bps = 0;
    let mut store = SqliteStore::open(&config.database).unwrap();
    store.migrate().unwrap();

    let mut state = StrategyState::new(
        "shared-fee".into(),
        "cycle".into(),
        config.symbol.clone(),
        d("5"),
        d("100"),
        3,
        3,
    );
    state.current_trade_date = Some(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap());
    state
        .lots
        .insert("A".into(), priced_lot("A", -1, 2, d("9.990")));
    state
        .lots
        .insert("B".into(), priced_lot("B", -2, 1, d("4.990")));
    let allocations = vec![
        LotQuantityAllocation {
            lot_id: "A".into(),
            tranche_id: "t-A".into(),
            quantity: 2,
            cost_basis: d("9.990"),
            worst_fill_price: d("5.000"),
            maximum_sell_fees: d("0.01"),
            worst_case_profit: Decimal::ZERO,
        },
        LotQuantityAllocation {
            lot_id: "B".into(),
            tranche_id: "t-B".into(),
            quantity: 1,
            cost_basis: d("4.990"),
            worst_fill_price: d("5.000"),
            maximum_sell_fees: d("0.01"),
            worst_case_profit: Decimal::ZERO,
        },
    ];
    let intent = OrderIntent {
        intent_id: "shared-fee-intent".into(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: "right".into(),
        grid_index: 1,
        direction: Direction::Sell,
        limit_price: d("5.000"),
        quantity: 3,
        budget: d("15"),
        target_lot_ids: vec!["A".into(), "B".into()],
        target_lot_allocations: allocations.clone(),
        profit_guard_version: gridedge_t::profit::PROFIT_GUARD_VERSION.into(),
        profit_guard_policy: Some(gridedge_t::domain::ProfitGuardPolicy::from(&config)),
        created_at: dt("2026-01-06 10:00:00"),
    };
    let mut gateway =
        PaperExecutionGateway::new(&config, "shared-fee", state.snapshot().unwrap()).unwrap();
    let ExecutionReport::Accepted { order_id, fills } = gateway.submit(&intent).unwrap() else {
        panic!("shared proof must be accepted");
    };
    assert_eq!(fills.len(), 1);
    let fill = Fill {
        fill_id: fills[0].fill_id.clone(),
        order_id,
        direction: Direction::Sell,
        price: fills[0].price,
        quantity: fills[0].quantity,
        commission: Decimal::ZERO,
        tax: d("0.02"),
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        target_lot_ids: intent.target_lot_ids,
        target_lot_allocations: allocations,
    };
    state.apply_fill(&fill, 1).unwrap();
    assert_eq!(state.lots["A"].realized_pnl, Decimal::ZERO);
    assert_eq!(state.lots["B"].realized_pnl, Decimal::ZERO);
}

#[test]
fn gate_at_touch_cannot_observe_the_current_bars_future_close() {
    fn decision_hash(temp: &TempDir, second_close: &str) -> String {
        let config = config_in(temp);
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new(
            config,
            store,
            Box::new(gridedge_t::gate::AlwaysExecuteGate),
            Some("no-lookahead".into()),
        )
        .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
            .unwrap();
        service
            .on_bar(&bar(
                "2026-01-05 09:31:00",
                "10",
                "10.01",
                "9.79",
                second_close,
            ))
            .unwrap();
        service
            .store
            .load_after("no-lookahead", 0)
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == EventType::GateDecisionMade)
            .unwrap()
            .payload["response"]["decision"]["input_snapshot_hash"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    let left = TempDir::new().unwrap();
    let right = TempDir::new().unwrap();
    assert_eq!(decision_hash(&left, "9.82"), decision_hash(&right, "9.95"));
}

struct InvalidGate;

impl GatePolicy for InvalidGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        Ok(GateDecision {
            probability: Decimal::ONE,
            alpha: Decimal::MAX,
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["INVALID".into()],
            model_name: "invalid-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

struct FractionalUnitAlgorithm;

impl QuantDecisionAlgorithm for FractionalUnitAlgorithm {
    fn decide(&self, request: &DecisionRequest) -> anyhow::Result<DecisionResponse> {
        let exercise_quantity = request.context.lot_size * 7;
        let defer_quantity = request.context.available_quantity - exercise_quantity;
        Ok(DecisionResponse {
            contract_version: request.contract_version,
            request_id: request.request_id.clone(),
            right_id: request.right.right_id.clone(),
            outcome: RightDecision::Exercise {
                exercise_quantity,
                defer_quantity,
            },
            decision: GateDecision {
                probability: Decimal::ONE,
                alpha: Decimal::ONE,
                alpha_numerator: None,
                alpha_denominator: None,
                action: "EXECUTE".into(),
                reason_codes: vec!["FRACTIONAL_UNIT_ATTACK".into()],
                model_name: "fractional-unit-test".into(),
                model_version: "3".into(),
                input_snapshot_hash: context_hash(&request.context)?,
                decided_at: request.context.current_time,
            },
        })
    }
}

#[test]
fn invalid_algorithm_output_fails_closed_before_order_intent() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(InvalidGate),
        Some("invalid-gate".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    assert!(service
        .state
        .grid_rights
        .values()
        .all(|right| right.status == GridRightStatus::Blocked));
}

#[test]
fn lot_aligned_but_fractional_unit_algorithm_output_enters_safe_without_intent() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    assert_eq!(config.gate.failure_mode, "safe");
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        store,
        Box::new(FractionalUnitAlgorithm),
        Some("fractional-unit-algorithm".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();

    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    let right = service.state.grid_rights.values().next().unwrap();
    assert_eq!(right.decision_contract_version, 3);
    assert_eq!(
        right.decision_gross_available_quantity,
        config.standard_quantity
    );
    assert_eq!(right.decision_platform_residual_quantity, 0);
    assert_eq!(
        right.decision_algorithm_authorized_quantity,
        config.standard_quantity
    );
    assert_eq!(right.decided_exercise_quantity, 0);
    assert_eq!(right.decided_defer_quantity, config.standard_quantity);
    assert_eq!(right.decision_platform_blocked_quantity, 0);
    assert_eq!(right.decision_intent_quantity, 0);
    assert_eq!(right.decision_remaining_quantity, config.standard_quantity);
    let events = service
        .store
        .load_after("fractional-unit-algorithm", 0)
        .unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == EventType::ErrorRecorded
            && event.payload["reason_code"] == "INVALID_DECISION"
    }));
    assert!(!events.iter().any(|event| {
        matches!(
            event.event_type,
            EventType::RightBalanceReserved | EventType::OrderIntentCreated
        )
    }));
}

#[test]
fn stale_run_writer_and_stale_snapshot_are_rejected() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("fenced.db");
    let mut first = SqliteStore::open(&path).unwrap();
    first.migrate().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let initial_state = StrategyState::new(
        "fenced".into(),
        "cycle".into(),
        config.symbol,
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let mut first_state = initial_state.clone();
    let mut first_sequence = 0;
    LedgerWriter::new(&mut first, &mut first_state, &mut first_sequence)
        .append(event("fenced", "one", EventType::RunStarted, json!({})))
        .unwrap();
    let mut stale = SqliteStore::open(&path).unwrap();
    stale.migrate().unwrap();
    let mut stale_state = first_state.clone();
    let mut stale_sequence = first_sequence;
    LedgerWriter::new(&mut first, &mut first_state, &mut first_sequence)
        .append(event("fenced", "two", EventType::ErrorRecorded, json!({})))
        .unwrap();
    assert!(
        LedgerWriter::new(&mut stale, &mut stale_state, &mut stale_sequence)
            .append(event(
                "fenced",
                "stale",
                EventType::ErrorRecorded,
                json!({}),
            ))
            .is_err()
    );
    assert!(
        LedgerWriter::new(&mut stale, &mut stale_state, &mut stale_sequence)
            .save_snapshot()
            .is_err()
    );
}

#[test]
fn recovery_rejects_strategy_configuration_drift() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("config-bound".into()),
    )
    .unwrap();
    drop(service);
    let mut changed = config.clone();
    changed.standard_budget += Decimal::ONE;
    let store = SqliteStore::open(&changed.database).unwrap();
    assert!(GridAutomationService::recover(
        changed,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "config-bound".into(),
    )
    .is_err());
}

#[test]
fn schema1_history_without_algorithm_identity_is_read_only_but_cannot_resume_execution() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("legacy-no-algorithm.db");
    let config_path = temp.path().join("config.yaml");
    let source = std::fs::read_to_string("configs/default.yaml").unwrap();
    let config_text = source.replace(
        "database: \"gridedge.db\"",
        &format!("database: {:?}", database_path.to_string_lossy()),
    );
    std::fs::write(&config_path, config_text).unwrap();
    let data_path = std::fs::canonicalize("tests/fixtures/sample.csv").unwrap();
    let run_id = "schema1-read-only-no-algorithm";
    let initial = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "replay",
            "--config",
            config_path.to_str().unwrap(),
            "--data",
            data_path.to_str().unwrap(),
            "--run-id",
            run_id,
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );

    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            r#"DROP TRIGGER events_are_append_only_update;
             DROP TRIGGER events_are_append_only_delete;
             UPDATE events SET schema_version=1
              WHERE run_id='schema1-read-only-no-algorithm'
               AND event_type='CONFIG_SNAPSHOTTED';
             UPDATE events
                SET event_type='ERROR_RECORDED',schema_version=1,
                    payload='{"entered_safe":false}'
              WHERE run_id='schema1-read-only-no-algorithm'
               AND event_type='ALGORITHM_REGISTERED';"#,
        )
        .unwrap();
    let (head_before, events_before): (i64, i64) = connection
        .query_row(
            "SELECT MAX(sequence_number),COUNT(*) FROM events WHERE run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    drop(connection);

    for command in ["status", "rebuild-state"] {
        let read = Command::new(env!("CARGO_BIN_EXE_gridedge"))
            .args([
                command,
                "--config",
                config_path.to_str().unwrap(),
                "--run-id",
                run_id,
                "--json",
            ])
            .output()
            .unwrap();
        assert!(
            read.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&read.stderr)
        );
    }

    let config = Config::load(&config_path).unwrap();
    let store = SqliteStore::open(&config.database).unwrap();
    let recovery_error = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        run_id.to_owned(),
    )
    .err()
    .expect("legacy history without algorithm identity must stay read-only");
    assert!(recovery_error
        .to_string()
        .contains("run lacks an audited algorithm manifest"));

    let replay = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "replay",
            "--config",
            config_path.to_str().unwrap(),
            "--data",
            data_path.to_str().unwrap(),
            "--run-id",
            run_id,
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!replay.status.success());
    assert!(
        String::from_utf8_lossy(&replay.stderr).contains("run lacks an audited algorithm manifest")
    );

    let resume = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "resume",
            "--config",
            config_path.to_str().unwrap(),
            "--run-id",
            run_id,
            "--reason",
            "must remain read-only",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!resume.status.success());
    assert!(
        String::from_utf8_lossy(&resume.stderr).contains("run lacks an audited algorithm manifest")
    );

    let connection = rusqlite::Connection::open(database_path).unwrap();
    let (head_after, events_after): (i64, i64) = connection
        .query_row(
            "SELECT MAX(sequence_number),COUNT(*) FROM events WHERE run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((head_after, events_after), (head_before, events_before));
}

#[test]
fn reconciliation_reads_an_independent_paper_broker_ledger() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("independent-broker".into()),
    )
    .unwrap();
    drop(service);

    let connection = rusqlite::Connection::open(&config.database).unwrap();
    let raw: String = connection
        .query_row(
            "SELECT snapshot_json FROM paper_accounts WHERE run_id='independent-broker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut broker: AccountSnapshot = serde_json::from_str(&raw).unwrap();
    broker.cash.available += Decimal::ONE;
    connection
        .execute(
            "UPDATE paper_accounts SET snapshot_json=?1 WHERE run_id='independent-broker'",
            [serde_json::to_string(&broker).unwrap()],
        )
        .unwrap();
    drop(connection);

    let store = SqliteStore::open(&config.database).unwrap();
    let mut recovered = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "independent-broker".into(),
    )
    .unwrap();
    let result = recovered.reconcile().unwrap();
    assert!(!result.matched);
    assert_eq!(recovered.state.mode, ServiceMode::Safe);
    assert!(result
        .differences
        .contains(&"cash_or_frozen_cash".to_owned()));
}

struct SlowGate;

impl GatePolicy for SlowGate {
    fn evaluate(&self, context: &GateContext) -> anyhow::Result<GateDecision> {
        std::thread::sleep(std::time::Duration::from_millis(30));
        Ok(GateDecision {
            probability: Decimal::ONE,
            alpha: Decimal::ONE,
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["TOO_LATE".into()],
            model_name: "slow-test".into(),
            model_version: "1".into(),
            input_snapshot_hash: context_hash(context)?,
            decided_at: context.current_time,
        })
    }
}

#[test]
fn algorithm_timeout_is_audited_and_cannot_create_an_order() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.gate.timeout_ms = 1;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(SlowGate),
        Some("timeout-gate".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.mode, ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    assert!(service
        .store
        .load_after("timeout-gate", 0)
        .unwrap()
        .iter()
        .any(|event| event.event_type == EventType::ErrorRecorded
            && event.payload["error"]
                .as_str()
                .is_some_and(|error| error.contains("timed out"))));
}

#[test]
fn rust_process_algorithm_runs_behind_a_killable_protocol_boundary() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.gate.timeout_ms = 2_000;
    let algorithm = ProcessAlgorithm::connect(
        env!("CARGO_BIN_EXE_gridedge_algorithm_probe"),
        Vec::new(),
        Duration::from_secs(3),
    )
    .unwrap();
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config,
        store,
        Box::new(algorithm),
        Some("process-algorithm".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.orders.len(), 1);
    assert!(service.state.grid_rights.values().any(|right| matches!(
        right.status,
        GridRightStatus::PartiallyExercised | GridRightStatus::Exercised
    )));
}

#[test]
fn process_algorithm_identity_binds_binary_arguments_and_rejects_checkpoint_claims() {
    let executable = env!("CARGO_BIN_EXE_gridedge_algorithm_probe");
    assert!(ProcessAlgorithm::connect(
        executable,
        vec!["--claim-checkpoint".to_owned()],
        Duration::from_secs(1),
    )
    .is_err());

    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let first = ProcessAlgorithm::connect(
        executable,
        vec!["--variant=a".to_owned()],
        Duration::from_secs(3),
    )
    .unwrap();
    let store = SqliteStore::open(&config.database).unwrap();
    let service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        store,
        Box::new(first),
        Some("algorithm-identity".into()),
    )
    .unwrap();
    drop(service);

    let replacement = ProcessAlgorithm::connect(
        executable,
        vec!["--variant=b".to_owned()],
        Duration::from_secs(3),
    )
    .unwrap();
    let store = SqliteStore::open(&config.database).unwrap();
    assert!(GridAutomationService::recover_with_algorithm(
        config,
        store,
        Box::new(replacement),
        "algorithm-identity".into(),
    )
    .is_err());
}

#[cfg(unix)]
#[test]
fn process_algorithm_executes_an_immutable_private_artifact_after_source_replacement() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().unwrap();
    let source = temp.path().join("algorithm-source");
    std::fs::copy(env!("CARGO_BIN_EXE_gridedge_algorithm_probe"), &source).unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o700)).unwrap();
    let algorithm = ProcessAlgorithm::connect(&source, Vec::new(), Duration::from_secs(3)).unwrap();

    std::fs::write(&source, b"replaced-after-registration").unwrap();
    let mut config = config_in(&temp);
    config.gate.timeout_ms = 4_000;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config,
        store,
        Box::new(algorithm),
        Some("immutable-algorithm-artifact".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.orders.len(), 1);
}

#[test]
fn process_algorithm_deadline_covers_requests_larger_than_an_os_pipe() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(FixedGate {
            probability: Decimal::ZERO,
            alpha: Decimal::ZERO,
        }),
        Some("large-decision-request".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    let payload = service
        .store
        .first_payload_by_type("large-decision-request", EventType::GateDecisionMade)
        .unwrap()
        .unwrap();
    let mut request: DecisionRequest = serde_json::from_value(payload["request"].clone()).unwrap();
    request.right.capacity.tranche_ids = (0..8_000)
        .map(|index| format!("large-request-{index:05}-{}", "x".repeat(80)))
        .collect();
    let algorithm = ProcessAlgorithm::connect(
        env!("CARGO_BIN_EXE_gridedge_algorithm_probe"),
        vec!["--ignore-stdin".into()],
        Duration::from_secs(2),
    )
    .unwrap();
    let started = std::time::Instant::now();
    assert!(algorithm.decide(&request).is_err());
    assert!(started.elapsed() < Duration::from_secs(4));
}

#[test]
fn process_algorithm_timeout_output_flood_and_descendants_are_bounded() {
    fn run_mode(arguments: Vec<String>, run_id: &str) -> (StrategyState, Duration) {
        let temp = TempDir::new().unwrap();
        let mut config = config_in(&temp);
        config.gate.timeout_ms = 2_000;
        let algorithm = ProcessAlgorithm::connect(
            env!("CARGO_BIN_EXE_gridedge_algorithm_probe"),
            arguments,
            Duration::from_secs(2),
        )
        .unwrap();
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new_with_algorithm(
            config,
            store,
            Box::new(algorithm),
            Some(run_id.into()),
        )
        .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
            .unwrap();
        let started = std::time::Instant::now();
        service
            .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
            .unwrap();
        (service.state, started.elapsed())
    }

    let (timeout_state, timeout_elapsed) = run_mode(vec!["--hang".into()], "process-hang");
    assert_eq!(timeout_state.mode, ServiceMode::Safe);
    assert!(timeout_elapsed < Duration::from_secs(4));

    let (flood_state, flood_elapsed) = run_mode(vec!["--flood".into()], "process-flood");
    assert_eq!(flood_state.mode, ServiceMode::Safe);
    assert!(flood_elapsed < Duration::from_secs(4));

    let sentinel_dir = TempDir::new().unwrap();
    let sentinel = sentinel_dir.path().join("descendant-survived");
    let (tree_state, tree_elapsed) = run_mode(
        vec![format!("--spawn-descendant={}", sentinel.display())],
        "process-tree",
    );
    assert_eq!(tree_state.mode, ServiceMode::Safe);
    assert!(tree_elapsed < Duration::from_secs(4));
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !sentinel.exists(),
        "algorithm descendant escaped process group"
    );

    let escaped_sentinel = sentinel_dir.path().join("setsid-descendant-survived");
    let (escaped_state, escaped_elapsed) = run_mode(
        vec![format!("--escape-session={}", escaped_sentinel.display())],
        "process-setsid-tree",
    );
    assert_eq!(escaped_state.mode, ServiceMode::Safe);
    assert!(escaped_elapsed < Duration::from_secs(4));
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !escaped_sentinel.exists(),
        "algorithm descendant escaped the containment boundary with setsid"
    );

    let (busy_state, busy_elapsed) = run_mode(vec!["--busy".into()], "process-busy");
    assert_eq!(busy_state.mode, ServiceMode::Safe);
    assert!(busy_elapsed < Duration::from_secs(4));

    #[cfg(target_os = "linux")]
    {
        let (memory_state, memory_elapsed) =
            run_mode(vec!["--over-memory".into()], "process-memory");
        assert_eq!(memory_state.mode, ServiceMode::Safe);
        assert!(memory_elapsed < Duration::from_secs(4));
    }
}

#[test]
fn continuous_and_recover_every_bar_have_identical_business_results() {
    fn execute(
        temp: &TempDir,
        recover_each_bar: bool,
    ) -> (StrategyState, Vec<(EventType, String)>) {
        let mut config = config_in(temp);
        config.gate.kind = "simple_rule".into();
        config.paper.reject_probability_bps = 2_000;
        config.paper.partial_fill_bps = 5_000;
        let feed = CsvReplayFeed::load("tests/fixtures/sample.csv", &config.symbol).unwrap();
        let bars = feed.bars()[..15].to_vec();
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new(
            config.clone(),
            store,
            gridedge_t::gate::from_config(&config).unwrap(),
            Some("restart-determinism".into()),
        )
        .unwrap();
        for (position, market) in bars.iter().enumerate() {
            if recover_each_bar && position > 0 {
                drop(service);
                let store = SqliteStore::open(&config.database).unwrap();
                service = GridAutomationService::recover(
                    config.clone(),
                    store,
                    gridedge_t::gate::from_config(&config).unwrap(),
                    "restart-determinism".into(),
                )
                .unwrap();
            }
            service.on_bar(market).unwrap_or_else(|error| {
                panic!("bar {position} recover={recover_each_bar} failed: {error:#}")
            });
        }
        let events = service
            .store
            .load_after("restart-determinism", 0)
            .unwrap()
            .into_iter()
            .filter(|event| {
                !matches!(
                    event.event_type,
                    EventType::RunStarted
                        | EventType::ConfigSnapshotted
                        | EventType::RecoveryCompleted
                )
            })
            .map(|event| (event.event_type, event.payload.to_string()))
            .collect();
        (service.state, events)
    }

    let continuous_temp = TempDir::new().unwrap();
    let recovered_temp = TempDir::new().unwrap();
    let (continuous, continuous_events) = execute(&continuous_temp, false);
    let (recovered, recovered_events) = execute(&recovered_temp, true);
    if continuous_events != recovered_events {
        let first = continuous_events
            .iter()
            .zip(&recovered_events)
            .position(|(left, right)| left != right)
            .unwrap_or(continuous_events.len().min(recovered_events.len()));
        panic!(
            "first business event mismatch at {first}: continuous={:?}, recovered={:?}",
            continuous_events.get(first),
            recovered_events.get(first)
        );
    }
    assert_eq!(continuous.cash, recovered.cash);
    assert_eq!(continuous.position, recovered.position);
    assert_eq!(
        serde_json::to_value(continuous.orders).unwrap(),
        serde_json::to_value(recovered.orders).unwrap()
    );
    assert_eq!(
        serde_json::to_value(continuous.lots).unwrap(),
        serde_json::to_value(recovered.lots).unwrap()
    );
    assert_eq!(
        serde_json::to_value(continuous.grid_rights).unwrap(),
        serde_json::to_value(recovered.grid_rights).unwrap()
    );
}

#[test]
fn every_durable_bar_workflow_crash_point_recovers_to_the_same_result() {
    fn run(
        temp: &TempDir,
        fault: Option<WorkflowFaultPoint>,
    ) -> (StrategyState, Vec<(EventType, String)>) {
        let mut config = config_in(temp);
        config.paper.partial_fill_bps = 10_000;
        config.paper.reject_probability_bps = 0;
        let first = bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10");
        let trigger = bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82");
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new(
            config.clone(),
            store,
            Box::new(gridedge_t::gate::AlwaysExecuteGate),
            Some("fault-matrix".into()),
        )
        .unwrap();
        service.on_bar(&first).unwrap();
        if let Some(point) = fault {
            service.inject_fault_once(point);
            assert!(
                service.on_bar(&trigger).is_err(),
                "fault {point:?} did not fire"
            );
            drop(service);
            let store = SqliteStore::open(&config.database).unwrap();
            service = GridAutomationService::recover(
                config,
                store,
                Box::new(gridedge_t::gate::AlwaysExecuteGate),
                "fault-matrix".into(),
            )
            .unwrap();
            service.on_bar(&trigger).unwrap();
        } else {
            service.on_bar(&trigger).unwrap();
        }
        let events = service
            .store
            .load_after("fault-matrix", 0)
            .unwrap()
            .into_iter()
            .filter(|event| {
                !matches!(
                    event.event_type,
                    EventType::RunStarted
                        | EventType::ConfigSnapshotted
                        | EventType::RecoveryCompleted
                )
            })
            .map(|event| (event.event_type, event.payload.to_string()))
            .collect();
        (service.state, events)
    }

    let baseline_temp = TempDir::new().unwrap();
    let (baseline, baseline_events) = run(&baseline_temp, None);
    for point in [
        WorkflowFaultPoint::MarketReceived,
        WorkflowFaultPoint::IntentCommitted,
        WorkflowFaultPoint::OrderSubmitted,
        WorkflowFaultPoint::BrokerSideEffect,
        WorkflowFaultPoint::OrderAccepted,
        WorkflowFaultPoint::FillApplied,
        WorkflowFaultPoint::DecisionsCommitted,
        WorkflowFaultPoint::BarProcessed,
    ] {
        let temp = TempDir::new().unwrap();
        let (recovered, recovered_events) = run(&temp, Some(point));
        assert_eq!(
            recovered_events, baseline_events,
            "event mismatch at {point:?}"
        );
        assert_eq!(recovered.cash, baseline.cash, "cash mismatch at {point:?}");
        assert_eq!(
            recovered.position, baseline.position,
            "position mismatch at {point:?}"
        );
        assert_eq!(
            serde_json::to_value(recovered.orders).unwrap(),
            serde_json::to_value(&baseline.orders).unwrap(),
            "orders mismatch at {point:?}"
        );
        assert_eq!(
            serde_json::to_value(recovered.grid_rights).unwrap(),
            serde_json::to_value(&baseline.grid_rights).unwrap(),
            "rights mismatch at {point:?}"
        );
    }
}

#[test]
fn sell_final_fill_crash_recovers_cycle_reset_and_rearm_atomically() {
    fn run(temp: &TempDir, crash_after_fill: bool) -> StrategyState {
        let config = config_in(temp);
        let store = SqliteStore::open(&config.database).unwrap();
        let mut service = GridAutomationService::start_new(
            config.clone(),
            store,
            Box::new(gridedge_t::gate::AlwaysExecuteGate),
            Some("sell-reset-fault".into()),
        )
        .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
            .unwrap();
        service
            .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
            .unwrap();
        let sell = bar("2026-01-06 09:30:00", "9.82", "10.21", "9.81", "10.15");
        if crash_after_fill {
            service.inject_fault_once(WorkflowFaultPoint::FillApplied);
            assert!(service.on_bar(&sell).is_err());
            drop(service);
            let store = SqliteStore::open(&config.database).unwrap();
            service = GridAutomationService::recover(
                config,
                store,
                Box::new(gridedge_t::gate::AlwaysExecuteGate),
                "sell-reset-fault".into(),
            )
            .unwrap();
            service.on_bar(&sell).unwrap();
        } else {
            service.on_bar(&sell).unwrap();
        }
        service.state
    }

    let baseline_temp = TempDir::new().unwrap();
    let recovered_temp = TempDir::new().unwrap();
    let baseline = run(&baseline_temp, false);
    let recovered = run(&recovered_temp, true);
    assert_eq!(recovered.cycle_id, baseline.cycle_id);
    assert_eq!(recovered.levels.len(), baseline.levels.len());
    assert!(!recovered.levels.is_empty());
    assert_eq!(
        serde_json::to_value(recovered.right_tranches).unwrap(),
        serde_json::to_value(baseline.right_tranches).unwrap()
    );
    assert_eq!(
        serde_json::to_value(recovered.grid_rights).unwrap(),
        serde_json::to_value(baseline.grid_rights).unwrap()
    );
}

#[test]
fn partial_fills_charge_the_minimum_commission_once_per_order() {
    let temp = TempDir::new().unwrap();
    let mut config = config_in(&temp);
    config.paper.partial_fill_bps = 10_000;
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("commission-per-order".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    let order = service.state.orders.values().next().unwrap();
    let expected = (order.filled_notional * config.commission_rate)
        .max(config.minimum_commission)
        .round_dp(2);
    assert_eq!(order.commission_charged, expected);
    assert_eq!(service.state.cash.total_fees, expected);
}

#[test]
fn recovery_rebuilds_from_the_log_when_all_snapshots_are_corrupt_and_enters_read_only_once() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("snapshot-fallback".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service.save_snapshot().unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.98", "9.99"))
        .unwrap();
    service.save_snapshot().unwrap();
    drop(service);

    let connection = rusqlite::Connection::open(&config.database).unwrap();
    connection
        .execute(
            "UPDATE snapshots SET checksum='corrupt' WHERE run_id='snapshot-fallback'",
            [],
        )
        .unwrap();
    drop(connection);

    let store = SqliteStore::open(&config.database).unwrap();
    let recovered = GridAutomationService::recover(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "snapshot-fallback".into(),
    )
    .unwrap();
    assert_eq!(recovered.state.last_price, Some(d("9.99")));
    assert_eq!(recovered.state.mode, ServiceMode::ReadOnly);
    let expected_business = json!({
        "cash": &recovered.state.cash,
        "position": &recovered.state.position,
        "lots": &recovered.state.lots,
        "rights": &recovered.state.grid_rights,
        "tranches": &recovered.state.right_tranches,
        "orders": &recovered.state.orders,
        "last_price": recovered.state.last_price,
    });
    let first_recovery_count = recovered
        .store
        .load_after("snapshot-fallback", 0)
        .unwrap()
        .iter()
        .filter(|event| event.event_type == EventType::RecoveryCompleted)
        .count();
    drop(recovered);

    let store = SqliteStore::open(&config.database).unwrap();
    let recovered_again = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "snapshot-fallback".into(),
    )
    .unwrap();
    assert_eq!(recovered_again.state.mode, ServiceMode::ReadOnly);
    assert_eq!(recovered_again.state.last_price, Some(d("9.99")));
    let events = recovered_again
        .store
        .load_after("snapshot-fallback", 0)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == EventType::ServiceModeChanged
                    && event.payload["mode"] == json!(ServiceMode::ReadOnly)
                    && event.payload["reason"] == "INVALID_NEWER_SNAPSHOT_FALLBACK"
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::RecoveryCompleted)
            .count(),
        first_recovery_count + 1
    );
    assert_eq!(
        json!({
            "cash": &recovered_again.state.cash,
            "position": &recovered_again.state.position,
            "lots": &recovered_again.state.lots,
            "rights": &recovered_again.state.grid_rights,
            "tranches": &recovered_again.state.right_tranches,
            "orders": &recovered_again.state.orders,
            "last_price": recovered_again.state.last_price,
        }),
        expected_business
    );
}

#[test]
fn checksum_valid_but_semantically_forged_snapshot_is_rejected_by_full_prefix_replay() {
    use sha2::{Digest, Sha256};

    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("semantic-snapshot-forgery".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    service.save_snapshot().unwrap();
    let expected_cash = service.state.cash.clone();
    drop(service);

    let connection = rusqlite::Connection::open(&config.database).unwrap();
    let (sequence, state_json): (i64, String) = connection
        .query_row(
            "SELECT sequence_number,state_json FROM snapshots WHERE run_id=?1 ORDER BY sequence_number DESC LIMIT 1",
            ["semantic-snapshot-forgery"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut forged: StrategyState = serde_json::from_str(&state_json).unwrap();
    forged.cash.available += d("123.45");
    let forged_json = serde_json::to_string(&forged).unwrap();
    let forged_checksum = hex::encode(Sha256::digest(forged_json.as_bytes()));
    connection
        .execute(
            "UPDATE snapshots SET state_json=?1,checksum=?2 WHERE run_id=?3 AND sequence_number=?4",
            rusqlite::params![
                forged_json,
                forged_checksum,
                "semantic-snapshot-forgery",
                sequence
            ],
        )
        .unwrap();
    drop(connection);

    let store = SqliteStore::open(&config.database).unwrap();
    let recovered = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "semantic-snapshot-forgery".into(),
    )
    .unwrap();
    assert_eq!(recovered.state.cash, expected_cash);
    assert_eq!(recovered.state.mode, ServiceMode::ReadOnly);
}

#[test]
fn v1_golden_event_replays_and_unknown_future_schema_fails_closed() {
    let raw = include_str!("fixtures/v1_market_event.json");
    let event: EventEnvelope = serde_json::from_str(raw).unwrap();
    let mut state = StrategyState::new(
        "v1-golden".into(),
        "cycle-v1".into(),
        "600000.SH".into(),
        d("10"),
        d("1000"),
        0,
        0,
    );
    state.apply_event(&event).unwrap();
    assert_eq!(state.last_price, Some(d("10")));
    let before = serde_json::to_value(&state).unwrap();
    let mut future = event;
    future.schema_version = 99;
    assert!(state.apply_event(&future).is_err());
    assert_eq!(serde_json::to_value(state).unwrap(), before);
}

#[test]
fn schema_v2_buy_without_embedded_fee_policy_remains_replayable() {
    let config = Config::load("configs/default.yaml").unwrap();
    let mut state = StrategyState::new(
        "v2-buy".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("10000"),
        0,
        0,
    );
    let intent = OrderIntent {
        intent_id: "v2-buy-intent".into(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: String::new(),
        grid_index: -1,
        direction: Direction::Buy,
        limit_price: d("10"),
        quantity: 100,
        budget: d("1000"),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
        profit_guard_version: String::new(),
        profit_guard_policy: None,
        created_at: dt("2026-01-05 10:00:00"),
    };
    let order = Order {
        order_id: "v2-buy-order".into(),
        intent,
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: d("1005"),
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    let apply_v2 =
        |state: &mut StrategyState, kind: EventType, key: &str, payload: serde_json::Value| {
            let mut event = EventEnvelope::new(
                kind,
                "v2-buy",
                "cycle",
                &config.symbol,
                dt("2026-01-05 10:00:00"),
                "v2-buy",
                None,
                key.into(),
                payload,
                "v2",
            );
            if matches!(
                kind,
                EventType::OrderIntentCreated
                    | EventType::OrderPartiallyFilled
                    | EventType::OrderFilled
            ) {
                event.schema_version = 2;
            }
            state.apply_event(&event).unwrap();
        };
    apply_v2(
        &mut state,
        EventType::OrderIntentCreated,
        "v2-buy-created",
        serde_json::to_value(order).unwrap(),
    );
    apply_v2(
        &mut state,
        EventType::OrderSubmitted,
        "v2-buy-submitted",
        json!({"order_id": "v2-buy-order"}),
    );
    apply_v2(
        &mut state,
        EventType::OrderAccepted,
        "v2-buy-accepted",
        json!({"order_id": "v2-buy-order"}),
    );
    apply_v2(
        &mut state,
        EventType::OrderFilled,
        "v2-buy-filled",
        json!({
            "grid_index": -1,
            "fill": Fill {
                fill_id: "v2-buy-fill".into(),
                order_id: "v2-buy-order".into(),
                direction: Direction::Buy,
                price: d("10"),
                quantity: 100,
                commission: d("5"),
                tax: Decimal::ZERO,
                trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
                target_lot_ids: vec![],
                target_lot_allocations: vec![],
            }
        }),
    );
    assert_eq!(state.lots["lot:v2-buy-fill"].remaining_cost, d("1005"));
    assert_eq!(state.orders["v2-buy-order"].status, OrderStatus::Filled);
}

#[test]
fn legacy_v2_fractional_decision_rebuilds_deterministically_but_cannot_be_newly_written() {
    let mut request = quantity_decision_request(3_000, 1_500, 100);
    request.contract_version = 2;
    request.context.available_quantity = 3_000;
    request.context.gross_available_quantity = 0;
    request.context.platform_residual_quantity = 0;
    request.context.algorithm_authorized_quantity = 0;
    let legacy_hash = legacy_v2_context_hash(&request.context).unwrap();
    assert_eq!(
        legacy_hash,
        "8856f1bc29ac21c3aa6575ce9acd1d0b68df49d3d051a878c05bd9309d42d76a"
    );
    let response = DecisionResponse {
        contract_version: 2,
        request_id: request.request_id.clone(),
        right_id: request.right.right_id.clone(),
        outcome: RightDecision::Exercise {
            exercise_quantity: 1_200,
            defer_quantity: 1_800,
        },
        decision: GateDecision {
            probability: d("0.4"),
            alpha: d("0.4"),
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["LEGACY_V2_GOLDEN".into()],
            model_name: "legacy-v2".into(),
            model_version: "2".into(),
            input_snapshot_hash: legacy_hash,
            decided_at: request.context.current_time,
        },
    };
    response.validate_for(&request).unwrap();

    let mut base = StrategyState::new(
        "quantity-contract".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    base.audited_standard_quantity = 1_500;
    base.audited_lot_size = 100;
    base.grid_rights
        .insert(request.right.right_id.clone(), request.right.clone());
    let mut decision_event = EventEnvelope::new(
        EventType::GateDecisionMade,
        &base.run_id,
        &base.cycle_id,
        &base.symbol,
        request.context.current_time,
        "legacy-v2",
        None,
        "legacy-v2-decision".into(),
        json!({
            "request": request,
            "response": response,
            "algorithm_succeeded": true
        }),
        "v2",
    );
    decision_event.schema_version = 2;
    let mut deferred = EventEnvelope::new(
        EventType::GridRightDeferred,
        &base.run_id,
        &base.cycle_id,
        &base.symbol,
        decision_event.event_time,
        "legacy-v2",
        None,
        "legacy-v2-deferred".into(),
        json!({
            "right_id": request.right.right_id,
            "decision_id": request.request_id,
            "remaining_quantity": 1800
        }),
        "v2",
    );
    deferred.schema_version = 2;

    let mut left = base.clone();
    let mut right = base.clone();
    for state in [&mut left, &mut right] {
        state.apply_event(&decision_event).unwrap();
        state.apply_event(&deferred).unwrap();
    }
    assert_eq!(
        serde_json::to_value(&left).unwrap(),
        serde_json::to_value(&right).unwrap()
    );
    let replayed = &left.grid_rights[&decision_event.payload["response"]["right_id"]
        .as_str()
        .unwrap()
        .to_owned()];
    assert_eq!(replayed.decision_contract_version, 2);
    assert_eq!(replayed.decided_exercise_quantity, 1_200);
    assert_eq!(replayed.decided_defer_quantity, 1_800);

    for legacy_event in [decision_event.clone(), deferred.clone()] {
        let temp = TempDir::new().unwrap();
        let mut store = SqliteStore::open(temp.path().join("legacy-write.db")).unwrap();
        store.migrate().unwrap();
        let mut state = base.clone();
        let before = serde_json::to_value(&state).unwrap();
        let mut sequence = 0;
        assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(legacy_event)
            .is_err());
        assert_eq!(sequence, 0);
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
    }

    let mut forbidden_residual = deferred;
    forbidden_residual.event_type = EventType::GridRightResidualHeld;
    forbidden_residual.idempotency_key = "legacy-v2-residual".into();
    assert!(base.apply_event(&forbidden_residual).is_err());
}

#[test]
fn v1_loss_making_sell_is_replayed_as_history_but_new_fills_remain_guarded() {
    let config = Config::load("configs/default.yaml").unwrap();
    let mut state = StrategyState::new(
        "legacy-loss".into(),
        "cycle".into(),
        config.symbol.clone(),
        d("10"),
        d("10000"),
        0,
        0,
    );
    state.current_trade_date = Some(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
    let apply = |state: &mut StrategyState,
                 kind: EventType,
                 key: &str,
                 time: NaiveDateTime,
                 payload: serde_json::Value| {
        let mut event = EventEnvelope::new(
            kind,
            "legacy-loss",
            "cycle",
            &config.symbol,
            time,
            "legacy",
            None,
            key.into(),
            payload,
            "v1",
        );
        event.schema_version = 1;
        state.apply_event(&event).unwrap();
    };

    let buy_intent = OrderIntent {
        intent_id: "legacy-buy-intent".into(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: String::new(),
        grid_index: -1,
        direction: Direction::Buy,
        limit_price: d("10"),
        quantity: 100,
        budget: d("1000"),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
        profit_guard_version: String::new(),
        profit_guard_policy: None,
        created_at: dt("2026-01-05 10:00:00"),
    };
    let buy_order = Order {
        order_id: "legacy-buy-order".into(),
        intent: buy_intent,
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: d("1005"),
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    apply(
        &mut state,
        EventType::OrderIntentCreated,
        "legacy-buy-create",
        dt("2026-01-05 10:00:00"),
        serde_json::to_value(&buy_order).unwrap(),
    );
    for (kind, key) in [
        (EventType::OrderSubmitted, "legacy-buy-submit"),
        (EventType::OrderAccepted, "legacy-buy-accept"),
    ] {
        apply(
            &mut state,
            kind,
            key,
            dt("2026-01-05 10:00:00"),
            json!({"order_id": "legacy-buy-order"}),
        );
    }
    apply(
        &mut state,
        EventType::OrderFilled,
        "legacy-buy-fill",
        dt("2026-01-05 10:00:00"),
        json!({
            "grid_index": -1,
            "fill": Fill {
                fill_id: "legacy-buy-fill".into(),
                order_id: "legacy-buy-order".into(),
                direction: Direction::Buy,
                price: d("10"),
                quantity: 100,
                commission: d("5"),
                tax: Decimal::ZERO,
                trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
                target_lot_ids: vec![],
                target_lot_allocations: vec![],
            }
        }),
    );
    state
        .roll_trading_day(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .unwrap();

    let sell_intent = OrderIntent {
        intent_id: "legacy-sell-intent".into(),
        origin: gridedge_t::domain::OrderIntentOrigin::GridRight,
        right_id: String::new(),
        grid_index: 1,
        direction: Direction::Sell,
        limit_price: d("10.05"),
        quantity: 100,
        budget: d("1005"),
        target_lot_ids: vec!["lot:legacy-buy-fill".into()],
        target_lot_allocations: vec![],
        profit_guard_version: String::new(),
        profit_guard_policy: None,
        created_at: dt("2026-01-06 10:00:00"),
    };
    let sell_order = Order {
        order_id: "legacy-sell-order".into(),
        intent: sell_intent,
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: Decimal::ZERO,
        reserved_quantity_remaining: 100,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    apply(
        &mut state,
        EventType::OrderIntentCreated,
        "legacy-sell-create",
        dt("2026-01-06 10:00:00"),
        serde_json::to_value(&sell_order).unwrap(),
    );
    for (kind, key) in [
        (EventType::OrderSubmitted, "legacy-sell-submit"),
        (EventType::OrderAccepted, "legacy-sell-accept"),
    ] {
        apply(
            &mut state,
            kind,
            key,
            dt("2026-01-06 10:00:00"),
            json!({"order_id": "legacy-sell-order"}),
        );
    }
    let legacy_loss = Fill {
        fill_id: "legacy-loss-fill".into(),
        order_id: "legacy-sell-order".into(),
        direction: Direction::Sell,
        price: d("10.05"),
        quantity: 100,
        commission: d("5"),
        tax: d("1.01"),
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        target_lot_ids: vec!["lot:legacy-buy-fill".into()],
        target_lot_allocations: vec![],
    };
    apply(
        &mut state,
        EventType::OrderFilled,
        "legacy-loss-fill",
        dt("2026-01-06 10:00:00"),
        json!({"grid_index": 1, "fill": legacy_loss}),
    );
    assert_eq!(state.realized_pnl, d("-6.01"));

    let before = serde_json::to_value(&state).unwrap();
    let mut guarded = state.clone();
    let result = guarded.apply_fill(
        &Fill {
            fill_id: "new-loss".into(),
            order_id: "new-loss-order".into(),
            direction: Direction::Sell,
            price: d("1"),
            quantity: 1,
            commission: Decimal::ZERO,
            tax: Decimal::ZERO,
            trade_date: NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
            target_lot_ids: vec![],
            target_lot_allocations: vec![],
        },
        1,
    );
    assert!(result.is_err());
    assert_eq!(serde_json::to_value(state).unwrap(), before);
}

#[test]
fn submitted_orders_reserve_resources_and_invalid_overfill_is_rejected_pre_ledger() {
    let config = Config::load("configs/default.yaml").unwrap();
    let mut state = StrategyState::new(
        "reservation".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        d("10000"),
        1000,
        1000,
    );
    let mut intent = sample_intent();
    intent.profit_guard_policy = Some(gridedge_t::domain::ProfitGuardPolicy::from(&config));
    state.audited_profit_guard_policy = intent.profit_guard_policy.clone();
    let order = Order {
        order_id: "reserved-order".into(),
        intent: intent.clone(),
        status: OrderStatus::Created,
        filled_quantity: 0,
        filled_notional: Decimal::ZERO,
        commission_charged: Decimal::ZERO,
        reserved_cash_remaining: d("1000"),
        reserved_quantity_remaining: 0,
        cancel_requested_reason: None,
        cancel_requested_at: None,
    };
    // This test isolates the execution reservation aggregate.  Schema 3 is a
    // historical intent shape; current schema 4 right-binding is covered by
    // the dedicated ledger-boundary test.
    let mut intent_event = event(
        "reservation",
        "intent",
        EventType::OrderIntentCreated,
        serde_json::to_value(order).unwrap(),
    );
    intent_event.schema_version = 3;
    state.apply_event(&intent_event).unwrap();
    state
        .apply_event(&event(
            "reservation",
            "submit",
            EventType::OrderSubmitted,
            json!({"order_id": "reserved-order"}),
        ))
        .unwrap();
    assert_eq!(state.cash.frozen, d("1000"));
    let risk = DefaultRiskChecker { config: &config };
    assert!(risk.check_buy(&state, 100, d("9500")).is_err());
    state
        .apply_event(&event(
            "reservation",
            "accept",
            EventType::OrderAccepted,
            json!({"order_id": "reserved-order"}),
        ))
        .unwrap();
    let overfill = Fill {
        fill_id: "overfill".into(),
        order_id: "reserved-order".into(),
        direction: Direction::Buy,
        price: intent.limit_price,
        quantity: intent.quantity + 1,
        commission: Decimal::ZERO,
        tax: Decimal::ZERO,
        trade_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        target_lot_ids: vec![],
        target_lot_allocations: vec![],
    };
    let before = serde_json::to_value(&state).unwrap();
    assert!(state
        .apply_event(&event(
            "reservation",
            "overfill",
            EventType::OrderFilled,
            json!({"fill": overfill, "grid_index": -1}),
        ))
        .is_err());
    assert_eq!(serde_json::to_value(&state).unwrap(), before);

    state
        .apply_event(&event(
            "reservation",
            "reject-after-accept",
            EventType::OrderRejected,
            json!({"order_id": "reserved-order"}),
        ))
        .unwrap();
    assert_eq!(state.cash.frozen, Decimal::ZERO);
}

#[test]
fn current_order_intent_is_exactly_bound_to_its_reserved_grid_right() {
    let source_temp = TempDir::new().unwrap();
    let config = config_in(&source_temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("intent-binding".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82",))
        .is_err());
    let valid_event = service
        .store
        .load_after("intent-binding", 0)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    assert_eq!(
        valid_event.schema_version,
        current_event_schema(EventType::OrderIntentCreated)
    );
    let valid_order: Order = serde_json::from_value(valid_event.payload.clone()).unwrap();
    let reserved_event = service
        .store
        .load_after("intent-binding", 0)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == EventType::GridRightReserved)
        .unwrap();
    let mut pre_intent = service.state.clone();
    pre_intent.orders.clear();
    pre_intent.validate_invariants().unwrap();
    let before = serde_json::to_value(&pre_intent).unwrap();

    for (schema, reservation, should_accept) in [
        (5, d("14725"), true),
        (5, d("14720"), false),
        (6, d("14720"), true),
        (6, d("14725"), false),
    ] {
        let mut candidate = pre_intent.clone();
        let mut versioned = valid_event.clone();
        versioned.schema_version = schema;
        let mut versioned_order = valid_order.clone();
        versioned_order.reserved_cash_remaining = reservation;
        versioned.payload = serde_json::to_value(versioned_order).unwrap();
        let before_versioned = serde_json::to_value(&candidate).unwrap();
        let result = candidate.apply_event(&versioned);
        assert_eq!(
            result.is_ok(),
            should_accept,
            "schema {schema} reservation {reservation} compatibility mismatch"
        );
        if !should_accept {
            assert_eq!(
                serde_json::to_value(candidate).unwrap(),
                before_versioned,
                "rejected schema {schema} reservation mutated the projection"
            );
        }
    }

    let attack_temp = TempDir::new().unwrap();
    let mut attack_store = SqliteStore::open(attack_temp.path().join("intent-attacks.db")).unwrap();
    attack_store.migrate().unwrap();
    let mut sequence = 0;
    let mut attacks = Vec::<(&str, Order)>::new();

    let mut missing_right = valid_order.clone();
    missing_right.intent.right_id = "missing-right".into();
    attacks.push(("missing-right", missing_right));

    let mut wrong_direction = valid_order.clone();
    wrong_direction.intent.direction = Direction::Sell;
    attacks.push(("wrong-direction", wrong_direction));

    let mut wrong_grid = valid_order.clone();
    wrong_grid.intent.grid_index -= 1;
    attacks.push(("wrong-grid", wrong_grid));

    let mut unaligned = valid_order.clone();
    unaligned.intent.quantity -= 1;
    attacks.push(("unaligned-quantity", unaligned));

    let mut prefilled = valid_order.clone();
    prefilled.status = OrderStatus::Submitted;
    prefilled.filled_quantity = 1;
    attacks.push(("prefilled-order", prefilled));

    let mut wrong_reservation = valid_order.clone();
    wrong_reservation.reserved_cash_remaining += Decimal::ONE;
    attacks.push(("wrong-reservation", wrong_reservation));

    let mut wrong_order_id = valid_order.clone();
    wrong_order_id.order_id = "forged-order-id".into();
    attacks.push(("wrong-order-id", wrong_order_id));

    for (name, attack) in attacks {
        let mut forged = valid_event.clone();
        forged.idempotency_key = format!("intent-attack:{name}");
        forged.event_id = format!("event-{name}");
        forged.payload = serde_json::to_value(attack).unwrap();
        assert!(
            LedgerWriter::new(&mut attack_store, &mut pre_intent, &mut sequence)
                .append(forged)
                .is_err()
        );
        assert_eq!(sequence, 0, "forged intent {name} entered the ledger");
        assert_eq!(
            serde_json::to_value(&pre_intent).unwrap(),
            before,
            "forged intent {name} mutated state"
        );
    }

    LedgerWriter::new(&mut attack_store, &mut pre_intent, &mut sequence)
        .append(valid_event.clone())
        .unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(pre_intent.orders.len(), 1);

    let mut pre_reserve = service.state.clone();
    pre_reserve.orders.clear();
    let right = pre_reserve
        .grid_rights
        .get_mut(&valid_order.intent.right_id)
        .unwrap();
    right.status = GridRightStatus::Granted;
    right.reserved_quantity = 0;
    for tranche in pre_reserve
        .right_tranches
        .values_mut()
        .filter(|tranche| tranche.owner_right_id == valid_order.intent.right_id)
    {
        tranche.available_quantity += tranche.reserved_quantity;
        tranche.reserved_quantity = 0;
    }
    pre_reserve.validate_invariants().unwrap();
    let before_unbacked = serde_json::to_value(&pre_reserve).unwrap();
    let unbacked_temp = TempDir::new().unwrap();
    let mut unbacked_store =
        SqliteStore::open(unbacked_temp.path().join("unbacked-intent.db")).unwrap();
    unbacked_store.migrate().unwrap();
    let mut unbacked_sequence = 0;
    let mut unbacked_batch = vec![reserved_event, valid_event];
    assert!(LedgerWriter::new(
        &mut unbacked_store,
        &mut pre_reserve,
        &mut unbacked_sequence
    )
    .append_batch(&mut unbacked_batch)
    .is_err());
    assert_eq!(unbacked_sequence, 0);
    assert_eq!(serde_json::to_value(&pre_reserve).unwrap(), before_unbacked);
}

#[test]
fn ledger_atomically_rejects_forged_current_quantity_partitions_and_fractional_intent() {
    let source_temp = TempDir::new().unwrap();
    let config = config_in(&source_temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("quantity-partition-source".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let before_sequence = service
        .store
        .load_after("quantity-partition-source", 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let before_state = service.state.clone();
    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .is_err());
    let canonical = service
        .store
        .load_after("quantity-partition-source", before_sequence)
        .unwrap();
    assert!(canonical.iter().any(|event| {
        event.event_type == EventType::GateDecisionMade
            && event.schema_version == current_event_schema(EventType::GateDecisionMade)
    }));
    assert!(canonical.iter().any(|event| {
        event.event_type == EventType::GridRightReserved
            && event.schema_version == current_event_schema(EventType::GridRightReserved)
    }));
    assert!(canonical.iter().any(|event| {
        event.event_type == EventType::OrderIntentCreated
            && event.schema_version == current_event_schema(EventType::OrderIntentCreated)
    }));

    let mutate_decision_context =
        |events: &mut [EventEnvelope], mutate: fn(&mut serde_json::Value)| {
            let event = events
                .iter_mut()
                .find(|event| event.event_type == EventType::GateDecisionMade)
                .unwrap();
            mutate(&mut event.payload["request"]["context"]);
            let context: GateContext =
                serde_json::from_value(event.payload["request"]["context"].clone()).unwrap();
            let contract_version = event.payload["request"]["contract_version"]
                .as_i64()
                .unwrap();
            event.payload["response"]["decision"]["input_snapshot_hash"] =
                json!(if contract_version
                    == i64::from(gridedge_t::decision::DECISION_CONTRACT_VERSION)
                {
                    context_hash_v4(&context).unwrap()
                } else {
                    context_hash(&context).unwrap()
                });
        };

    let mut attacks: Vec<(&str, Vec<EventEnvelope>)> = Vec::new();
    let mut fractional_decision = canonical.clone();
    let decision = fractional_decision
        .iter_mut()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    decision.payload["response"]["outcome"] = json!({
        "kind": "EXERCISE",
        "exercise_quantity": 700,
        "defer_quantity": 800
    });
    attacks.push(("fractional-E-D", fractional_decision));

    let mut forged_c = canonical.clone();
    mutate_decision_context(&mut forged_c, |context| {
        context["gross_available_quantity"] = json!(1_600);
    });
    attacks.push(("forged-C", forged_c));

    let mut forged_p = canonical.clone();
    mutate_decision_context(&mut forged_p, |context| {
        context["available_quantity"] = json!(1_400);
        context["algorithm_authorized_quantity"] = json!(1_400);
        context["platform_residual_quantity"] = json!(100);
    });
    attacks.push(("forged-P-A", forged_p));

    for (name, field, value) in [
        ("disposition-C", "gross_available_quantity", 1_600),
        ("disposition-P", "platform_residual_quantity", 100),
        ("disposition-A", "algorithm_authorized_quantity", 1_400),
    ] {
        let mut events = canonical.clone();
        let disposition = events
            .iter_mut()
            .find(|event| event.event_type == EventType::GridRightReserved)
            .unwrap();
        disposition.payload[field] = json!(value);
        attacks.push((name, events));
    }

    let mut fractional_disposition = canonical.clone();
    let disposition = fractional_disposition
        .iter_mut()
        .find(|event| event.event_type == EventType::GridRightReserved)
        .unwrap();
    disposition.payload["platform_blocked_quantity"] = json!(100);
    disposition.payload["order_intent_quantity"] = json!(1_400);
    disposition.payload["remaining_decision_quantity"] = json!(100);
    attacks.push(("fractional-B-I-R", fractional_disposition));

    let mut fractional_intent = canonical.clone();
    let intent = fractional_intent
        .iter_mut()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .unwrap();
    intent.payload["intent"]["quantity"] = json!(1_400);
    intent.payload["reserved_quantity_remaining"] = json!(1_400);
    attacks.push(("fractional-order-intent", fractional_intent));

    let mut invented_platform_block = canonical.clone();
    invented_platform_block.retain(|event| {
        !matches!(
            event.event_type,
            EventType::RightBalanceReserved | EventType::OrderIntentCreated
        )
    });
    let decision = invented_platform_block
        .iter()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    let exercise = decision.payload["response"]["outcome"]["exercise_quantity"]
        .as_i64()
        .unwrap();
    let defer = decision.payload["response"]["outcome"]["defer_quantity"]
        .as_i64()
        .unwrap();
    let disposition = invented_platform_block
        .iter_mut()
        .find(|event| event.event_type == EventType::GridRightReserved)
        .unwrap();
    disposition.event_type = EventType::GridRightBlocked;
    disposition.idempotency_key = "forged-platform-block".into();
    disposition.payload["reason"] = json!("BUY_RISK_REJECTED:forged");
    disposition.payload["platform_blocked_quantity"] = json!(exercise);
    disposition.payload["order_intent_quantity"] = json!(0);
    disposition.payload["remaining_decision_quantity"] = json!(defer + exercise);
    disposition
        .payload
        .as_object_mut()
        .unwrap()
        .remove("reserved_quantity");
    attacks.push((
        "invented-whole-unit-platform-block",
        invented_platform_block,
    ));

    for (name, mut events) in attacks {
        let attack_temp = TempDir::new().unwrap();
        let mut store = SqliteStore::open(attack_temp.path().join("attack.db")).unwrap();
        store.migrate().unwrap();
        let mut state = before_state.clone();
        let before = serde_json::to_value(&state).unwrap();
        let mut sequence = 0;
        assert!(
            LedgerWriter::new(&mut store, &mut state, &mut sequence)
                .append_batch(&mut events)
                .is_err(),
            "forged current quantity partition {name} entered the ledger"
        );
        assert_eq!(sequence, 0, "forged batch {name} changed the head");
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            before,
            "forged batch {name} changed the projection"
        );
    }
}

#[test]
fn ledger_recomputes_resource_aware_market_cash_and_pending_buy_evidence_from_prefix() {
    let source_temp = TempDir::new().unwrap();
    let mut config = Config::load("configs/resource_aware.yaml").unwrap();
    config.database = source_temp
        .path()
        .join("resource-evidence-source.db")
        .display()
        .to_string();
    let run_id = "resource-evidence-source";
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        gridedge_t::decision::algorithm_from_config(&config).unwrap(),
        Some(run_id.into()),
    )
    .unwrap();
    let start = dt("2026-01-05 09:30:00");
    let closes = [
        "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10", "10",
        "10", "9.96", "9.92", "9.86", "9.81",
    ];
    let mut previous = d("10");
    for (offset, close) in closes.into_iter().enumerate() {
        let close = d(close);
        service
            .on_bar(&MarketBar {
                timestamp: start + chrono::Duration::minutes(offset as i64),
                symbol: config.symbol.clone(),
                open: previous,
                high: previous.max(close),
                low: previous.min(close),
                close,
                volume: 100_000,
                amount: None,
            })
            .unwrap();
        previous = close;
    }
    let before_state = service.state.clone();
    let before_sequence = service
        .store
        .load_after(run_id, 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let prefix_database = source_temp.path().join("resource-prefix.db");
    copy_sqlite_database(std::path::Path::new(&config.database), &prefix_database);

    service.inject_fault_once(WorkflowFaultPoint::IntentCommitted);
    assert!(service
        .on_bar(&MarketBar {
            timestamp: start + chrono::Duration::minutes(20),
            symbol: config.symbol.clone(),
            open: d("9.81"),
            high: d("9.82"),
            low: d("9.79"),
            close: d("9.82"),
            volume: 100_000,
            amount: None,
        })
        .is_err());
    let canonical = service.store.load_after(run_id, before_sequence).unwrap();
    let canonical_decision = canonical
        .iter()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    assert_eq!(canonical_decision.schema_version, 4);
    assert_eq!(canonical_decision.payload["request"]["contract_version"], 4);

    let accepted_database = source_temp.path().join("resource-canonical.db");
    copy_sqlite_database(&prefix_database, &accepted_database);
    assert_canonical_batch_is_accepted_from(&before_state, &canonical, &accepted_database);

    let mut forged_alpha = canonical.clone();
    let decision = forged_alpha
        .iter_mut()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    decision.payload["response"]["decision"]["alpha_numerator"] = json!(0);
    decision.payload["response"]["decision"]["alpha"] = json!("0");
    let alpha_attack_database = source_temp.path().join("resource-alpha-ratio.db");
    copy_sqlite_database(&prefix_database, &alpha_attack_database);
    assert_forged_batch_is_atomically_rejected_from(
        &before_state,
        forged_alpha,
        &alpha_attack_database,
    );

    let mut attacks = Vec::new();
    let mut forged_bars = canonical.clone();
    let decision = forged_bars
        .iter_mut()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    decision.payload["request"]["context"]["funds_inventory"]["feature_bar_ids"][0] =
        json!("forged-processed-bar");
    decision.payload["request"]["context"]["funds_inventory"]["feature_bars_sha256"] =
        json!("ab".repeat(32));
    attacks.push(("processed-market-prefix", forged_bars));

    let mut forged_cash = canonical.clone();
    let decision = forged_cash
        .iter_mut()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    let cash = decision.payload["request"]["context"]["funds_inventory"]["cash_available"]
        .as_str()
        .unwrap()
        .parse::<Decimal>()
        .unwrap()
        + d("1000");
    let spendable = decision.payload["request"]["context"]["funds_inventory"]["spendable_cash"]
        .as_str()
        .unwrap()
        .parse::<Decimal>()
        .unwrap()
        + d("1000");
    decision.payload["request"]["context"]["funds_inventory"]["cash_available"] =
        json!(cash.to_string());
    decision.payload["request"]["context"]["funds_inventory"]["spendable_cash"] =
        json!(spendable.to_string());
    attacks.push(("cash", forged_cash));

    let mut forged_pending = canonical.clone();
    let decision = forged_pending
        .iter_mut()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    let evidence = &mut decision.payload["request"]["context"]["funds_inventory"];
    evidence["pending_buy_quantity"] = json!(config.standard_quantity);
    evidence["position_exposure_quantity"] =
        json!(config.initial_position + config.standard_quantity);
    evidence["position_headroom_units"] = json!(12);
    evidence["target_headroom_units"] = json!(5);
    attacks.push(("pending-buy-exposure", forged_pending));

    for (name, mut forged) in attacks {
        let decision = forged
            .iter_mut()
            .find(|event| event.event_type == EventType::GateDecisionMade)
            .unwrap();
        let context: GateContext =
            serde_json::from_value(decision.payload["request"]["context"].clone()).unwrap();
        decision.payload["response"]["decision"]["input_snapshot_hash"] =
            json!(context_hash_v4(&context).unwrap());
        let request: DecisionRequest =
            serde_json::from_value(decision.payload["request"].clone()).unwrap();
        let response: DecisionResponse =
            serde_json::from_value(decision.payload["response"].clone()).unwrap();
        response
            .validate_for(&request)
            .unwrap_or_else(|error| panic!("{name} was not a self-consistent attack: {error:#}"));

        let attack_database = source_temp.path().join(format!("resource-{name}.db"));
        copy_sqlite_database(&prefix_database, &attack_database);
        assert_forged_batch_is_atomically_rejected_from(&before_state, forged, &attack_database);
    }
}

#[test]
fn current_grant_recomputes_sell_partitions_price_and_requires_its_touch() {
    let source_temp = TempDir::new().unwrap();
    let config = config_in(&source_temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("sell-grant-boundary".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    assert_eq!(service.state.lots.len(), 1);

    let pre_grant = service.state.clone();
    let grant_time = dt("2026-01-05 10:00:00");
    let right_id = GridRight::id_for(&pre_grant.run_id, &pre_grant.cycle_id, 1, grant_time);
    let canonical_capacity = RightsGateCoordinator::new(&config, &pre_grant)
        .capacity(1, Direction::Sell, grant_time.date(), &right_id)
        .unwrap();
    assert_eq!(canonical_capacity.eligible_quantity, 0);
    assert_eq!(
        canonical_capacity.t_plus_one_blocked_quantity,
        config.standard_quantity
    );
    let canonical_price = GridSpec::from(&config).price(1).unwrap();

    let run_candidate = |name: &str,
                         right: GridRight,
                         include_touch: bool,
                         include_skip: bool,
                         should_pass: bool| {
        let temp = TempDir::new().unwrap();
        let mut store = SqliteStore::open(temp.path().join(format!("{name}.db"))).unwrap();
        store.migrate().unwrap();
        let mut state = pre_grant.clone();
        let mut sequence = 0;
        let current_high = if name == "non-crossing-market" {
            "10.10"
        } else {
            "10.21"
        };
        for (key, market) in [
            (
                "market-before",
                bar("2026-01-05 09:59:00", "10", "10.01", "9.99", "10"),
            ),
            (
                "market-current",
                bar("2026-01-05 10:00:00", "10", current_high, "9.99", "10.05"),
            ),
        ] {
            let market_event = EventEnvelope::new(
                EventType::MarketDataReceived,
                &state.run_id,
                &state.cycle_id,
                &state.symbol,
                market.timestamp,
                key,
                None,
                format!("{key}:{name}"),
                serde_json::to_value(market).unwrap(),
                &config.config_version,
            );
            LedgerWriter::new(&mut store, &mut state, &mut sequence)
                .append(market_event)
                .unwrap();
        }
        let before = serde_json::to_value(&state).unwrap();
        let before_sequence = sequence;
        let correlation = format!("sell-grant:{name}");
        let mut events = Vec::new();
        let right_id = right.right_id.clone();
        let right_price = right.grid_price;
        let gross_available_quantity = right.capacity.eligible_quantity;
        let (algorithm_authorized_quantity, platform_residual_quantity) =
            whole_unit_partition(gross_available_quantity, config.standard_quantity).unwrap();
        let decision_event = typed_decision_event(
            &config,
            &state.run_id,
            &right,
            0,
            &correlation,
            &format!("decision:{name}"),
        );
        if include_touch {
            events.push(EventEnvelope::new(
                EventType::GridLevelTouched,
                &state.run_id,
                &state.cycle_id,
                &state.symbol,
                grant_time,
                &correlation,
                None,
                format!("touch:{name}"),
                json!({"grid_index": 1, "price": right_price.to_string()}),
                &config.config_version,
            ));
        }
        events.push(EventEnvelope::new(
            EventType::GridRightGranted,
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            grant_time,
            &correlation,
            None,
            format!("grant:{name}"),
            serde_json::to_value(right).unwrap(),
            &config.config_version,
        ));
        if include_skip {
            events.push(EventEnvelope::new(
                EventType::GridLevelSkipped,
                &state.run_id,
                &state.cycle_id,
                &state.symbol,
                grant_time,
                &correlation,
                None,
                format!("skip:{name}"),
                json!({"grid_index": 1, "reason": "FORGED_CONTRADICTION"}),
                &config.config_version,
            ));
        }
        for tranche in sell_tranches_to_mint(&state, &right_id, 1) {
            events.push(EventEnvelope::new(
                EventType::RightTrancheMinted,
                &state.run_id,
                &state.cycle_id,
                &state.symbol,
                grant_time,
                &correlation,
                None,
                format!("mint:{name}:{}", tranche.tranche_id),
                serde_json::to_value(tranche).unwrap(),
                &config.config_version,
            ));
        }
        if name != "missing-decision" {
            events.push(decision_event);
            events.push(EventEnvelope::new(
                EventType::GridRightBlocked,
                &state.run_id,
                &state.cycle_id,
                &state.symbol,
                grant_time,
                &correlation,
                None,
                format!("disposition:{name}"),
                json!({
                    "right_id": right_id,
                    "decision_id": format!("decision:{right_id}"),
                    "reason": "SELL_BLOCKED_T_PLUS_ONE",
                    "gross_available_quantity": gross_available_quantity,
                    "platform_residual_quantity": platform_residual_quantity,
                    "algorithm_authorized_quantity": algorithm_authorized_quantity,
                    "platform_blocked_quantity": 0,
                    "order_intent_quantity": 0,
                    "remaining_decision_quantity": algorithm_authorized_quantity,
                    "remaining_budget": "0",
                    "remaining_quantity": gross_available_quantity,
                }),
                &config.config_version,
            ));
        }
        let event_count = i64::try_from(events.len()).unwrap();
        let result =
            LedgerWriter::new(&mut store, &mut state, &mut sequence).append_batch(&mut events);
        if should_pass {
            result.unwrap();
            assert_eq!(sequence, before_sequence + event_count);
        } else {
            assert!(result.is_err());
            assert_eq!(
                sequence, before_sequence,
                "forged grant {name} entered the ledger"
            );
            assert_eq!(serde_json::to_value(&state).unwrap(), before);
        }
    };

    run_candidate(
        "canonical",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            canonical_capacity.clone(),
        ),
        true,
        false,
        true,
    );
    run_candidate(
        "contradictory-grant-and-skip",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            canonical_capacity.clone(),
        ),
        true,
        true,
        false,
    );
    run_candidate(
        "non-crossing-market",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            canonical_capacity.clone(),
        ),
        true,
        false,
        false,
    );
    run_candidate(
        "missing-decision",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            canonical_capacity.clone(),
        ),
        true,
        false,
        false,
    );

    let mut forged_partition = canonical_capacity.clone();
    forged_partition.eligible_quantity = config.standard_quantity;
    forged_partition.eligible_lot_ids = forged_partition.t_plus_one_blocked_lot_ids.clone();
    forged_partition.t_plus_one_blocked_quantity = 0;
    forged_partition.t_plus_one_blocked_lot_ids.clear();
    run_candidate(
        "t1-as-eligible",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            forged_partition,
        ),
        true,
        false,
        false,
    );

    let mut forged_quantity = canonical_capacity.clone();
    forged_quantity.eligible_quantity = config.standard_quantity * 2;
    forged_quantity.eligible_lot_ids = forged_quantity.t_plus_one_blocked_lot_ids.clone();
    forged_quantity.t_plus_one_blocked_quantity = 0;
    forged_quantity.t_plus_one_blocked_lot_ids.clear();
    run_candidate(
        "phantom-double-quantity",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            forged_quantity,
        ),
        true,
        false,
        false,
    );

    run_candidate(
        "wrong-grid-price",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price + Decimal::ONE,
            grant_time,
            canonical_capacity.clone(),
        ),
        true,
        false,
        false,
    );
    run_candidate(
        "missing-mechanical-touch",
        GridRight::granted(
            &pre_grant.run_id,
            &pre_grant.cycle_id,
            &pre_grant.symbol,
            Direction::Sell,
            1,
            canonical_price,
            grant_time,
            canonical_capacity,
        ),
        false,
        false,
        false,
    );
}

#[test]
fn current_market_facts_are_envelope_bound_monotonic_and_idempotent() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("market-boundary.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "market-boundary".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("100000"),
        0,
        0,
    );
    let audited_config = Config::load("configs/default.yaml").unwrap();
    state.audited_config = Some(audited_config.clone());
    state.audited_standard_quantity = audited_config.standard_quantity;
    state.audited_lot_size = audited_config.lot_size;
    let mut sequence = 0;
    let future = bar("2026-01-05 10:00:00", "10", "10.01", "9.99", "10");
    let future_event = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        future.timestamp,
        "market-future",
        None,
        "market-future".into(),
        serde_json::to_value(&future).unwrap(),
        "v1",
    );
    assert!(matches!(
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(future_event.clone())
            .unwrap(),
        AppendOutcome::Appended(1)
    ));
    assert!(matches!(
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(future_event)
            .unwrap(),
        AppendOutcome::Duplicate
    ));
    assert_eq!(sequence, 1);

    let before = serde_json::to_value(&state).unwrap();
    let past = bar("2026-01-05 09:59:00", "10", "10.01", "9.79", "9.80");
    let past_event = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        past.timestamp,
        "market-past",
        None,
        "market-past".into(),
        serde_json::to_value(past).unwrap(),
        "v1",
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(past_event)
        .is_err());

    let mismatched = bar("2026-01-05 10:01:00", "10", "10.01", "9.99", "10");
    let mismatched_event = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 10:02:00"),
        "market-mismatch",
        None,
        "market-mismatch".into(),
        serde_json::to_value(mismatched).unwrap(),
        "v1",
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(mismatched_event)
        .is_err());

    let invalid = bar("2026-01-05 10:02:00", "10", "9.99", "9.90", "10");
    let invalid_event = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        invalid.timestamp,
        "market-invalid",
        None,
        "market-invalid".into(),
        serde_json::to_value(invalid).unwrap(),
        "v1",
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(invalid_event)
        .is_err());
    assert_eq!(sequence, 1);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    assert_eq!(
        store
            .event_count_by_type("market-boundary", EventType::MarketDataReceived)
            .unwrap(),
        1
    );
}

#[test]
fn one_grid_opportunity_can_have_only_one_semantic_touch() {
    let temp = TempDir::new().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let mut store = SqliteStore::open(temp.path().join("touch-unique.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "touch-unique".into(),
        "cycle".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    state.audited_config = Some(config.clone());
    state.audited_standard_quantity = config.standard_quantity;
    state.audited_lot_size = config.lot_size;
    state.levels = GridSpec::from(&config).levels().unwrap();
    let mut sequence = 0;
    let mut forged_level = state.levels[&-1].clone();
    forged_level.price = d("9.90");
    let forged_arm = EventEnvelope::new(
        EventType::GridLevelArmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 09:30:00"),
        "forged-arm",
        None,
        "forged-arm".into(),
        serde_json::to_value(forged_level).unwrap(),
        &config.config_version,
    );
    let before_forged_arm = serde_json::to_value(&state).unwrap();
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(forged_arm)
        .is_err());
    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_forged_arm);
    for (key, market) in [
        (
            "unique-market-before",
            bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"),
        ),
        (
            "unique-market-current",
            bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.80"),
        ),
    ] {
        let event = EventEnvelope::new(
            EventType::MarketDataReceived,
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            market.timestamp,
            key,
            None,
            key.into(),
            serde_json::to_value(market).unwrap(),
            &config.config_version,
        );
        LedgerWriter::new(&mut store, &mut state, &mut sequence)
            .append(event)
            .unwrap();
    }

    let opportunity_time = dt("2026-01-05 09:31:00");
    let opportunity_run = state.run_id.clone();
    let opportunity_cycle = state.cycle_id.clone();
    let opportunity_symbol = state.symbol.clone();
    let make_resolution = |suffix: &str| {
        let correlation = format!("touch-resolution:{suffix}");
        vec![
            EventEnvelope::new(
                EventType::GridLevelTouched,
                &opportunity_run,
                &opportunity_cycle,
                &opportunity_symbol,
                opportunity_time,
                &correlation,
                None,
                format!("touch:{suffix}"),
                json!({"grid_index": -1, "price": "9.80"}),
                &config.config_version,
            ),
            EventEnvelope::new(
                EventType::GridLevelSkipped,
                &opportunity_run,
                &opportunity_cycle,
                &opportunity_symbol,
                opportunity_time,
                &correlation,
                None,
                format!("skip:{suffix}"),
                json!({"grid_index": -1, "reason": "TEST_SKIP"}),
                &config.config_version,
            ),
        ]
    };
    let mut first = make_resolution("first");
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut first)
        .unwrap();
    let before = serde_json::to_value(&state).unwrap();
    let before_sequence = sequence;

    let mut duplicate_opportunity = make_resolution("second-correlation");
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut duplicate_opportunity)
        .is_err());
    assert_eq!(sequence, before_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);

    let uncleared = bar("2026-01-05 09:32:00", "9.80", "9.82", "9.79", "9.81");
    let uncleared_event = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        uncleared.timestamp,
        "uncleared-market",
        None,
        "uncleared-market".into(),
        serde_json::to_value(&uncleared).unwrap(),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(uncleared_event)
        .unwrap();
    let before_rearm = serde_json::to_value(&state).unwrap();
    let before_rearm_sequence = sequence;
    let rearm = EventEnvelope::new(
        EventType::GridLevelRearmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 09:32:00"),
        "forged-rearm",
        None,
        "forged-rearm".into(),
        json!({"grid_index": -1}),
        &config.config_version,
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(rearm)
        .is_err());
    assert_eq!(sequence, before_rearm_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_rearm);

    let decisions = EventEnvelope::new(
        EventType::MarketBarDecisionsCommitted,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        uncleared.timestamp,
        "uncleared-market",
        None,
        "uncleared-decisions".into(),
        json!({"symbol": state.symbol, "timestamp": uncleared.timestamp}),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(decisions)
        .unwrap();
    let duplicate_decisions = EventEnvelope::new(
        EventType::MarketBarDecisionsCommitted,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        uncleared.timestamp,
        "uncleared-market",
        None,
        "uncleared-decisions-second-key".into(),
        json!({"symbol": state.symbol, "timestamp": uncleared.timestamp}),
        &config.config_version,
    );
    let before_duplicate_decisions = serde_json::to_value(&state).unwrap();
    let before_duplicate_decisions_sequence = sequence;
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(duplicate_decisions)
        .is_err());
    assert_eq!(sequence, before_duplicate_decisions_sequence);
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        before_duplicate_decisions
    );
    let no_hysteresis_rearm = EventEnvelope::new(
        EventType::GridLevelRearmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        uncleared.timestamp,
        "uncleared-market",
        None,
        "uncleared-rearm".into(),
        json!({"grid_index": -1}),
        &config.config_version,
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(no_hysteresis_rearm)
        .is_err());
    let processed = EventEnvelope::new(
        EventType::MarketBarProcessed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        uncleared.timestamp,
        "uncleared-market",
        None,
        "uncleared-processed".into(),
        json!({
            "symbol": state.symbol,
            "timestamp": uncleared.timestamp,
            "close": uncleared.close.to_string()
        }),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(processed)
        .unwrap();

    let clearing = bar("2026-01-05 09:33:00", "9.81", "9.86", "9.79", "9.85");
    let clearing_market = EventEnvelope::new(
        EventType::MarketDataReceived,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "clearing-market".into(),
        serde_json::to_value(&clearing).unwrap(),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(clearing_market)
        .unwrap();
    let clearing_decisions = EventEnvelope::new(
        EventType::MarketBarDecisionsCommitted,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "clearing-decisions".into(),
        json!({"symbol": state.symbol, "timestamp": clearing.timestamp}),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(clearing_decisions)
        .unwrap();
    let processed_without_rearm = EventEnvelope::new(
        EventType::MarketBarProcessed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "clearing-processed-without-rearm".into(),
        json!({
            "symbol": state.symbol,
            "timestamp": clearing.timestamp,
            "close": clearing.close.to_string()
        }),
        &config.config_version,
    );
    let before_omitted_rearm = serde_json::to_value(&state).unwrap();
    let before_omitted_rearm_sequence = sequence;
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(processed_without_rearm)
        .is_err());
    assert_eq!(sequence, before_omitted_rearm_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_omitted_rearm);
    let valid_rearm = EventEnvelope::new(
        EventType::GridLevelRearmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "clearing-rearm".into(),
        json!({"grid_index": -1}),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(valid_rearm)
        .unwrap();

    let retro_correlation = "retro-touch";
    let mut retro_touch = vec![
        EventEnvelope::new(
            EventType::GridLevelTouched,
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            clearing.timestamp,
            retro_correlation,
            None,
            "retro-touch".into(),
            json!({"grid_index": -1, "price": "9.80"}),
            &config.config_version,
        ),
        EventEnvelope::new(
            EventType::GridLevelSkipped,
            &state.run_id,
            &state.cycle_id,
            &state.symbol,
            clearing.timestamp,
            retro_correlation,
            None,
            "retro-skip".into(),
            json!({"grid_index": -1, "reason": "SAME_BAR_RETROACTIVE"}),
            &config.config_version,
        ),
    ];
    let before_retro = serde_json::to_value(&state).unwrap();
    let before_retro_sequence = sequence;
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut retro_touch)
        .is_err());
    assert_eq!(sequence, before_retro_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_retro);

    let clearing_processed = EventEnvelope::new(
        EventType::MarketBarProcessed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "clearing-processed".into(),
        json!({
            "symbol": state.symbol,
            "timestamp": clearing.timestamp,
            "close": clearing.close.to_string()
        }),
        &config.config_version,
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(clearing_processed)
        .unwrap();
    let rearm_after_processed = EventEnvelope::new(
        EventType::GridLevelRearmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        clearing.timestamp,
        "clearing-market",
        None,
        "late-rearm".into(),
        json!({"grid_index": -1}),
        &config.config_version,
    );
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(rearm_after_processed)
        .is_err());

    let before_forged_cycle = serde_json::to_value(&state).unwrap();
    let before_forged_cycle_sequence = sequence;
    let mut forged_cycle = make_resolution("forged-cycle");
    for event in &mut forged_cycle {
        event.cycle_id = "forged-cycle".into();
    }
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut forged_cycle)
        .is_err());
    assert_eq!(sequence, before_forged_cycle_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_forged_cycle);
    assert_eq!(
        store
            .event_count_by_type("touch-unique", EventType::GridLevelTouched)
            .unwrap(),
        1
    );
}

#[test]
fn current_cycles_require_complete_atomic_grids_and_real_completion_evidence() {
    let temp = TempDir::new().unwrap();
    let config = Config::load("configs/default.yaml").unwrap();
    let mut store = SqliteStore::open(temp.path().join("cycle-atomic.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "cycle-atomic".into(),
        "cycle-one".into(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    state.audited_config = Some(config.clone());
    state.audited_standard_quantity = config.standard_quantity;
    state.audited_lot_size = config.lot_size;
    let mut sequence = 0;
    let started_at = dt("2026-01-05 09:00:00");
    let cycle_started = EventEnvelope::new(
        EventType::GridCycleStarted,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        started_at,
        "cycle-start",
        None,
        "cycle-start".into(),
        json!({"anchor": config.anchor_price.to_string()}),
        &config.config_version,
    );
    let one_level = GridSpec::from(&config)
        .levels()
        .unwrap()
        .remove(&-1)
        .unwrap();
    let one_arm = EventEnvelope::new(
        EventType::GridLevelArmed,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        started_at,
        "cycle-start",
        None,
        "cycle-one-arm".into(),
        serde_json::to_value(one_level).unwrap(),
        &config.config_version,
    );
    let before = serde_json::to_value(&state).unwrap();
    let mut incomplete_start = vec![cycle_started, one_arm];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut incomplete_start)
        .is_err());
    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    assert!(store.load_after("cycle-atomic", 0).unwrap().is_empty());

    state.grid_cycle_active = true;
    state.levels = GridSpec::from(&config).levels().unwrap();
    let reset_at = dt("2026-01-05 15:00:00");
    let old_cycle = state.cycle_id.clone();
    let new_cycle = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("{}:cycle-reset:{reset_at}", state.run_id).as_bytes(),
    )
    .to_string();
    let reset = EventEnvelope::new(
        EventType::GridCycleReset,
        &state.run_id,
        &old_cycle,
        &state.symbol,
        reset_at,
        "cycle-reset",
        None,
        "cycle-reset".into(),
        json!({"cycle_id": new_cycle, "previous_cycle_id": old_cycle}),
        &config.config_version,
    );
    let mut unjustified_reset = vec![reset];
    for level in GridSpec::from(&config).levels().unwrap().into_values() {
        unjustified_reset.push(EventEnvelope::new(
            EventType::GridLevelArmed,
            &state.run_id,
            &new_cycle,
            &state.symbol,
            reset_at,
            "cycle-reset",
            None,
            format!("reset-arm:{}", level.index),
            serde_json::to_value(level).unwrap(),
            &config.config_version,
        ));
    }
    let before_reset = serde_json::to_value(&state).unwrap();
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut unjustified_reset)
        .is_err());
    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(&state).unwrap(), before_reset);
}

#[test]
fn cycle_reset_expires_residual_right_and_conserves_its_available_tranche() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("residual-cycle-reset".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:31:00", "10", "10.01", "9.79", "9.82"))
        .unwrap();
    service
        .on_bar(&bar("2026-01-06 09:30:00", "9.82", "9.83", "9.81", "9.82"))
        .unwrap();
    let old_cycle = service.state.cycle_id.clone();
    let completing_sell = bar("2026-01-06 09:31:00", "9.82", "10.21", "9.81", "10.18");
    service.inject_fault_once(WorkflowFaultPoint::DecisionsCommitted);
    assert!(service.on_bar(&completing_sell).is_err());
    assert!(service
        .state
        .lots
        .values()
        .all(|lot| lot.remaining_quantity == 0));
    assert!(service.state.orders.values().any(|order| {
        order.status == OrderStatus::Filled && order.intent.direction == Direction::Sell
    }));
    let residual_right_id = GridRight::id_for(
        &service.state.run_id,
        &old_cycle,
        -2,
        dt("2026-01-06 09:30:30"),
    );
    let mut tranche = RightTranche::minted_buy_quantity(
        &old_cycle,
        &residual_right_id,
        &residual_right_id,
        -2,
        config.standard_quantity,
    );
    tranche.available_quantity = 700;
    tranche.revoked_quantity = config.standard_quantity - 700;
    let tranche_id = tranche.tranche_id.clone();
    let mut residual = GridRight::granted(
        &service.state.run_id,
        &old_cycle,
        &service.state.symbol,
        Direction::Buy,
        -2,
        GridSpec::from(&config).price(-2).unwrap(),
        dt("2026-01-06 09:30:30"),
        GridRightCapacity {
            mechanical_budget_cap: Decimal::ZERO,
            deployed_budget: Decimal::ZERO,
            available_budget: Decimal::ZERO,
            mechanical_quantity_cap: config.standard_quantity,
            deployed_quantity: 0,
            available_quantity: 700,
            eligible_quantity: 0,
            eligible_lot_ids: Vec::new(),
            accumulated_grid_indices: vec![-2],
            t_plus_one_blocked_quantity: 0,
            t_plus_one_blocked_lot_ids: Vec::new(),
            risk_blocked_quantity: 0,
            risk_blocked_lot_ids: Vec::new(),
            no_profit_blocked_quantity: 0,
            no_profit_blocked_lot_ids: Vec::new(),
            source_right_ids: Vec::new(),
            carried_in_budget: Decimal::ZERO,
            carried_in_quantity: 0,
            tranche_ids: vec![tranche_id.clone()],
        },
    );
    residual.status = GridRightStatus::Residual;
    residual.decision_id = Some(format!("decision:{residual_right_id}"));
    residual.decision_contract_version = 3;
    residual.decision_gross_available_quantity = 700;
    residual.decision_platform_residual_quantity = 700;
    residual.decision_algorithm_authorized_quantity = 0;
    residual.decision_is_algorithm = true;
    residual.decision_recorded = true;

    service
        .state
        .grid_rights
        .insert(residual_right_id.clone(), residual);
    service
        .state
        .right_tranches
        .insert(tranche_id.clone(), tranche);
    service.state.validate_invariants().unwrap();

    service.on_bar(&completing_sell).unwrap();
    assert_ne!(service.state.cycle_id, old_cycle);
    assert_eq!(
        service.state.grid_rights[&residual_right_id].status,
        GridRightStatus::Expired
    );
    let expired = &service.state.right_tranches[&tranche_id];
    assert_eq!(expired.available_quantity, 0);
    assert_eq!(expired.expired_quantity, 700);
    assert_eq!(
        expired.minted_quantity,
        expired.available_quantity
            + expired.reserved_quantity
            + expired.consumed_quantity
            + expired.revoked_quantity
            + expired.expired_quantity
    );
    let events = service.store.load_after("residual-cycle-reset", 0).unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == EventType::GridRightExpired
            && event.payload["right_id"] == residual_right_id
    }));
    assert!(events.iter().any(|event| {
        event.event_type == EventType::RightBalanceExpired
            && event.payload["allocations"]
                .as_array()
                .is_some_and(|allocations| {
                    allocations.iter().any(|allocation| {
                        allocation["tranche_id"] == tranche_id && allocation["quantity"] == 700
                    })
                })
    }));
}

#[test]
fn duplicate_events_cannot_poison_a_new_events_causation_chain() {
    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("causation.db")).unwrap();
    store.migrate().unwrap();
    let mut state = StrategyState::new(
        "causation".into(),
        "cycle".into(),
        "600000.SH".into(),
        d("10"),
        d("100000"),
        0,
        0,
    );
    let mut sequence = 0;
    let original = EventEnvelope::new(
        EventType::RunStarted,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 09:00:00"),
        "real-correlation",
        None,
        "real-key".into(),
        json!({}),
        "v1",
    );
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append(original.clone())
        .unwrap();

    let mut forged_duplicate = original.clone();
    forged_duplicate.event_id = "forged-event-id".into();
    forged_duplicate.correlation_id = "forged-correlation".into();
    let forged_child = EventEnvelope::new(
        EventType::AlgorithmRegistered,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 09:00:01"),
        "forged-correlation",
        None,
        "forged-child".into(),
        json!({}),
        "v1",
    );
    let before = serde_json::to_value(&state).unwrap();
    let before_sequence = sequence;
    let mut poisoned = vec![forged_duplicate, forged_child];
    assert!(LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut poisoned)
        .is_err());
    assert_eq!(sequence, before_sequence);
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    assert_eq!(store.load_after("causation", 0).unwrap().len(), 1);

    let legitimate_child = EventEnvelope::new(
        EventType::AlgorithmRegistered,
        &state.run_id,
        &state.cycle_id,
        &state.symbol,
        dt("2026-01-05 09:00:01"),
        "real-correlation",
        None,
        "legitimate-child".into(),
        json!({}),
        "v1",
    );
    let mut legitimate = vec![original.clone(), legitimate_child];
    LedgerWriter::new(&mut store, &mut state, &mut sequence)
        .append_batch(&mut legitimate)
        .unwrap();
    assert_eq!(
        legitimate[1].causation_id.as_deref(),
        Some(original.event_id.as_str())
    );
    assert_eq!(store.load_after("causation", 0).unwrap().len(), 2);
}

#[test]
fn recovery_completes_a_bar_received_before_a_crash_exactly_once() {
    let temp = TempDir::new().unwrap();
    let config = config_in(&temp);
    let store = SqliteStore::open(&config.database).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        Some("bar-crash".into()),
    )
    .unwrap();
    service
        .on_bar(&bar("2026-01-05 09:30:00", "10", "10.01", "9.99", "10"))
        .unwrap();
    let crossing = bar("2026-01-05 09:31:00", "10", "10.01", "9.74", "9.75");
    let last = service
        .store
        .load_after("bar-crash", 0)
        .unwrap()
        .last()
        .unwrap()
        .sequence_number;
    let key = format!("market:{}:{}", crossing.symbol, crossing.timestamp);
    let received = EventEnvelope::new(
        EventType::MarketDataReceived,
        "bar-crash",
        &service.state.cycle_id,
        &crossing.symbol,
        crossing.timestamp,
        &key,
        None,
        key.clone(),
        serde_json::to_value(&crossing).unwrap(),
        &config.config_version,
    );
    let mut injected_sequence = last;
    LedgerWriter::new(
        &mut service.store,
        &mut service.state,
        &mut injected_sequence,
    )
    .append(received)
    .unwrap();
    drop(service);

    let store = SqliteStore::open(&config.database).unwrap();
    let mut recovered = GridAutomationService::recover(
        config,
        store,
        Box::new(gridedge_t::gate::AlwaysExecuteGate),
        "bar-crash".into(),
    )
    .unwrap();
    recovered.on_bar(&crossing).unwrap();
    recovered.on_bar(&crossing).unwrap();
    assert_eq!(recovered.state.orders.len(), 1);
    assert_eq!(
        recovered
            .store
            .event_count_by_type("bar-crash", EventType::MarketBarProcessed)
            .unwrap(),
        2
    );
}
