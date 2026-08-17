use chrono::{Duration, NaiveDateTime};
use gridedge_t::{
    config::Config,
    data::MarketBar,
    decision::{
        algorithm_from_config, AlgorithmManifest, DecisionRequest, DecisionResponse, GridRight,
        GridRightCapacity, QuantDecisionAlgorithm, ResourceAwareWholeQAlgorithm, RightDecision,
        DECISION_CONTRACT_VERSION, WHOLE_UNIT_DECISION_CONTRACT_VERSION,
    },
    domain::{Direction, StrategyState},
    event::{current_event_schema, EventEnvelope, EventType},
    gate::{context_hash, context_hash_v4, FundsInventoryContextV1, GateContext, GateDecision},
    journal::{EventReader, SqliteStore},
    ledger::LedgerWriter,
    service::GridAutomationService,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use tempfile::TempDir;

const Q: i64 = 1_500;

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn dt(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}

#[derive(Clone, Copy)]
struct MarketCase {
    grid_index: i32,
    trade_levels: i32,
    depth: Decimal,
    trend: Decimal,
    location: Decimal,
    score: Decimal,
    passed: bool,
    history_bars: u32,
}

fn flat_deep_market() -> MarketCase {
    MarketCase {
        grid_index: -5,
        trade_levels: 5,
        depth: Decimal::ONE,
        trend: Decimal::ZERO,
        location: Decimal::ZERO,
        score: d("0.35"),
        passed: false,
        history_bars: 20,
    }
}

fn below_threshold_market() -> MarketCase {
    MarketCase {
        grid_index: -2,
        trade_levels: 5,
        depth: d("0.4"),
        trend: Decimal::ONE,
        location: d("0.2"),
        score: d("0.59"),
        passed: false,
        history_bars: 20,
    }
}

fn threshold_market() -> MarketCase {
    MarketCase {
        grid_index: -2,
        trade_levels: 4,
        depth: d("0.5"),
        trend: Decimal::ONE,
        location: d("0.1"),
        score: d("0.60"),
        passed: true,
        history_bars: 20,
    }
}

fn strong_market() -> MarketCase {
    MarketCase {
        grid_index: -5,
        trade_levels: 5,
        depth: Decimal::ONE,
        trend: Decimal::ONE,
        location: Decimal::ONE,
        score: Decimal::ONE,
        passed: true,
        history_bars: 20,
    }
}

fn request_v4(mechanical_units: i64, resource_units: i64, market: MarketCase) -> DecisionRequest {
    let authorized_quantity = mechanical_units * Q;
    let offered_units = resource_units.min(mechanical_units);
    let offered_quantity = offered_units * Q;
    let blocked_units = mechanical_units - offered_units;
    let rho = Decimal::from(resource_units) / Decimal::from(resource_units + 4);
    let multiplier = d("0.50") + d("0.50") * rho;
    let pace = if market.passed {
        (market.score * multiplier).min(d("0.50"))
    } else {
        Decimal::ZERO
    };
    let target_units = if market.passed && offered_units > 0 {
        let paced = (Decimal::from(offered_units) * pace)
            .floor()
            .to_string()
            .parse::<i64>()
            .unwrap();
        offered_units.min(2).min(paced.max(1))
    } else {
        0
    };
    let capacity = GridRightCapacity {
        mechanical_budget_cap: Decimal::ZERO,
        deployed_budget: Decimal::ZERO,
        available_budget: Decimal::ZERO,
        mechanical_quantity_cap: authorized_quantity,
        deployed_quantity: 0,
        available_quantity: authorized_quantity,
        eligible_quantity: 0,
        eligible_lot_ids: Vec::new(),
        accumulated_grid_indices: vec![market.grid_index],
        t_plus_one_blocked_quantity: 0,
        t_plus_one_blocked_lot_ids: Vec::new(),
        risk_blocked_quantity: 0,
        risk_blocked_lot_ids: Vec::new(),
        no_profit_blocked_quantity: 0,
        no_profit_blocked_lot_ids: Vec::new(),
        source_right_ids: Vec::new(),
        carried_in_budget: Decimal::ZERO,
        carried_in_quantity: 0,
        tranche_ids: vec!["v4-buy-tranche".into()],
    };
    let granted_at = dt("2026-01-05 10:30:00");
    let right = GridRight::granted(
        "decision-v4",
        "cycle-v4",
        "600000.SH",
        Direction::Buy,
        market.grid_index,
        d("10"),
        granted_at,
        capacity,
    );
    let resource = FundsInventoryContextV1 {
        schema_version: 1,
        feature_head_sequence: i64::from(market.history_bars),
        feature_bar_ids: (0..market.history_bars)
            .map(|index| format!("processed-bar-{index}"))
            .collect(),
        feature_bars_sha256: "ab".repeat(32),
        history_bars: market.history_bars,
        short_window: 5,
        long_window: 20,
        trade_levels: market.trade_levels,
        weight_depth: d("0.35"),
        weight_trend: d("0.40"),
        weight_location: d("0.25"),
        market_threshold: d("0.60"),
        depth: market.depth,
        recent_return_5: -market.trend * d("0.10"),
        vwap_deviation_20: -market.location * d("0.10"),
        range_scale_20: d("0.10"),
        trend_strength: market.trend,
        location_strength: market.location,
        market_score: market.score,
        market_signal_passed: market.passed,
        cash_available: d("100000000"),
        cash_frozen: Decimal::ZERO,
        minimum_free_cash: Decimal::ZERO,
        spendable_cash: d("100000000"),
        adverse_buy_price: Some(d("10.01")),
        cash_affordable_units: resource_units,
        position_total: 0,
        pending_buy_quantity: 0,
        position_exposure_quantity: 0,
        position_sellable: 0,
        position_today_bought: 0,
        position_frozen_sell: 0,
        max_position: 1_000_000,
        min_base_position: 0,
        target_position: 1_000_000,
        position_headroom_units: resource_units,
        target_headroom_units: resource_units,
        sellable_inventory_units: 0,
        mechanical_authorized_units: mechanical_units,
        resource_units,
        predecision_blocked_units: blocked_units,
        resource_ratio: rho,
        resource_multiplier: multiplier,
        pace,
        target_units,
    };
    let context = GateContext {
        right_id: right.right_id.clone(),
        symbol: right.symbol.clone(),
        direction: right.direction,
        grid_index: right.grid_index,
        grid_price: right.grid_price,
        current_price: right.grid_price,
        current_time: right.granted_at,
        available_budget: Decimal::ZERO,
        available_quantity: offered_quantity,
        gross_available_quantity: authorized_quantity,
        platform_residual_quantity: 0,
        algorithm_authorized_quantity: authorized_quantity,
        lot_size: 100,
        standard_quantity: Q,
        eligible_lot_ids: Vec::new(),
        accumulated_grid_indices: vec![market.grid_index],
        deployed_budget: Decimal::ZERO,
        current_position: 0,
        sellable_position: 0,
        recent_return: Decimal::ZERO,
        vwap_deviation: Decimal::ZERO,
        volatility: Decimal::ZERO,
        market_regime: "AUDITED_V4".into(),
        cycle_id: "cycle-v4".into(),
        funds_inventory: Some(resource),
    };
    DecisionRequest::new_with_contract(right, context, DECISION_CONTRACT_VERSION)
}

fn quantities(outcome: &RightDecision) -> (i64, i64) {
    outcome.quantities()
}

struct DeclaredVersions(Vec<u32>);

impl QuantDecisionAlgorithm for DeclaredVersions {
    fn manifest(&self) -> AlgorithmManifest {
        let mut manifest = ResourceAwareWholeQAlgorithm.manifest();
        manifest.supported_contract_versions = self.0.clone();
        manifest
    }

    fn decide(&self, request: &DecisionRequest) -> anyhow::Result<DecisionResponse> {
        ResourceAwareWholeQAlgorithm.decide(request)
    }
}

struct FailingV4;

impl QuantDecisionAlgorithm for FailingV4 {
    fn manifest(&self) -> AlgorithmManifest {
        ResourceAwareWholeQAlgorithm.manifest()
    }

    fn decide(&self, _request: &DecisionRequest) -> anyhow::Result<DecisionResponse> {
        anyhow::bail!("intentional v4 failure")
    }
}

fn market_bar(timestamp: NaiveDateTime, open: Decimal, close: Decimal) -> MarketBar {
    MarketBar {
        timestamp,
        symbol: "600000.SH".into(),
        open,
        high: open.max(close),
        low: open.min(close),
        close,
        volume: 100_000,
        amount: None,
    }
}

#[test]
fn decision_v4_is_a_distinct_resource_aware_algorithm_identity() {
    assert_eq!(DECISION_CONTRACT_VERSION, 4);
    assert_eq!(WHOLE_UNIT_DECISION_CONTRACT_VERSION, 3);
    let manifest = ResourceAwareWholeQAlgorithm.manifest();
    assert_eq!(manifest.algorithm_name, "resource-aware-whole-q");
    assert_eq!(manifest.algorithm_version, "1");
    assert_eq!(manifest.supported_contract_versions, vec![4]);
    assert!(manifest.deterministic);
    assert!(!manifest.supports_checkpoint);
}

#[test]
fn algorithm_contract_versions_are_canonical_and_bound_to_the_configured_strategy() {
    for (name, versions) in [
        ("future", vec![4, 99]),
        ("duplicate", vec![4, 4]),
        ("unsorted", vec![4, 3]),
    ] {
        let temp = TempDir::new().unwrap();
        let mut config = Config::load("configs/resource_aware.yaml").unwrap();
        config.database = temp.path().join(format!("{name}.db")).display().to_string();
        let result = GridAutomationService::start_new_with_algorithm(
            config.clone(),
            SqliteStore::open(&config.database).unwrap(),
            Box::new(DeclaredVersions(versions)),
            Some(format!("invalid-{name}")),
        );
        assert!(result.is_err());
        let store = SqliteStore::open(&config.database).unwrap();
        assert!(store
            .load_after(&format!("invalid-{name}"), 0)
            .unwrap()
            .is_empty());
    }

    let temp = TempDir::new().unwrap();
    let mut resource = Config::load("configs/resource_aware.yaml").unwrap();
    resource.database = temp.path().join("resource-v3.db").display().to_string();
    assert!(GridAutomationService::start_new_with_algorithm(
        resource.clone(),
        SqliteStore::open(&resource.database).unwrap(),
        Box::new(DeclaredVersions(vec![3])),
        Some("resource-v3".into()),
    )
    .is_err());

    let mut default = Config::load("configs/default.yaml").unwrap();
    default.database = temp.path().join("default-v4.db").display().to_string();
    assert!(GridAutomationService::start_new_with_algorithm(
        default.clone(),
        SqliteStore::open(&default.database).unwrap(),
        Box::new(DeclaredVersions(vec![4])),
        Some("default-v4".into()),
    )
    .is_err());

    resource.database = temp.path().join("resource-v3-v4.db").display().to_string();
    let service = GridAutomationService::start_new_with_algorithm(
        resource.clone(),
        SqliteStore::open(&resource.database).unwrap(),
        Box::new(DeclaredVersions(vec![3, 4])),
        Some("resource-v3-v4".into()),
    )
    .unwrap();
    let manifest = service
        .store
        .first_payload_by_type("resource-v3-v4", EventType::AlgorithmRegistered)
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest["supported_contract_versions"],
        serde_json::json!([3, 4])
    );
}

#[test]
fn money_and_inventory_never_raise_a_failing_market_score() {
    for market in [flat_deep_market(), below_threshold_market()] {
        let request = request_v4(10, 100, market);
        request.validate().unwrap();
        let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
        assert_eq!(quantities(&response.outcome), (0, 10 * Q));
        assert_eq!(response.decision.probability, market.score);
        assert_eq!(response.decision.alpha, Decimal::ZERO);
        assert_eq!(response.decision.action, "SKIP");
        assert!(response
            .decision
            .reason_codes
            .iter()
            .any(|reason| reason.contains("MARKET")));
        response.validate_for(&request).unwrap();
    }
}

#[test]
fn fewer_than_twenty_processed_bars_is_a_hard_warmup_defer() {
    let mut market = strong_market();
    market.history_bars = 19;
    market.passed = false;
    let request = request_v4(4, 100, market);
    let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
    assert_eq!(quantities(&response.outcome), (0, 4 * Q));
    assert!(response
        .decision
        .reason_codes
        .iter()
        .any(|reason| reason.contains("WARMUP")));
}

#[test]
fn one_unit_is_exercised_only_after_market_and_resource_both_pass() {
    let request = request_v4(1, 1, threshold_market());
    let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
    assert_eq!(quantities(&response.outcome), (Q, 0));
    assert_eq!(response.decision.action, "EXECUTE");
    response.validate_for(&request).unwrap();

    let blocked = request_v4(1, 0, threshold_market());
    let resource = blocked.context.funds_inventory.as_ref().unwrap();
    assert_eq!(resource.predecision_blocked_units, 1);
    assert_eq!(blocked.context.available_quantity, 0);
    let response = ResourceAwareWholeQAlgorithm.decide(&blocked).unwrap();
    assert_eq!(quantities(&response.outcome), (0, 0));
    response.validate_for(&blocked).unwrap();
}

#[test]
fn pending_buy_quantity_consumes_target_and_max_position_headroom() {
    let mut request = request_v4(1, 0, threshold_market());
    let resource = request.context.funds_inventory.as_mut().unwrap();
    resource.pending_buy_quantity = Q;
    resource.position_exposure_quantity = Q;
    resource.max_position = Q;
    resource.target_position = Q;
    resource.position_headroom_units = 0;
    resource.target_headroom_units = 0;
    request.validate().unwrap();
    let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
    assert_eq!(quantities(&response.outcome), (0, 0));
    response.validate_for(&request).unwrap();

    let mut forged = request;
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .position_exposure_quantity = 0;
    assert!(forged.validate().is_err());
    assert!(ResourceAwareWholeQAlgorithm.decide(&forged).is_err());
}

#[test]
fn pace_never_exercises_more_than_half_or_two_units_per_opportunity() {
    for (offered_units, expected_exercise_units) in [(2, 1), (4, 2), (20, 2)] {
        let request = request_v4(offered_units, 100, strong_market());
        let resource = request.context.funds_inventory.as_ref().unwrap();
        assert_eq!(resource.pace, d("0.50"));
        assert_eq!(resource.target_units, expected_exercise_units);
        let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
        assert_eq!(
            quantities(&response.outcome),
            (
                expected_exercise_units * Q,
                (offered_units - expected_exercise_units) * Q,
            )
        );
        response.validate_for(&request).unwrap();
    }
}

#[test]
fn v4_records_rational_alpha_without_using_decimal_to_recover_quantity() {
    let request = request_v4(3, 100, strong_market());
    let response = ResourceAwareWholeQAlgorithm.decide(&request).unwrap();
    assert_eq!(quantities(&response.outcome), (Q, 2 * Q));
    let decision = serde_json::to_value(&response.decision).unwrap();
    assert_eq!(decision["alpha_numerator"], 1);
    assert_eq!(decision["alpha_denominator"], 3);
    response.validate_for(&request).unwrap();

    let mut forged = response;
    forged.decision.alpha_numerator = Some(2);
    forged.decision.alpha = d("2").checked_div(d("3")).unwrap();
    assert!(forged.validate_for(&request).is_err());

    let mut forged_target = forged;
    forged_target.outcome = RightDecision::Exercise {
        exercise_quantity: 2 * Q,
        defer_quantity: Q,
    };
    assert!(forged_target.validate_for(&request).is_err());
}

#[test]
fn resource_aware_service_uses_only_twenty_processed_prefix_bars_and_writes_schema4() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::load("configs/resource_aware.yaml").unwrap();
    config.database = temp.path().join("resource-v4.db").display().to_string();
    let store = SqliteStore::open(&config.database).unwrap();
    let algorithm = algorithm_from_config(&config).unwrap();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        store,
        algorithm,
        Some("resource-v4-service".into()),
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
            .on_bar(&market_bar(
                start + Duration::minutes(offset as i64),
                previous,
                close,
            ))
            .unwrap();
        previous = close;
    }
    assert!(service.state.grid_rights.is_empty());

    let opportunity = MarketBar {
        timestamp: start + Duration::minutes(20),
        symbol: config.symbol.clone(),
        open: d("9.81"),
        high: d("9.82"),
        low: d("9.79"),
        close: d("9.82"),
        volume: 100_000,
        amount: None,
    };
    service.on_bar(&opportunity).unwrap();

    let events = service.store.load_after("resource-v4-service", 0).unwrap();
    let event = events
        .iter()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .expect("the first crossed grid must have a durable v4 decision");
    assert_eq!(
        event.schema_version,
        current_event_schema(EventType::GateDecisionMade)
    );
    assert_eq!(event.schema_version, 4);

    let request: DecisionRequest =
        serde_json::from_value(event.payload["request"].clone()).unwrap();
    let response: DecisionResponse =
        serde_json::from_value(event.payload["response"].clone()).unwrap();
    assert_eq!(request.contract_version, DECISION_CONTRACT_VERSION);
    response.validate_for(&request).unwrap();
    let resource = request
        .context
        .funds_inventory
        .as_ref()
        .expect("v4 decision must retain its resource evidence");
    assert_eq!(resource.history_bars, 20);
    assert_eq!(resource.feature_bar_ids.len(), 20);
    assert_eq!(resource.feature_bars_sha256.len(), 64);
    assert!(resource.feature_head_sequence < event.sequence_number);
    assert!(resource.market_signal_passed);
    assert_eq!(resource.mechanical_authorized_units, 1);
    assert_eq!(resource.predecision_blocked_units, 0);
    assert_eq!(resource.target_units, 1);
    assert_eq!(quantities(&response.outcome), (Q, 0));
    assert_eq!(response.decision.alpha_numerator, Some(1));
    assert_eq!(response.decision.alpha_denominator, Some(1));
    assert_eq!(
        response.decision.input_snapshot_hash,
        context_hash_v4(&request.context).unwrap()
    );
}

#[test]
fn resource_aware_algorithm_failure_is_audited_and_replays_as_canonical_fail_closed() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::load("configs/resource_aware.yaml").unwrap();
    config.database = temp
        .path()
        .join("resource-v4-failure.db")
        .display()
        .to_string();
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database).unwrap(),
        Box::new(FailingV4),
        Some("resource-v4-failure".into()),
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
            .on_bar(&market_bar(
                start + Duration::minutes(offset as i64),
                previous,
                close,
            ))
            .unwrap();
        previous = close;
    }
    service
        .on_bar(&MarketBar {
            timestamp: start + Duration::minutes(20),
            symbol: config.symbol.clone(),
            open: d("9.81"),
            high: d("9.82"),
            low: d("9.79"),
            close: d("9.82"),
            volume: 100_000,
            amount: None,
        })
        .unwrap();

    assert_eq!(service.state.mode, gridedge_t::domain::ServiceMode::Safe);
    assert!(service.state.orders.is_empty());
    let events = service.store.load_after("resource-v4-failure", 0).unwrap();
    let decision = events
        .iter()
        .find(|event| event.event_type == EventType::GateDecisionMade)
        .unwrap();
    assert_eq!(decision.payload["algorithm_succeeded"], false);
    let request: DecisionRequest =
        serde_json::from_value(decision.payload["request"].clone()).unwrap();
    let response: DecisionResponse =
        serde_json::from_value(decision.payload["response"].clone()).unwrap();
    assert_eq!(quantities(&response.outcome), (0, Q));
    response
        .validate_for_algorithm_status(&request, false)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::ErrorRecorded)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::MarketBarProcessed)
            .count(),
        21
    );

    let initial = StrategyState::new(
        "resource-v4-failure".into(),
        String::new(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let (rebuilt, sequence) = service.store.rebuild_full(initial).unwrap();
    assert_eq!(sequence, events.last().unwrap().sequence_number);
    assert_eq!(rebuilt.mode, gridedge_t::domain::ServiceMode::Safe);
    assert!(rebuilt.orders.is_empty());
}

#[test]
fn schema3_whole_unit_decision_remains_replay_only_after_schema4_release() {
    let mut request = request_v4(2, 2, strong_market());
    request.contract_version = WHOLE_UNIT_DECISION_CONTRACT_VERSION;
    request.context.funds_inventory = None;
    let response = DecisionResponse {
        contract_version: WHOLE_UNIT_DECISION_CONTRACT_VERSION,
        request_id: request.request_id.clone(),
        right_id: request.right.right_id.clone(),
        outcome: RightDecision::Exercise {
            exercise_quantity: Q,
            defer_quantity: Q,
        },
        decision: GateDecision {
            probability: d("0.5"),
            alpha: d("0.5"),
            alpha_numerator: None,
            alpha_denominator: None,
            action: "EXECUTE".into(),
            reason_codes: vec!["WHOLE_UNIT_V3_REPLAY".into()],
            model_name: "whole-unit-v3".into(),
            model_version: "3".into(),
            input_snapshot_hash: context_hash(&request.context).unwrap(),
            decided_at: request.context.current_time,
        },
    };
    response.validate_for(&request).unwrap();

    let mut event = EventEnvelope::new(
        EventType::GateDecisionMade,
        "decision-v4",
        "cycle-v4",
        "600000.SH",
        request.context.current_time,
        "schema3-replay",
        None,
        "schema3-decision".into(),
        serde_json::json!({
            "request": request.clone(),
            "response": response,
            "algorithm_succeeded": true
        }),
        "v3",
    );
    event.schema_version = 3;

    let mut replayed = StrategyState::new(
        "decision-v4".into(),
        "cycle-v4".into(),
        "600000.SH".into(),
        d("10"),
        d("100000"),
        0,
        0,
    );
    replayed.audited_standard_quantity = Q;
    replayed.audited_lot_size = 100;
    replayed
        .grid_rights
        .insert(request.right.right_id.clone(), request.right.clone());
    replayed.apply_event(&event).unwrap();
    let right = replayed.grid_rights.get(&request.right.right_id).unwrap();
    assert_eq!(right.decision_contract_version, 3);
    assert_eq!(right.decided_exercise_quantity, Q);
    assert_eq!(right.decided_defer_quantity, Q);

    let temp = TempDir::new().unwrap();
    let mut store = SqliteStore::open(temp.path().join("no-new-schema3.db")).unwrap();
    store.migrate().unwrap();
    let mut new_write_state = StrategyState::new(
        "decision-v4".into(),
        "cycle-v4".into(),
        "600000.SH".into(),
        d("10"),
        d("100000"),
        0,
        0,
    );
    new_write_state.audited_standard_quantity = Q;
    new_write_state.audited_lot_size = 100;
    new_write_state
        .grid_rights
        .insert(request.right.right_id.clone(), request.right.clone());
    let before = serde_json::to_value(&new_write_state).unwrap();
    let mut sequence = 0;
    assert!(
        LedgerWriter::new(&mut store, &mut new_write_state, &mut sequence)
            .append(event)
            .is_err()
    );
    assert_eq!(sequence, 0);
    assert_eq!(serde_json::to_value(new_write_state).unwrap(), before);
}

#[test]
fn v4_rejects_forged_market_feature_and_resource_pace_evidence() {
    fn rejected(request: DecisionRequest, label: &str) {
        assert!(
            ResourceAwareWholeQAlgorithm.decide(&request).is_err(),
            "forged v4 evidence was accepted: {label}"
        );
    }

    let canonical = request_v4(1, 1, threshold_market());
    ResourceAwareWholeQAlgorithm
        .decide(&canonical)
        .unwrap()
        .validate_for(&canonical)
        .unwrap();

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .weight_depth = d("0.34");
    rejected(forged, "weight_depth");

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .market_score = d("0.61");
    rejected(forged, "weighted market score");

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .feature_bar_ids
        .pop();
    rejected(forged, "processed feature-bar count");

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .feature_bars_sha256 = "not-a-sha256".into();
    rejected(forged, "processed feature-bar hash");

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .resource_ratio = Decimal::ZERO;
    rejected(forged, "resource ratio");

    let mut forged = canonical.clone();
    forged
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .resource_multiplier = Decimal::ONE;
    rejected(forged, "resource multiplier");

    let mut forged = canonical;
    forged.context.funds_inventory.as_mut().unwrap().pace = d("0.49");
    rejected(forged, "pace");
}

fn legacy_v3_context() -> GateContext {
    GateContext {
        right_id: "right-v3-golden".into(),
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
        standard_quantity: Q,
        eligible_lot_ids: vec![],
        accumulated_grid_indices: vec![-1, -2, -3],
        deployed_budget: Decimal::ZERO,
        current_position: 10_000,
        sellable_position: 10_000,
        recent_return: d("-0.02"),
        vwap_deviation: d("-0.01"),
        volatility: d("0.03"),
        market_regime: "DOWN".into(),
        cycle_id: "cycle".into(),
        funds_inventory: None,
    }
}

#[test]
fn decision_v3_hash_and_read_contract_are_unchanged_by_v4() {
    let context = legacy_v3_context();
    assert_eq!(
        context_hash(&context).unwrap(),
        "c64a36f6a4807163f0dbb2e9108259da87e15147a40f501ecb1c65d7adb403c1"
    );
    assert!(context_hash_v4(&context).is_err());

    let mut v3 = request_v4(3, 3, strong_market());
    v3.contract_version = WHOLE_UNIT_DECISION_CONTRACT_VERSION;
    v3.context.available_quantity = 3 * Q;
    v3.context.funds_inventory = None;
    v3.validate().unwrap();

    v3.context.funds_inventory = request_v4(3, 3, strong_market()).context.funds_inventory;
    assert!(v3.validate().is_err());
    assert_eq!(
        context_hash(&context).unwrap(),
        context_hash(&legacy_v3_context()).unwrap()
    );
}

#[test]
fn decision_v4_requires_resource_evidence_and_binds_it_into_the_hash() {
    let canonical = request_v4(4, 2, strong_market());
    canonical.validate().unwrap();
    let canonical_hash = context_hash_v4(&canonical.context).unwrap();

    let mut missing = canonical.clone();
    missing.context.funds_inventory = None;
    assert!(missing.validate().is_err());
    assert!(context_hash_v4(&missing.context).is_err());

    let mut changed_cash = canonical.clone();
    changed_cash
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .spendable_cash += d("0.01");
    assert_ne!(
        context_hash_v4(&changed_cash.context).unwrap(),
        canonical_hash
    );

    let mut changed_market = canonical;
    changed_market
        .context
        .funds_inventory
        .as_mut()
        .unwrap()
        .feature_bars_sha256 = "cd".repeat(32);
    assert_ne!(
        context_hash_v4(&changed_market.context).unwrap(),
        canonical_hash
    );
}
