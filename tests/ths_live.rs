use anyhow::Result;
use chrono::NaiveDateTime;
use gridedge_t::{
    data::MarketBar,
    decision::algorithm_from_config,
    domain::{
        Direction, InitialDeploymentEvaluation, InitialDeploymentOutcome, Order, OrderIntent,
        OrderIntentOrigin, OrderStatus,
    },
    event::{current_event_schema, market_evidence_sha256, EventEnvelope, EventType},
    journal::{EventReader, SqliteStore},
    rights::RightsGateCoordinator,
    risk::estimated_buy_reservation,
    service::GridAutomationService,
    ths_sim::{
        RemoteFilledEvidence, SimulationFillRecord, SimulationMarketQuote, SimulationOrderRecord,
    },
    ths_sim_outbox::{stage_from_source, StagedTerminalResolution, ThsSimOutbox},
    Config,
};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tempfile::tempdir;

const SOURCE: &str = "0123456789abcdef0123456789abcdef";

#[allow(dead_code, clippy::items_after_test_module)]
mod live_subject {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/bin/gridedge_ths_live.rs"
    ));

    pub(super) fn classify_untracked(
        records: &[SimulationOrderRecord],
        known: &std::collections::BTreeSet<String>,
    ) -> Vec<String> {
        untracked_open_contracts(records, known)
    }

    pub(super) fn validate_persisted_freshness(
        quotes: &[SimulationMarketQuote],
        maximum_unchanged_seconds: i64,
    ) -> anyhow::Result<()> {
        let mut freshness = PriceFreshness::default();
        for quote in quotes {
            freshness.observe(quote, chrono::Duration::seconds(maximum_unchanged_seconds))?;
        }
        Ok(())
    }

    pub(super) fn load_completed_bars(
        path: &std::path::Path,
        expected_symbol: &str,
    ) -> anyhow::Result<usize> {
        Ok(load_unique_bars(path, expected_symbol)?.len())
    }

    pub(super) fn audit_known_contracts(
        records: &[SimulationOrderRecord],
        outbox_path: &std::path::Path,
    ) -> anyhow::Result<Vec<String>> {
        let intents = ThsSimOutbox::open(outbox_path)?.staged_intents()?;
        remote_contract_violations_with_mode(records, &intents, false)
    }

    pub(super) fn terminal_permit_available(
        records: &[SimulationOrderRecord],
        outbox_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let intents = ThsSimOutbox::open(outbox_path)?.staged_intents()?;
        let modeled_fills = intents
            .iter()
            .map(|intent| {
                (
                    intent.order_id.clone(),
                    ModeledFill {
                        direction: intent.order.direction,
                        quantity: intent.order.quantity,
                        notional: intent.order.limit_price
                            * rust_decimal::Decimal::from(intent.order.quantity),
                    },
                )
            })
            .collect();
        remote_terminal_permit_for_modeled(records, &intents, &modeled_fills).map(|_| ())
    }

    pub(super) fn modeled_fill_from_source(
        source_database: &std::path::Path,
        run_id: &str,
        order_id: &str,
    ) -> anyhow::Result<(Direction, i64, rust_decimal::Decimal)> {
        let fills = load_modeled_fills(source_database, run_id)?;
        let fill = fills
            .get(order_id)
            .ok_or_else(|| anyhow::anyhow!("modeled fill not found"))?;
        let average = fill
            .notional
            .checked_div(rust_decimal::Decimal::from(fill.quantity))
            .ok_or_else(|| anyhow::anyhow!("modeled average is not representable"))?;
        Ok((fill.direction, fill.quantity, average))
    }

    pub(super) fn terminal_permit_from_source(
        records: &[SimulationOrderRecord],
        source_database: &std::path::Path,
        run_id: &str,
        outbox_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let intents = ThsSimOutbox::open(outbox_path)?.staged_intents()?;
        let modeled_fills = load_modeled_fills(source_database, run_id)?;
        remote_terminal_permit_for_modeled(records, &intents, &modeled_fills).map(|_| ())
    }

    pub(super) fn settlement_windows(now: chrono::NaiveDateTime) -> (bool, bool, bool) {
        (
            is_market_session(now),
            is_lunch_settlement_window(now),
            is_after_daily_window(now),
        )
    }

    pub(super) fn watermark_is_current(
        covered_through_us: Option<u64>,
        now: chrono::NaiveDateTime,
        maximum_age_seconds: i64,
    ) -> anyhow::Result<bool> {
        market_watermark_is_current(covered_through_us, now, maximum_age_seconds)
    }

    pub(super) fn bar_is_current(
        bar_timestamp: chrono::NaiveDateTime,
        now: chrono::NaiveDateTime,
        maximum_age_seconds: i64,
    ) -> anyhow::Result<bool> {
        market_bar_is_current(bar_timestamp, now, maximum_age_seconds)
    }

    pub(super) fn watermarks_are_contiguous(
        previous_covered_through_us: Option<u64>,
        covered_through_us: u64,
        maximum_gap_seconds: i64,
    ) -> anyhow::Result<bool> {
        market_watermarks_are_contiguous(
            previous_covered_through_us,
            covered_through_us,
            maximum_gap_seconds,
        )
    }

    pub(super) fn recovery_decision(
        mode: gridedge_t::domain::ServiceMode,
        completion_is_live: bool,
        completion_is_current: bool,
        previous_completion_is_current: bool,
        session_matches_today: bool,
        released_bar_count: usize,
        released_bars_are_current: bool,
    ) -> (bool, bool) {
        let enter_recovery = completion_requires_recovery(
            mode,
            completion_is_live,
            completion_is_current,
            previous_completion_is_current,
            session_matches_today,
            released_bar_count,
            released_bars_are_current,
        );
        let mode_after_precheck = if enter_recovery {
            gridedge_t::domain::ServiceMode::ReadOnly
        } else {
            mode
        };
        (
            enter_recovery,
            completion_allows_resume(
                mode_after_precheck,
                completion_is_live,
                completion_is_current,
                previous_completion_is_current,
                session_matches_today,
                released_bar_count,
                released_bars_are_current,
            ),
        )
    }

    pub(super) fn source_observation_recovery_decision(
        mode: gridedge_t::domain::ServiceMode,
        observation_is_current: bool,
        previous_observation_is_contiguous: bool,
        session_matches_today: bool,
    ) -> (bool, bool) {
        (
            source_observation_requires_recovery(
                mode,
                observation_is_current,
                previous_observation_is_contiguous,
                session_matches_today,
            ),
            source_observation_allows_resume(
                mode,
                observation_is_current,
                previous_observation_is_contiguous,
                session_matches_today,
            ),
        )
    }

    pub(super) fn source_observation_timeout_decision(
        mode: gridedge_t::domain::ServiceMode,
        observed_at_us: Option<u64>,
        now: chrono::NaiveDateTime,
        maximum_age_seconds: i64,
    ) -> anyhow::Result<bool> {
        source_observation_timeout_requires_recovery(mode, observed_at_us, now, maximum_age_seconds)
    }

    pub(super) fn preferred_liveness(
        observed_at_us: Option<u64>,
        reviewed_completion_received_at_us: Option<u64>,
    ) -> Option<u64> {
        preferred_market_liveness_watermark(observed_at_us, reviewed_completion_received_at_us)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn update_reviewed_completion_liveness(
        previous_received_at_us: Option<u64>,
        completion_is_live: bool,
        completion_is_current: bool,
        previous_completion_is_contiguous: bool,
        session_matches_today: bool,
        released_bar_count: usize,
        released_bars_are_current: bool,
        source_received_at_us: u64,
    ) -> Option<u64> {
        reviewed_completion_liveness_watermark(
            previous_received_at_us,
            completion_is_live,
            completion_is_current,
            previous_completion_is_contiguous,
            session_matches_today,
            released_bar_count,
            released_bars_are_current,
            source_received_at_us,
        )
    }

    pub(super) fn session_gate(
        has_complete_history: bool,
        has_resume_boundary: bool,
        allow_partial_boundary: bool,
    ) -> bool {
        market_session_boundary_allows_resume(
            has_complete_history,
            has_resume_boundary,
            allow_partial_boundary,
        )
    }

    pub(super) fn replay_market_messages_at(
        messages: Vec<(&str, Vec<u8>)>,
        now: chrono::NaiveDateTime,
    ) -> anyhow::Result<usize> {
        let durable = messages
            .into_iter()
            .map(|(topic, payload)| DurableMarketMessage {
                topic: topic.to_owned(),
                payload,
            })
            .collect();
        let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
        Ok(replay_market_messages(&mut builder, durable, now)?.len())
    }

    pub(super) fn validate_recovery_bars_at(
        bars: &[MarketBar],
        now: chrono::NaiveDateTime,
    ) -> anyhow::Result<()> {
        validate_recovery_bars_not_future(bars.iter(), now)
    }
}

#[test]
fn every_unknown_nonterminal_contract_blocks_before_market_or_order_ui() {
    let records = [
        remote_order("UNREPORTED", "未报", 0, None),
        remote_order("REPORTED", "已报", 0, None),
        remote_order("WAITING", "待报", 0, None),
        remote_order("UNFILLED", "未成交", 0, None),
        remote_order("PARTIAL", "部分成交", 0, Some("3.55")),
        remote_order("CANCELLED", "全部撤单", 100, None),
        remote_order("FILLED", "全部成交", 0, Some("3.55")),
    ];

    let blocked = live_subject::classify_untracked(&records, &BTreeSet::new())
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        blocked,
        ["PARTIAL", "REPORTED", "UNFILLED", "UNREPORTED", "WAITING"]
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
}

#[test]
fn launch_agent_starts_only_weekdays_at_0900_and_restarts_only_failures() {
    let plist = std::fs::read_to_string("deploy/com.gridedge.ths-sim.plist")
        .expect("reviewed launch-agent template");

    for weekday in 2..=6 {
        assert_eq!(
            plist
                .matches(&format!("<key>Weekday</key><integer>{weekday}</integer>"))
                .count(),
            1
        );
    }
    assert_eq!(
        plist.matches("<key>Hour</key><integer>9</integer>").count(),
        5
    );
    assert_eq!(
        plist
            .matches("<key>Minute</key><integer>0</integer>")
            .count(),
        5
    );
    assert!(plist
        .contains("<key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>"));
    assert!(plist.contains("<key>ThrottleInterval</key>\n  <integer>60</integer>"));
    let deployment_root = "__GRIDEDGE_DEPLOYMENT_ROOT__";
    assert!(plist.contains(&format!("{deployment_root}/bin/run_ths_android_sim.sh")));
    assert!(plist.contains(&format!("{deployment_root}/config/ths_002256_sim.yaml")));
    assert!(std::fs::read_to_string("deploy/ths_002256_sim.yaml")
        .expect("reviewed deployment config")
        .contains("database: \"__GRIDEDGE_DEPLOYMENT_ROOT__/runtime/002256-grid.db\""));
    assert!(plist.contains("ths-002256-20260819-grid15-opening-v1"));
    assert!(plist.contains(&format!(
        "{deployment_root}/runtime/002256-outbox-opening-v1.db"
    )));
    assert!(plist.contains(&format!(
        "{deployment_root}/runtime/002256-opening-v1-quotes.jsonl"
    )));
    assert!(plist.contains(&format!(
        "{deployment_root}/runtime/002256-opening-v1-bars.jsonl"
    )));
    assert!(plist.contains(&format!(
        "{deployment_root}/runtime/002256-opening-v1-market-mqtt.jsonl"
    )));
    assert!(plist.contains("--market-mqtt-host"));
    assert!(plist.contains("<string>__GRIDEDGE_MARKET_HOST__</string>"));
    assert!(plist.contains("--market-mqtt-port"));
    assert!(plist.contains("<string>8883</string>"));
    assert!(plist.contains("--market-mqtt-password-file"));
    assert!(plist.contains(&format!("{deployment_root}/market-mqtt/publisher.password")));
    assert!(plist.contains("--market-mqtt-ca-file"));
    assert!(plist.contains(&format!("{deployment_root}/market-mqtt/ca.crt")));
    assert!(plist.contains(&format!("{deployment_root}/logs/worker.stdout.log")));
    assert!(plist.contains(&format!("{deployment_root}/logs/worker.stderr.log")));
    assert!(!plist.contains("/Documents/"));
    assert!(!plist.contains("target/release"));
    assert!(plist.contains("--maximum-unchanged-seconds"));
    assert!(plist.contains("<string>60</string>"));
    for required in [
        "--simulation-adapter",
        "<string>android</string>",
        "--android-serial",
        "<string>emulator-5554</string>",
        "--android-avd-marker",
        "<string>THSP_API_32</string>",
        "--android-masked-account",
        "<string>**0000</string>",
        "--android-confirmation-account-sha256-file",
        "--android-money-actions-enabled",
        "--android-adb-path",
        "--execution-runner-file",
        "--execution-launch-plist-file",
    ] {
        assert!(
            plist.contains(required),
            "missing Android deployment gate {required}"
        );
    }
    assert!(plist.contains(&format!(
        "{deployment_root}/android-ths/confirmation-account.sha256"
    )));
    assert!(plist.contains("GRIDEDGE_ANDROID_SDK_ROOT"));
    assert!(plist.contains("__GRIDEDGE_USER_HOME__/Library/LaunchAgents"));
    assert!(plist.contains("__GRIDEDGE_ANDROID_SDK_ROOT__/platform-tools/adb"));
    let android_runner = std::fs::read_to_string("deploy/run_ths_android_sim.sh")
        .expect("reviewed Android unattended runner");
    assert!(android_runner.contains("requires exactly one ADB device"));
    assert!(android_runner.contains("ro.boot.qemu.avd_name"));
    assert!(android_runner.contains("-memory 4096"));
    assert!(android_runner.contains("window_animation_scale 0"));
    assert!(android_runner.contains("am start -n \"$package/$activity\""));
    assert!(android_runner.contains("android-runner-failures"));
    assert!(android_runner.contains("daily circuit breaker is open"));
    assert!(android_runner.contains("failure_count + 1"));
    assert!(android_runner.contains("\"$deployment_root/bin/gridedge_ths_live\" \"$@\""));

    let installer = std::fs::read_to_string("deploy/install_ths_sim.sh")
        .expect("reviewed deployment installer");
    let staging = std::fs::read_to_string("deploy/stage_ths_sim.sh")
        .expect("reviewed signed artifact staging boundary");
    assert!(staging.contains("cargo build --locked --release --bin gridedge_ths_live"));
    assert!(staging.contains("GRIDEDGE_CODESIGN_IDENTITY"));
    assert!(staging.contains("codesign --force --sign"));
    assert!(staging.contains("--identifier com.gridedge.ths-live"));
    assert!(staging.contains("candidate_sha256=$(shasum -a 256"));
    assert!(staging.contains("gridedge_ths_live-$candidate_sha256"));
    assert!(installer.contains("GRIDEDGE_CODESIGN_TEAM_ID"));
    assert!(installer.contains("WM4JXVE5GV"));
    assert!(installer.contains("GRIDEDGE_SIGNED_BINARY"));
    assert!(installer.contains("GRIDEDGE_SIGNED_SHA256"));
    assert!(!installer.contains("codesign --force --sign"));
    assert!(!installer.contains("cargo build"));
    assert!(installer.contains("install -m 755 deploy/run_ths_android_sim.sh"));
    assert!(installer.contains("codesign --verify --strict"));
    assert!(installer.contains("TeamIdentifier=$codesign_team_id"));
    assert!(installer.contains("frozen signed worker must not use an ad-hoc signature"));
    let verify_sha = installer
        .find("if [ \"$actual_sha256\" != \"$expected_sha256\" ]")
        .expect("frozen signed artifact SHA is checked before installation");
    let stage_copy = installer
        .find("install -m 755 \"$release_binary\" \"$installed_candidate\"")
        .expect("authorized bytes are first copied to a target-directory candidate");
    let candidate_sha = installer
        .find("if [ \"$candidate_sha256\" != \"$expected_sha256\" ]")
        .expect("target-directory candidate SHA is rechecked");
    let publish = installer
        .find("mv -f \"$installed_candidate\" \"$installed_binary\"")
        .expect("verified candidate is atomically published");
    let final_sha = installer
        .find("if [ \"$installed_sha256\" != \"$expected_sha256\" ]")
        .expect("published binary SHA is checked again");
    assert!(verify_sha < stage_copy && stage_copy < candidate_sha);
    assert!(candidate_sha < publish && publish < final_sha);
    assert!(installer.contains("cmp \"$release_binary\" \"$installed_candidate\""));
    assert!(installer.contains("installed_sha256=$(shasum -a 256 \"$installed_binary\""));
    assert!(installer.contains(r#"tell process "Dock""#));
    assert!(installer.contains("grep -F '/usr/bin/open'"));
    assert!(installer.contains("grep -F 'cn.com.10jqka.macstockPro'"));
}

#[test]
fn progressing_quote_evidence_remains_fresh_when_the_market_price_is_unchanged() {
    let first = market_quote("2026-08-18 09:30:00", "2026-08-18 09:30:01", "3.55", 'a');
    let second = market_quote("2026-08-18 09:31:40", "2026-08-18 09:31:41", "3.55", 'a');
    let after_restart = market_quote("2026-08-18 09:33:10", "2026-08-18 09:33:11", "3.55", 'a');

    live_subject::validate_persisted_freshness(&[first, second, after_restart], 60)
        .expect("fresh reviewed evidence may legitimately report an unchanged traded price");
}

#[test]
fn duplicate_or_nonadvancing_quote_evidence_fails_closed_even_when_price_changes() {
    let first = market_quote("2026-08-18 09:30:00", "2026-08-18 09:30:01", "3.55", 'a');
    let duplicate = first.clone();
    assert!(
        live_subject::validate_persisted_freshness(&[first.clone(), duplicate], 60).is_err(),
        "an exact replay of old quote evidence is stale"
    );

    let changed_price_without_new_evidence =
        market_quote("2026-08-18 09:30:00", "2026-08-18 09:30:01", "3.56", 'b');
    assert!(
        live_subject::validate_persisted_freshness(
            &[first, changed_price_without_new_evidence],
            60,
        )
        .is_err(),
        "price movement cannot make a repeated observed_at timestamp fresh"
    );
}

#[test]
fn completed_bar_log_rejects_identical_and_conflicting_duplicate_timestamps() -> Result<()> {
    let directory = tempdir()?;
    let timestamp = at("2026-08-18 09:35:00");
    let first = MarketBar {
        timestamp,
        symbol: "002256.SZ".to_owned(),
        open: Decimal::new(355, 2),
        high: Decimal::new(357, 2),
        low: Decimal::new(350, 2),
        close: Decimal::new(356, 2),
        volume: 0,
        amount: None,
    };
    let mut conflicting = first.clone();
    conflicting.close = Decimal::new(354, 2);

    for (name, second) in [("identical", first.clone()), ("conflicting", conflicting)] {
        let path = directory.path().join(format!("{name}.jsonl"));
        let contents = format!(
            "{}\n{}\n",
            serde_json::to_string(&first)?,
            serde_json::to_string(&second)?
        );
        std::fs::write(&path, contents)?;
        assert!(
            live_subject::load_completed_bars(&path, "002256.SZ").is_err(),
            "{name} duplicate timestamp must fail closed"
        );
    }
    Ok(())
}

#[test]
fn durable_cancelled_contract_that_is_still_open_in_ui_blocks_the_live_cycle() -> Result<()> {
    let directory = tempdir()?;
    let outbox_path = directory.path().join("outbox.db");
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a"),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;
    outbox.complete_submission(
        "intent-a",
        &request_sha,
        "known-contract",
        r#"{"contract_id":"known-contract"}"#,
    )?;
    outbox.begin_cancellation("intent-a", &request_sha, "known-contract")?;
    outbox.complete_cancellation(
        "intent-a",
        &request_sha,
        "known-contract",
        r#"{"contract_id":"known-contract","remark":"全部撤单"}"#,
    )?;
    drop(outbox);

    let records = [remote_order("known-contract", "未成交", 0, None)];
    let violations = live_subject::audit_known_contracts(&records, &outbox_path)?;
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("durably CANCELLED"));
    assert!(violations[0].contains("UI is not fully cancelled"));
    Ok(())
}

#[test]
fn known_nonterminal_contract_cannot_mint_the_permit_required_to_process_the_next_bar() -> Result<()>
{
    let directory = tempdir()?;
    let outbox_path = directory.path().join("outbox.db");
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event(1, "intent-a", "order-a"),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;
    outbox.complete_submission(
        "intent-a",
        &request_sha,
        "known-contract",
        r#"{"contract_id":"known-contract"}"#,
    )?;
    let before = outbox.staged_intents()?;
    drop(outbox);

    let open = [remote_order("known-contract", "未成交", 0, None)];
    let error = live_subject::terminal_permit_available(&open, &outbox_path)
        .expect_err("a known but unfinished contract cannot authorize another bar");
    assert!(error.to_string().contains("remains non-terminal"));
    assert_eq!(ThsSimOutbox::open(&outbox_path)?.staged_intents()?, before);

    let terminal = [remote_order("known-contract", "全部成交", 0, Some("3.55"))];
    live_subject::terminal_permit_available(&terminal, &outbox_path)?;
    Ok(())
}

#[test]
fn shadow_execution_permit_requires_complete_quantity_and_a_price_no_worse_than_paper() -> Result<()>
{
    let directory = tempdir()?;
    let outbox_path = directory.path().join("outbox.db");
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event_with_quantity(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;
    outbox.complete_submission(
        "intent-a",
        &request_sha,
        "known-contract",
        r#"{"contract_id":"known-contract"}"#,
    )?;
    drop(outbox);

    let incomplete = [remote_order_with_quantity_and_price(
        "known-contract",
        "全部成交",
        1400,
        "3.55",
        "3.55",
    )];
    assert!(live_subject::terminal_permit_available(&incomplete, &outbox_path).is_err());

    let worse_than_adverse_paper = [remote_order_with_quantity_and_price(
        "known-contract",
        "全部成交",
        1500,
        "3.55",
        "3.56",
    )];
    let error = live_subject::terminal_permit_available(&worse_than_adverse_paper, &outbox_path)
        .expect_err("a BUY fill worse than the modeled Paper average must block the next bar");
    assert!(error.to_string().contains("worse than"));

    for acceptable in ["3.55", "3.54"] {
        let records = [remote_order_with_quantity_and_price(
            "known-contract",
            "全部成交",
            1500,
            "3.55",
            acceptable,
        )];
        live_subject::terminal_permit_available(&records, &outbox_path)?;
    }
    Ok(())
}

#[test]
fn runtime_permit_uses_partial_and_final_fills_from_the_real_sqlite_source_ledger() -> Result<()> {
    let directory = tempdir()?;
    let source_path = directory.path().join("source.db");
    let outbox_path = directory.path().join("outbox.db");
    let run_id = "ths-modeled-fill-ledger";
    let mut config = Config::load("configs/default.yaml")?;
    config.database = source_path.display().to_string();
    config.paper.partial_fill_bps = 10_000;
    config.config_version = "ths-modeled-fill-regression".to_owned();
    config.validate()?;
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some(run_id.to_owned()),
    )?;
    service.on_bar(&source_bar(
        "2026-08-18 09:30:00",
        "10.00",
        "10.01",
        "9.99",
        "10.00",
    ))?;

    service.on_bar(&source_bar(
        "2026-08-18 09:35:00",
        "10.00",
        "10.01",
        "9.79",
        "9.82",
    ))?;
    let durable_events = service.store.load_after(run_id, 0)?;
    assert_eq!(
        durable_events
            .iter()
            .filter(|event| event.event_type == EventType::OrderPartiallyFilled)
            .count(),
        1
    );
    assert_eq!(
        durable_events
            .iter()
            .filter(|event| event.event_type == EventType::OrderFilled)
            .count(),
        1
    );

    stage_from_source(&source_path, run_id, &outbox_path, Some(0))?;
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    let staged = outbox.staged_intents()?;
    let [intent] = staged.as_slice() else {
        panic!("one staged BUY expected");
    };
    let intent = intent.clone();
    assert_eq!(intent.order.quantity, config.standard_quantity);
    let (direction, modeled_quantity, modeled_average) =
        live_subject::modeled_fill_from_source(&source_path, run_id, &intent.order_id)?;
    assert_eq!(direction, Direction::Buy);
    assert_eq!(modeled_quantity, config.standard_quantity);
    assert_eq!(modeled_average, Decimal::new(981, 2));
    assert!(modeled_average > intent.order.limit_price);

    outbox.begin_submission(&intent.intent_id, &intent.request_sha256, &[])?;
    outbox.complete_submission(
        &intent.intent_id,
        &intent.request_sha256,
        "runtime-contract",
        r#"{"contract_id":"runtime-contract"}"#,
    )?;
    drop(outbox);

    for acceptable in [modeled_average, modeled_average - Decimal::new(1, 2)] {
        let records = [default_symbol_filled_order(
            "runtime-contract",
            config.standard_quantity,
            intent.order.limit_price,
            acceptable,
        )];
        live_subject::terminal_permit_from_source(&records, &source_path, run_id, &outbox_path)?;
    }

    let worse = [default_symbol_filled_order(
        "runtime-contract",
        config.standard_quantity,
        intent.order.limit_price,
        modeled_average + Decimal::new(1, 2),
    )];
    let error =
        live_subject::terminal_permit_from_source(&worse, &source_path, run_id, &outbox_path)
            .expect_err("runtime permit must use the durable modeled fill, not the limit price");
    assert!(error.to_string().contains("worse than"));

    let incomplete = [default_symbol_filled_order(
        "runtime-contract",
        config.standard_quantity - config.lot_size,
        intent.order.limit_price,
        modeled_average,
    )];
    assert!(live_subject::terminal_permit_from_source(
        &incomplete,
        &source_path,
        run_id,
        &outbox_path,
    )
    .is_err());

    let aggregate_outbox_path = directory.path().join("aggregate-outbox.db");
    stage_from_source(&source_path, run_id, &aggregate_outbox_path, Some(0))?;
    let mut aggregate_outbox = ThsSimOutbox::open(&aggregate_outbox_path)?;
    let aggregate_intent = aggregate_outbox.staged_intents()?[0].clone();
    aggregate_outbox.begin_submission(
        &aggregate_intent.intent_id,
        &aggregate_intent.request_sha256,
        &[],
    )?;
    let aggregate_order = default_symbol_filled_order(
        "aggregate-contract",
        config.standard_quantity,
        aggregate_intent.order.limit_price,
        Decimal::new(980, 2),
    );
    aggregate_outbox.complete_submission(
        &aggregate_intent.intent_id,
        &aggregate_intent.request_sha256,
        "aggregate-contract",
        &serde_json::to_string(&aggregate_order)?,
    )?;
    aggregate_outbox.begin_cancellation(
        &aggregate_intent.intent_id,
        &aggregate_intent.request_sha256,
        "aggregate-contract",
    )?;
    aggregate_outbox.mark_cancellation_ambiguous(
        &aggregate_intent.intent_id,
        &aggregate_intent.request_sha256,
        "aggregate-contract",
        "order row disappeared before cancel confirmation",
        None,
    )?;
    let aggregate_fills = [
        remote_fill("fill-01", "aggregate-contract", 700, "9.80"),
        remote_fill("fill-02", "aggregate-contract", 800, "9.82"),
    ];
    let filled_quantity = aggregate_fills.iter().map(|fill| fill.quantity).sum();
    let filled_notional = aggregate_fills
        .iter()
        .map(|fill| fill.amount)
        .sum::<Decimal>();
    let evidence = json!({
        "evidence_contract_version": 1,
        "order": aggregate_order,
        "fills": aggregate_fills,
    });
    aggregate_outbox.record_remote_filled(
        &aggregate_intent.intent_id,
        &aggregate_intent.request_sha256,
        "aggregate-contract",
        "2026-08-19 10:40:00",
        2,
        filled_quantity,
        &filled_notional.to_string(),
        &serde_json::to_string(&evidence)?,
    )?;
    drop(aggregate_outbox);
    let error = live_subject::terminal_permit_from_source(
        &[aggregate_order],
        &source_path,
        run_id,
        &aggregate_outbox_path,
    )
    .expect_err(
        "permit must compare Paper to the exact fill-page aggregate, not the order-page price",
    );
    assert!(error.to_string().contains("worse than"));

    let conservative_outbox_path = directory.path().join("conservative-aggregate-outbox.db");
    stage_from_source(&source_path, run_id, &conservative_outbox_path, Some(0))?;
    let mut conservative_outbox = ThsSimOutbox::open(&conservative_outbox_path)?;
    let conservative_intent = conservative_outbox.staged_intents()?[0].clone();
    conservative_outbox.begin_submission(
        &conservative_intent.intent_id,
        &conservative_intent.request_sha256,
        &[],
    )?;
    let conservative_order = default_symbol_filled_order(
        "conservative-contract",
        config.standard_quantity,
        conservative_intent.order.limit_price,
        Decimal::new(980, 2),
    );
    conservative_outbox.complete_submission(
        &conservative_intent.intent_id,
        &conservative_intent.request_sha256,
        "conservative-contract",
        &serde_json::to_string(&conservative_order)?,
    )?;
    conservative_outbox.begin_cancellation(
        &conservative_intent.intent_id,
        &conservative_intent.request_sha256,
        "conservative-contract",
    )?;
    conservative_outbox.mark_cancellation_ambiguous(
        &conservative_intent.intent_id,
        &conservative_intent.request_sha256,
        "conservative-contract",
        "order row disappeared before cancel confirmation",
        None,
    )?;
    let conservative_fills = [
        remote_fill("fill-01", "conservative-contract", 700, "9.80"),
        remote_fill("fill-02", "conservative-contract", 800, "9.81"),
    ];
    let conservative_quantity = conservative_fills.iter().map(|fill| fill.quantity).sum();
    let conservative_notional = conservative_fills
        .iter()
        .map(|fill| fill.amount)
        .sum::<Decimal>();
    let conservative_evidence = json!({
        "evidence_contract_version": 1,
        "order": conservative_order,
        "fills": conservative_fills,
    });
    conservative_outbox.record_remote_filled(
        &conservative_intent.intent_id,
        &conservative_intent.request_sha256,
        "conservative-contract",
        "2026-08-19 10:41:00",
        2,
        conservative_quantity,
        &conservative_notional.to_string(),
        &serde_json::to_string(&conservative_evidence)?,
    )?;
    assert_eq!(
        conservative_outbox.staged_intents()?[0].terminal_resolution,
        StagedTerminalResolution::Filled
    );
    drop(conservative_outbox);
    live_subject::terminal_permit_from_source(
        &[conservative_order],
        &source_path,
        run_id,
        &conservative_outbox_path,
    )?;
    assert_eq!(
        ThsSimOutbox::open(&conservative_outbox_path)?.staged_intents()?[0].terminal_resolution,
        StagedTerminalResolution::Filled
    );
    Ok(())
}

#[test]
fn deployment_initial_half_position_is_one_durable_paper_order_before_outbox_execution(
) -> Result<()> {
    let directory = tempdir()?;
    let source_path = directory.path().join("source.db");
    let outbox_path = directory.path().join("outbox.db");
    let run_id = "ths-initial-half-position";
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = source_path.display().to_string();
    config.config_version = "ths-initial-half-regression".to_owned();
    config.validate()?;
    assert_eq!(config.initial_cash, Decimal::new(200_000, 0));
    assert_eq!(config.standard_quantity, 5_500);
    assert_eq!(
        config
            .gate
            .capital_inventory
            .as_ref()
            .expect("deployment cash floor")
            .minimum_free_cash,
        Decimal::new(100_000, 0)
    );

    let first_quote = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some(run_id.to_owned()),
    )?;
    service.on_bar(&first_quote)?;

    let events = service.store.load_after(run_id, 0)?;
    let market_event = events
        .iter()
        .find(|event| event.event_type == EventType::MarketDataReceived)
        .expect("durable source market event");
    let evaluation_event = events
        .iter()
        .find(|event| event.event_type == EventType::InitialDeploymentEvaluated)
        .expect("durable initial-deployment evaluation");
    let evaluation: InitialDeploymentEvaluation =
        serde_json::from_value(evaluation_event.payload.clone())?;
    assert_eq!(evaluation.outcome, InitialDeploymentOutcome::Authorized);
    assert_eq!(evaluation.source_market_event_id, market_event.event_id);
    assert_eq!(
        evaluation.source_evidence_sha256,
        market_evidence_sha256(&first_quote)?
    );
    assert_eq!(evaluation_event.event_time, market_event.event_time);
    assert!(evaluation_event.sequence_number > market_event.sequence_number);
    let processed_event = events
        .iter()
        .find(|event| event.event_type == EventType::MarketBarProcessed)
        .expect("completed market stage");
    assert!(
        evaluation_event.sequence_number < processed_event.sequence_number,
        "initial evaluation must bind the still-pending market fact"
    );
    let intents = events
        .iter()
        .filter(|event| event.event_type == EventType::OrderIntentCreated)
        .collect::<Vec<_>>();
    let [intent_event] = intents.as_slice() else {
        panic!("the first deployment quote must create exactly one durable initial-half BUY");
    };
    assert_eq!(intent_event.schema_version, 7);
    assert_eq!(intent_event.correlation_id, evaluation_event.correlation_id);
    assert!(intent_event.sequence_number > evaluation_event.sequence_number);
    let order: Order = serde_json::from_value(intent_event.payload.clone())?;
    let OrderIntentOrigin::InitialDeploymentV1 {
        deployment_id,
        source_market_event_id,
        source_evidence_sha256,
        policy_version,
    } = &order.intent.origin
    else {
        panic!("schema-7 initial order requires its typed origin");
    };
    assert_eq!(deployment_id, &evaluation.deployment_id);
    assert_eq!(source_market_event_id, &market_event.event_id);
    assert_eq!(source_evidence_sha256, &evaluation.source_evidence_sha256);
    assert_eq!(policy_version, &evaluation.policy_version);
    assert_eq!(order.intent.direction, Direction::Buy);
    assert_eq!(order.intent.quantity, 5 * config.standard_quantity);
    assert_eq!(order.intent.quantity, 27_500);
    assert_eq!(order.intent.limit_price, Decimal::new(351, 2));
    assert_eq!(service.state.position.total, 27_500);
    assert!(service.state.cash.available >= Decimal::new(100_000, 0));
    assert!(service.state.cash.available < config.initial_cash);
    assert!(service.state.grid_rights.is_empty());
    assert!(service.state.right_tranches.is_empty());
    assert!(service.state.historically_executed_levels.is_empty());
    assert!(service
        .state
        .lots
        .values()
        .all(|lot| lot.opening_allocation && lot.grid_index == 0));
    let durable_cash = service.state.cash.clone();
    let durable_position = service.state.position.clone();
    drop(service);

    let mut recovered = GridAutomationService::recover_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        run_id.to_owned(),
    )?;
    assert_eq!(recovered.state.cash, durable_cash);
    assert_eq!(recovered.state.position, durable_position);
    recovered.on_bar(&first_quote)?;
    let recovered_events = recovered.store.load_after(run_id, 0)?;
    assert_eq!(
        recovered_events
            .iter()
            .filter(|event| event.event_type == EventType::OrderIntentCreated)
            .count(),
        1,
        "restart and replay of the same evidence must not mint the initial allocation twice"
    );
    drop(recovered);

    stage_from_source(&source_path, run_id, &outbox_path, Some(0))?;
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    let staged = outbox.staged_intents()?;
    let [staged] = staged.as_slice() else {
        panic!("the outbox must derive exactly one order from the durable ledger");
    };
    assert_eq!(staged.order.direction, order.intent.direction);
    assert_eq!(staged.order.quantity, order.intent.quantity);
    assert_eq!(staged.order.limit_price, order.intent.limit_price);
    let staged = staged.clone();
    outbox.begin_submission(&staged.intent_id, &staged.request_sha256, &[])?;
    outbox.complete_submission(
        &staged.intent_id,
        &staged.request_sha256,
        "6210000001",
        r#"{"contract_id":"6210000001"}"#,
    )?;
    drop(outbox);

    let (direction, modeled_quantity, modeled_average) =
        live_subject::modeled_fill_from_source(&source_path, run_id, &staged.order_id)?;
    assert_eq!(direction, Direction::Buy);
    assert_eq!(modeled_quantity, 27_500);
    assert!(
        live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path).is_err(),
        "a merely SUBMITTED contract cannot disappear from today's order table"
    );
    let incomplete = [remote_order_with_quantity_and_price(
        "6210000001",
        "部分成交",
        27_400,
        "3.51",
        "3.370",
    )];
    assert!(live_subject::terminal_permit_from_source(
        &incomplete,
        &source_path,
        run_id,
        &outbox_path,
    )
    .is_err());
    let complete = [remote_order_with_quantity_and_price(
        "6210000001",
        "全部成交",
        27_500,
        "3.51",
        "3.370",
    )];
    live_subject::terminal_permit_from_source(&complete, &source_path, run_id, &outbox_path)?;

    let completed_order = complete[0].clone();
    let fill_slices = [
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_500, "3.370"),
        (2_100, "3.370"),
        (2_500, "3.380"),
        (2_500, "3.380"),
        (2_900, "3.380"),
    ];
    let remote_fills = fill_slices
        .iter()
        .enumerate()
        .map(|(index, (quantity, price))| {
            let price = price.parse::<Decimal>()?;
            Ok(SimulationFillRecord {
                fill_date: "20260818".to_owned(),
                fill_time: format!("0930{:02}", index + 2),
                symbol: staged.order.symbol.clone(),
                security_name: "兆新股份".to_owned(),
                direction,
                quantity: *quantity,
                price,
                amount: price
                    .checked_mul(Decimal::from(*quantity))
                    .expect("opening remote slice notional"),
                contract_id: "6210000001".to_owned(),
                fill_id: format!("6210000001-fill-{:02}", index + 1),
                fill_category: "普通成交".to_owned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let remote_notional = remote_fills.iter().map(|fill| fill.amount).sum::<Decimal>();
    assert_eq!(
        remote_fills.iter().map(|fill| fill.quantity).sum::<i64>(),
        27_500
    );
    assert_eq!(remote_notional, Decimal::new(92_754, 0));
    let terminal_evidence = RemoteFilledEvidence {
        evidence_contract_version: 1,
        order: completed_order,
        fills: remote_fills,
    };
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.record_remote_filled(
        &staged.intent_id,
        &staged.request_sha256,
        "6210000001",
        "2026-08-18 09:30:03.000",
        11,
        modeled_quantity,
        &remote_notional.normalize().to_string(),
        &serde_json::to_string(&terminal_evidence)?,
    )?;
    let frozen_outbox = outbox.staged_intents()?;
    assert_eq!(
        frozen_outbox[0].terminal_resolution,
        StagedTerminalResolution::Filled
    );
    drop(outbox);
    let source_head = SqliteStore::open(&source_path)?.latest_sequence(run_id)?;

    // The next trading day's “今天” filter legitimately omits yesterday's
    // fully reconciled contract. This read-only proof must be idempotent.
    live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path)?;
    assert!(live_subject::audit_known_contracts(&[], &outbox_path)?.is_empty());
    assert_eq!(
        ThsSimOutbox::open(&outbox_path)?.staged_intents()?,
        frozen_outbox,
        "cross-day audit must not restage or repeat a money action"
    );
    assert_eq!(
        SqliteStore::open(&source_path)?.latest_sequence(run_id)?,
        source_head
    );
    live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path)?;

    let contradictory_today_row = [remote_order_with_quantity_and_price(
        "6210000001",
        "全部成交",
        27_400,
        "3.51",
        &modeled_average.to_string(),
    )];
    assert!(live_subject::terminal_permit_from_source(
        &contradictory_today_row,
        &source_path,
        run_id,
        &outbox_path,
    )
    .is_err());
    let unknown_today_open = [remote_order("unknown-today", "未成交", 0, None)];
    assert!(live_subject::terminal_permit_from_source(
        &unknown_today_open,
        &source_path,
        run_id,
        &outbox_path,
    )
    .is_err());

    stage_from_source(&source_path, run_id, &outbox_path, None)?;
    assert_eq!(ThsSimOutbox::open(&outbox_path)?.staged_intents()?.len(), 1);

    let connection = Connection::open(&outbox_path)?;
    let canonical_terminal_evidence: String = connection.query_row(
        "SELECT evidence_json FROM remote_execution_facts WHERE intent_id=?1",
        [&staged.intent_id],
        |row| row.get(0),
    )?;
    connection.execute(
        "UPDATE remote_execution_facts SET evidence_json='{}' WHERE intent_id=?1",
        [&staged.intent_id],
    )?;
    drop(connection);
    assert!(
        live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path).is_err(),
        "a cross-day omission cannot hide tampered durable fill evidence"
    );
    assert_eq!(
        SqliteStore::open(&source_path)?.latest_sequence(run_id)?,
        source_head
    );

    let connection = Connection::open(&outbox_path)?;
    connection.execute(
        "UPDATE remote_execution_facts SET evidence_json=?1 WHERE intent_id=?2",
        params![canonical_terminal_evidence, staged.intent_id],
    )?;
    drop(connection);
    live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path)?;

    let connection = Connection::open(&outbox_path)?;
    connection.execute(
        "DELETE FROM remote_execution_facts WHERE intent_id=?1",
        [&staged.intent_id],
    )?;
    drop(connection);
    assert!(
        live_subject::terminal_permit_from_source(&[], &source_path, run_id, &outbox_path)
            .is_err(),
        "a bound contract absent from today's table still requires its complete durable FILLED fact"
    );
    assert_eq!(
        SqliteStore::open(&source_path)?.latest_sequence(run_id)?,
        source_head
    );

    let live_source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")?;
    let args = live_source
        .split_once("struct Args {")
        .expect("live CLI arguments")
        .1
        .split_once("}\n")
        .expect("live CLI argument boundary")
        .0;
    for forbidden in ["direction:", "quantity:", "price:", "contract_id:"] {
        assert!(
            !args.contains(forbidden),
            "the production live CLI must not accept temporary order field {forbidden}"
        );
    }
    Ok(())
}

#[test]
fn deployment_cash_floor_selects_five_units_when_six_are_mechanically_available() -> Result<()> {
    let directory = tempdir()?;
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = directory.path().join("six-units.db").display().to_string();
    config.trade_levels = 6;
    config.boundary_levels = 7;
    config.config_version = "ths-initial-six-unit-boundary".to_owned();
    config.validate()?;
    let quote = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some("ths-initial-six-unit-boundary".to_owned()),
    )?;
    service.on_bar(&quote)?;
    let evaluation = service
        .state
        .initial_deployment_evaluation
        .as_ref()
        .expect("one durable evaluation");
    assert_eq!(evaluation.maximum_units, 6);
    assert_eq!(evaluation.selected_units, 5);
    assert_eq!(evaluation.quantity, 27_500);
    assert!(evaluation.projected_remaining_cash >= Decimal::new(100_000, 0));
    let six_unit_reservation =
        estimated_buy_reservation(&config, quote.close, 6 * config.standard_quantity)?;
    assert!(config.initial_cash - six_unit_reservation < Decimal::new(100_000, 0));
    Ok(())
}

#[test]
fn initial_deployment_origin_is_bound_to_the_exact_unprocessed_market_evidence() -> Result<()> {
    let directory = tempdir()?;
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = directory.path().join("evidence.db").display().to_string();
    config.config_version = "ths-initial-evidence-binding".to_owned();
    config.validate()?;
    let run_id = "ths-initial-evidence-binding";
    let first = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some(run_id.to_owned()),
    )?;
    let before = service.state.clone();
    service.on_bar(&first)?;
    let events = service.store.load_after(run_id, 0)?;
    let market = events
        .iter()
        .find(|event| event.event_type == EventType::MarketDataReceived)
        .expect("market evidence")
        .clone();
    let day = events
        .iter()
        .find(|event| event.event_type == EventType::TradingDayRolled)
        .expect("trading day")
        .clone();
    let evaluation = events
        .iter()
        .find(|event| event.event_type == EventType::InitialDeploymentEvaluated)
        .expect("initial evaluation")
        .clone();
    let intent = events
        .iter()
        .find(|event| event.event_type == EventType::OrderIntentCreated)
        .expect("typed initial order")
        .clone();

    let mut pending = before.clone();
    pending.apply_event(&market)?;
    pending.apply_event(&day)?;
    let mut bad_hash = evaluation.clone();
    bad_hash.payload["source_evidence_sha256"] =
        json!("0000000000000000000000000000000000000000000000000000000000000000");
    assert!(pending.apply_event(&bad_hash).is_err());
    assert!(pending.initial_deployment_evaluation.is_none());
    pending.apply_event(&evaluation)?;

    let mut legacy_typed_origin = intent.clone();
    legacy_typed_origin.schema_version = 6;
    assert!(pending.apply_event(&legacy_typed_origin).is_err());
    assert!(pending.orders.is_empty());
    pending.apply_event(&intent)?;

    let mut processed_without_deployment = before;
    processed_without_deployment.apply_event(&market)?;
    processed_without_deployment.apply_event(&day)?;
    for event in &events {
        if matches!(
            event.event_type,
            EventType::MarketBarDecisionsCommitted | EventType::MarketBarProcessed
        ) {
            processed_without_deployment.apply_event(event)?;
        }
    }
    assert!(processed_without_deployment
        .pending_market_evidence
        .is_none());
    assert!(processed_without_deployment
        .apply_event(&evaluation)
        .is_err());
    assert!(processed_without_deployment
        .initial_deployment_evaluation
        .is_none());
    Ok(())
}

#[test]
fn opening_allocation_is_t1_blocked_then_becomes_a_sell_tranche_without_grid_history() -> Result<()>
{
    let directory = tempdir()?;
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = directory.path().join("opening-t1.db").display().to_string();
    config.config_version = "ths-opening-allocation-t1".to_owned();
    config.validate()?;
    let first = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some("ths-opening-allocation-t1".to_owned()),
    )?;
    service.on_bar(&first)?;
    assert_eq!(service.state.position.total, 27_500);
    assert_eq!(service.state.position.sellable, 0);
    assert_eq!(service.state.position.today_bought, 27_500);
    assert!(service.state.historically_executed_levels.is_empty());

    let same_day = RightsGateCoordinator::new(&config, &service.state).capacity(
        1,
        Direction::Sell,
        first.timestamp.date(),
        "same-day-opening-sell",
    )?;
    assert_eq!(same_day.eligible_quantity, 0);
    assert_eq!(same_day.t_plus_one_blocked_quantity, 27_500);
    assert_eq!(same_day.t_plus_one_blocked_lot_ids.len(), 1);

    let next_day = MarketBar {
        timestamp: at("2026-08-19 09:30:00"),
        ..first
    };
    service.on_bar(&next_day)?;
    assert_eq!(service.state.position.sellable, 27_500);
    assert_eq!(service.state.position.today_bought, 0);
    let eligible = RightsGateCoordinator::new(&config, &service.state).capacity(
        1,
        Direction::Sell,
        next_day.timestamp.date(),
        "next-day-opening-sell",
    )?;
    assert_eq!(eligible.t_plus_one_blocked_quantity, 0);
    assert_eq!(eligible.eligible_quantity, 27_500);
    assert_eq!(eligible.eligible_lot_ids.len(), 1);
    assert_eq!(eligible.tranche_ids.len(), 1);
    assert!(service.state.historically_executed_levels.is_empty());
    Ok(())
}

#[test]
fn rejected_initial_deployment_is_durable_once_but_never_outbox_eligible() -> Result<()> {
    let directory = tempdir()?;
    let source_path = directory.path().join("rejected.db");
    let outbox_path = directory.path().join("rejected-outbox.db");
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = source_path.display().to_string();
    config.paper.reject_probability_bps = 10_000;
    config.paper.partial_fill_bps = 0;
    config.config_version = "ths-initial-rejected".to_owned();
    config.validate()?;
    let run_id = "ths-initial-rejected";
    let first = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some(run_id.to_owned()),
    )?;
    service.on_bar(&first)?;
    let events = service.store.load_after(run_id, 0)?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::InitialDeploymentEvaluated)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::OrderIntentCreated)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::OrderRejected)
            .count(),
        1
    );
    assert_eq!(service.state.position.total, 0);
    assert!(service.state.lots.is_empty());
    assert!(service.state.grid_rights.is_empty());
    assert!(service.state.historically_executed_levels.is_empty());
    drop(service);

    let mut recovered = GridAutomationService::recover_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        run_id.to_owned(),
    )?;
    recovered.on_bar(&first)?;
    let recovered_events = recovered.store.load_after(run_id, 0)?;
    assert_eq!(
        recovered_events
            .iter()
            .filter(|event| event.event_type == EventType::InitialDeploymentEvaluated)
            .count(),
        1
    );
    assert_eq!(
        recovered_events
            .iter()
            .filter(|event| event.event_type == EventType::OrderIntentCreated)
            .count(),
        1
    );
    drop(recovered);

    let status = stage_from_source(&source_path, run_id, &outbox_path, Some(0))?;
    assert_eq!(
        status.eligible, 0,
        "Paper-rejected orders must cause zero UI submission"
    );
    assert_eq!(status.submitting, 0);
    assert_eq!(status.submitted, 0);
    Ok(())
}

#[test]
fn partially_filled_initial_deployment_conserves_one_order_and_opening_inventory() -> Result<()> {
    let directory = tempdir()?;
    let source_path = directory.path().join("partial.db");
    let outbox_path = directory.path().join("partial-outbox.db");
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = source_path.display().to_string();
    config.paper.reject_probability_bps = 0;
    config.paper.partial_fill_bps = 10_000;
    config.config_version = "ths-initial-partial".to_owned();
    config.validate()?;
    let run_id = "ths-initial-partial";
    let first = MarketBar {
        timestamp: at("2026-08-18 09:30:00"),
        symbol: config.symbol.clone(),
        open: Decimal::new(351, 2),
        high: Decimal::new(351, 2),
        low: Decimal::new(351, 2),
        close: Decimal::new(351, 2),
        volume: 0,
        amount: None,
    };
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some(run_id.to_owned()),
    )?;
    service.on_bar(&first)?;
    let events = service.store.load_after(run_id, 0)?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::OrderIntentCreated)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::OrderPartiallyFilled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::OrderFilled)
            .count(),
        1
    );
    assert_eq!(service.state.position.total, 27_500);
    assert_eq!(
        service
            .state
            .lots
            .values()
            .map(|lot| lot.remaining_quantity)
            .sum::<i64>(),
        27_500
    );
    assert!(service
        .state
        .lots
        .values()
        .all(|lot| lot.opening_allocation));
    assert!(service.state.historically_executed_levels.is_empty());
    drop(service);
    let status = stage_from_source(&source_path, run_id, &outbox_path, Some(0))?;
    assert_eq!(status.eligible, 1);
    assert_eq!(ThsSimOutbox::open(&outbox_path)?.staged_intents()?.len(), 1);
    Ok(())
}

#[test]
fn terminal_permit_rejects_duplicate_rows_for_the_same_remote_contract() -> Result<()> {
    let directory = tempdir()?;
    let outbox_path = directory.path().join("outbox.db");
    let mut outbox = ThsSimOutbox::open(&outbox_path)?;
    outbox.bind_or_verify(SOURCE, "run-a", Some(0))?;
    outbox.ingest(&[
        intent_event_with_quantity(1, "intent-a", "order-a", 1500),
        submitted_event(2, "order-a"),
    ])?;
    let request_sha = outbox.staged_intents()?[0].request_sha256.clone();
    outbox.begin_submission("intent-a", &request_sha, &[])?;
    outbox.complete_submission(
        "intent-a",
        &request_sha,
        "duplicate-contract",
        r#"{"contract_id":"duplicate-contract"}"#,
    )?;
    drop(outbox);

    let duplicate = remote_order_with_quantity_and_price(
        "duplicate-contract",
        "全部成交",
        1500,
        "3.55",
        "3.55",
    );
    let error =
        live_subject::terminal_permit_available(&[duplicate.clone(), duplicate], &outbox_path)
            .expect_err("one durable contract must correspond to exactly one visible order row");
    assert!(
        error.to_string().contains("appears more than once"),
        "unexpected duplicate-row diagnostic: {error:#}"
    );
    Ok(())
}

#[test]
fn mqtt_tick_path_persists_raw_events_and_defers_order_reconciliation_until_completed_bars() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let market_branch = source
        .split_once("let Some(message) = market.receive")
        .expect("MQTT receive branch")
        .1
        .split_once("fn probe_remote_orders")
        .expect("MQTT receive branch end")
        .0;
    let receive = 0;
    let append = market_branch
        .find("append_json_line(")
        .expect("durable MQTT append");
    let append_end = market_branch[append..]
        .find("DurableMarketMessage::from(&message)")
        .map(|offset| append + offset)
        .expect("durable MQTT evidence");
    let ingest = market_branch
        .find("builder.ingest_with_receipt_at(&message.topic, &message.payload, now)")
        .expect("source-sequenced trade builder");
    let completed = market_branch
        .find("for bar in receipt.bars")
        .expect("completed-bar boundary");
    let reconcile = market_branch
        .find("reconcile_and_process_bar(")
        .expect("permit-gated completed bar");
    assert!(
        receive < append
            && append <= append_end
            && append_end < ingest
            && ingest < completed
            && completed < reconcile
    );
    let raw_receipt = &market_branch[..completed];
    for forbidden in [
        "probe_remote_orders(",
        "verify_remote_contracts(",
        "run_outbox(",
        "remote_terminal_permit(",
        "process_bar(args,",
        "store_bar_if_new(",
        "service.on_bar(",
    ] {
        assert!(
            !raw_receipt.contains(forbidden),
            "raw MQTT receipt must not directly perform {forbidden}"
        );
    }
    assert!(source.contains("_permit: &RemoteTerminalPermit"));
}

#[test]
fn production_loop_uses_complete_mqtt_ticks_and_never_the_discrete_quote_probe() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let production = source
        .split_once("fn run() -> Result<()> {")
        .expect("production run")
        .1
        .split_once("fn probe_remote_orders")
        .expect("production run end")
        .0;
    assert!(production.contains("MarketMqttClient::connect("));
    assert!(production
        .contains("builder.ingest_with_receipt_at(&message.topic, &message.payload, now)"));
    assert!(production.contains("builder.has_complete_session(date)"));
    assert!(!production.contains("probe_current_market_quote"));
}

#[test]
fn remote_execution_identity_and_read_only_preflight_block_before_service_or_market_start() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let production = source
        .split_once("fn run() -> Result<()> {")
        .expect("production run")
        .1
        .split_once("fn probe_remote_orders")
        .expect("production run end")
        .0;

    let verify = production
        .find("verify_execution_identity(&execution_identity)")
        .expect("durable execution identity gate");
    let preflight = production
        .find("driver.startup_preflight()?")
        .expect("read-only remote preflight");
    let recover = production
        .find("GridAutomationService::recover_with_algorithm(")
        .expect("service recovery");
    let start = production
        .find("GridAutomationService::start_new_with_algorithm(")
        .expect("service start");
    let market = production
        .find("MarketMqttClient::connect(")
        .expect("market connection");

    assert!(verify < preflight);
    assert!(preflight < recover && preflight < start);
    assert!(recover < market && start < market);

    let bind_dispatch = production
        .find("if args.bind_execution_identity_only")
        .expect("identity-only dispatch");
    let bind_return = production[bind_dispatch..]
        .find("return Ok(())")
        .map(|offset| bind_dispatch + offset)
        .expect("identity-only early return");
    assert!(bind_return < verify && bind_return < market);
}

#[test]
fn identity_only_upgrade_rechecks_platform_files_and_source_before_append() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let binding = source
        .split_once("fn bind_execution_identity_only(")
        .expect("identity-only function")
        .1
        .split_once("fn verify_activated_binding_platform(")
        .expect("identity-only function end")
        .0;
    let first_platform = binding
        .find("verify_activated_binding_platform(args, config)?")
        .expect("first platform check");
    let first_identity = binding
        .find("build_driver_and_execution_identity(args, quote_symbol)?")
        .expect("first file-bound identity");
    let preflight = binding
        .find("driver.startup_preflight()?")
        .expect("read-only adapter preflight");
    let second_platform = binding[first_platform + 1..]
        .find("verify_activated_binding_platform(args, config)?")
        .map(|offset| first_platform + 1 + offset)
        .expect("second platform check");
    let second_identity = binding[first_identity + 1..]
        .find("build_driver_and_execution_identity(args, quote_symbol)?")
        .map(|offset| first_identity + 1 + offset)
        .expect("second file-bound identity");
    let identity_equal = binding
        .find("identity_after_preflight != identity_before_preflight")
        .expect("TOCTOU identity equality gate");
    let source_binding = binding
        .find("stage_from_source(")
        .expect("ledger/outbox source identity gate");
    let append = binding
        .find("upgrade_execution_identity(&identity_after_preflight)")
        .expect("append-only identity upgrade");
    assert!(first_platform < first_identity);
    assert!(first_identity < preflight && preflight < second_platform);
    assert!(second_platform < second_identity && second_identity < identity_equal);
    assert!(identity_equal < source_binding && source_binding < append);
}

#[test]
fn startup_backfill_stays_read_only_without_ui_until_the_live_watermark_is_current() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let production = source
        .split_once("fn run() -> Result<()> {")
        .expect("production run")
        .1
        .split_once("fn probe_remote_orders")
        .expect("production run end")
        .0;
    let recovered_start = production
        .find("for bar in recovered_completed_bars {")
        .expect("startup market backfill");
    let receive = production
        .find("let Some(message) = market.receive")
        .expect("live MQTT receive loop");
    let recovered = &production[recovered_start..receive];

    assert!(recovered.contains("process_market_recovery_bar("));
    assert!(!recovered.contains("reconcile_and_process_bar("));
    assert!(!recovered.contains("probe_remote_orders("));

    let connect = production
        .find("MarketMqttClient::connect(")
        .expect("MQTT connection");
    assert!(
        recovered_start < connect,
        "the MQTT keepalive must not start before local backfill finishes"
    );

    assert!(!recovered.contains("resume_after_market_data_recovery("));

    let live = &production[receive..];
    let current = live
        .find("market_watermark_is_current(")
        .expect("fresh completion watermark gate");
    let precheck = live
        .find("completion_requires_recovery(")
        .expect("pre-bar recovery decision");
    let completed_bars = live
        .find("for bar in receipt.bars")
        .expect("completion-bound bars");
    let resume = live
        .find("resume_after_market_data_recovery(")
        .expect("single remote reconciliation before RUNNING");
    assert!(current < precheck && precheck < completed_bars && completed_bars < resume);

    let receive_now = live
        .find("let now = Local::now().naive_local();")
        .expect("post-receive local clock sample");
    assert!(receive_now < current);

    let startup_recovery = production
        .find("service.enter_market_data_recovery(\"EASTMONEY_SESSION_HISTORY_BACKFILL\")")
        .expect("startup recovery mode");
    let durable_replay = production
        .find("for bar in bars.values()")
        .expect("durable bar replay");
    assert!(startup_recovery < durable_replay);

    let frozen_clock = production
        .find("let recovery_started_at = Local::now().naive_local()")
        .expect("one frozen restart clock");
    let load_bars = production
        .find("load_unique_bars(&args.bar_log")
        .expect("durable bar load");
    let validate_bars = production
        .find("validate_recovery_bars_not_future(bars.values(), recovery_started_at)")
        .expect("future durable bar gate");
    let replay_raw = production
        .find("let recovered_completed_bars = replay_market_messages(")
        .expect("future raw-event replay gate");
    let open_ledger = production
        .find("let mut store = SqliteStore::open(&config.database)")
        .expect("ledger open");
    assert!(
        frozen_clock < load_bars
            && load_bars < validate_bars
            && validate_bars < replay_raw
            && replay_raw < open_ledger,
        "all restart evidence must pass the frozen-clock gate before the ledger can be opened"
    );

    let live_event_gate = live
        .find("market_timestamp_not_future(receipt.event_timestamp_us, now")
        .expect("future live-event gate");
    assert!(receive_now < live_event_gate && live_event_gate < current);
}

#[test]
fn startup_durable_bar_replay_never_applies_non_executable_session_boundaries() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let replay = source
        .split_once("for bar in bars.values() {")
        .expect("startup durable-bar replay")
        .1
        .split_once("for bar in recovered_completed_bars {")
        .expect("startup reconstructed-bar replay")
        .0;

    assert!(
        replay.contains("if is_executable_strategy_bar(bar)"),
        "the durable bar log may contain the 11:30/15:00 completion boundary, which must not be sent to the strategy ledger"
    );
    assert_eq!(replay.matches("service.on_bar(bar)?").count(), 1);
}

#[test]
fn rolling_upgrade_uses_a_fresh_contiguous_completion_until_source_observations_arrive() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let gate = source
        .split_once("let completion_gate = receipt")
        .expect("completion liveness gate")
        .1
        .split_once("if let Some((")
        .expect("completion liveness gate end")
        .0;

    assert!(gate.contains("completion.covered_through_us"));
    assert!(gate.contains("completion.previous_covered_through_us"));
    assert!(gate.contains("market_watermark_is_current("));
    assert!(gate.contains("market_watermarks_are_contiguous("));
    assert!(gate.contains("source_observation_gate"));
    assert!(
        gate.contains(".map(|(observation, current, previous_current)|"),
        "a present source observation must remain the preferred liveness evidence"
    );
}

#[test]
fn rolling_upgrade_silence_prefers_observation_and_temporarily_falls_back_to_completion() {
    assert_eq!(
        live_subject::preferred_liveness(None, Some(59)),
        Some(59),
        "before the first source observation, the committed completion is the liveness clock"
    );
    assert_eq!(
        live_subject::preferred_liveness(Some(41), Some(59)),
        Some(41),
        "once an observation exists, a newer completion must never mask a stale observation"
    );
    assert_eq!(live_subject::preferred_liveness(None, None), None);
}

#[test]
fn rolling_upgrade_liveness_uses_only_the_received_clock_of_a_fully_reviewed_live_completion(
) -> Result<()> {
    let to_us = |timestamp: NaiveDateTime| {
        u64::try_from(
            (timestamp - chrono::Duration::hours(8))
                .and_utc()
                .timestamp_micros(),
        )
        .expect("positive reviewed timestamp")
    };
    let coverage = to_us(at("2026-08-26 14:57:00"));
    let source_received = to_us(at("2026-08-26 14:57:39"));

    let reviewed = live_subject::update_reviewed_completion_liveness(
        None,
        true,
        true,
        true,
        true,
        1,
        true,
        source_received,
    );
    assert_eq!(reviewed, Some(source_received));
    assert_ne!(reviewed, Some(coverage));
    assert!(!live_subject::source_observation_timeout_decision(
        gridedge_t::domain::ServiceMode::Running,
        live_subject::preferred_liveness(None, reviewed),
        at("2026-08-26 14:58:06"),
        60,
    )?);
    assert!(live_subject::source_observation_timeout_decision(
        gridedge_t::domain::ServiceMode::Running,
        live_subject::preferred_liveness(None, reviewed),
        at("2026-08-26 14:58:40"),
        60,
    )?);

    for invalid in [
        (false, true, true, true, 1, true),
        (true, false, true, true, 1, true),
        (true, true, false, true, 1, true),
        (true, true, true, false, 1, true),
        (true, true, true, true, 2, true),
        (true, true, true, true, 1, false),
    ] {
        assert_eq!(
            live_subject::update_reviewed_completion_liveness(
                Some(7),
                invalid.0,
                invalid.1,
                invalid.2,
                invalid.3,
                invalid.4,
                invalid.5,
                source_received,
            ),
            Some(7),
            "history, stale, gapped, cross-day, multi-bar, or stale-bar receipts must not refresh liveness",
        );
    }
    Ok(())
}

#[test]
fn recovery_rechecks_liveness_after_the_terminal_ui_audit_before_resuming() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let resume = source
        .split_once("fn resume_after_market_data_recovery(")
        .expect("recovery resume function")
        .1
        .split_once("#[cfg(test)]")
        .expect("recovery resume function end")
        .0;
    let audit = resume
        .find("reconcile_terminal_boundary(")
        .expect("terminal UI audit");
    let fresh_now = resume[audit..]
        .find("Local::now().naive_local()")
        .expect("fresh clock after terminal UI audit")
        + audit;
    let liveness = resume[fresh_now..]
        .find("market_watermark_is_current(")
        .expect("post-audit liveness recheck")
        + fresh_now;
    let durable_resume = resume
        .find("service.reconcile()")
        .expect("durable reconciliation before resume");
    assert!(audit < fresh_now && fresh_now < liveness && liveness < durable_resume);
}

#[test]
fn recovery_watermark_rejects_stale_and_future_completion() -> Result<()> {
    let now = at("2026-08-21 13:30:00");
    let to_us = |timestamp: NaiveDateTime| {
        u64::try_from(
            (timestamp - chrono::Duration::hours(8))
                .and_utc()
                .timestamp_micros(),
        )
        .expect("positive reviewed timestamp")
    };

    assert!(live_subject::watermark_is_current(
        Some(to_us(at("2026-08-21 13:29:00"))),
        now,
        60,
    )?);
    assert!(!live_subject::watermark_is_current(
        Some(to_us(at("2026-08-21 13:28:59"))),
        now,
        60,
    )?);
    assert!(!live_subject::watermark_is_current(None, now, 60)?);
    assert!(
        live_subject::watermark_is_current(Some(to_us(at("2026-08-21 13:30:01"))), now, 60,)
            .is_err()
    );
    assert!(live_subject::bar_is_current(
        at("2026-08-21 13:29:00"),
        now,
        60,
    )?);
    assert!(!live_subject::bar_is_current(
        at("2026-08-21 13:05:00"),
        now,
        60,
    )?);
    assert!(live_subject::bar_is_current(at("2026-08-21 13:30:01"), now, 60,).is_err());

    assert!(live_subject::watermarks_are_contiguous(
        Some(to_us(at("2026-08-21 13:29:00"))),
        to_us(at("2026-08-21 13:29:30")),
        60,
    )?);
    assert!(
        !live_subject::watermarks_are_contiguous(
            Some(to_us(at("2026-08-21 13:28:00"))),
            to_us(at("2026-08-21 13:29:30")),
            60,
        )?,
        "a real source-watermark gap must enter recovery"
    );
    assert!(
        live_subject::watermarks_are_contiguous(None, to_us(at("2026-08-21 13:29:30")), 60,)
            .is_ok_and(|contiguous| !contiguous)
    );
    assert!(live_subject::watermarks_are_contiguous(
        Some(to_us(at("2026-08-21 13:30:00"))),
        to_us(at("2026-08-21 13:29:30")),
        60,
    )
    .is_err());
    Ok(())
}

#[test]
fn ui_delay_does_not_reclassify_a_contiguous_previous_source_watermark_as_stale() -> Result<()> {
    let now_after_ui = at("2026-08-21 14:36:18");
    let previous = at("2026-08-21 14:35:00");
    let current = at("2026-08-21 14:35:30");
    let to_us = |timestamp: NaiveDateTime| {
        u64::try_from(
            (timestamp - chrono::Duration::hours(8))
                .and_utc()
                .timestamp_micros(),
        )
        .unwrap()
    };

    assert!(live_subject::watermark_is_current(
        Some(to_us(current)),
        now_after_ui,
        60,
    )?);
    assert!(!live_subject::watermark_is_current(
        Some(to_us(previous)),
        now_after_ui,
        60,
    )?);
    assert!(live_subject::watermarks_are_contiguous(
        Some(to_us(previous)),
        to_us(current),
        60,
    )?);

    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")?;
    let production = source
        .split_once("fn run() -> Result<()> {")
        .expect("production run")
        .1
        .split_once("fn probe_remote_orders")
        .expect("production run end")
        .0;
    assert!(production.contains("market_watermarks_are_contiguous("));
    assert!(!production.contains(
        "market_watermark_is_current(\n                completion.previous_covered_through_us"
    ));
    Ok(())
}

#[test]
fn restart_replay_rejects_future_raw_evidence_and_future_durable_bars_before_use() -> Result<()> {
    const TRADE_TOPIC: &str = "gridedge/market/v1/XSHE/002256/trade";
    const STATUS_TOPIC: &str = "gridedge/market/v1/XSHE/002256/status";
    let frozen_start = at("2026-08-21 14:00:00");

    let future_trade = market_event(
        1,
        "TRADE_TICK",
        "2026-08-22 09:31:00",
        json!({
            "price": {"mantissa": 333, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "future-trade",
            "source_page": 1
        }),
    );
    assert!(live_subject::replay_market_messages_at(
        vec![(TRADE_TOPIC, future_trade)],
        frozen_start,
    )
    .is_err());

    let future_status = market_event(
        1,
        "SOURCE_STATUS",
        "2026-08-22 09:35:01",
        json!({
            "status": "LIVE_CONTIGUOUS",
            "session_date": "2026-08-22",
            "covered_through_us": market_timestamp_us("2026-08-22 09:35:01"),
            "capture_sha256": "c".repeat(64)
        }),
    );
    assert!(live_subject::replay_market_messages_at(
        vec![(STATUS_TOPIC, future_status)],
        frozen_start,
    )
    .is_err());

    let future_bar = MarketBar {
        timestamp: at("2026-08-22 09:35:00"),
        symbol: "002256.SZ".to_owned(),
        open: Decimal::new(333, 2),
        high: Decimal::new(333, 2),
        low: Decimal::new(333, 2),
        close: Decimal::new(333, 2),
        volume: 100,
        amount: Some(Decimal::new(33300, 2)),
    };
    assert!(live_subject::validate_recovery_bars_at(&[future_bar], frozen_start).is_err());
    Ok(())
}

#[test]
fn source_observation_chain_controls_availability_without_claiming_trade_coverage() {
    use gridedge_t::domain::ServiceMode;

    assert_eq!(
        live_subject::source_observation_recovery_decision(ServiceMode::Running, true, true, true,),
        (false, false),
    );
    assert_eq!(
        live_subject::source_observation_recovery_decision(ServiceMode::Running, true, false, true,),
        (true, false),
    );
    assert_eq!(
        live_subject::source_observation_recovery_decision(ServiceMode::ReadOnly, true, true, true,),
        (false, true),
    );
    assert_eq!(
        live_subject::source_observation_recovery_decision(
            ServiceMode::ReadOnly,
            false,
            true,
            true,
        ),
        (false, false),
    );
}

#[test]
fn mqtt_silence_moves_running_to_recovery_after_the_source_observation_deadline() -> Result<()> {
    use gridedge_t::domain::ServiceMode;
    let now = at("2026-08-26 13:10:01");
    let to_us = |timestamp: NaiveDateTime| {
        u64::try_from(
            (timestamp - chrono::Duration::hours(8))
                .and_utc()
                .timestamp_micros(),
        )
        .unwrap()
    };
    assert!(!live_subject::source_observation_timeout_decision(
        ServiceMode::Running,
        Some(to_us(at("2026-08-26 13:09:30"))),
        now,
        60,
    )?);
    assert!(live_subject::source_observation_timeout_decision(
        ServiceMode::Running,
        Some(to_us(at("2026-08-26 13:09:00"))),
        now,
        60,
    )?);
    assert!(live_subject::source_observation_timeout_decision(
        ServiceMode::Running,
        None,
        now,
        60,
    )?);
    assert!(!live_subject::source_observation_timeout_decision(
        ServiceMode::ReadOnly,
        None,
        now,
        60,
    )?);
    Ok(())
}

#[test]
fn completion_receipts_choose_read_only_catchup_before_any_live_execution() {
    use gridedge_t::domain::ServiceMode;

    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, true, true, true, 1, true),
        (false, false),
        "one current live bar remains executable"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, false, false, true, 1, true),
        (true, false),
        "a stale completion enters recovery and cannot resume"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, true, true, true, 2, true),
        (true, false),
        "a multi-bucket release remains read-only until a later contiguous live receipt"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::ReadOnly, true, true, false, true, 0, true),
        (false, false),
        "a current receipt cannot resume until its previous watermark is also current"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::ReadOnly, true, true, true, true, 0, true),
        (false, true),
        "a second contiguous current live receipt releases startup recovery once"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::ReadOnly, false, true, false, true, 0, true),
        (false, false),
        "history completion alone never releases startup recovery"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, true, true, false, 1, true),
        (true, false),
        "a prior-session receipt remains read-only"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, false, true, true, true, 1, true),
        (true, false),
        "history completion always enters recovery before its bar"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, true, false, true, 1, true),
        (true, false),
        "a stale previous watermark remains read-only through the whole catch-up episode"
    );
    assert_eq!(
        live_subject::recovery_decision(ServiceMode::Running, true, true, true, true, 1, false),
        (true, false),
        "one stale released bar cannot immediately resume after read-only processing"
    );
}

#[test]
fn partial_session_resume_requires_a_completed_post_boundary_bucket_and_cli_authority() {
    assert!(!live_subject::session_gate(false, false, false));
    assert!(live_subject::session_gate(true, false, false));
    assert!(!live_subject::session_gate(false, true, false));
    assert!(live_subject::session_gate(false, true, true));

    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs").unwrap();
    let plist = std::fs::read_to_string("deploy/com.gridedge.ths-sim.plist").unwrap();
    assert!(source.contains("allow_partial_session_resume_boundary"));
    assert!(source.contains("builder.partial_session_resume_is_safe(date)"));
    assert!(plist.contains("--allow-partial-session-resume-boundary"));
}

#[test]
fn platform_upgrade_activation_exits_before_market_or_outbox_initialization() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let production = source
        .split_once("fn run() -> Result<()> {")
        .expect("production run")
        .1
        .split_once("fn probe_remote_orders")
        .expect("production run end")
        .0;
    let activation = production
        .find("activate_authorized_platform_upgrade(&args, &config)?")
        .expect("dedicated activation-only path");
    let activation_return = production[activation..]
        .find("return Ok(())")
        .map(|offset| activation + offset)
        .expect("activation-only early return");
    for later_initialization in [
        "WebTradeBarBuilder::new",
        "load_market_messages(",
        "MarketMqttClient::connect(",
        "ThsSimOutbox::open(",
        "MacOsSimulationUiDriver::default()",
    ] {
        let index = production
            .find(later_initialization)
            .unwrap_or_else(|| panic!("missing production initialization {later_initialization}"));
        assert!(
            activation_return < index,
            "activation-only must return before {later_initialization}"
        );
    }
}

#[test]
fn live_completed_bars_use_one_permit_path_and_recovery_bars_never_touch_ui() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let helper = source
        .split_once("fn reconcile_and_process_bar(")
        .expect("completed-bar reconciliation helper")
        .1
        .split_once("fn reconcile_terminal_boundary(")
        .expect("completed-bar helper end")
        .0;
    let probe = helper
        .find("let mut remote_records = probe_remote_orders(driver)?;")
        .expect("one initial immutable orders snapshot");
    let precheck = helper
        .find("verify_remote_contracts(&remote_records, &args.outbox, false)")
        .expect("unknown-contract precheck");
    let outbox = helper
        .find("run_outbox(")
        .expect("durable outbox reconciliation");
    let conditional_reprobe = outbox
        + helper[outbox..]
            .find("remote_records = probe_remote_orders(driver)?;")
            .expect("post-action orders snapshot");
    let permit = helper
        .find("remote_terminal_permit(&remote_records")
        .expect("strict terminal permit");
    let process = helper
        .find("process_bar(args, bars, service, bar, &permit)?")
        .expect("permit-bound bar processing");
    assert!(probe < precheck);
    assert!(precheck < outbox && outbox < conditional_reprobe && conditional_reprobe < permit);
    assert!(permit < process);
    let conditional = &helper[outbox..permit];
    assert!(conditional.contains(")? {\n        remote_records = probe_remote_orders(driver)?;"));
    assert_eq!(helper.matches("probe_remote_orders(driver)?").count(), 2);

    let process_body = source
        .split_once("fn process_bar(")
        .expect("bar processing helper")
        .1
        .split_once("fn run_outbox(")
        .expect("bar processing helper end")
        .0;
    let store = process_body
        .find("store_bar_if_new(")
        .expect("append-only completed bar");
    let service = process_body
        .find("service.on_bar(&bar)")
        .expect("core service application");
    assert!(store < service);

    let recovered = source
        .split_once("for bar in recovered_completed_bars {")
        .expect("reconstructed completed bars")
        .1
        .split_once("loop {")
        .expect("reconstructed completed-bar loop end")
        .0;
    assert_eq!(recovered.matches("process_market_recovery_bar(").count(), 1);
    assert!(!recovered.contains("reconcile_and_process_bar("));
    assert!(!recovered.contains("probe_remote_orders("));
}

#[test]
fn one_live_reconciliation_boundary_reuses_one_immutable_orders_snapshot() {
    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let helper = source
        .split_once("fn probe_remote_orders(")
        .expect("remote orders helper")
        .1
        .split_once("fn verify_remote_contracts(")
        .expect("remote orders helper end")
        .0;
    let full_orders_probes = helper.matches("driver.orders()?.records()").count();
    assert_eq!(
        full_orders_probes, 1,
        "the same immutable orders-table snapshot must drive unknown-contract audit and the \
         terminal permit; two full probes make the 5-second quote loop exceed its budget"
    );
}

#[test]
fn lunch_and_daily_cutoffs_reserve_a_full_five_minutes_for_final_orders() {
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 11:24:59")),
        (true, false, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 11:25:00")),
        (true, true, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 12:59:59")),
        (false, true, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 13:00:00")),
        (true, false, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 14:54:59")),
        (true, false, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 14:55:00")),
        (true, false, false)
    );
    assert_eq!(
        live_subject::settlement_windows(at("2026-08-18 15:05:00")),
        (false, false, true)
    );

    let source = std::fs::read_to_string("src/bin/gridedge_ths_live.rs")
        .expect("reviewed deployment orchestrator source");
    let day = source
        .find("if is_after_daily_window(Local::now().naive_local())")
        .unwrap();
    let receive = source.find("let Some(message) = market.receive").unwrap();
    assert!(day < receive);
    let day_branch = &source[day..receive];
    assert!(day_branch.contains("return Ok(())"));
    assert!(source.contains("fn is_executable_strategy_bar"));
    assert!(source.contains("(at(9, 35)..=at(11, 25)).contains(&time)"));
    assert!(source.contains("(at(13, 5)..=at(14, 55)).contains(&time)"));
}

fn remote_order(
    contract_id: &str,
    remark: &str,
    cancelled_quantity: i64,
    fill_price: Option<&str>,
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
        order_price: Decimal::new(355, 2),
        fill_price: fill_price.map(str::to_owned),
        contract_id: contract_id.to_owned(),
        order_attribute: "0".to_owned(),
    }
}

fn market_event(
    sequence: u64,
    event_type: &str,
    timestamp: &str,
    payload: serde_json::Value,
) -> Vec<u8> {
    let timestamp_us = market_timestamp_us(timestamp);
    let mut event = json!({
        "spec": "gridedge.market",
        "schema_version": 1,
        "event_type": event_type,
        "source": {
            "source_id": "eastmoney-web-time-sales",
            "source_instance_id": "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
            "source_type": "WEB_UI",
            "provider": "eastmoney",
            "provider_version": "eastmoney-time-sales-dom-v6"
        },
        "instrument": {
            "venue": "XSHE",
            "symbol": "002256",
            "asset_class": "EQUITY",
            "currency": "CNY"
        },
        "source_sequence": sequence,
        "ts_us": timestamp_us,
        "recv_us": timestamp_us + 1_000_000,
        "payload": payload,
        "evidence_sha256": "e".repeat(64)
    });
    event["payload"]["source_captured_at_us"] = json!(timestamp_us + 1_000_000);
    let mut identity = event.clone();
    identity.as_object_mut().unwrap().remove("recv_us");
    event["event_id"] = serde_json::Value::String(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&identity).unwrap())
    ));
    serde_json::to_vec(&event).unwrap()
}

fn market_timestamp_us(timestamp: &str) -> u64 {
    let datetime = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap();
    u64::try_from(
        (datetime - chrono::Duration::hours(8))
            .and_utc()
            .timestamp_micros(),
    )
    .unwrap()
}

fn remote_order_with_quantity_and_price(
    contract_id: &str,
    remark: &str,
    quantity: i64,
    order_price: &str,
    fill_price: &str,
) -> SimulationOrderRecord {
    SimulationOrderRecord {
        order_date: "20260818".to_owned(),
        order_time: "09:31:00".to_owned(),
        symbol: "002256".to_owned(),
        security_name: "兆新股份".to_owned(),
        direction: Direction::Buy,
        remark: remark.to_owned(),
        quantity,
        cancelled_quantity: 0,
        order_price: order_price.parse().unwrap(),
        fill_price: Some(fill_price.to_owned()),
        contract_id: contract_id.to_owned(),
        order_attribute: "0".to_owned(),
    }
}

fn default_symbol_filled_order(
    contract_id: &str,
    quantity: i64,
    order_price: Decimal,
    fill_price: Decimal,
) -> SimulationOrderRecord {
    SimulationOrderRecord {
        order_date: "20260818".to_owned(),
        order_time: "09:36:00".to_owned(),
        symbol: "600000".to_owned(),
        security_name: "浦发银行".to_owned(),
        direction: Direction::Buy,
        remark: "全部成交".to_owned(),
        quantity,
        cancelled_quantity: 0,
        order_price,
        fill_price: Some(fill_price.to_string()),
        contract_id: contract_id.to_owned(),
        order_attribute: "限价委托".to_owned(),
    }
}

fn remote_fill(
    fill_id: &str,
    contract_id: &str,
    quantity: i64,
    price: &str,
) -> SimulationFillRecord {
    let price = price.parse::<Decimal>().unwrap();
    SimulationFillRecord {
        fill_date: "20260819".to_owned(),
        fill_time: "104000".to_owned(),
        symbol: "600000".to_owned(),
        security_name: "浦发银行".to_owned(),
        direction: Direction::Buy,
        quantity,
        price,
        amount: price * Decimal::from(quantity),
        contract_id: contract_id.to_owned(),
        fill_id: fill_id.to_owned(),
        fill_category: "普通成交".to_owned(),
    }
}

fn source_bar(timestamp: &str, open: &str, high: &str, low: &str, close: &str) -> MarketBar {
    MarketBar {
        timestamp: at(timestamp),
        symbol: "600000.SH".to_owned(),
        open: open.parse().unwrap(),
        high: high.parse().unwrap(),
        low: low.parse().unwrap(),
        close: close.parse().unwrap(),
        volume: 0,
        amount: None,
    }
}

fn market_quote(
    observed_from: &str,
    observed_at: &str,
    price: &str,
    hash_character: char,
) -> SimulationMarketQuote {
    SimulationMarketQuote {
        observed_from: at(observed_from),
        observed_at: at(observed_at),
        symbol: "002256".to_owned(),
        last_price: price.parse().unwrap(),
        source_sha256: hash_character.to_string().repeat(64),
        application_bundle: "cn.com.10jqka.macstockPro".to_owned(),
        application_version: "5.3.2".to_owned(),
        account_marker: "模拟练习".to_owned(),
    }
}

fn intent_event(sequence: i64, intent_id: &str, order_id: &str) -> EventEnvelope {
    intent_event_with_quantity(sequence, intent_id, order_id, 100)
}

fn intent_event_with_quantity(
    sequence: i64,
    intent_id: &str,
    order_id: &str,
    quantity: i64,
) -> EventEnvelope {
    let time = at("2026-08-18 09:35:00");
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
            created_at: time,
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
        time,
    )
}

fn submitted_event(sequence: i64, order_id: &str) -> EventEnvelope {
    envelope(
        sequence,
        EventType::OrderSubmitted,
        json!({"order_id": order_id}),
        at("2026-08-18 09:35:00"),
    )
}

fn envelope(
    sequence: i64,
    event_type: EventType,
    payload: serde_json::Value,
    time: NaiveDateTime,
) -> EventEnvelope {
    EventEnvelope {
        event_id: format!("event-{sequence}"),
        event_type,
        schema_version: current_event_schema(event_type),
        run_id: "run-a".to_owned(),
        cycle_id: "cycle-a".to_owned(),
        symbol: "002256.SZ".to_owned(),
        event_time: time,
        recorded_at: time,
        sequence_number: sequence,
        correlation_id: format!("correlation-{sequence}"),
        causation_id: None,
        idempotency_key: format!("key-{sequence}"),
        payload,
        config_version: "test".to_owned(),
    }
}

fn at(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}
