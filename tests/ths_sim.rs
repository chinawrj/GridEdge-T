use anyhow::Result;
use chrono::NaiveDateTime;
use gridedge_t::{
    domain::Direction,
    ths_sim::{
        cancel_contract_with, click_prepared_form_with, confirm_prepared_form_with, dry_run,
        parse_fills_probe, parse_orders_probe, parse_probe, prepare_with, probe_fills_with,
        probe_market_quote_with, probe_orders_with, probe_with, submit_prepared_form_with,
        AppleScriptExecutor, SimulatedOrderDraft, SimulationOrderTable, SimulationOrderTableRow,
        TESTED_THS_VERSION, THS_BUNDLE_ID,
    },
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::{cell::Cell, cell::RefCell, collections::VecDeque};

const SIMULATION_PROBE: &str = "\
BUNDLE\tcn.com.10jqka.macstockPro\n\
VERSION\t5.3.2\n\
SUBMIT\t确定买入\n\
FIELD\t323\t225\t89\t20\t3.550\n\
FIELD\t304\t179\t138\t20\t002256\n\
FIELD\t323\t285\t89\t20\t1500\n\
MARKER\t1\n\
LIVE_TRANSFER\t0\n\
LIVE_EXIT\t0\n\
LIVE_ACCOUNT_SETTINGS\t0\n\
BUY_BUTTON\t1\n\
SELL_BUTTON\t1\n";

#[test]
fn simulation_probe_is_version_locked_and_maps_fields_by_geometry() -> Result<()> {
    let ui = parse_probe(SIMULATION_PROBE)?.verify_simulation()?;

    assert_eq!(ui.bundle_id, THS_BUNDLE_ID);
    assert_eq!(ui.version, TESTED_THS_VERSION);
    assert_eq!(ui.active_direction, Direction::Buy);
    assert_eq!(ui.code_field.value, "002256");
    assert_eq!(ui.price_field.value, "3.550");
    assert_eq!(ui.quantity_field.value, "1500");
    assert_eq!(ui.submit_label, "确定买入");
    Ok(())
}

#[test]
fn market_quote_refresh_changes_only_the_symbol_field_and_re_reads_the_simulation_page(
) -> Result<()> {
    let empty_limit_probe = simulation_probe_with_limit_value("");
    let executor =
        RecordingExecutor::new(["QUOTE_REFRESHED\t3.530\n".to_owned(), empty_limit_probe]);
    let observed_at = NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?;

    let quote = probe_market_quote_with(&executor, "002256", observed_at)?;

    assert_eq!(quote.observed_at, observed_at);
    assert_eq!(quote.symbol, "002256");
    assert_eq!(quote.last_price, Decimal::from_str("3.530")?);
    assert_eq!(quote.source_sha256.len(), 64);
    assert_eq!(quote.application_bundle, THS_BUNDLE_ID);
    assert_eq!(quote.application_version, TESTED_THS_VERSION);
    assert_eq!(quote.account_marker, "模拟练习");
    let scripts = executor.scripts.borrow();
    assert_eq!(
        scripts.len(),
        2,
        "refresh already performs the complete preflight; only one final probe may follow"
    );
    let refresh = &scripts[0];
    assert_windows_are_materialized_before_indexing(refresh);
    assert_quote_window_snapshot_has_no_filtered_or_live_collection_specifier(refresh);
    assert_text_identity_1700_fallback_is_narrow(refresh);
    assert_quote_page_has_independent_strict_live_account_negative_proof(refresh);
    assert_quote_refresh_reacquires_every_ax_reference_after_symbol_commit(refresh);
    assert_quote_window_snapshot_has_no_filtered_or_live_collection_specifier(&scripts[1]);
    assert_text_identity_1700_fallback_is_narrow(&scripts[1]);
    assert_quote_page_has_independent_strict_live_account_negative_proof(&scripts[1]);
    assert!(refresh.contains(&format!("bundle identifier is not \"{THS_BUNDLE_ID}\"")));
    assert!(refresh.contains(&format!(
        "version of application file is not \"{TESTED_THS_VERSION}\""
    )));
    assert!(refresh.contains("if matchedWindowCount is not 1"));
    assert!(refresh.contains("live transfer control is visible"));
    assert!(refresh.contains("live account exit control is visible"));
    assert!(refresh.contains("live account settings are visible"));
    assert!(refresh.contains("if (count of fields) is not 3"));
    let set_code = refresh
        .find("set value of codeField to \"002256\"")
        .expect("unique code field write");
    let focus_code = refresh
        .find("set focused of codeField to true")
        .expect("code field must receive focus before committing its edit");
    let commit_edit = refresh
        .find("key code 48")
        .or_else(|| refresh.find("perform action \"AXConfirm\" of codeField"))
        .expect("code edit must be committed with Tab or an equivalent AX confirmation");
    let bounded_wait = refresh
        .find("repeat with quoteRefreshAttempt from 1 to 20")
        .expect("quote refresh needs a bounded evidence wait");
    let acknowledge = refresh
        .find("return \"QUOTE_REFRESHED\" & tab & refreshedLastPrice")
        .expect("quote refresh must return the read-only market price evidence");
    assert!(set_code < focus_code && focus_code < commit_edit);
    assert!(commit_edit < bounded_wait && bounded_wait < acknowledge);
    assert!(refresh.contains("set quoteRefreshReady to false"));
    assert!(refresh.contains("set observedSymbol to value of codeField as text"));
    assert!(refresh.contains("set linkedSecurityName"));
    assert!(refresh.contains("linkedSecurityName is not \"\""));
    let wait = &refresh[bounded_wait..acknowledge];
    assert!(wait.contains("set frozenQuoteStaticTexts to {}"));
    assert!(wait.contains("set liveQuoteStaticTexts to get (static texts of targetWindow)"));
    assert!(wait.contains("repeat with candidateSecurityTextRef in liveQuoteStaticTexts"));
    assert!(
        wait.contains("set end of frozenQuoteStaticTexts to contents of candidateSecurityTextRef")
    );
    assert!(wait.contains("repeat with candidateSecurityTextRef in frozenQuoteStaticTexts"));
    assert!(!wait.contains("set observedStaticTextCount to count of static texts of targetWindow"));
    assert!(!wait.contains("item staticTextIndex of static texts of targetWindow"));
    assert!(wait.contains("set candidateSecurityAlive to false"));
    assert!(wait.contains("set candidateSecurityAlive to true"));
    assert!(wait.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(wait.contains("if candidateSecurityAlive then"));
    assert!(wait.contains("if (count of linkedSecurityNames) is 1"));
    assert!(wait.contains("限价委托"));
    assert!(!refresh.contains("set refreshedPrice to value of priceField as text"));
    assert!(!refresh.contains("set refreshedPriceNumber to refreshedPrice as real"));
    assert!(refresh.contains("if not quoteRefreshReady then error"));
    assert!(!refresh.contains("set value of priceField to"));
    assert!(!refresh.contains("set value of quantityField to"));
    assert!(!refresh.contains("click submitButton"));
    assert!(!refresh.contains("click confirmButton"));
    Ok(())
}

fn assert_quote_window_snapshot_has_no_filtered_or_live_collection_specifier(script: &str) {
    assert!(
        script.contains("set liveWindowObjects to get every window"),
        "quote evidence must materialize one unfiltered window list before inspecting roles"
    );
    assert!(
        !script.contains("every window whose subrole"),
        "a filtered System Events specifier can be re-resolved after window 3 disappears"
    );
    assert!(
        !script.contains("item staticTextIndex of static texts of"),
        "simulation-marker discovery must not index a live static-text collection"
    );
    for forbidden in [
        "set observedButtonCount to count of buttons of w",
        "set observedTextFieldCount to count of text fields of w",
    ] {
        assert!(
            !script.contains(forbidden),
            "quote evidence retained a live collection that can re-resolve a vanished window: {forbidden}"
        );
    }
}

fn assert_quote_refresh_reacquires_every_ax_reference_after_symbol_commit(script: &str) {
    let commit = script
        .find("key code 48")
        .expect("symbol edit must be committed");
    let wait = script
        .find("repeat with quoteRefreshAttempt from 1 to 20")
        .expect("bounded post-commit evidence loop");
    let acknowledge = script
        .find("return \"QUOTE_REFRESHED\" & tab & refreshedLastPrice")
        .expect("quote receipt");
    assert!(commit < wait && wait < acknowledge);
    let stale_gap = &script[commit..wait];
    for forbidden in [
        "position of codeField",
        "position of priceField",
        "value of codeField",
        "static texts of targetWindow",
    ] {
        assert!(
            !stale_gap.contains(forbidden),
            "the Tab commit invalidates every pre-commit AX reference: {forbidden}"
        );
    }

    let attempt = &script[wait..acknowledge];
    let windows = attempt
        .find("set attemptLiveWindows to get every window")
        .expect("each post-commit attempt must reacquire an unfiltered window snapshot");
    let texts = attempt
        .find("set attemptLiveStaticTexts to get (static texts of attemptWindow)")
        .expect("market labels must come from the newly acquired candidate window");
    let unique = attempt
        .find("if attemptMatchedWindowCount is not 1")
        .expect("each attempt must re-prove one simulation window");
    let fields = attempt
        .find("set attemptFields to {}")
        .expect("each attempt must rematerialize the three order fields");
    let positions = attempt
        .find("set codeFieldPosition to position of codeField")
        .expect("field geometry must come from the newly acquired fields");
    let symbol = attempt
        .find("set observedSymbol to value of codeField as text")
        .expect("symbol echo must come from the newly acquired code field");
    assert!(windows < texts && texts < unique && unique < fields);
    assert!(fields < positions && positions < symbol);
}

fn assert_text_identity_1700_fallback_is_narrow(script: &str) {
    let lines = script.lines().collect::<Vec<_>>();
    let mut fallback_count = 0;
    for (index, line) in lines.iter().enumerate() {
        if !line.contains("-1700") {
            continue;
        }
        fallback_count += 1;
        let context = lines[index.saturating_sub(24)..=index].join("\n");
        let reviewed_identity = [
            ("value of s as text", "role of s"),
            ("name of s as text", "role of s"),
            ("value of f as text", "role of f"),
            ("value of markerText as text", "role of markerText"),
            ("name of markerText as text", "role of markerText"),
            (
                "value of attemptStaticText as text",
                "role of attemptStaticText",
            ),
            (
                "name of attemptStaticText as text",
                "role of attemptStaticText",
            ),
            (
                "value of candidateSecurityText as text",
                "role of candidateSecurityText",
            ),
            (
                "name of candidateSecurityText as text",
                "role of candidateSecurityText",
            ),
        ]
        .iter()
        .find(|(operation, _)| context.contains(operation));
        assert!(
            reviewed_identity.is_some(),
            "-1700 may be tolerated only while coercing reviewed static/text-field identity; context:\n{context}"
        );
        let (_, role_proof) = reviewed_identity.unwrap();
        assert!(
            context.contains(role_proof),
            "a stale text may be skipped only after its AX role was successfully read; context:\n{context}"
        );
    }
    assert!(
        fallback_count > 0,
        "dynamic non-evidence AX text values need the reviewed -1700 fallback"
    );
}

fn assert_quote_page_has_independent_strict_live_account_negative_proof(script: &str) {
    for (control, diagnostic) in [
        (
            "if exists button \"转账\" of",
            "live transfer control is visible",
        ),
        (
            "if exists button \"退出\" of",
            "live account exit control is visible",
        ),
        (
            "if exists static text \"账户设置\" of",
            "live account settings are visible",
        ),
    ] {
        let proof = script
            .find(control)
            .unwrap_or_else(|| panic!("quote page lacks strict direct negative proof: {control}"));
        let failure = script[proof..]
            .find(diagnostic)
            .map(|offset| proof + offset)
            .expect("direct live-account proof must fail closed");
        assert!(proof < failure);
    }
}

#[test]
fn quote_price_comes_from_the_read_only_refresh_receipt_not_the_order_limit_field() -> Result<()> {
    let observed_at = NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?;

    let empty_limit = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.530\n".to_owned(),
        simulation_probe_with_limit_value(""),
    ]);
    let stale_limit = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.530\n".to_owned(),
        simulation_probe_with_limit_value("99.999"),
    ]);

    let empty_quote = probe_market_quote_with(&empty_limit, "002256", observed_at)?;
    let stale_quote = probe_market_quote_with(&stale_limit, "002256", observed_at)?;

    assert_eq!(empty_quote.last_price, Decimal::from_str("3.530")?);
    assert_eq!(stale_quote.last_price, Decimal::from_str("3.530")?);
    assert_eq!(empty_limit.scripts.borrow().len(), 2);
    assert_eq!(stale_limit.scripts.borrow().len(), 2);
    Ok(())
}

#[test]
fn quote_source_hash_binds_the_read_only_price_not_order_entry_residue() -> Result<()> {
    let observed_at = NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?;
    let empty_limit = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.530\n".to_owned(),
        simulation_probe_with_limit_value(""),
    ]);
    let stale_limit = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.530\n".to_owned(),
        simulation_probe_with_limit_value("99.999"),
    ]);
    let equivalent_decimal_scale = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.53\n".to_owned(),
        simulation_probe_with_limit_value(""),
    ]);
    let changed_market_price = RecordingExecutor::new([
        "QUOTE_REFRESHED\t3.540\n".to_owned(),
        simulation_probe_with_limit_value(""),
    ]);

    let empty_quote = probe_market_quote_with(&empty_limit, "002256", observed_at)?;
    let stale_quote = probe_market_quote_with(&stale_limit, "002256", observed_at)?;
    let equivalent_scale_quote =
        probe_market_quote_with(&equivalent_decimal_scale, "002256", observed_at)?;
    let changed_quote = probe_market_quote_with(&changed_market_price, "002256", observed_at)?;

    assert_ne!(
        empty_quote.source_sha256, changed_quote.source_sha256,
        "the durable evidence hash must bind the read-only market price"
    );
    assert_eq!(
        empty_quote.source_sha256, stale_quote.source_sha256,
        "irrelevant order-entry residue cannot change the durable market evidence identity"
    );
    assert_eq!(empty_quote.last_price, equivalent_scale_quote.last_price);
    assert_eq!(
        empty_quote.source_sha256, equivalent_scale_quote.source_sha256,
        "equivalent Decimal scales must have one canonical market evidence identity"
    );
    Ok(())
}

#[test]
fn malformed_or_ambiguous_read_only_quote_receipt_fails_before_the_final_probe() -> Result<()> {
    let observed_at = NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?;
    let invalid_receipts = [
        "QUOTE_REFRESHED\n",
        "QUOTE_REFRESHED\t3.530\t3.540\n",
        "QUOTE_REFRESHED\t0\n",
        "QUOTE_REFRESHED\t-3.530\n",
        "QUOTE_REFRESHED\tnot-a-decimal\n",
        "QUOTE_REFRESHED\t3.5301\n",
    ];

    for receipt in invalid_receipts {
        let executor = RecordingExecutor::new([
            receipt.to_owned(),
            simulation_probe_with_limit_value("88.888"),
        ]);
        let error = probe_market_quote_with(&executor, "002256", observed_at).unwrap_err();
        assert!(
            error.to_string().contains("quote") || error.to_string().contains("price"),
            "unexpected error for {receipt:?}: {error:#}"
        );
        assert_eq!(
            executor.scripts.borrow().len(),
            1,
            "an invalid or ambiguous read-only quote must fail before final page probing"
        );
    }
    Ok(())
}

fn simulation_probe_with_limit_value(value: &str) -> String {
    SIMULATION_PROBE.replacen(
        "FIELD\t323\t225\t89\t20\t3.550",
        &format!("FIELD\t323\t225\t89\t20\t{value}"),
        1,
    )
}

#[test]
fn quote_refresh_or_final_identity_failure_returns_no_quote_and_does_not_retry() -> Result<()> {
    let refresh_rejected = RecordingExecutor::new(["NOT_REFRESHED\n".to_owned()]);
    let error = probe_market_quote_with(
        &refresh_rejected,
        "002256",
        NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?,
    )
    .unwrap_err();
    assert!(error.to_string().contains("did not acknowledge"));
    assert_eq!(refresh_rejected.scripts.borrow().len(), 1);

    let live = SIMULATION_PROBE
        .replace("MARKER\t1", "MARKER\t0")
        .replace("LIVE_ACCOUNT_SETTINGS\t0", "LIVE_ACCOUNT_SETTINGS\t1");
    let final_identity_rejected =
        RecordingExecutor::new(["QUOTE_REFRESHED\t3.530\n".to_owned(), live]);
    let error = probe_market_quote_with(
        &final_identity_rejected,
        "002256",
        NaiveDateTime::parse_from_str("2026-08-18 09:31:02", "%Y-%m-%d %H:%M:%S")?,
    )
    .unwrap_err();
    assert!(error.to_string().contains("模拟练习"));
    assert_eq!(final_identity_rejected.scripts.borrow().len(), 2);
    Ok(())
}

#[test]
fn ignored_hardware_submit_cancel_gate_has_a_pre_side_effect_price_cushion() -> Result<()> {
    let source = std::fs::read_to_string("tests/ths_sim_macos.rs")?;
    let smoke = source
        .split_once("fn current_simulation_account_submits_reconciles_and_cancels_one_contract()")
        .map(|(_, suffix)| suffix)
        .and_then(|suffix| suffix.split_once("\n#[test]").map(|(body, _)| body))
        .ok_or_else(|| anyhow::anyhow!("missing reviewed submit-cancel hardware smoke"))?;

    assert!(smoke.contains("let smoke_limit_price = Decimal::new(310, 2);"));
    assert!(smoke.contains("let minimum_non_marketable_ratio = Decimal::new(105, 2);"));
    let quote = smoke
        .find("probe_current_market_quote(\"002256\")?")
        .expect("fresh simulation quote preflight");
    let cushion = smoke
        .find("observed_quote.last_price >= smoke_limit_price * minimum_non_marketable_ratio")
        .expect("5% non-marketable price cushion");
    let outbox = smoke.find("ThsSimOutbox::open").expect("durable outbox");
    let submit = smoke
        .find("execute_staged_intent(")
        .expect("unique hardware submission");
    let cancel = smoke
        .find("cancel_submitted_contract(")
        .expect("exact-contract hardware cancellation");
    assert!(quote < cushion && cushion < outbox && outbox < submit && submit < cancel);
    let before_cancel = &smoke[submit..cancel];
    assert!(before_cancel.contains("submitted.remote_order.fill_price.is_none()"));
    assert!(before_cancel.contains("submitted.remote_order.cancelled_quantity, 0"));
    Ok(())
}

#[test]
fn visible_simulation_tab_does_not_make_a_live_account_safe() -> Result<()> {
    let live = SIMULATION_PROBE
        .replace("MARKER\t1", "MARKER\t0")
        .replace("LIVE_TRANSFER\t0", "LIVE_TRANSFER\t1")
        .replace("LIVE_EXIT\t0", "LIVE_EXIT\t1")
        .replace("LIVE_ACCOUNT_SETTINGS\t0", "LIVE_ACCOUNT_SETTINGS\t1");

    let error = parse_probe(&live)?.verify_simulation().unwrap_err();
    assert!(error.to_string().contains("模拟练习"));
    Ok(())
}

#[test]
fn changed_version_or_ambiguous_layout_fails_closed() -> Result<()> {
    let changed_version = SIMULATION_PROBE.replace("VERSION\t5.3.2", "VERSION\t5.4.0");
    assert!(parse_probe(&changed_version)?
        .verify_simulation()
        .unwrap_err()
        .to_string()
        .contains("unsupported Tonghuashun version"));

    let extra_field =
        SIMULATION_PROBE.replace("MARKER\t1", "FIELD\t323\t345\t89\t20\textra\nMARKER\t1");
    assert!(parse_probe(&extra_field)?
        .verify_simulation()
        .unwrap_err()
        .to_string()
        .contains("exactly three order fields"));
    Ok(())
}

#[test]
fn order_draft_enforces_board_lot_price_and_notional_safety() -> Result<()> {
    let valid =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;
    assert_eq!(valid.quantity, 1500);

    assert!(SimulatedOrderDraft::new(Direction::Buy, "00225X", Decimal::ONE, 1500).is_err());
    assert!(SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::ONE, 1501).is_err());
    assert!(SimulatedOrderDraft::new(
        Direction::Buy,
        "002256",
        Decimal::from_str("100.00")?,
        1_000_000
    )
    .is_err());
    Ok(())
}

#[test]
fn dry_run_never_submits_and_requires_matching_direction() -> Result<()> {
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;
    let plan = dry_run(parse_probe(SIMULATION_PROBE)?, order.clone())?;
    assert!(!plan.would_submit);

    let sell = SIMULATION_PROBE.replace("SUBMIT\t确定买入", "SUBMIT\t确定卖出");
    assert!(dry_run(parse_probe(&sell)?, order).is_err());
    Ok(())
}

#[derive(Debug)]
struct FakeExecutor(&'static str);

impl AppleScriptExecutor for FakeExecutor {
    fn run(&self, _source: &str) -> Result<String> {
        Ok(self.0.to_owned())
    }
}

#[test]
fn executor_boundary_is_mockable_without_touching_macos_ui() -> Result<()> {
    let probe = probe_with(&FakeExecutor(SIMULATION_PROBE))?;
    assert_eq!(probe.simulation_marker_count, 1);
    Ok(())
}

fn assert_windows_are_materialized_before_indexing(script: &str) {
    assert!(script.contains("tell application \"同花顺\" to activate"));
    assert!(
        script.find("tell application \"同花顺\" to activate")
            < script.find("tell application \"System Events\"")
    );
    assert!(script.contains("set frozenWindows to {}"));
    assert!(script.contains("repeat with windowSnapshotAttempt from 1 to 3"));
    assert!(script.contains("set liveWindowObjects to get every window"));
    assert!(!script.contains("every window whose subrole"));
    assert!(script.contains("set liveWindowCount to count of liveWindowObjects"));
    assert!(script.contains("repeat with candidateWindowRef in liveWindowObjects"));
    assert!(script.contains("set end of frozenWindows to contents of candidateWindowRef"));
    assert!(script.contains("if not stableWindowSnapshot then error"));
    let indexed_frozen_snapshot = script
        .contains("set observedWindowCount to count of frozenWindows")
        && script.contains("set w to item windowIndex of frozenWindows")
        && script.contains("if wIsAlive then");
    let directly_iterated_frozen_snapshot = script
        .contains("repeat with candidateWindowRef in frozenWindows")
        && script.contains("set w to contents of candidateWindowRef")
        && script.contains("if subrole of w is \"AXStandardWindow\" then");
    assert!(indexed_frozen_snapshot || directly_iterated_frozen_snapshot);
    assert!(script.contains("set ignoredWindowRole to role of w"));
    assert!(!script.contains("if w is not missing value then"));
    assert!(!script.contains("set observedWindowCount to count of windows"));
    assert!(!script.contains("set candidateWindowRef to window liveWindowIndex"));
    assert!(!script.contains("a reference to (item liveWindowIndex of (every window"));
    assert!(!script.contains("repeat with candidateWindow in windows"));
    assert!(script.contains("on error errorMessage number errorNumber"));
    assert!(script.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    if indexed_frozen_snapshot {
        let atomic_indexed_snapshot = script.contains(
            "set ignoredWindowRole to role of w\n        set candidateWindowSubrole to subrole of w\n      on error errorMessage number errorNumber\n        error errorMessage number errorNumber\n      end try\n      set wIsAlive to candidateWindowSubrole is \"AXStandardWindow\""
        );
        let bounded_legacy_snapshot = script.contains(
            "set ignoredWindowRole to role of w\n        set candidateWindowSubrole to subrole of w\n        if candidateWindowSubrole is \"AXStandardWindow\" then set wIsAlive to true\n      on error errorMessage number errorNumber\n        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber\n      end try"
        );
        assert!(atomic_indexed_snapshot || bounded_legacy_snapshot);
    } else {
        assert!(script.contains("if subrole of w is \"AXStandardWindow\" then"));
        assert!(script.contains(
            "on error errorMessage number errorNumber\n        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber\n      end try"
        ));
    }
}

fn assert_no_live_ax_collection_specifiers(script: &str) {
    for forbidden in [
        "repeat with sa in scroll areas of",
        "repeat with candidateButton in buttons of",
        "repeat with candidateCheckbox in checkboxes of",
        "repeat with candidate in UI elements of",
        "repeat with child in UI elements of",
        "repeat with header in UI elements of",
        "item staticTextIndex of static texts of",
        "item rowIndex of rows of",
        "item windowIndex of windows",
        "item liveWindowIndex of (every window",
        "repeat with candidateRow in rows of",
        "repeat with candidateCell in static texts of",
        "repeat with candidateStaticText in static texts of",
    ] {
        assert!(
            !script.contains(forbidden),
            "money-action AppleScript retained a live AX collection specifier: {forbidden}"
        );
    }
}

fn assert_order_table_tree_is_materialized_before_iteration(script: &str) {
    for required in [
        "set frozenTargetButtons to {}",
        "set liveTargetButtons to get (buttons of targetWindow)",
        "set end of frozenTargetButtons to contents of candidateButtonRef",
        "repeat with candidateButtonRef in frozenTargetButtons",
        "set frozenFilterStaticTexts to {}",
        "set liveFilterStaticTexts to get (static texts of targetWindow)",
        "set end of frozenFilterStaticTexts to contents of candidateStaticTextRef",
        "repeat with candidateStaticTextRef in frozenFilterStaticTexts",
        "set frozenCheckboxes to {}",
        "set liveCheckboxes to get (checkboxes of targetWindow)",
        "set end of frozenCheckboxes to contents of candidateCheckboxRef",
        "repeat with candidateCheckboxRef in frozenCheckboxes",
        "set frozenScrollAreas to {}",
        "set liveScrollAreas to get (scroll areas of targetWindow)",
        "set end of frozenScrollAreas to contents of scrollAreaRef",
        "repeat with frozenScrollAreaRef in frozenScrollAreas",
        "set frozenScrollChildren to {}",
        "set liveScrollChildren to get (UI elements of sa)",
        "set end of frozenScrollChildren to contents of scrollChildRef",
        "repeat with frozenScrollChildRef in frozenScrollChildren",
        "set frozenTableChildren to {}",
        "set liveTableChildren to get (UI elements of candidate)",
        "set end of frozenTableChildren to contents of tableChildRef",
        "repeat with frozenTableChildRef in frozenTableChildren",
        "set frozenHeaderElements to {}",
        "set liveHeaderElements to get (UI elements of child)",
        "set end of frozenHeaderElements to contents of headerElementRef",
        "repeat with frozenHeaderElementRef in frozenHeaderElements",
        "set liveRows to get (rows of matchedTable)",
        "set end of tableRows to contents of liveRowRef",
        "set liveCells to get (entire contents of tableRow)",
        "set end of rowContents to contents of liveCellRef",
    ] {
        assert!(
            script.contains(required),
            "orders probe must freeze each AX collection before iteration: {required}"
        );
    }
    for forbidden in [
        "repeat with candidateButton in buttons of targetWindow",
        "repeat with candidateStaticText in static texts of targetWindow",
        "repeat with candidateCheckbox in checkboxes of targetWindow",
        "repeat with sa in scroll areas of targetWindow",
        "repeat with candidate in UI elements of sa",
        "repeat with child in UI elements of candidate",
        "repeat with header in UI elements of child",
        "repeat with tableRow in rows of matchedTable",
        "repeat with liveCellRef in entire contents of tableRow",
    ] {
        assert!(
            !script.contains(forbidden),
            "orders probe retained a lazy AX collection specifier: {forbidden}"
        );
    }
    assert!(script.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
}

fn assert_orders_probe_refinds_the_simulation_window_after_switching_tabs(script: &str) {
    let after_tab_switch = script
        .split_once("click commissionButton")
        .map(|(_, suffix)| suffix)
        .expect("orders probe must switch to its unique 委托 tab");
    let filter_read = after_tab_switch
        .find("set liveFilterStaticTexts to get (static texts of targetWindow)")
        .expect("filter evidence must use the post-tab target window");
    let refreshed_window_snapshot = after_tab_switch
        .find("set liveWindowObjects to get every window")
        .expect("tab switch must invalidate and reacquire the window snapshot");
    assert!(refreshed_window_snapshot < filter_read);
    let refreshed_proof = &after_tab_switch[refreshed_window_snapshot..filter_read];
    for required in [
        "set frozenWindows to {}",
        "set end of frozenWindows to contents of candidateWindowRef",
        "set ignoredWindowRole to role of w",
        "AXStandardWindow",
        "模拟练习",
        "if matchedWindowCount is 0 then error",
        "number -1719",
        "if matchedWindowCount is greater than 1 then error",
        "if exists button \"转账\" of targetWindow then error",
        "if exists button \"退出\" of targetWindow then error",
        "if exists static text \"账户设置\" of targetWindow then error",
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber",
    ] {
        assert!(
            refreshed_proof.contains(required),
            "post-tab window identity proof is incomplete: {required}"
        );
    }
}

fn assert_orders_probe_retries_the_entire_post_tab_table_snapshot(script: &str) {
    let after_tab_switch = script
        .split_once("click commissionButton")
        .map(|(_, suffix)| suffix)
        .expect("orders probe must switch to the 委托 tab");
    let attempt = after_tab_switch
        .find("repeat with orderSnapshotAttempt from 1 to 3")
        .expect("post-tab order evidence needs one bounded atomic retry boundary");
    let windows = after_tab_switch[attempt..]
        .find("set liveWindowObjects to get every window")
        .map(|offset| attempt + offset)
        .expect("every order attempt must start from a fresh unfiltered window list");
    let table_children = after_tab_switch[windows..]
        .find("set liveTableChildren to get (UI elements of candidate)")
        .map(|offset| windows + offset)
        .expect("table descendants must belong to the same atomic attempt");
    let rows = after_tab_switch[table_children..]
        .find("set liveRows to get (rows of matchedTable)")
        .map(|offset| table_children + offset)
        .expect("rows must belong to the same atomic attempt");
    let cells = after_tab_switch[rows..]
        .find("set liveCells to get (entire contents of tableRow)")
        .map(|offset| rows + offset)
        .expect("cells must belong to the same atomic attempt");
    let publish = after_tab_switch[cells..]
        .find("set out to attemptOut")
        .map(|offset| cells + offset)
        .expect("only a fully serialized order snapshot may be published");
    let transient_handler = after_tab_switch[publish..]
        .find("if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber")
        .map(|offset| publish + offset)
        .expect("stale descendants must retry while unknown errors retain their number");
    assert!(attempt < windows && windows < table_children);
    assert!(table_children < rows && rows < cells && cells < publish);
    assert!(publish < transient_handler);
    assert!(after_tab_switch.contains(
        "if not stableOrderSnapshot then error \"stable Tonghuashun order snapshot unavailable\""
    ));
}

fn assert_orders_probe_retries_a_transient_missing_post_tab_window(script: &str) {
    let after_tab_switch = script
        .split_once("click commissionButton")
        .map(|(_, suffix)| suffix)
        .expect("orders probe must switch to the 委托 tab");
    let attempt = after_tab_switch
        .find("repeat with orderSnapshotAttempt from 1 to 3")
        .expect("post-tab identity needs a bounded retry boundary");
    let attempt_body = &after_tab_switch[attempt..];

    assert!(
        attempt_body.contains("if matchedWindowCount is 0 then error")
            && attempt_body.contains("number -1719"),
        "a transient zero-window/zero-marker snapshot must discard the whole attempt and retry"
    );
    assert!(
        attempt_body.contains("if matchedWindowCount is greater than 1 then error"),
        "multiple simulation windows must remain immediately fatal"
    );
    assert!(attempt_body.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(attempt_body.contains(
        "if not stableOrderSnapshot then error \"stable Tonghuashun order snapshot unavailable\""
    ));
    assert!(
        !attempt_body.contains(
            "if matchedWindowCount is not 1 then error \"unique post-tab simulation order window not found\""
        ),
        "the old custom error bypasses all three bounded attempts when the rebuilt window is momentarily absent"
    );
}

#[test]
fn probe_raises_the_app_and_indexes_materialized_window_objects() -> Result<()> {
    let executor = RecordingExecutor::new([SIMULATION_PROBE.to_owned()]);

    probe_with(&executor)?;

    let scripts = executor.scripts.borrow();
    assert!(scripts[0].contains("set frontmost to true"));
    assert!(scripts[0].find("set frontmost to true") < scripts[0].find("set frozenWindows to {}"));
    assert_windows_are_materialized_before_indexing(&scripts[0]);
    assert!(scripts[0].contains("repeat with candidateWindowRef in frozenWindows"));
    assert!(scripts[0].contains("set frozenWindowStaticTexts to {}"));
    assert!(scripts[0].contains("repeat with candidateStaticTextRef in frozenWindowStaticTexts"));
    assert!(scripts[0].contains("set frozenWindowButtons to {}"));
    assert!(scripts[0].contains("repeat with candidateButtonRef in frozenWindowButtons"));
    assert!(scripts[0].contains("set frozenWindowTextFields to {}"));
    assert!(scripts[0].contains("repeat with candidateFieldRef in frozenWindowTextFields"));
    assert!(!scripts[0].contains("item staticTextIndex of static texts of"));
    assert!(!scripts[0].contains("set observedButtonCount to count of buttons of w"));
    assert!(!scripts[0].contains("item buttonIndex of buttons of w"));
    assert!(!scripts[0].contains("set observedTextFieldCount to count of text fields of w"));
    assert!(!scripts[0].contains("item textFieldIndex of text fields of w"));
    assert!(!scripts[0].contains("repeat with w in windows"));
    assert!(!scripts[0].contains("repeat with b in buttons of w"));
    assert!(!scripts[0].contains("repeat with f in text fields of w"));
    Ok(())
}

#[test]
fn final_probe_retries_an_atomic_snapshot_when_order_fields_temporarily_disappear() -> Result<()> {
    let executor = RecordingExecutor::new([SIMULATION_PROBE.to_owned()]);
    probe_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let script = &scripts[0];
    let attempt = script
        .split_once("repeat with windowSnapshotAttempt from 1 to 3")
        .map(|(_, suffix)| suffix)
        .expect("final probe must have a bounded atomic snapshot retry");
    let count_start = attempt
        .find("set attemptFieldCount to 0")
        .expect("each retry must own a fresh field counter");
    let field_append = attempt
        .find("set end of attemptOut to \"FIELD\\t\"")
        .expect("a readable field must be appended to the attempt-local receipt");
    let count_increment = attempt
        .find("set attemptFieldCount to attemptFieldCount + 1")
        .expect("only a fully readable field may contribute to the snapshot");
    let complete_guard = attempt
        .find("if attemptFieldCount is not 3 then error \"incomplete Tonghuashun order-field snapshot\" number -1719")
        .expect("zero/partial fields must retry inside AppleScript, not escape as partial evidence");
    let publish = attempt
        .find("set out to attemptOut")
        .expect("only one complete attempt may become the returned evidence");
    assert!(count_start < field_append);
    assert!(field_append < count_increment && count_increment < complete_guard);
    assert!(complete_guard < publish);
    assert!(attempt.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(!script.contains("确定买入\" of targetWindow"));
    assert!(!script.contains("确定卖出\" of targetWindow"));
    Ok(())
}

#[derive(Debug)]
struct RecordingExecutor {
    responses: RefCell<VecDeque<String>>,
    scripts: RefCell<Vec<String>>,
    native_double_clicks: RefCell<Vec<(i32, i32)>>,
}

impl RecordingExecutor {
    fn new(responses: impl IntoIterator<Item = String>) -> Self {
        Self {
            responses: RefCell::new(responses.into_iter().collect()),
            scripts: RefCell::new(Vec::new()),
            native_double_clicks: RefCell::new(Vec::new()),
        }
    }
}

impl AppleScriptExecutor for RecordingExecutor {
    fn run(&self, source: &str) -> Result<String> {
        self.scripts.borrow_mut().push(source.to_owned());
        self.responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("unexpected AppleScript call"))
    }

    fn native_double_click(&self, x: i32, y: i32) -> Result<()> {
        self.native_double_clicks.borrow_mut().push((x, y));
        Ok(())
    }
}

#[derive(Debug)]
struct RejectingMoneyWindowExecutor {
    responses: RefCell<VecDeque<String>>,
    scripts: RefCell<Vec<String>>,
    verification_calls: Cell<usize>,
    diagnostic: &'static str,
}

impl RejectingMoneyWindowExecutor {
    fn new(responses: impl IntoIterator<Item = String>, diagnostic: &'static str) -> Self {
        Self {
            responses: RefCell::new(responses.into_iter().collect()),
            scripts: RefCell::new(Vec::new()),
            verification_calls: Cell::new(0),
            diagnostic,
        }
    }
}

impl AppleScriptExecutor for RejectingMoneyWindowExecutor {
    fn run(&self, source: &str) -> Result<String> {
        self.scripts.borrow_mut().push(source.to_owned());
        self.responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("money-moving AppleScript ran after failed AX proof"))
    }

    fn verify_money_action_window(&self) -> Result<()> {
        self.verification_calls
            .set(self.verification_calls.get() + 1);
        anyhow::bail!(self.diagnostic)
    }
}

#[test]
fn native_ax_failure_stops_submit_confirm_and_cancel_before_any_money_action_script() -> Result<()>
{
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    let submit = RejectingMoneyWindowExecutor::new(
        [SIMULATION_PROBE.to_owned()],
        "expected one reviewed Tonghuashun simulation window, found 2",
    );
    let error = click_prepared_form_with(&submit, &order).unwrap_err();
    assert!(error.to_string().contains("found 2"));
    assert_eq!(submit.verification_calls.get(), 1);
    let submit_scripts = submit.scripts.borrow();
    assert_eq!(
        submit_scripts.len(),
        1,
        "only the read-only echo probe may run"
    );
    assert!(!submit_scripts
        .iter()
        .any(|script| script.contains("click submitButton")));
    drop(submit_scripts);

    for diagnostic in [
        "live-account controls are visible in the Tonghuashun simulation window: 账户设置",
        "accessibility attribute read failed with AXError -25204",
    ] {
        let confirm = RejectingMoneyWindowExecutor::new([], diagnostic);
        let error = confirm_prepared_form_with(&confirm, &order).unwrap_err();
        assert!(error.to_string().contains(diagnostic));
        assert_eq!(confirm.verification_calls.get(), 1);
        assert!(confirm.scripts.borrow().is_empty());

        let cancel = RejectingMoneyWindowExecutor::new([], diagnostic);
        let error = cancel_contract_with(&cancel, "6207077078").unwrap_err();
        assert!(error.to_string().contains(diagnostic));
        assert_eq!(cancel.verification_calls.get(), 1);
        assert!(cancel.scripts.borrow().is_empty());
    }
    Ok(())
}

#[test]
fn native_ax_money_action_verifier_recurses_and_fails_closed() -> Result<()> {
    let verifier = std::fs::read_to_string("src/ths_ax.rs")?;
    assert!(verifier.contains("copy_attribute(application.0, \"AXWindows\")"));
    assert!(verifier.contains("copy_attribute(element, \"AXChildren\")"));
    assert!(verifier.contains("inspect_element(child, depth + 1"));
    assert!(verifier.contains("role == \"AXStaticText\""));
    assert!(verifier.contains("Some(\"模拟练习\")"));
    for forbidden in ["账户设置", "转账", "退出"] {
        assert!(verifier.contains(forbidden));
    }
    assert!(verifier.contains("evidence.marker_window_count != 1"));
    assert!(verifier.contains("!evidence.unsafe_controls.is_empty()"));
    assert!(verifier.contains("MAX_AX_DEPTH"));
    assert!(verifier.contains("MAX_AX_NODES"));
    assert!(verifier.contains("AX_ERROR_ATTRIBUTE_UNSUPPORTED | AX_ERROR_NO_VALUE => Ok(None)"));
    assert!(verifier.contains("other => bail!(\"accessibility attribute read failed"));

    let action_boundary = std::fs::read_to_string("src/ths_sim.rs")?;
    assert!(action_boundary.contains(
        "crate::ths_ax::verify_reviewed_simulation_window()\n            .context(\"native Tonghuashun simulation-window verification failed\")?"
    ));
    for function in [
        "pub fn click_prepared_form_with(",
        "pub fn confirm_prepared_form_with(",
        "pub fn cancel_contract_with(",
    ] {
        let body = action_boundary
            .split_once(function)
            .map(|(_, suffix)| suffix)
            .and_then(|suffix| suffix.split_once("\n}").map(|(body, _)| body))
            .ok_or_else(|| anyhow::anyhow!("missing reviewed money action {function}"))?;
        let verification = body
            .find("verify_money_action_window()?")
            .ok_or_else(|| anyhow::anyhow!("{function} skipped native AX verification"))?;
        let action = body
            .find("executor.run(")
            .ok_or_else(|| anyhow::anyhow!("{function} has no executor boundary"))?;
        assert!(verification < action);
    }
    Ok(())
}

#[test]
fn cancellation_dispatches_one_native_double_click_not_two_axpress_actions() -> Result<()> {
    let simulation = std::fs::read_to_string("src/ths_sim.rs")?;
    let apple_cancel = simulation
        .split_once("fn cancellation_script(remote_contract_id: &str) -> String {")
        .map(|(_, suffix)| suffix)
        .and_then(|suffix| suffix.split_once("\nfn ").map(|(body, _)| body))
        .ok_or_else(|| anyhow::anyhow!("missing reviewed cancellation AppleScript"))?;
    for forbidden in [
        "click cancellableRow",
        "perform action \"AXPress\"",
        "click at cancellableRowClickPoint",
    ] {
        assert!(
            !apple_cancel.contains(forbidden),
            "a true native double-click cannot be synthesized with {forbidden}"
        );
    }

    let cancel_boundary = simulation
        .split_once("pub fn cancel_contract_with(")
        .map(|(_, suffix)| suffix)
        .and_then(|suffix| {
            suffix
                .split_once("\nfn verify_prepared_echo(")
                .map(|(body, _)| body)
        })
        .ok_or_else(|| anyhow::anyhow!("missing durable cancellation action boundary"))?;
    let cancel_boundary_lower = cancel_boundary.to_ascii_lowercase();
    assert_eq!(
        cancel_boundary_lower.matches("double_click").count(),
        1,
        "cancellation must dispatch one native double-click operation"
    );
    let verification = cancel_boundary
        .find("verify_money_action_window()?")
        .ok_or_else(|| anyhow::anyhow!("cancellation skipped native window verification"))?;
    let double_click = cancel_boundary_lower
        .find("double_click")
        .ok_or_else(|| anyhow::anyhow!("cancellation lacks native double-click dispatch"))?;
    assert!(verification < double_click);

    let native_ax = std::fs::read_to_string("src/ths_ax.rs")?;
    for primitive in [
        "CGEventCreateMouseEvent",
        "CGEventSetIntegerValueField",
        "kCGMouseEventClickState",
        "kCGEventLeftMouseDown",
        "kCGEventLeftMouseUp",
        "CGEventPost",
    ] {
        assert!(
            native_ax.contains(primitive),
            "native double-click is missing {primitive}"
        );
    }
    assert!(native_ax.contains("kCGMouseEventClickState, 1"));
    assert!(native_ax.contains("kCGMouseEventClickState, 2"));
    Ok(())
}

#[test]
fn native_double_click_revalidates_the_point_and_preallocates_all_four_events() -> Result<()> {
    let native_ax = std::fs::read_to_string("src/ths_ax.rs")?;
    let body = native_ax
        .split_once("pub fn native_double_click(x: i32, y: i32) -> Result<()> {")
        .map(|(_, suffix)| suffix)
        .and_then(|suffix| {
            suffix
                .split_once("\nfn inspect_element(")
                .map(|(body, _)| body)
        })
        .ok_or_else(|| anyhow::anyhow!("missing native double-click implementation"))?;

    let point_proof = body
        .find("verify_reviewed_simulation_point(x, y)")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "native click must re-prove that the stale AppleScript point is inside the \
                 current unique simulation-window frame"
            )
        })?;
    let first_post = body
        .find("CGEventPost(")
        .ok_or_else(|| anyhow::anyhow!("native double-click never posts its event sequence"))?;
    assert!(point_proof < first_post);

    for allocation in [
        "let first_down = create(",
        "let first_up = create(",
        "let second_down = create(",
        "let second_up = create(",
    ] {
        let allocation = body
            .find(allocation)
            .ok_or_else(|| anyhow::anyhow!("missing native event allocation {allocation}"))?;
        assert!(
            allocation < first_post,
            "all four native events must be allocated before the first irreversible post"
        );
    }
    let final_click_state = body
        .rfind("kCGMouseEventClickState, 2")
        .ok_or_else(|| anyhow::anyhow!("second native click state is missing"))?;
    assert!(
        final_click_state < first_post,
        "all click-state fields must be configured before the first irreversible post"
    );
    Ok(())
}

#[test]
fn malformed_cancel_ready_coordinates_never_reach_native_click_or_confirmation() {
    for raw in [
        "",
        "CANCEL_READY",
        "CANCEL_READY\t1",
        "CANCEL_READY\t1\t2\textra",
        "CANCEL_READY\t1.0\t2",
        "CANCEL_READY\t2147483648\t2",
        "NOT_CANCEL_READY\t1\t2",
    ] {
        let executor = RecordingExecutor::new([raw.to_owned()]);
        let error = cancel_contract_with(&executor, "SIM-6207021034")
            .expect_err("malformed preflight coordinates must fail closed");
        assert!(
            error.to_string().contains("cancellation") || error.to_string().contains("coordinate"),
            "unexpected preflight diagnostic for {raw:?}: {error:#}"
        );
        assert_eq!(executor.scripts.borrow().len(), 1);
        assert!(executor.native_double_clicks.borrow().is_empty());
    }
}

#[derive(Debug, Default)]
struct RejectingNativeClickExecutor {
    scripts: RefCell<Vec<String>>,
    native_calls: Cell<usize>,
}

impl AppleScriptExecutor for RejectingNativeClickExecutor {
    fn run(&self, source: &str) -> Result<String> {
        self.scripts.borrow_mut().push(source.to_owned());
        Ok("CANCEL_READY\t412\t300\n".to_owned())
    }

    fn native_double_click(&self, _x: i32, _y: i32) -> Result<()> {
        self.native_calls.set(self.native_calls.get() + 1);
        anyhow::bail!("fixture native double-click failed before posting")
    }
}

#[test]
fn native_double_click_failure_never_runs_the_confirmation_script() {
    let executor = RejectingNativeClickExecutor::default();

    let error = cancel_contract_with(&executor, "SIM-6207021034")
        .expect_err("native click failure must stop cancellation");

    assert!(error.to_string().contains("native double-click failed"));
    assert_eq!(executor.native_calls.get(), 1);
    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 1);
    assert!(scripts[0].contains("CANCEL_READY"));
    assert!(!scripts[0].contains("您确定要撤销这1笔委托?"));
}

#[test]
fn form_preparation_rechecks_values_and_has_no_submit_click() -> Result<()> {
    let executor = RecordingExecutor::new([
        SIMULATION_PROBE.to_owned(),
        "PREPARED\n".to_owned(),
        SIMULATION_PROBE.to_owned(),
    ]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    let prepared = prepare_with(&executor, order)?;

    assert!(!prepared.submitted);
    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 3);
    let prepare = &scripts[1];
    let set_code = prepare
        .find("set value of codeField to \"002256\"")
        .expect("unique code write");
    let focus_code = prepare
        .find("set focused of codeField to true")
        .expect("code field focus");
    let commit_code = prepare
        .find("key code 48")
        .or_else(|| prepare.find("perform action \"AXConfirm\" of codeField"))
        .expect("code edit must be committed before any order field write");
    let evidence_wait = prepare
        .find("repeat with preparationEvidenceAttempt from 1 to 20")
        .expect("bounded read-only security and quote evidence wait");
    let evidence_guard = prepare
        .find("if not preparationEvidenceReady then error")
        .expect("read-only evidence failure must stop before order field writes");
    let set_limit = prepare
        .find("set value of priceField to \"3.55\"")
        .expect("reviewed limit write");
    let set_quantity = prepare
        .find("set value of quantityField to \"1500\"")
        .expect("reviewed quantity write");
    assert!(
        set_code < focus_code
            && focus_code < commit_code
            && commit_code < evidence_wait
            && evidence_wait < evidence_guard
            && evidence_guard < set_limit
            && set_limit < set_quantity
    );
    let evidence = &prepare[evidence_wait..evidence_guard];
    assert!(evidence.contains("set frozenPreparationStaticTexts to {}"));
    assert!(evidence.contains("set linkedSecurityNames to {}"));
    assert!(evidence.contains("if (count of linkedSecurityNames) is 1"));
    assert!(evidence.contains("限价委托"));
    assert!(evidence.contains("ends with \"↑\" or candidateSecurityLabel ends with \"↓\""));
    assert!(evidence.contains("if (count of readOnlyMarketPriceCandidates) is 1"));
    assert!(!prepare.contains("set linkedMarketPrice to value of priceField as text"));
    assert!(!prepare.contains("value of priceField as text"));
    assert!(!prepare.contains("click button \"确定买入\""));
    assert!(!prepare.contains("click button \"确定卖出\""));
    assert!(!prepare.contains("click submitButton"));
    assert!(!prepare.contains("perform action \"AXPress\""));
    assert!(prepare.contains("set fPosition to position of fRef"));
    assert!(prepare.contains("set fields to {}"));
    assert!(prepare.contains("set end of fields to contents of f"));
    assert_windows_are_materialized_before_indexing(prepare);
    assert!(!prepare.contains("set fields to text fields of targetWindow"));
    assert!(!prepare.contains("item 2 of (position of f)"));
    Ok(())
}

#[test]
fn form_preparation_fails_if_the_ui_does_not_echo_the_order() -> Result<()> {
    let changed = SIMULATION_PROBE.replace(
        "FIELD\t323\t285\t89\t20\t1500",
        "FIELD\t323\t285\t89\t20\t1400",
    );
    let executor = RecordingExecutor::new([
        SIMULATION_PROBE.to_owned(),
        "PREPARED\n".to_owned(),
        changed,
    ]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    let error = prepare_with(&executor, order).unwrap_err();
    assert!(error.to_string().contains("quantity field does not match"));
    Ok(())
}

#[test]
fn prepared_form_click_rechecks_every_guard_and_clicks_exactly_once() -> Result<()> {
    let executor = RecordingExecutor::new([SIMULATION_PROBE.to_owned(), "CLICKED\n".to_owned()]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    click_prepared_form_with(&executor, &order)?;

    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 2);
    let submit = &scripts[1];
    assert_windows_are_materialized_before_indexing(submit);
    assert_eq!(submit.matches("click submitButton").count(), 1);
    assert!(submit.contains("set fields to {}"));
    assert!(submit.contains("set end of fields to contents of f"));
    assert!(!submit.contains("set fields to text fields of targetWindow"));
    assert!(!submit.contains("item 2 of text fields of targetWindow"));
    assert!(!submit.contains("item 3 of text fields of targetWindow"));
    assert!(submit.contains(&format!("bundle identifier is not \"{THS_BUNDLE_ID}\"")));
    assert!(submit.contains(&format!(
        "version of application file is not \"{TESTED_THS_VERSION}\""
    )));
    assert!(submit.find("模拟练习") < submit.find("click submitButton"));
    assert!(submit.find("live transfer control") < submit.find("click submitButton"));
    assert!(submit.find("code field changed") < submit.find("click submitButton"));
    assert!(submit.find("price field changed") < submit.find("click submitButton"));
    assert!(submit.find("quantity field changed") < submit.find("click submitButton"));
    Ok(())
}

#[test]
fn final_click_script_requires_one_unique_simulation_window_at_action_time() -> Result<()> {
    let executor = RecordingExecutor::new([SIMULATION_PROBE.to_owned(), "CLICKED\n".to_owned()]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    click_prepared_form_with(&executor, &order)?;

    let scripts = executor.scripts.borrow();
    let submit = &scripts[1];
    assert!(submit.contains("matchedWindowCount"));
    assert!(submit.contains("if matchedWindowCount is not 1"));
    assert!(submit.find("if matchedWindowCount is not 1") < submit.find("click submitButton"));
    Ok(())
}

#[test]
fn submission_confirmation_is_exactly_bound_to_the_prepared_simulation_order() -> Result<()> {
    let executor = RecordingExecutor::new([
        SIMULATION_PROBE.to_owned(),
        "CLICKED\n".to_owned(),
        "CONFIRMED\n".to_owned(),
    ]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    submit_prepared_form_with(&executor, &order)?;

    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 3);
    let confirm = &scripts[2];
    assert_windows_are_materialized_before_indexing(confirm);
    assert!(confirm.contains("您是否确认以上买入委托?"));
    assert!(confirm.contains("证券代码:002256"));
    assert!(confirm.contains("当前价格:3.55"));
    assert!(confirm.contains("买入数量:1500"));
    assert!(confirm.contains("if simulationWindowCount is not 1"));
    assert!(confirm.contains("value of attribute \"AXFocusedUIElement\""));
    assert!(confirm.contains("role of confirmationSheet is not \"AXSheet\""));
    assert!(confirm.contains("description of confirmationSheet is not \"警告\""));
    assert!(confirm.contains("(count of sheetButtons) is not 2"));
    assert!(confirm.contains("set sheetButtons to {}"));
    assert!(confirm.contains("set end of sheetButtons to contents of candidateButton"));
    assert!(confirm.contains("if (count of confirmButtons) is not 1"));
    assert_eq!(confirm.matches("click confirmButton").count(), 1);
    assert!(!confirm.contains("click button \"确认\""));
    assert!(confirm.find("证券代码:002256") < confirm.find("click confirmButton"));
    Ok(())
}

#[test]
fn every_money_moving_action_clicks_one_frozen_unique_button_reference() -> Result<()> {
    let submit_executor =
        RecordingExecutor::new([SIMULATION_PROBE.to_owned(), "CLICKED\n".to_owned()]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;
    click_prepared_form_with(&submit_executor, &order)?;
    let submit_scripts = submit_executor.scripts.borrow();
    let submit = &submit_scripts[1];
    assert!(submit.contains("set submitButtons to {}"));
    assert!(submit.contains("set end of submitButtons to candidateButtonRef"));
    assert!(submit.contains("if (count of submitButtons) is not 1"));
    assert_eq!(submit.matches("click submitButton").count(), 1);
    assert!(!submit.contains("click button \"确定买入\""));

    let cancel_executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);
    cancel_contract_with(&cancel_executor, "SIM-6207021034")?;
    let cancel_scripts = cancel_executor.scripts.borrow();
    let preflight = &cancel_scripts[0];
    let confirmation = &cancel_scripts[1];
    assert_windows_are_materialized_before_indexing(preflight);
    assert!(preflight.contains("if (count of commissionButtons) is not 1"));
    assert_eq!(preflight.matches("click commissionButton").count(), 1);
    assert!(!preflight.contains("click button \"委托\""));
    assert!(confirmation.contains("if (count of confirmButtons) is not 1"));
    assert_eq!(confirmation.matches("click confirmButton").count(), 1);
    assert!(!confirmation.contains("click button \"确认\""));
    assert_eq!(
        &*cancel_executor.native_double_clicks.borrow(),
        &[(412, 300)]
    );
    for forbidden in ["全撤", "撤买", "撤卖"] {
        assert!(!preflight.contains(forbidden));
        assert!(!confirmation.contains(forbidden));
    }
    Ok(())
}

#[test]
fn cancel_script_binds_the_only_cancellable_row_to_the_frozen_contract() -> Result<()> {
    let executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);

    cancel_contract_with(&executor, "SIM-6207021034")?;

    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 2);
    let cancel = &scripts[0];
    let confirmation = &scripts[1];
    assert_windows_are_materialized_before_indexing(cancel);
    assert_no_live_ax_collection_specifiers(cancel);
    assert_no_live_ax_collection_specifiers(confirmation);
    assert!(cancel.contains("if matchedWindowCount is not 1"));
    assert!(cancel.contains(
        "if exists static text \"账户设置\" of targetWindow then error \"live account settings are visible\""
    ));
    assert!(cancel.contains("set cancellableRows to {}"));
    assert!(cancel.contains("set end of cancellableRows to contents of candidateRow"));
    assert!(!cancel.contains("if (count of cancellableRows) is not 1"));
    assert!(cancel.contains("set contractCells to {}"));
    assert!(cancel.contains("set end of contractCells to contents of candidateCell"));
    assert!(cancel.contains("repeat with cell in contractCells"));
    assert!(!cancel.contains("item cellIndex of static texts of cancellableRow"));
    assert!(!cancel.contains("repeat with cell in entire contents of first row"));
    assert!(cancel.contains("SIM-6207021034"));
    assert!(cancel.contains("if (count of matchingRows) is not 1"));
    assert!(cancel.contains("set cancellableRow to item 1 of matchingRows"));
    assert!(cancel.contains("set cancellableRowPosition to position of cancellableRow"));
    assert!(cancel.contains("set cancellableRowSize to size of cancellableRow"));
    assert!(cancel.contains("cancellable row has invalid geometry"));
    assert!(cancel.contains("set cancellableRowClickPoint to"));
    assert!(cancel.contains("set targetWindowPosition to position of targetWindow"));
    assert!(cancel.contains("set targetWindowSize to size of targetWindow"));
    assert!(cancel.contains("simulation window has invalid geometry"));
    assert!(cancel.contains("cancellable row click point left the simulation window"));
    let geometry_guard = &cancel[cancel
        .find("set cancellableRowPosition to position of cancellableRow")
        .expect("row geometry")
        ..cancel
            .find("return \"CANCEL_READY\"")
            .expect("reviewed native-click point")];
    assert!(geometry_guard.contains("position of targetWindow"));
    assert!(geometry_guard.contains("size of targetWindow"));
    assert!(geometry_guard.contains("cancellableRowSize"));
    assert!(geometry_guard.contains("error"));
    assert!(!cancel.contains("click cancellableRow"));
    assert!(!cancel.contains("click at cancellableRowClickPoint"));
    assert!(cancel.contains("return \"CANCEL_READY\" & tab"));
    assert_eq!(&*executor.native_double_clicks.borrow(), &[(412, 300)]);
    assert!(!cancel.contains("全撤"));
    assert!(!cancel.contains("撤买"));
    assert!(!cancel.contains("撤卖"));
    assert!(confirmation.contains("您确定要撤销这1笔委托?"));
    assert!(confirmation.contains("value of attribute \"AXFocusedUIElement\""));
    assert!(confirmation.contains("repeat with confirmationAttempt from 1 to 20"));
    assert!(!confirmation.contains("repeat with ancestorDepth"));
    assert!(!confirmation.contains("value of attribute \"AXParent\""));
    let confirmation_poll = &confirmation[confirmation
        .find("repeat with confirmationAttempt from 1 to 20")
        .expect("bounded confirmation poll")
        ..confirmation
            .find("if confirmationSheet is missing value then error")
            .expect("missing confirmation rejection")];
    assert!(confirmation_poll.contains("on error errorMessage number errorNumber"));
    assert!(confirmation_poll.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(confirmation.contains("cancellation confirmation sheet did not appear"));
    assert!(!confirmation.contains("value of attribute \"AXWindow\""));
    assert!(!confirmation.contains("entire contents of confirmationWindow"));
    assert!(!confirmation.contains("confirmationDescendants"));
    assert!(confirmation.contains("role of confirmationSheet is not \"AXSheet\""));
    assert!(confirmation.contains("description of confirmationSheet is not \"警告\""));
    assert!(confirmation.contains("(count of sheetButtons) is not 2"));
    let confirmation_guard = &confirmation[confirmation
        .find("if confirmationSheet is missing value then error")
        .expect("bounded confirmation wait")
        ..confirmation
            .rfind("click confirmButton")
            .expect("unique confirmation click")];
    assert!(confirmation_guard.contains("confirmationSheet"));
    assert!(confirmation_guard.contains("error"));
    assert!(!confirmation_guard.contains("confirmationWindow"));
    assert!(!confirmation_guard.contains("simulationMarkerCount"));
    assert_eq!(confirmation.matches("click confirmButton").count(), 1);
    assert!(confirmation.find("您确定要撤销这1笔委托?") < confirmation.find("click confirmButton"));
    Ok(())
}

#[test]
fn exact_cancel_preflight_and_confirmation_never_reindex_live_ax_collections() -> Result<()> {
    let executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);

    cancel_contract_with(&executor, "SIM-6200000003")?;

    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 2);
    let preflight = &scripts[0];
    let confirmation = &scripts[1];
    assert_windows_are_materialized_before_indexing(preflight);
    assert_no_live_ax_collection_specifiers(preflight);
    assert_no_live_ax_collection_specifiers(confirmation);
    assert!(preflight.contains("set cancellableRows to {}"));
    assert!(preflight.contains("set contractCells to {}"));
    assert!(confirmation.contains("set sheetButtons to {}"));
    assert_eq!(&*executor.native_double_clicks.borrow(), &[(412, 300)]);

    let source = std::fs::read_to_string("src/ths_sim.rs")?;
    let boundary = source
        .split_once("pub fn cancel_contract_with(")
        .map(|(_, suffix)| suffix)
        .and_then(|suffix| {
            suffix
                .split_once("\nfn parse_cancellation_preflight")
                .map(|(body, _)| body)
        })
        .ok_or_else(|| anyhow::anyhow!("missing exact-cancel action boundary"))?;
    let verifications = boundary
        .match_indices("executor.verify_money_action_window()?")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(verifications.len(), 2);
    let preflight = boundary
        .find("executor.run(&cancellation_script(remote_contract_id))?")
        .expect("exact-contract preflight");
    let double_click = boundary
        .find("executor.native_double_click(x, y)?")
        .expect("single native double-click");
    let confirmation = boundary
        .find("executor.run(&cancellation_confirmation_script())?")
        .expect("focused exact-sheet confirmation");
    assert!(
        verifications[0] < preflight
            && preflight < double_click
            && double_click < verifications[1]
            && verifications[1] < confirmation
    );
    Ok(())
}

#[test]
fn cancellation_confirmation_uses_only_the_focused_exact_sheet_after_native_window_proof(
) -> Result<()> {
    let executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);

    cancel_contract_with(&executor, "SIM-6200000002")?;

    let scripts = executor.scripts.borrow();
    let confirmation = &scripts[1];
    assert!(confirmation.contains("value of attribute \"AXFocusedUIElement\""));
    assert!(confirmation.contains("role of confirmationSheet is not \"AXSheet\""));
    assert!(confirmation.contains("description of confirmationSheet is not \"警告\""));
    assert!(confirmation.contains("(count of sheetButtons) is not 2"));
    assert!(confirmation.contains("您确定要撤销这1笔委托?"));
    assert!(!confirmation.contains("AXWindow"));
    assert!(!confirmation.contains("AXParent"));
    assert!(!confirmation.contains("entire contents"));
    assert!(!confirmation.contains("confirmationDescendants"));
    assert!(!confirmation.contains("simulationMarkerCount"));
    assert!(confirmation.contains("on error errorMessage number errorNumber"));
    assert!(confirmation.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    Ok(())
}

#[test]
fn cancel_script_never_downgrades_an_unreadable_live_account_guard_to_absent() -> Result<()> {
    let executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);

    cancel_contract_with(&executor, "SIM-6207021034")?;

    let scripts = executor.scripts.borrow();
    let cancel = &scripts[0];
    assert!(cancel.contains(
        "if exists static text \"账户设置\" of targetWindow then error \"live account settings are visible\""
    ));
    let safety_guards = &cancel[cancel
        .find("if exists button \"转账\" of targetWindow")
        .expect("live transfer guard")
        ..cancel
            .find("set commissionButtons to")
            .expect("first money-moving navigation")];
    assert!(safety_guards.contains("账户设置"));
    assert!(safety_guards.contains("targetWindow"));
    Ok(())
}

#[test]
fn invalid_contract_id_never_reaches_macos_accessibility() {
    let executor = RecordingExecutor::new([]);
    assert!(cancel_contract_with(&executor, "bad contract/id").is_err());
    assert!(executor.scripts.borrow().is_empty());
}

#[test]
fn prepared_form_click_stops_before_action_when_echo_changed() -> Result<()> {
    let changed = SIMULATION_PROBE.replace(
        "FIELD\t323\t285\t89\t20\t1500",
        "FIELD\t323\t285\t89\t20\t1400",
    );
    let executor = RecordingExecutor::new([changed]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    let error = click_prepared_form_with(&executor, &order).unwrap_err();

    assert!(error.to_string().contains("quantity field does not match"));
    assert_eq!(executor.scripts.borrow().len(), 1);
    Ok(())
}

#[test]
fn prepared_form_click_stops_before_action_on_live_account_page() -> Result<()> {
    let live = SIMULATION_PROBE
        .replace("MARKER\t1", "MARKER\t0")
        .replace("LIVE_TRANSFER\t0", "LIVE_TRANSFER\t1");
    let executor = RecordingExecutor::new([live]);
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;

    let error = click_prepared_form_with(&executor, &order).unwrap_err();

    assert!(error.to_string().contains("模拟练习"));
    assert_eq!(executor.scripts.borrow().len(), 1);
    Ok(())
}

const EMPTY_ORDER_TABLE: &str = "\
ORDER_TABLE\t1\n\
HEADER\t委托日期\n\
HEADER\t委托时间\n\
HEADER\t证券代码\n\
HEADER\t证券名称\n\
HEADER\t操作\n\
HEADER\t备注\n\
HEADER\t委托数量\n\
HEADER\t撤销数量\n\
HEADER\t委托价格\n\
HEADER\t成交价格\n\
HEADER\t合同编号\n\
HEADER\t委托属性\n\
ROWS\t0\n";

const EMPTY_ORDER_TABLE_WITH_DECLARATION_ID: &str = "\
ORDER_TABLE\t1\n\
HEADER\t委托日期\n\
HEADER\t委托时间\n\
HEADER\t证券代码\n\
HEADER\t证券名称\n\
HEADER\t操作\n\
HEADER\t备注\n\
HEADER\t委托数量\n\
HEADER\t撤销数量\n\
HEADER\t委托价格\n\
HEADER\t成交价格\n\
HEADER\t合同编号\n\
HEADER\t申报编号\n\
HEADER\t委托属性\n\
ROWS\t0\n";

const EMPTY_FILL_TABLE: &str = "\
FILL_TABLE\t1\n\
HEADER\t成交日期\n\
HEADER\t成交时间\n\
HEADER\t证券代码\n\
HEADER\t证券名称\n\
HEADER\t操作\n\
HEADER\t成交数量\n\
HEADER\t成交均价\n\
HEADER\t成交金额\n\
HEADER\t合同编号\n\
HEADER\t成交编号\n\
HEADER\t成交类别\n\
ROWS\t0\n";

const EMPTY_FILL_TABLE_WITH_DECLARATION_ID: &str = "\
FILL_TABLE\t1\n\
HEADER\t成交日期\n\
HEADER\t成交时间\n\
HEADER\t证券代码\n\
HEADER\t证券名称\n\
HEADER\t操作\n\
HEADER\t成交数量\n\
HEADER\t成交均价\n\
HEADER\t成交金额\n\
HEADER\t合同编号\n\
HEADER\t成交编号\n\
HEADER\t申报编号\n\
HEADER\t成交类别\n\
ROWS\t0\n";

#[test]
fn fill_table_probe_is_read_only_schema_locked_and_materializes_exact_records() -> Result<()> {
    let mut raw = EMPTY_FILL_TABLE.replace("ROWS\t0", "ROWS\t2");
    for (row, fill_id, quantity, price, amount) in [
        (1, "fill-001", "2500", "3.510", "8775.00"),
        (2, "fill-002", "2500", "3.51", "8775"),
    ] {
        let cells = [
            "20260819",
            "103000",
            "002256",
            "兆新股份",
            "买入",
            quantity,
            price,
            amount,
            "6210000001",
            fill_id,
            "普通成交",
        ];
        for (index, value) in cells.iter().enumerate().rev() {
            raw.push_str(&format!(
                "CELL\t{row}\t{}\t{}\t{value}\n",
                index + 1,
                100 + index * 50
            ));
        }
    }
    let executor = RecordingExecutor::new([raw.clone()]);
    let table = probe_fills_with(&executor)?;
    let records = table.records()?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].contract_id, "6210000001");
    assert_eq!(records[0].fill_id, "fill-001");
    assert_eq!(records[0].quantity, 2_500);
    assert_eq!(records[0].price, Decimal::from_str("3.510")?);
    assert_eq!(records[0].amount, Decimal::from_str("8775.00")?);
    assert_eq!(records[0].direction, Direction::Buy);

    let scripts = executor.scripts.borrow();
    assert_eq!(scripts.len(), 1);
    assert!(scripts[0].contains("成交"));
    assert!(scripts[0].contains("set liveTargetButtons to get (buttons of targetWindow)"));
    assert!(scripts[0].contains("set end of frozenTargetButtons to contents of candidateButtonRef"));
    assert!(scripts[0].contains("if (count of fillButtons) is not 1"));
    assert_eq!(scripts[0].matches("click item 1 of fillButtons").count(), 1);
    assert!(scripts[0].contains("set liveRows to get (rows of matchedTable)"));
    assert!(scripts[0].contains("set end of frozenRows to contents of rowRef"));
    assert!(scripts[0].contains("set liveCells to get (entire contents of tableRow)"));
    assert!(scripts[0].contains("set end of frozenCells to contents of cellRef"));
    let negative_proof = scripts[0]
        .find("if exists button \"转账\" of w")
        .expect("live-account negative proof");
    let fill_tab_click = scripts[0]
        .find("click item 1 of fillButtons")
        .expect("one read-only fill-tab navigation");
    assert!(negative_proof < fill_tab_click);
    let after_fill_tab = &scripts[0][fill_tab_click..];
    let retry = after_fill_tab
        .find("repeat with fillSnapshotAttempt from 1 to 3")
        .expect("post-tab fill evidence needs one bounded atomic retry boundary");
    let refreshed_windows = after_fill_tab[retry..]
        .find("set liveWindowObjects to get every window")
        .map(|offset| retry + offset)
        .expect("every fill attempt must reacquire a fresh window snapshot");
    let rows = after_fill_tab[refreshed_windows..]
        .find("set liveRows to get (rows of matchedTable)")
        .map(|offset| refreshed_windows + offset)
        .expect("fill rows must belong to the same atomic attempt");
    let publish = after_fill_tab[rows..]
        .find("set stableFillSnapshot to true")
        .map(|offset| rows + offset)
        .expect("only a complete fill snapshot may be published");
    assert!(retry < refreshed_windows && refreshed_windows < rows && rows < publish);
    assert!(after_fill_tab.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(after_fill_tab.contains(
        "if not stableFillSnapshot then error \"stable Tonghuashun fill snapshot unavailable\""
    ));
    assert!(!scripts[0].contains("确定买入"));
    assert!(!scripts[0].contains("确定卖出"));
    assert!(!scripts[0].contains("撤销这1笔委托"));

    assert!(parse_fills_probe(&raw.replace("HEADER\t成交均价", "HEADER\t成交价格")).is_err());
    assert!(parse_fills_probe(&raw.replace("fill-002", "fill-001"))?
        .records()
        .is_err());
    assert!(parse_fills_probe(&raw.replace("8775.00", "8775.01"))?
        .records()
        .is_err());
    assert!(
        parse_fills_probe(&raw.replace("CELL\t1\t6\t350\t2500", "CELL\t1\t6\t350\t0"))?
            .records()
            .is_err()
    );
    assert!(
        parse_fills_probe(&raw.replace("CELL\t1\t7\t400\t3.510", "CELL\t1\t7\t400\t0"))?
            .records()
            .is_err()
    );
    Ok(())
}

#[test]
fn fill_probe_post_tab_snapshot_is_atomic_and_never_swallows_unknown_marker_errors() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_FILL_TABLE.to_owned()]);
    probe_fills_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let after_tab = scripts[0]
        .split_once("click item 1 of fillButtons")
        .map(|(_, suffix)| suffix)
        .expect("fill probe must navigate to the unique 成交 tab");

    let ordered = [
        "repeat with fillSnapshotAttempt from 1 to 3",
        "set liveWindowObjects to get every window",
        "if matchedWindowCount is 0 then error \"post-tab simulation fill window is rebuilding\" number -1719",
        "if matchedWindowCount is greater than 1 then error \"multiple post-tab simulation fill windows found\"",
        "set liveScrollAreas to get (scroll areas of targetWindow)",
        "if names is expectedHeaders or names is expectedHeadersWithReportId then",
        "set liveRows to get (rows of matchedTable)",
        "set liveCells to get (entire contents of tableRow)",
        "set stableFillSnapshot to true",
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber",
    ];
    let mut cursor = 0;
    for needle in ordered {
        let offset = after_tab[cursor..]
            .find(needle)
            .unwrap_or_else(|| panic!("missing post-tab atomic evidence: {needle}"));
        cursor += offset + needle.len();
    }

    assert!(after_tab.contains("set hasSimulationMarker to exists static text \"模拟练习\" of w"));
    assert!(
        after_tab.contains("set hasNestedLiveControl to (exists static text \"账户设置\" of w)")
    );
    assert!(after_tab.contains("if exists button \"转账\" of w then set hasNestedLiveControl"));
    assert!(after_tab.contains("if exists button \"退出\" of w then set hasNestedLiveControl"));
    assert!(after_tab
        .contains("if hasNestedLiveControl then error \"live account control is visible\""));
    assert!(after_tab.contains(
        "if matchedCount is 0 then error \"reviewed simulation fill table was not found\""
    ));
    assert!(after_tab.contains(
        "if matchedCount is greater than 1 then error \"simulation fill table is not unique\""
    ));
    assert!(!after_tab.contains("确定买入"));
    assert!(!after_tab.contains("确定卖出"));
    Ok(())
}

#[test]
fn fill_probe_preflight_retries_only_transient_missing_simulation_window() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_FILL_TABLE.to_owned()]);
    probe_fills_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let before_tab = scripts[0]
        .split_once("click item 1 of fillButtons")
        .map(|(prefix, _)| prefix)
        .expect("fill probe must navigate to the unique 成交 tab");
    let retry = before_tab
        .find("repeat with fillPreflightAttempt from 1 to 3")
        .expect("fill preflight needs a bounded transient retry");
    let attempt = &before_tab[retry..];
    assert!(attempt.contains("set liveWindowObjects to get every window"));
    assert!(attempt.contains(
        "if matchedWindowCount is 0 then error \"simulation fill window is rebuilding\" number -1719"
    ));
    assert!(attempt.contains(
        "if matchedWindowCount is greater than 1 then error \"multiple simulation fill windows found\""
    ));
    assert!(attempt.contains("set stableFillPreflight to true"));
    assert!(attempt.contains(
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber"
    ));
    assert!(attempt.contains(
        "if not stableFillPreflight then error \"stable Tonghuashun fill preflight unavailable\""
    ));
    assert!(before_tab.contains("if exists button \"转账\" of w then set hasNestedLiveControl"));
    assert!(before_tab.contains("if exists button \"退出\" of w then set hasNestedLiveControl"));
    assert!(
        before_tab.contains("set hasNestedLiveControl to (exists static text \"账户设置\" of w)")
    );
    Ok(())
}

#[test]
fn fill_probe_identity_ignores_unrelated_dynamic_window_texts() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_FILL_TABLE.to_owned()]);
    probe_fills_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let (before_tab, after_tab) = scripts[0]
        .split_once("click item 1 of fillButtons")
        .expect("fill probe must navigate to the unique 成交 tab");

    for identity in [before_tab, after_tab] {
        assert!(
            identity.contains("set hasSimulationMarker to exists static text \"模拟练习\" of w")
        );
        assert!(identity.contains("set liveWindowScrollAreas to get (scroll areas of w)"));
        assert!(identity.contains(
            "set nestedSimulationMarker to exists static text \"模拟练习\" of candidateScrollArea"
        ));
        assert!(!identity.contains("static texts of w"));
        assert!(!identity.contains("entire contents of w"));
    }
    Ok(())
}

#[test]
fn reviewed_fill_declaration_id_column_is_audited_without_changing_fill_binding() -> Result<()> {
    let mut raw = EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("ROWS\t0", "ROWS\t1");
    for (index, value) in [
        "20260821",
        "130000",
        "002256",
        "兆新股份",
        "买入",
        "100",
        "3.32",
        "332",
        "6213000001",
        "fill-130000-1",
        "202608211300000001",
        "普通成交",
    ]
    .iter()
    .enumerate()
    .rev()
    {
        raw.push_str(&format!(
            "CELL\t1\t{}\t{}\t{value}\n",
            index + 1,
            100 + index * 50
        ));
    }
    let executor = RecordingExecutor::new([raw.clone()]);

    let table = probe_fills_with(&executor)?;
    assert_eq!(table.headers.len(), 12);
    assert_eq!(table.headers[8], "合同编号");
    assert_eq!(table.headers[9], "成交编号");
    assert_eq!(table.headers[10], "申报编号");
    assert_eq!(table.rows[0].cells[10], "202608211300000001");
    let records = table.records()?;
    let [record] = records.as_slice() else {
        panic!("one reviewed fill expected")
    };
    assert_eq!(record.contract_id, "6213000001");
    assert_eq!(record.fill_id, "fill-130000-1");
    assert_eq!(record.quantity, 100);

    let script = &executor.scripts.borrow()[0];
    assert!(script.contains("\"合同编号\", \"成交编号\", \"申报编号\", \"成交类别\""));
    assert!(script.contains("set matchedHeaders to names"));
    assert!(script.contains("repeat with headerName in matchedHeaders"));
    assert!(script.contains("if matchedCount is greater than 1"));
    assert!(script.contains("set liveRows to get (rows of matchedTable)"));
    assert!(!script.contains("确定买入"));
    assert!(!script.contains("确定卖出"));
    assert!(!script.contains("撤销"));
    Ok(())
}

#[test]
fn reviewed_fill_header_variants_and_remote_identities_remain_fail_closed() -> Result<()> {
    let attacks = [
        EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("HEADER\t合同编号\n", ""),
        EMPTY_FILL_TABLE_WITH_DECLARATION_ID
            .replace("HEADER\t申报编号\n", "HEADER\t申报编号\nHEADER\t状态说明\n"),
        EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace(
            "HEADER\t成交编号\nHEADER\t申报编号\n",
            "HEADER\t申报编号\nHEADER\t成交编号\n",
        ),
        EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("HEADER\t申报编号\n", "HEADER\t报单编号\n"),
    ];
    for attacked in attacks {
        assert!(
            parse_fills_probe(&attacked).is_err(),
            "only the two exact reviewed fill layouts may be accepted"
        );
    }

    let make_row = |row: usize, contract: &str, fill: &str, report: &str| {
        [
            "20260821",
            "130000",
            "002256",
            "兆新股份",
            "买入",
            "100",
            "3.32",
            "332",
            contract,
            fill,
            report,
            "普通成交",
        ]
        .iter()
        .enumerate()
        .map(|(index, value)| {
            format!(
                "CELL\t{row}\t{}\t{}\t{value}\n",
                index + 1,
                100 + index * 50
            )
        })
        .collect::<String>()
    };

    for (contract, fill) in [("", "fill-1"), ("contract-1", "")] {
        let mut raw = EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("ROWS\t0", "ROWS\t1");
        raw.push_str(&make_row(1, contract, fill, "valid-report-id"));
        assert!(
            parse_fills_probe(&raw)?.records().is_err(),
            "a valid declaration ID must never replace an invalid contract or fill ID"
        );
    }

    let mut duplicate_fill = EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("ROWS\t0", "ROWS\t2");
    duplicate_fill.push_str(&make_row(1, "contract-1", "fill-1", "report-1"));
    duplicate_fill.push_str(&make_row(2, "contract-1", "fill-1", "report-2"));
    assert!(
        parse_fills_probe(&duplicate_fill)?.records().is_err(),
        "different declaration IDs must not make one duplicate fill ID unique"
    );

    let mut shared_report = EMPTY_FILL_TABLE_WITH_DECLARATION_ID.replace("ROWS\t0", "ROWS\t2");
    shared_report.push_str(&make_row(1, "contract-1", "fill-1", "report-1"));
    shared_report.push_str(&make_row(2, "contract-1", "fill-2", "report-1"));
    assert_eq!(parse_fills_probe(&shared_report)?.records()?.len(), 2);
    Ok(())
}

#[test]
fn order_table_probe_is_schema_locked_and_read_only() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_ORDER_TABLE.to_owned()]);

    let table = probe_orders_with(&executor)?;

    assert_eq!(table.headers.len(), 12);
    assert!(table.rows.is_empty());
    let scripts = executor.scripts.borrow();
    assert_windows_are_materialized_before_indexing(&scripts[0]);
    assert_orders_probe_refinds_the_simulation_window_after_switching_tabs(&scripts[0]);
    assert_orders_probe_retries_the_entire_post_tab_table_snapshot(&scripts[0]);
    assert_orders_probe_retries_a_transient_missing_post_tab_window(&scripts[0]);
    assert_order_table_tree_is_materialized_before_iteration(&scripts[0]);
    assert!(scripts[0].contains("set commissionButtons to {}"));
    assert!(scripts[0].contains("if (count of commissionButtons) is not 1"));
    assert_eq!(scripts[0].matches("click commissionButton").count(), 1);
    assert!(!scripts[0].contains("click button \"委托\""));
    assert!(scripts[0].contains("if (value of cancellableOnlyCheckbox as integer) is 1"));
    assert!(!scripts[0].contains("click button \"确定买入\""));
    assert!(!scripts[0].contains("click button \"确定卖出\""));
    Ok(())
}

#[test]
fn order_probe_preflight_retries_only_transient_window_rebuilds_before_tab_click() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_ORDER_TABLE.to_owned()]);
    probe_orders_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let before_tab = scripts[0]
        .split_once("click commissionButton")
        .map(|(prefix, _)| prefix)
        .expect("orders probe must navigate to the unique 委托 tab");
    let retry = before_tab
        .find("repeat with orderPreflightAttempt from 1 to 5")
        .expect("orders preflight needs a bounded transient retry");
    let attempt = &before_tab[retry..];
    for required in [
        "set liveWindowObjects to get every window",
        "if matchedWindowCount is 0 then error \"simulation order window is rebuilding\" number -1719",
        "if matchedWindowCount is greater than 1 then error \"multiple simulation order windows found\"",
        "if exists button \"转账\" of targetWindow then error",
        "if exists button \"退出\" of targetWindow then error",
        "if exists static text \"账户设置\" of targetWindow then error",
        "if (count of commissionButtons) is not 1 then error \"simulation commission tab is not unique\"",
        "set stableOrderPreflight to true",
        "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber",
    ] {
        assert!(attempt.contains(required), "missing order preflight evidence: {required}");
    }
    // Every object that contributes to the uniqueness proof must be readable
    // in the same attempt.  A transient AX failure may retry the whole
    // preflight, but it must never silently omit a second window, a live
    // account marker, or another 委托 button.
    for partial_evidence_handler in [
        "set wIsAlive to false\n      try",
        "set accountLabel to value of s as text\n        end try",
        "set accountLabel to name of s as text\n          end try",
        "if (name of candidateButton as text) is \"委托\" then set end of commissionButtons to candidateButton\n      end try",
    ] {
        assert!(
            !attempt.contains(partial_evidence_handler),
            "order preflight must not publish partial AX evidence: {partial_evidence_handler}"
        );
    }
    for atomic_read in [
        "set ignoredWindowRole to role of w\n        set candidateWindowSubrole to subrole of w\n      on error errorMessage number errorNumber\n        error errorMessage number errorNumber",
        "set hasSimulationMarker to exists static text \"模拟练习\" of w\n        set hasNestedLiveControl to (exists static text \"账户设置\" of w)",
        "if exists button \"退出\" of w then set hasNestedLiveControl to true\n      on error errorMessage number errorNumber\n        error errorMessage number errorNumber",
        "set candidateButtonName to name of candidateButton as text\n      on error errorMessage number errorNumber",
    ] {
        assert!(
            attempt.contains(atomic_read),
            "order preflight is missing an atomic AX read: {atomic_read}"
        );
    }
    assert!(before_tab.contains("set lastOrderPreflightError to \"not attempted\""));
    assert!(attempt
        .contains("set lastOrderPreflightError to errorMessage & \" [\" & errorNumber & \"]\""));
    assert!(attempt.contains(
        "if not stableOrderPreflight then error \"stable Tonghuashun order preflight unavailable: \" & lastOrderPreflightError"
    ));
    Ok(())
}

#[test]
fn order_probe_identity_preflight_ignores_unrelated_dynamic_window_texts() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_ORDER_TABLE.to_owned()]);
    probe_orders_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let before_tab = scripts[0]
        .split_once("click commissionButton")
        .map(|(prefix, _)| prefix)
        .expect("orders probe must navigate to the unique 委托 tab");

    assert!(before_tab.contains("set hasSimulationMarker to exists static text \"模拟练习\" of w"));
    assert!(
        before_tab.contains("set hasNestedLiveControl to (exists static text \"账户设置\" of w)")
    );
    assert!(before_tab.contains("if exists button \"转账\" of w then"));
    assert!(before_tab.contains("if exists button \"退出\" of w then"));
    assert!(
        !before_tab.contains("static texts of w"),
        "the identity proof must not freeze unrelated dynamic texts such as the ticking CN clock"
    );
    assert!(
        !before_tab.contains("entire contents of w"),
        "the identity proof must not traverse unrelated dynamic descendants"
    );
    Ok(())
}

#[test]
fn order_probe_post_tab_identity_ignores_unrelated_dynamic_window_texts() -> Result<()> {
    let executor = RecordingExecutor::new([EMPTY_ORDER_TABLE.to_owned()]);
    probe_orders_with(&executor)?;
    let scripts = executor.scripts.borrow();
    let after_tab = scripts[0]
        .split_once("click commissionButton")
        .map(|(_, suffix)| suffix)
        .expect("orders probe must navigate to the unique 委托 tab");
    let identity = after_tab
        .split_once("set frozenFilterStaticTexts to {}")
        .map(|(prefix, _)| prefix)
        .expect("post-tab identity must precede the reviewed order filter");

    assert!(identity.contains("set hasSimulationMarker to exists static text \"模拟练习\" of w"));
    assert!(identity.contains("set liveWindowScrollAreas to get (scroll areas of w)"));
    assert!(identity.contains(
        "set nestedSimulationMarker to exists static text \"模拟练习\" of candidateScrollArea"
    ));
    assert!(!identity.contains("static texts of w"));
    assert!(!identity.contains("entire contents of w"));
    Ok(())
}

#[test]
fn reviewed_declaration_id_column_is_audited_without_changing_contract_binding() -> Result<()> {
    let cells = [
        "20260820",
        "101501",
        "002256",
        "兆新股份",
        "买入",
        "未成交",
        "100",
        "0",
        "3.51",
        "0.000",
        "6212000001",
        "202608201015010001",
        "限价委托",
    ];
    let raw = order_table_with_declaration_id_row(&cells);
    let executor = RecordingExecutor::new([raw.clone()]);

    let table = probe_orders_with(&executor)?;

    assert_eq!(table.headers.len(), 13);
    assert_eq!(table.headers[10], "合同编号");
    assert_eq!(table.headers[11], "申报编号");
    assert_eq!(table.rows[0].cells[10], "6212000001");
    assert_eq!(table.rows[0].cells[11], "202608201015010001");
    let records = table.records()?;
    let [record] = records.as_slice() else {
        panic!("one reviewed order expected")
    };
    assert_eq!(record.contract_id, "6212000001");
    assert_eq!(record.quantity, 100);

    let script = &executor.scripts.borrow()[0];
    assert_order_table_tree_is_materialized_before_iteration(script);
    assert!(script.contains("\"合同编号\", \"申报编号\", \"委托属性\""));
    assert!(script.contains("set liveRows to get (rows of matchedTable)"));
    assert!(script.contains("set liveCells to get (entire contents of tableRow)"));
    assert!(!script.contains("确定买入"));
    assert!(!script.contains("确定卖出"));
    assert!(!script.contains("全撤"));

    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.51")?, 100)?;
    let matched = table.unique_new_match(&order, &[])?;
    assert_eq!(matched.contract_id, "6212000001");

    let cancel_executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);
    cancel_contract_with(&cancel_executor, "6212000001")?;
    let cancel_script = &cancel_executor.scripts.borrow()[0];
    assert!(cancel_script.contains("\"合同编号\", \"申报编号\", \"委托属性\""));
    assert!(cancel_script.contains("set liveRows to get (rows of matchedTable)"));
    assert!(cancel_script.contains("set liveCells to get (entire contents of candidateRow)"));
    assert!(!cancel_script.contains("全撤"));
    assert!(!cancel_script.contains("撤买"));
    assert!(!cancel_script.contains("撤卖"));
    Ok(())
}

#[test]
fn reviewed_order_header_variants_reject_missing_duplicate_reordered_or_unknown_columns() {
    let attacks = [
        EMPTY_ORDER_TABLE_WITH_DECLARATION_ID.replace("HEADER\t合同编号\n", ""),
        EMPTY_ORDER_TABLE_WITH_DECLARATION_ID.replace("HEADER\t申报编号\n", "HEADER\t合同编号\n"),
        EMPTY_ORDER_TABLE_WITH_DECLARATION_ID.replace(
            "HEADER\t合同编号\nHEADER\t申报编号\n",
            "HEADER\t申报编号\nHEADER\t合同编号\n",
        ),
        EMPTY_ORDER_TABLE_WITH_DECLARATION_ID
            .replace("HEADER\t委托属性\n", "HEADER\t状态说明\nHEADER\t委托属性\n"),
    ];
    for attacked in attacks {
        assert!(
            parse_orders_probe(&attacked).is_err(),
            "only the two explicitly reviewed header layouts may be accepted"
        );
    }
}

#[test]
fn order_filter_is_bound_to_one_checkbox_even_when_ax_duplicates_the_visible_label() -> Result<()> {
    let probe_executor = RecordingExecutor::new([EMPTY_ORDER_TABLE.to_owned()]);
    probe_orders_with(&probe_executor)?;
    let probe_scripts = probe_executor.scripts.borrow();
    let probe_script = &probe_scripts[0];

    let cancel_executor = RecordingExecutor::new([
        "CANCEL_READY\t412\t300\n".to_owned(),
        "CANCELLED_CLICKED\n".to_owned(),
    ]);
    cancel_contract_with(&cancel_executor, "SIM-6207021034")?;
    let cancel_scripts = cancel_executor.scripts.borrow();
    let cancel_script = &cancel_scripts[0];

    for (script, frozen_labels, append_label, iterate_labels, live_checkboxes, append_checkbox) in [
        (
            probe_script,
            "set frozenFilterStaticTexts to {}",
            "set end of frozenFilterStaticTexts to contents of candidateStaticTextRef",
            "repeat with candidateStaticTextRef in frozenFilterStaticTexts",
            "set liveCheckboxes to get (checkboxes of targetWindow)",
            "set end of frozenCheckboxes to contents of candidateCheckboxRef",
        ),
        (
            cancel_script,
            "set frozenStaticTexts to {}",
            "set end of frozenStaticTexts to contents of candidateStaticTextRef",
            "repeat with candidateLabel in frozenStaticTexts",
            "set liveTargetCheckboxes to get (checkboxes of targetWindow)",
            "set end of frozenCheckboxes to contents of candidateCheckboxRef",
        ),
    ] {
        assert!(script.contains("只显示可撤委托"));
        assert!(script.contains(frozen_labels));
        assert!(script.contains(append_label));
        assert!(script.contains(iterate_labels));
        assert!(script.contains("set matchedFilterCheckboxes to {}"));
        assert!(script.contains("set candidateFilterCheckboxMatched to false"));
        assert!(script.contains("set candidateFilterCheckboxMatched to true"));
        assert!(script.contains(
            "if candidateFilterCheckboxMatched then set end of matchedFilterCheckboxes to candidateCheckbox"
        ));
        assert!(script.contains("if (count of matchedFilterCheckboxes) is not 1"));
        assert!(!script.contains("if (count of filterLabels) is not 1"));
        assert!(script.contains("set frozenCheckboxes to {}"));
        assert!(script.contains(live_checkboxes));
        assert!(script.contains(append_checkbox));
        assert!(script.contains("set observedCheckboxCount to count of frozenCheckboxes"));
        assert!(script.contains("item 1 of filterLabelPosition"));
        assert!(script.contains("item 2 of filterLabelPosition"));
        assert!(!script.contains("if (count of checkboxes of targetWindow) is not 1"));
        assert!(!script.contains("set cancellableOnlyCheckbox to first checkbox"));
    }
    Ok(())
}

#[test]
fn order_table_rows_use_stable_ax_ordinals_when_a_column_has_a_misleading_x_position() -> Result<()>
{
    let cells = [
        "20260817",
        "195500",
        "002256",
        "兆新股份",
        "买入",
        "未报",
        "100",
        "0",
        "3.55",
        "0",
        "SIM-1",
        "限价委托",
    ];
    let mut raw = EMPTY_ORDER_TABLE.replace("ROWS\t0", "ROWS\t1");
    for (index, value) in cells.iter().enumerate().rev() {
        let x = if index == 11 { 420 } else { 100 + index * 50 };
        raw.push_str(&format!("CELL\t1\t{}\t{x}\t{}\n", index + 1, value));
    }

    let table = parse_orders_probe(&raw)?;

    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0].cells, cells);
    let incomplete = raw
        .lines()
        .filter(|line| !line.ends_with("限价委托"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(parse_orders_probe(&incomplete).is_err());
    let duplicate_ordinal = raw.replace("CELL\t1\t12\t420\t限价委托", "CELL\t1\t11\t420\t限价委托");
    assert!(parse_orders_probe(&duplicate_ordinal).is_err());
    Ok(())
}

#[test]
fn changed_order_table_headers_fail_closed() {
    let changed = EMPTY_ORDER_TABLE.replace("HEADER\t合同编号", "HEADER\t订单编号");
    assert!(parse_orders_probe(&changed).is_err());
}

#[test]
fn zero_fill_price_means_unfilled_only_and_nonzero_prices_remain_strict() -> Result<()> {
    for remark in ["未报", "未成交"] {
        for zero in ["0", "0.000"] {
            let table = parse_orders_probe(&single_order_row(remark, zero))?;
            let records = table.records()?;
            let [record] = records.as_slice() else {
                panic!("one order row expected");
            };
            assert_eq!(
                record.fill_price, None,
                "{remark} with THS sentinel {zero} must mean no fill"
            );
        }
    }

    for remark in ["部分撤单", "全部撤单"] {
        for zero in ["0", "0.000"] {
            let table = parse_orders_probe(&single_order_row(remark, zero))?;
            let records = table.records()?;
            let [record] = records.as_slice() else {
                panic!("one order row expected");
            };
            assert_eq!(
                record.fill_price, None,
                "{remark} with THS sentinel {zero} must mean no fill"
            );
        }
        let table = parse_orders_probe(&single_order_row(remark, "3.551"))?;
        let records = table.records()?;
        let [record] = records.as_slice() else {
            panic!("one order row expected");
        };
        assert_eq!(record.fill_price.as_deref(), Some("3.551"));
    }

    for remark in ["部分成交", "全部成交"] {
        for zero in ["0", "0.000"] {
            let table = parse_orders_probe(&single_order_row(remark, zero))?;
            assert!(
                table.records().is_err(),
                "{remark} cannot claim a zero fill price"
            );
        }
        let table = parse_orders_probe(&single_order_row(remark, "3.551"))?;
        let records = table.records()?;
        let [record] = records.as_slice() else {
            panic!("one order row expected");
        };
        assert_eq!(record.fill_price.as_deref(), Some("3.551"));
    }

    for remark in [
        "未报",
        "未成交",
        "部分成交",
        "全部成交",
        "部分撤单",
        "全部撤单",
    ] {
        let table = parse_orders_probe(&single_order_row(remark, "-0.001"))?;
        assert!(
            table.records().is_err(),
            "negative fill price must fail closed for {remark}"
        );
    }
    Ok(())
}

#[test]
fn reconciliation_ignores_preexisting_manual_orders_and_requires_one_new_contract() -> Result<()> {
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 1500)?;
    let table = SimulationOrderTable {
        headers: vec![
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
        rows: vec![
            order_row("existing", "002256", "买入", "1500", "3.55"),
            order_row("new-contract", "002256", "买入", "1500", "3.550"),
        ],
    };

    let matched = table.unique_new_match(&order, &["existing".to_owned()])?;

    assert_eq!(matched.contract_id, "new-contract");
    assert_eq!(matched.quantity, 1500);
    assert_eq!(matched.order_price, Decimal::from_str("3.55")?);
    assert!(table.unique_new_match(&order, &[]).is_err());
    assert!(SimulationOrderTable {
        headers: table.headers.clone(),
        rows: vec![order_row("existing", "002256", "买入", "1500", "3.55")],
    }
    .unique_new_match(&order, &["existing".to_owned()])
    .is_err());

    let ambiguous = SimulationOrderTable {
        headers: table.headers,
        rows: vec![
            order_row("new-a", "002256", "买入", "1500", "3.55"),
            order_row("new-b", "002256", "买入", "1500", "3.55"),
        ],
    };
    assert!(ambiguous.unique_new_match(&order, &[]).is_err());
    Ok(())
}

fn single_order_row(remark: &str, fill_price: &str) -> String {
    let cells = [
        "20260818",
        "093100",
        "002256",
        "兆新股份",
        "买入",
        remark,
        "100",
        "0",
        "3.55",
        fill_price,
        "SIM-FILL-PRICE",
        "限价委托",
    ];
    let mut raw = EMPTY_ORDER_TABLE.replace("ROWS\t0", "ROWS\t1");
    for (index, value) in cells.iter().enumerate() {
        raw.push_str(&format!(
            "CELL\t1\t{}\t{}\t{value}\n",
            index + 1,
            100 + index * 50
        ));
    }
    raw
}

fn order_table_with_declaration_id_row(cells: &[&str; 13]) -> String {
    let mut raw = EMPTY_ORDER_TABLE_WITH_DECLARATION_ID.replace("ROWS\t0", "ROWS\t1");
    for (index, value) in cells.iter().enumerate().rev() {
        raw.push_str(&format!(
            "CELL\t1\t{}\t{}\t{value}\n",
            index + 1,
            100 + index * 50
        ));
    }
    raw
}

fn order_row(
    contract_id: &str,
    symbol: &str,
    operation: &str,
    quantity: &str,
    price: &str,
) -> SimulationOrderTableRow {
    SimulationOrderTableRow {
        cells: vec![
            "20260817",
            "195500",
            symbol,
            "兆新股份",
            operation,
            "未报",
            quantity,
            "0",
            price,
            "--",
            contract_id,
            "限价委托",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
    }
}
