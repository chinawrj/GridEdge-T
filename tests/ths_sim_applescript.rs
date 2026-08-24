use anyhow::{bail, Result};
use gridedge_t::{
    domain::Direction,
    ths_sim::{
        prepare_with, probe_orders_with, probe_with, AppleScriptExecutor, SimulatedOrderDraft,
    },
};
use rust_decimal::Decimal;
use std::{cell::Cell, str::FromStr};

const PREPARED_SIMULATION_PROBE: &str = "\
BUNDLE\tcn.com.10jqka.macstockPro\n\
VERSION\t5.3.2\n\
SUBMIT\t确定买入\n\
FIELD\t323\t285\t89\t20\t100\n\
FIELD\t304\t179\t138\t20\t002256\n\
FIELD\t323\t225\t89\t20\t3.55\n\
MARKER\t1\n\
LIVE_TRANSFER\t0\n\
LIVE_EXIT\t0\n\
LIVE_ACCOUNT_SETTINGS\t0\n\
BUY_BUTTON\t1\n\
SELL_BUTTON\t1\n";

#[derive(Debug, Default)]
struct LazyAccessibilityCollectionRegression {
    call: Cell<usize>,
}

impl AppleScriptExecutor for LazyAccessibilityCollectionRegression {
    fn run(&self, source: &str) -> Result<String> {
        let call = self.call.get();
        self.call.set(call + 1);
        match call {
            0 | 2 => Ok(PREPARED_SIMULATION_PROBE.to_owned()),
            1 => {
                if source.contains("set fields to text fields of targetWindow") {
                    bail!("System Events: Can't get item 3 of every text field (Invalid index)");
                }
                assert!(source.contains("set fields to {}"));
                assert!(source.contains("set end of fields to contents of f"));
                assert!(!source.contains("item 2 of text fields of targetWindow"));
                assert!(!source.contains("item 3 of text fields of targetWindow"));
                assert!(source.contains("set fPosition to position of fRef"));
                assert!(source.contains("if item 2 of fPosition < item 2 of codePosition"));
                assert!(source.contains("if item 2 of fPosition > item 2 of quantityPosition"));
                Ok("PREPARED\n".to_owned())
            }
            _ => bail!("unexpected AppleScript invocation"),
        }
    }
}

#[test]
fn input_fields_are_materialized_before_the_app_rebuilds_its_accessibility_tree() -> Result<()> {
    let executor = LazyAccessibilityCollectionRegression::default();
    let order =
        SimulatedOrderDraft::new(Direction::Buy, "002256", Decimal::from_str("3.55")?, 100)?;

    let prepared = prepare_with(&executor, order)?;

    assert!(!prepared.submitted);
    assert_eq!(executor.call.get(), 3);
    Ok(())
}

#[derive(Debug, Default)]
struct DisappearingWindowCollectionRegression;

impl AppleScriptExecutor for DisappearingWindowCollectionRegression {
    fn run(&self, source: &str) -> Result<String> {
        if source.contains("item windowIndex of windows")
            || source.contains("repeat with candidateWindow in windows")
        {
            bail!("System Events: Can't get window 3 of every window (Invalid index)");
        }
        assert!(source.contains("set frozenWindows to {}"));
        assert!(source.contains("tell application \"同花顺\" to activate"));
        assert!(source.contains("repeat with windowSnapshotAttempt from 1 to 3"));
        assert!(source.contains("set liveWindowObjects to get every window"));
        assert!(!source.contains("every window whose subrole"));
        assert!(source.contains("set liveWindowCount to count of liveWindowObjects"));
        assert!(source.contains("repeat with candidateWindowRef in liveWindowObjects"));
        assert!(!source.contains("a reference to (item liveWindowIndex of (every window"));
        assert!(source.contains("set end of frozenWindows to contents of candidateWindowRef"));
        assert!(source.contains("if not stableWindowSnapshot then error"));
        assert!(source.contains("repeat with candidateWindowRef in frozenWindows"));
        assert!(source.contains("set w to contents of candidateWindowRef"));
        assert!(!source.contains("set observedWindowCount to count of frozenWindows"));
        assert!(!source.contains("item windowIndex of frozenWindows"));
        assert!(source.contains("set ignoredWindowRole to role of w"));
        assert!(!source.contains("if w is not missing value then"));
        assert!(source.contains("if subrole of w is \"AXStandardWindow\" then"));
        assert!(source.contains("if (count of frozenWindows) is not liveWindowCount then error"));
        if !source.contains(
            "if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber",
        )
        {
            bail!("System Events: Can't get window 4 of application process (Invalid index)");
        }
        Ok(PREPARED_SIMULATION_PROBE.to_owned())
    }
}

#[test]
fn disappearing_auxiliary_window_cannot_invalidate_a_materialized_window_list() -> Result<()> {
    let executor = DisappearingWindowCollectionRegression;

    let old_live_collection_pattern =
        "set observedWindowCount to count of windows\nset w to item windowIndex of windows";
    assert!(executor.run(old_live_collection_pattern).is_err());
    let materialized_but_unprotected_pattern = "set frozenWindows to {}\nrepeat with candidateWindow in windows\nset end of frozenWindows to contents of candidateWindow\nend repeat\nset observedWindowCount to count of frozenWindows\nset w to item windowIndex of frozenWindows";
    assert!(executor.run(materialized_but_unprotected_pattern).is_err());

    let probe = probe_with(&executor)?.verify_simulation()?;
    assert_eq!(probe.code_field.value, "002256");
    assert_eq!(probe.quantity_field.value, "100");
    Ok(())
}

#[derive(Debug, Default)]
struct NestedScrollAreaSimulationMarkerRegression;

impl AppleScriptExecutor for NestedScrollAreaSimulationMarkerRegression {
    fn run(&self, source: &str) -> Result<String> {
        let discovery = source
            .split_once("click commissionButton")
            .map(|(prefix, _)| prefix)
            .ok_or_else(|| anyhow::anyhow!("window uniqueness guard is missing"))?;
        if !discovery.contains("set liveWindowScrollAreas to get (scroll areas of w)") {
            bail!(
                "fixture: 模拟练习 is an AXStaticText inside a nested AXScrollArea, not a direct static text of the AXStandardWindow"
            );
        }
        assert!(discovery.contains(
            "set nestedSimulationMarker to exists static text \"模拟练习\" of candidateScrollArea"
        ));
        assert!(!discovery.contains("entire contents of w"));
        assert!(source.contains("if matchedWindowCount is 0 then error"));
        assert!(source.contains("if matchedWindowCount is greater than 1 then error"));
        assert!(!source.contains("set matchedWindowCount to 1"));
        Ok(EMPTY_ORDER_TABLE.to_owned())
    }
}

#[test]
fn nested_scroll_area_marker_selects_one_read_only_orders_window() -> Result<()> {
    let executor = NestedScrollAreaSimulationMarkerRegression;

    let table = probe_orders_with(&executor)?;

    assert!(table.rows.is_empty());
    assert_eq!(table.headers.len(), 12);
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

#[test]
fn production_activation_uses_the_reviewed_bundle_not_the_dynamic_dock_ui() {
    let source = include_str!("../src/ths_sim.rs");

    assert!(source.contains("Command::new(\"/usr/bin/open\")"));
    assert!(source.contains(".arg(\"-b\")"));
    assert!(source.contains(".arg(THS_BUNDLE_ID)"));
    assert!(!source.contains("tell process \"Dock\""));
    assert!(!source.contains("thsDockItems"));
    assert!(!source.contains("click thsDockItem"));
}
