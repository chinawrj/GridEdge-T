use crate::domain::Direction;
use anyhow::{anyhow, bail, Context, Result};
use chrono::{Local, NaiveDateTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::process::Command;

pub const THS_APP_NAME: &str = "同花顺";
pub const THS_BUNDLE_ID: &str = "cn.com.10jqka.macstockPro";
pub const TESTED_THS_VERSION: &str = "5.3.2";
pub const SIMULATION_MARKER: &str = "模拟练习";
pub const MAX_SIMULATED_ORDER_QUANTITY: i64 = 1_000_000;
pub const MAX_SIMULATED_ORDER_NOTIONAL: Decimal = Decimal::from_parts(50_000_000, 0, 0, false, 0);

const PROBE_SCRIPT: &str = r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then return "ERROR\tNOT_RUNNING"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    set stableWindowSnapshot to false
    set out to {}
    repeat with windowSnapshotAttempt from 1 to 3
      try
        set attemptOut to {}
        set end of attemptOut to "BUNDLE\t" & bundle identifier
        set end of attemptOut to "VERSION\t" & version of application file
        set markerCount to 0
        set transferCount to 0
        set exitCount to 0
        set accountSettingsCount to 0
        set buyCount to 0
        set sellCount to 0
        set attemptFieldCount to 0
        set frozenWindows to {}
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is not liveWindowCount then error "unstable Tonghuashun window snapshot" number -1719
        repeat with candidateWindowRef in frozenWindows
          set w to contents of candidateWindowRef
          set ignoredWindowRole to role of w
          if subrole of w is "AXStandardWindow" then
            if exists button "转账" of w then error "live transfer control is visible"
            if exists button "退出" of w then error "live account exit control is visible"
            if exists static text "账户设置" of w then error "live account settings are visible"
            set frozenWindowStaticTexts to {}
            set liveWindowStaticTexts to get (static texts of w)
            repeat with candidateStaticTextRef in liveWindowStaticTexts
              set end of frozenWindowStaticTexts to contents of candidateStaticTextRef
            end repeat
            set frozenWindowButtons to {}
            set liveWindowButtons to get (buttons of w)
            repeat with candidateButtonRef in liveWindowButtons
              set end of frozenWindowButtons to contents of candidateButtonRef
            end repeat
            set frozenWindowTextFields to {}
            set liveWindowTextFields to get (text fields of w)
            repeat with candidateFieldRef in liveWindowTextFields
              set end of frozenWindowTextFields to contents of candidateFieldRef
            end repeat
            repeat with candidateStaticTextRef in frozenWindowStaticTexts
              set s to missing value
              set staticTextAlive to false
              try
                set s to contents of candidateStaticTextRef
                set ignoredStaticTextRole to role of s
                set staticTextAlive to true
              on error errorMessage number errorNumber
                if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              set v to ""
              if staticTextAlive then try
                set v to value of s as text
              on error errorMessage number errorNumber
                if errorNumber is -1719 or errorNumber is -1728 then set staticTextAlive to false
                if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              if staticTextAlive and v is "" then
                try
                  set v to name of s as text
                on error errorMessage number errorNumber
                  if errorNumber is -1719 or errorNumber is -1728 then set staticTextAlive to false
                  if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
                end try
              end if
              if staticTextAlive and v is "模拟练习" then set markerCount to markerCount + 1
            end repeat
            repeat with candidateButtonRef in frozenWindowButtons
              set buttonAlive to false
              try
                set b to contents of candidateButtonRef
                set ignoredButtonRole to role of b
                set n to name of b as text
                set buttonAlive to true
              on error errorMessage number errorNumber
                if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              if buttonAlive then
                if n is "买入" then set buyCount to buyCount + 1
                if n is "卖出" then set sellCount to sellCount + 1
                if n is "确定买入" or n is "确定卖出" then set end of attemptOut to "SUBMIT\t" & n
              end if
            end repeat
            repeat with candidateFieldRef in frozenWindowTextFields
              set fieldAlive to false
              try
                set f to contents of candidateFieldRef
                set ignoredFieldRole to role of f
                set p to position of f
                set z to size of f
                set fieldAlive to true
              on error errorMessage number errorNumber
                if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              set v to ""
              if fieldAlive then try
                set v to value of f as text
              on error errorMessage number errorNumber
                if errorNumber is -1719 or errorNumber is -1728 then set fieldAlive to false
                if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              if fieldAlive then
                set end of attemptOut to "FIELD\t" & (item 1 of p) & "\t" & (item 2 of p) & "\t" & (item 1 of z) & "\t" & (item 2 of z) & "\t" & v
                set attemptFieldCount to attemptFieldCount + 1
              end if
            end repeat
          end if
        end repeat
        if attemptFieldCount is not 3 then error "incomplete Tonghuashun order-field snapshot" number -1719
        set end of attemptOut to "MARKER\t" & markerCount
        set end of attemptOut to "LIVE_TRANSFER\t" & transferCount
        set end of attemptOut to "LIVE_EXIT\t" & exitCount
        set end of attemptOut to "LIVE_ACCOUNT_SETTINGS\t" & accountSettingsCount
        set end of attemptOut to "BUY_BUTTON\t" & buyCount
        set end of attemptOut to "SELL_BUTTON\t" & sellCount
        set out to attemptOut
        set stableWindowSnapshot to true
        exit repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun UI evidence unavailable"
    set AppleScript's text item delimiters to linefeed
    return out as text
  end tell
end tell
"#;

const ORDERS_PROBE_SCRIPT: &str = r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then return "ERROR\tNOT_RUNNING"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "cn.com.10jqka.macstockPro" then error "unexpected application bundle"
    if version of application file is not "5.3.2" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {}
      try
        set liveWindowObjects to get every window
        set frozenWindows to {}
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
      set hasSimulationMarker to false
      set hasNestedLiveControl to false
      set frozenWindowStaticTexts to {}
      try
        repeat with candidateStaticText in static texts of w
          set end of frozenWindowStaticTexts to contents of candidateStaticText
        end repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      repeat with candidateStaticText in frozenWindowStaticTexts
        set s to contents of candidateStaticText
        set markerValue to ""
        try
          set markerValue to value of s as text
        end try
        if markerValue is "" then
          try
            set markerValue to name of s as text
          end try
        end if
        if markerValue is "模拟练习" then set hasSimulationMarker to true
      end repeat
      -- Some reviewed 5.3.2 layouts nest the account marker below an
      -- AXScrollArea. Only take the slower descendant path when the stable,
      -- materialized direct collection did not contain the marker.
      if not hasSimulationMarker then
        set frozenWindowDescendants to {}
        try
          set frozenWindowDescendants to entire contents of w
        on error errorMessage number errorNumber
          if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
        end try
        repeat with candidateDescendant in frozenWindowDescendants
          set descendantRef to contents of candidateDescendant
          try
            set descendantRole to role of descendantRef
            if descendantRole is "AXStaticText" then
              set descendantValue to value of descendantRef as text
              if descendantValue is "模拟练习" then set hasSimulationMarker to true
              if descendantValue is "账户设置" then set hasNestedLiveControl to true
            else if descendantRole is "AXButton" then
              set descendantName to name of descendantRef as text
              if descendantName is "转账" or descendantName is "退出" then set hasNestedLiveControl to true
            end if
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
        end repeat
      end if
      if hasSimulationMarker then
        if hasNestedLiveControl then error "live account control is visible"
        set targetWindow to w
        set matchedWindowCount to matchedWindowCount + 1
      end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set frozenTargetStaticTexts to {}
    set liveTargetStaticTexts to get (static texts of targetWindow)
    repeat with candidateStaticTextRef in liveTargetStaticTexts
      set end of frozenTargetStaticTexts to contents of candidateStaticTextRef
    end repeat
    repeat with candidateStaticTextRef in frozenTargetStaticTexts
      set s to missing value
      try
        set s to contents of candidateStaticTextRef
        set ignoredTargetStaticTextRole to role of s
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if s is not missing value then
        set accountLabel to ""
        try
          set accountLabel to value of s as text
        end try
        if accountLabel is "" then
          try
            set accountLabel to name of s as text
          end try
        end if
        if accountLabel is "账户设置" then error "live account settings are visible"
      end if
    end repeat
    set frozenTargetButtons to {}
    set liveTargetButtons to get (buttons of targetWindow)
    repeat with candidateButtonRef in liveTargetButtons
      set end of frozenTargetButtons to contents of candidateButtonRef
    end repeat
    set commissionButtons to {}
    repeat with candidateButtonRef in frozenTargetButtons
      set candidateButton to contents of candidateButtonRef
      try
        if (name of candidateButton as text) is "委托" then set end of commissionButtons to candidateButton
      end try
    end repeat
    if (count of commissionButtons) is not 1 then error "simulation commission tab is not unique"
    set commissionButton to item 1 of commissionButtons
    click commissionButton
    delay 0.3
    -- Switching tabs rebuilds the reviewed 5.3.2 AX window. Never carry the
    -- pre-click window reference across that boundary.
    set stableOrderSnapshot to false
    repeat with orderSnapshotAttempt from 1 to 3
      try
        set attemptOut to {}
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {}
      try
        set liveWindowObjects to get every window
        set frozenWindows to {}
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable post-tab Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
        set hasSimulationMarker to false
        set hasNestedLiveControl to false
        set frozenWindowStaticTexts to {}
        try
          set liveWindowStaticTexts to get (static texts of w)
          repeat with candidateStaticTextRef in liveWindowStaticTexts
            set end of frozenWindowStaticTexts to contents of candidateStaticTextRef
          end repeat
        on error errorMessage number errorNumber
          if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
        end try
        repeat with candidateStaticTextRef in frozenWindowStaticTexts
          set s to contents of candidateStaticTextRef
          set markerValue to ""
          try
            set markerValue to value of s as text
          end try
          if markerValue is "" then
            try
              set markerValue to name of s as text
            end try
          end if
          if markerValue is "模拟练习" then set hasSimulationMarker to true
        end repeat
        if not hasSimulationMarker then
          set frozenWindowDescendants to {}
          try
            set frozenWindowDescendants to entire contents of w
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          repeat with candidateDescendant in frozenWindowDescendants
            set descendantRef to contents of candidateDescendant
            try
              set descendantRole to role of descendantRef
              if descendantRole is "AXStaticText" then
                set descendantValue to value of descendantRef as text
                if descendantValue is "模拟练习" then set hasSimulationMarker to true
                if descendantValue is "账户设置" then set hasNestedLiveControl to true
              else if descendantRole is "AXButton" then
                set descendantName to name of descendantRef as text
                if descendantName is "转账" or descendantName is "退出" then set hasNestedLiveControl to true
              end if
            on error errorMessage number errorNumber
              if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
            end try
          end repeat
        end if
        if hasSimulationMarker then
          if hasNestedLiveControl then error "live account control is visible"
          set targetWindow to w
          set matchedWindowCount to matchedWindowCount + 1
        end if
      end if
    end repeat
    if matchedWindowCount is 0 then error "post-tab simulation order window is rebuilding" number -1719
    if matchedWindowCount is greater than 1 then error "multiple post-tab simulation order windows found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set frozenFilterStaticTexts to {}
    set liveFilterStaticTexts to get (static texts of targetWindow)
    repeat with candidateStaticTextRef in liveFilterStaticTexts
      set end of frozenFilterStaticTexts to contents of candidateStaticTextRef
    end repeat
    set filterLabels to {}
    repeat with candidateStaticTextRef in frozenFilterStaticTexts
      try
        set candidateLabelRef to contents of candidateStaticTextRef
        if (value of candidateLabelRef as text) is "只显示可撤委托" then set end of filterLabels to candidateLabelRef
      end try
    end repeat
    if (count of filterLabels) is not 1 then error "simulation order filter label is not unique"
    set filterLabel to item 1 of filterLabels
    set filterLabelPosition to position of filterLabel
    set frozenCheckboxes to {}
    set liveCheckboxes to get (checkboxes of targetWindow)
    repeat with candidateCheckboxRef in liveCheckboxes
      set end of frozenCheckboxes to contents of candidateCheckboxRef
    end repeat
    set cancellableOnlyCheckbox to missing value
    set filterCheckboxCount to 0
    set observedCheckboxCount to count of frozenCheckboxes
    repeat with candidateCheckboxRef in frozenCheckboxes
      try
        set candidateCheckbox to contents of candidateCheckboxRef
        set checkboxPosition to position of candidateCheckbox
        if (item 1 of checkboxPosition) < (item 1 of filterLabelPosition) and ((item 1 of filterLabelPosition) - (item 1 of checkboxPosition)) <= 30 and (item 2 of checkboxPosition) >= ((item 2 of filterLabelPosition) - 10) and (item 2 of checkboxPosition) <= ((item 2 of filterLabelPosition) + 10) then
          set cancellableOnlyCheckbox to candidateCheckbox
          set filterCheckboxCount to filterCheckboxCount + 1
        end if
      end try
    end repeat
    if filterCheckboxCount is not 1 then error "simulation order filter checkbox is not unique"
    if (value of cancellableOnlyCheckbox as integer) is 1 then
      click cancellableOnlyCheckbox
      delay 0.3
    end if
    set expectedHeaders to {"委托日期", "委托时间", "证券代码", "证券名称", "操作", "备注", "委托数量", "撤销数量", "委托价格", "成交价格", "合同编号", "委托属性"}
    set expectedHeadersWithReportId to {"委托日期", "委托时间", "证券代码", "证券名称", "操作", "备注", "委托数量", "撤销数量", "委托价格", "成交价格", "合同编号", "申报编号", "委托属性"}
    set matchedTable to missing value
    set matchedCount to 0
    set frozenScrollAreas to {}
    set liveScrollAreas to get (scroll areas of targetWindow)
    repeat with scrollAreaRef in liveScrollAreas
      set end of frozenScrollAreas to contents of scrollAreaRef
    end repeat
    repeat with frozenScrollAreaRef in frozenScrollAreas
      set sa to contents of frozenScrollAreaRef
      set frozenScrollChildren to {}
      set liveScrollChildren to get (UI elements of sa)
      repeat with scrollChildRef in liveScrollChildren
        set end of frozenScrollChildren to contents of scrollChildRef
      end repeat
      repeat with frozenScrollChildRef in frozenScrollChildren
        set candidate to contents of frozenScrollChildRef
        if role of candidate is "AXTable" then
          set frozenTableChildren to {}
          set liveTableChildren to get (UI elements of candidate)
          repeat with tableChildRef in liveTableChildren
            set end of frozenTableChildren to contents of tableChildRef
          end repeat
          repeat with frozenTableChildRef in frozenTableChildren
            set child to contents of frozenTableChildRef
            if role of child is "AXGroup" then
              set names to {}
              set frozenHeaderElements to {}
              set liveHeaderElements to get (UI elements of child)
              repeat with headerElementRef in liveHeaderElements
                set end of frozenHeaderElements to contents of headerElementRef
              end repeat
              repeat with frozenHeaderElementRef in frozenHeaderElements
                set header to contents of frozenHeaderElementRef
                if role of header is "AXButton" then
                  try
                    set end of names to name of header as text
                  end try
                end if
              end repeat
              if names is expectedHeaders or names is expectedHeadersWithReportId then
                set matchedTable to candidate
                set matchedCount to matchedCount + 1
                set expectedHeaders to names
              end if
            end if
          end repeat
        end if
      end repeat
    end repeat
    if matchedCount is not 1 then error "simulation order table is not unique"
    set attemptOut to {"ORDER_TABLE\t1"}
    repeat with headerName in expectedHeaders
      set end of attemptOut to "HEADER\t" & headerName
    end repeat
    set tableRows to {}
    set liveRows to get (rows of matchedTable)
    repeat with liveRowRef in liveRows
      set end of tableRows to contents of liveRowRef
    end repeat
    set end of attemptOut to "ROWS\t" & (count of tableRows)
    set rowIndex to 0
    repeat with tableRow in tableRows
      set rowIndex to rowIndex + 1
      set rowContents to {}
      set liveCells to get (entire contents of tableRow)
      repeat with liveCellRef in liveCells
        set end of rowContents to contents of liveCellRef
      end repeat
      set cellIndex to 0
      repeat with cell in rowContents
        try
          if role of cell is "AXStaticText" then
            set cellIndex to cellIndex + 1
            set p to position of cell
            set v to value of cell as text
            set end of attemptOut to "CELL\t" & rowIndex & "\t" & cellIndex & "\t" & (item 1 of p) & "\t" & v
          end if
        end try
      end repeat
    end repeat
        set out to attemptOut
        set stableOrderSnapshot to true
        exit repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.2
    end repeat
    if not stableOrderSnapshot then error "stable Tonghuashun order snapshot unavailable"
    set AppleScript's text item delimiters to linefeed
    return out as text
  end tell
end tell
"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AxRect {
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AxTextField {
    pub frame: AxRect,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ThsUiProbe {
    pub bundle_id: String,
    pub version: String,
    pub simulation_marker_count: usize,
    pub live_transfer_button_count: usize,
    pub live_exit_button_count: usize,
    pub live_account_settings_count: usize,
    pub buy_button_count: usize,
    pub sell_button_count: usize,
    pub submit_labels: Vec<String>,
    pub text_fields: Vec<AxTextField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedSimulationUi {
    pub bundle_id: String,
    pub version: String,
    pub marker: String,
    pub active_direction: Direction,
    pub code_field: AxTextField,
    pub price_field: AxTextField,
    pub quantity_field: AxTextField,
    pub submit_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedOrderDraft {
    pub direction: Direction,
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DryRunPlan {
    pub ui: VerifiedSimulationUi,
    pub order: SimulatedOrderDraft,
    pub would_submit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreparedSimulationForm {
    pub ui: VerifiedSimulationUi,
    pub order: SimulatedOrderDraft,
    pub submitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationMarketQuote {
    /// Wall-clock time immediately before the reviewed UI evidence is read.
    pub observed_from: NaiveDateTime,
    /// Wall-clock time after the reviewed UI evidence has been completely read.
    pub observed_at: NaiveDateTime,
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub last_price: Decimal,
    pub source_sha256: String,
    pub application_bundle: String,
    pub application_version: String,
    pub account_marker: String,
}

impl SimulationMarketQuote {
    pub fn validate_reviewed_evidence(&self, expected_symbol: &str) -> Result<()> {
        validate_a_share_symbol(expected_symbol)?;
        if self.symbol != expected_symbol {
            bail!("Tonghuashun quote symbol changed while validating evidence");
        }
        if self.observed_from > self.observed_at {
            bail!("Tonghuashun quote evidence time window is reversed");
        }
        if self.last_price <= Decimal::ZERO || self.last_price.scale() > 3 {
            bail!("Tonghuashun refreshed quote is outside the A-share price envelope");
        }
        if self.source_sha256.len() != 64
            || !self
                .source_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("Tonghuashun quote evidence hash is not canonical SHA-256");
        }
        if self.application_bundle != THS_BUNDLE_ID {
            bail!("Tonghuashun quote came from an unreviewed application bundle");
        }
        if self.application_version != TESTED_THS_VERSION {
            bail!("Tonghuashun quote came from an unreviewed application version");
        }
        if self.account_marker != SIMULATION_MARKER {
            bail!("Tonghuashun quote did not come from the simulation account");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationOrderTableRow {
    pub cells: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationOrderTable {
    pub headers: Vec<String>,
    pub rows: Vec<SimulationOrderTableRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationOrderRecord {
    pub order_date: String,
    pub order_time: String,
    pub symbol: String,
    pub security_name: String,
    pub direction: Direction,
    pub remark: String,
    pub quantity: i64,
    pub cancelled_quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub order_price: Decimal,
    pub fill_price: Option<String>,
    pub contract_id: String,
    pub order_attribute: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationFillTableRow {
    pub cells: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulationFillTable {
    pub headers: Vec<String>,
    pub rows: Vec<SimulationFillTableRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationFillRecord {
    pub fill_date: String,
    pub fill_time: String,
    pub symbol: String,
    pub security_name: String,
    pub direction: Direction,
    pub quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub amount: Decimal,
    pub contract_id: String,
    pub fill_id: String,
    pub fill_category: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFilledEvidence {
    pub evidence_contract_version: u32,
    pub order: SimulationOrderRecord,
    pub fills: Vec<SimulationFillRecord>,
}

pub trait AppleScriptExecutor {
    fn run(&self, source: &str) -> Result<String>;

    fn verify_money_action_window(&self) -> Result<()> {
        Ok(())
    }

    fn native_double_click(&self, _x: i32, _y: i32) -> Result<()> {
        bail!("native double-click is unavailable for this executor")
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemOsaScript;

impl AppleScriptExecutor for SystemOsaScript {
    fn run(&self, source: &str) -> Result<String> {
        let raised = Command::new("/usr/bin/open")
            .arg("-b")
            .arg(THS_BUNDLE_ID)
            .output()
            .context("failed to launch macOS Tonghuashun window activation")?;
        if !raised.status.success() {
            bail!(
                "failed to activate the reviewed Tonghuashun application: {}",
                String::from_utf8_lossy(&raised.stderr).trim()
            );
        }
        let output = Command::new("/usr/bin/osascript")
            .arg("-e")
            .arg(source)
            .output()
            .context("failed to launch macOS AppleScript runtime")?;
        if !output.status.success() {
            bail!(
                "macOS accessibility probe failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).context("AppleScript probe returned non-UTF-8 output")
    }

    fn verify_money_action_window(&self) -> Result<()> {
        #[cfg(target_os = "macos")]
        crate::ths_ax::verify_reviewed_simulation_window()
            .context("native Tonghuashun simulation-window verification failed")?;
        Ok(())
    }

    fn native_double_click(&self, x: i32, y: i32) -> Result<()> {
        #[cfg(target_os = "macos")]
        crate::ths_ax::native_double_click(x, y)
            .context("native Tonghuashun contract-row double-click failed")?;
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (x, y);
            bail!("native Tonghuashun double-click requires macOS");
        }
        Ok(())
    }
}

pub fn probe_current_ui() -> Result<ThsUiProbe> {
    probe_with(&SystemOsaScript)
}

pub fn probe_current_market_quote(symbol: &str) -> Result<SimulationMarketQuote> {
    let observed_from = Local::now().naive_local();
    let (ui, last_price) = collect_market_quote_evidence(&SystemOsaScript, symbol)?;
    let observed_at = Local::now().naive_local();
    build_market_quote(symbol, observed_from, observed_at, ui, last_price)
}

pub fn probe_market_quote_with(
    executor: &dyn AppleScriptExecutor,
    symbol: &str,
    observed_at: NaiveDateTime,
) -> Result<SimulationMarketQuote> {
    probe_market_quote_with_window(executor, symbol, observed_at, observed_at)
}

pub fn probe_market_quote_with_window(
    executor: &dyn AppleScriptExecutor,
    symbol: &str,
    observed_from: NaiveDateTime,
    observed_at: NaiveDateTime,
) -> Result<SimulationMarketQuote> {
    let (ui, last_price) = collect_market_quote_evidence(executor, symbol)?;
    build_market_quote(symbol, observed_from, observed_at, ui, last_price)
}

fn collect_market_quote_evidence(
    executor: &dyn AppleScriptExecutor,
    symbol: &str,
) -> Result<(VerifiedSimulationUi, Decimal)> {
    validate_a_share_symbol(symbol)?;
    let refreshed = executor.run(&market_quote_refresh_script(symbol))?;
    let receipt = refreshed.trim().split('\t').collect::<Vec<_>>();
    let ["QUOTE_REFRESHED", refreshed_last_price] = receipt.as_slice() else {
        bail!("Tonghuashun quote refresh did not acknowledge completion with one market price");
    };
    let last_price = refreshed_last_price
        .parse::<Decimal>()
        .context("Tonghuashun refreshed quote price is not a decimal")?;
    if last_price <= Decimal::ZERO || last_price.scale() > 3 {
        bail!("Tonghuashun refreshed quote price is outside the A-share price envelope");
    }
    let ui = probe_with(executor)?.verify_simulation()?;
    if ui.code_field.value != symbol {
        bail!("Tonghuashun quote symbol does not match the requested symbol");
    }
    Ok((ui, last_price))
}

fn build_market_quote(
    symbol: &str,
    observed_from: NaiveDateTime,
    observed_at: NaiveDateTime,
    ui: VerifiedSimulationUi,
    last_price: Decimal,
) -> Result<SimulationMarketQuote> {
    #[derive(Serialize)]
    struct QuoteSourceEvidence<'a> {
        evidence_version: u32,
        symbol: &'a str,
        last_price: String,
        application_bundle: &'a str,
        application_version: &'a str,
        account_marker: &'a str,
    }

    let evidence = serde_json::to_vec(&QuoteSourceEvidence {
        evidence_version: 1,
        symbol,
        last_price: last_price.normalize().to_string(),
        application_bundle: &ui.bundle_id,
        application_version: &ui.version,
        account_marker: &ui.marker,
    })?;
    let quote = SimulationMarketQuote {
        observed_from,
        observed_at,
        symbol: symbol.to_owned(),
        last_price,
        source_sha256: hex::encode(Sha256::digest(evidence)),
        application_bundle: ui.bundle_id,
        application_version: ui.version,
        account_marker: ui.marker,
    };
    quote.validate_reviewed_evidence(symbol)?;
    Ok(quote)
}

pub fn probe_current_orders() -> Result<SimulationOrderTable> {
    probe_orders_with(&SystemOsaScript)
}

pub fn probe_current_fills() -> Result<SimulationFillTable> {
    probe_fills_with(&SystemOsaScript)
}

pub fn probe_fills_with(executor: &dyn AppleScriptExecutor) -> Result<SimulationFillTable> {
    parse_fills_probe(&executor.run(&fills_probe_script())?)
}

fn fills_probe_script() -> String {
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {{}}
    set liveWindowObjects to get every window
    set liveWindowCount to count of liveWindowObjects
    repeat with candidateWindowRef in liveWindowObjects
      set end of frozenWindows to contents of candidateWindowRef
    end repeat
    if (count of frozenWindows) is not liveWindowCount then error "unstable Tonghuashun window snapshot"
    repeat with candidateWindowRef in frozenWindows
      set w to contents of candidateWindowRef
      set ignoredWindowRole to role of w
      if subrole of w is "AXStandardWindow" then
        if exists button "转账" of w then error "live transfer control is visible"
        if exists button "退出" of w then error "live account exit control is visible"
        if exists static text "账户设置" of w then error "live account settings are visible"
        set frozenWindowStaticTexts to {{}}
        set liveWindowStaticTexts to get (static texts of w)
        repeat with candidateStaticTextRef in liveWindowStaticTexts
          set end of frozenWindowStaticTexts to contents of candidateStaticTextRef
        end repeat
        set hasSimulationMarker to false
        repeat with candidateStaticTextRef in frozenWindowStaticTexts
          set s to missing value
          set staticTextAlive to false
          try
            set s to contents of candidateStaticTextRef
            set ignoredStaticTextRole to role of s
            set staticTextAlive to true
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          set markerValue to ""
          if staticTextAlive then try
            set markerValue to value of s as text
          on error errorMessage number errorNumber
            if errorNumber is -1719 or errorNumber is -1728 then set staticTextAlive to false
            if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          if staticTextAlive and markerValue is "" then
            try
              set markerValue to name of s as text
            on error errorMessage number errorNumber
              if errorNumber is -1719 or errorNumber is -1728 then set staticTextAlive to false
              if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
            end try
          end if
          if staticTextAlive and markerValue is "模拟练习" then set hasSimulationMarker to true
          if staticTextAlive and markerValue is "账户设置" then error "live account settings are visible"
        end repeat
        if hasSimulationMarker then
          set targetWindow to w
          set matchedWindowCount to matchedWindowCount + 1
        end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    set frozenTargetButtons to {{}}
    set liveTargetButtons to get (buttons of targetWindow)
    repeat with candidateButtonRef in liveTargetButtons
      set end of frozenTargetButtons to contents of candidateButtonRef
    end repeat
    set fillButtons to {{}}
    repeat with candidateButtonRef in frozenTargetButtons
      set candidateButton to contents of candidateButtonRef
      if (name of candidateButton as text) is "成交" then set end of fillButtons to candidateButton
    end repeat
    if (count of fillButtons) is not 1 then error "simulation fill tab is not unique"
    click item 1 of fillButtons
    delay 0.3
    set expectedHeaders to {{"成交日期", "成交时间", "证券代码", "证券名称", "操作", "成交数量", "成交均价", "成交金额", "合同编号", "成交编号", "成交类别"}}
    set matchedTable to missing value
    set matchedCount to 0
    set frozenScrollAreas to {{}}
    set liveScrollAreas to get (scroll areas of targetWindow)
    repeat with scrollAreaRef in liveScrollAreas
      set end of frozenScrollAreas to contents of scrollAreaRef
    end repeat
    repeat with scrollAreaRef in frozenScrollAreas
      set sa to contents of scrollAreaRef
      set frozenScrollChildren to {{}}
      set liveScrollChildren to get (UI elements of sa)
      repeat with childRef in liveScrollChildren
        set end of frozenScrollChildren to contents of childRef
      end repeat
      repeat with childRef in frozenScrollChildren
        set candidate to contents of childRef
        if role of candidate is "AXTable" then
          set frozenTableChildren to {{}}
          set liveTableChildren to get (UI elements of candidate)
          repeat with tableChildRef in liveTableChildren
            set end of frozenTableChildren to contents of tableChildRef
          end repeat
          repeat with tableChildRef in frozenTableChildren
            set tableChild to contents of tableChildRef
            if role of tableChild is "AXGroup" then
              set names to {{}}
              set frozenHeaders to {{}}
              set liveHeaders to get (UI elements of tableChild)
              repeat with headerRef in liveHeaders
                set end of frozenHeaders to contents of headerRef
              end repeat
              repeat with headerRef in frozenHeaders
                set headerElement to contents of headerRef
                if role of headerElement is "AXButton" then set end of names to name of headerElement as text
              end repeat
              if names is expectedHeaders then
                set matchedTable to candidate
                set matchedCount to matchedCount + 1
              end if
            end if
          end repeat
        end if
      end repeat
    end repeat
    if matchedCount is not 1 then error "simulation fill table is not unique"
    set out to {{"FILL_TABLE\t1"}}
    repeat with headerName in expectedHeaders
      set end of out to "HEADER\t" & headerName
    end repeat
    set frozenRows to {{}}
    set liveRows to get (rows of matchedTable)
    repeat with rowRef in liveRows
      set end of frozenRows to contents of rowRef
    end repeat
    set end of out to "ROWS\t" & (count of frozenRows)
    set rowIndex to 0
    repeat with rowRef in frozenRows
      set rowIndex to rowIndex + 1
      set tableRow to contents of rowRef
      set frozenCells to {{}}
      set liveCells to get (entire contents of tableRow)
      repeat with cellRef in liveCells
        set end of frozenCells to contents of cellRef
      end repeat
      set cellIndex to 0
      repeat with cellRef in frozenCells
        set cellElement to contents of cellRef
        if role of cellElement is "AXStaticText" then
          set cellIndex to cellIndex + 1
          set cellPosition to position of cellElement
          set cellValue to value of cellElement as text
          set end of out to "CELL\t" & rowIndex & "\t" & cellIndex & "\t" & (item 1 of cellPosition) & "\t" & cellValue
        end if
      end repeat
    end repeat
    set AppleScript's text item delimiters to linefeed
    return out as text
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
    )
}

pub fn probe_orders_with(executor: &dyn AppleScriptExecutor) -> Result<SimulationOrderTable> {
    parse_orders_probe(&executor.run(ORDERS_PROBE_SCRIPT)?)
}

pub fn parse_orders_probe(raw: &str) -> Result<SimulationOrderTable> {
    if let Some(error) = raw.strip_prefix("ERROR\t") {
        bail!("Tonghuashun order table is unavailable: {}", error.trim());
    }
    let mut found = false;
    let mut headers = Vec::new();
    let mut expected_rows = None;
    let mut positioned_rows = std::collections::BTreeMap::<usize, Vec<(usize, i64, String)>>::new();
    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let parts = line.split('\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["ORDER_TABLE", "1"] => found = true,
            ["HEADER", value] => headers.push((*value).to_owned()),
            ["ROWS", value] => expected_rows = Some(parse_count(value, "order row")?),
            ["CELL", row, ordinal, x, value] => {
                let row = parse_count(row, "order row index")?;
                if row == 0 {
                    bail!("order row index is one-based");
                }
                positioned_rows.entry(row).or_default().push((
                    parse_count(ordinal, "order cell ordinal")?,
                    parse_integer(x, "order cell x")?,
                    (*value).to_owned(),
                ));
            }
            _ => bail!("unexpected Tonghuashun order-table probe row: {line}"),
        }
    }
    if !found {
        bail!("Tonghuashun order table was not found");
    }
    let legacy_headers = [
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
    ];
    let report_id_headers = [
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
        "申报编号",
        "委托属性",
    ];
    if headers != legacy_headers && headers != report_id_headers {
        bail!("Tonghuashun order table headers changed");
    }
    let expected_rows = expected_rows.context("order-table probe lacks its row count")?;
    if positioned_rows.keys().any(|row| *row > expected_rows) {
        bail!("order-table probe contains a row outside its declared count");
    }
    let mut rows = Vec::with_capacity(expected_rows);
    for row_index in 1..=expected_rows {
        let mut cells = positioned_rows.remove(&row_index).unwrap_or_default();
        cells.sort_by_key(|(ordinal, _, _)| *ordinal);
        if cells
            .iter()
            .enumerate()
            .any(|(index, (ordinal, _, _))| *ordinal != index + 1)
        {
            bail!("order-table row has duplicate or non-contiguous cell ordinals");
        }
        let cells = cells
            .into_iter()
            .map(|(_, _, value)| value)
            .collect::<Vec<_>>();
        if cells.len() != headers.len() {
            bail!("order-table row does not contain every expected column");
        }
        rows.push(SimulationOrderTableRow { cells });
    }
    Ok(SimulationOrderTable { headers, rows })
}

pub fn parse_fills_probe(raw: &str) -> Result<SimulationFillTable> {
    if let Some(error) = raw.strip_prefix("ERROR\t") {
        bail!("Tonghuashun fill table is unavailable: {}", error.trim());
    }
    let mut found = false;
    let mut headers = Vec::new();
    let mut expected_rows = None;
    let mut positioned_rows = std::collections::BTreeMap::<usize, Vec<(usize, i64, String)>>::new();
    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let parts = line.split('\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["FILL_TABLE", "1"] => found = true,
            ["HEADER", value] => headers.push((*value).to_owned()),
            ["ROWS", value] => expected_rows = Some(parse_count(value, "fill row")?),
            ["CELL", row, ordinal, x, value] => {
                let row = parse_count(row, "fill row index")?;
                if row == 0 {
                    bail!("fill row index is one-based");
                }
                positioned_rows.entry(row).or_default().push((
                    parse_count(ordinal, "fill cell ordinal")?,
                    parse_integer(x, "fill cell x")?,
                    (*value).to_owned(),
                ));
            }
            _ => bail!("unexpected Tonghuashun fill-table probe row: {line}"),
        }
    }
    if !found {
        bail!("Tonghuashun fill table was not found");
    }
    let expected_headers = [
        "成交日期",
        "成交时间",
        "证券代码",
        "证券名称",
        "操作",
        "成交数量",
        "成交均价",
        "成交金额",
        "合同编号",
        "成交编号",
        "成交类别",
    ];
    if headers != expected_headers {
        bail!("Tonghuashun fill table headers changed");
    }
    let expected_rows = expected_rows.context("fill-table probe lacks its row count")?;
    if positioned_rows.keys().any(|row| *row > expected_rows) {
        bail!("fill-table probe contains a row outside its declared count");
    }
    let mut rows = Vec::with_capacity(expected_rows);
    for row_index in 1..=expected_rows {
        let mut cells = positioned_rows.remove(&row_index).unwrap_or_default();
        cells.sort_by_key(|(ordinal, _, _)| *ordinal);
        if cells
            .iter()
            .enumerate()
            .any(|(index, (ordinal, _, _))| *ordinal != index + 1)
        {
            bail!("fill-table row has duplicate or non-contiguous cell ordinals");
        }
        let cells = cells
            .into_iter()
            .map(|(_, _, value)| value)
            .collect::<Vec<_>>();
        if cells.len() != expected_headers.len() {
            bail!("fill-table row does not contain every expected column");
        }
        rows.push(SimulationFillTableRow { cells });
    }
    Ok(SimulationFillTable { headers, rows })
}

impl SimulationOrderTable {
    pub fn records(&self) -> Result<Vec<SimulationOrderRecord>> {
        self.rows.iter().map(parse_order_record).collect()
    }

    pub fn unique_new_match(
        &self,
        order: &SimulatedOrderDraft,
        baseline_contract_ids: &[String],
    ) -> Result<SimulationOrderRecord> {
        let baseline = baseline_contract_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        if baseline.len() != baseline_contract_ids.len() {
            bail!("baseline contract IDs are not unique");
        }
        let records = self.records()?;
        let unique_contracts = records
            .iter()
            .map(|record| record.contract_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if unique_contracts.len() != records.len() {
            bail!("Tonghuashun order table contains duplicate contract IDs");
        }
        let matches = records
            .into_iter()
            .filter(|record| {
                !baseline.contains(record.contract_id.as_str())
                    && record.symbol == order.symbol
                    && record.direction == order.direction
                    && record.quantity == order.quantity
                    && record.order_price == order.limit_price
            })
            .collect::<Vec<_>>();
        let [record] = matches.as_slice() else {
            bail!(
                "expected one new matching Tonghuashun contract, found {}",
                matches.len()
            );
        };
        Ok(record.clone())
    }
}

impl SimulationFillTable {
    pub fn records(&self) -> Result<Vec<SimulationFillRecord>> {
        let records = self
            .rows
            .iter()
            .map(parse_fill_record)
            .collect::<Result<Vec<_>>>()?;
        let unique_fill_ids = records
            .iter()
            .map(|record| record.fill_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if unique_fill_ids.len() != records.len() {
            bail!("Tonghuashun fill table contains duplicate fill IDs");
        }
        Ok(records)
    }
}

fn parse_order_record(row: &SimulationOrderTableRow) -> Result<SimulationOrderRecord> {
    let (
        order_date,
        order_time,
        symbol,
        security_name,
        operation,
        remark,
        quantity,
        cancelled,
        order_price,
        fill_price,
        contract_id,
        attribute,
    ) = match row.cells.as_slice() {
        [order_date, order_time, symbol, security_name, operation, remark, quantity, cancelled, order_price, fill_price, contract_id, attribute] => {
            (
                order_date,
                order_time,
                symbol,
                security_name,
                operation,
                remark,
                quantity,
                cancelled,
                order_price,
                fill_price,
                contract_id,
                attribute,
            )
        }
        [order_date, order_time, symbol, security_name, operation, remark, quantity, cancelled, order_price, fill_price, contract_id, _report_id, attribute] => {
            (
                order_date,
                order_time,
                symbol,
                security_name,
                operation,
                remark,
                quantity,
                cancelled,
                order_price,
                fill_price,
                contract_id,
                attribute,
            )
        }
        _ => bail!("Tonghuashun order row does not have a reviewed column shape"),
    };
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("Tonghuashun order row has an invalid A-share symbol");
    }
    let direction = match operation.as_str() {
        "买入" | "证券买入" => Direction::Buy,
        "卖出" | "证券卖出" => Direction::Sell,
        other => bail!("unsupported Tonghuashun order operation: {other}"),
    };
    if contract_id.is_empty()
        || contract_id.len() > 128
        || !contract_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("Tonghuashun order row has an invalid contract ID");
    }
    let fill_price = match fill_price.as_str() {
        "" | "-" | "--" => None,
        value => {
            let parsed = parse_decimal_cell(value, "fill price")?;
            if parsed.is_sign_negative() {
                bail!("Tonghuashun fill price cannot be negative");
            }
            if parsed.is_zero() {
                if matches!(remark.as_str(), "未报" | "已报" | "待报" | "未成交")
                    || remark.contains('撤')
                {
                    None
                } else {
                    bail!("Tonghuashun terminal or partial fill cannot have a zero price");
                }
            } else {
                Some(parsed.to_string())
            }
        }
    };
    Ok(SimulationOrderRecord {
        order_date: order_date.clone(),
        order_time: order_time.clone(),
        symbol: symbol.clone(),
        security_name: security_name.clone(),
        direction,
        remark: remark.clone(),
        quantity: parse_integer_cell(quantity, "order quantity")?,
        cancelled_quantity: parse_integer_cell(cancelled, "cancelled quantity")?,
        order_price: parse_decimal_cell(order_price, "order price")?,
        fill_price,
        contract_id: contract_id.clone(),
        order_attribute: attribute.clone(),
    })
}

fn parse_fill_record(row: &SimulationFillTableRow) -> Result<SimulationFillRecord> {
    let [fill_date, fill_time, symbol, security_name, operation, quantity, price, amount, contract_id, fill_id, fill_category] =
        row.cells.as_slice()
    else {
        bail!("Tonghuashun fill row does not have eleven cells");
    };
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("Tonghuashun fill row has an invalid A-share symbol");
    }
    let direction = match operation.as_str() {
        "买入" | "证券买入" => Direction::Buy,
        "卖出" | "证券卖出" => Direction::Sell,
        other => bail!("unsupported Tonghuashun fill operation: {other}"),
    };
    validate_remote_identity(contract_id, "contract")?;
    validate_remote_identity(fill_id, "fill")?;
    let quantity = parse_integer_cell(quantity, "fill quantity")?;
    let price = parse_decimal_cell(price, "fill price")?;
    let amount = parse_decimal_cell(amount, "fill amount")?;
    if quantity <= 0 || price <= Decimal::ZERO || amount <= Decimal::ZERO {
        bail!("Tonghuashun fill row has a non-positive quantity, price or amount");
    }
    let expected_amount = price
        .checked_mul(Decimal::from(quantity))
        .context("Tonghuashun fill amount overflow")?;
    if amount != expected_amount {
        bail!("Tonghuashun fill row amount does not equal price times quantity");
    }
    Ok(SimulationFillRecord {
        fill_date: fill_date.clone(),
        fill_time: fill_time.clone(),
        symbol: symbol.clone(),
        security_name: security_name.clone(),
        direction,
        quantity,
        price,
        amount,
        contract_id: contract_id.clone(),
        fill_id: fill_id.clone(),
        fill_category: fill_category.clone(),
    })
}

fn validate_remote_identity(value: &str, kind: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("Tonghuashun {kind} ID is invalid");
    }
    Ok(())
}

fn parse_integer_cell(value: &str, field: &str) -> Result<i64> {
    value
        .replace(',', "")
        .parse::<i64>()
        .with_context(|| format!("invalid Tonghuashun {field}: {value}"))
}

fn parse_decimal_cell(value: &str, field: &str) -> Result<Decimal> {
    value
        .replace(',', "")
        .parse::<Decimal>()
        .with_context(|| format!("invalid Tonghuashun {field}: {value}"))
}

pub fn probe_with(executor: &dyn AppleScriptExecutor) -> Result<ThsUiProbe> {
    let raw = executor.run(PROBE_SCRIPT)?;
    parse_probe(&raw)
}

pub fn parse_probe(raw: &str) -> Result<ThsUiProbe> {
    if let Some(error) = raw.strip_prefix("ERROR\t") {
        bail!("Tonghuashun UI is unavailable: {}", error.trim());
    }

    let mut probe = ThsUiProbe {
        bundle_id: String::new(),
        version: String::new(),
        simulation_marker_count: 0,
        live_transfer_button_count: 0,
        live_exit_button_count: 0,
        live_account_settings_count: 0,
        buy_button_count: 0,
        sell_button_count: 0,
        submit_labels: Vec::new(),
        text_fields: Vec::new(),
    };
    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let parts = line.split('\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["BUNDLE", value] => probe.bundle_id = (*value).to_owned(),
            ["VERSION", value] => probe.version = (*value).to_owned(),
            ["SUBMIT", value] => probe.submit_labels.push((*value).to_owned()),
            ["FIELD", x, y, width, height, value] => {
                probe.text_fields.push(AxTextField {
                    frame: AxRect {
                        x: parse_integer(x, "field x")?,
                        y: parse_integer(y, "field y")?,
                        width: parse_integer(width, "field width")?,
                        height: parse_integer(height, "field height")?,
                    },
                    value: (*value).to_owned(),
                });
            }
            ["MARKER", value] => probe.simulation_marker_count = parse_count(value, "marker")?,
            ["LIVE_TRANSFER", value] => {
                probe.live_transfer_button_count = parse_count(value, "transfer")?
            }
            ["LIVE_EXIT", value] => probe.live_exit_button_count = parse_count(value, "exit")?,
            ["LIVE_ACCOUNT_SETTINGS", value] => {
                probe.live_account_settings_count = parse_count(value, "account settings")?
            }
            ["BUY_BUTTON", value] => probe.buy_button_count = parse_count(value, "buy button")?,
            ["SELL_BUTTON", value] => probe.sell_button_count = parse_count(value, "sell button")?,
            _ => bail!("unexpected Tonghuashun accessibility probe row: {line}"),
        }
    }
    Ok(probe)
}

impl ThsUiProbe {
    pub fn verify_simulation(self) -> Result<VerifiedSimulationUi> {
        if self.bundle_id != THS_BUNDLE_ID {
            bail!("unexpected application bundle: {}", self.bundle_id);
        }
        if self.version != TESTED_THS_VERSION {
            bail!(
                "unsupported Tonghuashun version {}; expected {}",
                self.version,
                TESTED_THS_VERSION
            );
        }
        if self.simulation_marker_count != 1 {
            bail!(
                "the unique '{}' marker is not active (observed {}; fields={}, buy={}, sell={}, submits={})",
                SIMULATION_MARKER,
                self.simulation_marker_count,
                self.text_fields.len(),
                self.buy_button_count,
                self.sell_button_count,
                self.submit_labels.len()
            );
        }
        if self.live_transfer_button_count != 0
            || self.live_exit_button_count != 0
            || self.live_account_settings_count != 0
        {
            bail!("live-account controls are visible; refusing UI automation");
        }
        if self.buy_button_count != 1 || self.sell_button_count != 1 {
            bail!("buy/sell controls are not uniquely identifiable");
        }
        if self.submit_labels.len() != 1 {
            bail!("the submit control is not uniquely identifiable");
        }
        let active_direction = match self.submit_labels[0].as_str() {
            "确定买入" => Direction::Buy,
            "确定卖出" => Direction::Sell,
            other => bail!("unsupported submit control: {other}"),
        };
        if self.text_fields.len() != 3 {
            bail!(
                "expected exactly three order fields, found {}",
                self.text_fields.len()
            );
        }
        let mut fields = self.text_fields;
        fields.sort_by_key(|field| field.frame.y);
        for field in &fields {
            if !(60..=220).contains(&field.frame.width) || !(15..=36).contains(&field.frame.height)
            {
                bail!("order field geometry is outside the tested envelope");
            }
        }
        if fields[1].frame.y - fields[0].frame.y < 20
            || fields[1].frame.y - fields[0].frame.y > 80
            || fields[2].frame.y - fields[1].frame.y < 20
            || fields[2].frame.y - fields[1].frame.y > 80
        {
            bail!("order fields are not in the tested code/price/quantity layout");
        }
        let min_x = fields
            .iter()
            .map(|field| field.frame.x)
            .min()
            .unwrap_or_default();
        let max_x = fields
            .iter()
            .map(|field| field.frame.x)
            .max()
            .unwrap_or_default();
        if max_x - min_x > 40 {
            bail!("order fields are not horizontally aligned");
        }
        Ok(VerifiedSimulationUi {
            bundle_id: self.bundle_id,
            version: self.version,
            marker: SIMULATION_MARKER.to_owned(),
            active_direction,
            code_field: fields.remove(0),
            price_field: fields.remove(0),
            quantity_field: fields.remove(0),
            submit_label: self.submit_labels.into_iter().next().unwrap_or_default(),
        })
    }
}

impl SimulatedOrderDraft {
    pub fn new(
        direction: Direction,
        symbol: impl Into<String>,
        limit_price: Decimal,
        quantity: i64,
    ) -> Result<Self> {
        let symbol = symbol.into();
        validate_a_share_symbol(&symbol)?;
        if limit_price <= Decimal::ZERO || limit_price.scale() > 3 {
            bail!("limit price must be positive with no more than three decimals");
        }
        if quantity <= 0 || quantity % 100 != 0 {
            bail!("simulated order quantity must be a positive board-lot multiple of 100");
        }
        if quantity > MAX_SIMULATED_ORDER_QUANTITY {
            bail!("simulated order quantity exceeds the safety ceiling");
        }
        let notional = limit_price
            .checked_mul(Decimal::from(quantity))
            .context("simulated order notional overflow")?;
        if notional > MAX_SIMULATED_ORDER_NOTIONAL {
            bail!("simulated order notional exceeds the safety ceiling");
        }
        Ok(Self {
            direction,
            symbol,
            limit_price,
            quantity,
        })
    }
}

fn validate_a_share_symbol(symbol: &str) -> Result<()> {
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("A-share symbol must contain exactly six digits");
    }
    Ok(())
}

pub fn dry_run(probe: ThsUiProbe, order: SimulatedOrderDraft) -> Result<DryRunPlan> {
    let ui = probe.verify_simulation()?;
    if ui.active_direction != order.direction {
        bail!("active UI direction does not match the requested dry-run direction");
    }
    Ok(DryRunPlan {
        ui,
        order,
        would_submit: false,
    })
}

pub fn prepare_current_form(order: SimulatedOrderDraft) -> Result<PreparedSimulationForm> {
    prepare_with(&SystemOsaScript, order)
}

pub fn prepare_with(
    executor: &dyn AppleScriptExecutor,
    order: SimulatedOrderDraft,
) -> Result<PreparedSimulationForm> {
    let before = probe_with(executor)?.verify_simulation()?;
    let source = preparation_script(&order, before.active_direction);
    let result = executor.run(&source)?;
    if result.trim() != "PREPARED" {
        bail!("Tonghuashun form preparation did not acknowledge completion");
    }
    let ui = probe_with(executor)?.verify_simulation()?;
    verify_prepared_echo(&ui, &order)?;
    Ok(PreparedSimulationForm {
        ui,
        order,
        submitted: false,
    })
}

pub fn click_prepared_form_with(
    executor: &dyn AppleScriptExecutor,
    order: &SimulatedOrderDraft,
) -> Result<()> {
    let ui = probe_with(executor)?.verify_simulation()?;
    verify_prepared_echo(&ui, order)?;
    executor.verify_money_action_window()?;
    let result = executor.run(&submission_script(order))?;
    if result.trim() != "CLICKED" {
        bail!("Tonghuashun submit control did not acknowledge its click");
    }
    Ok(())
}

pub fn confirm_prepared_form_with(
    executor: &dyn AppleScriptExecutor,
    order: &SimulatedOrderDraft,
) -> Result<()> {
    executor.verify_money_action_window()?;
    let result = executor.run(&confirmation_script(order))?;
    if result.trim() != "CONFIRMED" {
        bail!("Tonghuashun confirmation dialog did not acknowledge its click");
    }
    Ok(())
}

pub fn submit_prepared_form_with(
    executor: &dyn AppleScriptExecutor,
    order: &SimulatedOrderDraft,
) -> Result<()> {
    click_prepared_form_with(executor, order)?;
    confirm_prepared_form_with(executor, order)
}

pub fn cancel_contract_with(
    executor: &dyn AppleScriptExecutor,
    remote_contract_id: &str,
) -> Result<()> {
    validate_remote_contract_id(remote_contract_id)?;
    executor.verify_money_action_window()?;
    let preflight = executor.run(&cancellation_script(remote_contract_id))?;
    let (x, y) = parse_cancellation_preflight(&preflight)?;
    executor.native_double_click(x, y)?;
    executor.verify_money_action_window()?;
    let result = executor.run(&cancellation_confirmation_script())?;
    if result.trim() != "CANCELLED_CLICKED" {
        bail!("Tonghuashun cancel dialog did not acknowledge its click");
    }
    Ok(())
}

fn parse_cancellation_preflight(value: &str) -> Result<(i32, i32)> {
    let fields = value.trim().split('\t').collect::<Vec<_>>();
    let ["CANCEL_READY", x, y] = fields.as_slice() else {
        bail!("Tonghuashun cancellation preflight did not return one reviewed point");
    };
    let x = x
        .parse::<i32>()
        .context("Tonghuashun cancellation X coordinate is not an integer")?;
    let y = y
        .parse::<i32>()
        .context("Tonghuashun cancellation Y coordinate is not an integer")?;
    Ok((x, y))
}

fn verify_prepared_echo(ui: &VerifiedSimulationUi, order: &SimulatedOrderDraft) -> Result<()> {
    if ui.active_direction != order.direction {
        bail!("Tonghuashun direction changed while preparing the form");
    }
    if ui.code_field.value != order.symbol {
        bail!("Tonghuashun code field does not match the prepared order");
    }
    let observed_price = ui
        .price_field
        .value
        .parse::<Decimal>()
        .context("Tonghuashun price field is not a decimal")?;
    if observed_price != order.limit_price {
        bail!("Tonghuashun price field does not match the prepared order");
    }
    let observed_quantity = ui
        .quantity_field
        .value
        .parse::<i64>()
        .context("Tonghuashun quantity field is not an integer")?;
    if observed_quantity != order.quantity {
        bail!("Tonghuashun quantity field does not match the prepared order");
    }
    Ok(())
}

fn submission_script(order: &SimulatedOrderDraft) -> String {
    let requested_submit = match order.direction {
        Direction::Buy => "确定买入",
        Direction::Sell => "确定卖出",
    };
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {{}}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {{}}
      try
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
      set hasSimulationMarker to false
      set frozenWindowStaticTexts to {{}}
      try
        set liveWindowStaticTexts to get (static texts of w)
        repeat with candidateStaticTextRef in liveWindowStaticTexts
          set end of frozenWindowStaticTexts to contents of candidateStaticTextRef
        end repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      repeat with candidateStaticTextRef in frozenWindowStaticTexts
        set s to missing value
        try
          set s to contents of candidateStaticTextRef
          set ignoredStaticTextRole to role of s
        end try
        if s is not missing value then
          set markerValue to ""
          try
            set markerValue to value of s as text
          end try
          if markerValue is "" then
            try
              set markerValue to name of s as text
            end try
          end if
          if markerValue is "模拟练习" then set hasSimulationMarker to true
        end if
      end repeat
      if hasSimulationMarker then
        set targetWindow to w
        set matchedWindowCount to matchedWindowCount + 1
      end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set frozenTargetStaticTexts to {{}}
    set liveTargetStaticTexts to get (static texts of targetWindow)
    repeat with candidateStaticTextRef in liveTargetStaticTexts
      set end of frozenTargetStaticTexts to contents of candidateStaticTextRef
    end repeat
    repeat with candidateStaticTextRef in frozenTargetStaticTexts
      set s to missing value
      try
        set s to contents of candidateStaticTextRef
        set ignoredStaticTextRole to role of s
      end try
      if s is not missing value then
        set accountLabel to ""
        try
          set accountLabel to value of s as text
        end try
        if accountLabel is "" then
          try
            set accountLabel to name of s as text
          end try
        end if
        if accountLabel is "账户设置" then error "live account settings are visible"
      end if
    end repeat
    set submitButtons to {{}}
    repeat with candidateButton in buttons of targetWindow
      set candidateButtonRef to contents of candidateButton
      try
        if (name of candidateButtonRef as text) is "{requested_submit}" then set end of submitButtons to candidateButtonRef
      end try
    end repeat
    if (count of submitButtons) is not 1 then error "requested submit direction is not uniquely active"
    set submitButton to item 1 of submitButtons
    set fields to {{}}
    repeat with f in text fields of targetWindow
      set end of fields to contents of f
    end repeat
    if (count of fields) is not 3 then error "simulation order form does not have exactly three fields"
    set codeField to item 1 of fields
    set priceField to item 1 of fields
    set quantityField to item 1 of fields
    repeat with f in fields
      set fRef to contents of f
      set fPosition to position of fRef
      set codePosition to position of codeField
      set quantityPosition to position of quantityField
      if item 2 of fPosition < item 2 of codePosition then set codeField to fRef
      if item 2 of fPosition > item 2 of quantityPosition then set quantityField to fRef
    end repeat
    repeat with f in fields
      set fRef to contents of f
      if fRef is not codeField and fRef is not quantityField then set priceField to fRef
    end repeat
    if value of codeField as text is not "{symbol}" then error "code field changed before submit"
    if value of priceField as text is not "{price}" then error "price field changed before submit"
    if value of quantityField as text is not "{quantity}" then error "quantity field changed before submit"
    click submitButton
    return "CLICKED"
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
        requested_submit = requested_submit,
        symbol = order.symbol,
        price = order.limit_price,
        quantity = order.quantity,
    )
}

fn confirmation_script(order: &SimulatedOrderDraft) -> String {
    let direction = match order.direction {
        Direction::Buy => "买入",
        Direction::Sell => "卖出",
    };
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set simulationWindowCount to 0
    set frozenWindows to {{}}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {{}}
      try
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
      set hasSimulationMarker to false
      set frozenCancelWindowStaticTexts to {{}}
      try
        set liveCancelWindowStaticTexts to get (static texts of w)
        repeat with candidateStaticTextRef in liveCancelWindowStaticTexts
          set end of frozenCancelWindowStaticTexts to contents of candidateStaticTextRef
        end repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      repeat with candidateStaticTextRef in frozenCancelWindowStaticTexts
        set s to missing value
        try
          set s to contents of candidateStaticTextRef
          set ignoredStaticTextRole to role of s
        end try
        if s is not missing value then
          set markerValue to ""
          try
            set markerValue to value of s as text
          end try
          if markerValue is "" then
            try
              set markerValue to name of s as text
            end try
          end if
          if markerValue is "模拟练习" then set hasSimulationMarker to true
        end if
      end repeat
      if hasSimulationMarker then
        set simulationWindowCount to simulationWindowCount + 1
        if exists button "转账" of w then error "live transfer control is visible"
        if exists button "退出" of w then error "live account exit control is visible"
        set observedStaticTextCount to 0
        try
          set observedStaticTextCount to count of static texts of w
        on error errorMessage number errorNumber
          if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
        end try
        repeat with staticTextIndex from 1 to observedStaticTextCount
          set s to missing value
          try
            set s to item staticTextIndex of static texts of w
          end try
          if s is not missing value then
            set accountLabel to ""
            try
              set accountLabel to value of s as text
            end try
            if accountLabel is "" then
              try
                set accountLabel to name of s as text
              end try
            end if
            if accountLabel is "账户设置" then error "live account settings are visible"
          end if
        end repeat
      end if
      end if
    end repeat
    if simulationWindowCount is not 1 then error "unique simulation account is not active"
    set confirmationSheet to value of attribute "AXFocusedUIElement"
    if role of confirmationSheet is not "AXSheet" then error "focused control is not a confirmation sheet"
    if description of confirmationSheet is not "警告" then error "unexpected confirmation sheet description"
    set sheetButtons to {{}}
    repeat with candidateButton in buttons of confirmationSheet
      set end of sheetButtons to contents of candidateButton
    end repeat
    if (count of sheetButtons) is not 2 then error "confirmation sheet button count changed"
    set confirmButtons to {{}}
    set cancelButtons to {{}}
    repeat with candidateButton in sheetButtons
      set candidateButtonRef to contents of candidateButton
      try
        if (name of candidateButtonRef as text) is "确认" then set end of confirmButtons to candidateButtonRef
        if (name of candidateButtonRef as text) is "取消" then set end of cancelButtons to candidateButtonRef
      end try
    end repeat
    if (count of confirmButtons) is not 1 then error "confirmation action is not unique"
    if (count of cancelButtons) is not 1 then error "confirmation cancel action is not unique"
    set confirmButton to item 1 of confirmButtons
    set messageText to ""
    set observedStaticTextCount to count of static texts of confirmationSheet
    repeat with staticTextIndex from 1 to observedStaticTextCount
      try
        set s to item staticTextIndex of static texts of confirmationSheet
        set messageText to messageText & (value of s as text) & linefeed
      end try
    end repeat
    if not (messageText contains "您是否确认以上{direction}委托?" and messageText contains "证券代码:{symbol}" and messageText contains "当前价格:{price}" and messageText contains "{direction}数量:{quantity}") then error "exact simulation confirmation dialog not found"
    click confirmButton
    return "CONFIRMED"
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
        direction = direction,
        symbol = order.symbol,
        price = order.limit_price,
        quantity = order.quantity,
    )
}

fn cancellation_script(remote_contract_id: &str) -> String {
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {{}}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {{}}
      try
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
      set hasSimulationMarker to false
      set frozenCancelWindowStaticTexts to {{}}
      try
        set liveCancelWindowStaticTexts to get (static texts of w)
        repeat with candidateStaticTextRef in liveCancelWindowStaticTexts
          set end of frozenCancelWindowStaticTexts to contents of candidateStaticTextRef
        end repeat
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      repeat with candidateStaticTextRef in frozenCancelWindowStaticTexts
        set s to missing value
        try
          set s to contents of candidateStaticTextRef
          set ignoredStaticTextRole to role of s
        end try
        if s is not missing value then
          set markerValue to ""
          try
            set markerValue to value of s as text
          end try
          if markerValue is "" then
            try
              set markerValue to name of s as text
            end try
          end if
          if markerValue is "模拟练习" then set hasSimulationMarker to true
        end if
      end repeat
      if hasSimulationMarker then
        set targetWindow to contents of w
        set matchedWindowCount to matchedWindowCount + 1
      end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set frozenCancelTargetStaticTexts to {{}}
    set liveCancelTargetStaticTexts to get (static texts of targetWindow)
    repeat with candidateStaticTextRef in liveCancelTargetStaticTexts
      set end of frozenCancelTargetStaticTexts to contents of candidateStaticTextRef
    end repeat
    repeat with candidateStaticTextRef in frozenCancelTargetStaticTexts
      set s to missing value
      try
        set s to contents of candidateStaticTextRef
        set ignoredStaticTextRole to role of s
      end try
      if s is not missing value then
        set accountLabel to ""
        try
          set accountLabel to value of s as text
        end try
        if accountLabel is "" then
          try
            set accountLabel to name of s as text
          end try
        end if
        if accountLabel is "账户设置" then error "live account settings are visible"
      end if
    end repeat
    set commissionButtons to {{}}
    set frozenTargetButtons to {{}}
    set liveTargetButtons to get (buttons of targetWindow)
    repeat with candidateButtonRef in liveTargetButtons
      set end of frozenTargetButtons to contents of candidateButtonRef
    end repeat
    repeat with frozenButtonRef in frozenTargetButtons
      set candidateButtonRef to contents of frozenButtonRef
      try
        if (name of candidateButtonRef as text) is "委托" then set end of commissionButtons to candidateButtonRef
      end try
    end repeat
    if (count of commissionButtons) is not 1 then error "simulation commission tab is not unique"
    set commissionButton to item 1 of commissionButtons
    click commissionButton
    delay 0.3
    set frozenStaticTexts to {{}}
    set liveFilterStaticTexts to get (static texts of targetWindow)
    repeat with candidateStaticTextRef in liveFilterStaticTexts
      set end of frozenStaticTexts to contents of candidateStaticTextRef
    end repeat
    set filterLabels to {{}}
    repeat with candidateLabel in frozenStaticTexts
      try
        set candidateLabelRef to contents of candidateLabel
        if (value of candidateLabelRef as text) is "只显示可撤委托" then set end of filterLabels to candidateLabelRef
      end try
    end repeat
    if (count of filterLabels) is not 1 then error "simulation order filter label is not unique"
    set filterLabel to item 1 of filterLabels
    set filterLabelPosition to position of filterLabel
    set frozenCheckboxes to {{}}
    set liveTargetCheckboxes to get (checkboxes of targetWindow)
    repeat with candidateCheckboxRef in liveTargetCheckboxes
      set end of frozenCheckboxes to contents of candidateCheckboxRef
    end repeat
    set cancellableOnlyCheckbox to missing value
    set filterCheckboxCount to 0
    set observedCheckboxCount to count of frozenCheckboxes
    repeat with checkboxIndex from 1 to observedCheckboxCount
      try
        set candidateCheckbox to item checkboxIndex of frozenCheckboxes
        set checkboxPosition to position of candidateCheckbox
        if (item 1 of checkboxPosition) < (item 1 of filterLabelPosition) and ((item 1 of filterLabelPosition) - (item 1 of checkboxPosition)) <= 30 and (item 2 of checkboxPosition) >= ((item 2 of filterLabelPosition) - 10) and (item 2 of checkboxPosition) <= ((item 2 of filterLabelPosition) + 10) then
          set cancellableOnlyCheckbox to candidateCheckbox
          set filterCheckboxCount to filterCheckboxCount + 1
        end if
      end try
    end repeat
    if filterCheckboxCount is not 1 then error "simulation order filter checkbox is not unique"
    if (value of cancellableOnlyCheckbox as integer) is 0 then
      click cancellableOnlyCheckbox
      delay 0.3
    end if
    set expectedHeaders to {{"委托日期", "委托时间", "证券代码", "证券名称", "操作", "备注", "委托数量", "撤销数量", "委托价格", "成交价格", "合同编号", "委托属性"}}
    set expectedHeadersWithReportId to {{"委托日期", "委托时间", "证券代码", "证券名称", "操作", "备注", "委托数量", "撤销数量", "委托价格", "成交价格", "合同编号", "申报编号", "委托属性"}}
    set matchedTable to missing value
    set matchedTableCount to 0
    set frozenScrollAreas to {{}}
    set liveScrollAreas to get (scroll areas of targetWindow)
    repeat with scrollAreaRef in liveScrollAreas
      set end of frozenScrollAreas to contents of scrollAreaRef
    end repeat
    repeat with frozenScrollAreaRef in frozenScrollAreas
      set sa to contents of frozenScrollAreaRef
      set frozenScrollChildren to {{}}
      set liveScrollChildren to get (UI elements of sa)
      repeat with scrollChildRef in liveScrollChildren
        set end of frozenScrollChildren to contents of scrollChildRef
      end repeat
      repeat with frozenScrollChildRef in frozenScrollChildren
        set candidate to contents of frozenScrollChildRef
        try
          if role of candidate is "AXTable" then
            set frozenTableChildren to {{}}
            set liveTableChildren to get (UI elements of candidate)
            repeat with tableChildRef in liveTableChildren
              set end of frozenTableChildren to contents of tableChildRef
            end repeat
            repeat with frozenTableChildRef in frozenTableChildren
              set child to contents of frozenTableChildRef
              if role of child is "AXGroup" then
                set names to {{}}
                set frozenHeaderElements to {{}}
                set liveHeaderElements to get (UI elements of child)
                repeat with headerElementRef in liveHeaderElements
                  set end of frozenHeaderElements to contents of headerElementRef
                end repeat
                repeat with frozenHeaderElementRef in frozenHeaderElements
                  set header to contents of frozenHeaderElementRef
                  if role of header is "AXButton" then
                    try
                      set end of names to name of header as text
                    end try
                  end if
                end repeat
                if names is expectedHeaders or names is expectedHeadersWithReportId then
                  set matchedTable to contents of candidate
                  set matchedTableCount to matchedTableCount + 1
                end if
              end if
            end repeat
          end if
        end try
      end repeat
    end repeat
    if matchedTableCount is not 1 then error "simulation cancellable-order table is not unique"
    set cancellableRows to {{}}
    set liveRows to get (rows of matchedTable)
    repeat with candidateRowRef in liveRows
      set end of cancellableRows to contents of candidateRowRef
    end repeat
    set matchingRows to {{}}
    repeat with frozenCandidateRowRef in cancellableRows
      set candidateRow to contents of frozenCandidateRowRef
      set contractCells to {{}}
      set liveCells to get (entire contents of candidateRow)
      repeat with candidateCellRef in liveCells
        set end of contractCells to contents of candidateCellRef
      end repeat
      set rowContractMatchCount to 0
      repeat with cell in contractCells
        try
          set cellRef to contents of cell
          if (value of cellRef as text) is "{contract_id}" then set rowContractMatchCount to rowContractMatchCount + 1
        end try
      end repeat
      if rowContractMatchCount is 1 then set end of matchingRows to candidateRow
    end repeat
    if (count of matchingRows) is not 1 then error "frozen contract is not one unique cancellable row"
    set cancellableRow to item 1 of matchingRows
    set cancellableRowPosition to position of cancellableRow
    set cancellableRowSize to size of cancellableRow
    if (item 1 of cancellableRowSize) <= 0 or (item 2 of cancellableRowSize) <= 0 then error "cancellable row has invalid geometry"
    set cancellableRowClickPoint to {{(item 1 of cancellableRowPosition) + ((item 1 of cancellableRowSize) div 2), (item 2 of cancellableRowPosition) + ((item 2 of cancellableRowSize) div 2)}}
    set targetWindowPosition to position of targetWindow
    set targetWindowSize to size of targetWindow
    if (item 1 of targetWindowSize) <= 0 or (item 2 of targetWindowSize) <= 0 then error "simulation window has invalid geometry"
    if (item 1 of cancellableRowClickPoint) < (item 1 of targetWindowPosition) or (item 1 of cancellableRowClickPoint) > ((item 1 of targetWindowPosition) + (item 1 of targetWindowSize)) or (item 2 of cancellableRowClickPoint) < (item 2 of targetWindowPosition) or (item 2 of cancellableRowClickPoint) > ((item 2 of targetWindowPosition) + (item 2 of targetWindowSize)) then error "cancellable row click point left the simulation window"
    return "CANCEL_READY" & tab & ((item 1 of cancellableRowClickPoint) as text) & tab & ((item 2 of cancellableRowClickPoint) as text)
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
        contract_id = remote_contract_id,
    )
}

fn cancellation_confirmation_script() -> String {
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set confirmationSheet to missing value
    repeat with confirmationAttempt from 1 to 20
      delay 0.05
      try
        set focusedControl to value of attribute "AXFocusedUIElement"
        if role of focusedControl is "AXSheet" then set confirmationSheet to contents of focusedControl
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if confirmationSheet is not missing value then exit repeat
    end repeat
    if confirmationSheet is missing value then error "cancellation confirmation sheet did not appear"
    if role of confirmationSheet is not "AXSheet" then error "focused control is not a cancellation sheet"
    if description of confirmationSheet is not "警告" then error "unexpected cancellation sheet description"
    set sheetButtons to {{}}
    set liveSheetButtons to get (buttons of confirmationSheet)
    repeat with candidateButtonRef in liveSheetButtons
      set end of sheetButtons to contents of candidateButtonRef
    end repeat
    if (count of sheetButtons) is not 2 then error "cancellation sheet button count changed"
    set confirmButtons to {{}}
    set cancelButtons to {{}}
    repeat with candidateButton in sheetButtons
      set candidateButtonRef to contents of candidateButton
      try
        if (name of candidateButtonRef as text) is "确认" then set end of confirmButtons to candidateButtonRef
        if (name of candidateButtonRef as text) is "取消" then set end of cancelButtons to candidateButtonRef
      end try
    end repeat
    if (count of confirmButtons) is not 1 then error "cancellation confirmation action is not unique"
    if (count of cancelButtons) is not 1 then error "cancellation cancel action is not unique"
    set confirmButton to item 1 of confirmButtons
    set messageText to ""
    set frozenConfirmationStaticTexts to {{}}
    set liveConfirmationStaticTexts to get (static texts of confirmationSheet)
    repeat with candidateStaticTextRef in liveConfirmationStaticTexts
      set end of frozenConfirmationStaticTexts to contents of candidateStaticTextRef
    end repeat
    repeat with candidateStaticTextRef in frozenConfirmationStaticTexts
      try
        set s to contents of candidateStaticTextRef
        set ignoredStaticTextRole to role of s
        set messageText to messageText & (value of s as text) & linefeed
      end try
    end repeat
    if not (messageText contains "您确定要撤销这1笔委托?") then error "exact one-contract cancel confirmation not found"
    click confirmButton
    return "CANCELLED_CLICKED"
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
    )
}

fn validate_remote_contract_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("Tonghuashun contract ID is not canonical");
    }
    Ok(())
}

fn market_quote_refresh_script(symbol: &str) -> String {
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {{}}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {{}}
      try
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
        set hasSimulationMarker to false
        set frozenWindowStaticTexts to {{}}
        try
          set liveWindowStaticTexts to get (static texts of w)
          repeat with candidateStaticTextRef in liveWindowStaticTexts
            set end of frozenWindowStaticTexts to contents of candidateStaticTextRef
          end repeat
        on error errorMessage number errorNumber
          if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
        end try
        repeat with candidateStaticTextRef in frozenWindowStaticTexts
          set markerText to contents of candidateStaticTextRef
          set markerTextAlive to false
          try
            set ignoredMarkerTextRole to role of markerText
            set markerTextAlive to true
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          set markerValue to ""
          if markerTextAlive then try
            set markerValue to value of markerText as text
          on error errorMessage number errorNumber
            if errorNumber is -1719 or errorNumber is -1728 then set markerTextAlive to false
            if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          if markerTextAlive and markerValue is "" then
            try
              set markerValue to name of markerText as text
            on error errorMessage number errorNumber
              if errorNumber is -1719 or errorNumber is -1728 then set markerTextAlive to false
              if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
            end try
          end if
          if markerTextAlive and markerValue is "模拟练习" then set hasSimulationMarker to true
        end repeat
        if hasSimulationMarker then
          set targetWindow to contents of w
          set matchedWindowCount to matchedWindowCount + 1
        end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set fields to {{}}
    repeat with candidateField in text fields of targetWindow
      set end of fields to contents of candidateField
    end repeat
    if (count of fields) is not 3 then error "simulation order form does not have exactly three fields"
    set codeField to item 1 of fields
    set quantityField to item 1 of fields
    repeat with candidateField in fields
      set fieldRef to contents of candidateField
      set fieldPosition to position of fieldRef
      set codePosition to position of codeField
      set quantityPosition to position of quantityField
      if item 2 of fieldPosition < item 2 of codePosition then set codeField to fieldRef
      if item 2 of fieldPosition > item 2 of quantityPosition then set quantityField to fieldRef
    end repeat
    set priceField to missing value
    repeat with candidateField in fields
      set fieldRef to contents of candidateField
      if fieldRef is not codeField and fieldRef is not quantityField then set priceField to fieldRef
    end repeat
    if priceField is missing value then error "simulation quote field is missing"
    set value of codeField to "{symbol}"
    set focused of codeField to true
    key code 48
    set quoteRefreshReady to false
    set refreshedLastPrice to ""
    repeat with quoteRefreshAttempt from 1 to 20
      delay 0.05
      set quoteAttemptReady to false
      set frozenQuoteStaticTexts to {{}}
      try
        set attemptFrozenWindows to {{}}
        set attemptLiveWindows to get every window
        set attemptLiveWindowCount to count of attemptLiveWindows
        repeat with attemptWindowRef in attemptLiveWindows
          set end of attemptFrozenWindows to contents of attemptWindowRef
        end repeat
        if (count of attemptFrozenWindows) is not attemptLiveWindowCount then error "unstable Tonghuashun quote window snapshot"
        set attemptTargetWindow to missing value
        set attemptMatchedWindowCount to 0
        repeat with attemptWindowRef in attemptFrozenWindows
          set attemptWindow to contents of attemptWindowRef
          set ignoredAttemptWindowRole to role of attemptWindow
          if subrole of attemptWindow is "AXStandardWindow" then
            set attemptWindowStaticTexts to {{}}
            set attemptLiveStaticTexts to get (static texts of attemptWindow)
            repeat with attemptStaticTextRef in attemptLiveStaticTexts
              set end of attemptWindowStaticTexts to contents of attemptStaticTextRef
            end repeat
            set attemptHasSimulationMarker to false
            repeat with attemptStaticTextRef in attemptWindowStaticTexts
              set attemptStaticText to contents of attemptStaticTextRef
              set attemptStaticTextAlive to false
              try
                set ignoredAttemptStaticTextRole to role of attemptStaticText
                set attemptStaticTextAlive to true
              on error errorMessage number errorNumber
                if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              set attemptMarkerValue to ""
              if attemptStaticTextAlive then try
                set attemptMarkerValue to value of attemptStaticText as text
              on error errorMessage number errorNumber
                if errorNumber is -1719 or errorNumber is -1728 then set attemptStaticTextAlive to false
                if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
              if attemptStaticTextAlive and attemptMarkerValue is "" then
                try
                  set attemptMarkerValue to name of attemptStaticText as text
                on error errorMessage number errorNumber
                  if errorNumber is -1719 or errorNumber is -1728 then set attemptStaticTextAlive to false
                  if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
                end try
              end if
              if attemptStaticTextAlive and attemptMarkerValue is "模拟练习" then set attemptHasSimulationMarker to true
            end repeat
            if attemptHasSimulationMarker then
              set attemptTargetWindow to attemptWindow
              set attemptMatchedWindowCount to attemptMatchedWindowCount + 1
            end if
          end if
        end repeat
        if attemptMatchedWindowCount is not 1 then error "unique simulation order window not found after symbol commit"
        if exists button "转账" of attemptTargetWindow then error "live transfer control is visible after symbol commit"
        if exists button "退出" of attemptTargetWindow then error "live account exit control is visible after symbol commit"
        if exists static text "账户设置" of attemptTargetWindow then error "live account settings are visible after symbol commit"
        set attemptFields to {{}}
        set attemptLiveFields to get (text fields of attemptTargetWindow)
        repeat with attemptFieldRef in attemptLiveFields
          set end of attemptFields to contents of attemptFieldRef
        end repeat
        if (count of attemptFields) is not 3 then error "simulation order form changed after symbol commit"
        set codeField to item 1 of attemptFields
        set quantityField to item 1 of attemptFields
        repeat with attemptFieldRef in attemptFields
          set attemptField to contents of attemptFieldRef
          set attemptFieldPosition to position of attemptField
          set codePosition to position of codeField
          set quantityPosition to position of quantityField
          if item 2 of attemptFieldPosition < item 2 of codePosition then set codeField to attemptField
          if item 2 of attemptFieldPosition > item 2 of quantityPosition then set quantityField to attemptField
        end repeat
        set priceField to missing value
        repeat with attemptFieldRef in attemptFields
          set attemptField to contents of attemptFieldRef
          if attemptField is not codeField and attemptField is not quantityField then set priceField to attemptField
        end repeat
        if priceField is missing value then error "simulation quote field is missing after symbol commit"
        set targetWindow to attemptTargetWindow
        set codeFieldPosition to position of codeField
        set priceFieldPosition to position of priceField
        set observedSymbol to value of codeField as text
        set frozenQuoteStaticTexts to {{}}
        set liveQuoteStaticTexts to get (static texts of targetWindow)
        repeat with candidateSecurityTextRef in liveQuoteStaticTexts
          set end of frozenQuoteStaticTexts to contents of candidateSecurityTextRef
        end repeat
        set quoteAttemptReady to true
      set linkedSecurityName to ""
      set linkedSecurityNames to {{}}
      set quoteAnchorPositions to {{}}
      set quotePriceLabels to {{}}
      set quotePricePositions to {{}}
      if quoteAttemptReady then
        repeat with candidateSecurityTextRef in frozenQuoteStaticTexts
          set candidateSecurityAlive to false
          try
            set candidateSecurityText to contents of candidateSecurityTextRef
            set ignoredCandidateSecurityRole to role of candidateSecurityText
            set candidateSecurityPosition to position of candidateSecurityText
            set candidateSecurityAlive to true
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          if candidateSecurityAlive then
            set candidateSecurityLabel to ""
            try
              set candidateSecurityLabel to value of candidateSecurityText as text
            on error errorMessage number errorNumber
              if errorNumber is -1719 or errorNumber is -1728 then set candidateSecurityAlive to false
              if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
            end try
            if candidateSecurityAlive and candidateSecurityLabel is "" then
              try
                set candidateSecurityLabel to name of candidateSecurityText as text
              on error errorMessage number errorNumber
                if errorNumber is -1719 or errorNumber is -1728 then set candidateSecurityAlive to false
                if errorNumber is not -1700 and errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
              end try
            end if
            if candidateSecurityAlive then
              if candidateSecurityLabel is not "" and (item 1 of candidateSecurityPosition) >= (item 1 of codeFieldPosition) and (item 2 of candidateSecurityPosition) > (item 2 of codeFieldPosition) and (item 2 of candidateSecurityPosition) < (item 2 of priceFieldPosition) then set end of linkedSecurityNames to candidateSecurityLabel
              if candidateSecurityLabel is "限价委托" then set end of quoteAnchorPositions to candidateSecurityPosition
              if candidateSecurityLabel ends with "↑" or candidateSecurityLabel ends with "↓" then
                set end of quotePriceLabels to candidateSecurityLabel
                set end of quotePricePositions to candidateSecurityPosition
              end if
            end if
          end if
        end repeat
        if (count of linkedSecurityNames) is 1 then set linkedSecurityName to item 1 of linkedSecurityNames
        set refreshedLastPriceCandidates to {{}}
        if (count of quoteAnchorPositions) is 1 then
          set quoteAnchorPosition to item 1 of quoteAnchorPositions
          repeat with quotePriceIndex from 1 to count of quotePriceLabels
            set quotePricePosition to item quotePriceIndex of quotePricePositions
            if (item 1 of quotePricePosition) > (item 1 of quoteAnchorPosition) and (item 2 of quotePricePosition) < (item 2 of quoteAnchorPosition) and ((item 2 of quoteAnchorPosition) - (item 2 of quotePricePosition)) <= 60 then
              set quotePriceLabel to item quotePriceIndex of quotePriceLabels
              set quotePriceText to text 1 thru -2 of quotePriceLabel
              set quotePriceNumber to 0
              try
                set quotePriceNumber to quotePriceText as real
              end try
              if quotePriceNumber > 0 then set end of refreshedLastPriceCandidates to quotePriceText
            end if
          end repeat
        end if
        if (count of refreshedLastPriceCandidates) is 1 then set refreshedLastPrice to item 1 of refreshedLastPriceCandidates
        if observedSymbol is "{symbol}" and linkedSecurityName is not "" and refreshedLastPrice is not "" then
          set quoteRefreshReady to true
          exit repeat
        end if
      end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
    end repeat
    if not quoteRefreshReady then error "simulation quote is unavailable after committed symbol edit"
    return "QUOTE_REFRESHED" & tab & refreshedLastPrice
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
        symbol = symbol,
    )
}

fn preparation_script(order: &SimulatedOrderDraft, active_direction: Direction) -> String {
    let requested_tab = match order.direction {
        Direction::Buy => "买入",
        Direction::Sell => "卖出",
    };
    let requested_submit = match order.direction {
        Direction::Buy => "确定买入",
        Direction::Sell => "确定卖出",
    };
    let switch_direction = if active_direction == order.direction {
        String::new()
    } else {
        format!(
            r#"set directionButtons to {{}}
      repeat with candidateButton in buttons of targetWindow
        set candidateButtonRef to contents of candidateButton
        try
          if (name of candidateButtonRef as text) is "{requested_tab}" then set end of directionButtons to candidateButtonRef
        end try
      end repeat
      if (count of directionButtons) is not 1 then error "requested simulation direction tab is not unique"
      set directionButton to item 1 of directionButtons
      click directionButton
      delay 0.3
      "#
        )
    };
    format!(
        r#"
tell application "同花顺" to activate
tell application "System Events"
  if not (exists process "同花顺") then error "Tonghuashun is not running"
  tell process "同花顺"
    set frontmost to true
    delay 0.2
    if bundle identifier is not "{bundle}" then error "unexpected application bundle"
    if version of application file is not "{version}" then error "unsupported application version"
    set targetWindow to missing value
    set matchedWindowCount to 0
    set frozenWindows to {{}}
    set stableWindowSnapshot to false
    repeat with windowSnapshotAttempt from 1 to 3
      set frozenWindows to {{}}
      try
        set liveWindowObjects to get every window
        set liveWindowCount to count of liveWindowObjects
        repeat with candidateWindowRef in liveWindowObjects
          set end of frozenWindows to contents of candidateWindowRef
        end repeat
        if (count of frozenWindows) is liveWindowCount then
          set stableWindowSnapshot to true
          exit repeat
        end if
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      delay 0.05
    end repeat
    if not stableWindowSnapshot then error "stable Tonghuashun window snapshot unavailable"
    set observedWindowCount to count of frozenWindows
    repeat with windowIndex from 1 to observedWindowCount
      set w to missing value
      set wIsAlive to false
      try
        set w to item windowIndex of frozenWindows
        set ignoredWindowRole to role of w
        set candidateWindowSubrole to subrole of w
        if candidateWindowSubrole is "AXStandardWindow" then set wIsAlive to true
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      if wIsAlive then
      set hasSimulationMarker to false
      set observedStaticTextCount to 0
      try
        set observedStaticTextCount to count of static texts of w
      on error errorMessage number errorNumber
        if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
      end try
      repeat with staticTextIndex from 1 to observedStaticTextCount
        set s to missing value
        try
          set s to item staticTextIndex of static texts of w
        end try
        if s is not missing value then
          set markerValue to ""
          try
            set markerValue to value of s as text
          end try
          if markerValue is "" then
            try
              set markerValue to name of s as text
            end try
          end if
          if markerValue is "模拟练习" then set hasSimulationMarker to true
        end if
      end repeat
      if hasSimulationMarker then
        set targetWindow to w
        set matchedWindowCount to matchedWindowCount + 1
      end if
      end if
    end repeat
    if matchedWindowCount is not 1 then error "unique simulation order window not found"
    if exists button "转账" of targetWindow then error "live transfer control is visible"
    if exists button "退出" of targetWindow then error "live account exit control is visible"
    if exists static text "账户设置" of targetWindow then error "live account settings are visible"
    set observedStaticTextCount to count of static texts of targetWindow
    repeat with staticTextIndex from 1 to observedStaticTextCount
      set s to missing value
      try
        set s to item staticTextIndex of static texts of targetWindow
      end try
      if s is not missing value then
        set accountLabel to ""
        try
          set accountLabel to value of s as text
        end try
        if accountLabel is "" then
          try
            set accountLabel to name of s as text
          end try
        end if
        if accountLabel is "账户设置" then error "live account settings are visible"
      end if
    end repeat
    {switch_direction}      if not (exists button "{requested_submit}" of targetWindow) then error "requested submit direction is not active"
    set fields to {{}}
    repeat with f in text fields of targetWindow
      set end of fields to contents of f
    end repeat
    if (count of fields) is not 3 then error "simulation order form does not have exactly three fields"
    set codeField to item 1 of fields
    set priceField to item 1 of fields
    set quantityField to item 1 of fields
    repeat with f in fields
      set fRef to contents of f
      set fPosition to position of fRef
      set codePosition to position of codeField
      set quantityPosition to position of quantityField
      if item 2 of fPosition < item 2 of codePosition then set codeField to fRef
      if item 2 of fPosition > item 2 of quantityPosition then set quantityField to fRef
    end repeat
    repeat with f in fields
      set fRef to contents of f
      if fRef is not codeField and fRef is not quantityField then set priceField to fRef
    end repeat
    set value of codeField to "{symbol}"
    set focused of codeField to true
    key code 48
    set codeFieldPosition to position of codeField
    set priceFieldPosition to position of priceField
    set preparationEvidenceReady to false
    repeat with preparationEvidenceAttempt from 1 to 20
      delay 0.05
      set observedSymbol to value of codeField as text
      set linkedSecurityNames to {{}}
      set quoteAnchorPositions to {{}}
      set quotePriceLabels to {{}}
      set quotePricePositions to {{}}
      set frozenPreparationStaticTexts to {{}}
      set livePreparationStaticTexts to get (static texts of targetWindow)
      repeat with candidateSecurityTextRef in livePreparationStaticTexts
        set end of frozenPreparationStaticTexts to contents of candidateSecurityTextRef
      end repeat
      repeat with candidateSecurityTextRef in frozenPreparationStaticTexts
        set candidateSecurityAlive to false
        try
          set candidateSecurityText to contents of candidateSecurityTextRef
          set ignoredCandidateSecurityRole to role of candidateSecurityText
          set candidateSecurityPosition to position of candidateSecurityText
          set candidateSecurityAlive to true
        on error errorMessage number errorNumber
          if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
        end try
        if candidateSecurityAlive then
          set candidateSecurityLabel to ""
          try
            set candidateSecurityLabel to value of candidateSecurityText as text
          on error errorMessage number errorNumber
            if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
          end try
          if candidateSecurityLabel is "" then
            try
              set candidateSecurityLabel to name of candidateSecurityText as text
            on error errorMessage number errorNumber
              if errorNumber is not -1719 and errorNumber is not -1728 then error errorMessage number errorNumber
            end try
          end if
          if candidateSecurityLabel is not "" and (item 1 of candidateSecurityPosition) >= (item 1 of codeFieldPosition) and (item 2 of candidateSecurityPosition) > (item 2 of codeFieldPosition) and (item 2 of candidateSecurityPosition) < (item 2 of priceFieldPosition) then set end of linkedSecurityNames to candidateSecurityLabel
          if candidateSecurityLabel is "限价委托" then set end of quoteAnchorPositions to candidateSecurityPosition
          if candidateSecurityLabel ends with "↑" or candidateSecurityLabel ends with "↓" then
            set end of quotePriceLabels to candidateSecurityLabel
            set end of quotePricePositions to candidateSecurityPosition
          end if
        end if
      end repeat
      set linkedSecurityName to ""
      if (count of linkedSecurityNames) is 1 then set linkedSecurityName to item 1 of linkedSecurityNames
      set readOnlyMarketPriceCandidates to {{}}
      if (count of quoteAnchorPositions) is 1 then
        set quoteAnchorPosition to item 1 of quoteAnchorPositions
        repeat with quotePriceIndex from 1 to count of quotePriceLabels
          set quotePricePosition to item quotePriceIndex of quotePricePositions
          if (item 1 of quotePricePosition) > (item 1 of quoteAnchorPosition) and (item 2 of quotePricePosition) < (item 2 of quoteAnchorPosition) and ((item 2 of quoteAnchorPosition) - (item 2 of quotePricePosition)) <= 60 then
            set quotePriceLabel to item quotePriceIndex of quotePriceLabels
            set quotePriceText to text 1 thru -2 of quotePriceLabel
            set quotePriceNumber to 0
            try
              set quotePriceNumber to quotePriceText as real
            end try
            if quotePriceNumber > 0 then set end of readOnlyMarketPriceCandidates to quotePriceText
          end if
        end repeat
      end if
      set readOnlyMarketPrice to ""
      if (count of readOnlyMarketPriceCandidates) is 1 then set readOnlyMarketPrice to item 1 of readOnlyMarketPriceCandidates
      if observedSymbol is "{symbol}" and linkedSecurityName is not "" and readOnlyMarketPrice is not "" then
        set preparationEvidenceReady to true
        exit repeat
      end if
    end repeat
    if not preparationEvidenceReady then error "simulation security did not resolve to unique read-only evidence"
    set value of priceField to "{price}"
    set value of quantityField to "{quantity}"
    delay 0.2
    return "PREPARED"
  end tell
end tell
"#,
        bundle = THS_BUNDLE_ID,
        version = TESTED_THS_VERSION,
        switch_direction = switch_direction,
        requested_submit = requested_submit,
        symbol = order.symbol,
        price = order.limit_price,
        quantity = order.quantity,
    )
}

fn parse_integer(value: &str, field: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .with_context(|| format!("invalid {field}: {value}"))
}

fn parse_count(value: &str, field: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("invalid {field} count: {value}"))
}

pub fn direction_from_cli(value: &str) -> Result<Direction> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(Direction::Buy),
        "sell" => Ok(Direction::Sell),
        _ => Err(anyhow!("direction must be buy or sell")),
    }
}
