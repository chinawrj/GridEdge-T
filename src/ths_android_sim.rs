use crate::{
    domain::Direction,
    ths_sim::{
        SimulatedOrderDraft, SimulationFillTable, SimulationFillTableRow, SimulationOrderTable,
        SimulationOrderTableRow, MAX_SIMULATED_ORDER_NOTIONAL, MAX_SIMULATED_ORDER_QUANTITY,
    },
    ths_sim_execution::SimulationUiDriver,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    process::Command,
    str::FromStr,
    thread,
    time::Duration as StdDuration,
};

pub const ANDROID_THS_PACKAGE: &str = "com.hexin.plat.android.supremacy";
pub const TESTED_ANDROID_THS_VERSION: &str = "10.94.09";
pub const ANDROID_SIMULATION_TITLE: &str = "模拟炒股";
pub const ANDROID_SIMULATION_LANDING_MARKER: &str = "模拟练习区";

const ORDER_HEADERS: [&str; 12] = [
    "委托日期",
    "委托时间",
    "证券代码",
    "证券名称",
    "操作",
    "备注",
    "委托数量",
    "撤单数量",
    "委托价格",
    "成交价格",
    "合同编号",
    "委托属性",
];
const FILL_HEADERS: [&str; 11] = [
    "成交日期",
    "成交时间",
    "证券代码",
    "证券名称",
    "操作",
    "成交数量",
    "成交价格",
    "成交金额",
    "合同编号",
    "成交编号",
    "成交类型",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndroidThsConfig {
    pub adb_path: PathBuf,
    pub serial: String,
    pub avd_name_marker: String,
    pub package: String,
    pub tested_version: String,
    pub masked_account: String,
    pub expected_symbol: String,
    pub expected_security_name: String,
    pub confirmation_account_sha256: Option<String>,
    pub money_actions_enabled: bool,
}

impl Default for AndroidThsConfig {
    fn default() -> Self {
        Self {
            adb_path: PathBuf::from("/opt/homebrew/bin/adb"),
            serial: "emulator-5554".to_owned(),
            avd_name_marker: "THS".to_owned(),
            package: ANDROID_THS_PACKAGE.to_owned(),
            tested_version: TESTED_ANDROID_THS_VERSION.to_owned(),
            masked_account: "**0000".to_owned(),
            expected_symbol: "002256".to_owned(),
            expected_security_name: "兆新股份".to_owned(),
            confirmation_account_sha256: None,
            money_actions_enabled: false,
        }
    }
}

impl AndroidThsConfig {
    pub fn validate(&self) -> Result<()> {
        if self.serial.is_empty()
            || !self
                .serial
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            bail!("Android Tonghuashun serial is invalid");
        }
        if self.avd_name_marker.is_empty()
            || !self.avd_name_marker.to_ascii_uppercase().contains("THS")
        {
            bail!("Android Tonghuashun AVD marker must explicitly contain THS");
        }
        if self.package != ANDROID_THS_PACKAGE {
            bail!("Android Tonghuashun package is not the reviewed package");
        }
        if self.tested_version != TESTED_ANDROID_THS_VERSION {
            bail!("Android Tonghuashun version is not the reviewed version");
        }
        if self.masked_account.len() < 4 || !self.masked_account.starts_with("**") {
            bail!("Android Tonghuashun masked account is invalid");
        }
        validate_symbol(&self.expected_symbol)?;
        if self.expected_security_name.trim().is_empty() {
            bail!("Android Tonghuashun expected security name is empty");
        }
        if self
            .confirmation_account_sha256
            .as_ref()
            .is_some_and(|hash| {
                hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            bail!("Android Tonghuashun confirmation account hash is invalid");
        }
        if self.money_actions_enabled && self.confirmation_account_sha256.is_none() {
            bail!("Android Tonghuashun money actions require a confirmation account hash");
        }
        Ok(())
    }
}

pub trait AdbExecutor {
    fn run(&mut self, args: &[String]) -> Result<String>;

    fn host_now(&self) -> DateTime<FixedOffset> {
        Local::now().fixed_offset()
    }
}

#[derive(Debug, Clone)]
pub struct SystemAdb {
    path: PathBuf,
}

impl SystemAdb {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl AdbExecutor for SystemAdb {
    fn run(&mut self, args: &[String]) -> Result<String> {
        let output = Command::new(&self.path)
            .args(args)
            .output()
            .with_context(|| format!("failed to launch ADB at {}", self.path.display()))?;
        if !output.status.success() {
            bail!(
                "ADB command {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).context("ADB returned non-UTF-8 output")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AndroidSimulationEvidence {
    pub serial: String,
    pub avd_name: String,
    pub package: String,
    pub version: String,
    pub page_title: String,
    pub masked_account: String,
    pub control_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AndroidOrderTimeDisposition {
    Current,
    QuarantinedTerminalPreviousSession,
}

pub struct AndroidThsSimulationUiDriver<E = SystemAdb> {
    config: AndroidThsConfig,
    executor: E,
    prepared: Option<SimulatedOrderDraft>,
    last_orders: Vec<AndroidOrder>,
    quarantined_terminal_order_ids: Vec<String>,
}

impl AndroidThsSimulationUiDriver<SystemAdb> {
    pub fn new(config: AndroidThsConfig) -> Result<Self> {
        config.validate()?;
        let executor = SystemAdb::new(config.adb_path.clone());
        Ok(Self::with_executor(config, executor))
    }
}

impl<E: AdbExecutor> AndroidThsSimulationUiDriver<E> {
    pub fn with_executor(config: AndroidThsConfig, executor: E) -> Self {
        Self {
            config,
            executor,
            prepared: None,
            last_orders: Vec::new(),
            quarantined_terminal_order_ids: Vec::new(),
        }
    }

    pub fn probe_identity(&mut self) -> Result<AndroidSimulationEvidence> {
        let (avd_name, version) = self.verify_device_identity()?;
        let snapshot = self.ensure_simulation_form()?;
        let mut evidence = verify_simulation_form(&snapshot, &self.config)?;
        evidence.avd_name = avd_name;
        evidence.version = version;
        Ok(evidence)
    }

    pub fn probe_cancellable_contract_ids(&mut self) -> Result<Vec<String>> {
        let form = self.ensure_simulation_form()?;
        self.tap_unique(&form, |node| {
            node.resource_id.ends_with("/id/btn") && node.content_desc == "撤单"
        })?;
        let snapshot = self.snapshot()?;
        verify_simulation_shell(&snapshot, &self.config)?;
        let date = self.device_now()?.date_naive().to_string();
        cancellable_ids(
            parse_cancellable_order_summaries(&snapshot)?,
            &date,
            &self.config.expected_symbol,
        )
    }

    fn adb(&mut self, tail: &[&str]) -> Result<String> {
        let mut args = vec!["-s".to_owned(), self.config.serial.clone()];
        args.extend(tail.iter().map(|value| (*value).to_owned()));
        self.executor.run(&args)
    }

    fn verify_device_identity(&mut self) -> Result<(String, String)> {
        self.config.validate()?;
        let devices = self
            .executor
            .run(&["devices".to_owned(), "-l".to_owned()])?;
        let connected = devices
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        let selected = connected.first().map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next(), fields.next())
        });
        if connected.len() != 1
            || selected != Some((Some(self.config.serial.as_str()), Some("device")))
        {
            bail!("Android Tonghuashun requires exactly one reviewed connected emulator");
        }

        let avd_name = self
            .adb(&["shell", "getprop", "ro.boot.qemu.avd_name"])?
            .trim()
            .to_owned();
        if !avd_name
            .to_ascii_uppercase()
            .contains(&self.config.avd_name_marker.to_ascii_uppercase())
        {
            bail!("connected Android emulator is not the reviewed THS AVD");
        }

        let package = self.config.package.clone();
        let package_dump = self.adb(&["shell", "dumpsys", "package", &package])?;
        let version = package_dump
            .lines()
            .find_map(|line| line.trim().strip_prefix("versionName="))
            .context("Android Tonghuashun package version is unavailable")?
            .trim()
            .to_owned();
        if version != self.config.tested_version {
            bail!("Android Tonghuashun application version changed");
        }

        let activities = self.adb(&["shell", "dumpsys", "activity", "activities"])?;
        if parse_top_resumed_package(&activities)? != self.config.package {
            bail!("Android Tonghuashun is not the resumed application");
        }
        Ok((avd_name, version))
    }

    fn device_now(&mut self) -> Result<DateTime<FixedOffset>> {
        let value = self
            .adb(&["shell", "date", "+%Y-%m-%dT%H:%M:%S%z"])?
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_owned();
        let observed = DateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%z")
            .context("Android Tonghuashun device clock is invalid")?;
        if observed.offset().local_minus_utc() != 8 * 60 * 60 {
            bail!("Android Tonghuashun emulator is not in Asia/Shanghai");
        }
        let host = self.executor.host_now();
        if (host.timestamp() - observed.timestamp()).abs() > 120 {
            bail!("Android Tonghuashun emulator clock differs from the host");
        }
        Ok(observed)
    }

    fn snapshot(&mut self) -> Result<UiSnapshot> {
        let mut last_error = None;
        for attempt in 0..10 {
            let result = (|| {
                self.adb(&[
                    "shell",
                    "uiautomator",
                    "dump",
                    "/sdcard/gridedge-ths-window.xml",
                ])?;
                let xml = self.adb(&["exec-out", "cat", "/sdcard/gridedge-ths-window.xml"])?;
                UiSnapshot::parse(&xml)
            })();
            match result {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => last_error = Some(error),
            }
            if attempt < 9 {
                thread::sleep(StdDuration::from_millis(500));
            }
        }
        Err(last_error.context("Android UI snapshot retry exhausted")?)
    }

    fn wait_snapshot(
        &mut self,
        evidence: &str,
        accept: impl Fn(&UiSnapshot) -> bool,
    ) -> Result<UiSnapshot> {
        for attempt in 0..5 {
            let snapshot = self.snapshot()?;
            if accept(&snapshot) {
                return Ok(snapshot);
            }
            if attempt < 4 {
                thread::sleep(StdDuration::from_millis(250));
            }
        }
        bail!("Android Tonghuashun did not expose {evidence} after a bounded wait")
    }

    fn key_back(&mut self) -> Result<()> {
        self.adb(&["shell", "input", "keyevent", "4"])?;
        Ok(())
    }

    fn tap(&mut self, bounds: Bounds) -> Result<()> {
        let (x, y) = bounds.center();
        self.adb(&["shell", "input", "tap", &x.to_string(), &y.to_string()])?;
        Ok(())
    }

    fn tap_unique(
        &mut self,
        snapshot: &UiSnapshot,
        predicate: impl Fn(&UiNode) -> bool,
    ) -> Result<()> {
        let matches = snapshot
            .nodes
            .iter()
            .filter(|node| node.bounds.is_visible() && predicate(node))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            bail!("Android Tonghuashun action target is not unique");
        }
        self.tap(matches[0].bounds)
    }

    fn ensure_simulation_form(&mut self) -> Result<UiSnapshot> {
        self.verify_device_identity()?;
        let reviewed_config = self.config.clone();
        let mut snapshot = self.snapshot()?;
        if verify_simulation_form(&snapshot, &self.config).is_ok() {
            return Ok(snapshot);
        }

        for _ in 0..3 {
            let title = snapshot.text_for_resource_suffix("/id/page_title_view");
            if matches!(
                title.as_deref(),
                Some("当日委托" | "当日成交" | "历史委托" | "历史成交")
            ) || snapshot.has_exact_text("委托撤单确认")
            {
                self.key_back()?;
                snapshot = self.snapshot()?;
                if verify_simulation_form(&snapshot, &self.config).is_ok() {
                    return Ok(snapshot);
                }
            } else {
                break;
            }
        }

        if snapshot.has_exact_text(ANDROID_SIMULATION_LANDING_MARKER) {
            self.tap_unique(&snapshot, |node| node.text == "买入")?;
            let form = self.wait_snapshot("the simulation order form", |candidate| {
                verify_simulation_form(candidate, &reviewed_config).is_ok()
            })?;
            verify_simulation_form(&form, &self.config)?;
            return Ok(form);
        }

        if verify_simulation_shell(&snapshot, &self.config).is_ok() {
            self.tap_unique(&snapshot, |node| {
                node.resource_id.ends_with("/id/btn") && node.content_desc == "买入"
            })?;
            let form = self.wait_snapshot("the simulation buy form", |candidate| {
                verify_simulation_form(candidate, &reviewed_config).is_ok()
            })?;
            verify_simulation_form(&form, &self.config)?;
            return Ok(form);
        }

        if let Some(trade) = snapshot.unique_node(|node| node.content_desc == "交易")? {
            self.tap(trade.bounds)?;
            snapshot = self.wait_snapshot("the trading page", |candidate| {
                candidate.has_exact_text("模拟")
                    || candidate.has_exact_text(ANDROID_SIMULATION_LANDING_MARKER)
                    || verify_simulation_form(candidate, &reviewed_config).is_ok()
            })?;
        }
        if verify_simulation_form(&snapshot, &self.config).is_ok() {
            return Ok(snapshot);
        }
        if !snapshot.has_exact_text(ANDROID_SIMULATION_LANDING_MARKER) {
            let simulation = snapshot.unique_node(|node| {
                node.text == "模拟" && node.bounds.top < 300 && node.bounds.is_visible()
            })?;
            let simulation = simulation.context("Android simulation tab is unavailable")?;
            self.tap(simulation.bounds)?;
            snapshot = self.wait_snapshot("the simulation landing page", |candidate| {
                candidate.has_exact_text(ANDROID_SIMULATION_LANDING_MARKER)
                    || verify_simulation_form(candidate, &reviewed_config).is_ok()
            })?;
        }
        if verify_simulation_form(&snapshot, &self.config).is_ok() {
            return Ok(snapshot);
        }
        if !snapshot.has_exact_text(ANDROID_SIMULATION_LANDING_MARKER) {
            bail!("Android Tonghuashun did not reach the reviewed simulation landing page");
        }
        self.tap_unique(&snapshot, |node| node.text == "买入")?;
        let form = self.wait_snapshot("the simulation order form", |candidate| {
            verify_simulation_form(candidate, &reviewed_config).is_ok()
        })?;
        verify_simulation_form(&form, &self.config)?;
        Ok(form)
    }

    fn select_direction(&mut self, direction: Direction) -> Result<UiSnapshot> {
        let snapshot = self.ensure_simulation_form()?;
        let label = match direction {
            Direction::Buy => "买入",
            Direction::Sell => "卖出",
        };
        self.tap_unique(&snapshot, |node| {
            node.resource_id.ends_with("/id/btn") && node.content_desc == label
        })?;
        let selected = self.snapshot()?;
        verify_simulation_form(&selected, &self.config)?;
        let expected_submit = submit_label(direction);
        selected
            .unique_node(|node| node.text == expected_submit)?
            .context("Android Tonghuashun direction did not select the reviewed submit form")?;
        Ok(selected)
    }

    fn clear_and_type(&mut self, bounds: Bounds, value: &str) -> Result<()> {
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
        {
            bail!("Android Tonghuashun form value has unsupported characters");
        }
        self.tap(bounds)?;
        self.adb(&["shell", "input", "keyevent", "123"])?;
        for _ in 0..32 {
            self.adb(&["shell", "input", "keyevent", "67"])?;
        }
        self.adb(&["shell", "input", "text", value])?;
        Ok(())
    }

    fn select_security_from_search(&mut self) -> Result<UiSnapshot> {
        let expected_symbol = self.config.expected_symbol.clone();
        let expected_name = self.config.expected_security_name.clone();
        let reviewed_config = self.config.clone();
        let search =
            self.wait_snapshot("the exact selected security or search result", |snapshot| {
                selected_security_on_form(snapshot, &reviewed_config)
                    || matching_security_rows(snapshot, &expected_symbol, &expected_name).len() == 1
            })?;
        if selected_security_on_form(&search, &self.config) {
            return Ok(search);
        }
        let rows = matching_security_rows(&search, &expected_symbol, &expected_name);
        if rows.len() != 1 {
            bail!("Android Tonghuashun security search result is ambiguous");
        }
        self.tap(rows[0].bounds)?;
        self.wait_snapshot("the selected simulation security", |snapshot| {
            selected_security_on_form(snapshot, &reviewed_config)
        })
    }

    fn read_orders(&mut self) -> Result<Vec<AndroidOrder>> {
        let form = self.ensure_simulation_form()?;
        self.tap_unique(&form, |node| node.resource_id.ends_with("/id/weituo"))?;
        let snapshot = self.snapshot()?;
        verify_simulation_shell(&snapshot, &self.config)?;
        let summaries = parse_order_summaries(&snapshot)?;
        let observed_at = self.device_now()?;
        let date = observed_at.date_naive().to_string();
        validate_iso_date(&date)?;
        let mut reviewed_orders = Vec::with_capacity(summaries.len());
        let mut identities = BTreeSet::new();
        self.quarantined_terminal_order_ids.clear();
        for summary in summaries {
            let disposition = classify_android_order_time(
                &summary.order_time,
                &summary.remark,
                observed_at.naive_local(),
            )?;
            self.tap(summary.row_bounds)?;
            let detail = self.snapshot()?;
            let identity = parse_cancel_detail(&detail, &self.config)?;
            self.key_back()?;
            if identity.symbol != self.config.expected_symbol
                || identity.security_name != self.config.expected_security_name
                || identity.direction != summary.direction
                || identity.quantity != summary.quantity
                || identity.price != summary.order_price
            {
                bail!("Android Tonghuashun order row changed while reading its identity");
            }
            let order = summary.into_order(date.clone(), identity.symbol);
            if !identities.insert(order.contract_id.clone()) {
                bail!("Android Tonghuashun contains ambiguous duplicate order identities");
            }
            reviewed_orders.push((disposition, order));
        }
        let mut orders = Vec::with_capacity(reviewed_orders.len());
        for (disposition, order) in reviewed_orders {
            if disposition == AndroidOrderTimeDisposition::QuarantinedTerminalPreviousSession {
                eprintln!(
                    "quarantined reviewed previous-session terminal Android contract {}",
                    order.contract_id
                );
                self.quarantined_terminal_order_ids.push(order.contract_id);
                continue;
            }
            orders.push(order);
        }
        Ok(orders)
    }

    fn read_fills(&mut self) -> Result<Vec<AndroidFill>> {
        let form = self.ensure_simulation_form()?;
        self.tap_unique(&form, |node| node.resource_id.ends_with("/id/chengjiao"))?;
        let snapshot = self.snapshot()?;
        verify_simulation_shell(&snapshot, &self.config)?;
        parse_fills(&snapshot, &self.config, &self.last_orders)
    }
}

impl<E: AdbExecutor> SimulationUiDriver for AndroidThsSimulationUiDriver<E> {
    fn orders(&mut self) -> Result<SimulationOrderTable> {
        let orders = self.read_orders()?;
        self.last_orders = orders.clone();
        Ok(SimulationOrderTable {
            headers: ORDER_HEADERS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            rows: orders.into_iter().map(AndroidOrder::table_row).collect(),
        })
    }

    fn fills(&mut self) -> Result<SimulationFillTable> {
        let fills = self.read_fills()?;
        Ok(SimulationFillTable {
            headers: FILL_HEADERS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            rows: fills.into_iter().map(AndroidFill::table_row).collect(),
        })
    }

    fn startup_preflight(&mut self) -> Result<()> {
        self.probe_identity()?;
        self.orders()?;
        self.fills()?;
        self.probe_cancellable_contract_ids()?;
        if !self.quarantined_terminal_order_ids.is_empty() {
            eprintln!(
                "Android startup preflight quarantined terminal contracts: {}",
                self.quarantined_terminal_order_ids.join(",")
            );
        }
        Ok(())
    }

    fn prepare(&mut self, order: &SimulatedOrderDraft) -> Result<()> {
        validate_order(order, &self.config.expected_symbol)?;
        let snapshot = self.select_direction(order.direction)?;
        let code = snapshot
            .unique_node(|node| node.resource_id.ends_with("/id/auto_stockcode"))?
            .context("Android Tonghuashun stock-code field is missing")?
            .bounds;
        self.clear_and_type(code, &order.symbol)?;
        let selected = self.select_security_from_search()?;
        let (price, _) = reviewed_order_edit_fields(&selected)?;
        self.clear_and_type(price, &order.limit_price.normalize().to_string())?;
        let priced = self.snapshot()?;
        verify_simulation_form(&priced, &self.config)?;
        let (_, quantity) = reviewed_order_edit_fields(&priced)?;
        self.clear_and_type(quantity, &order.quantity.to_string())?;
        let echoed = self.snapshot()?;
        verify_simulation_form(&echoed, &self.config)?;
        verify_order_echo(&echoed, order)?;
        self.prepared = Some(order.clone());
        Ok(())
    }

    fn submit_once(&mut self, order: &SimulatedOrderDraft) -> Result<()> {
        if !self.config.money_actions_enabled {
            bail!("Android Tonghuashun money actions are disabled by configuration");
        }
        if self.prepared.as_ref() != Some(order) {
            bail!("Android Tonghuashun submit does not match the prepared order");
        }
        self.verify_device_identity()?;
        let snapshot = self.snapshot()?;
        verify_simulation_form(&snapshot, &self.config)?;
        verify_order_echo(&snapshot, order)?;
        self.tap_unique(&snapshot, |node| node.text == submit_label(order.direction))?;
        let reviewed_config = self.config.clone();
        let confirmation = self
            .wait_snapshot("the exact simulation submit confirmation", |candidate| {
                verify_submit_confirmation(candidate, &reviewed_config, order).is_ok()
            })?;
        self.verify_device_identity()?;
        let confirm_bounds = verify_submit_confirmation(&confirmation, &self.config, order)?;
        self.tap(confirm_bounds)?;
        let reviewed_config = self.config.clone();
        let outcome = self.wait_snapshot("the exact simulation submit outcome", |candidate| {
            verify_submit_outcome(candidate, &reviewed_config).is_ok()
        })?;
        self.verify_device_identity()?;
        let dismiss = verify_submit_outcome(&outcome, &self.config)?;
        self.tap(dismiss)?;
        self.prepared = None;
        Ok(())
    }

    fn cancel_contract_once(&mut self, contract_id: &str) -> Result<()> {
        if !self.config.money_actions_enabled {
            bail!("Android Tonghuashun money actions are disabled by configuration");
        }
        let orders = self.read_orders()?;
        let target = orders
            .iter()
            .filter(|order| order.contract_id == contract_id)
            .collect::<Vec<_>>();
        if target.len() != 1 {
            bail!("Android Tonghuashun cancel contract is missing or ambiguous");
        }
        if target[0].cancelled_quantity > 0 || target[0].remark.contains('撤') {
            bail!("Android Tonghuashun contract is already cancelled");
        }
        let form = self.ensure_simulation_form()?;
        self.tap_unique(&form, |node| {
            node.resource_id.ends_with("/id/btn") && node.content_desc == "撤单"
        })?;
        let snapshot = self.snapshot()?;
        verify_simulation_shell(&snapshot, &self.config)?;
        let summaries = parse_cancellable_order_summaries(&snapshot)?;
        let matching = summaries
            .into_iter()
            .filter(|summary| {
                summary.synthetic_contract(&target[0].order_date, &self.config.expected_symbol)
                    == contract_id
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            bail!("Android Tonghuashun cancel row changed or became ambiguous before action");
        }
        let summary = &matching[0];
        self.tap(summary.row_bounds)?;
        let detail = self.snapshot()?;
        self.verify_device_identity()?;
        let identity = verify_final_cancel_confirmation(&detail, &self.config)?;
        if identity.symbol != target[0].symbol
            || identity.security_name != target[0].security_name
            || identity.direction != target[0].direction
            || identity.quantity != target[0].quantity
            || identity.price != target[0].order_price
        {
            bail!("Android Tonghuashun cancel confirmation does not match the contract");
        }
        self.tap_unique(&detail, |node| {
            node.resource_id.ends_with("/id/option_chedan") && node.text == "撤单"
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Bounds {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
}

impl Bounds {
    fn parse(value: &str) -> Result<Self> {
        let numbers = value
            .split(|ch: char| !ch.is_ascii_digit() && ch != '-')
            .filter(|part| !part.is_empty())
            .map(str::parse::<i32>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if numbers.len() != 4 {
            bail!("Android UI node has invalid bounds");
        }
        Ok(Self {
            left: numbers[0],
            top: numbers[1],
            right: numbers[2],
            bottom: numbers[3],
        })
    }

    fn is_visible(self) -> bool {
        self.right > self.left && self.bottom > self.top
    }

    fn center(self) -> (i32, i32) {
        ((self.left + self.right) / 2, (self.top + self.bottom) / 2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UiNode {
    index: usize,
    parent: Option<usize>,
    children: Vec<usize>,
    text: String,
    resource_id: String,
    class: String,
    package: String,
    content_desc: String,
    clickable: bool,
    bounds: Bounds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiSnapshot {
    nodes: Vec<UiNode>,
}

impl UiSnapshot {
    pub fn parse(xml: &str) -> Result<Self> {
        let document = roxmltree::Document::parse(xml).context("invalid Android UI XML")?;
        let root = document
            .descendants()
            .find(|node| node.has_tag_name("hierarchy"))
            .context("Android UI hierarchy root is missing")?;
        let mut snapshot = Self { nodes: Vec::new() };
        for child in root.children().filter(|node| node.has_tag_name("node")) {
            snapshot.push_xml_node(child, None)?;
        }
        if snapshot.nodes.is_empty() {
            bail!("Android UI hierarchy is empty");
        }
        Ok(snapshot)
    }

    fn push_xml_node(
        &mut self,
        node: roxmltree::Node<'_, '_>,
        parent: Option<usize>,
    ) -> Result<usize> {
        let index = self.nodes.len();
        self.nodes.push(UiNode {
            index,
            parent,
            children: Vec::new(),
            text: node.attribute("text").unwrap_or_default().to_owned(),
            resource_id: node
                .attribute("resource-id")
                .unwrap_or_default()
                .replace(":id/", "/id/"),
            class: node.attribute("class").unwrap_or_default().to_owned(),
            package: node.attribute("package").unwrap_or_default().to_owned(),
            content_desc: node
                .attribute("content-desc")
                .unwrap_or_default()
                .to_owned(),
            clickable: node.attribute("clickable") == Some("true"),
            bounds: Bounds::parse(node.attribute("bounds").unwrap_or_default())?,
        });
        let mut children = Vec::new();
        for child in node.children().filter(|child| child.has_tag_name("node")) {
            children.push(self.push_xml_node(child, Some(index))?);
        }
        self.nodes[index].children = children;
        Ok(index)
    }

    fn has_exact_text(&self, text: &str) -> bool {
        self.nodes
            .iter()
            .any(|node| node.bounds.is_visible() && node.text == text)
    }

    fn text_for_resource_suffix(&self, suffix: &str) -> Option<String> {
        self.nodes
            .iter()
            .find(|node| node.resource_id.ends_with(suffix) && node.bounds.is_visible())
            .map(|node| node.text.clone())
    }

    fn unique_node(&self, predicate: impl Fn(&UiNode) -> bool) -> Result<Option<&UiNode>> {
        let matches = self
            .nodes
            .iter()
            .filter(|node| predicate(node))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            bail!("Android UI evidence is ambiguous");
        }
        Ok(matches.into_iter().next())
    }

    fn descendants(&self, index: usize) -> Vec<&UiNode> {
        let mut found = Vec::new();
        let mut pending = self.nodes[index]
            .children
            .iter()
            .rev()
            .copied()
            .collect::<Vec<_>>();
        while let Some(child) = pending.pop() {
            found.push(&self.nodes[child]);
            pending.extend(self.nodes[child].children.iter().rev().copied());
        }
        found
    }

    fn descendant_text(&self, index: usize, suffix: &str) -> Result<String> {
        let matches = self
            .descendants(index)
            .into_iter()
            .filter(|node| node.resource_id.ends_with(suffix))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            bail!("Android UI row field is missing or ambiguous");
        }
        Ok(matches[0].text.clone())
    }

    fn clickable_ancestor_bounds_within(
        &self,
        mut index: usize,
        allowed: Option<&BTreeSet<usize>>,
    ) -> Result<Bounds> {
        loop {
            if allowed.is_some_and(|indices| !indices.contains(&index)) {
                bail!("Android UI row escaped its reviewed structural region");
            }
            let node = &self.nodes[index];
            if node.clickable && node.bounds.is_visible() {
                return Ok(node.bounds);
            }
            index = node
                .parent
                .context("Android UI row has no clickable ancestor")?;
        }
    }
}

fn matching_security_rows<'a>(
    snapshot: &'a UiSnapshot,
    symbol: &str,
    security_name: &str,
) -> Vec<&'a UiNode> {
    snapshot
        .nodes
        .iter()
        .filter(|node| {
            node.clickable
                && node.bounds.is_visible()
                && node.children.iter().any(|child| {
                    let child = &snapshot.nodes[*child];
                    child.resource_id.ends_with("/id/stockcode_tv") && child.text == symbol
                })
                && node.children.iter().any(|child| {
                    let child = &snapshot.nodes[*child];
                    child.resource_id.ends_with("/id/stockname_tv") && child.text == security_name
                })
        })
        .collect()
}

fn selected_security_on_form(snapshot: &UiSnapshot, config: &AndroidThsConfig) -> bool {
    verify_simulation_form(snapshot, config).is_ok()
        && matches!(
            exact_resource_text(snapshot, "/id/auto_stockcode").as_deref(),
            Ok(value) if value == config.expected_symbol
        )
        && matches!(
            exact_resource_text(snapshot, "/id/stockname").as_deref(),
            Ok(value) if value == config.expected_security_name
        )
}

pub fn verify_simulation_xml(
    xml: &str,
    config: &AndroidThsConfig,
) -> Result<AndroidSimulationEvidence> {
    let snapshot = UiSnapshot::parse(xml)?;
    verify_simulation_shell(&snapshot, config)
}

pub fn parse_top_resumed_package(activities: &str) -> Result<String> {
    let resumed = activities
        .lines()
        .filter(|line| line.contains("topResumedActivity="))
        .collect::<Vec<_>>();
    if resumed.len() != 1 {
        bail!("Android requires exactly one top resumed activity");
    }
    let components = resumed[0]
        .split_whitespace()
        .filter_map(|field| {
            let field = field.trim_matches(|ch: char| matches!(ch, '{' | '}' | ','));
            let (package, activity) = field.split_once('/')?;
            if package.is_empty() || activity.is_empty() {
                return None;
            }
            Some(package.to_owned())
        })
        .collect::<Vec<_>>();
    if components.len() != 1 {
        bail!("Android top resumed component is missing or ambiguous");
    }
    Ok(components[0].clone())
}

pub fn verify_cancel_confirmation_xml(xml: &str, config: &AndroidThsConfig) -> Result<()> {
    let snapshot = UiSnapshot::parse(xml)?;
    verify_final_cancel_confirmation(&snapshot, config).map(|_| ())
}

pub fn verify_submit_confirmation_xml(
    xml: &str,
    config: &AndroidThsConfig,
    order: &SimulatedOrderDraft,
) -> Result<()> {
    let snapshot = UiSnapshot::parse(xml)?;
    verify_submit_confirmation(&snapshot, config, order).map(|_| ())
}

pub fn verify_submit_outcome_xml(xml: &str, config: &AndroidThsConfig) -> Result<()> {
    let snapshot = UiSnapshot::parse(xml)?;
    verify_submit_outcome(&snapshot, config).map(|_| ())
}

pub fn parse_android_orders_xml(
    order_xml: &str,
    detail_xmls: &[&str],
    date: &str,
    config: &AndroidThsConfig,
) -> Result<SimulationOrderTable> {
    config.validate()?;
    validate_iso_date(date)?;
    let snapshot = UiSnapshot::parse(order_xml)?;
    verify_reviewed_package_tree(&snapshot, config)?;
    let summaries = parse_order_summaries(&snapshot)?;
    if summaries.len() != detail_xmls.len() {
        bail!("Android Tonghuashun order details do not cover every visible row");
    }
    let mut orders = Vec::with_capacity(summaries.len());
    for (summary, detail_xml) in summaries.into_iter().zip(detail_xmls) {
        let detail = UiSnapshot::parse(detail_xml)?;
        let identity = parse_cancel_detail(&detail, config)?;
        if identity.symbol != config.expected_symbol
            || identity.security_name != config.expected_security_name
            || identity.direction != summary.direction
            || identity.quantity != summary.quantity
            || identity.price != summary.order_price
        {
            bail!("Android Tonghuashun order detail does not match its row");
        }
        orders.push(summary.into_order(date.to_owned(), identity.symbol));
    }
    let mut identities = BTreeSet::new();
    for order in &orders {
        if !identities.insert(order.contract_id.clone()) {
            bail!("Android Tonghuashun contains ambiguous duplicate order identities");
        }
    }
    Ok(SimulationOrderTable {
        headers: ORDER_HEADERS
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        rows: orders.into_iter().map(AndroidOrder::table_row).collect(),
    })
}

pub fn cancellable_contract_ids_from_xml(
    cancel_xml: &str,
    date: &str,
    config: &AndroidThsConfig,
) -> Result<Vec<String>> {
    config.validate()?;
    validate_iso_date(date)?;
    let snapshot = UiSnapshot::parse(cancel_xml)?;
    verify_reviewed_package_tree(&snapshot, config)?;
    let summaries = parse_cancellable_order_summaries(&snapshot)?;
    cancellable_ids(summaries, date, &config.expected_symbol)
}

fn cancellable_ids(summaries: Vec<OrderSummary>, date: &str, symbol: &str) -> Result<Vec<String>> {
    let ids = summaries
        .into_iter()
        .map(|summary| summary.synthetic_contract(date, symbol))
        .collect::<Vec<_>>();
    if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
        bail!("Android Tonghuashun cancellable contracts are ambiguous");
    }
    Ok(ids)
}

pub fn parse_android_fills_xml(
    fill_xml: &str,
    orders: &SimulationOrderTable,
    config: &AndroidThsConfig,
) -> Result<SimulationFillTable> {
    config.validate()?;
    let known_orders = orders
        .records()?
        .into_iter()
        .map(|order| {
            let filled_quantity = inferred_filled_quantity(&order)?;
            let fill_price = order
                .fill_price
                .as_deref()
                .map(|value| parse_decimal(value, "order fill price"))
                .transpose()?;
            Ok(AndroidOrder {
                order_date: order.order_date,
                order_time: order.order_time,
                symbol: order.symbol,
                security_name: order.security_name,
                direction: order.direction,
                remark: order.remark,
                quantity: order.quantity,
                filled_quantity,
                cancelled_quantity: order.cancelled_quantity,
                order_price: order.order_price,
                fill_price,
                contract_id: order.contract_id,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let snapshot = UiSnapshot::parse(fill_xml)?;
    verify_reviewed_package_tree(&snapshot, config)?;
    let fills = parse_fills(&snapshot, config, &known_orders)?;
    Ok(SimulationFillTable {
        headers: FILL_HEADERS
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        rows: fills.into_iter().map(AndroidFill::table_row).collect(),
    })
}

fn verify_simulation_form(
    snapshot: &UiSnapshot,
    config: &AndroidThsConfig,
) -> Result<AndroidSimulationEvidence> {
    let evidence = verify_simulation_shell(snapshot, config)?;
    for suffix in [
        "/id/auto_stockcode",
        "/id/main_scroller",
        "/id/weituo",
        "/id/chengjiao",
    ] {
        snapshot
            .unique_node(|node| node.resource_id.ends_with(suffix) && node.bounds.is_visible())?
            .with_context(|| format!("Android Tonghuashun form control {suffix} is missing"))?;
    }
    Ok(evidence)
}

fn verify_simulation_shell(
    snapshot: &UiSnapshot,
    config: &AndroidThsConfig,
) -> Result<AndroidSimulationEvidence> {
    config.validate()?;
    verify_reviewed_package_tree(snapshot, config)?;
    let title = snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/page_title_view") && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun page title is missing")?;
    if title.text != ANDROID_SIMULATION_TITLE {
        bail!("Android Tonghuashun is not on the simulation trading form");
    }
    let account = snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/account_info_view") && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun masked account is missing")?;
    if account.text != config.masked_account {
        bail!("Android Tonghuashun simulation account changed");
    }
    for forbidden in ["中信证券", "账户设置", "银证转账", "退出登录"] {
        if snapshot.has_exact_text(forbidden) {
            bail!("Android Tonghuashun real-account evidence is visible");
        }
    }
    let mut controls = Vec::new();
    for label in ["买入", "卖出", "撤单", "持仓", "查询"] {
        let matches = snapshot
            .nodes
            .iter()
            .filter(|node| {
                node.bounds.is_visible()
                    && node.resource_id.ends_with("/id/btn")
                    && node.content_desc == label
            })
            .count();
        if matches != 1 {
            bail!("Android Tonghuashun reviewed navigation controls changed");
        }
        controls.push(label.to_owned());
    }
    Ok(AndroidSimulationEvidence {
        serial: config.serial.clone(),
        avd_name: String::new(),
        package: config.package.clone(),
        version: config.tested_version.clone(),
        page_title: title.text.clone(),
        masked_account: account.text.clone(),
        control_labels: controls,
    })
}

fn verify_reviewed_package_tree(snapshot: &UiSnapshot, config: &AndroidThsConfig) -> Result<()> {
    if snapshot
        .nodes
        .iter()
        .any(|node| node.package != config.package)
    {
        bail!("Android UI hierarchy contains an unreviewed package");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderSummary {
    security_name: String,
    order_time: String,
    order_price: Decimal,
    fill_price: Option<Decimal>,
    quantity: i64,
    filled_quantity: i64,
    direction: Direction,
    remark: String,
    row_bounds: Bounds,
}

impl OrderSummary {
    fn synthetic_contract(&self, date: &str, symbol: &str) -> String {
        stable_id(
            "ANDROID",
            &[
                date,
                &self.order_time,
                symbol,
                direction_label(self.direction),
                &self.order_price.normalize().to_string(),
                &self.quantity.to_string(),
            ],
        )
    }

    fn into_order(self, date: String, symbol: String) -> AndroidOrder {
        let cancelled_quantity = if self.remark.contains('撤') {
            self.quantity.saturating_sub(self.filled_quantity)
        } else {
            0
        };
        let contract_id = self.synthetic_contract(&date, &symbol);
        AndroidOrder {
            order_date: date,
            order_time: self.order_time,
            symbol,
            security_name: self.security_name,
            direction: self.direction,
            remark: self.remark,
            quantity: self.quantity,
            filled_quantity: self.filled_quantity,
            cancelled_quantity,
            order_price: self.order_price,
            fill_price: self.fill_price,
            contract_id,
        }
    }
}

pub fn classify_android_order_time(
    order_time: &str,
    remark: &str,
    observed_at: NaiveDateTime,
) -> Result<AndroidOrderTimeDisposition> {
    let time = NaiveTime::parse_from_str(order_time, "%H:%M:%S")
        .context("Android Tonghuashun order time is invalid")?;
    let ordered_at = NaiveDateTime::new(observed_at.date(), time);
    if ordered_at <= observed_at {
        return Ok(AndroidOrderTimeDisposition::Current);
    }
    let reviewed_terminal = matches!(remark, "全部撤单" | "全部成交" | "废单");
    if reviewed_terminal {
        return Ok(AndroidOrderTimeDisposition::QuarantinedTerminalPreviousSession);
    }
    bail!("Android Tonghuashun current-day order is in the future")
}

pub fn validate_android_order_time_not_future(
    order_time: &str,
    observed_at: NaiveDateTime,
) -> Result<()> {
    let time = NaiveTime::parse_from_str(order_time, "%H:%M:%S")
        .context("Android Tonghuashun order time is invalid")?;
    let ordered_at = NaiveDateTime::new(observed_at.date(), time);
    if ordered_at > observed_at {
        bail!("Android Tonghuashun current-day order is in the future");
    }
    Ok(())
}

fn parse_order_summaries(snapshot: &UiSnapshot) -> Result<Vec<OrderSummary>> {
    let headers = ["委托时间", "委托/均价", "委托/成交", "状态"];
    for header in headers {
        if !snapshot.has_exact_text(header) {
            bail!("Android Tonghuashun order headers changed");
        }
    }
    parse_order_summaries_from_indices(snapshot, None, false)
}

fn parse_order_summaries_from_indices(
    snapshot: &UiSnapshot,
    allowed: Option<&BTreeSet<usize>>,
    allow_direct_clickable_row: bool,
) -> Result<Vec<OrderSummary>> {
    let mut rows = Vec::new();
    for node in &snapshot.nodes {
        if allowed.is_some_and(|indices| !indices.contains(&node.index)) {
            continue;
        }
        let direct_row = allow_direct_clickable_row
            && node.clickable
            && !snapshot
                .descendants(node.index)
                .iter()
                .any(|child| child.resource_id.ends_with("/id/container"));
        if !(node.resource_id.ends_with("/id/container") || direct_row)
            || node.bounds.bottom - node.bounds.top > 300
        {
            continue;
        }
        let has_shape = [
            "/id/result0",
            "/id/result1",
            "/id/result2",
            "/id/result3",
            "/id/first_tv",
            "/id/second_tv",
            "/id/result6",
            "/id/result7",
        ]
        .iter()
        .all(|suffix| snapshot.descendant_text(node.index, suffix).is_ok());
        if !has_shape {
            continue;
        }
        let order_time = snapshot.descendant_text(node.index, "/id/result1")?;
        validate_time(&order_time)?;
        let quantity = parse_integer(
            &snapshot.descendant_text(node.index, "/id/first_tv")?,
            "order quantity",
        )?;
        let filled_quantity = parse_integer(
            &snapshot.descendant_text(node.index, "/id/second_tv")?,
            "filled quantity",
        )?;
        if quantity <= 0 || filled_quantity < 0 || filled_quantity > quantity {
            bail!("Android Tonghuashun order quantities are invalid");
        }
        let order_price = parse_decimal(
            &snapshot.descendant_text(node.index, "/id/result2")?,
            "order price",
        )?;
        let raw_fill_price = parse_decimal(
            &snapshot.descendant_text(node.index, "/id/result3")?,
            "fill price",
        )?;
        let direction = parse_direction(&snapshot.descendant_text(node.index, "/id/result6")?)?;
        rows.push(OrderSummary {
            security_name: snapshot.descendant_text(node.index, "/id/result0")?,
            order_time,
            order_price,
            fill_price: (!raw_fill_price.is_zero()).then_some(raw_fill_price),
            quantity,
            filled_quantity,
            direction,
            remark: snapshot.descendant_text(node.index, "/id/result7")?,
            row_bounds: snapshot.clickable_ancestor_bounds_within(node.index, allowed)?,
        });
    }
    Ok(rows)
}

fn parse_cancellable_order_summaries(snapshot: &UiSnapshot) -> Result<Vec<OrderSummary>> {
    for header in ["委托时间", "委托/均价", "委托/成交", "状态"] {
        if !snapshot.has_exact_text(header) {
            bail!("Android Tonghuashun cancel headers changed");
        }
    }
    let recycler = snapshot
        .unique_node(|node| node.resource_id.ends_with("/id/chedan_recycler_view"))?
        .context("Android Tonghuashun cancellable-order list is missing")?;
    let separator_positions = recycler
        .children
        .iter()
        .enumerate()
        .filter_map(|(position, child)| {
            (snapshot.nodes[*child]
                .resource_id
                .ends_with("/id/cannot_chedan_title_text")
                || snapshot
                    .descendants(*child)
                    .iter()
                    .any(|node| node.resource_id.ends_with("/id/cannot_chedan_title_text")))
            .then_some(position)
        })
        .collect::<Vec<_>>();
    if separator_positions.len() > 1 {
        bail!("Android Tonghuashun cancel separator is ambiguous");
    }
    let separator_position = separator_positions
        .first()
        .copied()
        .unwrap_or(recycler.children.len());
    let allowed = recycler.children[..separator_position]
        .iter()
        .flat_map(|child| {
            std::iter::once(&snapshot.nodes[*child]).chain(snapshot.descendants(*child))
        })
        .map(|node| node.index)
        .collect::<BTreeSet<_>>();
    let rows = parse_order_summaries_from_indices(snapshot, Some(&allowed), true)?;
    let empty_marker = allowed.iter().any(|index| {
        snapshot.nodes[*index]
            .resource_id
            .ends_with("/id/chedan_empty_layout")
    });
    if empty_marker && !rows.is_empty() {
        bail!("Android Tonghuashun cancel region contradicts its empty marker");
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CancelIdentity {
    symbol: String,
    security_name: String,
    direction: Direction,
    quantity: i64,
    price: Decimal,
}

fn parse_cancel_detail(snapshot: &UiSnapshot, config: &AndroidThsConfig) -> Result<CancelIdentity> {
    verify_reviewed_package_tree(snapshot, config)?;
    if !snapshot.has_exact_text("委托撤单确认") {
        bail!("Android Tonghuashun order identity dialog is missing");
    }
    let operation = exact_resource_text(snapshot, "/id/option_textview")?;
    let security_name = strip_label(
        &exact_resource_text(snapshot, "/id/stockname_textview")?,
        "名称",
    )?;
    let symbol = strip_label(
        &exact_resource_text(snapshot, "/id/stockcode_textview")?,
        "代码",
    )?;
    let quantity = parse_integer(
        &strip_label(
            &exact_resource_text(snapshot, "/id/ordernumber_textview")?,
            "数量",
        )?,
        "order quantity",
    )?;
    let price = parse_decimal(
        &strip_label(
            &exact_resource_text(snapshot, "/id/orderprice_textview")?,
            "价格",
        )?,
        "order price",
    )?;
    validate_symbol(&symbol)?;
    let direction = if operation.contains("买入") {
        Direction::Buy
    } else if operation.contains("卖出") {
        Direction::Sell
    } else {
        bail!("Android Tonghuashun cancel direction is unsupported");
    };
    Ok(CancelIdentity {
        symbol,
        security_name,
        direction,
        quantity,
        price,
    })
}

fn verify_final_cancel_confirmation(
    snapshot: &UiSnapshot,
    config: &AndroidThsConfig,
) -> Result<CancelIdentity> {
    if snapshot
        .nodes
        .iter()
        .any(|node| node.resource_id.ends_with("/id/page_title_view"))
    {
        verify_simulation_shell(snapshot, config)?;
    } else {
        verify_reviewed_package_tree(snapshot, config)?;
    }
    if !snapshot.has_exact_text("您是否确认以上撤单？") {
        bail!("Android Tonghuashun final cancel confirmation changed");
    }
    snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/option_chedan")
                && node.text == "撤单"
                && node.clickable
                && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun final cancel action is missing")?;
    for (suffix, text) in [
        ("/id/option_chedan_and_buy", "撤单后继续买入"),
        ("/id/option_cancel", "取消"),
    ] {
        snapshot
            .unique_node(|node| {
                node.resource_id.ends_with(suffix)
                    && node.text == text
                    && node.clickable
                    && node.bounds.is_visible()
            })?
            .with_context(|| {
                format!("Android Tonghuashun final cancel option {text} is missing")
            })?;
    }
    parse_cancel_detail(snapshot, config)
}

fn verify_submit_confirmation(
    snapshot: &UiSnapshot,
    config: &AndroidThsConfig,
    order: &SimulatedOrderDraft,
) -> Result<Bounds> {
    config.validate()?;
    validate_order(order, &config.expected_symbol)?;
    verify_reviewed_package_tree(snapshot, config)?;
    let expected_title = match order.direction {
        Direction::Buy => "委托买入确认",
        Direction::Sell => "委托卖出确认",
    };
    if !snapshot.has_exact_text(expected_title) || !snapshot.has_exact_text("您是否确认以上委托?")
    {
        bail!("Android Tonghuashun submit confirmation identity changed");
    }
    let account = exact_resource_text(snapshot, "/id/account_value")?;
    let account_hash = hex::encode(Sha256::digest(account.trim().as_bytes()));
    if config.confirmation_account_sha256.as_deref() != Some(account_hash.as_str()) {
        bail!("Android Tonghuashun submit confirmation account changed");
    }
    let name = exact_resource_text(snapshot, "/id/stock_name_value")?;
    let symbol = exact_resource_text(snapshot, "/id/stock_code_value")?;
    let quantity = exact_resource_text(snapshot, "/id/number_value")?;
    let price = exact_resource_text(snapshot, "/id/price_value")?;
    if name.trim() != config.expected_security_name
        || symbol.trim() != order.symbol
        || parse_integer(quantity.trim(), "confirmed quantity")? != order.quantity
        || parse_decimal(price.trim(), "confirmed price")? != order.limit_price
    {
        bail!("Android Tonghuashun submit confirmation changed the durable order intent");
    }
    snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/cancel_btn")
                && node.text == "取消"
                && node.clickable
                && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun submit cancellation button is missing")?;
    let expected_confirmation = match order.direction {
        Direction::Buy => "确认买入",
        Direction::Sell => "确认卖出",
    };
    Ok(snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/ok_btn")
                && node.text == expected_confirmation
                && node.clickable
                && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun final submit action is missing")?
        .bounds)
}

fn verify_submit_outcome(snapshot: &UiSnapshot, config: &AndroidThsConfig) -> Result<Bounds> {
    config.validate()?;
    verify_reviewed_package_tree(snapshot, config)?;
    if !snapshot.has_exact_text("系统信息") {
        bail!("Android Tonghuashun submit outcome title changed");
    }
    let prompt = exact_resource_text(snapshot, "/id/prompt_content")?;
    let contract = prompt
        .strip_prefix("委托已提交，合同号为：")
        .context("Android Tonghuashun did not confirm a submitted simulation order")?;
    if contract.is_empty() || !contract.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("Android Tonghuashun submit outcome contract is invalid");
    }
    Ok(snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/ok_btn")
                && node.text == "确定"
                && node.clickable
                && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun submit outcome dismissal is missing")?
        .bounds)
}

fn exact_resource_text(snapshot: &UiSnapshot, suffix: &str) -> Result<String> {
    snapshot
        .unique_node(|node| node.resource_id.ends_with(suffix) && node.bounds.is_visible())?
        .map(|node| node.text.clone())
        .with_context(|| format!("Android UI field {suffix} is missing"))
}

fn inferred_filled_quantity(order: &crate::ths_sim::SimulationOrderRecord) -> Result<i64> {
    if order.remark.contains("全部成交") {
        return Ok(order.quantity);
    }
    if order.cancelled_quantity > 0 {
        return order
            .quantity
            .checked_sub(order.cancelled_quantity)
            .context("Android order cancelled quantity exceeds its quantity");
    }
    if order.remark.contains("未成交") || order.remark.contains("已报") {
        return Ok(0);
    }
    if order.fill_price.is_some() {
        return Ok(order.quantity);
    }
    Ok(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AndroidOrder {
    order_date: String,
    order_time: String,
    symbol: String,
    security_name: String,
    direction: Direction,
    remark: String,
    quantity: i64,
    filled_quantity: i64,
    cancelled_quantity: i64,
    order_price: Decimal,
    fill_price: Option<Decimal>,
    contract_id: String,
}

impl AndroidOrder {
    fn table_row(self) -> SimulationOrderTableRow {
        SimulationOrderTableRow {
            cells: vec![
                self.order_date,
                self.order_time,
                self.symbol,
                self.security_name,
                direction_label(self.direction).to_owned(),
                self.remark,
                self.quantity.to_string(),
                self.cancelled_quantity.to_string(),
                self.order_price.normalize().to_string(),
                self.fill_price
                    .map(|value| value.normalize().to_string())
                    .unwrap_or_default(),
                self.contract_id,
                "Android同花顺模拟".to_owned(),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AndroidFill {
    fill_date: String,
    fill_time: String,
    symbol: String,
    security_name: String,
    direction: Direction,
    quantity: i64,
    price: Decimal,
    amount: Decimal,
    contract_id: String,
    fill_id: String,
}

impl AndroidFill {
    fn table_row(self) -> SimulationFillTableRow {
        SimulationFillTableRow {
            cells: vec![
                self.fill_date,
                self.fill_time,
                self.symbol,
                self.security_name,
                direction_label(self.direction).to_owned(),
                self.quantity.to_string(),
                self.price.normalize().to_string(),
                self.amount.normalize().to_string(),
                self.contract_id,
                self.fill_id,
                "普通成交".to_owned(),
            ],
        }
    }
}

fn parse_fills(
    snapshot: &UiSnapshot,
    config: &AndroidThsConfig,
    orders: &[AndroidOrder],
) -> Result<Vec<AndroidFill>> {
    if snapshot.has_exact_text("没有成交数据。") || snapshot.has_exact_text("没有成交数据")
    {
        return Ok(Vec::new());
    }
    for header in ["成交时间", "成交价", "成交量", "成交额"] {
        if !snapshot.has_exact_text(header) {
            bail!("Android Tonghuashun fill headers changed");
        }
    }
    let list = snapshot
        .unique_node(|node| {
            (node.resource_id.ends_with("/id/recyclerview_id")
                || node.resource_id.ends_with("/id/codelist"))
                && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun fill list is missing")?;
    let order_dates = orders
        .iter()
        .map(|order| order.order_date.as_str())
        .collect::<BTreeSet<_>>();
    let current_fill_date = match order_dates.len() {
        0 => None,
        1 => order_dates.iter().next().copied(),
        _ => None,
    };
    let mut fills = Vec::new();
    for child in &list.children {
        let leaves = snapshot
            .descendants(*child)
            .into_iter()
            .filter(|node| {
                node.class == "android.widget.TextView"
                    && !node.text.trim().is_empty()
                    && node.bounds.is_visible()
            })
            .map(|node| node.text.trim().to_owned())
            .collect::<Vec<_>>();
        if leaves.is_empty() {
            continue;
        }
        if leaves.len() != 6 {
            bail!("Android Tonghuashun fill row has an unreviewed shape");
        }
        let security_name = leaves[0].clone();
        if security_name != config.expected_security_name {
            bail!("Android Tonghuashun fill belongs to an unknown security");
        }
        let (fill_date, fill_time) = parse_fill_timestamp(&leaves[1], current_fill_date)?;
        let price = parse_decimal(&leaves[2], "fill price")?;
        let quantity = parse_integer(&leaves[3], "fill quantity")?;
        let direction = parse_direction(&leaves[4])?;
        let amount = parse_decimal(&leaves[5], "fill amount")?;
        if quantity <= 0 || price <= Decimal::ZERO || amount != price * Decimal::from(quantity) {
            bail!("Android Tonghuashun fill arithmetic is invalid");
        }
        let candidates = orders
            .iter()
            .filter(|order| {
                order.order_date == fill_date
                    && order.symbol == config.expected_symbol
                    && order.direction == direction
                    && order.quantity >= quantity
                    && order.order_time <= fill_time
                    && match direction {
                        Direction::Buy => price <= order.order_price,
                        Direction::Sell => price >= order.order_price,
                    }
            })
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            bail!("Android Tonghuashun fill cannot be assigned to one unique order");
        }
        let fill_id = stable_id(
            "ANDROIDFILL",
            &[
                &fill_date,
                &fill_time,
                &config.expected_symbol,
                direction_label(direction),
                &price.normalize().to_string(),
                &quantity.to_string(),
                &amount.normalize().to_string(),
            ],
        );
        fills.push(AndroidFill {
            fill_date,
            fill_time,
            symbol: config.expected_symbol.clone(),
            security_name,
            direction,
            quantity,
            price,
            amount,
            contract_id: candidates[0].contract_id.clone(),
            fill_id,
        });
    }
    let mut ids = BTreeSet::new();
    let mut filled_by_contract = BTreeMap::<&str, i64>::new();
    for fill in &fills {
        if !ids.insert(fill.fill_id.clone()) {
            bail!("Android Tonghuashun contains duplicate fill identities");
        }
        let total = filled_by_contract
            .entry(fill.contract_id.as_str())
            .or_default();
        *total = total
            .checked_add(fill.quantity)
            .context("Android Tonghuashun cumulative fill quantity overflow")?;
    }
    for order in orders {
        let total = filled_by_contract
            .get(order.contract_id.as_str())
            .copied()
            .unwrap_or_default();
        if total > order.filled_quantity {
            bail!("Android Tonghuashun fills exceed the order table filled quantity");
        }
    }
    Ok(fills)
}

fn verify_order_echo(snapshot: &UiSnapshot, order: &SimulatedOrderDraft) -> Result<()> {
    let code = exact_resource_text(snapshot, "/id/auto_stockcode")?;
    let (price, quantity) = reviewed_order_edit_nodes(snapshot)?;
    if code != order.symbol
        || parse_decimal(&price.text, "echoed price")? != order.limit_price
        || parse_integer(&quantity.text, "echoed quantity")? != order.quantity
    {
        bail!("Android Tonghuashun form echo does not match the durable order intent");
    }
    Ok(())
}

fn reviewed_order_edit_nodes(snapshot: &UiSnapshot) -> Result<(&UiNode, &UiNode)> {
    let scroller = snapshot
        .unique_node(|node| {
            node.resource_id.ends_with("/id/main_scroller") && node.bounds.is_visible()
        })?
        .context("Android Tonghuashun order form scroller is missing")?;
    let mut fields = snapshot
        .descendants(scroller.index)
        .into_iter()
        .filter(|node| {
            node.class == "android.widget.EditText"
                && node.bounds.is_visible()
                && !node.resource_id.ends_with("/id/auto_stockcode")
        })
        .collect::<Vec<_>>();
    fields.sort_by_key(|node| (node.bounds.top, node.bounds.left));
    let [price, quantity] = fields.as_slice() else {
        bail!("Android Tonghuashun price and quantity fields changed");
    };
    if price.bounds.top >= quantity.bounds.top {
        bail!("Android Tonghuashun price and quantity field order changed");
    }
    Ok((price, quantity))
}

fn reviewed_order_edit_fields(snapshot: &UiSnapshot) -> Result<(Bounds, Bounds)> {
    let (price, quantity) = reviewed_order_edit_nodes(snapshot)?;
    Ok((price.bounds, quantity.bounds))
}

fn validate_order(order: &SimulatedOrderDraft, expected_symbol: &str) -> Result<()> {
    validate_symbol(&order.symbol)?;
    if order.symbol != expected_symbol {
        bail!("Android Tonghuashun order symbol does not match the configured strategy");
    }
    if order.limit_price <= Decimal::ZERO || order.limit_price.scale() > 3 {
        bail!("Android Tonghuashun order price is outside the A-share envelope");
    }
    if order.quantity <= 0
        || order.quantity > MAX_SIMULATED_ORDER_QUANTITY
        || order.quantity % 100 != 0
    {
        bail!("Android Tonghuashun order quantity is invalid");
    }
    if order.limit_price * Decimal::from(order.quantity) > MAX_SIMULATED_ORDER_NOTIONAL {
        bail!("Android Tonghuashun order exceeds the single-order notional limit");
    }
    Ok(())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("Android Tonghuashun symbol is invalid");
    }
    Ok(())
}

fn validate_iso_date(date: &str) -> Result<()> {
    if date.len() != 10
        || date.as_bytes()[4] != b'-'
        || date.as_bytes()[7] != b'-'
        || !date
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        bail!("Android Tonghuashun device date is invalid");
    }
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .context("Android Tonghuashun device date is not a real calendar date")?;
    Ok(())
}

fn validate_time(time: &str) -> Result<()> {
    if time.len() != 8
        || time.as_bytes()[2] != b':'
        || time.as_bytes()[5] != b':'
        || !time
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 5) || byte.is_ascii_digit())
    {
        bail!("Android Tonghuashun time is invalid");
    }
    NaiveTime::parse_from_str(time, "%H:%M:%S")
        .context("Android Tonghuashun time is not a real clock time")?;
    Ok(())
}

fn parse_fill_timestamp(value: &str, current_fill_date: Option<&str>) -> Result<(String, String)> {
    let mut parts = value.split_whitespace();
    let first = parts.next().context("Android fill timestamp is empty")?;
    let second = parts.next();
    if parts.next().is_some() {
        bail!("Android fill timestamp has an unreviewed shape");
    }
    match second {
        Some(time) => {
            if first.len() != 8 || !first.bytes().all(|byte| byte.is_ascii_digit()) {
                bail!("Android fill date is invalid");
            }
            validate_time(time)?;
            Ok((
                format!("{}-{}-{}", &first[0..4], &first[4..6], &first[6..8]),
                time.to_owned(),
            ))
        }
        None => {
            validate_time(first)?;
            let date = current_fill_date
                .context("Android current-day fill cannot be assigned to one unique order date")?;
            validate_iso_date(date)?;
            Ok((date.to_owned(), first.to_owned()))
        }
    }
}

fn parse_direction(value: &str) -> Result<Direction> {
    match value {
        "买入" | "证券买入" => Ok(Direction::Buy),
        "卖出" | "证券卖出" => Ok(Direction::Sell),
        _ => bail!("Android Tonghuashun direction is unsupported"),
    }
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "买入",
        Direction::Sell => "卖出",
    }
}

fn submit_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "买 入(模拟炒股)",
        Direction::Sell => "卖 出(模拟炒股)",
    }
}

fn strip_label(value: &str, label: &str) -> Result<String> {
    value
        .strip_prefix(label)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("Android Tonghuashun detail field {label} is invalid"))
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal> {
    let normalized = value.trim().replace(',', "");
    let parsed = Decimal::from_str(&normalized)
        .with_context(|| format!("Android Tonghuashun {field} is not decimal"))?;
    if parsed.is_sign_negative() {
        bail!("Android Tonghuashun {field} is negative");
    }
    Ok(parsed)
}

fn parse_integer(value: &str, field: &str) -> Result<i64> {
    value
        .trim()
        .replace(',', "")
        .parse::<i64>()
        .with_context(|| format!("Android Tonghuashun {field} is not an integer"))
}

fn stable_id(prefix: &str, fields: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hex::encode(hasher.finalize());
    format!("{prefix}-{}", &digest[..32])
}
