use anyhow::{bail, Result};
use gridedge_t::ths_android_sim::{
    AdbExecutor, AndroidThsConfig, AndroidThsSimulationUiDriver, ANDROID_THS_PACKAGE,
};
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

const DUMP_PATH: &str = "/sdcard/gridedge-ths-window.xml";
const SUCCESS: &str = "UI hierchary dumped to: /sdcard/gridedge-ths-window.xml\r\n";

#[derive(Default)]
struct Observed {
    dumps: usize,
    reads_after_dump: Vec<usize>,
    inputs: usize,
}

struct DumpReceiptAdb {
    receipts: VecDeque<String>,
    fallback_receipt: String,
    retained_xml: String,
    observed: Rc<RefCell<Observed>>,
}

impl AdbExecutor for DumpReceiptAdb {
    fn run(&mut self, args: &[String]) -> Result<String> {
        if args == ["devices", "-l"] {
            return Ok("List of devices attached\nemulator-5554 device product:test\n".into());
        }
        assert_eq!(&args[..2], ["-s", "emulator-5554"]);
        let tail = args[2..].iter().map(String::as_str).collect::<Vec<_>>();
        match tail.as_slice() {
            ["shell", "getprop", "ro.boot.qemu.avd_name"] => Ok("THS_API_32\n".into()),
            ["shell", "dumpsys", "package", ANDROID_THS_PACKAGE] => {
                Ok("versionName=10.94.09\n".into())
            }
            ["shell", "dumpsys", "activity", "activities"] => Ok(format!(
                "topResumedActivity=ActivityRecord{{x u0 {ANDROID_THS_PACKAGE}/com.hexin.plat.android.Hexin}}\n"
            )),
            ["shell", "uiautomator", "dump", DUMP_PATH] => {
                self.observed.borrow_mut().dumps += 1;
                // An Ok result models exit status 0, including Android's semantic failure.
                Ok(self.receipts.pop_front().unwrap_or_else(|| self.fallback_receipt.clone()))
            }
            ["exec-out", "cat", DUMP_PATH] => {
                let mut observed = self.observed.borrow_mut();
                let dumps = observed.dumps;
                observed.reads_after_dump.push(dumps);
                Ok(self.retained_xml.clone())
            }
            ["shell", "input", ..] => {
                self.observed.borrow_mut().inputs += 1;
                bail!("UI input must not use an unacknowledged retained dump")
            }
            _ => bail!("unexpected fixture command: {tail:?}"),
        }
    }
}

fn node(text: &str, id: &str, description: &str) -> String {
    format!(
        r#"<node text="{text}" resource-id="{ANDROID_THS_PACKAGE}:id/{id}" class="android.widget.TextView" package="{ANDROID_THS_PACKAGE}" content-desc="{description}" clickable="true" bounds="[10,10][100,100]"/>"#
    )
}

fn retained_valid_form() -> String {
    let mut nodes = node("模拟炒股", "page_title_view", "");
    nodes += &node("**0000", "account_info_view", "");
    for label in ["买入", "卖出", "撤单", "持仓", "查询"] {
        nodes += &node(label, "btn", label);
    }
    for id in ["auto_stockcode", "main_scroller", "weituo", "chengjiao"] {
        nodes += &node("", id, "");
    }
    format!("<?xml version=\"1.0\"?><hierarchy>{nodes}</hierarchy>")
}

fn driver(
    receipts: &[&str],
    fallback_receipt: &str,
    retained_xml: String,
) -> (
    AndroidThsSimulationUiDriver<DumpReceiptAdb>,
    Rc<RefCell<Observed>>,
) {
    let observed = Rc::new(RefCell::new(Observed::default()));
    let executor = DumpReceiptAdb {
        receipts: receipts.iter().map(|value| (*value).to_owned()).collect(),
        fallback_receipt: fallback_receipt.to_owned(),
        retained_xml,
        observed: Rc::clone(&observed),
    };
    (
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor),
        observed,
    )
}

fn assert_unacknowledged_dump_rejected(receipt: &str) {
    let (mut driver, observed) = driver(&[], receipt, retained_valid_form());
    let result = driver.probe_identity();
    let observed = observed.borrow();
    assert!(
        observed.reads_after_dump.is_empty(),
        "stale XML was read after unacknowledged dump {receipt:?}: {:?}",
        observed.reads_after_dump
    );
    assert_eq!(observed.inputs, 0);
    assert!(
        result.is_err(),
        "stale XML must not be accepted as current identity"
    );
    assert_eq!(
        observed.dumps, 10,
        "preserve the existing bounded retry budget"
    );
}

#[test]
fn exit_zero_idle_failure_cannot_read_retained_xml() {
    assert_unacknowledged_dump_rejected("ERROR: could not get idle state.\r\n");
}

#[test]
fn empty_dump_stdout_cannot_read_retained_xml() {
    assert_unacknowledged_dump_rejected("");
}

#[test]
fn wrong_dump_destination_cannot_read_retained_xml() {
    assert_unacknowledged_dump_rejected("UI hierchary dumped to: /sdcard/other.xml\n");
}

#[test]
fn success_substring_with_error_cannot_read_retained_xml() {
    assert_unacknowledged_dump_rejected(&format!("{SUCCESS}ERROR: could not get idle state.\n"));
}

#[test]
fn exit_zero_idle_failure_cannot_navigate_using_retained_landing_xml() {
    let xml = format!(
        "<hierarchy>{}{}</hierarchy>",
        node("模拟练习区", "landing", ""),
        node("买入", "buy", "")
    );
    let (mut driver, observed) = driver(&[], "ERROR: could not get idle state.\n", xml);
    assert!(driver.probe_identity().is_err());
    let observed = observed.borrow();
    assert_eq!(observed.inputs, 0, "stale landing XML caused a UI click");
    assert!(observed.reads_after_dump.is_empty());
    assert_eq!(observed.dumps, 10);
}

#[test]
fn transient_exit_zero_idle_failures_retry_before_reading_xml() -> Result<()> {
    let (mut driver, observed) = driver(
        &[
            "ERROR: could not get idle state.\n",
            "ERROR: could not get idle state.\n",
        ],
        SUCCESS,
        retained_valid_form(),
    );
    let evidence = driver.probe_identity()?;
    assert_eq!(evidence.masked_account, "**0000");
    let observed = observed.borrow();
    assert_eq!(observed.dumps, 3);
    assert_eq!(observed.reads_after_dump, [3]);
    assert_eq!(observed.inputs, 0);
    Ok(())
}

#[test]
fn canonical_success_receipt_preserves_identity_probe() -> Result<()> {
    for success in [SUCCESS, SUCCESS.trim()] {
        let (mut driver, observed) = driver(&[], success, retained_valid_form());
        let evidence = driver.probe_identity()?;
        assert_eq!(evidence.package, ANDROID_THS_PACKAGE);
        assert_eq!(evidence.masked_account, "**0000");
        let observed = observed.borrow();
        assert_eq!(observed.dumps, 1);
        assert_eq!(observed.reads_after_dump, [1]);
        assert_eq!(observed.inputs, 0);
    }
    Ok(())
}
