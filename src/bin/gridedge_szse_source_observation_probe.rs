//! Research-only recorder for the public SZSE quote-page minute snapshot.
//!
//! The endpoint returns minute last/average prices plus exact volume and amount,
//! not OHLC bars or a source-native completion/revision sequence. Consequently
//! every capture remains isolated research evidence and can never be published
//! as formal market data by this program.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, NaiveTime, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

const URL: &str = "https://www.szse.cn/api/market/ssjjhq/getTimeData?marketId=1&code=002256";
const REFERER: &str =
    "https://www.szse.cn/English/siteMarketData/siteMarketDatas/lookup/index.html";
const EXPECTED_CODE: &str = "002256";
const EXPECTED_NAME: &str = "兆新股份";
const KIND: &str = "UNREVIEWED_SZSE_PUBLIC_MINUTE_RESEARCH_NOT_TRADING_EVIDENCE";

#[derive(Parser)]
struct Args {
    /// New, exclusive directory under an existing research parent.
    #[arg(long)]
    output_dir: PathBuf,
    /// Offset-qualified observation start; defaults to now.
    #[arg(long)]
    start_at: Option<DateTime<Utc>>,
    /// Offset-qualified stop deadline, at most four hours after start.
    #[arg(long)]
    stop_at: Option<DateTime<Utc>>,
    #[arg(long, default_value_t = 30)]
    interval_seconds: u64,
    /// Record one response and exit, rather than scheduling a window.
    #[arg(long, conflicts_with_all = ["start_at", "stop_at"])]
    once: bool,
    /// Offline diagnostic input, only with --once.
    #[arg(long, requires = "once")]
    fixture: Option<PathBuf>,
}

#[derive(Deserialize)]
struct SourceResponse {
    code: String,
    datetime: String,
    data: SourceData,
}

#[derive(Deserialize)]
struct SourceData {
    code: String,
    name: String,
    #[serde(rename = "marketTime")]
    market_time: String,
    volume: u64,
    amount: Box<RawValue>,
    picupdata: Vec<Vec<Box<RawValue>>>,
}

fn decimal(value: &RawValue, field: &str) -> Result<Decimal> {
    // Preserve numeric JSON tokens verbatim; never pass money through Value/f64.
    let raw = value.get();
    let decoded: String;
    let text = if raw.starts_with('"') {
        decoded = serde_json::from_str(raw)?;
        &decoded
    } else {
        raw
    };
    Decimal::from_str_exact(text).with_context(|| format!("invalid decimal field {field}"))
}

fn unsigned(value: &RawValue, field: &str) -> Result<u64> {
    serde_json::from_str(value.get())
        .with_context(|| format!("invalid unsigned integer field {field}"))
}

fn amount_cents(value: &RawValue, field: &str) -> Result<u128> {
    let amount = decimal(value, field)?;
    let coefficient = u128::try_from(amount.mantissa()).context("negative amount")?;
    let scale = amount.scale();
    if scale <= 2 {
        coefficient
            .checked_mul(10_u128.pow(2 - scale))
            .context("amount overflow")
    } else {
        let divisor = 10_u128.pow(scale - 2);
        if coefficient % divisor != 0 {
            bail!("sub-cent amount in {field}");
        }
        Ok(coefficient / divisor)
    }
}

fn format_cents(amount: u128) -> String {
    format!("{}.{:02}", amount / 100, amount % 100)
}

fn regular_session_time(time: NaiveTime) -> bool {
    let morning_start = NaiveTime::from_hms_opt(9, 30, 0).expect("valid constant");
    let morning_end = NaiveTime::from_hms_opt(11, 30, 0).expect("valid constant");
    let afternoon_start = NaiveTime::from_hms_opt(13, 1, 0).expect("valid constant");
    let afternoon_end = NaiveTime::from_hms_opt(15, 0, 0).expect("valid constant");
    (morning_start..=morning_end).contains(&time)
        || (afternoon_start..=afternoon_end).contains(&time)
}

fn inspect(bytes: &[u8]) -> Result<Value> {
    if bytes.is_empty() || bytes.len() > 1_048_576 {
        bail!("source response size outside diagnostic bounds");
    }
    let root: SourceResponse = serde_json::from_slice(bytes)?;
    if root.code != "0" {
        bail!("source response code is not successful");
    }
    let data = &root.data;
    if data.code != EXPECTED_CODE || data.name != EXPECTED_NAME {
        bail!("source instrument identity mismatch");
    }

    let snapshot_at = NaiveDateTime::parse_from_str(&root.datetime, "%Y-%m-%d %H:%M")?;
    let market_time = NaiveDateTime::parse_from_str(&data.market_time, "%Y-%m-%d %H:%M:%S")?;
    if snapshot_at.date() != market_time.date() {
        bail!("snapshot and market dates differ");
    }

    let aggregate_volume_lots = data.volume;
    let aggregate_amount = amount_cents(&data.amount, "amount")?;

    let rows = &data.picupdata;
    if rows.is_empty() || rows.len() > 512 {
        bail!("unexpected minute row count");
    }

    let mut regular_volume_lots = 0_u64;
    let mut regular_amount = 0_u128;
    let mut regular_rows = 0_u64;
    let mut after_hours_rows = 0_u64;
    let mut previous = None;
    let mut last_regular_time = None;
    for (index, row) in rows.iter().enumerate() {
        if row.len() != 7 {
            bail!("minute row {index} does not have seven fields");
        }
        let time = NaiveTime::parse_from_str(
            &serde_json::from_str::<String>(row[0].get())
                .with_context(|| format!("minute row {index} lacks time"))?,
            "%H:%M",
        )?;
        if previous.is_some_and(|prior| time <= prior) {
            bail!("duplicate or reordered minute timestamp");
        }
        previous = Some(time);

        // Official quote-page JavaScript labels these as Last, Avg., Change,
        // % Change, Volume and Amount. They are not an OHLC record.
        for (offset, field) in ["last", "average", "change", "change_percent"]
            .iter()
            .enumerate()
        {
            let value = decimal(&row[offset + 1], field)?;
            if offset < 2 && value <= Decimal::ZERO {
                bail!("invalid minute price field");
            }
        }
        let volume_lots = unsigned(&row[5], "minute volume")?;
        let amount = amount_cents(&row[6], "minute amount")?;

        if regular_session_time(time) {
            regular_rows += 1;
            regular_volume_lots = regular_volume_lots
                .checked_add(volume_lots)
                .context("minute volume overflow")?;
            regular_amount = regular_amount
                .checked_add(amount)
                .context("minute amount overflow")?;
            last_regular_time = Some(time);
        } else if time >= NaiveTime::from_hms_opt(15, 6, 0).expect("valid constant") {
            after_hours_rows += 1;
        } else {
            bail!("minute row is outside reviewed regular/after-hours labels");
        }
    }

    let totals_reconcile =
        regular_volume_lots == aggregate_volume_lots && regular_amount == aggregate_amount;
    Ok(json!({
        "kind": KIND,
        "instrument_code": EXPECTED_CODE,
        "instrument_name": EXPECTED_NAME,
        "snapshot_date": snapshot_at.date().to_string(),
        "snapshot_at": snapshot_at.to_string(),
        "market_time": market_time.to_string(),
        "row_count": rows.len(),
        "regular_session_rows": regular_rows,
        "after_hours_rows": after_hours_rows,
        "last_regular_label": last_regular_time.context("no regular-session row")?.format("%H:%M").to_string(),
        "aggregate_volume_lots": aggregate_volume_lots,
        "regular_volume_lots": regular_volume_lots,
        "aggregate_amount": format_cents(aggregate_amount),
        "regular_amount": format_cents(regular_amount),
        "regular_session_totals_reconcile": totals_reconcile,
        "minute_fields": ["time", "last", "average", "change", "change_percent", "volume_lots", "amount"],
        "ohlc_present": false,
        "source_sequence_present": false,
        "source_completion_proven": false,
        "source_revision_semantics_proven": false,
        "formal_publication": false,
        "admitted": false,
    }))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn status(dir: &Path, phase: &str) -> Result<()> {
    let tmp = dir.join("status.next");
    write_new(
        &tmp,
        &serde_json::to_vec_pretty(&json!({
            "at": Utc::now(), "pid": std::process::id(), "phase": phase, "kind": KIND,
        }))?,
    )?;
    fs::rename(tmp, dir.join("status.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn capture(dir: &Path, index: usize, fixture: Option<&Path>) -> Result<()> {
    let started = Utc::now();
    let response = if let Some(path) = fixture {
        Ok(fs::read(path)?)
    } else {
        match Command::new("/usr/bin/curl")
            .args([
                "--silent",
                "--show-error",
                "--fail-with-body",
                "--compressed",
                "--proto",
                "=https",
                "--connect-timeout",
                "8",
                "--max-time",
                "20",
                "--max-filesize",
                "1048576",
                "--user-agent",
                "Mozilla/5.0",
                "--referer",
                REFERER,
                URL,
            ])
            .output()
        {
            Ok(output) if output.status.success() => Ok(output.stdout),
            Ok(output) => Err(format!("curl_exit={:?}", output.status.code())),
            Err(error) => Err(format!("curl_spawn={:?}", error.kind())),
        }
    };
    let received = Utc::now();
    let mut record = json!({
        "kind": KIND, "sequence": index, "started_at": started,
        "received_at": received, "url": URL, "fixture": fixture.is_some(),
        "formal_publication": false, "admitted": false,
    });
    match response {
        Ok(bytes) => {
            let name = format!("response-{index:05}.json");
            write_new(&dir.join(&name), &bytes)?;
            record["raw_file"] = json!(name);
            record["raw_sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
            match inspect(&bytes) {
                Ok(summary) => record["inspection"] = summary,
                Err(error) => record["inspection_error"] = json!(error.to_string()),
            }
        }
        Err(error) => record["transport_error"] = json!(error),
    }
    write_new(
        &dir.join(format!("receipt-{index:05}.json")),
        &serde_json::to_vec_pretty(&record)?,
    )?;
    println!("{}", serde_json::to_string(&record)?);
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !(15..=60).contains(&args.interval_seconds) {
        bail!("research interval must be 15..60 seconds");
    }
    let now = Utc::now();
    let start = args.start_at.unwrap_or(now);
    let stop = if args.once {
        now
    } else {
        args.stop_at
            .context("--stop-at is required unless --once")?
    };
    if !args.once
        && (stop <= start
            || stop <= now
            || stop - start > chrono::Duration::hours(4)
            || start - now > chrono::Duration::hours(24))
    {
        bail!("invalid research window");
    }
    fs::create_dir(&args.output_dir).context("research output must be a new directory")?;
    write_new(
        &args.output_dir.join("identity.json"),
        &serde_json::to_vec_pretty(&json!({
            "kind": KIND, "pid": std::process::id(), "created_at": now,
            "start_at": start, "stop_at": stop, "interval_seconds": args.interval_seconds,
            "url": URL, "referer": REFERER, "fixture": args.fixture,
            "formal_publication": false, "admitted": false,
        }))?,
    )?;
    while Utc::now() < start {
        status(&args.output_dir, "WAITING_FOR_RESEARCH_WINDOW")?;
        thread::sleep(Duration::from_secs(15));
    }
    let mut index = 0;
    loop {
        if !args.once && Utc::now() >= stop {
            break;
        }
        status(&args.output_dir, "CAPTURING_RESEARCH_ONLY")?;
        capture(&args.output_dir, index, args.fixture.as_deref())?;
        index += 1;
        if args.once {
            break;
        }
        thread::sleep(Duration::from_secs(args.interval_seconds));
    }
    status(&args.output_dir, "COMPLETED_RESEARCH_ONLY")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(code: &str, name: &str, volume: u64, amount: &str, rows: &str) -> Vec<u8> {
        format!(
            r#"{{"datetime":"2026-09-08 15:30","code":"0","data":{{"code":"{code}","name":"{name}","marketTime":"2026-09-08 15:30:00","volume":{volume},"amount":{amount},"picupdata":[{rows}]}}}}"#
        )
        .into_bytes()
    }

    const REGULAR: &str = r#"["15:00","3.39","3.38","0.05","1.50",3723,1262097.00]"#;

    #[test]
    fn raw_amount_preserves_high_precision_and_one_cent_difference() {
        let rows = r#"["15:00","3.39","3.38","0.05","1.50",1,9007199254740991.01]"#;
        let exact = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            1,
            "9007199254740991.01",
            rows,
        ))
        .unwrap();
        assert_eq!(exact["regular_amount"], "9007199254740991.01");
        assert_eq!(exact["aggregate_amount"], "9007199254740991.01");
        let mismatch = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            1,
            "9007199254740991.02",
            rows,
        ))
        .unwrap();
        assert_eq!(mismatch["regular_session_totals_reconcile"], false);
        assert_eq!(mismatch["admitted"], false);
    }

    #[test]
    fn numeric_overflow_excess_precision_and_duplicate_fields_are_rejected() {
        let overflow = r#"["14:59","3.39","3.38","0.05","1.50",1,79228162514264337593543950335],["15:00","3.39","3.38","0.05","1.50",1,1]"#;
        let value = inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 2, "1", overflow)).unwrap();
        assert_eq!(value["regular_amount"], "79228162514264337593543950336.00");
        assert_eq!(value["regular_session_totals_reconcile"], false);
        let too_precise =
            r#"["15:00","3.39","3.38","0.05","1.50",1,0.12345678901234567890123456789]"#;
        assert!(inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 1, "1", too_precise)).is_err());
        let duplicate = String::from_utf8(response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            3723,
            "1262097",
            REGULAR,
        ))
        .unwrap()
        .replace("\"amount\":1262097", "\"amount\":1262097,\"amount\":0");
        assert!(inspect(duplicate.as_bytes()).is_err());
    }

    #[test]
    fn maximum_decimal_plus_one_cent_never_silently_reconciles() {
        let rows = r#"["14:59","3.39","3.38","0.05","1.50",1,79228162514264337593543950335],["15:00","3.39","3.38","0.05","1.50",1,0.01]"#;
        let value = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            2,
            "79228162514264337593543950335",
            rows,
        ))
        .unwrap();
        assert_eq!(value["regular_amount"], "79228162514264337593543950335.01");
        assert_eq!(value["regular_session_totals_reconcile"], false);
        assert_eq!(value["admitted"], false);
        let subcent = rows.replace("0.01]", "0.001]");
        assert!(inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 2, "1", &subcent)).is_err());
    }

    #[test]
    fn exact_regular_totals_still_do_not_admit_the_source() {
        let value = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            3723,
            "1262097.00",
            REGULAR,
        ))
        .unwrap();
        assert_eq!(value["regular_session_totals_reconcile"], true);
        assert_eq!(value["ohlc_present"], false);
        assert_eq!(value["source_completion_proven"], false);
        assert_eq!(value["admitted"], false);
    }

    #[test]
    fn after_hours_rows_are_excluded_from_regular_aggregate() {
        let rows = format!("{REGULAR},[\"15:06\",\"3.39\",\"3.38\",\"0.05\",\"1.50\",11,3729.00]");
        let value = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            3723,
            "1262097.00",
            &rows,
        ))
        .unwrap();
        assert_eq!(value["regular_volume_lots"], 3723);
        assert_eq!(value["after_hours_rows"], 1);
    }

    #[test]
    fn identity_mismatch_is_rejected() {
        assert!(inspect(&response("000001", EXPECTED_NAME, 3723, "1262097", REGULAR)).is_err());
        assert!(inspect(&response(EXPECTED_CODE, "other", 3723, "1262097", REGULAR)).is_err());
    }

    #[test]
    fn aggregate_mismatch_remains_visible_without_false_admission() {
        let value = inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            3724,
            "1262097",
            REGULAR,
        ))
        .unwrap();
        assert_eq!(value["regular_session_totals_reconcile"], false);
        assert_eq!(value["admitted"], false);
    }

    #[test]
    fn duplicate_reordered_and_unknown_session_rows_are_rejected() {
        let duplicate = format!("{REGULAR},{REGULAR}");
        assert!(inspect(&response(
            EXPECTED_CODE,
            EXPECTED_NAME,
            7446,
            "2524194",
            &duplicate
        ))
        .is_err());
        let unknown = r#"["12:00","3.39","3.38","0.05","1.50",1,3.39]"#;
        assert!(inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 1, "3.39", unknown)).is_err());
    }

    #[test]
    fn invalid_decimal_or_row_shape_is_rejected() {
        let invalid = r#"["15:00","NaN","3.38","0.05","1.50",1,3.39]"#;
        assert!(inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 1, "3.39", invalid)).is_err());
        let short = r#"["15:00","3.39",1]"#;
        assert!(inspect(&response(EXPECTED_CODE, EXPECTED_NAME, 1, "3.39", short)).is_err());
    }

    #[test]
    fn immutable_evidence_cannot_be_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("raw");
        write_new(&file, b"first").unwrap();
        assert!(write_new(&file, b"second").is_err());
        assert_eq!(fs::read(file).unwrap(), b"first");
    }
}
