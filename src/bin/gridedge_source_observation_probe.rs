//! Research-only raw source recorder. No market publisher or trading imports.
//! A returned/labeled bar is never represented as admitted or final market data.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    thread,
    time::Duration,
};

const URL: &str = "https://money.finance.sina.com.cn/quotes_service/api/jsonp_v2.php/var%20gridedge_probe=/CN_MarketData.getKLineData?symbol=sz002256&scale=5&ma=no&datalen=1023";
const PREFIX: &str = "/*<script>location.href='//sina.com';</script>*/";
const KIND: &str = "UNREVIEWED_SOURCE_RESEARCH_NOT_TRADING_EVIDENCE";

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
    /// Offline diagnostic input, only with --once. Never evaluated as JavaScript.
    #[arg(long, requires = "once")]
    fixture: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    day: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

fn inspect(bytes: &[u8]) -> Result<serde_json::Value> {
    if bytes.is_empty() || bytes.len() > 1_048_576 {
        bail!("source response size outside diagnostic bounds");
    }
    let text = std::str::from_utf8(bytes)?.trim();
    let body = text
        .strip_prefix(PREFIX)
        .context("unexpected source wrapper")?
        .trim()
        .strip_prefix("var gridedge_probe=(")
        .and_then(|s| s.strip_suffix(");"))
        .context("unexpected JSONP assignment; response was not executed")?;
    let rows: Vec<Row> = serde_json::from_str(body)?;
    if rows.is_empty() || rows.len() > 1023 {
        bail!("unexpected source row count");
    }
    let mut previous = None;
    let mut dates = BTreeSet::new();
    for row in &rows {
        let at = NaiveDateTime::parse_from_str(&row.day, "%Y-%m-%d %H:%M:%S")?;
        if previous.is_some_and(|p| at <= p) {
            bail!("duplicate or reordered source timestamp");
        }
        previous = Some(at);
        dates.insert(at.date().to_string());
        let open = Decimal::from_str(&row.open)?;
        let high = Decimal::from_str(&row.high)?;
        let low = Decimal::from_str(&row.low)?;
        let close = Decimal::from_str(&row.close)?;
        let volume: u64 = row.volume.parse()?;
        if low <= Decimal::ZERO || open < low || close < low || open > high || close > high {
            bail!("invalid source OHLC");
        }
        // Quantity is parsed as an integer; zero volume remains observable.
        let _ = volume;
    }
    Ok(json!({
        "kind": KIND,
        "row_count": rows.len(),
        "first_labeled_time": rows[0].day,
        "last_labeled_time": rows.last().context("empty source")?.day,
        "observed_dates": dates,
        "source_completeness_proven": false,
        "instrument_echo_present": false,
        "amount_present": false,
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
                "--fail",
                "--proto",
                "=https",
                "--connect-timeout",
                "8",
                "--max-time",
                "20",
                "--max-filesize",
                "1048576",
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
        "admitted": false,
    });
    match response {
        Ok(bytes) => {
            let name = format!("response-{index:05}.jsonp");
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
    // Raw and receipt are separate immutable files, including failures.
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
    // This fails if the directory already exists: no re-use of a previous run.
    fs::create_dir(&args.output_dir).context("research output must be a new directory")?;
    write_new(
        &args.output_dir.join("identity.json"),
        &serde_json::to_vec_pretty(&json!({
            "kind": KIND, "pid": std::process::id(), "created_at": now,
            "start_at": start, "stop_at": stop, "interval_seconds": args.interval_seconds,
            "url": URL, "fixture": args.fixture, "formal_publication": false,
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

    fn body(rows: &str) -> Vec<u8> {
        format!("{PREFIX}\nvar gridedge_probe=({rows});").into_bytes()
    }

    const ROW: &str = r#"{"day":"2026-09-07 15:00:00","open":"3.340","high":"3.350","low":"3.340","close":"3.340","volume":"642600"}"#;

    #[test]
    fn valid_ohlc_is_not_proof_of_completion() {
        let value = inspect(&body(&format!("[{ROW}]"))).unwrap();
        assert_eq!(value["admitted"], false);
        assert_eq!(value["source_completeness_proven"], false);
        assert_eq!(value["instrument_echo_present"], false);
    }

    #[test]
    fn latest_future_bucket_remains_raw_research_only() {
        let row = ROW.replace("2026-09-07", "2099-09-07");
        assert_eq!(
            inspect(&body(&format!("[{row}]"))).unwrap()["admitted"],
            false
        );
    }

    #[test]
    fn does_not_execute_or_accept_extra_javascript() {
        let mut bytes = body(&format!("[{ROW}]"));
        bytes.extend_from_slice(b";dangerous()");
        assert!(inspect(&bytes).is_err());
    }

    #[test]
    fn duplicate_or_reordered_rows_are_rejected() {
        assert!(inspect(&body(&format!("[{ROW},{ROW}]"))).is_err());
        let early = ROW.replace("15:00:00", "14:55:00");
        assert!(inspect(&body(&format!("[{ROW},{early}]"))).is_err());
    }

    #[test]
    fn decimal_bounds_and_integer_volume_are_checked() {
        for invalid in [
            ROW.replace("3.350", "3.330"),
            ROW.replace("642600", "-1"),
            ROW.replace("642600", "1.5"),
            ROW.replace("3.340", "NaN"),
        ] {
            assert!(inspect(&body(&format!("[{invalid}]"))).is_err());
        }
    }

    #[test]
    fn schema_drift_and_empty_results_are_rejected() {
        assert!(inspect(&body("[]")).is_err());
        let unknown = ROW.replace("\"volume\"", "\"new_volume\"");
        assert!(inspect(&body(&format!("[{unknown}]"))).is_err());
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
