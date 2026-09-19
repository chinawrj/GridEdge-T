//! Offline research analysis only. No network, publisher, ledger or execution API.
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Read, path::Path, str::FromStr};

const KIND: &str = "UNREVIEWED_SOURCE_RESEARCH_NOT_TRADING_EVIDENCE";
const URL: &str = "https://money.finance.sina.com.cn/quotes_service/api/jsonp_v2.php/var%20gridedge_probe=/CN_MarketData.getKLineData?symbol=sz002256&scale=5&ma=no&datalen=1023";

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input_dir: std::path::PathBuf,
    #[arg(long)]
    date: NaiveDate,
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

#[derive(Clone, PartialEq)]
struct Prices {
    ohlc: [Decimal; 4],
    volume: u64,
}

fn rows(bytes: &[u8]) -> Result<Vec<(NaiveDateTime, Prices)>> {
    let body = std::str::from_utf8(bytes)?
        .trim()
        .strip_prefix("/*<script>location.href='//sina.com';</script>*/")
        .context("unexpected wrapper")?
        .trim()
        .strip_prefix("var gridedge_probe=(")
        .and_then(|s| s.strip_suffix(");"))
        .context("unexpected assignment")?;
    let raw: Vec<Row> = serde_json::from_str(body)?;
    ensure!(!raw.is_empty() && raw.len() <= 1023, "invalid row count");
    let mut result = Vec::new();
    let mut previous = None;
    for row in raw {
        let day = NaiveDateTime::parse_from_str(&row.day, "%Y-%m-%d %H:%M:%S")?;
        ensure!(previous.is_none_or(|p| day > p), "unordered rows");
        previous = Some(day);
        let [o, h, l, c] = [row.open, row.high, row.low, row.close]
            .map(|s| Decimal::from_str(&s))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| anyhow::anyhow!("OHLC length"))?;
        ensure!(
            l > Decimal::ZERO && o >= l && c >= l && o <= h && c <= h,
            "invalid OHLC"
        );
        result.push((
            day,
            Prices {
                ohlc: [o, h, l, c],
                volume: row.volume.parse()?,
            },
        ));
    }
    Ok(result)
}

fn bytes(path: &Path) -> Result<Vec<u8>> {
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "symlink evidence rejected"
    );
    let mut value = Vec::new();
    fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut value)?;
    ensure!(value.len() <= 1_048_576, "oversize evidence");
    Ok(value)
}

fn hash(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}
fn time(v: &Value, key: &str) -> Result<DateTime<Utc>> {
    Ok(
        DateTime::parse_from_rfc3339(v[key].as_str().context("missing timestamp")?)?
            .with_timezone(&Utc),
    )
}

struct Seen {
    first: DateTime<Utc>,
    last: DateTime<Utc>,
    first_after_end: Option<DateTime<Utc>>,
    values: Prices,
    observations: usize,
    revisions: Vec<Value>,
}

fn analyze(dir: &Path, date: NaiveDate) -> Result<Value> {
    let identity_bytes = bytes(&dir.join("identity.json"))?;
    let identity: Value = serde_json::from_slice(&identity_bytes)?;
    ensure!(
        identity["kind"] == KIND
            && identity["url"] == URL
            && identity["formal_publication"] == false,
        "unbound research identity"
    );
    let fixture = !identity["fixture"].is_null();
    let mut receipts = fs::read_dir(dir)?
        .map(|e| Ok(e?.file_name()))
        .collect::<Result<Vec<_>>>()?;
    receipts.retain(|n| n.to_string_lossy().starts_with("receipt-"));
    receipts.sort();
    ensure!(
        !receipts.is_empty() && receipts.len() <= 4096,
        "no receipts or excessive count"
    );
    let offset = FixedOffset::east_opt(8 * 3600).context("offset")?;
    let mut seen: BTreeMap<NaiveDateTime, Seen> = BTreeMap::new();
    let mut failures = Vec::new();
    let mut previous_received = None;
    let mut first_started = None;
    let mut max_gap_ms = 0;
    let mut digest = Sha256::new();
    let mut disappearing = Vec::new();
    for (index, name) in receipts.iter().enumerate() {
        ensure!(
            name == format!("receipt-{index:05}.json").as_str(),
            "receipt sequence gap/name mismatch"
        );
        let receipt_bytes = bytes(&dir.join(name))?;
        digest.update((receipt_bytes.len() as u64).to_be_bytes());
        digest.update(&receipt_bytes);
        let r: Value = serde_json::from_slice(&receipt_bytes)?;
        ensure!(
            r["kind"] == KIND
                && r["admitted"] == false
                && r["url"] == URL
                && r["sequence"].as_u64() == Some(index as u64)
                && r["fixture"] == fixture,
            "receipt identity mismatch"
        );
        let start = time(&r, "started_at")?;
        let received = time(&r, "received_at")?;
        ensure!(
            received >= start && previous_received.is_none_or(|p| start >= p),
            "observation time reversal"
        );
        if let Some(p) = previous_received {
            max_gap_ms = max_gap_ms.max((received - p).num_milliseconds());
        }
        previous_received = Some(received);
        first_started.get_or_insert(start);
        if r.get("transport_error").is_some() {
            ensure!(r.get("raw_file").is_none(), "failure mixed with response");
            failures.push(json!({"sequence": index, "transport_error": r["transport_error"]}));
            continue;
        }
        let raw_name = format!("response-{index:05}.jsonp");
        ensure!(r["raw_file"] == raw_name, "raw path binding mismatch");
        let raw = bytes(&dir.join(raw_name))?;
        ensure!(r["raw_sha256"] == hash(&raw), "raw hash mismatch");
        let parsed = match rows(&raw) {
            Ok(rows) => rows,
            Err(error) => {
                failures.push(json!({"sequence": index, "parse_error": error.to_string()}));
                continue;
            }
        };
        let current: BTreeMap<_, _> = parsed
            .into_iter()
            .filter(|(t, _)| t.date() == date)
            .collect();
        for label in seen.keys().filter(|label| !current.contains_key(label)) {
            disappearing.push(json!({"sequence": index, "label": label}));
        }
        for (label, values) in current {
            let end = offset
                .from_local_datetime(&label)
                .single()
                .context("label timezone")?
                .with_timezone(&Utc);
            if let Some(s) = seen.get_mut(&label) {
                if s.values != values {
                    let changed_fields: Vec<_> = ["open", "high", "low", "close"]
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| s.values.ohlc[*i] != values.ohlc[*i])
                        .map(|(_, name)| *name)
                        .collect();
                    s.revisions
                        .push(json!({"sequence": index, "received_at": received,
                        "observed_after_label_end": received > end,
                        "changed_fields": changed_fields,
                        "volume_changed": s.values.volume != values.volume}));
                }
                s.values = values;
                s.last = received;
                s.observations += 1;
                if received >= end {
                    s.first_after_end.get_or_insert(received);
                }
            } else {
                seen.insert(
                    label,
                    Seen {
                        first: received,
                        last: received,
                        first_after_end: (received >= end).then_some(received),
                        values,
                        observations: 1,
                        revisions: Vec::new(),
                    },
                );
            }
        }
    }
    let expected: Vec<_> = [(9, 30), (13, 0)]
        .into_iter()
        .flat_map(|(h, m)| {
            let start = date.and_hms_opt(h, m, 0).expect("fixed valid time");
            (1..=24).map(move |i| start + Duration::minutes(i * 5))
        })
        .collect();
    let missing: Vec<_> = expected.iter().filter(|t| !seen.contains_key(t)).collect();
    let unexpected: Vec<_> = seen.keys().filter(|t| !expected.contains(t)).collect();
    let bars: Vec<_> = seen.iter().map(|(label, s)| {
        let end = offset.from_local_datetime(label).single().expect("fixed offset").with_timezone(&Utc);
        json!({"label": label, "first_seen_at": s.first, "last_seen_at": s.last,
            "first_seen_minus_label_ms": (s.first-end).num_milliseconds(),
            "first_observation_at_or_after_label_ms": s.first_after_end.map(|t| (t-end).num_milliseconds()),
            "observations": s.observations, "revisions": s.revisions})
    }).collect();
    Ok(
        json!({"kind": KIND, "admitted": false, "source_completeness_proven": false,
        "claim": "OBSERVED_BEHAVIOR_ONLY_NOT_FINALITY_OR_TRADING_ACCEPTANCE",
        "identity_sha256": hash(&identity_bytes), "ordered_receipts_sha256": format!("{:x}", digest.finalize()),
        "date": date, "url": URL, "fixture": fixture, "receipt_count": receipts.len(),
        "first_request_at": first_started, "last_receipt_at": previous_received,
        "maximum_receipt_gap_ms": max_gap_ms, "failures": failures,
        "missing_session_labels_not_necessarily_due_yet": missing, "unexpected_session_labels": unexpected,
        "disappearing_label_observations": disappearing, "bars": bars,
        "limitations": ["No symbol echo, amount or source finality flag",
            "A stable response does not certify completeness", "Post-label revisions describe observation timing, not exchange correction timing",
            "Requested date is an analysis filter, not an authenticated trading calendar"]}),
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.input_dir.is_dir() {
        bail!("research input directory missing");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&analyze(&args.input_dir, args.date)?)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        fs::write(
            d.path().join("identity.json"),
            json!({"kind":KIND,"url":URL,"formal_publication":false,"fixture":"test"}).to_string(),
        )
        .unwrap();
        d
    }
    fn add(d: &Path, index: usize, at: &str, close: &str) {
        let raw = format!("/*<script>location.href='//sina.com';</script>*/\nvar gridedge_probe=([{{\"day\":\"2026-09-08 09:35:00\",\"open\":\"3\",\"high\":\"4\",\"low\":\"2\",\"close\":\"{close}\",\"volume\":\"100\"}}]);");
        let name = format!("response-{index:05}.jsonp");
        fs::write(d.join(&name), &raw).unwrap();
        fs::write(d.join(format!("receipt-{index:05}.json")), json!({"kind":KIND,"url":URL,"admitted":false,"fixture":true,"sequence":index,"started_at":at,"received_at":at,"raw_file":name,"raw_sha256":hash(raw.as_bytes())}).to_string()).unwrap();
    }
    fn report(d: &Path) -> Result<Value> {
        analyze(d, NaiveDate::from_ymd_opt(2026, 9, 8).unwrap())
    }
    #[test]
    fn forming_and_post_label_changes_never_prove_finality() {
        let d = setup();
        add(d.path(), 0, "2026-09-08T09:34:30+08:00", "3");
        add(d.path(), 1, "2026-09-08T09:35:30+08:00", "3.1");
        let r = report(d.path()).unwrap();
        assert_eq!(r["bars"][0]["first_seen_minus_label_ms"], -30000);
        assert_eq!(
            r["bars"][0]["first_observation_at_or_after_label_ms"],
            30000
        );
        assert_eq!(
            r["bars"][0]["revisions"][0]["observed_after_label_end"],
            true
        );
        assert_eq!(r["source_completeness_proven"], false);
        assert_eq!(r["admitted"], false);
        assert_eq!(
            r["missing_session_labels_not_necessarily_due_yet"]
                .as_array()
                .unwrap()
                .len(),
            47
        );
    }
    #[test]
    fn decimal_formatting_is_not_a_price_revision() {
        let d = setup();
        add(d.path(), 0, "2026-09-08T09:35:30+08:00", "3");
        add(d.path(), 1, "2026-09-08T09:36:00+08:00", "3.000");
        let r = report(d.path()).unwrap();
        assert_eq!(r["bars"][0]["revisions"], json!([]));
        assert_eq!(r["source_completeness_proven"], false);
    }
    #[test]
    fn tampered_raw_and_sequence_holes_fail() {
        let d = setup();
        add(d.path(), 1, "2026-09-08T09:35:30+08:00", "3");
        assert!(report(d.path()).is_err());
        add(d.path(), 0, "2026-09-08T09:35:00+08:00", "3");
        fs::write(d.path().join("response-00000.jsonp"), "tampered").unwrap();
        assert!(report(d.path()).is_err());
    }
    #[test]
    fn time_reversal_and_identity_drift_fail() {
        let d = setup();
        add(d.path(), 0, "2026-09-08T09:36:00+08:00", "3");
        add(d.path(), 1, "2026-09-08T09:35:00+08:00", "3");
        assert!(report(d.path()).is_err());
        fs::write(d.path().join("identity.json"), "{}").unwrap();
        assert!(report(d.path()).is_err());
    }
    #[test]
    fn transport_failure_remains_visible_and_empty_directory_is_not_success() {
        let d = setup();
        assert!(report(d.path()).is_err());
        fs::write(d.path().join("receipt-00000.json"),json!({"kind":KIND,"url":URL,"admitted":false,"fixture":true,"sequence":0,"started_at":"2026-09-08T09:35:00+08:00","received_at":"2026-09-08T09:35:20+08:00","transport_error":"curl_exit=28"}).to_string()).unwrap();
        let r = report(d.path()).unwrap();
        assert_eq!(r["failures"].as_array().unwrap().len(), 1);
        assert_eq!(r["bars"], json!([]));
        assert_eq!(r["source_completeness_proven"], false);
    }
    #[test]
    fn raw_parser_rejects_schema_extra_code_and_bad_financial_fields() {
        let d = setup();
        add(d.path(), 0, "2026-09-08T09:35:00+08:00", "5");
        let r = report(d.path()).unwrap();
        assert_eq!(r["failures"].as_array().unwrap().len(), 1);
        assert!(rows(b"var gridedge_probe=([]); alert(1)").is_err());
        add(d.path(), 0, "2026-09-08T09:35:00+08:00", "3");
        let raw = fs::read_to_string(d.path().join("response-00000.jsonp")).unwrap();
        assert!(rows(
            raw.replace("\"volume\":\"100\"", "\"volume\":\"1.5\"")
                .as_bytes()
        )
        .is_err());
        assert!(rows(
            raw.replace(
                "\"volume\":\"100\"",
                "\"volume\":\"100\",\"unreviewed\":true"
            )
            .as_bytes()
        )
        .is_err());
        let parsed = rows(
            raw.replace("\"volume\":\"100\"", "\"volume\":\"0\"")
                .as_bytes(),
        )
        .unwrap();
        assert_eq!(parsed[0].1.volume, 0);
    }
}
