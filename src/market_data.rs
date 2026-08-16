use crate::data::MarketBar;
use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use flate2::read::ZlibDecoder;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    str::FromStr,
    thread,
    time::Duration as StdDuration,
};

const BAOSTOCK_HOST: &str = "public-api.baostock.com";
const BAOSTOCK_PORT: u16 = 10030;
const CLIENT_VERSION: &str = "00.9.30";
const HEADER_LENGTH: usize = 21;
const SPLIT: char = '\x01';
const PAGE_SIZE: usize = 2_000;
const MINUTE_FIELDS: &str = "date,time,code,open,high,low,close,volume,amount,adjustflag";
const COMPRESSED_END: &[u8] = b"<![CDATA[]]>\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Adjustment {
    None,
    Forward,
    Backward,
}

impl Adjustment {
    fn flag(self) -> &'static str {
        match self {
            Self::Backward => "1",
            Self::Forward => "2",
            Self::None => "3",
        }
    }
}

impl FromStr for Adjustment {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "none" | "raw" | "3" => Ok(Self::None),
            "forward" | "qfq" | "2" => Ok(Self::Forward),
            "backward" | "hfq" | "1" => Ok(Self::Backward),
            _ => bail!("adjustment must be none, forward/qfq, or backward/hfq"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub symbol: String,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub frequency_minutes: u16,
    pub adjustment: Adjustment,
    pub output: PathBuf,
    pub raw_output: PathBuf,
    pub report_output: PathBuf,
    pub retries: u8,
    pub rate_limit_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityReport {
    pub source: String,
    pub source_endpoint: String,
    pub symbol: String,
    pub requested_start: NaiveDate,
    pub requested_end: NaiveDate,
    pub frequency_minutes: u16,
    pub adjustment: Adjustment,
    pub rows: usize,
    pub trading_days: usize,
    pub full_trading_days: usize,
    pub partial_trading_days: Vec<PartialTradingDay>,
    pub first_timestamp: NaiveDateTime,
    pub last_timestamp: NaiveDateTime,
    pub duplicate_timestamps: usize,
    pub invalid_ohlc_rows: usize,
    pub negative_volume_rows: usize,
    pub off_session_rows: usize,
    pub zero_volume_rows: usize,
    pub missing_expected_bars_on_observed_days: usize,
    pub processed_sha256: String,
    pub raw_file: String,
    pub processed_file: String,
    pub generated_at_utc: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialTradingDay {
    pub date: NaiveDate,
    pub bars: usize,
    pub expected: usize,
}

#[derive(Debug, Clone)]
struct NativeBar {
    date: String,
    time: String,
    code: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    amount: String,
    adjustflag: String,
}

#[derive(Debug, Deserialize)]
struct RecordSet {
    record: Vec<Vec<String>>,
}

struct BaoStockClient {
    connection: BufReader<TcpStream>,
}

impl BaoStockClient {
    fn connect() -> Result<Self> {
        let address = (BAOSTOCK_HOST, BAOSTOCK_PORT)
            .to_socket_addrs()
            .context("failed to resolve BaoStock")?
            .next()
            .context("BaoStock resolved to no address")?;
        let stream = TcpStream::connect_timeout(&address, StdDuration::from_secs(20))
            .context("failed to connect to BaoStock")?;
        stream.set_read_timeout(Some(StdDuration::from_secs(45)))?;
        stream.set_write_timeout(Some(StdDuration::from_secs(20)))?;
        let mut client = Self {
            connection: BufReader::new(stream),
        };
        let login = format!("login{SPLIT}anonymous{SPLIT}123456{SPLIT}0");
        let response = client.exchange("00", &login)?;
        let fields: Vec<_> = response.split(SPLIT).collect();
        if fields.first().copied() != Some("0") {
            bail!(
                "BaoStock login failed: {} {}",
                fields.first().unwrap_or(&"unknown"),
                fields.get(1).unwrap_or(&"unknown error")
            )
        }
        Ok(client)
    }

    fn query_history(
        &mut self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        frequency_minutes: u16,
        adjustment: Adjustment,
    ) -> Result<Vec<NativeBar>> {
        let native_symbol = to_baostock_symbol(symbol)?;
        let mut all = Vec::new();
        for page in 1usize.. {
            let body = [
                "query_history_k_data_plus".to_owned(),
                "anonymous".to_owned(),
                page.to_string(),
                PAGE_SIZE.to_string(),
                native_symbol.clone(),
                MINUTE_FIELDS.to_owned(),
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
                frequency_minutes.to_string(),
                adjustment.flag().to_owned(),
            ]
            .join("\x01");
            let response = self.exchange("95", &body)?;
            let fields: Vec<_> = response.split(SPLIT).collect();
            if fields.first().copied() != Some("0") {
                bail!(
                    "BaoStock history query failed: {} {}",
                    fields.first().unwrap_or(&"unknown"),
                    fields.get(1).unwrap_or(&"unknown error")
                )
            }
            let json = fields
                .get(6)
                .context("BaoStock response has no record set")?;
            let records: RecordSet =
                serde_json::from_str(json).context("invalid BaoStock record JSON")?;
            let count = records.record.len();
            for row in records.record {
                if row.len() != 10 {
                    bail!("BaoStock minute row has {} fields, expected 10", row.len())
                }
                all.push(NativeBar {
                    date: row[0].clone(),
                    time: row[1].clone(),
                    code: row[2].clone(),
                    open: row[3].clone(),
                    high: row[4].clone(),
                    low: row[5].clone(),
                    close: row[6].clone(),
                    volume: row[7].clone(),
                    amount: row[8].clone(),
                    adjustflag: row[9].clone(),
                });
            }
            if count < PAGE_SIZE {
                break;
            }
        }
        Ok(all)
    }

    fn exchange(&mut self, message_type: &str, body: &str) -> Result<String> {
        let header = format!(
            "{CLIENT_VERSION}{SPLIT}{message_type}{SPLIT}{:010}",
            body.len()
        );
        debug_assert_eq!(header.len(), HEADER_LENGTH);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(format!("{header}{body}").as_bytes());
        let crc = hasher.finalize();
        let message = format!("{header}{body}{SPLIT}{crc}\n");
        self.connection
            .get_mut()
            .write_all(message.as_bytes())
            .context("failed to send BaoStock request")?;
        self.connection.get_mut().flush()?;

        let mut raw = Vec::new();
        loop {
            let read = self
                .connection
                .read_until(b'\n', &mut raw)
                .context("failed to receive BaoStock response")?;
            if read == 0 {
                bail!("BaoStock closed the connection")
            }
            if raw.len() >= HEADER_LENGTH {
                let response_type = std::str::from_utf8(&raw[8..10])?;
                if response_type == "96" {
                    if raw.ends_with(COMPRESSED_END) {
                        break;
                    }
                } else if raw.ends_with(b"\n") {
                    break;
                }
            }
            if raw.len() > 64 * 1024 * 1024 {
                bail!("BaoStock response exceeded safety limit")
            }
        }
        parse_response(&raw)
    }
}

fn parse_response(raw: &[u8]) -> Result<String> {
    if raw.len() < HEADER_LENGTH {
        bail!("BaoStock response is shorter than its header")
    }
    let header = std::str::from_utf8(&raw[..HEADER_LENGTH])?;
    let header_fields: Vec<_> = header.split(SPLIT).collect();
    if header_fields.len() != 3 {
        bail!("invalid BaoStock response header")
    }
    let message_type = header_fields[1];
    let body_length = header_fields[2]
        .parse::<usize>()
        .context("invalid BaoStock body length")?;
    let end = HEADER_LENGTH
        .checked_add(body_length)
        .context("BaoStock body length overflow")?;
    if end > raw.len() {
        bail!("truncated BaoStock response body")
    }
    let body = &raw[HEADER_LENGTH..end];
    let decoded = if message_type == "96" {
        let mut decoder = ZlibDecoder::new(body);
        let mut decoded = String::new();
        decoder
            .read_to_string(&mut decoded)
            .context("failed to decompress BaoStock response")?;
        decoded
    } else {
        String::from_utf8(body.to_vec()).context("BaoStock response is not UTF-8")?
    };
    Ok(decoded.trim_end_matches(SPLIT).to_owned())
}

pub fn fetch_klines(request: &FetchRequest) -> Result<DataQualityReport> {
    validate_request(request)?;
    ensure_parent(&request.output)?;
    ensure_parent(&request.raw_output)?;
    ensure_parent(&request.report_output)?;

    let mut native_rows = Vec::new();
    for (segment_index, (start, end)) in date_segments(request.start, request.end)
        .into_iter()
        .enumerate()
    {
        let mut last_error = None;
        for attempt in 0..=request.retries {
            match BaoStockClient::connect().and_then(|mut client| {
                client.query_history(
                    &request.symbol,
                    start,
                    end,
                    request.frequency_minutes,
                    request.adjustment,
                )
            }) {
                Ok(mut rows) => {
                    native_rows.append(&mut rows);
                    last_error = None;
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < request.retries {
                        thread::sleep(StdDuration::from_millis(
                            500u64.saturating_mul(1u64 << attempt.min(6)),
                        ));
                    }
                }
            }
        }
        if let Some(error) = last_error {
            return Err(error)
                .with_context(|| format!("failed BaoStock segment {start} through {end}"));
        }
        if segment_index > 0 || request.rate_limit_ms > 0 {
            thread::sleep(StdDuration::from_millis(request.rate_limit_ms));
        }
    }

    write_native_csv(&request.raw_output, &native_rows)?;
    let mut unique = BTreeMap::new();
    for row in &native_rows {
        let bar = native_to_market_bar(row, &request.symbol)?;
        unique.insert(bar.timestamp, bar);
    }
    let bars: Vec<_> = unique.into_values().collect();
    if bars.is_empty() {
        bail!("BaoStock returned no bars for the requested range")
    }
    write_processed_csv(&request.output, &bars)?;
    let mut report = build_quality_report(request, &bars, native_rows.len())?;
    report.processed_sha256 = sha256_file(&request.output)?;
    let report_json = serde_json::to_vec_pretty(&report)?;
    fs::write(&request.report_output, report_json).with_context(|| {
        format!(
            "failed to write quality report {}",
            request.report_output.display()
        )
    })?;
    Ok(report)
}

fn validate_request(request: &FetchRequest) -> Result<()> {
    if request.start > request.end {
        bail!("start date must not exceed end date")
    }
    if !matches!(request.frequency_minutes, 5 | 15 | 30 | 60) {
        bail!("BaoStock supports minute frequencies 5, 15, 30, and 60")
    }
    to_baostock_symbol(&request.symbol)?;
    Ok(())
}

fn date_segments(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let segment_end = (cursor + Duration::days(89)).min(end);
        result.push((cursor, segment_end));
        cursor = segment_end + Duration::days(1);
    }
    result
}

fn to_baostock_symbol(symbol: &str) -> Result<String> {
    let normalized = symbol.trim().to_ascii_uppercase();
    let (code, market) = normalized
        .split_once('.')
        .context("symbol must use CODE.SZ or CODE.SH format")?;
    if code.len() != 6 || !code.chars().all(|character| character.is_ascii_digit()) {
        bail!("symbol code must contain six digits")
    }
    let prefix = match market {
        "SZ" => "sz",
        "SH" => "sh",
        _ => bail!("symbol market must be SZ or SH"),
    };
    Ok(format!("{prefix}.{code}"))
}

fn native_to_market_bar(row: &NativeBar, symbol: &str) -> Result<MarketBar> {
    let timestamp = parse_baostock_time(&row.time)?;
    let date = NaiveDate::parse_from_str(&row.date, "%Y-%m-%d")?;
    if timestamp.date() != date {
        bail!("BaoStock row date/time mismatch: {} {}", row.date, row.time)
    }
    let expected_native = to_baostock_symbol(symbol)?;
    if row.code.to_ascii_lowercase() != expected_native {
        bail!("BaoStock returned unexpected symbol {}", row.code)
    }
    Ok(MarketBar {
        timestamp,
        symbol: symbol.to_ascii_uppercase(),
        open: Decimal::from_str(&row.open).context("invalid open price")?,
        high: Decimal::from_str(&row.high).context("invalid high price")?,
        low: Decimal::from_str(&row.low).context("invalid low price")?,
        close: Decimal::from_str(&row.close).context("invalid close price")?,
        volume: row.volume.parse().context("invalid volume")?,
        amount: if row.amount.trim().is_empty() {
            None
        } else {
            Some(Decimal::from_str(&row.amount).context("invalid amount")?)
        },
    })
}

fn parse_baostock_time(value: &str) -> Result<NaiveDateTime> {
    if value.len() < 14
        || !value[..14]
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        bail!("invalid BaoStock minute timestamp {value}")
    }
    NaiveDateTime::parse_from_str(&value[..14], "%Y%m%d%H%M%S")
        .with_context(|| format!("invalid BaoStock minute timestamp {value}"))
}

fn write_native_csv(path: &Path, rows: &[NativeBar]) -> Result<()> {
    let mut writer = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create raw data file {}", path.display()))?;
    writer.write_record([
        "date",
        "time",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adjustflag",
    ])?;
    for row in rows {
        writer.write_record([
            &row.date,
            &row.time,
            &row.code,
            &row.open,
            &row.high,
            &row.low,
            &row.close,
            &row.volume,
            &row.amount,
            &row.adjustflag,
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_processed_csv(path: &Path, bars: &[MarketBar]) -> Result<()> {
    let mut writer = csv::Writer::from_path(path)
        .with_context(|| format!("failed to create replay file {}", path.display()))?;
    for bar in bars {
        writer.serialize(bar)?;
    }
    writer.flush()?;
    Ok(())
}

fn build_quality_report(
    request: &FetchRequest,
    bars: &[MarketBar],
    source_rows: usize,
) -> Result<DataQualityReport> {
    let expected_per_day = 240usize / usize::from(request.frequency_minutes);
    let mut per_day = BTreeMap::<NaiveDate, usize>::new();
    let mut seen = BTreeSet::new();
    let mut duplicate_timestamps = source_rows.saturating_sub(bars.len());
    let mut invalid_ohlc_rows = 0;
    let mut negative_volume_rows = 0;
    let mut off_session_rows = 0;
    let mut zero_volume_rows = 0;
    for bar in bars {
        if !seen.insert(bar.timestamp) {
            duplicate_timestamps += 1;
        }
        *per_day.entry(bar.timestamp.date()).or_default() += 1;
        if bar.low > bar.open
            || bar.low > bar.close
            || bar.high < bar.open
            || bar.high < bar.close
            || bar.low > bar.high
            || [bar.open, bar.high, bar.low, bar.close]
                .iter()
                .any(|price| *price <= Decimal::ZERO)
        {
            invalid_ohlc_rows += 1;
        }
        if bar.volume < 0 {
            negative_volume_rows += 1;
        } else if bar.volume == 0 {
            zero_volume_rows += 1;
        }
        if !is_expected_minute(bar.timestamp, request.frequency_minutes) {
            off_session_rows += 1;
        }
    }
    let partial_trading_days: Vec<_> = per_day
        .iter()
        .filter(|(_, count)| **count != expected_per_day)
        .map(|(date, count)| PartialTradingDay {
            date: *date,
            bars: *count,
            expected: expected_per_day,
        })
        .collect();
    let missing_expected_bars_on_observed_days = partial_trading_days
        .iter()
        .map(|day| day.expected.saturating_sub(day.bars))
        .sum();
    let first_timestamp = bars
        .first()
        .context("quality report requires bars")?
        .timestamp;
    let last_timestamp = bars
        .last()
        .context("quality report requires bars")?
        .timestamp;
    let mut notes = vec![
        "Observed-day completeness does not classify whole-day stock suspensions as missing data."
            .to_owned(),
        "Unadjusted bars are intended for execution replay; forward-adjusted bars are better suited to return-based research."
            .to_owned(),
        "Source data is for research use; verify vendor terms before redistribution.".to_owned(),
    ];
    if last_timestamp.date() < request.end {
        notes.push(format!(
            "Last returned trading date is {}, before requested end {}; this can reflect weekends, holidays, suspension, or source publication latency.",
            last_timestamp.date(), request.end
        ));
    }
    Ok(DataQualityReport {
        source: "BaoStock".to_owned(),
        source_endpoint: format!("{BAOSTOCK_HOST}:{BAOSTOCK_PORT}"),
        symbol: request.symbol.to_ascii_uppercase(),
        requested_start: request.start,
        requested_end: request.end,
        frequency_minutes: request.frequency_minutes,
        adjustment: request.adjustment,
        rows: bars.len(),
        trading_days: per_day.len(),
        full_trading_days: per_day.len().saturating_sub(partial_trading_days.len()),
        partial_trading_days,
        first_timestamp,
        last_timestamp,
        duplicate_timestamps,
        invalid_ohlc_rows,
        negative_volume_rows,
        off_session_rows,
        zero_volume_rows,
        missing_expected_bars_on_observed_days,
        processed_sha256: String::new(),
        raw_file: request.raw_output.display().to_string(),
        processed_file: request.output.display().to_string(),
        generated_at_utc: chrono::Utc::now().to_rfc3339(),
        notes,
    })
}

fn is_expected_minute(timestamp: NaiveDateTime, frequency: u16) -> bool {
    if timestamp.second() != 0 || !timestamp.minute().is_multiple_of(u32::from(frequency)) {
        return false;
    }
    let minute = timestamp.hour() * 60 + timestamp.minute();
    let morning = (9 * 60 + 30 + u32::from(frequency)..=11 * 60 + 30).contains(&minute);
    let afternoon = (13 * 60 + u32::from(frequency)..=15 * 60).contains(&minute);
    morning || afternoon
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

pub fn default_paths(
    symbol: &str,
    frequency_minutes: u16,
    adjustment: Adjustment,
    start: NaiveDate,
    end: NaiveDate,
) -> (PathBuf, PathBuf, PathBuf) {
    let adjustment_name = match adjustment {
        Adjustment::None => "raw",
        Adjustment::Forward => "qfq",
        Adjustment::Backward => "hfq",
    };
    let stem = format!(
        "{}_{}m_{}_{}_{}",
        symbol.to_ascii_uppercase(),
        frequency_minutes,
        adjustment_name,
        start.format("%Y%m%d"),
        end.format("%Y%m%d")
    );
    (
        PathBuf::from(format!("data/processed/{stem}.csv")),
        PathBuf::from(format!("data/raw/baostock_{stem}.csv")),
        PathBuf::from(format!("data/reports/{stem}.quality.json")),
    )
}

pub fn default_three_year_range(today: NaiveDate) -> (NaiveDate, NaiveDate) {
    let start = NaiveDate::from_ymd_opt(today.year() - 3, today.month(), today.day())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(today.year() - 3, today.month(), 28).unwrap());
    (start, today)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_are_normalized_for_baostock() {
        assert_eq!(to_baostock_symbol("002256.SZ").unwrap(), "sz.002256");
        assert_eq!(to_baostock_symbol("600000.sh").unwrap(), "sh.600000");
        assert!(to_baostock_symbol("002256").is_err());
    }

    #[test]
    fn ranges_are_split_without_overlap() {
        let start = NaiveDate::from_ymd_opt(2023, 8, 14).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 14).unwrap();
        let segments = date_segments(start, end);
        assert_eq!(segments.first().unwrap().0, start);
        assert_eq!(segments.last().unwrap().1, end);
        for pair in segments.windows(2) {
            assert_eq!(pair[0].1 + Duration::days(1), pair[1].0);
        }
    }

    #[test]
    fn baostock_timestamp_is_parsed_at_bar_end() {
        let value = parse_baostock_time("20240102093500000").unwrap();
        assert_eq!(
            value.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-02 09:35:00"
        );
        assert!(is_expected_minute(value, 5));
        assert!(!is_expected_minute(
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            5
        ));
    }

    #[test]
    fn three_year_range_handles_leap_day() {
        let today = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(
            default_three_year_range(today).0,
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );
    }
}
