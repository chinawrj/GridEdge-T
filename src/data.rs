use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs::File, io::Read, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketBar {
    #[serde(with = "csv_datetime")]
    pub timestamp: NaiveDateTime,
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
    pub volume: i64,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub amount: Option<Decimal>,
}

mod csv_datetime {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.format(FORMAT).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&value, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub trait MarketDataFeed {
    fn next_bar(&mut self) -> Option<MarketBar>;
    fn pause(&mut self);
    fn resume(&mut self);
    fn stop(&mut self);
}

pub struct CsvReplayFeed {
    bars: Vec<MarketBar>,
    cursor: usize,
    paused: bool,
    stopped: bool,
}

impl CsvReplayFeed {
    pub fn load(path: impl AsRef<Path>, expected_symbol: &str) -> Result<Self> {
        let file = File::open(path.as_ref())
            .with_context(|| format!("failed to open replay file {}", path.as_ref().display()))?;
        Self::from_reader(file, &path.as_ref().display().to_string(), expected_symbol)
    }

    pub fn load_bytes(bytes: &[u8], source: &str, expected_symbol: &str) -> Result<Self> {
        Self::from_reader(bytes, source, expected_symbol)
    }

    fn from_reader(source_reader: impl Read, source: &str, expected_symbol: &str) -> Result<Self> {
        let mut reader = csv::Reader::from_reader(source_reader);
        let mut bars = Vec::new();
        let mut seen = BTreeSet::new();
        let mut previous = None;
        for (row_number, row) in reader.deserialize::<MarketBar>().enumerate() {
            let bar =
                row.with_context(|| format!("invalid CSV row {} in {source}", row_number + 2))?;
            validate_bar(&bar, expected_symbol)?;
            if !seen.insert(bar.timestamp) {
                bail!("duplicate timestamp {}", bar.timestamp)
            }
            if previous.is_some_and(|p| p >= bar.timestamp) {
                bail!(
                    "timestamps are not strictly increasing at {}",
                    bar.timestamp
                )
            }
            previous = Some(bar.timestamp);
            bars.push(bar);
        }
        if bars.is_empty() {
            bail!("replay file contains no bars")
        }
        Ok(Self {
            bars,
            cursor: 0,
            paused: false,
            stopped: false,
        })
    }

    pub fn bars(&self) -> &[MarketBar] {
        &self.bars
    }
}

impl MarketDataFeed for CsvReplayFeed {
    fn next_bar(&mut self) -> Option<MarketBar> {
        if self.paused || self.stopped {
            return None;
        }
        let bar = self.bars.get(self.cursor).cloned();
        if bar.is_some() {
            self.cursor += 1;
        }
        bar
    }

    fn pause(&mut self) {
        self.paused = true;
    }

    fn resume(&mut self) {
        self.paused = false;
    }

    fn stop(&mut self) {
        self.stopped = true;
    }
}

pub(crate) fn validate_bar(bar: &MarketBar, expected_symbol: &str) -> Result<()> {
    if bar.symbol != expected_symbol {
        bail!("symbol {} does not match {}", bar.symbol, expected_symbol)
    }
    if [bar.open, bar.high, bar.low, bar.close]
        .iter()
        .any(|p| *p <= Decimal::ZERO)
    {
        bail!("prices must be positive at {}", bar.timestamp)
    }
    if bar.low > bar.open
        || bar.low > bar.close
        || bar.high < bar.open
        || bar.high < bar.close
        || bar.low > bar.high
    {
        bail!("invalid OHLC relationship at {}", bar.timestamp)
    }
    if bar.volume < 0 {
        bail!("negative volume at {}", bar.timestamp)
    }
    if bar.volume > crate::config::MAX_SHARE_QUANTITY {
        bail!("volume exceeds the numeric envelope at {}", bar.timestamp)
    }
    if bar.amount.is_some_and(|amount| amount < Decimal::ZERO) {
        bail!("negative amount at {}", bar.timestamp)
    }
    for price in [bar.open, bar.high, bar.low, bar.close] {
        price
            .checked_mul(Decimal::from(crate::config::MAX_SHARE_QUANTITY))
            .with_context(|| {
                format!(
                    "market price exceeds the numeric envelope at {}",
                    bar.timestamp
                )
            })?;
    }
    if !is_trading_time(bar.timestamp.date(), bar.timestamp.time()) {
        bail!("bar outside A-share trading session at {}", bar.timestamp)
    }
    Ok(())
}

fn is_trading_time(_date: NaiveDate, time: NaiveTime) -> bool {
    let morning_start = NaiveTime::from_hms_opt(9, 30, 0).expect("valid time");
    let morning_end = NaiveTime::from_hms_opt(11, 30, 0).expect("valid time");
    let afternoon_start = NaiveTime::from_hms_opt(13, 0, 0).expect("valid time");
    let afternoon_end = NaiveTime::from_hms_opt(15, 0, 0).expect("valid time");
    (morning_start..=morning_end).contains(&time)
        || (afternoon_start..=afternoon_end).contains(&time)
}

pub trait Clock {
    fn now(&self) -> NaiveDateTime;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> NaiveDateTime {
        chrono::Utc::now().naive_utc()
    }
}

pub struct ReplayClock {
    current: NaiveDateTime,
}
impl ReplayClock {
    pub fn new(current: NaiveDateTime) -> Self {
        Self { current }
    }
    pub fn advance(&mut self, current: NaiveDateTime) {
        self.current = current;
    }
}
impl Clock for ReplayClock {
    fn now(&self) -> NaiveDateTime {
        self.current
    }
}
