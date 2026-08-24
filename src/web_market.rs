use crate::data::{validate_bar, MarketBar};
use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

const SOURCE_ID: &str = "eastmoney-web-time-sales";
const PROVIDER_VERSION: &str = "eastmoney-time-sales-dom-v6";
const LEGACY_PROVIDER_VERSION: &str = "eastmoney-time-sales-dom-v5";
const LEGACY_SOURCE_INSTANCE: &str = "8101d65c-bdba-4de3-83e0-8983506f159e";
const LEGACY_FINAL_SEQUENCE: u64 = 2_763;

#[derive(Debug, Clone)]
struct TradeTick {
    sequence: u64,
    event_id: String,
    source_instance_id: Uuid,
    timestamp_us: u64,
    received_us: u64,
    price: Decimal,
    quantity: i64,
}

#[derive(Debug, Clone)]
struct SourceStatus {
    sequence: u64,
    event_id: String,
    source_instance_id: Uuid,
    timestamp_us: u64,
    received_us: u64,
    session_date: NaiveDate,
    status: String,
    covered_through_us: u64,
}

#[derive(Debug, Clone)]
enum StreamEvent {
    Trade(TradeTick),
    Status(SourceStatus),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCompletionKind {
    SessionHistoryComplete,
    LiveContiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceCompletion {
    pub source_sequence: u64,
    pub session_date: NaiveDate,
    pub kind: SourceCompletionKind,
    pub previous_covered_through_us: Option<u64>,
    pub covered_through_us: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketIngestReceipt {
    pub event_timestamp_us: u64,
    pub received_timestamp_us: u64,
    pub bars: Vec<MarketBar>,
    pub completion: Option<SourceCompletion>,
    pub quarantine_reason: Option<String>,
}

impl StreamEvent {
    fn sequence(&self) -> u64 {
        match self {
            Self::Trade(event) => event.sequence,
            Self::Status(event) => event.sequence,
        }
    }

    fn event_id(&self) -> &str {
        match self {
            Self::Trade(event) => &event.event_id,
            Self::Status(event) => &event.event_id,
        }
    }

    fn source_instance_id(&self) -> Uuid {
        match self {
            Self::Trade(event) => event.source_instance_id,
            Self::Status(event) => event.source_instance_id,
        }
    }

    fn timestamp_us(&self) -> u64 {
        match self {
            Self::Trade(event) => event.timestamp_us,
            Self::Status(event) => event.timestamp_us,
        }
    }

    fn received_us(&self) -> u64 {
        match self {
            Self::Trade(event) => event.received_us,
            Self::Status(event) => event.received_us,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingTradeBar {
    end: NaiveDateTime,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: i64,
    amount: Decimal,
}

impl PendingTradeBar {
    fn from_tick(end: NaiveDateTime, tick: &TradeTick) -> Result<Self> {
        let amount = tick
            .price
            .checked_mul(Decimal::from(tick.quantity))
            .context("trade amount overflow")?;
        Ok(Self {
            end,
            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            volume: tick.quantity,
            amount,
        })
    }

    fn with_tick(&self, tick: &TradeTick) -> Result<Self> {
        let tick_amount = tick
            .price
            .checked_mul(Decimal::from(tick.quantity))
            .context("trade amount overflow")?;
        Ok(Self {
            end: self.end,
            open: self.open,
            high: self.high.max(tick.price),
            low: self.low.min(tick.price),
            close: tick.price,
            volume: self
                .volume
                .checked_add(tick.quantity)
                .context("trade volume overflow")?,
            amount: self
                .amount
                .checked_add(tick_amount)
                .context("trade amount overflow")?,
        })
    }

    fn market_bar(&self, symbol: &str) -> Result<MarketBar> {
        let bar = MarketBar {
            timestamp: self.end,
            symbol: symbol.to_owned(),
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            amount: Some(self.amount),
        };
        validate_bar(&bar, symbol)?;
        Ok(bar)
    }
}

/// Rebuilds exact five-minute OHLCV bars from one reviewed, source-sequenced
/// Eastmoney time-and-sales stream. Ticks remain pending until a later
/// SOURCE_STATUS proves that the source is complete through the bucket end.
#[derive(Debug, Clone)]
pub struct WebTradeBarBuilder {
    symbol: String,
    venue: String,
    quote_symbol: String,
    interval_minutes: u32,
    source_instance_id: Option<Uuid>,
    last_sequence: Option<u64>,
    seen_sequences: BTreeMap<u64, String>,
    quarantined_sequences: BTreeSet<u64>,
    pending: BTreeMap<NaiveDateTime, PendingTradeBar>,
    emitted: BTreeSet<NaiveDateTime>,
    covered_through_us: Option<u64>,
    complete_sessions: BTreeSet<NaiveDate>,
}

impl WebTradeBarBuilder {
    pub fn new(symbol: impl Into<String>, interval_minutes: u32) -> Result<Self> {
        let symbol = symbol.into();
        let (quote_symbol, venue) = if let Some(value) = symbol.strip_suffix(".SZ") {
            (value, "XSHE")
        } else if let Some(value) = symbol.strip_suffix(".SH") {
            (value, "XSHG")
        } else {
            bail!("web market symbol must end in .SH or .SZ")
        };
        if quote_symbol.len() != 6 || !quote_symbol.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("web market symbol must contain six digits")
        }
        if interval_minutes == 0 || 120 % interval_minutes != 0 {
            bail!("web trade bar interval must divide the 120-minute A-share session")
        }
        let quote_symbol = quote_symbol.to_owned();
        let venue = venue.to_owned();
        Ok(Self {
            symbol,
            venue,
            quote_symbol,
            interval_minutes,
            source_instance_id: None,
            last_sequence: None,
            seen_sequences: BTreeMap::new(),
            quarantined_sequences: BTreeSet::new(),
            pending: BTreeMap::new(),
            emitted: BTreeSet::new(),
            covered_through_us: None,
            complete_sessions: BTreeSet::new(),
        })
    }

    pub fn has_complete_session(&self, date: NaiveDate) -> bool {
        self.complete_sessions.contains(&date)
    }

    pub fn covered_through_us(&self) -> Option<u64> {
        self.covered_through_us
    }

    pub fn ingest(&mut self, topic: &str, bytes: &[u8]) -> Result<Vec<MarketBar>> {
        Ok(self.ingest_with_receipt(topic, bytes)?.bars)
    }

    pub fn ingest_with_receipt(
        &mut self,
        topic: &str,
        bytes: &[u8],
    ) -> Result<MarketIngestReceipt> {
        self.ingest_with_receipt_bounded(topic, bytes, None)
    }

    pub fn ingest_with_receipt_at(
        &mut self,
        topic: &str,
        bytes: &[u8],
        received_by: NaiveDateTime,
    ) -> Result<MarketIngestReceipt> {
        self.ingest_with_receipt_bounded(topic, bytes, Some(received_by))
    }

    fn ingest_with_receipt_bounded(
        &mut self,
        topic: &str,
        bytes: &[u8],
        received_by: Option<NaiveDateTime>,
    ) -> Result<MarketIngestReceipt> {
        let event = parse_event(topic, bytes, &self.venue, &self.quote_symbol)?;
        if let Some(bound) = received_by {
            if shanghai_datetime(event.timestamp_us())? > bound
                || shanghai_datetime(event.received_us())? > bound
            {
                bail!("market event or source receipt is later than the atomic ingest bound")
            }
        }
        if let Some(source_instance_id) = self.source_instance_id {
            if source_instance_id != event.source_instance_id() {
                bail!("market source instance changed without an explicit reset")
            }
        }
        if let Some(existing) = self.seen_sequences.get(&event.sequence()) {
            if existing == event.event_id() {
                return Ok(MarketIngestReceipt {
                    event_timestamp_us: event.timestamp_us(),
                    received_timestamp_us: event.received_us(),
                    bars: Vec::new(),
                    completion: None,
                    quarantine_reason: self
                        .quarantined_sequences
                        .contains(&event.sequence())
                        .then(|| "OUTSIDE_REVIEWED_A_SHARE_SESSION".to_owned()),
                });
            }
            bail!("market source sequence was reused with a different event")
        }
        if let Some(last_sequence) = self.last_sequence {
            let expected = last_sequence
                .checked_add(1)
                .context("market source sequence overflow")?;
            if event.sequence() != expected {
                bail!("market source sequence is not contiguous")
            }
        }

        validate_event_integrity_before_disposition(&event)?;
        if event_is_outside_reviewed_session(&event)? {
            self.source_instance_id = Some(event.source_instance_id());
            self.last_sequence = Some(event.sequence());
            self.seen_sequences
                .insert(event.sequence(), event.event_id().to_owned());
            self.quarantined_sequences.insert(event.sequence());
            return Ok(MarketIngestReceipt {
                event_timestamp_us: event.timestamp_us(),
                received_timestamp_us: event.received_us(),
                bars: Vec::new(),
                completion: None,
                quarantine_reason: Some("OUTSIDE_REVIEWED_A_SHARE_SESSION".to_owned()),
            });
        }

        let (bars, completion) = match &event {
            StreamEvent::Trade(tick) => {
                self.apply_trade(tick)?;
                (Vec::new(), None)
            }
            StreamEvent::Status(status) => {
                let previous_covered_through_us = self.covered_through_us;
                let kind = match status.status.as_str() {
                    "SESSION_HISTORY_COMPLETE" => SourceCompletionKind::SessionHistoryComplete,
                    "LIVE_CONTIGUOUS" => SourceCompletionKind::LiveContiguous,
                    _ => bail!("market source status has an invalid completion kind"),
                };
                (
                    self.apply_status(status)?,
                    Some(SourceCompletion {
                        source_sequence: status.sequence,
                        session_date: status.session_date,
                        kind,
                        previous_covered_through_us,
                        covered_through_us: status.covered_through_us,
                    }),
                )
            }
        };
        self.source_instance_id = Some(event.source_instance_id());
        self.last_sequence = Some(event.sequence());
        self.seen_sequences
            .insert(event.sequence(), event.event_id().to_owned());
        Ok(MarketIngestReceipt {
            event_timestamp_us: event.timestamp_us(),
            received_timestamp_us: event.received_us(),
            bars,
            completion,
            quarantine_reason: None,
        })
    }

    fn apply_trade(&mut self, tick: &TradeTick) -> Result<()> {
        let timestamp = shanghai_datetime(tick.timestamp_us)?;
        let Some(end) = trade_bucket_end(timestamp, self.interval_minutes)? else {
            return Ok(());
        };
        if self.emitted.contains(&end) {
            bail!("a trade arrived behind an already emitted market watermark")
        }
        let updated = match self.pending.get(&end) {
            Some(existing) => existing.with_tick(tick)?,
            None => PendingTradeBar::from_tick(end, tick)?,
        };
        self.pending.insert(end, updated);
        Ok(())
    }

    fn apply_status(&mut self, status: &SourceStatus) -> Result<Vec<MarketBar>> {
        if status.timestamp_us != status.covered_through_us
            || !matches!(
                status.status.as_str(),
                "SESSION_HISTORY_COMPLETE" | "LIVE_CONTIGUOUS"
            )
        {
            bail!("market source status has an invalid completion watermark")
        }
        if self
            .covered_through_us
            .is_some_and(|previous| status.covered_through_us < previous)
        {
            bail!("market source watermark moved backwards")
        }
        let covered = shanghai_datetime(status.covered_through_us)?;
        if covered.date() != status.session_date {
            bail!("market source watermark disagrees with its session date")
        }
        let completed_ends = self
            .pending
            .keys()
            .copied()
            .take_while(|end| *end <= covered)
            .collect::<Vec<_>>();
        let mut bars = Vec::with_capacity(completed_ends.len());
        for end in &completed_ends {
            bars.push(
                self.pending
                    .get(end)
                    .context("pending market bar disappeared")?
                    .market_bar(&self.symbol)?,
            );
        }
        for end in completed_ends {
            self.pending.remove(&end);
            self.emitted.insert(end);
        }
        self.covered_through_us = Some(status.covered_through_us);
        if status.status == "SESSION_HISTORY_COMPLETE" {
            self.complete_sessions.insert(status.session_date);
        }
        Ok(bars)
    }
}

fn validate_event_integrity_before_disposition(event: &StreamEvent) -> Result<()> {
    if event.timestamp_us() > event.received_us() {
        bail!("market event timestamp is later than its source capture clock")
    }
    if let StreamEvent::Status(status) = event {
        if status.timestamp_us != status.covered_through_us
            || !matches!(
                status.status.as_str(),
                "SESSION_HISTORY_COMPLETE" | "LIVE_CONTIGUOUS"
            )
        {
            bail!("market source status has an invalid completion watermark")
        }
        if shanghai_datetime(status.covered_through_us)?.date() != status.session_date {
            bail!("market source watermark disagrees with its session date")
        }
    }
    Ok(())
}

fn event_is_outside_reviewed_session(event: &StreamEvent) -> Result<bool> {
    let timestamp = shanghai_datetime(event.timestamp_us())?;
    let received = shanghai_datetime(event.received_us())?;
    if timestamp.date() != received.date() || !is_reviewed_ashare_trading_date(timestamp.date())? {
        return Ok(true);
    }
    let time = timestamp.time();
    let received_time = received.time();
    let morning_start = NaiveTime::from_hms_opt(9, 15, 0).expect("valid time");
    let morning_end = NaiveTime::from_hms_opt(11, 30, 0).expect("valid time");
    let afternoon_start = NaiveTime::from_hms_opt(13, 0, 0).expect("valid time");
    let afternoon_end = NaiveTime::from_hms_opt(15, 0, 0).expect("valid time");
    let event_in_session = (time >= morning_start && time <= morning_end)
        || (time >= afternoon_start && time <= afternoon_end);
    let morning_capture_end = NaiveTime::from_hms_opt(11, 31, 0).expect("valid time");
    let afternoon_capture_start = NaiveTime::from_hms_opt(12, 59, 0).expect("valid time");
    let afternoon_capture_end = NaiveTime::from_hms_opt(15, 1, 0).expect("valid time");
    let capture_in_window = (received_time >= morning_start
        && received_time <= morning_capture_end)
        || (received_time >= afternoon_capture_start && received_time <= afternoon_capture_end);
    Ok(!(event_in_session && capture_in_window))
}

fn is_reviewed_ashare_trading_date(date: NaiveDate) -> Result<bool> {
    if date.year() != 2026 {
        bail!("market date is outside reviewed calendar SSE_2026_NOTICE_45")
    }
    if date.weekday().number_from_monday() > 5 {
        return Ok(false);
    }
    const CLOSED: &[(u32, u32)] = &[
        (1, 1),
        (1, 2),
        (2, 16),
        (2, 17),
        (2, 18),
        (2, 19),
        (2, 20),
        (2, 23),
        (4, 6),
        (5, 1),
        (5, 4),
        (5, 5),
        (6, 19),
        (9, 25),
        (10, 1),
        (10, 2),
        (10, 5),
        (10, 6),
        (10, 7),
    ];
    Ok(!CLOSED.contains(&(date.month(), date.day())))
}

fn parse_event(topic: &str, bytes: &[u8], venue: &str, symbol: &str) -> Result<StreamEvent> {
    if bytes.is_empty() || bytes.len() > 1_048_576 {
        bail!("market MQTT payload size is invalid")
    }
    let value: Value = serde_json::from_slice(bytes).context("market MQTT payload is not JSON")?;
    let object = value
        .as_object()
        .context("market event must be one JSON object")?;
    let event_id = string_field(object, "event_id")?;
    validate_sha256(event_id, "market event id")?;
    let mut identity = value.clone();
    let identity_object = identity
        .as_object_mut()
        .context("market identity must be an object")?;
    identity_object.remove("event_id");
    identity_object.remove("recv_us");
    let computed = format!("{:x}", Sha256::digest(serde_json::to_vec(&identity)?));
    if computed != event_id {
        bail!("market event id does not match canonical event bytes")
    }
    if string_field(object, "spec")? != "gridedge.market"
        || u64_field(object, "schema_version")? != 1
    {
        bail!("unsupported market event envelope")
    }
    validate_sha256(
        string_field(object, "evidence_sha256")?,
        "market evidence hash",
    )?;
    let source = object
        .get("source")
        .and_then(Value::as_object)
        .context("market event lacks source")?;
    if string_field(source, "source_id")? != SOURCE_ID
        || string_field(source, "source_type")? != "WEB_UI"
        || string_field(source, "provider")? != "eastmoney"
    {
        bail!("market event source is not the reviewed Eastmoney provider")
    }
    let provider_version = string_field(source, "provider_version")?;
    let source_instance_id = Uuid::parse_str(string_field(source, "source_instance_id")?)
        .context("market source instance is not a UUID")?;
    let instrument = object
        .get("instrument")
        .and_then(Value::as_object)
        .context("market event lacks instrument")?;
    if string_field(instrument, "venue")? != venue
        || string_field(instrument, "symbol")? != symbol
        || string_field(instrument, "asset_class")? != "EQUITY"
        || string_field(instrument, "currency")? != "CNY"
    {
        bail!("market event instrument does not match the configured run")
    }
    let sequence = u64_field(object, "source_sequence")?;
    let timestamp_us = u64_field(object, "ts_us")?;
    let received_us = u64_field(object, "recv_us")?;
    let payload = object
        .get("payload")
        .and_then(Value::as_object)
        .context("market event lacks payload")?;
    if provider_version == PROVIDER_VERSION {
        if u64_field(payload, "source_captured_at_us")? != received_us {
            bail!("market source capture evidence disagrees with recv_us")
        }
    } else if provider_version != LEGACY_PROVIDER_VERSION
        || source_instance_id.to_string() != LEGACY_SOURCE_INSTANCE
        || sequence > LEGACY_FINAL_SEQUENCE
    {
        bail!("market event source version is outside the signed compatibility boundary")
    }
    match string_field(object, "event_type")? {
        "TRADE_TICK" => {
            if topic != format!("gridedge/market/v1/{venue}/{symbol}/trade") {
                bail!("trade event arrived on the wrong MQTT topic")
            }
            let price = payload
                .get("price")
                .and_then(Value::as_object)
                .context("trade tick lacks price")?;
            let mantissa = i64_field(price, "mantissa")?;
            let scale =
                u32::try_from(u64_field(price, "scale")?).context("trade price scale overflow")?;
            if mantissa <= 0 || scale > 6 {
                bail!("trade tick price is invalid")
            }
            let quantity = i64::try_from(u64_field(payload, "quantity")?)
                .context("trade quantity overflow")?;
            if quantity <= 0
                || string_field(payload, "unit")? != "SHARE"
                || string_field(payload, "source_row_key")?.is_empty()
            {
                bail!("trade tick quantity or identity is invalid")
            }
            Ok(StreamEvent::Trade(TradeTick {
                sequence,
                event_id: event_id.to_owned(),
                source_instance_id,
                timestamp_us,
                received_us,
                price: Decimal::from_i128_with_scale(i128::from(mantissa), scale),
                quantity,
            }))
        }
        "SOURCE_STATUS" => {
            if topic != format!("gridedge/market/v1/{venue}/{symbol}/status") {
                bail!("status event arrived on the wrong MQTT topic")
            }
            let session_date =
                NaiveDate::parse_from_str(string_field(payload, "session_date")?, "%Y-%m-%d")
                    .context("source status session date is invalid")?;
            validate_sha256(string_field(payload, "capture_sha256")?, "capture hash")?;
            Ok(StreamEvent::Status(SourceStatus {
                sequence,
                event_id: event_id.to_owned(),
                source_instance_id,
                timestamp_us,
                received_us,
                session_date,
                status: string_field(payload, "status")?.to_owned(),
                covered_through_us: u64_field(payload, "covered_through_us")?,
            }))
        }
        _ => bail!("unsupported market event type for the trade-bar source"),
    }
}

fn shanghai_datetime(timestamp_us: u64) -> Result<NaiveDateTime> {
    let signed =
        i64::try_from(timestamp_us).context("market timestamp exceeds the supported clock")?;
    let utc =
        chrono::DateTime::from_timestamp_micros(signed).context("market timestamp is invalid")?;
    Ok((utc + Duration::hours(8)).naive_utc())
}

fn trade_bucket_end(
    timestamp: NaiveDateTime,
    interval_minutes: u32,
) -> Result<Option<NaiveDateTime>> {
    let date = timestamp.date();
    if date.weekday().number_from_monday() > 5 {
        bail!("trade tick is outside an A-share trading day")
    }
    let time = timestamp.time();
    let preopen = NaiveTime::from_hms_opt(9, 15, 0).expect("valid time");
    let opening_auction = NaiveTime::from_hms_opt(9, 25, 0).expect("valid time");
    let morning_start = NaiveTime::from_hms_opt(9, 30, 0).expect("valid time");
    let morning_end = NaiveTime::from_hms_opt(11, 30, 0).expect("valid time");
    let afternoon_start = NaiveTime::from_hms_opt(13, 0, 0).expect("valid time");
    let afternoon_end = NaiveTime::from_hms_opt(15, 0, 0).expect("valid time");
    if time >= preopen && time < opening_auction {
        return Ok(None);
    }
    if time >= opening_auction
        && time < morning_start + Duration::minutes(i64::from(interval_minutes))
    {
        return Ok(Some(date.and_time(
            morning_start + Duration::minutes(i64::from(interval_minutes)),
        )));
    }
    if time == morning_end || time == afternoon_end {
        return Ok(Some(timestamp));
    }
    for (start, end) in [
        (morning_start, morning_end),
        (afternoon_start, afternoon_end),
    ] {
        if time >= start && time < end {
            let elapsed = timestamp - date.and_time(start);
            let width_seconds = i64::from(interval_minutes) * 60;
            let bucket = elapsed.num_seconds() / width_seconds;
            return Ok(Some(
                date.and_time(start) + Duration::seconds((bucket + 1) * width_seconds),
            ));
        }
    }
    bail!("trade tick is outside the continuous A-share session")
}

fn string_field<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("market field {key} must be a string"))
}

fn u64_field(object: &serde_json::Map<String, Value>, key: &str) -> Result<u64> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("market field {key} must be u64"))
}

fn i64_field(object: &serde_json::Map<String, Value>, key: &str) -> Result<i64> {
    object
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("market field {key} must be i64"))
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be lowercase SHA-256")
    }
    Ok(())
}
