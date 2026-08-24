use crate::{data::MarketBar, ths_sim::SimulationMarketQuote};
use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
struct PendingBar {
    end: NaiveDateTime,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    samples: usize,
}

/// Deterministically aggregates timestamped prices read from the reviewed
/// Tonghuashun simulation page into completed A-share OHLC bars. No synthetic
/// volume or missing bars are invented.
#[derive(Debug, Clone)]
pub struct ThsQuoteBarBuilder {
    quote_symbol: String,
    interval_minutes: u32,
    pending: Option<PendingBar>,
    last_observed_at: Option<NaiveDateTime>,
}

impl ThsQuoteBarBuilder {
    pub fn new(symbol: impl Into<String>, interval_minutes: u32) -> Result<Self> {
        let symbol = symbol.into();
        let quote_symbol = symbol
            .strip_suffix(".SH")
            .or_else(|| symbol.strip_suffix(".SZ"))
            .context("Tonghuashun market symbol must end in .SH or .SZ")?
            .to_owned();
        if quote_symbol.len() != 6 || !quote_symbol.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("Tonghuashun market symbol must contain six digits");
        }
        if interval_minutes == 0 || 120 % interval_minutes != 0 {
            bail!("quote bar interval must divide the 120-minute A-share session");
        }
        Ok(Self {
            quote_symbol,
            interval_minutes,
            pending: None,
            last_observed_at: None,
        })
    }

    pub fn quote_symbol(&self) -> &str {
        &self.quote_symbol
    }

    pub fn push(&mut self, quote: SimulationMarketQuote) -> Result<Option<MarketBar>> {
        quote.validate_reviewed_evidence(&self.quote_symbol)?;
        if self
            .last_observed_at
            .is_some_and(|previous| quote.observed_at <= previous)
        {
            bail!("Tonghuashun quote timestamps must be strictly increasing");
        }
        let started_end = bucket_end(quote.observed_from, self.interval_minutes)?;
        let end = bucket_end(quote.observed_at, self.interval_minutes)?;
        if started_end != end {
            bail!("Tonghuashun quote evidence crossed a market-bar boundary");
        }
        let emitted = match self.pending.as_mut() {
            Some(pending) if pending.end == end => {
                pending.high = pending.high.max(quote.last_price);
                pending.low = pending.low.min(quote.last_price);
                pending.close = quote.last_price;
                pending.samples = pending
                    .samples
                    .checked_add(1)
                    .context("Tonghuashun quote sample count overflow")?;
                None
            }
            Some(pending) if pending.end > end => {
                bail!("Tonghuashun quote moved into an earlier market bucket")
            }
            Some(_) => {
                bail!("discrete last-price samples cannot complete an OHLC market bar")
            }
            None => {
                self.pending = Some(PendingBar::from_quote(end, quote.last_price));
                None
            }
        };
        self.last_observed_at = Some(quote.observed_at);
        Ok(emitted)
    }

    pub fn flush_through(&mut self, now: NaiveDateTime) -> Result<Option<MarketBar>> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.end <= now)
        {
            bail!("discrete last-price samples cannot complete an OHLC market bar");
        }
        Ok(None)
    }
}

impl PendingBar {
    fn from_quote(end: NaiveDateTime, price: Decimal) -> Self {
        Self {
            end,
            high: price,
            low: price,
            close: price,
            samples: 1,
        }
    }
}

fn bucket_end(observed_at: NaiveDateTime, interval_minutes: u32) -> Result<NaiveDateTime> {
    let date = observed_at.date();
    let time = observed_at.time();
    if date.weekday().number_from_monday() > 5 {
        bail!("Tonghuashun quote is outside an A-share trading day at {observed_at}");
    }
    for (session_start, session_end) in sessions(date) {
        if observed_at >= session_start && observed_at < session_end {
            let elapsed = observed_at - session_start;
            let width_seconds = i64::from(interval_minutes) * 60;
            let bucket = elapsed.num_seconds() / width_seconds;
            let end = session_start + Duration::seconds((bucket + 1) * width_seconds);
            return Ok(end.min(session_end));
        }
    }
    bail!("Tonghuashun quote is outside the A-share trading session at {time}")
}

fn sessions(date: NaiveDate) -> [(NaiveDateTime, NaiveDateTime); 2] {
    let morning_start = date.and_time(NaiveTime::from_hms_opt(9, 30, 0).expect("valid time"));
    let morning_end = date.and_time(NaiveTime::from_hms_opt(11, 30, 0).expect("valid time"));
    let afternoon_start = date.and_time(NaiveTime::from_hms_opt(13, 0, 0).expect("valid time"));
    let afternoon_end = date.and_time(NaiveTime::from_hms_opt(15, 0, 0).expect("valid time"));
    [
        (morning_start, morning_end),
        (afternoon_start, afternoon_end),
    ]
}
